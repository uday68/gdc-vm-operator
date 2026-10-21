package workqueue

import (
	"container/heap"
	"math"
	"sync"
	"time"
)

// Clock defines time operations for testing and deterministic timing.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer abstracts time.Timer for unit testing.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) NewTimer(d time.Duration) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

type realTimer struct {
	t *time.Timer
}

func (r *realTimer) C() <-chan time.Time { return r.t.C }
func (r *realTimer) Stop() bool          { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool {
	return r.t.Reset(d)
}

// RateLimiter calculates retry delay based on failure frequency.
type RateLimiter interface {
	When(item string) time.Duration
	Forget(item string)
	NumRequeues(item string) int
	Reset()
}

// ExponentialFailureRateLimiter doubles delay on each consecutive failure up to maxDelay.
type ExponentialFailureRateLimiter struct {
	baseDelay time.Duration
	maxDelay  time.Duration
	failures  map[string]int
	mu        sync.Mutex
}

// NewExponentialFailureRateLimiter initializes an exponential backoff limiter.
func NewExponentialFailureRateLimiter(baseDelay, maxDelay time.Duration) *ExponentialFailureRateLimiter {
	if baseDelay <= 0 {
		baseDelay = 200 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = 10 * time.Second
	}
	return &ExponentialFailureRateLimiter{
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		failures:  make(map[string]int),
	}
}

// When returns the backoff delay for the given item.
func (r *ExponentialFailureRateLimiter) When(item string) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.failures[item]++
	exp := r.failures[item] - 1
	if exp > 30 {
		exp = 30
	}

	backoff := float64(r.baseDelay) * math.Pow(2, float64(exp))
	if backoff > float64(r.maxDelay) {
		return r.maxDelay
	}
	return time.Duration(backoff)
}

// Forget clears the failure count once an item reconciles successfully.
func (r *ExponentialFailureRateLimiter) Forget(item string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failures, item)
}

// NumRequeues returns consecutive failure count for diagnostics.
func (r *ExponentialFailureRateLimiter) NumRequeues(item string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failures[item]
}

// Reset clears all failure tracking.
func (r *ExponentialFailureRateLimiter) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = make(map[string]int)
}

// WorkQueue is the primary interface for deduplicating, rate-limited workqueues.
type WorkQueue interface {
	Add(item string)
	Len() int
	Get() (item string, shutdown bool)
	Done(item string)
	ShutDown()
	IsShuttingDown() bool
	AddAfter(item string, duration time.Duration)
	AddRateLimited(item string) time.Duration
	Forget(item string)
	NumRequeues(item string) int
}

type waitFor struct {
	data    string
	readyAt time.Time
	index   int
}

type waitForPriorityQueue []*waitFor

func (pq waitForPriorityQueue) Len() int           { return len(pq) }
func (pq waitForPriorityQueue) Less(i, j int) bool { return pq[i].readyAt.Before(pq[j].readyAt) }
func (pq waitForPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *waitForPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*waitFor)
	item.index = n
	*pq = append(*pq, item)
}
func (pq *waitForPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// Queue provides a thread-safe, deduplicating workqueue for controller events.
type Queue struct {
	queue        []string
	dirty        map[string]struct{} // items scheduled or waiting
	processing   map[string]struct{} // items currently being reconciled
	cond         *sync.Cond
	shuttingDown bool
	stopCh       chan struct{}
	waitingCh    chan waitFor
	clock        Clock
	rateLimiter  RateLimiter
	wg           sync.WaitGroup
	mu           sync.Mutex
}

// NewQueue constructs a Kubernetes-style deduplicating workqueue with rate limiting and centralized delay handling.
func NewQueue(limiter RateLimiter) *Queue {
	return NewQueueWithClock(limiter, realClock{})
}

// NewQueueWithClock constructs a workqueue with an injectable clock for deterministic testing.
func NewQueueWithClock(limiter RateLimiter, clock Clock) *Queue {
	if limiter == nil {
		limiter = NewExponentialFailureRateLimiter(200*time.Millisecond, 10*time.Second)
	}
	if clock == nil {
		clock = realClock{}
	}
	q := &Queue{
		dirty:       make(map[string]struct{}),
		processing:  make(map[string]struct{}),
		stopCh:      make(chan struct{}),
		waitingCh:   make(chan waitFor, 2048),
		clock:       clock,
		rateLimiter: limiter,
	}
	q.cond = sync.NewCond(&q.mu)

	// Launch single centralized background delay manager
	q.wg.Add(1)
	go q.waitingLoop()

	return q
}

// waitingLoop processes delayed items using a single background goroutine and a min-heap priority queue.
// This completely avoids unbounded goroutine spawning on retries.
func (q *Queue) waitingLoop() {
	defer q.wg.Done()

	pq := &waitForPriorityQueue{}
	heap.Init(pq)

	var nextTimer Timer
	var timerCh <-chan time.Time

	for {
		if q.IsShuttingDown() {
			if nextTimer != nil {
				nextTimer.Stop()
			}
			return
		}

		now := q.clock.Now()
		// Drain all expired items from priority queue into workqueue
		for pq.Len() > 0 {
			first := (*pq)[0]
			if first.readyAt.After(now) {
				break
			}
			item := heap.Pop(pq).(*waitFor)
			q.Add(item.data)
		}

		// Calculate timer for earliest pending item
		if pq.Len() > 0 {
			earliest := (*pq)[0].readyAt
			d := earliest.Sub(now)
			if d <= 0 {
				d = time.Nanosecond
			}
			if nextTimer == nil {
				nextTimer = q.clock.NewTimer(d)
			} else {
				nextTimer.Reset(d)
			}
			timerCh = nextTimer.C()
		} else {
			timerCh = nil
		}

		select {
		case <-q.stopCh:
			if nextTimer != nil {
				nextTimer.Stop()
			}
			return
		case entry, ok := <-q.waitingCh:
			if !ok {
				if nextTimer != nil {
					nextTimer.Stop()
				}
				return
			}
			if entry.readyAt.After(q.clock.Now()) {
				heap.Push(pq, &entry)
			} else {
				q.Add(entry.data)
			}
		case <-timerCh:
			// Timer fired, loop back to drain expired items
		}
	}
}

// Add queues an item if not already dirty (deduplication).
func (q *Queue) Add(item string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.shuttingDown {
		return
	}

	if _, exists := q.dirty[item]; exists {
		return
	}

	q.dirty[item] = struct{}{}
	if _, isProcessing := q.processing[item]; isProcessing {
		return
	}

	q.queue = append(q.queue, item)
	q.cond.Signal()
}

// AddAfter schedules an item to be added after the specified duration.
func (q *Queue) AddAfter(item string, duration time.Duration) {
	if q.IsShuttingDown() {
		return
	}
	if duration <= 0 {
		q.Add(item)
		return
	}

	select {
	case q.waitingCh <- waitFor{data: item, readyAt: q.clock.Now().Add(duration)}:
	case <-q.stopCh:
	}
}

// AddRateLimited schedules an item after an exponential backoff delay.
func (q *Queue) AddRateLimited(item string) time.Duration {
	delay := q.rateLimiter.When(item)
	q.AddAfter(item, delay)
	return delay
}

// Forget resets failure tracking after a successful reconciliation pass.
func (q *Queue) Forget(item string) {
	q.rateLimiter.Forget(item)
}

// NumRequeues returns consecutive retry count.
func (q *Queue) NumRequeues(item string) int {
	return q.rateLimiter.NumRequeues(item)
}

// Get blocks until an item is available or queue is shut down.
// Once returned, the item is marked as processing and cannot be returned to any other worker
// until Done(item) is called.
func (q *Queue) Get() (item string, shutdown bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.queue) == 0 && !q.shuttingDown {
		q.cond.Wait()
	}

	if len(q.queue) == 0 {
		return "", true
	}

	item = q.queue[0]
	q.queue = q.queue[1:]
	q.processing[item] = struct{}{}
	delete(q.dirty, item)

	return item, false
}

// Done marks an item as finished processing.
// If the item was re-added while being processed, it is immediately re-enqueued for subsequent processing.
func (q *Queue) Done(item string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.processing, item)
	if _, isDirty := q.dirty[item]; isDirty {
		if !q.shuttingDown {
			q.queue = append(q.queue, item)
			q.cond.Signal()
		}
	}
}

// ShutDown signals the queue to stop accepting new work, unblocks all workers,
// terminates the delay loop, and cleans up resources.
func (q *Queue) ShutDown() {
	q.mu.Lock()
	if q.shuttingDown {
		q.mu.Unlock()
		return
	}
	q.shuttingDown = true
	close(q.stopCh)
	q.cond.Broadcast()
	q.mu.Unlock()

	// Wait for waitingLoop to terminate
	q.wg.Wait()
}

// IsShuttingDown checks if the workqueue is closed.
func (q *Queue) IsShuttingDown() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.shuttingDown
}

// Len returns current number of items waiting in queue.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}


