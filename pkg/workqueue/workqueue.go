package workqueue

import (
	"math"
	"sync"
	"time"
)

// RateLimiter calculates retry delay based on failure frequency.
type RateLimiter interface {
	When(item string) time.Duration
	Forget(item string)
	NumRequeues(item string) int
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
		baseDelay = 500 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = 16 * time.Second
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

// Queue provides a thread-safe, deduplicating workqueue for controller events.
type Queue struct {
	queue       []string
	dirty       map[string]struct{} // items scheduled or waiting
	processing  map[string]struct{} // items currently being reconciled
	cond        *sync.Cond
	shuttingDown bool
	rateLimiter RateLimiter
	mu          sync.Mutex
}

// NewQueue constructs a Kubernetes-style deduplicating workqueue with rate limiting.
func NewQueue(limiter RateLimiter) *Queue {
	if limiter == nil {
		limiter = NewExponentialFailureRateLimiter(500*time.Millisecond, 16*time.Second)
	}
	q := &Queue{
		dirty:       make(map[string]struct{}),
		processing:  make(map[string]struct{}),
		rateLimiter: limiter,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
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

// AddRateLimited schedules an item after an exponential backoff delay.
func (q *Queue) AddRateLimited(item string) time.Duration {
	delay := q.rateLimiter.When(item)
	go func() {
		time.Sleep(delay)
		q.Add(item)
	}()
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
func (q *Queue) Done(item string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.processing, item)
	if _, isDirty := q.dirty[item]; isDirty {
		q.queue = append(q.queue, item)
		q.cond.Signal()
	}
}

// ShutDown signals the queue to stop accepting new work.
func (q *Queue) ShutDown() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.shuttingDown = true
	q.cond.Broadcast()
}

// Len returns current number of items waiting in queue.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}

