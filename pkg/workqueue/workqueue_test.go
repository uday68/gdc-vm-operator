package workqueue

import (
	"sync"
	"testing"
	"time"
)

func TestWorkQueue_DeduplicationAndOrder(t *testing.T) {
	q := NewQueue(nil)
	defer q.ShutDown()

	// Add items with duplicates
	q.Add("vm-alpha")
	q.Add("vm-beta")
	q.Add("vm-alpha") // duplicate, must be deduplicated
	q.Add("vm-gamma")

	if q.Len() != 3 {
		t.Fatalf("expected queue length 3 after deduplication, got %d", q.Len())
	}

	// Read first item
	item1, shutdown := q.Get()
	if shutdown || item1 != "vm-alpha" {
		t.Fatalf("expected 'vm-alpha', got '%s'", item1)
	}
	q.Done(item1)

	// Read second item
	item2, shutdown := q.Get()
	if shutdown || item2 != "vm-beta" {
		t.Fatalf("expected 'vm-beta', got '%s'", item2)
	}
	q.Done(item2)

	// Read third item
	item3, shutdown := q.Get()
	if shutdown || item3 != "vm-gamma" {
		t.Fatalf("expected 'vm-gamma', got '%s'", item3)
	}
	q.Done(item3)
}

func TestExponentialRateLimiter(t *testing.T) {
	limiter := NewExponentialFailureRateLimiter(100*time.Millisecond, 1*time.Second)

	// Attempt 1: 100ms
	d1 := limiter.When("crash-vm")
	if d1 != 100*time.Millisecond {
		t.Errorf("expected 100ms on first failure, got %v", d1)
	}

	// Attempt 2: 200ms
	d2 := limiter.When("crash-vm")
	if d2 != 200*time.Millisecond {
		t.Errorf("expected 200ms on second failure, got %v", d2)
	}

	// Attempt 3: 400ms
	d3 := limiter.When("crash-vm")
	if d3 != 400*time.Millisecond {
		t.Errorf("expected 400ms on third failure, got %v", d3)
	}

	// Attempt 4: 800ms
	d4 := limiter.When("crash-vm")
	if d4 != 800*time.Millisecond {
		t.Errorf("expected 800ms on fourth failure, got %v", d4)
	}

	// Attempt 5: Caps at maxDelay (1s)
	d5 := limiter.When("crash-vm")
	if d5 != 1*time.Second {
		t.Errorf("expected cap at 1s, got %v", d5)
	}

	// Forget resets counter
	limiter.Forget("crash-vm")
	dReset := limiter.When("crash-vm")
	if dReset != 100*time.Millisecond {
		t.Errorf("expected reset to 100ms, got %v", dReset)
	}
}

func TestWorkQueue_ConcurrentWorkers(t *testing.T) {
	q := NewQueue(nil)
	var wg sync.WaitGroup
	processedCount := 0
	var mu sync.Mutex

	// 3 worker goroutines
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item, shutdown := q.Get()
				if shutdown {
					return
				}
				mu.Lock()
				processedCount++
				mu.Unlock()
				q.Done(item)
			}
		}()
	}

	// Feed 20 unique items
	for i := 0; i < 20; i++ {
		q.Add(string(rune('A' + i)))
	}

	// Allow workers to consume
	time.Sleep(100 * time.Millisecond)
	q.ShutDown()
	wg.Wait()

	if processedCount != 20 {
		t.Errorf("expected 20 processed items, got %d", processedCount)
	}
}

func TestWorkQueue_AddDuringProcessing(t *testing.T) {
	q := NewQueue(nil)
	defer q.ShutDown()

	q.Add("worker-key")

	// 1. Worker gets item
	item, shutdown := q.Get()
	if shutdown || item != "worker-key" {
		t.Fatalf("expected 'worker-key', got '%s'", item)
	}

	// 2. While item is being processed, add it multiple times
	q.Add("worker-key")
	q.Add("worker-key")
	q.Add("worker-key")

	// Verify it is NOT yet in the queue (since worker is still processing it)
	if q.Len() != 0 {
		t.Fatalf("item should not be re-queued while still being processed, got len=%d", q.Len())
	}

	// 3. Worker finishes processing
	q.Done(item)

	// Item must now be re-enqueued exactly once
	if q.Len() != 1 {
		t.Fatalf("expected item to be re-enqueued exactly once after Done, got len=%d", q.Len())
	}

	item2, shutdown2 := q.Get()
	if shutdown2 || item2 != "worker-key" {
		t.Fatalf("expected 'worker-key' on second pass, got '%s'", item2)
	}
	q.Done(item2)

	// Now queue must be empty
	if q.Len() != 0 {
		t.Fatalf("queue should be empty after second Done, got len=%d", q.Len())
	}
}

func TestWorkQueue_ConcurrentWorkersKeyExclusion(t *testing.T) {
	q := NewQueue(nil)
	defer q.ShutDown()

	var mu sync.Mutex
	activeKeys := make(map[string]int)
	overlapDetected := false

	const numWorkers = 8
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				key, shutdown := q.Get()
				if shutdown {
					return
				}

				mu.Lock()
				activeKeys[key]++
				if activeKeys[key] > 1 {
					overlapDetected = true
				}
				mu.Unlock()

				// Simulate processing duration
				time.Sleep(5 * time.Millisecond)

				mu.Lock()
				activeKeys[key]--
				mu.Unlock()

				q.Done(key)
			}
		}()
	}

	// Rapidly spam duplicate keys across workers
	keys := []string{"vm-1", "vm-2", "vm-3", "vm-4"}
	for round := 0; round < 50; round++ {
		for _, k := range keys {
			q.Add(k)
		}
		time.Sleep(2 * time.Millisecond)
	}

	time.Sleep(100 * time.Millisecond)
	q.ShutDown()
	wg.Wait()

	if overlapDetected {
		t.Fatal("Concurrency violation: multiple workers processed the same key simultaneously!")
	}
}

func TestWorkQueue_DelayedRetriesAndCleanShutdown(t *testing.T) {
	q := NewQueue(nil)

	// 1. AddAfter with short delay
	start := time.Now()
	q.AddAfter("delayed-key", 50*time.Millisecond)

	item, shutdown := q.Get()
	if shutdown || item != "delayed-key" {
		t.Fatalf("expected 'delayed-key', got '%s'", item)
	}
	elapsed := time.Since(start)
	if elapsed < 40*time.Millisecond {
		t.Fatalf("item returned too early: %v", elapsed)
	}
	q.Done(item)

	// 2. Add long delayed item, shutdown immediately
	q.AddAfter("long-delay", 10*time.Second)
	shutdownStart := time.Now()
	q.ShutDown()
	shutdownElapsed := time.Since(shutdownStart)

	if shutdownElapsed > 1*time.Second {
		t.Fatalf("ShutDown blocked on delayed item: took %v", shutdownElapsed)
	}

	// Workers after shutdown must receive shutdown signal
	_, isShutdown := q.Get()
	if !isShutdown {
		t.Fatal("expected Get() to return shutdown=true after ShutDown()")
	}
}

func TestWorkQueue_ShutdownWithBlockedWorkers(t *testing.T) {
	q := NewQueue(nil)
	const numWorkers = 5
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			item, shutdown := q.Get()
			if !shutdown {
				t.Errorf("expected shutdown=true for blocked worker, got item='%s'", item)
			}
		}()
	}

	// Ensure workers are blocked in q.Get()
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	q.ShutDown()
	wg.Wait()

	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("ShutDown took too long to unblock workers: %v", time.Since(start))
	}
}

func BenchmarkQueue_Throughput(b *testing.B) {
	q := NewQueue(nil)
	defer q.ShutDown()

	go func() {
		for {
			item, shutdown := q.Get()
			if shutdown {
				return
			}
			q.Done(item)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Add("bench-item")
	}
}

func BenchmarkQueue_DuplicatePressure(b *testing.B) {
	q := NewQueue(nil)
	defer q.ShutDown()

	const numConsumers = 4
	for i := 0; i < numConsumers; i++ {
		go func() {
			for {
				item, shutdown := q.Get()
				if shutdown {
					return
				}
				q.Done(item)
			}
		}()
	}

	keys := []string{"key-1", "key-2", "key-3", "key-4", "key-5"}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			q.Add(keys[idx%len(keys)])
			idx++
		}
	})
}


