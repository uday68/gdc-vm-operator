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

