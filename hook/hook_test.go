package hook

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentRegistrationAndDispatch verifies that a callback registration
// can overlap lifecycle dispatch without racing on the callback slices. Run
// this test with -race; the old append-and-range implementation reports a
// race here.
func TestConcurrentRegistrationAndDispatch(t *testing.T) {
	h := NewHooks()

	const iterations = 1_000
	start := make(chan struct{})
	var registered atomic.Int32
	var called atomic.Int32
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			h.AddTaskStarted(func(context.Context) {
				called.Add(1)
			})
			registered.Add(1)
		}
	}()

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			h.DispatchTaskStarted(context.Background())
		}
	}()

	close(start)
	wg.Wait()
	// The concurrent dispatch loop is intentionally only a race exercise: it
	// may finish before the first registration on a fast scheduler. Dispatch
	// once after both goroutines finish to deterministically verify that the
	// published callback snapshot is usable.
	h.DispatchTaskStarted(context.Background())

	if got := registered.Load(); got != iterations {
		t.Fatalf("registered callbacks = %d, want %d", got, iterations)
	}
	if got := called.Load(); got == 0 {
		t.Fatal("registered callbacks were never dispatched")
	}
}

// TestCallbackCanRegisterAnotherCallback confirms dispatch does not hold the
// registration mutex while invoking user code, preserving reentrant use.
func TestCallbackCanRegisterAnotherCallback(t *testing.T) {
	h := NewHooks()
	var calls atomic.Int32

	h.AddTaskStarted(func(context.Context) {
		calls.Add(1)
		h.AddTaskStarted(func(context.Context) { calls.Add(1) })
	})

	h.DispatchTaskStarted(context.Background())
	h.DispatchTaskStarted(context.Background())

	if got := calls.Load(); got != 3 {
		t.Fatalf("callback calls = %d, want 3", got)
	}
}
