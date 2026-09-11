package agilepool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// panicHooks is a hooks implementation that deliberately panics at selected
// lifecycle points. It mimics a custom hooks implementation that does not
// recover its own callbacks (the bundled hook.Hooks does).
type panicHooks struct {
	panicSubmitted  bool
	panicEnqueued   bool
	panicStarted    bool
	panicCompleted  bool
	panicPoolClosed bool
}

func (h *panicHooks) DispatchTaskSubmitted(context.Context) {
	if h.panicSubmitted {
		panic("submitted hook panic")
	}
}

func (h *panicHooks) DispatchTaskEnqueued(context.Context) {
	if h.panicEnqueued {
		panic("enqueued hook panic")
	}
}

func (h *panicHooks) DispatchTaskStarted(context.Context) {
	if h.panicStarted {
		panic("started hook panic")
	}
}

func (h *panicHooks) DispatchTaskCompleted(context.Context, any) {
	if h.panicCompleted {
		panic("completed hook panic")
	}
}

func (h *panicHooks) DispatchPoolClosed(*Pool) {
	if h.panicPoolClosed {
		panic("pool closed hook panic")
	}
}

// waitPoolDone reports whether p.Wait() returns within timeout. A pool whose
// wg was leaked by a panicking hook (or a dead worker goroutine) blocks here.
func waitPoolDone(p *Pool, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		p.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// TestHookCompletedPanicDoesNotSkipDone is the regression test for the
// runTask defer ordering bug: p.done() used to live in the same deferred
// closure after DispatchTaskCompleted, so a panicking Completed hook skipped
// wg.Done and left Wait() blocked forever.
func TestHookCompletedPanicDoesNotSkipDone(t *testing.T) {
	p := NewPool(NewConfig())
	defer p.Close()
	if err := p.SetHook(&panicHooks{panicCompleted: true}); err != nil {
		t.Fatal(err)
	}

	const n = 10
	var executed atomic.Int32
	for i := 0; i < n; i++ {
		p.Submit(TaskFunc(func() error {
			executed.Add(1)
			return nil
		}))
	}
	if !waitPoolDone(p, 5*time.Second) {
		t.Fatal("Wait() blocked: a panicking Completed hook skipped wg.Done")
	}
	if got := executed.Load(); got != n {
		t.Fatalf("executed = %d, want %d", got, n)
	}

	// The worker goroutine must survive the hook panic and keep serving.
	p.Submit(TaskFunc(func() error {
		executed.Add(1)
		return nil
	}))
	if !waitPoolDone(p, 5*time.Second) {
		t.Fatal("Wait() blocked after a Completed hook panic")
	}
	if got := executed.Load(); got != n+1 {
		t.Fatalf("executed = %d, want %d", got, n+1)
	}
}

// TestHookStartedPanicDoesNotPreventExecution guards the dispatch order in
// runTask: the Started hook used to be dispatched before any defer was
// registered, so its panic crashed the worker goroutine before the task ran.
func TestHookStartedPanicDoesNotPreventExecution(t *testing.T) {
	p := NewPool(NewConfig())
	defer p.Close()
	if err := p.SetHook(&panicHooks{panicStarted: true}); err != nil {
		t.Fatal(err)
	}

	var executed atomic.Int32
	p.Submit(TaskFunc(func() error {
		executed.Add(1)
		return nil
	}))
	if !waitPoolDone(p, 5*time.Second) {
		t.Fatal("Wait() blocked")
	}
	if got := executed.Load(); got != 1 {
		t.Fatalf("executed = %d, want 1", got)
	}
}

// TestHookSubmittedPanicDoesNotAbortSubmit guards the submission path: the
// Submitted hook fires after wg.Add(1), so a panic there used to leak the
// WaitGroup before the task was enqueued.
func TestHookSubmittedPanicDoesNotAbortSubmit(t *testing.T) {
	p := NewPool(NewConfig())
	defer p.Close()
	if err := p.SetHook(&panicHooks{panicSubmitted: true}); err != nil {
		t.Fatal(err)
	}

	var executed atomic.Int32
	p.Submit(TaskFunc(func() error {
		executed.Add(1)
		return nil
	}))
	if !waitPoolDone(p, 5*time.Second) {
		t.Fatal("Wait() blocked: a panicking Submitted hook leaked the wg")
	}
	if got := executed.Load(); got != 1 {
		t.Fatalf("executed = %d, want 1", got)
	}
}

// TestHookPoolClosedPanicDoesNotAbortClose guards Pool.Close: a panicking
// PoolClosed hook must not propagate to the Close caller.
func TestHookPoolClosedPanicDoesNotAbortClose(t *testing.T) {
	p := NewPool(NewConfig())
	if err := p.SetHook(&panicHooks{panicPoolClosed: true}); err != nil {
		t.Fatal(err)
	}
	p.Submit(TaskFunc(func() error { return nil }))
	if !waitPoolDone(p, 5*time.Second) {
		t.Fatal("Wait() blocked")
	}
	p.Close() // must return normally despite the panicking hook
}

// TestSetHookConcurrentWithTaskDispatch verifies that replacing or disabling
// a hook while Submit and worker dispatch paths are active is race-free. It
// intentionally exercises Submitted, Enqueued, Started, and Completed.
func TestSetHookConcurrentWithTaskDispatch(t *testing.T) {
	p := NewPool(NewConfig(WithWorkerNumCapacity(8)))
	defer p.Close()

	const (
		switches = 1_000
		tasks    = 1_000
	)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < switches; i++ {
			if err := p.SetHook(&panicHooks{}); err != nil {
				t.Errorf("SetHook() error = %v", err)
				return
			}
			if err := p.SetHook(nil); err != nil {
				t.Errorf("SetHook(nil) error = %v", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < tasks; i++ {
			p.Submit(TaskFunc(func() error { return nil }))
		}
	}()

	close(start)
	wg.Wait()
	if !waitPoolDone(p, 5*time.Second) {
		t.Fatal("Wait() blocked after concurrent SetHook and task dispatch")
	}
}
