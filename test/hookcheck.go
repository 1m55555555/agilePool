package main

// hookcheck.go holds the shared machinery for the hook-stability plugin
// family (hcount/hpanic/horder/hctx/hblock/hchurn/hreenter/hclose). The
// plugins themselves only define a scenario: build a private pool, install
// adversarial hooks, run tasks, then assert invariants. Everything common —
// scenario pool construction, counting callbacks, drain/leak waiting and
// PASS/FAIL reporting — lives here so each scenario file stays readable.
//
// Every scenario is self-contained on purpose: it creates its own pools
// with a known capacity/queue (instead of borrowing the session --pool) so
// invariants are deterministic (e.g. queue >= task count rules out the
// overflow-buffer path where Enqueued is intentionally skipped) and Close
// semantics can be exercised without disturbing other segments.

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

// discardLogger swallows pool log lines. Hook-panic scenarios log every
// recovered panic through the pool logger by design; pointing it at this
// keeps the PASS/FAIL output readable while the recovery paths are still
// exercised.
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}
func (discardLogger) Println(...any)        {}

// counters is one atomic counter per lifecycle event. Scenario callbacks
// only increment these (the Store-layering rule of the host applies here
// too: hot-path data stays in atomic state owned by the plugin).
type counters struct {
	Submitted atomic.Int64
	Enqueued  atomic.Int64
	Started   atomic.Int64
	Completed atomic.Int64
}

// addTo registers n callbacks per lifecycle event on h, each incrementing
// its own counter, so a scenario can install "many hooks" cheaply.
func (c *counters) addTo(h *hook.Hooks, n int) {
	for i := 0; i < n; i++ {
		h.AddTaskSubmitted(func(context.Context) { c.Submitted.Add(1) })
		h.AddTaskEnqueued(func(context.Context) { c.Enqueued.Add(1) })
		h.AddTaskStarted(func(context.Context) { c.Started.Add(1) })
		h.AddTaskCompleted(func(context.Context, any) { c.Completed.Add(1) })
	}
}

func (c *counters) snapshot() (submitted, enqueued, started, completed int64) {
	return c.Submitted.Load(), c.Enqueued.Load(), c.Started.Load(), c.Completed.Load()
}

// panicHooks is a hostile hooks implementation: every dispatch method
// panics before delegating, mimicking a custom hooks implementation that
// does not recover its own callbacks. The pool must absorb these panics in
// dispatchHook and keep the submission/worker/close paths intact.
type panicHooks struct {
	h          *hook.Hooks
	panicWhen  func(dispatch string) bool
	panicValue any
}

func (p *panicHooks) DispatchTaskSubmitted(ctx context.Context) {
	if p.panicWhen("submitted") {
		panic(p.panicValue)
	}
	if p.h != nil {
		p.h.DispatchTaskSubmitted(ctx)
	}
}

func (p *panicHooks) DispatchTaskEnqueued(ctx context.Context) {
	if p.panicWhen("enqueued") {
		panic(p.panicValue)
	}
	if p.h != nil {
		p.h.DispatchTaskEnqueued(ctx)
	}
}

func (p *panicHooks) DispatchTaskStarted(ctx context.Context) {
	if p.panicWhen("started") {
		panic(p.panicValue)
	}
	if p.h != nil {
		p.h.DispatchTaskStarted(ctx)
	}
}

func (p *panicHooks) DispatchTaskCompleted(ctx context.Context, rec any) {
	if p.panicWhen("completed") {
		panic(p.panicValue)
	}
	if p.h != nil {
		p.h.DispatchTaskCompleted(ctx, rec)
	}
}

func (p *panicHooks) DispatchPoolClosed(pool *agilepool.Pool) {
	if p.panicWhen("closed") {
		panic(p.panicValue)
	}
	if p.h != nil {
		p.h.DispatchPoolClosed(pool)
	}
}

// newScenarioPool builds a small, block-mode pool with a channel queue big
// enough to hold every task of the scenario, so accepted tasks always take
// the direct channel path (Enqueued fires exactly once per task) and the
// assertion surface is deterministic. Noise-free logger keeps adversarial
// scenarios quiet.
func newScenarioPool(capacity, queue int64) *agilepool.Pool {
	cfg := agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(capacity),
		agilepool.WithTaskQueueSize(queue),
		agilepool.WithBlockMode(agilepool.BLOCK),
		agilepool.WithIdleContainerType(agilepool.LinkedListType),
		agilepool.WithCleanPeriod(100*time.Millisecond),
	)
	p := agilepool.NewPool(cfg)
	p.SetLogger(discardLogger{})
	return p
}

// drain waits for the pool to finish every in-flight task, reporting
// whether Wait returned inside the timeout. A regression that leaks wg.Done
// (e.g. a hook panic skipping pool bookkeeping) blocks here instead of
// hanging the scenario forever.
func drain(p *agilepool.Pool, timeout time.Duration) bool {
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

// settleGoroutines polls until the goroutine count returns to near the
// session baseline (pool Close is asynchronous: cleaner/scaler/sampler
// notice closePoolCn on their next tick). grace is the tolerated slack for
// runtime goroutines not owned by the scenario.
func settleGoroutines(baseline int, grace int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline+grace {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// goroutineBaseline snapshots the current goroutine count. Scenarios call
// it right before creating their pools.
func goroutineBaseline() int {
	return runtime.NumGoroutine()
}

// report prints a PASS/FAIL line and converts a failure into the error the
// host reports with the [plugin] prefix. name is the plugin name so the
// PASS lines and the error prefix agree.
func report(name string, rt *Runtime, ok bool, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if ok {
		fmt.Fprintf(rt.Out, "[%s] PASS %s\n", name, msg)
		return nil
	}
	fmt.Fprintf(rt.Out, "[%s] FAIL %s\n", name, msg)
	return fmt.Errorf("%s", msg)
}
