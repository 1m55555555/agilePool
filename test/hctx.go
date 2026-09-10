package main

// hctx stress-tests context handling across the hook boundary:
//
//  1. live markers: SubmitCtx payloads must reach all four events unchanged
//     (the pool unwraps contextTask for hooks on the submit and worker
//     paths; a lost/unwrapped ctx would drop the marker).
//  2. already-canceled ctx: SubmitCtx rejects it before any hook fires —
//     the counters must not move at all.
//  3. cancel while queued: tasks still get Started/Completed exactly once
//     each (the worker always starts and finishes what it dequeues, even
//     when the body is skipped via the canceled context), the pool drains,
//     and no submitter or worker goroutine is stranded.

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

type hctxPlugin struct{}

func (hctxPlugin) Name() string { return "hctx" }
func (hctxPlugin) Desc() string { return "hook stability: context payload and cancellation semantics" }

func (hctxPlugin) Options() []Option {
	return []Option{
		{Name: "num", Default: "3000", Help: "tasks in the live-marker wave"},
		{Name: "queued", Default: "150", Help: "tasks in the cancel-while-queued wave"},
	}
}

// ctxMarkerKey carries the wave marker through SubmitCtx.
type ctxMarkerKey struct{}

func (hctxPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)
	num, err := GetInt(opts, "num", 3000)
	if err != nil {
		return err
	}
	queued, err := GetInt(opts, "queued", 150)
	if err != nil {
		return err
	}
	if num < 1 || queued < 1 {
		return fmt.Errorf("num/queued must be >= 1")
	}

	baseline := goroutineBaseline()
	var badCtx atomic.Int64
	var executed atomic.Int64
	var cnt counters

	install := func(p *agilepool.Pool) error {
		h := hook.NewHooks()
		h.AddTaskSubmitted(func(ctx context.Context) {
			if _, ok := ctx.Value(ctxMarkerKey{}).(int); !ok {
				badCtx.Add(1)
			}
		})
		h.AddTaskEnqueued(func(ctx context.Context) {
			if _, ok := ctx.Value(ctxMarkerKey{}).(int); !ok {
				badCtx.Add(1)
			}
		})
		h.AddTaskStarted(func(ctx context.Context) {
			if _, ok := ctx.Value(ctxMarkerKey{}).(int); !ok {
				badCtx.Add(1)
			}
		})
		h.AddTaskCompleted(func(ctx context.Context, _ any) {
			if _, ok := ctx.Value(ctxMarkerKey{}).(int); !ok {
				badCtx.Add(1)
			}
		})
		cnt.addTo(h, 1)
		return p.SetHook(h)
	}

	// Wave 1: live marker propagation with all events firing.
	p1 := newScenarioPool(500, int64(num))
	if err := install(p1); err != nil {
		return err
	}
	for i := 0; i < num; i++ {
		ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxMarkerKey{}, i))
		task := agilepool.TaskFunc(func() error {
			executed.Add(1)
			time.Sleep(2 * time.Millisecond)
			cancel() // exercised end-to-end, harmless for hooks
			return nil
		})
		p1.SubmitCtx(ctx, task)
	}
	if !drain(p1, 30*time.Second) {
		return fmt.Errorf("[live markers] pool did not drain within 30s")
	}
	s1, e1, st1, c1 := cnt.snapshot()
	if err := report("hctx", rt,
		s1 == int64(num) && e1 == int64(num) && st1 == int64(num) && c1 == int64(num),
		"[live markers] totals submitted=%d enqueued=%d started=%d completed=%d for num=%d",
		s1, e1, st1, c1, num); err != nil {
		return err
	}
	if err := report("hctx", rt, executed.Load() == int64(num),
		"[live markers] %d tasks executed", executed.Load()); err != nil {
		return err
	}
	p1.SetHook(nil)
	p1.Close()

	// Wave 2: already-canceled contexts must be rejected before any hook.
	p2 := newScenarioPool(8, 64)
	if err := install(p2); err != nil {
		return err
	}
	canceled, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	for i := 0; i < 100; i++ {
		p2.SubmitCtx(canceled, agilepool.TaskFunc(func() error { return nil }))
	}
	s2, e2, st2, c2 := cnt.snapshot()
	if err := report("hctx", rt,
		s2 == s1 && e2 == e1 && st2 == st1 && c2 == c1,
		"[pre-canceled] rejected 100 submits, counters unchanged (S=%d E=%d St=%d C=%d)",
		s2, e2, st2, c2); err != nil {
		return err
	}
	p2.SetHook(nil)
	p2.Close()

	// Wave 3: cancel while tasks are queued. Two slow workers and a small
	// queue guarantee a backlog; cancel lands mid-drain.
	p3 := newScenarioPool(2, 16)
	if err := install(p3); err != nil {
		return err
	}
	live, stop := context.WithCancel(context.Background())
	go func() {
		time.Sleep(15 * time.Millisecond)
		stop()
	}()
	executed.Store(0)
	for i := 0; i < queued; i++ {
		ctx := context.WithValue(live, ctxMarkerKey{}, i)
		p3.SubmitCtx(ctx, agilepool.TaskFunc(func() error {
			executed.Add(1)
			time.Sleep(15 * time.Millisecond)
			return nil
		}))
	}
	if !drain(p3, 30*time.Second) {
		return fmt.Errorf("[cancel-queued] pool did not drain: a canceled context stranded a submitter or worker")
	}
	s3, e3, st3, c3 := cnt.snapshot()
	ds, de, dst, dc := s3-s1, e3-e1, st3-st1, c3-c1
	execDone := executed.Load()
	if err := report("hctx", rt, dst == dc && dst == int64(queued),
		"[cancel-queued] started=%d completed=%d of %d queued: every dequeued task completes exactly once",
		dst, dc, queued); err != nil {
		return err
	}
	if err := report("hctx", rt, execDone <= int64(queued) && de <= ds && dst <= ds,
		"[cancel-queued] %d/%d bodies ran before cancellation, hooks stayed symmetric", execDone, queued); err != nil {
		return err
	}
	p3.SetHook(nil)
	p3.Close()

	if err := report("hctx", rt, badCtx.Load() == 0,
		"ctx payload lost across %d hook invocations", badCtx.Load()); err != nil {
		return err
	}
	if !settleGoroutines(baseline, 4, 5*time.Second) {
		return report("hctx", rt, false,
			"goroutine leak: %d goroutines after scenario, baseline %d", goroutineBaseline(), baseline)
	}
	return report("hctx", rt, true, "context semantics held across all three waves")
}
