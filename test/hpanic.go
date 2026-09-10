package main

// hpanic stress-tests panic immunity of the hook system. Two levels:
//
//   - level=callback: every registered callback of the stage under test
//     panics; hook.Hooks must recover each one (invoke) while the
//     counting callbacks registered before AND after them keep receiving
//     events, and the pool keeps executing tasks. The panicking callback is
//     deliberately sandwiched: a recover path that panics itself (e.g. a nil
//     logger) aborts the rest of the callback list and starves the "after"
//     counters, which must fail this scenario.
//   - level=dispatch: the whole hooks implementation panics at the dispatch
//     entry of the stage under test; pool.dispatchHook must recover it and
//     keep the submission/worker/Close paths and their bookkeeping intact.
//
// stage=all walks the five lifecycle points one by one (each on a fresh
// pool), and after each burst the pool must still execute a plain follow-up
// task — a worker goroutine killed by a panic would fail that burst.

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

const panicMsg = "hook stability panic"

type hpanicPlugin struct{}

func (hpanicPlugin) Name() string { return "hpanic" }
func (hpanicPlugin) Desc() string { return "hook stability: panicking hooks must not kill pool paths" }

func (hpanicPlugin) Options() []Option {
	return []Option{
		{Name: "num", Default: "3000", Help: "tasks per stage burst"},
		{Name: "level", Default: "dispatch", Help: "panic level: dispatch (whole hooks impl) / callback (per callback)"},
		{Name: "stage", Default: "all", Help: "stage under panic: all/submitted/enqueued/started/completed/closed"},
	}
}

func (hpanicPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)
	num, err := GetInt(opts, "num", 3000)
	if err != nil {
		return err
	}
	level := GetString(opts, "level", "dispatch")
	switch level {
	case "dispatch", "callback":
	default:
		return fmt.Errorf("option %q: unsupported value %q", "level", level)
	}
	stage := GetString(opts, "stage", "all")
	if num < 1 {
		return fmt.Errorf("option %q: must be >= 1", "num")
	}

	var stages []string
	switch stage {
	case "all":
		stages = []string{"submitted", "enqueued", "started", "completed", "closed"}
	case "submitted", "enqueued", "started", "completed", "closed":
		stages = []string{stage}
	default:
		return fmt.Errorf("option %q: unsupported value %q", "stage", stage)
	}

	baseline := goroutineBaseline()
	// Run every stage even after a failure, so the output lists all broken
	// lifecycle points instead of only the first one.
	var failures []string
	for _, s := range stages {
		if err := runPanicStage(rt, s, level, num); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", s, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d/%d stage(s) failed: %s", len(failures), len(stages), strings.Join(failures, " | "))
	}
	if !settleGoroutines(baseline, 4, 5*time.Second) {
		return report("hpanic", rt, false,
			"goroutine leak: %d goroutines after scenario, baseline %d", goroutineBaseline(), baseline)
	}
	return report("hpanic", rt, true, "survived panics at %s level across %d stage(s)", level, len(stages))
}

// runPanicStage runs one stage burst on a private pool and closes it. The
// pool returns closed after the "closed" stage.
func runPanicStage(rt *Runtime, stage, level string, num int) error {
	p := newScenarioPool(500, int64(num))
	closedPool := stage == "closed"
	if !closedPool {
		defer p.Close()
	}

	var executed atomic.Int64
	panicFn := func() { panic(panicMsg) }

	var want int64 = int64(num)
	if level == "callback" {
		h := hook.NewHooks()
		// The panicking callback sits BETWEEN two counter sets: recovery is
		// per callback, so both siblings must still see every event. This is
		// the assertion that catches a recovery path which panics itself
		// (e.g. the zero-value Hooks logger): the remainder of the callback
		// list is aborted and the "after" set goes to zero.
		var before, after counters
		before.addTo(h, 1)
		switch stage {
		case "submitted":
			h.AddTaskSubmitted(func(context.Context) { panicFn() })
		case "enqueued":
			h.AddTaskEnqueued(func(context.Context) { panicFn() })
		case "started":
			h.AddTaskStarted(func(context.Context) { panicFn() })
		case "completed":
			h.AddTaskCompleted(func(context.Context, any) { panicFn() })
		case "closed":
			h.AddPoolClosed(func(*agilepool.Pool) { panicFn() })
		}
		after.addTo(h, 1)
		if err := p.SetHook(h); err != nil {
			return err
		}
		submitBurst(p, &executed, num)
		if !drain(p, 30*time.Second) {
			return fmt.Errorf("[%s] callback panic blocked drain", stage)
		}
		if stage == "closed" {
			p.Close()
		}
		bs, be, bst, bc := before.snapshot()
		as, ae, ast, ac := after.snapshot()
		if err := report("hpanic", rt,
			bs == want && be == want && bst == want && bc == want &&
				as == want && ae == want && ast == want && ac == want,
			"[%s/callback] sibling counter sets saw every event: before(S=%d E=%d St=%d C=%d) after(S=%d E=%d St=%d C=%d), want all == %d",
			stage, bs, be, bst, bc, as, ae, ast, ac, want); err != nil {
			return err
		}
		if err := report("hpanic", rt, executed.Load() == int64(num),
			"[%s/callback] %d tasks executed despite panicking %s hook", stage, executed.Load(), stage); err != nil {
			return err
		}
		if stage == "closed" {
			return report("hpanic", rt, true,
				"[%s/callback] Close returned and panicking OnPoolClosed was contained", stage)
		}
		return nil
	}

	// level=dispatch: hostile hooks implementation panicking at dispatch.
	ph := &panicHooks{
		h:          hook.NewHooks(),
		panicWhen:  func(d string) bool { return d == stage },
		panicValue: panicMsg,
	}
	if err := p.SetHook(ph); err != nil {
		return err
	}
	submitBurst(p, &executed, num)
	if !drain(p, 30*time.Second) {
		return fmt.Errorf("[%s] dispatch panic blocked drain", stage)
	}
	if stage == "closed" {
		p.Close()
	}
	if err := report("hpanic", rt, executed.Load() == int64(num),
		"[%s/dispatch] %d tasks executed with %s dispatch panicking", stage, executed.Load(), stage); err != nil {
		return err
	}
	if stage == "closed" {
		// The pool is gone: follow-up submissions must be dropped silently
		// (no crash, no hook dispatch), not executed.
		for i := 0; i < num; i++ {
			p.Submit(agilepool.TaskFunc(func() error {
				executed.Add(1)
				return nil
			}))
		}
		if !drain(p, 30*time.Second) {
			return fmt.Errorf("[%s] post-close submissions blocked drain", stage)
		}
		if err := report("hpanic", rt, executed.Load() == int64(num),
			"[%s/dispatch] %d post-close submissions dropped without events", stage, num); err != nil {
			return err
		}
		return report("hpanic", rt, true,
			"[%s/dispatch] Close survived a panicking PoolClosed dispatch", stage)
	}
	// The worker goroutines must have survived: a plain task without hooks
	// still runs after the hostile hooks are detached.
	p.SetHook(nil)
	submitBurst(p, &executed, num)
	if !drain(p, 30*time.Second) {
		return fmt.Errorf("[%s] post-panic follow-up burst blocked drain", stage)
	}
	if err := report("hpanic", rt, executed.Load() == 2*int64(num),
		"[%s/dispatch] follow-up burst of %d also executed after hooks detached", stage, num); err != nil {
		return err
	}
	return nil
}

func submitBurst(p *agilepool.Pool, executed *atomic.Int64, n int) {
	for i := 0; i < n; i++ {
		p.Submit(agilepool.TaskFunc(func() error {
			executed.Add(1)
			return nil
		}))
	}
}
