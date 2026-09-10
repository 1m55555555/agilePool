package main

// hchurn stress-tests the supported registration window of the hook
// system: hooks are registered up front (here from many goroutines at
// once, exercising Add under contention) and must be frozen before the
// first task starts — dispatching already-running tasks with a changing
// callback list is outside the contract and is never done here.
//
// Two accounting checks:
//
//  1. the pre-registered counting hook sees every event of wave one
//     exactly num times;
//  2. every hook added during the registration burst also sees every
//     event (their shared counters must land on num * churners * adds),
//     proving the burst registered everyone with no loss or duplication;
//  3. after wave one a fresh Hooks instance on the same pool accounts
//     exactly, i.e. swapping the hook set between runs stays intact.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

type hchurnPlugin struct{}

func (hchurnPlugin) Name() string { return "hchurn" }
func (hchurnPlugin) Desc() string {
	return "hook stability: concurrent registration burst, then exact accounting"
}

func (hchurnPlugin) Options() []Option {
	return []Option{
		{Name: "num", Default: "10000", Help: "tasks in the first wave"},
		{Name: "num2", Default: "5000", Help: "tasks in the verification wave"},
		{Name: "churners", Default: "4", Help: "goroutines registering hooks before dispatch"},
		{Name: "adds", Default: "25", Help: "hooks each churner registers"},
		{Name: "workers", Default: "500", Help: "worker capacity of the scenario pool"},
	}
}

func (hchurnPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)
	num, err := GetInt(opts, "num", 10000)
	if err != nil {
		return err
	}
	num2, err := GetInt(opts, "num2", 5000)
	if err != nil {
		return err
	}
	churners, err := GetInt(opts, "churners", 4)
	if err != nil {
		return err
	}
	adds, err := GetInt(opts, "adds", 25)
	if err != nil {
		return err
	}
	workers, err := GetInt(opts, "workers", 500)
	if err != nil {
		return err
	}
	if num < 1 || num2 < 1 || workers < 1 {
		return fmt.Errorf("num/num2/workers must be >= 1")
	}
	if churners < 0 || adds < 0 {
		return fmt.Errorf("churners/adds must be >= 0")
	}

	baseline := goroutineBaseline()
	p := newScenarioPool(int64(workers), int64(num)+int64(num2))
	defer p.Close()

	var executed atomic.Int64
	task := agilepool.TaskFunc(func() error { executed.Add(1); return nil })

	// Startup phase: a counting hook is registered first, then churners
	// register `adds` hooks each from their own goroutines. All of this
	// happens before the pool starts running tasks (SetHook and the first
	// Submit come after the burst), which is the contract's registration
	// window. Every churn-added hook increments the shared `burst`
	// counters, so after wave one they must show num * churners * adds.
	churn := hook.NewHooks()
	var first counters // the pre-registered hook: must see num per event
	var burst counters // the burst hooks: must see num * churners * adds per event
	first.addTo(churn, 1)

	var churnWG sync.WaitGroup
	for g := 0; g < churners; g++ {
		churnWG.Add(1)
		go func() {
			defer churnWG.Done()
			for i := 0; i < adds; i++ {
				// One hook per event type per iteration, so every event
				// list deterministically receives churners*adds burst hooks.
				churn.AddTaskSubmitted(func(context.Context) { burst.Submitted.Add(1) })
				churn.AddTaskEnqueued(func(context.Context) { burst.Enqueued.Add(1) })
				churn.AddTaskStarted(func(context.Context) { burst.Started.Add(1) })
				churn.AddTaskCompleted(func(context.Context, any) { burst.Completed.Add(1) })
			}
		}()
	}
	churnWG.Wait() // registration window closed before any dispatch

	if err := p.SetHook(churn); err != nil {
		return err
	}
	for i := 0; i < num; i++ {
		p.Submit(task)
	}
	if !drain(p, 30*time.Second) {
		return fmt.Errorf("wave one did not drain within 30s")
	}

	perBurst := int64(num * churners * adds)
	s, e, st, c := first.snapshot()
	if err := report("hchurn", rt,
		s == int64(num) && e == int64(num) && st == int64(num) && c == int64(num),
		"pre-registered hook saw submitted=%d enqueued=%d started=%d completed=%d, want all == %d",
		s, e, st, c, num); err != nil {
		return err
	}
	bs, be, bst, bc := burst.snapshot()
	if err := report("hchurn", rt,
		bs == perBurst && be == perBurst && bst == perBurst && bc == perBurst,
		"%d burst hooks each saw every event: submitted=%d enqueued=%d started=%d completed=%d, want all == %d",
		churners*adds, bs, be, bst, bc, perBurst); err != nil {
		return err
	}

	// Verification wave: fresh hooks on the same pool must account exactly,
	// proving the swap between runs did not corrupt dispatch state.
	verify := hook.NewHooks()
	var cnt2 counters
	cnt2.addTo(verify, 1)
	if err := p.SetHook(verify); err != nil {
		return err
	}
	for i := 0; i < num2; i++ {
		p.Submit(task)
	}
	if !drain(p, 30*time.Second) {
		return fmt.Errorf("verification wave did not drain within 30s")
	}

	s2, e2, st2, c2 := cnt2.snapshot()
	if err := report("hchurn", rt,
		s2 == int64(num2) && e2 == int64(num2) && st2 == int64(num2) && c2 == int64(num2),
		"post-burst hook set: submitted=%d enqueued=%d started=%d completed=%d, want all == %d",
		s2, e2, st2, c2, num2); err != nil {
		return err
	}
	if err := report("hchurn", rt, executed.Load() == int64(num+num2),
		"tasks executed %d == %d across both waves", executed.Load(), num+num2); err != nil {
		return err
	}

	p.SetHook(nil)
	p.Close()
	if !settleGoroutines(baseline, 4, 5*time.Second) {
		return report("hchurn", rt, false,
			"goroutine leak: %d goroutines after close, baseline %d", goroutineBaseline(), baseline)
	}
	return report("hchurn", rt, true,
		"survived a %d-goroutine registration burst of %d hooks, accounting exact", churners, churners*adds)
}
