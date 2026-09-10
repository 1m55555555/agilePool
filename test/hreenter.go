package main

// hreenter stress-tests reentrant hook dispatch: a hook itself submits new
// tasks to the pool while the pool is inside a dispatch on the same
// goroutine. Submission from a Submitted callback recurses through
// DispatchTaskSubmitted on the submitter's stack; submission from a
// Completed callback recurses from worker goroutines. Both must be safe
// (dispatch holds no pool locks) and every reentered task must flow through
// the full event cycle once.
//
// A global budget bounds the total reentries and a depth limit bounds each
// recursion chain, so the scenario cannot fan out forever.

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

type hreenterPlugin struct{}

func (hreenterPlugin) Name() string { return "hreenter" }
func (hreenterPlugin) Desc() string {
	return "hook stability: hook-triggered resubmission (reentrant dispatch)"
}

func (hreenterPlugin) Options() []Option {
	return []Option{
		{Name: "num", Default: "2000", Help: "tasks to submit"},
		{Name: "budget", Default: "2000", Help: "maximum reentries across the whole run"},
		{Name: "depth", Default: "32", Help: "maximum recursion depth of one reentry chain"},
		{Name: "stage", Default: "submitted", Help: "hook that reenters: submitted/completed"},
	}
}

func (hreenterPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)
	num, err := GetInt(opts, "num", 2000)
	if err != nil {
		return err
	}
	budget, err := GetInt(opts, "budget", 2000)
	if err != nil {
		return err
	}
	depth, err := GetInt(opts, "depth", 32)
	if err != nil {
		return err
	}
	stage := GetString(opts, "stage", "submitted")
	switch stage {
	case "submitted", "completed":
	default:
		return fmt.Errorf("option %q: unsupported value %q", "stage", stage)
	}
	if num < 1 || budget < 0 || depth < 1 {
		return fmt.Errorf("num must be >= 1, budget >= 0, depth >= 1")
	}

	baseline := goroutineBaseline()
	// Queue sized for the worst case: num originals plus every reentry.
	p := newScenarioPool(1000, int64(num+budget))
	defer p.Close()

	var remaining atomic.Int64
	remaining.Store(int64(budget))
	var curDepth atomic.Int64
	var reentered atomic.Int64
	var executed atomic.Int64

	task := func() agilepool.Task {
		return agilepool.TaskFunc(func() error {
			executed.Add(1)
			return nil
		})
	}

	h := hook.NewHooks()
	var cnt counters
	cnt.addTo(h, 1)
	reenter := func() {
		// Depth is incremented for the duration of the nested Submit only,
		// so chains recurse to at most `depth` while sibling chains on
		// other goroutines are unaffected.
		if curDepth.Load() >= int64(depth) {
			return
		}
		if remaining.Add(-1) < 0 {
			remaining.Add(1)
			return
		}
		reentered.Add(1)
		curDepth.Add(1)
		defer curDepth.Add(-1)
		p.Submit(task())
	}
	if stage == "submitted" {
		h.AddTaskSubmitted(func(context.Context) { reenter() })
	} else {
		h.AddTaskCompleted(func(context.Context, any) { reenter() })
	}
	if err := p.SetHook(h); err != nil {
		return err
	}

	for i := 0; i < num; i++ {
		p.Submit(task())
	}
	if !drain(p, 60*time.Second) {
		return fmt.Errorf("pool did not drain: reentrant dispatch deadlocked (num=%d budget=%d)", num, budget)
	}

	total := int64(num) + reentered.Load()
	s, e, st, c := cnt.snapshot()
	if err := report("hreenter", rt,
		s == total && e == total && st == total && c == total,
		"counters submitted=%d enqueued=%d started=%d completed=%d for %d originals + %d reentries",
		s, e, st, c, num, reentered.Load()); err != nil {
		return err
	}
	if err := report("hreenter", rt, executed.Load() == total,
		"executed %d == %d originals + %d reentries", executed.Load(), num, reentered.Load()); err != nil {
		return err
	}
	if err := report("hreenter", rt, reentered.Load() > 0 && reentered.Load() <= int64(budget),
		"reentered %d tasks within budget %d (depth limit %d)", reentered.Load(), budget, depth); err != nil {
		return err
	}

	p.SetHook(nil)
	p.Close()
	if !settleGoroutines(baseline, 4, 5*time.Second) {
		return report("hreenter", rt, false,
			"goroutine leak: %d goroutines after close, baseline %d", goroutineBaseline(), baseline)
	}
	return report("hreenter", rt, true, "reentrant dispatch from %s hooks stayed stable", stage)
}
