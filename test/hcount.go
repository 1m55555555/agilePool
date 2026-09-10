package main

// hcount stress-tests hook dispatch at scale: many callbacks per lifecycle
// event across many tasks, asserting that no event is lost or duplicated
// under real worker concurrency. The scenario pool uses queue == task count
// so every accepted task takes the direct channel path (Enqueued fires
// exactly once per task) and the four counters must all land on num.
//
// This is the accounting baseline of the family: hpanic/hblock/hchurn keep
// the same four-counter contract wherever their scenario allows it.

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

type hcountPlugin struct{}

func (hcountPlugin) Name() string { return "hcount" }
func (hcountPlugin) Desc() string {
	return "hook stability: N callbacks x M tasks, exact per-event accounting"
}

func (hcountPlugin) Options() []Option {
	return []Option{
		{Name: "num", Default: "20000", Help: "tasks to submit"},
		{Name: "hooks", Default: "8", Help: "callbacks per lifecycle event"},
		{Name: "workers", Default: "1000", Help: "worker capacity of the scenario pool"},
	}
}

func (hcountPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)
	num, err := GetInt(opts, "num", 20000)
	if err != nil {
		return err
	}
	nHooks, err := GetInt(opts, "hooks", 8)
	if err != nil {
		return err
	}
	workers, err := GetInt(opts, "workers", 1000)
	if err != nil {
		return err
	}
	if num < 1 || nHooks < 1 || workers < 1 {
		return fmt.Errorf("num/hooks/workers must all be >= 1")
	}

	baseline := goroutineBaseline()
	p := newScenarioPool(int64(workers), int64(num))
	// Error paths leave the pool for defer; the success path closes it
	// explicitly so the goroutine-settling check runs after Close.
	defer p.Close()

	h := hook.NewHooks()
	var cnt counters
	cnt.addTo(h, nHooks)
	if err := p.SetHook(h); err != nil {
		return err
	}

	var executed atomic.Int64
	for i := 0; i < num; i++ {
		p.Submit(agilepool.TaskFunc(func() error {
			executed.Add(1)
			return nil
		}))
	}

	start := time.Now()
	if !drain(p, 30*time.Second) {
		return fmt.Errorf("pool did not drain within 30s: hook dispatch lost a wg.Done")
	}
	elapsed := time.Since(start)

	s, e, st, c := cnt.snapshot()
	want := int64(num) * int64(nHooks)
	if err := report("hcount", rt,
		s == want && e == want && st == want && c == want,
		"counters submitted=%d enqueued=%d started=%d completed=%d, want all == %d (%d hooks x %d tasks in %s)",
		s, e, st, c, want, nHooks, num, elapsed.Round(time.Millisecond)); err != nil {
		return err
	}
	if err := report("hcount", rt, executed.Load() == int64(num),
		"tasks executed %d == num %d", executed.Load(), num); err != nil {
		return err
	}

	p.SetHook(nil)
	p.Close()
	if !settleGoroutines(baseline, 4, 5*time.Second) {
		return report("hcount", rt, false,
			"goroutine leak: %d goroutines after close, baseline %d", goroutineBaseline(), baseline)
	}
	return report("hcount", rt, true, "pool closed cleanly, goroutines settled")
}
