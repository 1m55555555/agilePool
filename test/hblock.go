package main

// hblock stress-tests the hook system under slow callbacks: every lifecycle
// callback sleeps, so at any instant a large share of workers and the
// submitting goroutine are inside hook dispatch. The pool must not deadlock
// (dispatch holds no pool locks) and every task must still see exactly one
// of each event. The reported per-task hook time is informational — the
// invariant is completion, not speed.

import (
	"context"
	"fmt"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

type hblockPlugin struct{}

func (hblockPlugin) Name() string { return "hblock" }
func (hblockPlugin) Desc() string {
	return "hook stability: slow/blocking hooks must not deadlock the pool"
}

func (hblockPlugin) Options() []Option {
	return []Option{
		{Name: "num", Default: "5000", Help: "tasks to submit"},
		{Name: "delay-us", Default: "200", Help: "sleep per hook invocation (microseconds)"},
		{Name: "workers", Default: "200", Help: "worker capacity of the scenario pool"},
	}
}

func (hblockPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)
	num, err := GetInt(opts, "num", 5000)
	if err != nil {
		return err
	}
	delayUs, err := GetInt(opts, "delay-us", 200)
	if err != nil {
		return err
	}
	workers, err := GetInt(opts, "workers", 200)
	if err != nil {
		return err
	}
	if num < 1 || workers < 1 {
		return fmt.Errorf("num/workers must be >= 1")
	}
	if delayUs < 0 {
		return fmt.Errorf("option %q: must be >= 0", "delay-us")
	}

	baseline := goroutineBaseline()
	p := newScenarioPool(int64(workers), int64(num))
	defer p.Close()

	h := hook.NewHooks()
	var cnt counters
	slow := func() { time.Sleep(time.Duration(delayUs) * time.Microsecond) }
	h.AddTaskSubmitted(func(context.Context) { slow(); cnt.Submitted.Add(1) })
	h.AddTaskEnqueued(func(context.Context) { slow(); cnt.Enqueued.Add(1) })
	h.AddTaskStarted(func(context.Context) { slow(); cnt.Started.Add(1) })
	h.AddTaskCompleted(func(context.Context, any) { slow(); cnt.Completed.Add(1) })
	if err := p.SetHook(h); err != nil {
		return err
	}

	start := time.Now()
	for i := 0; i < num; i++ {
		p.Submit(agilepool.TaskFunc(func() error { return nil }))
	}
	if !drain(p, 60*time.Second) {
		return fmt.Errorf("pool deadlocked with %dus blocking hooks (did not drain in 60s)", delayUs)
	}
	elapsed := time.Since(start)

	s, e, st, c := cnt.snapshot()
	if err := report("hblock", rt,
		s == int64(num) && e == int64(num) && st == int64(num) && c == int64(num),
		"counters submitted=%d enqueued=%d started=%d completed=%d, want all == %d",
		s, e, st, c, num); err != nil {
		return err
	}
	perTask := elapsed.Seconds() * 1e6 / float64(num)
	if err := report("hblock", rt, true,
		"%d tasks drained in %s (%.1f us/task incl. 4x %dus hook sleeps)",
		num, elapsed.Round(time.Millisecond), perTask, delayUs); err != nil {
		return err
	}

	p.SetHook(nil)
	p.Close()
	if !settleGoroutines(baseline, 4, 5*time.Second) {
		return report("hblock", rt, false,
			"goroutine leak: %d goroutines after close, baseline %d", goroutineBaseline(), baseline)
	}
	return report("hblock", rt, true, "slow hooks did not deadlock or lose events")
}
