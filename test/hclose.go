package main

// hclose stress-tests OnPoolClosed and the closed-pool behavior around it:
//
//  1. exactly-once: Close fires OnPoolClosed once; a second Close is a no-op.
//  2. racing Close: several goroutines closing simultaneously still fire the
//     hook exactly once (the closed flag CAS wins exactly one).
//  3. panicking OnPoolClosed: both a panicking callback and a hostile hooks
//     implementation panicking at DispatchPoolClosed must not abort Close.
//  4. post-close silence: submissions after Close are dropped and must not
//     generate any hook event; Wait still balances.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

type hclosePlugin struct{}

func (hclosePlugin) Name() string { return "hclose" }
func (hclosePlugin) Desc() string {
	return "hook stability: OnPoolClosed exactly-once and closed-pool silence"
}

func (hclosePlugin) Options() []Option {
	return []Option{
		{Name: "num", Default: "2000", Help: "tasks submitted before/around Close"},
		{Name: "closers", Default: "4", Help: "goroutines racing to Close in scenario 2"},
	}
}

func (hclosePlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)
	num, err := GetInt(opts, "num", 2000)
	if err != nil {
		return err
	}
	closers, err := GetInt(opts, "closers", 4)
	if err != nil {
		return err
	}
	if num < 1 || closers < 2 {
		return fmt.Errorf("num must be >= 1, closers >= 2")
	}

	baseline := goroutineBaseline()

	// Scenario 1: sequential double Close, exactly one hook.
	{
		p := newScenarioPool(500, int64(num))
		var closed atomic.Int64
		var closedPool atomic.Pointer[agilepool.Pool]
		h := hook.NewHooks()
		h.AddPoolClosed(func(pool *agilepool.Pool) {
			closed.Add(1)
			closedPool.Store(pool)
		})
		if err := p.SetHook(h); err != nil {
			return err
		}
		for i := 0; i < num; i++ {
			p.Submit(agilepool.TaskFunc(func() error { return nil }))
		}
		if !drain(p, 30*time.Second) {
			return fmt.Errorf("[exactly-once] pool did not drain")
		}
		p.Close()
		p.Close() // second Close must be a no-op for the hook
		if err := report("hclose", rt, closed.Load() == 1 && closedPool.Load() == p,
			"[exactly-once] OnPoolClosed fired %d time(s), pool pointer identity held", closed.Load()); err != nil {
			return err
		}
	}

	// Scenario 2: racing Closes.
	{
		p := newScenarioPool(500, int64(num))
		var closed atomic.Int64
		var executed atomic.Int64
		h := hook.NewHooks()
		h.AddPoolClosed(func(*agilepool.Pool) { closed.Add(1) })
		if err := p.SetHook(h); err != nil {
			return err
		}
		for i := 0; i < num; i++ {
			p.Submit(agilepool.TaskFunc(func() error {
				executed.Add(1)
				return nil
			}))
		}
		var wg sync.WaitGroup
		for i := 0; i < closers; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); p.Close() }()
		}
		wg.Wait()
		// In-flight tasks must still complete after Close (graceful drain).
		if !drain(p, 30*time.Second) {
			return fmt.Errorf("[racing] pool did not drain after racing Close")
		}
		if err := report("hclose", rt, closed.Load() == 1,
			"[racing] %d concurrent Close calls fired OnPoolClosed %d time(s)", closers, closed.Load()); err != nil {
			return err
		}
		if err := report("hclose", rt, executed.Load() == int64(num),
			"[racing] all %d tasks completed despite racing Close", executed.Load()); err != nil {
			return err
		}
	}

	// Scenario 3a: panicking OnPoolClosed callback (bundled Hooks recovers).
	{
		p := newScenarioPool(100, 100)
		h := hook.NewHooks()
		h.AddPoolClosed(func(*agilepool.Pool) { panic(panicMsg) })
		if err := p.SetHook(h); err != nil {
			return err
		}
		p.Close() // must return normally
		if err := report("hclose", rt, true,
			"[panicking-callback] Close returned despite panicking OnPoolClosed callback"); err != nil {
			return err
		}
	}

	// Scenario 3b: hostile hooks implementation panicking at dispatch.
	{
		p := newScenarioPool(100, 100)
		ph := &panicHooks{panicWhen: func(d string) bool { return d == "closed" }, panicValue: panicMsg}
		if err := p.SetHook(ph); err != nil {
			return err
		}
		p.Close() // must return normally
		if err := report("hclose", rt, true,
			"[panicking-dispatch] Close returned despite DispatchPoolClosed panicking"); err != nil {
			return err
		}
	}

	// Scenario 4: post-close silence.
	{
		p := newScenarioPool(100, 100)
		var cnt counters
		var closed atomic.Int64
		h := hook.NewHooks()
		cnt.addTo(h, 1)
		h.AddPoolClosed(func(*agilepool.Pool) { closed.Add(1) })
		if err := p.SetHook(h); err != nil {
			return err
		}
		p.Close()
		for i := 0; i < 200; i++ {
			p.Submit(agilepool.TaskFunc(func() error { return nil }))
			p.TrySubmit(agilepool.TaskFunc(func() error { return nil }))
		}
		if !drain(p, 10*time.Second) {
			return fmt.Errorf("[silence] Wait did not return after post-close submissions")
		}
		s, e, st, c := cnt.snapshot()
		if err := report("hclose", rt,
			s == 0 && e == 0 && st == 0 && c == 0 && closed.Load() == 1,
			"[silence] 400 post-close submissions fired no events (S=%d E=%d St=%d C=%d, closed=%d)",
			s, e, st, c, closed.Load()); err != nil {
			return err
		}
	}

	if !settleGoroutines(baseline, 4, 5*time.Second) {
		return report("hclose", rt, false,
			"goroutine leak: %d goroutines after scenario, baseline %d", goroutineBaseline(), baseline)
	}
	return report("hclose", rt, true, "OnPoolClosed and closed-pool semantics stable")
}
