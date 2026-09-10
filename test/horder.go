package main

// horder verifies the per-task lifecycle contract under full concurrency:
// for every task the four events fire exactly once, Submitted strictly
// precedes Enqueued/Started (submission-path program order), Started
// strictly precedes Completed (worker-path program order), the context
// payload reaches all four events unchanged, and Completed delivers the
// exact value a panicking task panicked with (nil otherwise).
//
// Events of one task are guarded by a per-id state machine. Only the
// guarantees that are real happen-before edges are enforced — Enqueued may
// legitimately land after Started if a worker picks the task up before the
// submitter's enqueue callback runs, so no assertion depends on that race.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

// orderCtxKey carries a task's id through SubmitCtx into the hooks.
type orderCtxKey struct{}

const (
	orderEvSubmitted = iota
	orderEvEnqueued
	orderEvStarted
	orderEvCompleted
)

// panicSentinel is the value a panicking task panics with: ids below
// panicsTotal are expected to surface it in OnTaskCompleted.
func panicSentinel(id int) int { return 900000 + id }

// orderRec is one task's event state machine. Transitions:
//
//	submitted: state 0 -> 1 (only legal first event)
//	enqueued / started: any time after submitted (two writers may race)
//	completed: only after started (worker goroutine program order)
//
// Everything else (duplicates, out-of-order, missing) is an anomaly.
type orderRec struct {
	mu        sync.Mutex
	state     int
	flags     [4]bool
	anomaly   bool
	recovered any
}

func (r *orderRec) fire(ev int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.flags[ev] {
		r.anomaly = true // duplicate event
		return
	}
	switch ev {
	case orderEvSubmitted:
		if r.state != 0 {
			r.anomaly = true
			return
		}
		r.state = 1
	case orderEvEnqueued, orderEvStarted:
		if r.state == 0 { // before submitted: impossible by program order
			r.anomaly = true
			return
		}
	case orderEvCompleted:
		if !r.flags[orderEvStarted] {
			r.anomaly = true
			return
		}
	}
	r.flags[ev] = true
}

func (r *orderRec) setRecovered(v any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recovered = v
}

type horderPlugin struct{}

func (horderPlugin) Name() string { return "horder" }
func (horderPlugin) Desc() string { return "hook stability: per-task event order and payload fidelity" }

func (horderPlugin) Options() []Option {
	return []Option{
		{Name: "num", Default: "20000", Help: "tasks to submit"},
		{Name: "panics", Default: "2000", Help: "how many leading tasks panic (recovered-value check)"},
		{Name: "workers", Default: "1000", Help: "worker capacity of the scenario pool"},
	}
}

func (horderPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)
	num, err := GetInt(opts, "num", 20000)
	if err != nil {
		return err
	}
	panics, err := GetInt(opts, "panics", 2000)
	if err != nil {
		return err
	}
	workers, err := GetInt(opts, "workers", 1000)
	if err != nil {
		return err
	}
	if num < 1 || workers < 1 {
		return fmt.Errorf("num/workers must be >= 1")
	}
	if panics < 0 || panics > num {
		return fmt.Errorf("option %q: must be within [0, num]", "panics")
	}

	baseline := goroutineBaseline()
	p := newScenarioPool(int64(workers), int64(num))
	defer p.Close()

	recs := make([]orderRec, num)
	var badCtx atomic.Int64
	var cnt counters
	var submitted, enqueued, started, completed atomic.Int64

	expectPanic := func(id int) bool { return id < panics }

	h := hook.NewHooks()
	h.AddTaskSubmitted(func(ctx context.Context) {
		id, ok := ctx.Value(orderCtxKey{}).(int)
		if !ok {
			badCtx.Add(1)
			return
		}
		submitted.Add(1)
		recs[id].fire(orderEvSubmitted)
	})
	h.AddTaskEnqueued(func(ctx context.Context) {
		id, ok := ctx.Value(orderCtxKey{}).(int)
		if !ok {
			badCtx.Add(1)
			return
		}
		enqueued.Add(1)
		recs[id].fire(orderEvEnqueued)
	})
	h.AddTaskStarted(func(ctx context.Context) {
		id, ok := ctx.Value(orderCtxKey{}).(int)
		if !ok {
			badCtx.Add(1)
			return
		}
		started.Add(1)
		recs[id].fire(orderEvStarted)
	})
	h.AddTaskCompleted(func(ctx context.Context, recovered any) {
		id, ok := ctx.Value(orderCtxKey{}).(int)
		if !ok {
			badCtx.Add(1)
			return
		}
		completed.Add(1)
		recs[id].fire(orderEvCompleted)
		recs[id].setRecovered(recovered)
	})
	cnt.addTo(h, 1)
	if err := p.SetHook(h); err != nil {
		return err
	}

	for id := 0; id < num; id++ {
		id := id
		var task agilepool.Task = agilepool.TaskFunc(func() error {
			if expectPanic(id) {
				panic(panicSentinel(id))
			}
			return nil
		})
		p.SubmitCtx(context.WithValue(context.Background(), orderCtxKey{}, id), task)
	}

	if !drain(p, 30*time.Second) {
		return fmt.Errorf("pool did not drain within 30s")
	}

	s, e, st, c := cnt.snapshot()
	msg := fmt.Sprintf("submitted=%d enqueued=%d started=%d completed=%d", s, e, st, c)
	if err := report("horder", rt,
		s == int64(num) && e == int64(num) && st == int64(num) && c == int64(num),
		"event totals for %d tasks: %s", num, msg); err != nil {
		return err
	}

	var anomalies int64
	var badRecovered int64
	for id := 0; id < num; id++ {
		r := &recs[id]
		if r.anomaly {
			anomalies++
			continue
		}
		got := r.recovered
		if expectPanic(id) {
			if v, ok := got.(int); !ok || v != panicSentinel(id) {
				badRecovered++
			}
		} else if got != nil {
			badRecovered++
		}
	}
	if err := report("horder", rt, badCtx.Load() == 0,
		"context payload missing/mismatched in %d hook invocations", badCtx.Load()); err != nil {
		return err
	}
	if err := report("horder", rt, anomalies == 0,
		"per-task order anomalies: %d tasks had duplicate/out-of-order events", anomalies); err != nil {
		return err
	}
	if err := report("horder", rt, badRecovered == 0,
		"OnTaskCompleted recovered payload wrong for %d tasks (%d panic tasks)", badRecovered, panics); err != nil {
		return err
	}

	p.SetHook(nil)
	p.Close()
	if !settleGoroutines(baseline, 4, 5*time.Second) {
		return report("horder", rt, false,
			"goroutine leak: %d goroutines after close, baseline %d", goroutineBaseline(), baseline)
	}
	return report("horder", rt, true, "per-task order/payload contract held for all %d tasks", num)
}
