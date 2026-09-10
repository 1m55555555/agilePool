package main

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

// hookStats holds the hook mode and the four lifecycle counters. It is the
// plugin's own state; only the handle is shared via Store for metrics to
// read optionally. Per the Store layering rule the counters themselves are
// never written through the shared table.
type hookStats struct {
	Mode      string
	Submitted atomic.Int64
	Enqueued  atomic.Int64
	Started   atomic.Int64
	Completed atomic.Int64
}

// hook instruments the pool (old setupHooks). Modes:
//
//   - none: no callbacks registered (raw pool baseline)
//   - hook: one atomic counter per lifecycle event, measuring callback
//     dispatch overhead
//   - trace: the old trace mode depended on context-tracing public APIs
//     (OnTaskSubmitted/TimingHook/NewContext) that were removed upstream;
//     internal/context is not wired up yet, so trace is explicitly rejected
//
// Counters use atomics because all four events sit on the per-task hot path;
// any heavier synchronization would pollute the overhead being measured.
type hookPlugin struct{}

func (hookPlugin) Name() string { return "hook" }
func (hookPlugin) Desc() string { return "event instrumentation (none/hook; trace pending upstream)" }

func (hookPlugin) Deps() []string { return []string{"pool"} }

func (hookPlugin) Options() []Option {
	return []Option{
		{Name: "mode", Default: "none", Help: "instrumentation mode: none/hook (trace pending)"},
	}
}

func (hookPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	mode := strings.ToLower(GetString(ParseOptions(args), "mode", "none"))
	switch mode {
	case "none", "hook":
	case "trace":
		return fmt.Errorf("mode=trace is not supported yet: depends on upstream context tracing wiring (internal/context)")
	default:
		return fmt.Errorf("option %q: unsupported value %q", "mode", mode)
	}

	stats := &hookStats{Mode: mode}
	if mode == "hook" {
		p, err := Require[*agilepool.Pool](rt.Store, "pool", "pool")
		if err != nil {
			return err
		}
		h := hook.NewHooks()
		h.AddTaskSubmitted(func(context.Context) { stats.Submitted.Add(1) })
		h.AddTaskEnqueued(func(context.Context) { stats.Enqueued.Add(1) })
		h.AddTaskStarted(func(context.Context) { stats.Started.Add(1) })
		h.AddTaskCompleted(func(context.Context, any) { stats.Completed.Add(1) })
		if err := p.SetHook(h); err != nil {
			return err
		}
	}
	// Provide the handle in every mode so metrics does not need to know
	// whether hook ran: it reads with Get and treats absence as none/zero.
	Provide(rt.Store, "hook", "stats", stats)
	return nil
}
