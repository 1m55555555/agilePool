package main

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"time"
)

// taskParams is the parsed task duration model (session-level, kept in Store).
type taskParams struct {
	Type    string
	BaseMs  int // fixed/uniform base
	ExtraMs int // uniform random range
	MeanMs  int // normal mean
	SigmaMs int // normal stddev
}

// task builds the task duration model (old -T/--task-* flags) and provides
// two outputs: a duration factory (durfn) that submit calls once per task on
// the hot path, and the params table (session metadata). Types match the old
// tool: fixed/uniform/normal.
type taskPlugin struct{}

func (taskPlugin) Name() string { return "task" }
func (taskPlugin) Desc() string { return "task duration model, provides a duration factory" }

func (taskPlugin) Options() []Option {
	return []Option{
		{Name: "type", Default: "fixed", Help: "duration type: fixed/uniform/normal"},
		{Name: "base", Default: "10", Help: "fixed/uniform base duration (ms)"},
		{Name: "extra", Default: "0", Help: "uniform random range (ms)"},
		{Name: "mean", Default: "10", Help: "normal mean (ms)"},
		{Name: "sigma", Default: "5", Help: "normal stddev (ms)"},
	}
}

func (taskPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)

	kind := strings.ToLower(GetString(opts, "type", "fixed"))
	switch kind {
	case "fixed", "uniform", "normal":
	default:
		return fmt.Errorf("option %q: unsupported value %q", "type", kind)
	}

	base, err := GetInt(opts, "base", 10)
	if err != nil {
		return err
	}
	extra, err := GetInt(opts, "extra", 0)
	if err != nil {
		return err
	}
	mean, err := GetInt(opts, "mean", 10)
	if err != nil {
		return err
	}
	sigma, err := GetInt(opts, "sigma", 5)
	if err != nil {
		return err
	}

	Provide(rt.Store, "task", "durfn", buildDurationFn(taskParams{
		Type: kind, BaseMs: base, ExtraMs: extra, MeanMs: mean, SigmaMs: sigma,
	}))
	Provide(rt.Store, "task", "params", taskParams{
		Type: kind, BaseMs: base, ExtraMs: extra, MeanMs: mean, SigmaMs: sigma,
	})
	return nil
}

// buildDurationFn returns a concurrency-safe duration generator. The
// math/rand/v2 top-level functions are goroutine-safe, so submit shards can
// share one factory.
func buildDurationFn(cfg taskParams) func() time.Duration {
	switch cfg.Type {
	case "uniform":
		base := float64(cfg.BaseMs)
		extra := float64(cfg.ExtraMs)
		return func() time.Duration {
			return time.Duration(base+rand.Float64()*extra) * time.Millisecond
		}
	case "normal":
		mean := float64(cfg.MeanMs)
		sigma := float64(cfg.SigmaMs)
		return func() time.Duration {
			v := math.Max(0, rand.NormFloat64()*sigma+mean)
			return time.Duration(v) * time.Millisecond
		}
	}
	// fixed (and the default fallback): capture the constant once.
	d := time.Duration(cfg.BaseMs) * time.Millisecond
	return func() time.Duration { return d }
}
