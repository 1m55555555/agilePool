package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

// poolParams is the parsed pool configuration (session-level, kept in Store
// for readers like metrics).
type poolParams struct {
	Workers     int64
	Queue       int64
	CleanPeriod time.Duration
	Container   string
	Mode        string
}

// pool creates the pool from its options and holds it until End closes it.
// The pool is created in Start rather than Run: hook/metrics need the pool
// to exist before any task is submitted, and the Start phase precedes all
// Runs — which is also why Start takes the segment args (see Lifecycle).
// Run has nothing left to do.
type poolPlugin struct{}

func (poolPlugin) Name() string { return "pool" }
func (poolPlugin) Desc() string { return "create the pool from options, Close on End" }

func (poolPlugin) Options() []Option {
	return []Option{
		{Name: "workers", Default: "20000", Help: "max worker count"},
		{Name: "queue", Default: "10000", Help: "task queue size"},
		{Name: "container", Default: "linkedlist", Help: "idle container: linkedlist/minheap/slice/ringqueue/treap"},
		{Name: "mode", Default: "block", Help: "work mode: block/nonblock"},
		{Name: "clean-period", Default: "500ms", Help: "idle worker cleanup period"},
	}
}

func (poolPlugin) Start(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)

	workers, err := GetInt(opts, "workers", 20000)
	if err != nil {
		return err
	}
	queue, err := GetInt(opts, "queue", 10000)
	if err != nil {
		return err
	}
	clean, err := GetDuration(opts, "clean-period", 500*time.Millisecond)
	if err != nil {
		return err
	}

	container := strings.ToLower(GetString(opts, "container", "linkedlist"))
	var ct agilepool.IdleContainerType
	switch container {
	case "linkedlist":
		ct = agilepool.LinkedListType
	case "minheap":
		ct = agilepool.MinHeapType
	case "slice":
		ct = agilepool.SliceType
	case "ringqueue":
		ct = agilepool.RingQueueType
	case "treap":
		ct = agilepool.TreapType
	default:
		return fmt.Errorf("option %q: unsupported value %q", "container", container)
	}

	mode := strings.ToLower(GetString(opts, "mode", "block"))
	var wm agilepool.WorkMode
	switch mode {
	case "block":
		wm = agilepool.BLOCK
	case "nonblock":
		wm = agilepool.NONBLOCK
	default:
		return fmt.Errorf("option %q: unsupported value %q", "mode", mode)
	}

	cfg := agilepool.NewConfig(
		agilepool.WithCleanPeriod(clean),
		agilepool.WithWorkerNumCapacity(int64(workers)),
		agilepool.WithTaskQueueSize(int64(queue)),
		agilepool.WithIdleContainerType(ct),
		agilepool.WithBlockMode(wm),
	)
	p := agilepool.NewPool(cfg)
	Provide(rt.Store, "pool", "pool", p)
	Provide(rt.Store, "pool", "params", poolParams{
		Workers: int64(workers), Queue: int64(queue), CleanPeriod: clean,
		Container: container, Mode: mode,
	})
	return nil
}

func (poolPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	return nil
}

func (poolPlugin) End(ctx context.Context, rt *Runtime) error {
	p, err := Require[*agilepool.Pool](rt.Store, "pool", "pool")
	if err != nil {
		return err
	}
	p.Close()
	return nil
}
