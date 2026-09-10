package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/shirou/gopsutil/v3/cpu"
)

// metrics replaces the old Couter plus the three-phase shutdown pieces that
// were main-level (final sample tick, wait-exit observation window, and the
// Elapsed/Done summary). Phase split:
//
//   - Start: parse options, open the file, launch the sampler goroutine.
//     It must happen before submit runs so the whole submission period is
//     covered — hence args on Start (see Lifecycle).
//   - Run: nothing to do.
//   - End: sleep one interval so the sampler records the drained state, run
//     the wait-exit window (the sampler keeps ticking, capturing idle memory
//     changes), then stop the sampler, close the file and print the summary.
//
// The sampler reads the pool handle from Store on every tick instead of
// requiring it up front: the pool is provided during pool.Start so it is
// normally in place before the first tick, but reading lazily avoids a hard
// ordering dependency (e.g. --pool written after --metrics).
type metricsPlugin struct {
	interval float64 // sampling interval (seconds)
	format   string
	file     string
	waitExit int

	f     *os.File
	stop  chan struct{}
	wg    sync.WaitGroup
	start time.Time
	rt    *Runtime
	store *Store
}

func (m *metricsPlugin) Name() string { return "metrics" }
func (m *metricsPlugin) Desc() string {
	return "sample runtime/pool/GC metrics periodically, summary on End"
}

func (m *metricsPlugin) Deps() []string { return []string{"pool"} }

func (m *metricsPlugin) Options() []Option {
	return []Option{
		{Name: "interval", Default: "1", Help: "sampling interval (seconds); omit the segment to disable"},
		{Name: "format", Default: "csv", Help: "output format: csv/json"},
		{Name: "file", Default: "metrics.csv", Help: "output file path"},
		{Name: "wait-exit", Default: "0", Help: "extra observation seconds after draining (idle memory)"},
	}
}

func (m *metricsPlugin) Start(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)

	interval, err := GetFloat(opts, "interval", 1)
	if err != nil {
		return err
	}
	if interval <= 0 {
		return fmt.Errorf("option %q: must be > 0 (omit --metrics to disable sampling)", "interval")
	}
	format := GetString(opts, "format", "csv")
	if format != "csv" && format != "json" {
		return fmt.Errorf("option %q: unsupported value %q", "format", format)
	}
	file := GetString(opts, "file", "metrics.csv")
	waitExit, err := GetInt(opts, "wait-exit", 0)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create %s: %w", file, err)
	}

	m.interval = interval
	m.format = format
	m.file = file
	m.waitExit = waitExit
	m.f = f
	m.stop = make(chan struct{})
	m.start = time.Now()
	m.rt = rt
	m.store = rt.Store

	m.wg.Add(1)
	go m.sampleLoop()
	return nil
}

func (m *metricsPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	return nil
}

func (m *metricsPlugin) End(ctx context.Context, rt *Runtime) error {
	defer m.f.Close()
	// Final sample tick: tasks are drained and workers are still exiting;
	// sleeping one interval lets the sampler record the end state. With
	// wait-exit set, the sampler keeps running through the observation
	// window so memory changes stay visible, matching the old tool.
	if m.waitExit > 0 {
		fmt.Fprintf(rt.Out, "  Sampling for %.3fs...\n", m.interval)
		time.Sleep(time.Duration(m.interval * float64(time.Second)))
		fmt.Fprintf(rt.Out, "  Waiting %ds to observe memory changes...\n", m.waitExit)
		time.Sleep(time.Duration(m.waitExit) * time.Second)
	} else {
		fmt.Fprintf(rt.Out, "  Waiting %.3fs for final metrics collection...\n", m.interval)
		time.Sleep(time.Duration(m.interval * float64(time.Second)))
	}

	close(m.stop)
	m.wg.Wait()

	elapsed := time.Since(m.start)
	fmt.Fprintf(rt.Out, "  Elapsed:       %s\n", elapsed)
	fmt.Fprintln(rt.Out, "  Done.")
	return nil
}

// sampleLoop ticks every interval and writes a runtime/pool/GC/CPU/hook row.
// The CSV header is written on the first tick (same behavior as the old
// Couter) and the field set matches the old tool column for column.
func (m *metricsPlugin) sampleLoop() {
	defer m.wg.Done()
	tick := time.NewTicker(time.Duration(m.interval * float64(time.Second)))
	defer tick.Stop()

	csvHeader := "run_sec,goroutines,heap_alloc_mb,total_alloc_mb,sys_mb," +
		"gc_total,gc_pause_total_ms,gc_pause_avg_ms,gc_cpu_pct," +
		"last_gc_sec,next_gc_mb," +
		"workers_running,workers_idle,workers_created,task_queue_len," +
		"cpu_pct,hook_mode,hook_submitted,hook_enqueued,hook_started,hook_completed\n"
	headerWritten := false

	for {
		select {
		case <-tick.C:
			if m.format == "csv" && !headerWritten {
				fmt.Fprint(m.f, csvHeader)
				headerWritten = true
			}
			m.sample()
		case <-m.stop:
			return
		}
	}
}

// sample reads one row of state. Pool and hook data are optional reads: if
// the pool is not up yet (or the hook segment did not run), zeroes are used.
func (m *metricsPlugin) sample() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	cpuPct, _ := cpu.Percent(100*time.Millisecond, false)
	var cpuUsage float64
	if len(cpuPct) > 0 {
		cpuUsage = cpuPct[0]
	}

	var running, idle, created int64
	var queueLen int
	if p, ok := Get[*agilepool.Pool](m.store, "pool", "pool"); ok {
		running = p.GetRunningWorkersNum()
		idle = p.GetIdleWorkerCount()
		created = p.GetWorkerCreateCount()
		queueLen = p.GetTaskQueueLen()
	}
	hookMode := "none"
	var hookSubmitted, hookEnqueued, hookStarted, hookCompleted int64
	if stats, ok := Get[*hookStats](m.store, "hook", "stats"); ok {
		hookMode = stats.Mode
		hookSubmitted = stats.Submitted.Load()
		hookEnqueued = stats.Enqueued.Load()
		hookStarted = stats.Started.Load()
		hookCompleted = stats.Completed.Load()
	}

	runSec := time.Since(m.start).Seconds()

	var lastGCRel float64
	if memStats.NumGC > 0 {
		lastGCRel = float64(memStats.LastGC-uint64(m.start.UnixNano())) / 1e9
	}
	totalPauseMs := float64(memStats.PauseTotalNs) / 1e6
	var avgPauseMs float64
	if memStats.NumGC > 0 {
		avgPauseMs = totalPauseMs / float64(memStats.NumGC)
	}

	switch m.format {
	case "csv":
		fmt.Fprintf(m.f, "%.3f,%d,%.2f,%.2f,%.2f,%d,%.2f,%.2f,%.4f,%.3f,%.2f,%d,%d,%d,%d,%.2f,%s,%d,%d,%d,%d\n",
			runSec,
			runtime.NumGoroutine(),
			float64(memStats.Alloc)/1024/1024,
			float64(memStats.TotalAlloc)/1024/1024,
			float64(memStats.Sys)/1024/1024,
			memStats.NumGC,
			totalPauseMs,
			avgPauseMs,
			memStats.GCCPUFraction*100,
			lastGCRel,
			float64(memStats.NextGC)/1024/1024,
			running, idle, created, queueLen, cpuUsage,
			hookMode, hookSubmitted, hookEnqueued, hookStarted, hookCompleted)
	case "json":
		fmt.Fprintf(m.f, `{"run_sec":%.3f,"goroutines":%d,"heap_alloc_mb":%.2f,`+
			`"total_alloc_mb":%.2f,"sys_mb":%.2f,"gc_total":%d,`+
			`"gc_pause_total_ms":%.2f,"gc_pause_avg_ms":%.2f,`+
			`"gc_cpu_pct":%.4f,"last_gc_sec":%.3f,"next_gc_mb":%.2f,`+
			`"workers_running":%d,"workers_idle":%d,`+
			`"workers_created":%d,"task_queue_len":%d,`+
			`"cpu_pct":%.2f,"hook_mode":"%s","hook_submitted":%d,"hook_enqueued":%d,"hook_started":%d,"hook_completed":%d}`+"\n",
			runSec,
			runtime.NumGoroutine(),
			float64(memStats.Alloc)/1024/1024,
			float64(memStats.TotalAlloc)/1024/1024,
			float64(memStats.Sys)/1024/1024,
			memStats.NumGC,
			totalPauseMs,
			avgPauseMs,
			memStats.GCCPUFraction*100,
			lastGCRel,
			float64(memStats.NextGC)/1024/1024,
			running, idle, created, queueLen,
			cpuUsage, hookMode, hookSubmitted, hookEnqueued, hookStarted, hookCompleted)
	}
}
