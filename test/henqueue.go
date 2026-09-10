package main

// henqueue stress-tests OnTaskEnqueued on the overflow-buffer path, which the
// rest of the family deliberately avoids by sizing queue == task count. With
// a small queue and slow tasks most submissions end up in the chunked buffer,
// where the current library has two known problems:
//
//   - tasks a worker pulls out of the buffer via PopBatch never fire Enqueued
//     (the event is skipped), so Enqueued accounting is wrong under backlog;
//   - when a buffered task is forwarded to the channel, Enqueued is dispatched
//     while taskBuf's lock is held, so a slow callback stalls the buffer and a
//     reentrant callback that submits again can self-deadlock on the
//     non-reentrant mutex.
//
// The scenario asserts the contract the event is supposed to honour — exactly
// one Enqueued per accepted task, callbacks free to block or resubmit — so it
// fails (rather than masks) while the library violates it. Both waves run
// even when the first one fails:
//
//  1. accounting: all four counters land on num even though the queue is far
//     smaller than the submission count; a slow Enqueued callback must not
//     stall the pool.
//  2. reentrancy: the Enqueued callback submits new tasks while the pool is
//     dispatching. The submission loop runs on a watchdog goroutine so a
//     self-deadlock reports FAIL instead of hanging the harness.
//
// Ordering note: Enqueued may be observed after Started (and, on the buffer
// path, after Completed). Nothing here asserts Enqueued ordering, only its
// exactly-once count.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

type henqueuePlugin struct{}

func (henqueuePlugin) Name() string { return "henqueue" }
func (henqueuePlugin) Desc() string {
	return "hook stability: Enqueued accounting and reentrancy on the overflow buffer"
}

func (henqueuePlugin) Options() []Option {
	return []Option{
		{Name: "num", Default: "2000", Help: "tasks in the accounting wave"},
		{Name: "reenter", Default: "500", Help: "reentrant submissions per reentrant attempt"},
		{Name: "workers", Default: "1", Help: "worker capacity (keep well below num)"},
		{Name: "queue", Default: "2", Help: "channel capacity (keep well below num)"},
		{Name: "delay-us", Default: "50", Help: "sleep per slow Enqueued callback (microseconds)"},
		{Name: "submitters", Default: "1", Help: "concurrent submitters per reentrant attempt (1 maximizes the race window)"},
		{Name: "rtask-us", Default: "100", Help: "task duration in the reentrant wave (microseconds)"},
		{Name: "attempts", Default: "8", Help: "independent reentrant attempts on fresh pools; the first deadlock is reported"},
	}
}

func (henqueuePlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)
	num, err := GetInt(opts, "num", 2000)
	if err != nil {
		return err
	}
	reenter, err := GetInt(opts, "reenter", 500)
	if err != nil {
		return err
	}
	workers, err := GetInt(opts, "workers", 1)
	if err != nil {
		return err
	}
	queue, err := GetInt(opts, "queue", 2)
	if err != nil {
		return err
	}
	delayUs, err := GetInt(opts, "delay-us", 50)
	if err != nil {
		return err
	}
	submitters, err := GetInt(opts, "submitters", 1)
	if err != nil {
		return err
	}
	rtaskUs, err := GetInt(opts, "rtask-us", 100)
	if err != nil {
		return err
	}
	attempts, err := GetInt(opts, "attempts", 8)
	if err != nil {
		return err
	}
	if num < 1 || workers < 1 || queue < 1 {
		return fmt.Errorf("num/workers/queue must be >= 1")
	}
	if reenter < 1 {
		return fmt.Errorf("option %q: must be >= 1", "reenter")
	}
	if delayUs < 0 || rtaskUs < 0 {
		return fmt.Errorf("delay-us/rtask-us must be >= 0")
	}
	if submitters < 1 {
		return fmt.Errorf("option %q: must be >= 1", "submitters")
	}
	if attempts < 1 {
		return fmt.Errorf("option %q: must be >= 1", "attempts")
	}

	baseline := goroutineBaseline()

	// Both waves run even when one fails; the process must not settle
	// goroutines after a wave failure because a reentrant deadlock leaves a
	// submitter wedged on the buffer lock until the process exits.
	var failures []string
	if err := overflowWave(rt, num, workers, queue, delayUs); err != nil {
		failures = append(failures, err.Error())
	}
	if err := reentrantWave(rt, num, reenter, workers, queue, submitters, rtaskUs, attempts); err != nil {
		failures = append(failures, err.Error())
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d/2 wave(s) failed: %s", len(failures), strings.Join(failures, " | "))
	}

	if !settleGoroutines(baseline, 4, 5*time.Second) {
		return report("henqueue", rt, false,
			"goroutine leak: %d goroutines after scenario, baseline %d", goroutineBaseline(), baseline)
	}
	return report("henqueue", rt, true, "overflow-buffer Enqueued semantics stable")
}

// overflowWave asserts exact accounting through the overflow buffer. One slow
// worker and a tiny queue guarantee a backlog, so most tasks travel through
// the buffer; every accepted task must still fire Enqueued exactly once.
func overflowWave(rt *Runtime, num, workers, queue, delayUs int) error {
	p := newScenarioPool(int64(workers), int64(queue))
	h := hook.NewHooks()
	var cnt counters
	cnt.addTo(h, 1)
	var slowEnqueued atomic.Int64
	h.AddTaskEnqueued(func(context.Context) {
		time.Sleep(time.Duration(delayUs) * time.Microsecond)
		slowEnqueued.Add(1)
	})
	if err := p.SetHook(h); err != nil {
		return err
	}

	var executed atomic.Int64
	// Track the peak channel occupancy during submission: one slow worker and
	// a tiny queue guarantee the channel hits capacity, which is the
	// observable proof that the backlog overflowed into the buffer. Checking
	// only after the loop would race with the worker draining a slot.
	maxQueueLen := 0
	for i := 0; i < num; i++ {
		if l := p.GetTaskQueueLen(); l > maxQueueLen {
			maxQueueLen = l
		}
		p.Submit(agilepool.TaskFunc(func() error {
			executed.Add(1)
			time.Sleep(time.Millisecond)
			return nil
		}))
	}
	if err := report("henqueue", rt,
		maxQueueLen == p.GetTaskQueueCapacity(),
		"[overflow] channel hit capacity during submission (max len=%d cap=%d): overflow buffer exercised",
		maxQueueLen, p.GetTaskQueueCapacity()); err != nil {
		return err
	}
	if !drain(p, 60*time.Second) {
		return fmt.Errorf("[overflow] pool did not drain within 60s")
	}

	s, e, st, c := cnt.snapshot()
	if err := report("henqueue", rt,
		s == int64(num) && e == int64(num) && st == int64(num) && c == int64(num),
		"[overflow] queue=%d num=%d counters submitted=%d enqueued=%d started=%d completed=%d, want all == %d",
		queue, num, s, e, st, c, num); err != nil {
		return err
	}
	if err := report("henqueue", rt, slowEnqueued.Load() == int64(num),
		"[overflow] slow Enqueued callback saw %d/%d events without stalling the pool", slowEnqueued.Load(), num); err != nil {
		return err
	}
	if err := report("henqueue", rt, executed.Load() == int64(num),
		"[overflow] tasks executed %d == num %d", executed.Load(), num); err != nil {
		return err
	}
	p.SetHook(nil)
	p.Close()
	return nil
}

// reentrantWave submits from inside an Enqueued callback while the overflow
// buffer is hot. The self-deadlock needs a narrow interleaving — the direct
// channel send fails, a worker frees a slot, the buffer-lock forward then
// succeeds and its Enqueued dispatch re-enters Submit — so a single attempt
// only hits it intermittently. The wave therefore runs `attempts` independent
// attempts on fresh pools: the first deadlock is reported immediately, while
// attempts that merely mis-account (Enqueued skipped) are aggregated.
func reentrantWave(rt *Runtime, num, reenter, workers, queue, submitters, rtaskUs, attempts int) error {
	var failures []string
	for a := 1; a <= attempts; a++ {
		deadlocked, err := reentrantAttempt(rt, a, num, reenter, workers, queue, submitters, rtaskUs)
		if deadlocked {
			return err
		}
		if err != nil {
			failures = append(failures, fmt.Sprintf("attempt %d: %v", a, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d/%d reentrant attempt(s) failed: %s", len(failures), attempts, strings.Join(failures, " | "))
	}
	return nil
}

// reentrantAttempt is one fresh-pool reentrancy attempt. It reports a true
// deadlock separately from ordinary assertion failures: a wedged attempt must
// stop the wave at once, because its submitter still holds the old pool's
// buffer lock. Each attempt's submit loop runs on a watchdog goroutine, so a
// wedged Submit reports FAIL instead of hanging the process.
func reentrantAttempt(rt *Runtime, attempt, num, reenter, workers, queue, submitters, rtaskUs int) (bool, error) {
	p := newScenarioPool(int64(workers), int64(queue))
	h := hook.NewHooks()
	var cnt counters
	cnt.addTo(h, 1)
	var remaining atomic.Int64
	remaining.Store(int64(reenter))
	var reentered atomic.Int64
	var executed atomic.Int64
	h.AddTaskEnqueued(func(context.Context) {
		if remaining.Add(-1) < 0 {
			return
		}
		reentered.Add(1)
		p.Submit(agilepool.TaskFunc(func() error {
			executed.Add(1)
			return nil
		}))
	})
	if err := p.SetHook(h); err != nil {
		return false, err
	}

	per := num / submitters
	if per < 1 {
		per = 1
	}
	originals := int64(per * submitters)

	var submitWG sync.WaitGroup
	submitDone := make(chan struct{})
	for s := 0; s < submitters; s++ {
		submitWG.Add(1)
		go func() {
			defer submitWG.Done()
			for i := 0; i < per; i++ {
				p.Submit(agilepool.TaskFunc(func() error {
					executed.Add(1)
					time.Sleep(time.Duration(rtaskUs) * time.Microsecond)
					return nil
				}))
			}
		}()
	}
	go func() {
		submitWG.Wait()
		close(submitDone)
	}()

	select {
	case <-submitDone:
	case <-time.After(10 * time.Second):
		// Do NOT Close here: Close takes the same buffer lock the wedged
		// submitter holds. The harness exits the process on the error.
		return true, report("henqueue", rt, false,
			"[reentrant] attempt %d: submitter deadlocked after %d submissions with %d submitters: reentrant Enqueued dispatch re-entered the buffer lock (reentered=%d)",
			attempt, cnt.Submitted.Load(), submitters, reentered.Load())
	}
	if !drain(p, 60*time.Second) {
		return false, fmt.Errorf("[reentrant] attempt %d: pool did not drain: reentrant Enqueued callback deadlocked (%d originals, reenter=%d)", attempt, originals, reenter)
	}
	p.SetHook(nil)
	defer p.Close()

	total := originals + reentered.Load()
	s, e, st, c := cnt.snapshot()
	if err := report("henqueue", rt,
		s == total && e == total && st == total && c == total,
		"[reentrant] attempt %d counters submitted=%d enqueued=%d started=%d completed=%d for %d originals + %d reentries",
		attempt, s, e, st, c, originals, reentered.Load()); err != nil {
		return false, err
	}
	if err := report("henqueue", rt, executed.Load() == total,
		"[reentrant] attempt %d executed %d == %d originals + %d reentries", attempt, executed.Load(), originals, reentered.Load()); err != nil {
		return false, err
	}
	if err := report("henqueue", rt, reentered.Load() == int64(reenter),
		"[reentrant] attempt %d reentered %d tasks, want exactly budget %d", attempt, reentered.Load(), reenter); err != nil {
		return false, err
	}
	return false, nil
}
