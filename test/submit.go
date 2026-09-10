package main

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

// submitParams is the parsed submit configuration (session-level, kept in
// Store for readers like metrics).
type submitParams struct {
	Strategy string
	Num      int
}

// phase is one stage of a phased burst: relative start (s), duration (s),
// rate (submissions per second).
type phase struct {
	startSec    int
	durationSec int
	ratePerSec  float64
}

// submit implements the old -U/--submit-* strategies (immediate/linear/
// constant/poisson/phased), logic ported as-is. Run blocks until every task
// is submitted and the pool drains, so the plugin needs no End phase.
type submitPlugin struct{}

func (submitPlugin) Name() string { return "submit" }
func (submitPlugin) Desc() string { return "submit tasks per strategy and wait for drain" }

func (submitPlugin) Deps() []string { return []string{"pool", "task"} }

func (submitPlugin) Options() []Option {
	return []Option{
		{Name: "strategy", Default: "immediate", Help: "strategy: immediate/linear/constant/poisson/phased"},
		{Name: "num", Default: "1000000", Help: "total tasks to submit"},
		{Name: "interval", Default: "10", Help: "linear/constant submit interval (ms)"},
		{Name: "jitter", Default: "0", Help: "linear random jitter (ms)"},
		{Name: "mean-interval", Default: "50", Help: "poisson mean interval (ms)"},
		{Name: "phases", Default: "", Help: "phased schedule: startSec,durSec,ratePerSec;... (semicolon separated)"},
		{Name: "shards", Default: "1", Help: "phased parallel shards"},
		{Name: "submitters", Default: "1", Help: "phased submitters per shard"},
	}
}

func (submitPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)

	strategy := strings.ToLower(GetString(opts, "strategy", "immediate"))
	switch strategy {
	case "immediate", "linear", "constant", "poisson", "phased":
	default:
		return fmt.Errorf("option %q: unsupported value %q", "strategy", strategy)
	}

	num, err := GetInt(opts, "num", 1000000)
	if err != nil {
		return err
	}
	interval, err := GetInt(opts, "interval", 10)
	if err != nil {
		return err
	}
	jitter, err := GetInt(opts, "jitter", 0)
	if err != nil {
		return err
	}
	meanInterval, err := GetInt(opts, "mean-interval", 50)
	if err != nil {
		return err
	}
	shards, err := GetInt(opts, "shards", 1)
	if err != nil {
		return err
	}
	submitters, err := GetInt(opts, "submitters", 1)
	if err != nil {
		return err
	}
	phases, err := parsePhases(GetString(opts, "phases", ""))
	if err != nil {
		return err
	}
	if strategy == "phased" && len(phases) == 0 {
		return fmt.Errorf("option %q: phased requires a non-empty phases schedule", "phases")
	}

	p, err := Require[*agilepool.Pool](rt.Store, "pool", "pool")
	if err != nil {
		return err
	}
	durFn, err := Require[func() time.Duration](rt.Store, "task", "durfn")
	if err != nil {
		return err
	}

	Provide(rt.Store, "submit", "params", submitParams{Strategy: strategy, Num: num})

	switch strategy {
	case "immediate":
		runImmediate(p, num, durFn)
	case "linear":
		runTimedSubmit(p, num, durFn, time.Duration(interval)*time.Millisecond,
			time.Duration(jitter)*time.Millisecond, false)
	case "constant":
		runTimedSubmit(p, num, durFn, time.Duration(interval)*time.Millisecond, 0, true)
	case "poisson":
		runTimedSubmit(p, num, durFn, time.Duration(meanInterval)*time.Millisecond, 0, false)
	case "phased":
		runPhased(p, phases, shards, submitters, durFn)
	}
	return nil
}

// parsePhases parses the "startSec,durSec,ratePerSec;..." schedule.
func parsePhases(s string) ([]phase, error) {
	if s == "" {
		return nil, nil
	}
	var phases []phase
	for i, seg := range strings.Split(s, ";") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		parts := strings.Split(seg, ",")
		if len(parts) != 3 {
			return nil, fmt.Errorf("phase %d: expected 3 comma-separated values, got %d", i, len(parts))
		}
		start, e1 := strconv.Atoi(strings.TrimSpace(parts[0]))
		dur, e2 := strconv.Atoi(strings.TrimSpace(parts[1]))
		rate, e3 := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
		if e1 != nil || e2 != nil || e3 != nil {
			return nil, fmt.Errorf("phase %d: invalid number", i)
		}
		phases = append(phases, phase{startSec: start, durationSec: dur, ratePerSec: rate})
	}
	return phases, nil
}

// newTask wraps a duration factory into a task that sleeps one random
// duration and returns.
func newTask(durFn func() time.Duration) agilepool.Task {
	return agilepool.TaskFunc(func() error {
		time.Sleep(durFn())
		return nil
	})
}

// runImmediate submits all tasks back to back, then blocks until the pool
// drains (Wait covers all submitted tasks).
func runImmediate(p *agilepool.Pool, n int, durFn func() time.Duration) {
	for i := 0; i < n; i++ {
		p.Submit(newTask(durFn))
	}
	p.Wait()
}

// runTimedSubmit submits one task per interval. isConstant pins the delay to
// baseInterval; otherwise jitter>0 yields a linear random delay and
// jitter==0 a poisson delay (matching the old tool). The WaitGroup tracks
// submission attempts: a TrySubmit rejection never queues, so its wg.Done
// is issued immediately; pool.Wait afterwards covers drain.
func runTimedSubmit(p *agilepool.Pool, n int, durFn func() time.Duration, baseInterval, jitter time.Duration, isConstant bool) {
	var wg sync.WaitGroup
	wg.Add(n)

	go func() {
		baseNs := float64(baseInterval)
		jitterNs := float64(jitter)
		for i := 0; i < n; i++ {
			if !p.TrySubmit(newTaskDone(durFn, &wg)) {
				wg.Done()
			}

			var delay time.Duration
			switch {
			case isConstant:
				delay = baseInterval
			case baseInterval == 0:
				delay = 0
			case jitter == 0:
				delay = time.Duration(-baseNs * math.Log(rand.Float64())) // poisson
			default:
				delay = time.Duration(baseNs + rand.Float64()*jitterNs) // linear
			}
			if delay > 0 {
				time.Sleep(delay)
			}
		}
	}()

	wg.Wait()
	p.Wait()
}

// newTaskDone wraps a task that signals wg.Done on completion (timed paths).
func newTaskDone(durFn func() time.Duration, wg *sync.WaitGroup) agilepool.Task {
	return agilepool.TaskFunc(func() error {
		defer wg.Done()
		time.Sleep(durFn())
		return nil
	})
}

// runPhased drives a multi-phase burst through token channels (ported from
// the old tool): one token dispenser per shard feeds the shard's submitters,
// and the dispenser controls the rate.
func runPhased(p *agilepool.Pool, phases []phase, shards, submitters int, durFn func() time.Duration) {
	if shards < 1 {
		shards = 1
	}
	if submitters < 1 {
		submitters = 1
	}

	var submitWG sync.WaitGroup
	for s := 0; s < shards; s++ {
		tokenCh := make(chan struct{}, 10000)
		go dispenseTokens(tokenCh, phases, shards)
		for g := 0; g < submitters; g++ {
			submitWG.Add(1)
			go func() {
				defer submitWG.Done()
				for range tokenCh {
					p.Submit(newTask(durFn))
				}
			}()
		}
	}

	submitWG.Wait()
	p.Wait()
}

// dispenseTokens emits one token per millisecond step using a float
// accumulator, which supports sub-millisecond rates (e.g. 0.5 tokens/s per
// shard fires every 2 s). Tokens are dropped when the channel is full, as in
// the old tool. Gaps between phases are slept through until the next start.
func dispenseTokens(tokenCh chan<- struct{}, phases []phase, shards int) {
	defer close(tokenCh)

	var elapsedMs int
	for _, p := range phases {
		startMs := p.startSec * 1000
		if startMs > elapsedMs {
			time.Sleep(time.Duration(startMs-elapsedMs) * time.Millisecond)
			elapsedMs = startMs
		}

		ratePerShard := p.ratePerSec / float64(shards)
		var acc float64
		totalMs := p.durationSec * 1000
		for ms := 0; ms < totalMs; ms++ {
			acc += ratePerShard / 1000
			for acc >= 1 {
				select {
				case tokenCh <- struct{}{}:
				default:
				}
				acc--
			}
			time.Sleep(time.Millisecond)
		}
		elapsedMs += totalMs
	}
}
