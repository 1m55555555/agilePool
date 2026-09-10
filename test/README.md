# agilepool Test Harness (Plugin-Based)

The performance test tool for go-agile-pool. The full design document and
**plugin authoring guide** (English) live in
[`docs/test-harness.md`](../docs/test-harness.md), and a runnable integration
example is in [`example.go`](example.go). Chinese version:
[`README_zh-CN.md`](README_zh-CN.md).

## What this is

The legacy tool flattened pool options, task duration models, submit
strategies, instrumentation, sampling and profiling into a single `main`
with short-flag prefixes (`-T`/`-U`/`-w`...), which was ambiguous and hard to
compose. The harness is rebuilt as a **plugin-based, sub-command style CLI**:
every capability is an independent plugin and the host framework only splits
segments, plans dependencies and drives the lifecycle. **Adding a capability
= one new file + one line in `plugins.go`; the framework stays untouched.**

## Implemented plugins

| Segment | Purpose | Key options (default) | Lifecycle |
|---|---|---|---|
| `--pool` | create and hold the pool | `workers`(20000) `queue`(10000) `container`(linkedlist) `mode`(block) `clean-period`(500ms) | Start creates / End `Close` |
| `--task` | task duration model | `type`(fixed) `base`(10) `extra`(0) `mean`(10) `sigma`(5) | — |
| `--submit` | submit per strategy, block until drain | `strategy`(immediate) `num`(1000000) `interval`(10) `jitter`(0) `mean-interval`(50) `phases` `shards`(1) `submitters`(1) | — |
| `--hook` | event instrumentation | `mode`(none; hook counters; trace pending upstream tracing) | — |
| `--metrics` | periodic sampling + summary | `interval`(1s) `format`(csv) `file`(metrics.csv) `wait-exit`(0) | Start sampler / End final tick + window + summary |
| `--profile` | pprof enveloping the session | `cpu`(false) `mem`(false) | Start CPU / End stop CPU + write heap |

### Hook-stability stress plugins (`h*` family)

Each is a standalone scenario that builds its own private pools, installs
adversarial hooks and asserts PASS/FAIL invariants (exit 1 on the first
FAIL). They are deliberately dependency-free so `--hcount` alone works:

| Segment | Scenario under stress | Key options (default) |
|---|---|---|
| `--hcount` | exact per-event accounting: N callbacks × M tasks, no lost/duplicated event | `num`(20000) `hooks`(8) `workers`(1000) |
| `--hpanic` | panicking hooks must not kill submit/worker/Close paths | `num`(3000) `level`(dispatch/callback) `stage`(all/…) |
| `--horder` | per-task order (Submitted first, Started before Completed) + ctx payload + panic-value fidelity | `num`(20000) `panics`(2000) `workers`(1000) |
| `--hctx` | ctx payload through all events; pre-canceled and cancel-while-queued semantics | `num`(3000) `queued`(150) |
| `--hblock` | slow/blocking hooks must not deadlock or lose events | `num`(5000) `delay-us`(200) `workers`(200) |
| `--hchurn` | concurrent registration burst before dispatch (contract window), exact accounting | `num`(10000) `num2`(5000) `churners`(4) `adds`(25) |
| `--hreenter` | reentrant dispatch: hooks that submit new tasks | `num`(2000) `budget`(2000) `depth`(32) `stage`(submitted/completed) |
| `--hclose` | OnPoolClosed exactly-once (incl. racing Close) and post-close silence | `num`(2000) `closers`(4) |
| `--henqueue` | Enqueued accounting on the overflow buffer (queue << num), slow and reentrant Enqueued callbacks | `num`(2000) `reenter`(500) `workers`(1) `queue`(2) `delay-us`(50) `submitters`(1) `rtask-us`(100) `attempts`(8) |

Submit strategies: immediate / linear / constant / poisson / phased. Task
duration types: fixed / uniform / normal. Dependencies:
`submit ← pool+task`, `hook ← pool`, `metrics ← pool`. Missing dependency
segments are auto-inserted with defaults (e.g. `--submit` alone also runs
pool/task); misordered dependencies, duplicate segments and cycles all exit
with code 2.

## Quick start

```text
# list plugins and get help
go run . --list
go run . --help submit

# a full benchmark scenario (hook counters appear in the metrics rows)
go run . --pool workers=20000 queue=10000 \
         --task type=fixed base=500 \
         --hook mode=hook \
         --submit strategy=immediate num=200000 \
         --metrics interval=1 format=csv file=out.csv
```

Regression scripts covering the whole scenario set (equivalent to the old
run_test, scenario for scenario, keeping the legacy auto-generated output
file names):

```text
run_test.bat        # Windows
run_test.sh         # Linux/macOS
run_hook_stress.bat # Windows: all h* hook-stability scenarios
run_hook_stress.sh  # Linux/macOS: same; run_hook_stress.* race -> -race build
```

`test/` is its own Go module (`github.com/Yiming1997/agilePool/v2/test`); a
`replace` directive points the `github.com/Yiming1997/agilePool/v2` import
at the repo root directory, so the harness always builds against the local
library and never downloads it.

## Conventions at a glance

- A **segment head** is a registered plugin name with a `--` prefix; it opens
  a new segment anywhere. Segment arguments are passed through **verbatim,
  in order** for the plugin to parse (`-T fixed --task-base 500` works), and
  `key=value` is also supported (`ParseOptions`/`GetInt`/`GetDuration`/
  `GetBool`/`GetFloat`).
- Lifecycle: Start (before all Runs, in order, with segment args) → Run
  (segment by segment) → End (reverse teardown). A failure aborts the run,
  but started segments still get their End.
- Plugins only exchange **session-level objects** (pool handle/configs/
  factories) through the shared Store; per-task counters stay in plugin-owned
  atomic state and metrics reads them through the handle — the shared table
  never enters the hot path.
- Exit codes: 0 success / 1 runtime error / 2 usage error; errors carry a
  `[plugin]` prefix.
- CLI analysis, execution model, and the column-by-column mapping to the
  legacy data format are detailed in
  [`docs/test-harness.md`](../docs/test-harness.md).

## Layout

```text
docs/test-harness.md  design doc + plugin authoring guide (English, repo-root docs/)
test/
  README.md         this file (English)
  README_zh-CN.md   Chinese version
  main.go           host: segment split / planning / lifecycle / help
  plugin.go         interfaces (Plugin/Optioned/Lifecycle/Depender) + Runtime
  option.go         key=value parsing and typed getters
  store.go          session-level shared Store (Provide/Get/Require)
  registry.go       registry (duplicate/dependency registration checks)
  plan.go           dependency planning (auto-insert/order/cycles)
  cli.go            segment splitting + --list/--help
  plugins.go        central registration table
  example.go        demo plugins provider/consumer (not registered by default)
  pool.go ...       the six real plugins (one file per plugin)
  hookcheck.go      shared machinery for the h* stability family
  hcount.go ...     the nine h* hook-stability plugins (h*.go, one per file)
  run_test.bat      Windows regression script (new syntax)
  run_test.sh       Linux/macOS regression script (same scenarios as the bat)
  run_hook_stress.bat/.sh  hook-stability scenario scripts (optional `race` arg)
  plot_csv.py       plots metrics_*.csv into per-file SVGs (run from the results dir)
```

Code comments are English, matching the rest of the library; the design doc
(`docs/test-harness.md`) is English, this README is bilingual.
