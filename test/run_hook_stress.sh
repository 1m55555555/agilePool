#!/bin/bash
# Hook-system stability scenarios (h* plugin family). Each scenario is a
# standalone plugin that builds its own private pools and asserts PASS/FAIL
# invariants. Every scenario runs even when an earlier one fails, so one
# broken area never hides another; the script exits 1 if any scenario FAILed.
# Run with the race detector:  run_hook_stress.sh race
#   (race builds a -race binary; slower, use it in CI to catch hook races)
# Per-plugin knobs: see `./agilepool_test --help hcount` etc.
if [ "$1" = "race" ]; then
    go build -race -o agilepool_test .
else
    go build -o agilepool_test .
fi

FAILED=0
run() {
    "$@" || FAILED=1
}

run ./agilepool_test --hcount
run ./agilepool_test --hpanic num=3000 stage=all
run ./agilepool_test --hpanic num=200 level=callback stage=all 2>/dev/null
run ./agilepool_test --horder
run ./agilepool_test --hctx
run ./agilepool_test --hblock num=2000 delay-us=100
run ./agilepool_test --hchurn
run ./agilepool_test --hreenter
run ./agilepool_test --hclose
run ./agilepool_test --henqueue

if [ "$FAILED" -ne 0 ]; then
    echo "hook-stability suite: FAILURES"
    exit 1
fi
echo "hook-stability suite: all PASS"
