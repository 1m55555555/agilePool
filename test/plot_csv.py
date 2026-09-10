"""
plot_csv.py — plot harness metrics CSVs produced by the plugin-based test
harness (see docs/test-harness.md and the run_test scripts).

Reads every `metrics_*.csv` in the current directory (column layout of the
current `--metrics` plugin: run_sec, goroutines, memory/GC/CPU, worker
counters, task queue, hook counters), draws one subplot per metric against
run_sec, and saves `metrics_<name>.svg` next to each file.

Usage:
    python plot_csv.py          # from the directory holding the CSVs
"""

import glob
import os

import matplotlib.pyplot as plt
import pandas as pd

# Metrics common to every run (columns exist in the current CSV header).
METRICS = [
    ("heap_alloc_mb", "Heap Alloc (MB)"),
    ("total_alloc_mb", "Total Alloc (MB)"),
    ("sys_mb", "System Memory (MB)"),
    ("workers_running", "Workers Running"),
    ("workers_idle", "Workers Idle"),
    ("workers_created", "Workers Created"),
    ("gc_pause_total_ms", "GC Pause Total (ms)"),
    ("gc_pause_avg_ms", "GC Pause Avg (ms)"),
    ("gc_cpu_pct", "GC CPU (%)"),
    ("cpu_pct", "CPU (%)"),
    ("gc_total", "GC Total Count"),
    ("goroutines", "Goroutines"),
    ("task_queue_len", "Task Queue Length"),
]

# Hook counters only exist when the run had a hook segment (mode != none);
# plot them when any row is non-zero, otherwise they would be flat zero lines.
HOOK_METRICS = [
    ("hook_submitted", "Hook Submitted"),
    ("hook_enqueued", "Hook Enqueued"),
    ("hook_started", "Hook Started"),
    ("hook_completed", "Hook Completed"),
]


def main():
    csv_files = sorted(glob.glob("metrics_*.csv"))
    if not csv_files:
        print("no metrics_*.csv files found")
        return

    for f in csv_files:
        name = os.path.splitext(f)[0]

        try:
            df = pd.read_csv(f)
        except Exception as e:  # malformed file
            print(f"  -> skip {f}: {e}")
            continue

        if df.empty:
            # A run that finished before the first sampling tick produces no
            # rows (the header is only written on the first tick).
            print(f"  -> skip {f}: empty file (run shorter than the sampling interval?)")
            continue

        metrics = list(METRICS)
        if df["hook_submitted"].sum() > 0:
            metrics += HOOK_METRICS
        metrics = [(col, title) for col, title in metrics if col in df.columns]
        if not metrics:
            print(f"  -> skip {f}: no valid metric columns")
            continue

        print(f"\n=== {name} === ({len(df)} rows)")

        n = len(metrics)
        ncols = min(3, n)
        nrows = (n + ncols - 1) // ncols
        fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows))
        fig.suptitle(name, fontsize=16, fontweight="bold")
        axes = [axes] if n == 1 else axes.flatten()

        for idx, (col, title) in enumerate(metrics):
            ax = axes[idx]
            df.plot(x="run_sec", y=[col], ax=ax, marker=".", linewidth=1.5, legend=False)
            ax.set_title(title, fontsize=12)
            ax.set_xlabel("Time (seconds)")
            ax.set_ylabel(title)
            ax.grid(True, alpha=0.3)

            # Annotate the last sampled value.
            last_time = df["run_sec"].iloc[-1]
            last_val = df[col].iloc[-1]
            ax.annotate(
                f"{last_val:.2f}",
                xy=(last_time, last_val),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=8,
                bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.3),
            )

        for idx in range(len(metrics), len(axes)):
            axes[idx].axis("off")

        plt.tight_layout()
        svg_file = f"{name}.svg"
        plt.savefig(svg_file, format="svg")
        plt.close()
        print(f"  -> {svg_file}")


if __name__ == "__main__":
    main()
