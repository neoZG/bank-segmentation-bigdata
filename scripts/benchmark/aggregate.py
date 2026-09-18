"""Aggregates results/benchmark_results.csv into summary tables and
comparison charts (time + memory, per query, across libraries).

Run after the example_query.py scripts (and/or after merging in teammates'
own benchmark_results.csv rows from the shared Kaggle notebook) with:

    uv run python scripts/benchmark/aggregate.py
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
RESULTS_PATH = REPO_ROOT / "results" / "benchmark_results.csv"
PLOTS_DIR = REPO_ROOT / "results" / "plots"


def load_results() -> pd.DataFrame:
    if not RESULTS_PATH.exists():
        raise FileNotFoundError(
            f"No results yet at {RESULTS_PATH}. Run the example_query.py "
            "scripts (or import scripts/benchmark/harness.py into your own "
            "queries) first."
        )
    return pd.read_csv(RESULTS_PATH)


_ENV_COLORS = {"local": "#4C72B0", "cloud": "#DD8452", "cloud_distributed": "#55A868"}


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["query_id", "library", "environment"])[["seconds", "peak_rss_mb"]].agg(
        ["mean", "median", "count"]
    )


def plot_comparison(df: pd.DataFrame) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    for query_id, group in df.groupby("query_id"):
        avg = group.groupby(["library", "environment"])[["seconds", "peak_rss_mb"]].mean()

        for metric, ylabel, fname_suffix, title in [
            ("seconds", "seconds (lower is better)", "time", "Execution time"),
            ("peak_rss_mb", "peak RSS, MB (lower is better)", "memory", "Memory usage"),
        ]:
            pivot = avg[metric].unstack("environment").fillna(0)
            colors = [_ENV_COLORS.get(c, "#999999") for c in pivot.columns]
            fig, ax = plt.subplots()
            pivot.plot(kind="bar", ax=ax, color=colors)
            ax.set_ylabel(ylabel)
            ax.set_title(f"{title} — {query_id}")
            ax.legend(title="environment")
            fig.tight_layout()
            fig.savefig(PLOTS_DIR / f"{query_id}_{fname_suffix}.png")
            plt.close(fig)


def main() -> None:
    df = load_results()
    summary = summarize(df)
    print(summary.to_string())
    plot_comparison(df)
    print(f"\nPlots saved to {PLOTS_DIR}")


if __name__ == "__main__":
    main()
