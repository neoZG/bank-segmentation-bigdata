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


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["query_id", "library"])[["seconds", "peak_rss_mb"]].agg(
        ["mean", "median", "count"]
    )


def plot_comparison(df: pd.DataFrame) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    for query_id, group in df.groupby("query_id"):
        avg = group.groupby("library")[["seconds", "peak_rss_mb"]].mean()

        fig, ax = plt.subplots()
        avg["seconds"].plot(kind="bar", ax=ax, color="#4C72B0")
        ax.set_ylabel("seconds (lower is better)")
        ax.set_title(f"Execution time — {query_id}")
        fig.tight_layout()
        fig.savefig(PLOTS_DIR / f"{query_id}_time.png")
        plt.close(fig)

        fig, ax = plt.subplots()
        avg["peak_rss_mb"].plot(kind="bar", ax=ax, color="#DD8452")
        ax.set_ylabel("peak RSS, MB (lower is better)")
        ax.set_title(f"Memory usage — {query_id}")
        fig.tight_layout()
        fig.savefig(PLOTS_DIR / f"{query_id}_memory.png")
        plt.close(fig)


def main() -> None:
    df = load_results()
    summary = summarize(df)
    print(summary.to_string())
    plot_comparison(df)
    print(f"\nPlots saved to {PLOTS_DIR}")


if __name__ == "__main__":
    main()
