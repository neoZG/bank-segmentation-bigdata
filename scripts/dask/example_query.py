"""Control query — see scripts/polars/example_query.py for the shared
rationale. Same logical query, Dask implementation.

Run with:
    uv run python scripts/dask/example_query.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import dask.dataframe as dd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark.harness import track  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = os.environ.get(
    "BENCHMARK_DATA_PATH",
    str(REPO_ROOT / "data" / "raw" / "bank_transactions.csv"),
)
# When set (e.g. "tcp://10.128.0.3:8786"), connect to a real multi-machine
# Dask cluster instead of the default single-machine threaded scheduler.
DASK_SCHEDULER = os.environ.get("BENCHMARK_DASK_SCHEDULER")
AMOUNT_COL = "TransactionAmount (INR)"


def main() -> None:
    client = None
    if DASK_SCHEDULER:
        from dask.distributed import Client

        client = Client(DASK_SCHEDULER)
        print(f"Dask cluster: {client.dashboard_link} | workers: {len(client.scheduler_info()['workers'])}")

    if os.environ.get("BENCHMARK_MODE") == "read":
        # dd.read_csv is lazy (it only builds a plan), so .compute() is needed
        # to actually force the data to be read.
        with track("read_csv", "dask", "read full CSV into memory"):
            df = dd.read_csv(DATA_PATH).compute()
        print(f"rows: {len(df)}")
        if client is not None:
            client.close()
        return

    with track("control_groupby_location", "dask", "filter>0, group by CustLocation, agg, sort"):
        ddf = dd.read_csv(DATA_PATH)
        filtered = ddf[ddf[AMOUNT_COL] > 0]
        result = (
            filtered.groupby("CustLocation")[AMOUNT_COL]
            .agg(["count", "sum", "mean"])
            .compute()
            .rename(columns={"count": "txn_count", "sum": "total_amount", "mean": "avg_amount"})
            .sort_values("total_amount", ascending=False)
        )
    print(result.head(10))

    if client is not None:
        client.close()


if __name__ == "__main__":
    main()
