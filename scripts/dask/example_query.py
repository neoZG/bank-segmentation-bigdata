"""Control query — see scripts/polars/example_query.py for the shared
rationale. Same logical query, Dask implementation.

Run with:
    uv run python scripts/dask/example_query.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import dask.dataframe as dd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark.harness import track  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = REPO_ROOT / "data" / "raw" / "bank_transactions.csv"
AMOUNT_COL = "TransactionAmount (INR)"


def main() -> None:
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


if __name__ == "__main__":
    main()
