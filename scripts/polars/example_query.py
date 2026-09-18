"""Control query — run identically in polars/dask/modin/pyspark so Neo's
benchmark comparison has at least one true apples-to-apples data point,
independent of whatever the 10-query sets end up covering.

Query: filter TransactionAmount (INR) > 0, group by CustLocation, compute
count/sum/mean of TransactionAmount (INR), sort by total desc.

Run with:
    uv run python scripts/polars/example_query.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark.harness import track  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = os.environ.get(
    "BENCHMARK_DATA_PATH",
    str(REPO_ROOT / "data" / "raw" / "bank_transactions.csv"),
)
AMOUNT_COL = "TransactionAmount (INR)"


def main() -> None:
    with track("control_groupby_location", "polars", "filter>0, group by CustLocation, agg, sort"):
        df = pl.read_csv(DATA_PATH)
        result = (
            df.filter(pl.col(AMOUNT_COL) > 0)
            .group_by("CustLocation")
            .agg(
                pl.len().alias("txn_count"),
                pl.col(AMOUNT_COL).sum().alias("total_amount"),
                pl.col(AMOUNT_COL).mean().alias("avg_amount"),
            )
            .sort("total_amount", descending=True)
        )
    print(result.head(10))


if __name__ == "__main__":
    main()
