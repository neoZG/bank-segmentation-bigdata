"""Control query — see scripts/polars/example_query.py for the shared
rationale. Same logical query, Modin implementation (Dask engine).

Run with:
    uv run python scripts/modin/example_query.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# Modin's default Dask engine spins up one worker per CPU core, which is
# noisy (and slow to start) on a many-core machine — cap it for a laptop-
# scale benchmark. Must be set before modin.pandas is imported.
os.environ.setdefault("MODIN_CPUS", "4")

import modin.pandas as pd  # noqa: E402

# distributed configures its own loggers on import, so silence it afterwards —
# this is cosmetic cluster-teardown chatter, not a real failure.
for _name in ("distributed", "distributed.scheduler", "distributed.worker", "distributed.nanny"):
    logging.getLogger(_name).setLevel(logging.CRITICAL)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark.harness import track  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = REPO_ROOT / "data" / "raw" / "bank_transactions.csv"
AMOUNT_COL = "TransactionAmount (INR)"


def main() -> None:
    with track("control_groupby_location", "modin", "filter>0, group by CustLocation, agg, sort"):
        df = pd.read_csv(DATA_PATH)
        filtered = df[df[AMOUNT_COL] > 0]
        result = (
            filtered.groupby("CustLocation")[AMOUNT_COL]
            .agg(txn_count="count", total_amount="sum", avg_amount="mean")
            .sort_values("total_amount", ascending=False)
        )
    print(result.head(10))


if __name__ == "__main__":
    main()
