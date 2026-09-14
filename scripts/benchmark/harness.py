"""Shared benchmark utility for comparing Polars/Dask/Modin/PySpark queries.

Pure stdlib + psutil, so this file can also be pasted as-is into the team's
shared Kaggle notebook if someone wants to instrument their own queries there
— just copy the resulting results/benchmark_results.csv rows back into this
repo so aggregate.py can chart everyone's numbers together.

Usage:
    from benchmark.harness import track

    with track("q1_groupby_location", "polars", "group by CustLocation"):
        result = df.group_by("CustLocation").agg(...)

Caveat: this measures the *driver* process only. Fine for this project's
local, single-machine setup — would need extending to capture distributed
worker memory if Dask/Spark ever ran against a remote cluster.
"""

from __future__ import annotations

import contextlib
import csv
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil

try:
    _DEFAULT_RESULTS_PATH = Path(__file__).resolve().parents[2] / "results" / "benchmark_results.csv"
except NameError:
    # __file__ is undefined when this code is pasted directly into a notebook
    # cell — fall back to a results/ folder next to the notebook's cwd.
    _DEFAULT_RESULTS_PATH = Path.cwd() / "results" / "benchmark_results.csv"

RESULTS_PATH = Path(os.environ.get("BENCHMARK_RESULTS_PATH", _DEFAULT_RESULTS_PATH))

_FIELDS = [
    "timestamp",
    "library",
    "query_id",
    "description",
    "seconds",
    "peak_rss_mb",
    "row_count_in",
    "row_count_out",
]


class _PeakMemorySampler:
    """Polls this process' RSS on a background thread so we catch a peak that
    a single before/after snapshot would miss (Dask/Spark do real work on
    worker threads inside this same process during .compute()/.collect())."""

    def __init__(self, interval: float = 0.05):
        self._interval = interval
        self._process = psutil.Process()
        self._peak_mb = self._process.memory_info().rss / 1e6
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._poll, daemon=True)

    def _poll(self) -> None:
        while not self._stop_event.is_set():
            rss_mb = self._process.memory_info().rss / 1e6
            self._peak_mb = max(self._peak_mb, rss_mb)
            self._stop_event.wait(self._interval)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> float:
        self._stop_event.set()
        self._thread.join(timeout=1)
        return self._peak_mb


@contextlib.contextmanager
def track(
    query_id: str,
    library: str,
    description: str = "",
    row_count_in: int | None = None,
    row_count_out: int | None = None,
):
    """Times a block of code and estimates its peak memory footprint, then
    appends one row to results/benchmark_results.csv."""
    sampler = _PeakMemorySampler()
    sampler.start()
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        peak_mb = sampler.stop()
        _append_result(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "library": library,
                "query_id": query_id,
                "description": description,
                "seconds": round(elapsed, 4),
                "peak_rss_mb": round(peak_mb, 2),
                "row_count_in": row_count_in if row_count_in is not None else "",
                "row_count_out": row_count_out if row_count_out is not None else "",
            }
        )


def _append_result(row: dict) -> None:
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    is_new = not RESULTS_PATH.exists()
    with RESULTS_PATH.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(row)
