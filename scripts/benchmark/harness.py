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
worker memory if Dask/Spark ever ran against a remote cluster. As of the
local-vs-cloud comparison, each row is tagged via the `environment` field
(default "local", override with BENCHMARK_ENVIRONMENT=cloud or
cloud_distributed); a "cloud_distributed" row (PySpark on YARN) still only
measures the driver's memory while real computation happens on executors
elsewhere, so its peak_rss_mb isn't comparable to the other environments —
only its wall-clock seconds is.
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
    "environment",
    "query_id",
    "description",
    "seconds",
    "cpu_time_s",
    "peak_rss_mb",
    "net_recv_mb",
    "row_count_in",
    "row_count_out",
]


def _tree_rss_mb(process: psutil.Process) -> float:
    """RSS of this process plus its children. PySpark runs the actual work in
    a child JVM, so measuring only this process would report just the thin
    Python wrapper (~130MB) instead of the memory really being used."""
    total = process.memory_info().rss
    for child in process.children(recursive=True):
        try:
            total += child.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total / 1e6


def _tree_cpu_s(process: psutil.Process) -> float:
    """CPU time (user+system) of this process plus its children, same reason."""
    total = sum(process.cpu_times()[:2])
    for child in process.children(recursive=True):
        try:
            total += sum(child.cpu_times()[:2])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total


class _PeakMemorySampler:
    """Polls the process tree's RSS on a background thread so we catch a peak
    that a single before/after snapshot would miss (Dask/Spark do real work on
    worker threads inside this same process during .compute()/.collect())."""

    def __init__(self, interval: float = 0.005):
        self._interval = interval
        self._process = psutil.Process()
        self._peak_mb = _tree_rss_mb(self._process)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._poll, daemon=True)

    def _poll(self) -> None:
        while not self._stop_event.is_set():
            self._peak_mb = max(self._peak_mb, _tree_rss_mb(self._process))
            self._stop_event.wait(self._interval)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> float:
        self._stop_event.set()
        self._thread.join(timeout=1)
        # Final reading taken synchronously: an operation that finishes faster
        # than the polling interval would otherwise report only its starting
        # baseline, never its actual peak.
        self._peak_mb = max(self._peak_mb, _tree_rss_mb(self._process))
        return self._peak_mb


@contextlib.contextmanager
def track(
    query_id: str,
    library: str,
    description: str = "",
    environment: str | None = None,
    row_count_in: int | None = None,
    row_count_out: int | None = None,
):
    """Times a block of code and estimates its peak memory footprint, then
    appends one row to results/benchmark_results.csv."""
    env = environment if environment is not None else os.environ.get("BENCHMARK_ENVIRONMENT", "local")
    process = psutil.Process()
    sampler = _PeakMemorySampler()
    sampler.start()
    # CPU time (user+system) shows whether the block was actually computing or
    # just waiting on I/O: cpu_time much lower than wall time means waiting.
    cpu_before = _tree_cpu_s(process)
    # System-wide counter — on a dedicated VM it's effectively this process'
    # traffic, but on a laptop other applications can inflate it.
    net_before = psutil.net_io_counters().bytes_recv
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        cpu_elapsed = _tree_cpu_s(process) - cpu_before
        net_recv_mb = (psutil.net_io_counters().bytes_recv - net_before) / 1e6
        peak_mb = sampler.stop()
        _append_result(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "library": library,
                "environment": env,
                "query_id": query_id,
                "description": description,
                "seconds": round(elapsed, 4),
                "cpu_time_s": round(cpu_elapsed, 4),
                "peak_rss_mb": round(peak_mb, 2),
                "net_recv_mb": round(net_recv_mb, 2),
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
