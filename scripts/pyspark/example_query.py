"""Control query — see scripts/polars/example_query.py for the shared
rationale. Same logical query, PySpark implementation.

Needs a local JRE installed (e.g. `sudo pacman -S jre-openjdk-headless`).

Run with:
    uv run python scripts/pyspark/example_query.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Spark 4.x doesn't yet support brand-new JDKs (tested: breaks on JDK 26) —
# pin to JDK 21 explicitly rather than relying on the system default so this
# doesn't silently break again on a machine with a newer default JDK.
_JAVA_21_CANDIDATES = ["/usr/lib/jvm/java-21-openjdk"]
for _candidate in _JAVA_21_CANDIDATES:
    if Path(_candidate).exists():
        os.environ["JAVA_HOME"] = _candidate
        break

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark.harness import track  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = os.environ.get(
    "BENCHMARK_DATA_PATH",
    str(REPO_ROOT / "data" / "raw" / "bank_transactions.csv"),
)
SPARK_MASTER = os.environ.get("BENCHMARK_SPARK_MASTER", "local[*]")
AMOUNT_COL = "TransactionAmount (INR)"


def main() -> None:
    spark = (
        SparkSession.builder.appName("bank-segmentation-control-query")
        .master(SPARK_MASTER)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    with track("control_groupby_location", "pyspark", "filter>0, group by CustLocation, agg, sort"):
        df = spark.read.csv(str(DATA_PATH), header=True, inferSchema=True)
        result = (
            df.filter(F.col(AMOUNT_COL) > 0)
            .groupBy("CustLocation")
            .agg(
                F.count("*").alias("txn_count"),
                F.sum(AMOUNT_COL).alias("total_amount"),
                F.avg(AMOUNT_COL).alias("avg_amount"),
            )
            .orderBy(F.col("total_amount").desc())
        )
        result_pd = result.toPandas()  # forces execution, mirrors .compute()

    print(result_pd.head(10))
    spark.stop()


if __name__ == "__main__":
    main()
