"""Downloads the Kaggle bank transactions dataset into data/raw/.

Requires Kaggle credentials, in any of the forms the `kaggle` CLI accepts:
- ~/.kaggle/kaggle.json (older key-file style), or
- ~/.kaggle/access_token (newer single-token style), or
- the KAGGLE_API_TOKEN environment variable.
Generate one at kaggle.com -> Account -> "Create New API Token".

Run with:
    uv run python scripts/download_dataset.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"
DATASET = "shivamb/bank-customer-segmentation"
EXPECTED_NAME = "bank_transactions.csv"


def main() -> None:
    kaggle_dir = Path.home() / ".kaggle"
    has_credentials = (
        (kaggle_dir / "kaggle.json").exists()
        or (kaggle_dir / "access_token").exists()
        or bool(os.environ.get("KAGGLE_API_TOKEN"))
    )
    if not has_credentials:
        sys.exit(
            "No Kaggle credentials found. Generate one at kaggle.com -> "
            f"Account -> 'Create New API Token', then save it to "
            f"{kaggle_dir / 'kaggle.json'} or {kaggle_dir / 'access_token'} "
            "(chmod 600 either way), or export KAGGLE_API_TOKEN."
        )

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["kaggle", "datasets", "download", "-d", DATASET, "-p", str(RAW_DIR), "--unzip"],
        check=True,
    )

    csv_files = sorted(RAW_DIR.glob("*.csv"))
    if not csv_files:
        sys.exit("Download finished but no CSV file was found in data/raw/.")

    target = RAW_DIR / EXPECTED_NAME
    if len(csv_files) == 1 and csv_files[0] != target:
        csv_files[0].rename(target)
        csv_files = [target]

    for f in csv_files:
        with f.open() as fh:
            row_count = sum(1 for _ in fh) - 1  # minus header
        print(f"{f.name}: {row_count:,} rows")


if __name__ == "__main__":
    main()
