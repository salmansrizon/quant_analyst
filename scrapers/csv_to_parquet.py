"""
csv_to_parquet.py — Convert raw CSV scraper output to partitioned Parquet.

Runs automatically after each scraper job via Dagster sensor, or manually:
    python csv_to_parquet.py
"""

import os
import glob
import shutil
import pandas as pd
from datetime import datetime
from pathlib import Path

DATA_ROOT  = os.environ.get("DATA_ROOT", "/data")
RAW_DIR    = os.path.join(DATA_ROOT, "raw")
PROC_DIR   = os.path.join(DATA_ROOT, "processed")
ARCH_DIR   = os.path.join(DATA_ROOT, "archive")

# CSV → output dataset name mapping
CSV_MAP = {
    "lankabd_data_all_sectors.csv":        "lankabd_data_all_sectors",
    "lankabd_price_archive_3years.csv":    "lankabd_price_archive_3years",
    "lankabd_announcements_3years.csv":    "lankabd_announcements_3years",
}


def csv_to_parquet(csv_path: str, dataset_name: str):
    today = datetime.utcnow().strftime("%Y%m%d")
    out_dir = os.path.join(PROC_DIR, dataset_name)
    arch_dir = os.path.join(ARCH_DIR, dataset_name)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    Path(arch_dir).mkdir(parents=True, exist_ok=True)

    print(f"Converting {csv_path} → {out_dir}/")
    df = pd.read_csv(csv_path, low_memory=False)

    # Basic cleaning
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.drop_duplicates()

    out_file   = os.path.join(out_dir, f"{today}.parquet")
    arch_file  = os.path.join(arch_dir, f"{today}.parquet")

    df.to_parquet(out_file,  index=False, engine="pyarrow", compression="snappy")
    df.to_parquet(arch_file, index=False, engine="pyarrow", compression="snappy")

    # Archive the raw CSV too
    raw_arch = os.path.join(ARCH_DIR, "csv")
    Path(raw_arch).mkdir(parents=True, exist_ok=True)
    shutil.copy2(csv_path, os.path.join(raw_arch, f"{dataset_name}_{today}.csv"))

    print(f"  ✓ {len(df):,} rows written → {out_file}")
    return df


def run_all():
    # Check /data/raw first, then working directory (for legacy local runs)
    search_dirs = [RAW_DIR, "."]
    for csv_name, dataset in CSV_MAP.items():
        found = False
        for d in search_dirs:
            path = os.path.join(d, csv_name)
            if os.path.exists(path):
                csv_to_parquet(path, dataset)
                found = True
                break
        if not found:
            print(f"  – {csv_name} not found, skipping")


if __name__ == "__main__":
    run_all()
