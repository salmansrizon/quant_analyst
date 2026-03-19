"""
definitions.py  —  Dagster Definitions for the LankaBD data lake pipeline.

Registers:
  • Assets   : available_sectors, lankabd_data_all_sectors  (from dataGrid.py)
  • Jobs     : datagrid_job, price_archive_job, announcement_job + sector variants
  • Schedules: daily chain  06:00 → 06:30 → 07:00 UTC
  • Sensor   : auto-convert CSVs to Parquet after each scraper job succeeds
  • Resources: FilesystemIOManager writing to /data/processed
"""

import os
import sys

# Ensure the scrapers folder is on the path when loaded by Dagster
sys.path.insert(0, os.path.dirname(__file__))

from dagster import (
    Definitions,
    ScheduleDefinition,
    FilesystemIOManager,
    define_asset_job,
    load_assets_from_modules,
    run_status_sensor,
    RunStatusSensorContext,
    DagsterRunStatus,
    RunRequest,
    job,
    op,
    OpExecutionContext,
)

import dataGrid
import priceArchive
import announcement

# ── Assets ────────────────────────────────────────────────────────────────────
datagrid_assets = load_assets_from_modules([dataGrid])

# ── Jobs ──────────────────────────────────────────────────────────────────────
datagrid_job        = define_asset_job("datagrid_job", selection="group:datagrid")
price_job           = priceArchive.price_archive_job
price_by_sector_job = priceArchive.price_archive_by_sector_job
ann_job             = announcement.announcement_job
ann_by_sector_job   = announcement.announcement_by_sector_job


@op(name="convert_csvs_to_parquet")
def convert_op(context: OpExecutionContext):
    """Convert new CSV files in /data/raw to Parquet in /data/processed."""
    import subprocess
    script = os.path.join(os.path.dirname(__file__), "csv_to_parquet.py")
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr)


@job(name="parquet_conversion_job")
def parquet_conversion_job():
    convert_op()


# ── Schedules ─────────────────────────────────────────────────────────────────
datagrid_daily = ScheduleDefinition(
    job=datagrid_job,
    cron_schedule="0 6 * * *",
    name="datagrid_daily",
    description="Refresh sector/symbol master at 06:00 UTC",
)

price_daily = ScheduleDefinition(
    job=price_job,
    cron_schedule="30 6 * * *",
    name="price_archive_daily",
    description="Scrape 3-year price history at 06:30 UTC",
)

announcement_daily = ScheduleDefinition(
    job=ann_job,
    cron_schedule="0 7 * * *",
    name="announcement_daily",
    description="Scrape market announcements at 07:00 UTC",
)

# ── Sensor: trigger Parquet conversion after any scraper job succeeds ─────────
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[price_job, price_by_sector_job, ann_job, ann_by_sector_job],
    name="csv_to_parquet_sensor",
    description="Runs Parquet conversion whenever a scraper job succeeds.",
    request_job=parquet_conversion_job,
)
def csv_to_parquet_sensor(context: RunStatusSensorContext):
    return RunRequest(run_key=context.dagster_run.run_id)


# ── IO Manager ────────────────────────────────────────────────────────────────
DATA_ROOT  = os.environ.get("DATA_ROOT", "/data")
io_manager = FilesystemIOManager(base_dir=os.path.join(DATA_ROOT, "processed"))

# ── Definitions ───────────────────────────────────────────────────────────────
defs = Definitions(
    assets=datagrid_assets,
    jobs=[
        datagrid_job,
        price_job,
        price_by_sector_job,
        ann_job,
        ann_by_sector_job,
        parquet_conversion_job,
    ],
    schedules=[datagrid_daily, price_daily, announcement_daily],
    sensors=[csv_to_parquet_sensor],
    resources={"io_manager": io_manager},
)
