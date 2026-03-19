"""
pg_loader.py — Load Parquet files from the data lake into Postgres lake schema.

Works both locally and on Render. Render injects DATABASE_URL automatically.

Run after scrapers:
    docker compose run --rm scraper python pg_loader.py        # local
    render.com → lankabd-scraper-daily → Trigger Run          # Render
"""

import os
import glob
import duckdb
from sqlalchemy import create_engine, text
from datetime import datetime

DATA_ROOT = os.environ.get("DATA_ROOT", "/data")

# ── Build Postgres connection URL ─────────────────────────────────────────────
# Render provides DATABASE_URL — local uses individual DAGSTER_PG_* vars
def get_pg_url():
    url = os.environ.get("DATABASE_URL", "")
    if url:
        # Render sometimes uses postgres:// — SQLAlchemy needs postgresql+psycopg2://
        return url.replace("postgres://", "postgresql+psycopg2://").replace(
            "postgresql://", "postgresql+psycopg2://"
        )
    # Fall back to individual env vars (local Docker)
    host = os.environ.get("DAGSTER_PG_HOST", "postgres")
    user = os.environ.get("DAGSTER_PG_USER", "dagster")
    pwd  = os.environ.get("DAGSTER_PG_PASSWORD", "dagster_secret")
    db   = os.environ.get("DAGSTER_PG_DB", "dagster")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:5432/{db}"

# Dataset → Parquet directory mapping
DATASETS = {
    "lake.sectors":       f"{DATA_ROOT}/processed/lankabd_data_all_sectors",
    "lake.prices":        f"{DATA_ROOT}/processed/lankabd_price_archive_3years",
    "lake.announcements": f"{DATA_ROOT}/processed/lankabd_announcements_3years",
}


def load_dataset(table: str, parquet_dir: str, engine):
    files = sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))
    if not files:
        print(f"  – No Parquet files at {parquet_dir}, skipping {table}")
        return 0

    print(f"  Loading {len(files)} file(s) → {table} ...")
    con = duckdb.connect()
    df = con.execute(
        f"SELECT * FROM read_parquet({files!r}, union_by_name=true)"
    ).df()
    con.close()

    # Normalise column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    schema, tbl = table.split(".")
    df.to_sql(
        tbl, engine,
        schema=schema,
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi",
    )
    print(f"  ✓ {len(df):,} rows → {table}")
    return len(df)


def run():
    print(f"\n{'='*60}")
    print(f"  pg_loader  {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'='*60}")

    engine = create_engine(get_pg_url())

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS lake"))
        # Create superset_reader if it doesn't exist
        conn.execute(text("""
            DO $$
            BEGIN
              IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'superset_reader') THEN
                CREATE ROLE superset_reader LOGIN PASSWORD 'superset_reader_pass';
              END IF;
            END $$
        """))
        conn.execute(text("GRANT CONNECT ON DATABASE dagster TO superset_reader"))
        conn.execute(text("GRANT USAGE ON SCHEMA lake TO superset_reader"))

    total = 0
    for table, parquet_dir in DATASETS.items():
        total += load_dataset(table, parquet_dir, engine)

    # Refresh read grants after table replace
    with engine.begin() as conn:
        conn.execute(text(
            "GRANT SELECT ON ALL TABLES IN SCHEMA lake TO superset_reader"
        ))

    print(f"\n  Total rows loaded: {total:,}")
    print(f"  Superset connection:")
    print(f"    Host:     <your-postgres-host>")
    print(f"    DB:       dagster")
    print(f"    Schema:   lake")
    print(f"    User:     superset_reader")
    print(f"    Password: superset_reader_pass")
    engine.dispose()


if __name__ == "__main__":
    run()
