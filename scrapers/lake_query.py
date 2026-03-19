"""
lake_query.py — Run ad-hoc DuckDB queries against the data lake.

Usage (inside the container or locally with DATA_ROOT set):
    python lake_query.py "SELECT sector, COUNT(*) FROM read_parquet('/data/processed/lankabd_data_all_sectors/*.parquet') GROUP BY 1"

Or interactively:
    python lake_query.py --shell
"""

import os
import sys
import argparse
import duckdb

DATA_ROOT = os.environ.get("DATA_ROOT", "/data")


def get_conn():
    db_path = os.path.join(DATA_ROOT, "lake.duckdb")
    conn = duckdb.connect(db_path)
    # Register convenient views over Parquet files
    processed = os.path.join(DATA_ROOT, "processed")
    views = {
        "sectors":       f"{processed}/lankabd_data_all_sectors/*.parquet",
        "prices":        f"{processed}/lankabd_price_archive_3years/*.parquet",
        "announcements": f"{processed}/lankabd_announcements_3years/*.parquet",
    }
    for name, path in views.items():
        try:
            conn.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{path}', union_by_name=true)")
        except duckdb.IOException:
            pass  # file not yet produced – view will be empty
    return conn


def run_query(sql: str):
    conn = get_conn()
    result = conn.execute(sql).df()
    print(result.to_string(index=False))
    conn.close()


def interactive_shell():
    conn = get_conn()
    print("DuckDB shell — type .quit to exit, .tables to list views\n")
    while True:
        try:
            sql = input("duckdb> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not sql:
            continue
        if sql in (".quit", "quit", "exit"):
            break
        if sql == ".tables":
            sql = "SHOW TABLES"
        try:
            result = conn.execute(sql).df()
            print(result.to_string(index=False), "\n")
        except Exception as e:
            print(f"Error: {e}\n")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query the LankaBD data lake via DuckDB")
    parser.add_argument("query", nargs="?", help="SQL query to run")
    parser.add_argument("--shell", action="store_true", help="Start interactive shell")
    args = parser.parse_args()

    if args.shell:
        interactive_shell()
    elif args.query:
        run_query(args.query)
    else:
        parser.print_help()
