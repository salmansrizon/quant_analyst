# LankaBD Data Lake

Scrapes LankaBD market data (sectors, prices, announcements), stores it as
Parquet in a data lake, and exposes it via Apache Superset dashboards.
Orchestrated by Dagster with daily schedules.

---

## Architecture

```
lankabd.com
     │
     ▼
┌──────────────────────────────────────┐
│  Scrapers (Python)                   │
│  dataGrid.py → priceArchive.py →     │
│  announcement.py → csv_to_parquet.py │
└──────────────┬───────────────────────┘
               │
               ▼
         /data (Parquet)
               │
       ┌───────┴────────┐
       ▼                ▼
  pg_loader.py      lake_query.py
  (Postgres)        (DuckDB shell)
       │
       ▼
  lake.* tables
       │
       ▼
  Apache Superset
```

---

## Local Development

```bash
cp .env.example .env
docker compose build
docker compose up -d

# Run scrapers manually
docker compose run --rm scraper python dataGrid.py
docker compose run --rm scraper python priceArchive.py
docker compose run --rm scraper python announcement.py
docker compose run --rm scraper python csv_to_parquet.py
docker compose run --rm scraper python pg_loader.py

# Open UIs
# Dagster:  http://localhost:3000
# Superset: http://localhost:8088  (admin / admin123)
```

---

## Render Deployment

### 1. Push to GitHub

```bash
git init && git add . && git commit -m "initial"
git remote add origin https://github.com/YOUR_USER/lankabd_datalake.git
git push -u origin main
```

### 2. Deploy Blueprint

```
render.com → New → Blueprint → connect repo
Render reads render.yaml and creates all services automatically
```

### 3. Set secret env vars in Render dashboard

```
lankabd-superset service → Environment → Add:
  SUPERSET_ADMIN_PASSWORD = your_secure_password
```

### 4. Trigger first scraper run

```
Render dashboard → lankabd-scraper-daily → Trigger Run
```

### 5. Connect Superset to lake data

```
https://lankabd-superset.onrender.com → Login
Settings → Database Connections → + Database
  Type:     PostgreSQL
  Host:     (from lankabd-postgres Internal URL in Render dashboard)
  Port:     5432
  Database: dagster
  Schema:   lake
  Username: superset_reader
  Password: superset_reader_pass
```

---

## Service URLs

| Service | Local | Render |
|---|---|---|
| Dagster | http://localhost:3000 | https://lankabd-dagster.onrender.com |
| Superset | http://localhost:8088 | https://lankabd-superset.onrender.com |

---

## Render Cost Breakdown

| Service | Plan | Cost |
|---|---|---|
| lankabd-dagster (webserver) | Free | $0 |
| lankabd-dagster-daemon | Starter | $7/mo |
| lankabd-superset | Starter | $7/mo |
| lankabd-postgres | Free (90 days) | $0 → $7/mo |
| lankabd-superset-db | Free (90 days) | $0 → $7/mo |
| lankabd-scraper-daily (cron) | Starter | ~$1/mo |
| **Total** | | **~$15/mo** |

---

## Monitoring (free)

```
uptimerobot.com → New Monitor × 2
  https://lankabd-dagster.onrender.com/server_info
  https://lankabd-superset.onrender.com/health
  Interval: 5 minutes → email alert on down
```

---

## Data Lake Layout

```
/data/
├── raw/                          CSV files from scrapers
├── processed/
│   ├── lankabd_data_all_sectors/    YYYYMMDD.parquet
│   ├── lankabd_price_archive_3years/ YYYYMMDD.parquet
│   └── lankabd_announcements_3years/ YYYYMMDD.parquet
├── archive/                      dated snapshots
└── logs/                         Dagster compute logs
```

---

## DuckDB Query Shell (local only)

```bash
docker compose run --rm scraper python lake_query.py --shell

# Example queries:
# SELECT sector, COUNT(*) FROM sectors GROUP BY 1 ORDER BY 2 DESC;
# SELECT symbol, AVG(close) FROM prices GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
# .quit
```
