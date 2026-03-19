import os

# ── Security ──────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change_me_in_production")

# ── Superset metadata DB ──────────────────────────────────────────────────────
# Render injects DATABASE_URL from the linked superset-db
_db_url = os.environ.get("DATABASE_URL", "")
# SQLAlchemy requires postgresql+psycopg2:// dialect prefix
SQLALCHEMY_DATABASE_URI = _db_url.replace(
    "postgresql://", "postgresql+psycopg2://"
).replace(
    "postgres://", "postgresql+psycopg2://"
)

# ── Feature flags ─────────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DRILL_TO_DETAIL": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ALERT_REPORTS": True,
}

# ── Security (relax for dev — tighten for production) ─────────────────────────
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False

# ── SQL Lab ───────────────────────────────────────────────────────────────────
SUPERSET_WEBSERVER_TIMEOUT = 300
SQL_MAX_ROW = 100000
DISPLAY_MAX_ROW = 10000

# ── Cache ─────────────────────────────────────────────────────────────────────
CACHE_CONFIG      = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 600}
