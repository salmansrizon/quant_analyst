# ── Stage 1: base ─────────────────────────────────────────────────────────────
FROM python:3.12-slim AS base

WORKDIR /opt/scrapers

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libxml2-dev \
        libxslt1-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ── Stage 2: Python dependencies ──────────────────────────────────────────────
FROM base AS deps

COPY scrapers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Stage 3: runtime ──────────────────────────────────────────────────────────
FROM deps AS runtime

# Copy scraper source
COPY scrapers/ /opt/scrapers/

# Embed dagster.yaml — no volume mount needed on Render
COPY dagster_home/dagster.yaml /dagster_home/dagster.yaml

# Data lake directories
RUN mkdir -p /data/raw /data/processed /data/archive /data/logs /dagster_home

ENV DAGSTER_HOME=/dagster_home
ENV PYTHONPATH=/opt/scrapers

# Render injects $PORT — default 10000 for local fallback
EXPOSE 10000

# startCommand in render.yaml overrides this — used only for local docker run
CMD ["sh", "-c", "dagster-webserver -h 0.0.0.0 -p ${PORT:-10000} -m definitions"]
