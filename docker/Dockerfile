# ── Stage 1: base ─────────────────────────────────────────────────────────────
FROM python:3.12-slim AS base

WORKDIR /opt/scrapers

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libxml2-dev \
        libxslt1-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ── Stage 2: Python dependencies ──────────────────────────────────────────────
FROM base AS deps

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Stage 3: runtime ──────────────────────────────────────────────────────────
FROM deps AS runtime

# Copy all scraper source
COPY . /opt/scrapers/

# Copy dagster.yaml into the image for Render (no volume mounts on Render)
COPY ../dagster_home/dagster.yaml /dagster_home/dagster.yaml

# Data lake directories — on Render these live on the persistent disk at /data
RUN mkdir -p /data/raw /data/processed /data/archive /data/logs /dagster_home

ENV DAGSTER_HOME=/dagster_home
ENV PYTHONPATH=/opt/scrapers

# Render injects PORT env var — default to 10000
EXPOSE 10000

# Default: Dagster webserver — overridden per service in render.yaml
CMD ["sh", "-c", "dagster-webserver -h 0.0.0.0 -p ${PORT:-10000} -m definitions"]
