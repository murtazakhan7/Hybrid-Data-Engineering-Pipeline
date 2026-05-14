# ─────────────────────────────────────────────────────────────
# Image used by every Python service in docker-compose.yml.
# Bundles Java 17 (Temurin) + Python 3.11 + project requirements
# + the PostgreSQL JDBC driver Spark needs to talk to Postgres.
# ─────────────────────────────────────────────────────────────
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/workspace

# Java 17 (required by PySpark 3.5; matches SETUP_GUIDE.docx pin).
# procps gives `ps` which Spark workers shell out to.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        ca-certificates \
        procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# PostgreSQL JDBC driver — version pinned to match SETUP_GUIDE.docx.
RUN mkdir -p /opt/jars \
 && curl -fsSL -o /opt/jars/postgresql-42.7.3.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Python deps. requirements.txt is copied separately so this layer is
# cached when only application code changes.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip \
 && pip install -r /tmp/requirements.txt

WORKDIR /workspace

# Project code is bind-mounted at runtime via docker-compose, so we
# don't COPY it here — keeps rebuilds fast while editing locally.
