# Docker Setup — Hybrid Data Engineering Pipeline

This stack replaces the host installs of Java 17, Spark 3.5, Kafka 3.9,
Zookeeper, and PostgreSQL 16 from `SETUP_GUIDE.docx`. All those services
plus the Python apps run as containers; nothing except Docker Desktop is
required on your machine.

## What's running

| Service        | Image                     | Host port | Purpose                          |
| -------------- | ------------------------- | --------- | -------------------------------- |
| `zookeeper`    | `bitnami/zookeeper:3.9`   | 2181      | Coordination for Kafka           |
| `kafka`        | `bitnami/kafka:3.9`       | 9092      | Streaming broker                 |
| `kafka-init`   | `bitnami/kafka:3.9`       | —         | Creates `sales_topic` on startup |
| `postgres`     | `postgres:16`             | 5432      | Warehouse + analytics tables     |
| `spark-master` | `bitnami/spark:3.5`       | 7077,8080 | Optional Spark cluster master    |
| `spark-worker` | `bitnami/spark:3.5`       | —         | Optional Spark cluster worker    |
| `app`          | local build (`Dockerfile`)| —         | Idle shell for one-shot scripts  |
| `producer`     | local build               | —         | `streaming/kafka_producer.py`    |
| `streamer`     | local build               | —         | `streaming/spark_streaming.py`   |
| `dashboard`    | local build               | 8501      | Streamlit at http://localhost:8501 |

The `app`/`producer`/`streamer`/`dashboard` containers share a single
image built from `Dockerfile`: Python 3.11 + Java 17 (Temurin OpenJDK) +
`requirements.txt` + the PostgreSQL JDBC driver pre-downloaded to
`/opt/jars/postgresql-42.7.3.jar`.

## Prerequisites

- Docker Desktop (Windows / Mac) or Docker Engine + Compose v2 (Linux)
- A Kaggle API token at `%USERPROFILE%\.kaggle\kaggle.json`
  (only needed for `ingestion/load_dataset.py`)

## First-time build

```bash
docker compose build
```

This installs Python deps inside the image (~5 min the first time).

## Run order

### 1. Start the infrastructure

```bash
docker compose up -d zookeeper kafka postgres
```

Wait until `docker compose ps` shows them all `healthy`. The
`kafka-init` container will auto-run once and create `sales_topic`.

`create_tables.sql` is applied to the new database automatically on the
first launch of the postgres container.

### 2. Load the dataset (one-time)

```bash
docker compose run --rm app python ingestion/load_dataset.py
```

The CSV is written to `./data/ecommerce_sales.csv` on your host (bind
mount), so subsequent runs reuse it.

### 3. Spark batch (one-time)

```bash
docker compose run --rm app python batch/spark_batch.py
```

### 4. Streaming services + dashboard

```bash
docker compose up -d producer streamer dashboard
```

Open http://localhost:8501.

## Everyday commands

```bash
# Tail logs from any service
docker compose logs -f streamer

# psql into the database
docker compose exec postgres psql -U postgres -d ecommerce_pipeline

# Apply the window-function queries
docker compose exec -T postgres \
    psql -U postgres -d ecommerce_pipeline < sql/window_functions.sql

# Drop into the app container for ad-hoc Python
docker compose exec app bash

# Stop everything, keep data
docker compose stop

# Tear down completely, including volumes (data lost)
docker compose down -v
```

## Configuration

`config/settings.py` reads these env vars (defaults in parens):

| Var               | In-container value         | Host default                          |
| ----------------- | -------------------------- | ------------------------------------- |
| `DB_HOST`         | `postgres`                 | `localhost`                           |
| `DB_PORT`         | `5432`                     | `5432`                                |
| `DB_NAME`         | `ecommerce_pipeline`       | `ecommerce_pipeline`                  |
| `DB_USER`         | `postgres`                 | `postgres`                            |
| `DB_PASSWORD`     | `murtazamuhammad7`         | `murtazamuhammad7`                    |
| `KAFKA_BOOTSTRAP` | `kafka:29092`              | `localhost:9092`                      |
| `KAFKA_TOPIC`     | `sales_topic`              | `sales_topic`                         |
| `SPARK_MASTER`    | `local[*]`                 | `local[*]`                            |
| `JDBC_JAR_PATH`   | `/opt/jars/postgresql-42.7.3.jar` | `/opt/spark/jars/postgresql-42.7.3.jar` |

To use the Spark cluster instead of in-process `local[*]`, set
`SPARK_MASTER=spark://spark-master:7077` on the relevant app service.

To point at a non-default Kaggle credentials folder, create a `.env`
next to `docker-compose.yml`:

```env
KAGGLE_DIR=C:/Users/YourName/.kaggle
```

## Verifying it works

```bash
# Postgres tables present
docker compose exec postgres \
    psql -U postgres -d ecommerce_pipeline -c "\dt"

# Kafka topic present
docker compose exec kafka kafka-topics.sh \
    --bootstrap-server kafka:29092 --list

# Spark master UI
open http://localhost:8080

# Dashboard
open http://localhost:8501
```

## Mapping to SETUP_GUIDE.docx

| SETUP_GUIDE section          | Docker equivalent                                 |
| ---------------------------- | ------------------------------------------------- |
| §1 PostgreSQL install        | `postgres` service + auto-loaded `create_tables.sql` |
| §2 Java 17                   | Baked into the `app` image (Temurin OpenJDK 17)   |
| §3 Spark 3.5                 | `spark-master` + `spark-worker` (or `pyspark` local mode in `app`) |
| §3.4 JDBC driver             | Pre-downloaded to `/opt/jars/postgresql-42.7.3.jar` |
| §3.5 `getSubject` workaround | Set as `SPARK_SUBMIT_OPTS` on all app services    |
| §4 winutils                  | Not needed — containers run Linux                 |
| §5 Kafka 3.9                 | `kafka` + `zookeeper` services                    |
| §5.4 `log.dirs` Windows fix  | Not needed — containers run Linux                 |
| §6 Python deps               | `Dockerfile` runs `pip install -r requirements.txt` |
| §8 Run order                 | See "Run order" above                             |

Everything in §9 (Windows env vars) and §10 (winutils / Java 21 troubleshooting)
is obsolete in the containerised setup.
