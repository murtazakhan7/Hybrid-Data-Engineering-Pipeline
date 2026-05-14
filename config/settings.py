# config/settings.py
# ─────────────────────────────────────────────
# Central configuration for the entire pipeline
# ─────────────────────────────────────────────
# Every value can be overridden by an environment variable so the same
# code runs both on a developer's host (defaults below) and inside a
# Docker container (compose injects DB_HOST=postgres, KAFKA_BOOTSTRAP=
# kafka:29092, JDBC_JAR_PATH=/opt/jars/..., etc).
import os

# ── PostgreSQL ────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "port":     int(os.environ.get("DB_PORT", "5432")),
    "database": os.environ.get("DB_NAME",     "ecommerce_pipeline"),
    "user":     os.environ.get("DB_USER",     "postgres"),
    "password": os.environ.get("DB_PASSWORD", "murtazamuhammad7"),
}

# JDBC URL used by PySpark
JDBC_URL = (
    f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}"
    f"/{DB_CONFIG['database']}"
)
JDBC_PROPS = {
    "user":     DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver":   "org.postgresql.Driver",
}

# ── Kafka ─────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC",     "sales_topic")

# ── Spark ─────────────────────────────────────
SPARK_APP_NAME      = os.environ.get("SPARK_APP_NAME", "EcommercePipeline")
SPARK_MASTER        = os.environ.get("SPARK_MASTER",   "local[*]")
# Path to PostgreSQL JDBC jar (download from https://jdbc.postgresql.org/)
JDBC_JAR_PATH       = os.environ.get(
    "JDBC_JAR_PATH",
    "/opt/spark/jars/postgresql-42.7.3.jar",
)
# Path to Spark-Kafka connector jar
KAFKA_SPARK_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# ── Dataset ───────────────────────────────────
DATASET_KAGGLE_ID   = "miadul/e-commerce-sales-transactions-dataset"
DATASET_LOCAL_PATH  = "data/ecommerce_sales.csv"

# ── Streaming ─────────────────────────────────
PRODUCER_DELAY_SEC  = 1      # seconds between Kafka messages
STREAM_TRIGGER_SEC  = 10     # Spark micro-batch trigger interval
STREAM_CHECKPOINT   = os.environ.get("STREAM_CHECKPOINT", "/tmp/spark_checkpoint")
STREAM_WINDOW_MIN   = 1      # window size in minutes
