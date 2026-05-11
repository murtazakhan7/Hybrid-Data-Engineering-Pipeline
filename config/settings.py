# config/settings.py
# ─────────────────────────────────────────────
# Central configuration for the entire pipeline
# ─────────────────────────────────────────────

# ── PostgreSQL ────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "ecommerce_pipeline",
    "user":     "postgres",
    "password": "murtazamuhammad7",       # ← change to your password
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
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "sales_topic"

# ── Spark ─────────────────────────────────────
SPARK_APP_NAME      = "EcommercePipeline"
SPARK_MASTER        = "local[*]"
# Path to PostgreSQL JDBC jar (download from https://jdbc.postgresql.org/)
JDBC_JAR_PATH       = "/opt/spark/jars/postgresql-42.7.3.jar"
# Path to Spark-Kafka connector jar
KAFKA_SPARK_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# ── Dataset ───────────────────────────────────
DATASET_KAGGLE_ID   = "miadul/e-commerce-sales-transactions-dataset"
DATASET_LOCAL_PATH  = "data/ecommerce_sales.csv"

# ── Streaming ─────────────────────────────────
PRODUCER_DELAY_SEC  = 1      # seconds between Kafka messages
STREAM_TRIGGER_SEC  = 10     # Spark micro-batch trigger interval
STREAM_CHECKPOINT   = "/tmp/spark_checkpoint"
STREAM_WINDOW_MIN   = 1      # window size in minutes
