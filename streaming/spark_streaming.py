# streaming/spark_streaming.py
# ─────────────────────────────────────────────────────────────────────
# Spark Structured Streaming Consumer
#   1. Reads JSON messages from Kafka (sales_topic)
#   2. Parses and transforms the stream
#   3. Computes 1-minute tumbling window aggregations
#   4. Writes results to PostgreSQL (stream_sales + stream_window_agg)
# ─────────────────────────────────────────────────────────────────────

import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType,
)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    SPARK_APP_NAME, SPARK_MASTER,
    KAFKA_BOOTSTRAP, KAFKA_TOPIC, KAFKA_SPARK_PACKAGE,
    JDBC_URL, JDBC_PROPS, JDBC_JAR_PATH,
    STREAM_TRIGGER_SEC, STREAM_CHECKPOINT, STREAM_WINDOW_MIN,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ── Schema matching Kafka JSON payload ───────────────────────────────
SALES_SCHEMA = StructType([
    StructField("order_id",      StringType(),    True),
    StructField("customer_id",   StringType(),    True),
    StructField("product_id",    StringType(),    True),
    StructField("category",      StringType(),    True),
    StructField("price",         DoubleType(),    True),
    StructField("quantity",      IntegerType(),   True),
    StructField("discount",      DoubleType(),    True),
    StructField("total_amount",  DoubleType(),    True),
    StructField("total_revenue", DoubleType(),    True),
    StructField("order_date",    StringType(),    True),  # parsed below
    StructField("region",        StringType(),    True),
    StructField("kafka_ts",      StringType(),    True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}_Streaming")
        .master(SPARK_MASTER)
        .config("spark.jars.packages", KAFKA_SPARK_PACKAGE)
        .config("spark.jars", JDBC_JAR_PATH)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", STREAM_CHECKPOINT)
        .getOrCreate()
    )


# ── Batch writer: called per micro-batch ─────────────────────────────
def write_stream_sales(batch_df, batch_id: int) -> None:
    """Write raw parsed messages to stream_sales table."""
    if batch_df.rdd.isEmpty():
        return
    log.info("Batch %d — writing %d rows to stream_sales", batch_id, batch_df.count())
    cols = ["order_id", "customer_id", "product_id", "category",
            "price", "quantity", "discount", "total_amount",
            "total_revenue", "order_date", "region"]
    existing = [c for c in cols if c in batch_df.columns]
    (batch_df.select(existing)
             .write
             .format("jdbc")
             .option("url",      JDBC_URL)
             .option("dbtable",  "stream_sales")
             .option("user",     JDBC_PROPS["user"])
             .option("password", JDBC_PROPS["password"])
             .option("driver",   JDBC_PROPS["driver"])
             .mode("append")
             .save())


def write_window_agg(batch_df, batch_id: int) -> None:
    """Write window aggregations to stream_window_agg table."""
    if batch_df.rdd.isEmpty():
        return
    log.info("Batch %d — writing window agg to stream_window_agg", batch_id)
    (batch_df.write
             .format("jdbc")
             .option("url",      JDBC_URL)
             .option("dbtable",  "stream_window_agg")
             .option("user",     JDBC_PROPS["user"])
             .option("password", JDBC_PROPS["password"])
             .option("driver",   JDBC_PROPS["driver"])
             .mode("append")
             .save())


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark Streaming starting …")

    # ── 1. Read from Kafka ────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # ── 2. Parse JSON payload ─────────────────────────────────────────
    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str",
                    "timestamp AS kafka_timestamp")
        .withColumn("data", F.from_json(F.col("json_str"), SALES_SCHEMA))
        .select("data.*", "kafka_timestamp")
        .withColumn("order_date",
                    F.to_timestamp("order_date", "yyyy-MM-dd'T'HH:mm:ss"))
        # Use kafka_timestamp as the event time for windowing
        .withColumn("event_time",
                    F.coalesce(F.col("order_date"), F.col("kafka_timestamp")))
        .withWatermark("event_time", "2 minutes")
    )

    # ── 3. Stream to stream_sales (raw) ──────────────────────────────
    raw_query = (
        parsed
        .writeStream
        .foreachBatch(write_stream_sales)
        .outputMode("append")
        .trigger(processingTime=f"{STREAM_TRIGGER_SEC} seconds")
        .option("checkpointLocation", f"{STREAM_CHECKPOINT}/raw")
        .start()
    )

    # ── 4. Window aggregation: 1-min tumbling, by category ───────────
    window_duration = f"{STREAM_WINDOW_MIN} minutes"
    windowed = (
        parsed
        .groupBy(
            F.window("event_time", window_duration),
            "category",
        )
        .agg(
            F.round(F.sum("total_revenue"), 2).alias("window_revenue"),
            F.count("order_id").alias("window_orders"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "category",
            "window_revenue",
            "window_orders",
        )
    )

    window_query = (
        windowed
        .writeStream
        .foreachBatch(write_window_agg)
        .outputMode("update")
        .trigger(processingTime=f"{STREAM_TRIGGER_SEC} seconds")
        .option("checkpointLocation", f"{STREAM_CHECKPOINT}/window")
        .start()
    )

    log.info("✅ Streaming queries running — waiting for data …")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
