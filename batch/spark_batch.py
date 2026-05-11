# batch/spark_batch.py
# ──────────────────────────────────────────────────────────────────────
# Step 2: Spark Batch Pipeline
#   • Load CSV  → clean  → feature-engineer  → aggregate  → write PG
# ──────────────────────────────────────────────────────────────────────

import os
import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    SPARK_APP_NAME, SPARK_MASTER,
    JDBC_URL, JDBC_PROPS, JDBC_JAR_PATH,
    DATASET_LOCAL_PATH,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ── 1. Spark Session ──────────────────────────────────────────────────
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.jars", JDBC_JAR_PATH)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ── 2. Load ───────────────────────────────────────────────────────────
def load_csv(spark: SparkSession) -> DataFrame:
    log.info("Loading CSV from %s", DATASET_LOCAL_PATH)
    df = spark.read.csv(DATASET_LOCAL_PATH, header=True, inferSchema=True)
    log.info("Raw shape: %d rows × %d cols", df.count(), len(df.columns))
    df.printSchema()
    return df


# ── 3. Clean ──────────────────────────────────────────────────────────
def clean(df: DataFrame) -> DataFrame:
    log.info("Cleaning data …")

    # Standardise column names to snake_case
    for col in df.columns:
        df = df.withColumnRenamed(col, col.strip().lower().replace(" ", "_"))

    # Drop rows where essential fields are null
    essential = ["order_id", "customer_id", "product_id",
                 "price", "quantity", "order_date"]
    df = df.dropna(subset=essential)

    # Drop exact duplicates
    before = df.count()
    df = df.dropDuplicates(["order_id"])
    log.info("Dropped %d duplicate order_ids", before - df.count())

    # Cast numeric columns
    df = (df
          .withColumn("price",             F.col("price").cast(DoubleType()))
          .withColumn("quantity",          F.col("quantity").cast(IntegerType()))
          .withColumn("discount",          F.coalesce(
                                               F.col("discount").cast(DoubleType()),
                                               F.lit(0.0)))
          .withColumn("profit_margin",     F.coalesce(
                                               F.col("profit_margin").cast(DoubleType()),
                                               F.lit(0.1)))
          .withColumn("total_amount",      F.coalesce(
                                               F.col("total_amount").cast(DoubleType()),
                                               F.lit(0.0)))
          )

    # Parse order_date — try multiple formats
    df = df.withColumn(
        "order_date",
        F.coalesce(
            F.to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("order_date", "yyyy-MM-dd"),
            F.to_timestamp("order_date", "MM/dd/yyyy"),
        )
    ).dropna(subset=["order_date"])

    # Standardise category (trim + title-case)
    df = df.withColumn("category",
                       F.initcap(F.trim(F.col("category"))))

    # Standardise region
    if "region" in df.columns:
        df = df.withColumn("region",
                           F.upper(F.trim(F.col("region"))))
    else:
        df = df.withColumn("region", F.lit("UNKNOWN"))

    log.info("Clean shape: %d rows", df.count())
    return df


# ── 4. Feature Engineering ────────────────────────────────────────────
def engineer(df: DataFrame) -> DataFrame:
    log.info("Engineering features …")

    df = (df
          # Revenue after discount
          .withColumn("total_revenue",
                      F.round(
                          F.col("price") * F.col("quantity")
                          * (F.lit(1.0) - F.col("discount")),
                          2))

          # Estimated profit
          .withColumn("profit_estimate",
                      F.round(F.col("total_revenue") * F.col("profit_margin"), 2))

          # Temporal features
          .withColumn("order_month", F.month("order_date"))
          .withColumn("order_day",   F.dayofmonth("order_date"))
          .withColumn("order_hour",  F.hour("order_date"))
          .withColumn("order_year",  F.year("order_date"))

          # Customer segment by total_amount spend
          .withColumn("customer_segment",
                      F.when(F.col("total_amount") >= 500, "High Value")
                       .when(F.col("total_amount") >= 150, "Mid Value")
                       .otherwise("Low Value"))
          )

    log.info("Feature engineering done.")
    return df


# ── 5. Aggregations ───────────────────────────────────────────────────
def aggregate(df: DataFrame) -> dict[str, DataFrame]:
    log.info("Running aggregations …")

    category_revenue = (
        df.groupBy("category")
          .agg(
              F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
              F.round(F.avg("total_revenue"), 2).alias("avg_revenue"),
              F.count("order_id").alias("order_count"),
          )
          .orderBy(F.desc("total_revenue"))
    )

    daily_sales = (
        df.groupBy(
            F.to_date("order_date").alias("sale_date"),
            "region"
        )
        .agg(
            F.round(F.sum("total_revenue"), 2).alias("daily_revenue"),
            F.count("order_id").alias("daily_orders"),
        )
        .orderBy("sale_date")
    )

    top_products = (
        df.groupBy("product_id", "category")
          .agg(
              F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
              F.sum("quantity").alias("units_sold"),
              F.round(F.avg("price"), 2).alias("avg_price"),
          )
          .orderBy(F.desc("total_revenue"))
          .limit(100)
    )

    return {
        "category_revenue": category_revenue,
        "daily_sales":      daily_sales,
        "top_products":     top_products,
    }


# ── 6. Write to PostgreSQL ────────────────────────────────────────────
def write_pg(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    log.info("Writing table '%s' → PostgreSQL (mode=%s) …", table, mode)
    (df.write
       .format("jdbc")
       .option("url", JDBC_URL)
       .option("dbtable", table)
       .option("user", JDBC_PROPS["user"])
       .option("password", JDBC_PROPS["password"])
       .option("driver", JDBC_PROPS["driver"])
       .mode(mode)
       .save())
    log.info("Table '%s' written successfully.", table)


# ── 7. Select final columns for batch_sales ───────────────────────────
BATCH_COLS = [
    "order_id", "customer_id", "product_id", "category",
    "price", "quantity", "discount", "total_amount",
    "total_revenue", "profit_estimate", "order_date", "region",
    "order_month", "order_day", "order_hour", "customer_segment",
]


# ── MAIN ──────────────────────────────────────────────────────────────
def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw  = load_csv(spark)
    cln  = clean(raw)
    feat = engineer(cln)

    # Keep only columns that exist in the DataFrame
    existing = [c for c in BATCH_COLS if c in feat.columns]
    batch_df = feat.select(existing)

    # Write main batch table
    write_pg(batch_df, "batch_sales")

    # Write analytics tables
    aggs = aggregate(feat)
    for table_name, agg_df in aggs.items():
        write_pg(agg_df, table_name)

    log.info("✅ Batch pipeline complete.")
    spark.stop()


if __name__ == "__main__":
    main()
