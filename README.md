# Hybrid Data Engineering Pipeline — E-Commerce Sales

## Architecture

```
┌────────────────────────────────────────────────────┐
│          Kaggle Dataset (34,500 records)            │
│   miadul/e-commerce-sales-transactions-dataset      │
└─────────────────────┬──────────────────────────────┘
                      │ kagglehub load
                      ▼
┌─────────────────────────────────────────────────────┐
│             Spark Batch Processing                   │
│   • Data Cleaning (nulls, types, duplicates)        │
│   • Feature Engineering (revenue, segments)         │
│   • Aggregations (category, region, daily)          │
└─────────────────────┬───────────────────────────────┘
                      │ JDBC write
                      ▼
┌─────────────────────────────────────────────────────┐
│             PostgreSQL — Warehouse                   │
│   • batch_sales          • category_revenue         │
│   • stream_sales         • daily_sales              │
│   • analytics tables     • top_products             │
└──────────┬──────────────────────────────────────────┘
           │                         ▲
           │ read rows               │ JDBC write
           ▼                         │
┌──────────────────────┐   ┌─────────────────────────┐
│   Kafka Producer     │   │   Spark Streaming        │
│   (sales_topic)      │──▶│   • JSON parse           │
│   1 msg / second     │   │   • Window aggregations  │
└──────────────────────┘   │   • Category live stats  │
                           └─────────────────────────┘
                                        │
                                        ▼
                           ┌─────────────────────────┐
                           │   Streamlit Dashboard    │
                           │   • Batch trends         │
                           │   • Top categories       │
                           │   • Real-time revenue    │
                           └─────────────────────────┘
```

## Folder Structure

```
ecommerce_pipeline/
├── config/
│   └── settings.py          # DB, Kafka, Spark config
├── ingestion/
│   └── load_dataset.py      # kagglehub loader + CSV save
├── batch/
│   └── spark_batch.py       # Spark cleaning + feature engineering + PG write
├── streaming/
│   ├── kafka_producer.py    # Kafka producer (row-by-row simulation)
│   └── spark_streaming.py   # Spark Structured Streaming consumer
├── sql/
│   ├── create_tables.sql    # All PostgreSQL DDL
│   └── window_functions.sql # 12 window function queries
├── dashboard/
│   └── app.py               # Streamlit dashboard
├── requirements.txt
└── README.md
```

## Setup Instructions

### 1. Prerequisites
```bash
# Install Python deps
pip install -r requirements.txt

# PostgreSQL must be running locally
# Create database:
psql -U postgres -c "CREATE DATABASE ecommerce_pipeline;"

# Run table creation
psql -U postgres -d ecommerce_pipeline -f sql/create_tables.sql
```

### 2. Run Order
```bash
# Step 1: Load + save dataset
python ingestion/load_dataset.py

# Step 2: Spark batch processing
python batch/spark_batch.py

# Step 3: Apply window functions (in psql or pgAdmin)
psql -U postgres -d ecommerce_pipeline -f sql/window_functions.sql

# Step 4: Start Kafka (in separate terminal)
# Start Zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
# Start Kafka:     bin/kafka-server-start.sh config/server.properties
# Create topic:    bin/kafka-topics.sh --create --topic sales_topic --bootstrap-server localhost:9092

# Step 5: Start Kafka producer (separate terminal)
python streaming/kafka_producer.py

# Step 6: Start Spark streaming consumer (separate terminal)
python streaming/spark_streaming.py

# Step 7: Launch dashboard
streamlit run dashboard/app.py
```

## Technologies Used
| Layer | Tool |
|---|---|
| Ingestion (Batch) | kagglehub + PySpark |
| Ingestion (Stream) | Apache Kafka |
| Processing (Batch) | Apache Spark (PySpark) |
| Processing (Stream) | Spark Structured Streaming |
| Storage | PostgreSQL |
| Analytics | SQL Window Functions |
| Visualization | Streamlit + Plotly |
| DataOps | Logging + schema validation |
