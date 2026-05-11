-- sql/create_tables.sql
-- ─────────────────────────────────────────────────────
-- Run once before starting the pipeline:
--   psql -U postgres -d ecommerce_pipeline -f sql/create_tables.sql
-- ─────────────────────────────────────────────────────

-- ── batch_sales ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS batch_sales (
    order_id          TEXT PRIMARY KEY,
    customer_id       TEXT,
    product_id        TEXT,
    category          TEXT,
    price             NUMERIC(12,2),
    quantity          INTEGER,
    discount          NUMERIC(5,4),
    total_amount      NUMERIC(12,2),
    total_revenue     NUMERIC(12,2),
    profit_estimate   NUMERIC(12,2),
    order_date        TIMESTAMP,
    region            TEXT,
    order_month       INTEGER,
    order_day         INTEGER,
    order_hour        INTEGER,
    customer_segment  TEXT
);

-- ── stream_sales ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS stream_sales (
    id                SERIAL PRIMARY KEY,
    order_id          TEXT,
    customer_id       TEXT,
    product_id        TEXT,
    category          TEXT,
    price             NUMERIC(12,2),
    quantity          INTEGER,
    discount          NUMERIC(5,4),
    total_amount      NUMERIC(12,2),
    total_revenue     NUMERIC(12,2),
    order_date        TIMESTAMP,
    region            TEXT,
    ingested_at       TIMESTAMP DEFAULT NOW()
);

-- ── category_revenue ──────────────────────────────────
CREATE TABLE IF NOT EXISTS category_revenue (
    category      TEXT PRIMARY KEY,
    total_revenue NUMERIC(14,2),
    avg_revenue   NUMERIC(12,2),
    order_count   INTEGER
);

-- ── daily_sales ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_sales (
    sale_date     DATE,
    region        TEXT,
    daily_revenue NUMERIC(14,2),
    daily_orders  INTEGER,
    PRIMARY KEY (sale_date, region)
);

-- ── top_products ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS top_products (
    product_id    TEXT PRIMARY KEY,
    category      TEXT,
    total_revenue NUMERIC(14,2),
    units_sold    INTEGER,
    avg_price     NUMERIC(12,2)
);

-- ── stream_window_agg ─────────────────────────────────
-- Written by Spark Streaming: 1-minute window aggregations
CREATE TABLE IF NOT EXISTS stream_window_agg (
    id               SERIAL PRIMARY KEY,
    window_start     TIMESTAMP,
    window_end       TIMESTAMP,
    category         TEXT,
    window_revenue   NUMERIC(14,2),
    window_orders    INTEGER,
    computed_at      TIMESTAMP DEFAULT NOW()
);

-- ── Indexes ───────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_batch_date     ON batch_sales (order_date);
CREATE INDEX IF NOT EXISTS idx_batch_category ON batch_sales (category);
CREATE INDEX IF NOT EXISTS idx_batch_region   ON batch_sales (region);
CREATE INDEX IF NOT EXISTS idx_stream_date    ON stream_sales (order_date);
CREATE INDEX IF NOT EXISTS idx_stream_cat     ON stream_sales (category);
