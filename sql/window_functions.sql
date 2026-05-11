-- sql/window_functions.sql
-- ─────────────────────────────────────────────────────────────────────
-- 12 Window Function Queries on batch_sales
-- Run after batch pipeline completes.
-- ─────────────────────────────────────────────────────────────────────

-- ── Q1. Overall average discounted price (all rows) ───────────────────
-- Uses AVG() OVER () — no partition, window spans entire table
SELECT
    order_id,
    product_id,
    category,
    price,
    discount,
    total_revenue,
    ROUND(AVG(price) OVER (), 2)  AS overall_avg_price
FROM batch_sales
ORDER BY order_date
LIMIT 100;

-- ── Q2. Average price per category ────────────────────────────────────
SELECT
    order_id,
    product_id,
    category,
    price,
    ROUND(AVG(price) OVER (PARTITION BY category), 2) AS avg_price_in_category
FROM batch_sales
ORDER BY category, price DESC;

-- ── Q3. Total ratings (order_count proxy) per category ────────────────
SELECT
    order_id,
    product_id,
    category,
    quantity,
    SUM(quantity) OVER (PARTITION BY category) AS total_units_in_category
FROM batch_sales
ORDER BY category;

-- ── Q4. RANK products by revenue within each category ─────────────────
SELECT
    order_id,
    product_id,
    category,
    total_revenue,
    RANK() OVER (
        PARTITION BY category
        ORDER BY total_revenue DESC
    ) AS revenue_rank
FROM batch_sales
ORDER BY category, revenue_rank;

-- ── Q5. DENSE_RANK by discount within each category ───────────────────
SELECT
    order_id,
    product_id,
    category,
    discount,
    DENSE_RANK() OVER (
        PARTITION BY category
        ORDER BY discount DESC
    ) AS discount_dense_rank
FROM batch_sales
ORDER BY category, discount_dense_rank;

-- ── Q6. ROW_NUMBER per category ordered by revenue ────────────────────
SELECT
    order_id,
    product_id,
    category,
    total_revenue,
    ROW_NUMBER() OVER (
        PARTITION BY category
        ORDER BY total_revenue DESC
    ) AS row_num
FROM batch_sales
ORDER BY category, row_num;

-- ── Q7. Running total revenue per category ────────────────────────────
SELECT
    order_id,
    category,
    order_date,
    total_revenue,
    ROUND(
        SUM(total_revenue) OVER (
            PARTITION BY category
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2
    ) AS running_total_revenue
FROM batch_sales
ORDER BY category, order_date;

-- ── Q8. 7-row moving average of price per category ────────────────────
SELECT
    order_id,
    category,
    order_date,
    price,
    ROUND(
        AVG(price) OVER (
            PARTITION BY category
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2
    ) AS moving_avg_price_7
FROM batch_sales
ORDER BY category, order_date;

-- ── Q9. LAG: previous product's revenue in same category ──────────────
SELECT
    order_id,
    product_id,
    category,
    total_revenue,
    LAG(total_revenue, 1) OVER (
        PARTITION BY category
        ORDER BY order_date
    ) AS prev_revenue
FROM batch_sales
ORDER BY category, order_date;

-- ── Q10. LEAD: next product's price in same category ──────────────────
SELECT
    order_id,
    product_id,
    category,
    price,
    LEAD(price, 1) OVER (
        PARTITION BY category
        ORDER BY order_date
    ) AS next_price
FROM batch_sales
ORDER BY category, order_date;

-- ── Q11. Revenue difference vs previous row (LAG) ─────────────────────
SELECT
    order_id,
    category,
    order_date,
    total_revenue,
    LAG(total_revenue) OVER (
        PARTITION BY category
        ORDER BY order_date
    )                                          AS prev_revenue,
    ROUND(
        total_revenue
        - LAG(total_revenue) OVER (
            PARTITION BY category
            ORDER BY order_date
          ),
    2)                                         AS revenue_delta
FROM batch_sales
ORDER BY category, order_date;

-- ── Q12. % contribution of each order to category total revenue ───────
SELECT
    order_id,
    product_id,
    category,
    total_revenue,
    SUM(total_revenue) OVER (PARTITION BY category) AS category_total,
    ROUND(
        100.0 * total_revenue
        / NULLIF(SUM(total_revenue) OVER (PARTITION BY category), 0),
    2) AS pct_contribution
FROM batch_sales
ORDER BY category, pct_contribution DESC;

-- ─────────────────────────────────────────────────────────────────────
-- Bonus: Global running total across all orders (no partition)
-- ─────────────────────────────────────────────────────────────────────
SELECT
    order_id,
    order_date,
    total_revenue,
    ROUND(
        SUM(total_revenue) OVER (ORDER BY order_date
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2) AS global_running_total
FROM batch_sales
ORDER BY order_date;

-- ─────────────────────────────────────────────────────────────────────
-- Bonus: Rank within region AND category combined
-- ─────────────────────────────────────────────────────────────────────
SELECT
    order_id,
    product_id,
    category,
    region,
    total_revenue,
    RANK() OVER (
        PARTITION BY region, category
        ORDER BY total_revenue DESC
    ) AS rank_in_region_category
FROM batch_sales
ORDER BY region, category, rank_in_region_category;
