# dashboard/app.py
# ─────────────────────────────────────────────────────────────────────
# Streamlit Dashboard
#   • Batch: sales trends, top categories, region breakdown
#   • Streaming: live revenue counter, live order feed, window agg chart
#   • Auto-refreshes every 10 seconds for real-time components
# ─────────────────────────────────────────────────────────────────────

import os
import sys
import time
import logging
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from streamlit_autorefresh import st_autorefresh

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DB_CONFIG

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── Page config ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="E-Commerce Data Pipeline Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Auto-refresh every 10 seconds for real-time section ───────────────
refresh_count = st_autorefresh(interval=10_000, key="dashboard_refresh")

# ── Custom CSS ────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d6a9f 100%);
        padding: 1.2rem 1.5rem;
        border-radius: 12px;
        color: white;
        margin-bottom: 1rem;
    }
    .metric-card h2 { margin: 0; font-size: 2rem; }
    .metric-card p  { margin: 0; opacity: 0.8; font-size: 0.9rem; }
    .live-badge {
        background: #e74c3c;
        color: white;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: bold;
        animation: blink 1s step-start infinite;
    }
    @keyframes blink { 50% { opacity: 0; } }
    .section-header {
        border-left: 4px solid #2d6a9f;
        padding-left: 12px;
        margin: 1.5rem 0 1rem;
    }
</style>
""", unsafe_allow_html=True)


# ── DB connection (cached 10 seconds for live data) ───────────────────
@st.cache_resource
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def query(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = get_connection()
        return pd.read_sql(sql, conn, params=params)
    except Exception as e:
        log.error("DB query failed: %s", e)
        return pd.DataFrame()


# ── Sidebar: Filters ──────────────────────────────────────────────────
st.sidebar.title("🔧 Filters")
st.sidebar.markdown("---")

# Category filter
cats_df = query("SELECT DISTINCT category FROM batch_sales ORDER BY category")
categories = ["All"] + (cats_df["category"].tolist() if not cats_df.empty else [])
sel_category = st.sidebar.selectbox("Category", categories)

# Region filter
reg_df = query("SELECT DISTINCT region FROM batch_sales ORDER BY region")
regions = ["All"] + (reg_df["region"].tolist() if not reg_df.empty else [])
sel_region = st.sidebar.selectbox("Region", regions)

# Date range
date_range = st.sidebar.date_input(
    "Date Range",
    value=[datetime.now() - timedelta(days=365), datetime.now()],
)
date_from = date_range[0] if len(date_range) > 0 else datetime.now() - timedelta(days=365)
date_to   = date_range[1] if len(date_range) > 1 else datetime.now()

st.sidebar.markdown("---")
st.sidebar.info(f"🔄 Auto-refresh every 10s\nRefresh #{refresh_count}")

# ── Build WHERE clause from filters ──────────────────────────────────
def build_where(alias: str = "") -> tuple[str, list]:
    prefix = f"{alias}." if alias else ""
    clauses, params = [], []
    clauses.append(f"{prefix}order_date BETWEEN %s AND %s")
    params += [date_from, date_to]
    if sel_category != "All":
        clauses.append(f"{prefix}category = %s")
        params.append(sel_category)
    if sel_region != "All":
        clauses.append(f"{prefix}region = %s")
        params.append(sel_region)
    return " WHERE " + " AND ".join(clauses), params


# ══════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════
st.title("🛒 E-Commerce Hybrid Data Pipeline Dashboard")
st.markdown("**Batch + Stream Processing** | PostgreSQL + Spark + Kafka")
st.markdown("---")


# ══════════════════════════════════════════════════════════════════════
# SECTION 1 — KPI Metrics
# ══════════════════════════════════════════════════════════════════════
where, params = build_where()
kpi_df = query(
    f"""
    SELECT
        COUNT(order_id)                  AS total_orders,
        ROUND(SUM(total_revenue)::NUMERIC, 2)   AS total_revenue,
        ROUND(AVG(total_revenue)::NUMERIC, 2)   AS avg_order_value,
        COUNT(DISTINCT customer_id)      AS unique_customers
    FROM batch_sales {where}
    """,
    params,
)

col1, col2, col3, col4 = st.columns(4)
if not kpi_df.empty:
    row = kpi_df.iloc[0]
    with col1:
        st.metric("📦 Total Orders",     f"{int(row.get('total_orders',0)):,}")
    with col2:
        st.metric("💰 Total Revenue",    f"${float(row.get('total_revenue',0)):,.2f}")
    with col3:
        st.metric("🧾 Avg Order Value",  f"${float(row.get('avg_order_value',0)):,.2f}")
    with col4:
        st.metric("👤 Unique Customers", f"{int(row.get('unique_customers',0)):,}")

st.markdown("---")


# ══════════════════════════════════════════════════════════════════════
# SECTION 2 — Batch Visualisations
# ══════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-header"><h2>📊 Batch Analytics</h2></div>',
            unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["📈 Sales Trends", "🛒 Categories", "🌍 Regions"])

# ── Tab 1: Sales Trends ───────────────────────────────────────────────
with tab1:
    trend_df = query(
        f"""
        SELECT
            DATE_TRUNC('day', order_date) AS sale_date,
            ROUND(SUM(total_revenue)::NUMERIC, 2) AS daily_revenue,
            COUNT(order_id)               AS daily_orders
        FROM batch_sales {where}
        GROUP BY 1
        ORDER BY 1
        """,
        params,
    )

    if not trend_df.empty:
        fig = px.area(
            trend_df,
            x="sale_date", y="daily_revenue",
            title="Daily Revenue Over Time",
            labels={"sale_date": "Date", "daily_revenue": "Revenue ($)"},
            color_discrete_sequence=["#2d6a9f"],
        )
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        # Monthly summary
        trend_df["month"] = pd.to_datetime(trend_df["sale_date"]).dt.to_period("M").astype(str)
        monthly = trend_df.groupby("month").agg(
            revenue=("daily_revenue", "sum"),
            orders=("daily_orders",   "sum")
        ).reset_index()

        fig2 = px.bar(monthly, x="month", y="revenue",
                      title="Monthly Revenue Summary",
                      color="revenue",
                      color_continuous_scale="Blues")
        fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No trend data available for selected filters.")

# ── Tab 2: Category Analysis ──────────────────────────────────────────
with tab2:
    cat_df = query(
        f"""
        SELECT
            category,
            ROUND(SUM(total_revenue)::NUMERIC, 2) AS total_revenue,
            COUNT(order_id)                AS order_count,
            ROUND(AVG(total_revenue)::NUMERIC, 2) AS avg_revenue,
            RANK() OVER (ORDER BY SUM(total_revenue) DESC) AS revenue_rank
        FROM batch_sales {where}
        GROUP BY category
        ORDER BY total_revenue DESC
        LIMIT 15
        """,
        params,
    )

    if not cat_df.empty:
        col_a, col_b = st.columns(2)
        with col_a:
            fig = px.bar(
                cat_df, x="total_revenue", y="category",
                orientation="h",
                title="Top 15 Categories by Revenue",
                color="total_revenue",
                color_continuous_scale="Blues",
                labels={"total_revenue": "Revenue ($)", "category": ""},
            )
            fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            fig2 = px.pie(
                cat_df.head(8),
                values="total_revenue",
                names="category",
                title="Revenue Share (Top 8)",
                hole=0.4,
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.dataframe(
            cat_df[["revenue_rank", "category", "total_revenue",
                    "order_count", "avg_revenue"]],
            use_container_width=True,
        )
    else:
        st.info("No category data available.")

# ── Tab 3: Region Analysis ────────────────────────────────────────────
with tab3:
    reg_data = query(
        f"""
        SELECT
            region,
            ROUND(SUM(total_revenue)::NUMERIC, 2) AS total_revenue,
            COUNT(order_id)                AS order_count,
            customer_segment,
            COUNT(customer_id)             AS segment_count
        FROM batch_sales {where}
        GROUP BY region, customer_segment
        ORDER BY total_revenue DESC
        """,
        params,
    )

    if not reg_data.empty:
        reg_sum = reg_data.groupby("region")["total_revenue"].sum().reset_index()
        fig = px.bar(reg_sum, x="region", y="total_revenue",
                     title="Revenue by Region",
                     color="total_revenue",
                     color_continuous_scale="Teal")
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.sunburst(
            reg_data,
            path=["region", "customer_segment"],
            values="segment_count",
            title="Customer Segments by Region",
            color="total_revenue",
            color_continuous_scale="RdBu",
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No region data available.")


st.markdown("---")


# ══════════════════════════════════════════════════════════════════════
# SECTION 3 — Real-Time Streaming
# ══════════════════════════════════════════════════════════════════════
st.markdown(
    '<div class="section-header">'
    '<h2>⚡ Real-Time Stream <span class="live-badge">LIVE</span></h2>'
    '</div>',
    unsafe_allow_html=True,
)

# ── Live KPIs ─────────────────────────────────────────────────────────
live_df = query("""
    SELECT
        COUNT(id)                               AS live_orders,
        ROUND(SUM(total_revenue)::NUMERIC, 2)   AS live_revenue,
        MAX(ingested_at)                        AS last_event
    FROM stream_sales
    WHERE ingested_at >= NOW() - INTERVAL '5 minutes'
""")

col1, col2, col3 = st.columns(3)
if not live_df.empty:
    row = live_df.iloc[0]
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <p>Orders (last 5 min)</p>
            <h2>{int(row.get('live_orders', 0)):,}</h2>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <p>Revenue (last 5 min)</p>
            <h2>${float(row.get('live_revenue') or 0):,.2f}</h2>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        last = row.get("last_event")
        st.markdown(f"""
        <div class="metric-card">
            <p>Last Event</p>
            <h2 style="font-size:1.1rem">{str(last)[:19] if last else '—'}</h2>
        </div>
        """, unsafe_allow_html=True)

# ── Window Aggregation Chart ──────────────────────────────────────────
win_df = query("""
    SELECT
        window_start,
        category,
        window_revenue,
        window_orders
    FROM stream_window_agg
    WHERE window_start >= NOW() - INTERVAL '30 minutes'
    ORDER BY window_start DESC
    LIMIT 200
""")

if not win_df.empty:
    fig = px.bar(
        win_df,
        x="window_start", y="window_revenue",
        color="category",
        title="Revenue by Category (1-min Windows, Last 30 min)",
        labels={"window_start": "Window", "window_revenue": "Revenue ($)"},
        barmode="stack",
    )
    fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("⏳ Waiting for streaming data … Start the Kafka producer and "
            "Spark streaming consumer to see live data here.")

# ── Live Order Feed ───────────────────────────────────────────────────
st.subheader("📡 Live Order Feed (last 20 events)")
feed_df = query("""
    SELECT
        ingested_at, order_id, category, region,
        ROUND(total_revenue::NUMERIC, 2) AS revenue
    FROM stream_sales
    ORDER BY ingested_at DESC
    LIMIT 20
""")

if not feed_df.empty:
    st.dataframe(feed_df, use_container_width=True)
else:
    st.info("No streaming events received yet.")


# ══════════════════════════════════════════════════════════════════════
# SECTION 4 — Batch vs Stream Comparison
# ══════════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown('<div class="section-header"><h2>🔀 Batch vs Stream Comparison</h2></div>',
            unsafe_allow_html=True)

batch_cat = query("""
    SELECT category,
           ROUND(SUM(total_revenue)::NUMERIC, 2) AS batch_revenue
    FROM batch_sales
    GROUP BY category
    ORDER BY batch_revenue DESC
    LIMIT 10
""")

stream_cat = query("""
    SELECT category,
           ROUND(SUM(total_revenue)::NUMERIC, 2) AS stream_revenue
    FROM stream_sales
    GROUP BY category
    ORDER BY stream_revenue DESC
    LIMIT 10
""")

if not batch_cat.empty and not stream_cat.empty:
    merged = batch_cat.merge(stream_cat, on="category", how="outer").fillna(0)
    fig = go.Figure()
    fig.add_trace(go.Bar(name="Batch", x=merged["category"],
                         y=merged["batch_revenue"],  marker_color="#2d6a9f"))
    fig.add_trace(go.Bar(name="Stream", x=merged["category"],
                         y=merged["stream_revenue"], marker_color="#e74c3c"))
    fig.update_layout(
        barmode="group",
        title="Revenue per Category: Batch vs Stream",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis_tickangle=-30,
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Batch and/or stream data not yet available for comparison.")

st.markdown("---")
st.caption(f"Dashboard last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
