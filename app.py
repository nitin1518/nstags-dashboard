from __future__ import annotations

import re
import time
from datetime import date, timedelta
from io import StringIO
from urllib.parse import urlparse

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from botocore.exceptions import ClientError

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="nsTags | Retail Intelligence",
    page_icon="📈",
    layout="wide",
)

# =========================================================
# LIGHTWEIGHT STYLES - STABLE OVER FANCY
# =========================================================
st.markdown(
    """
    <style>
    .main .block-container {padding-top: 1.2rem; padding-bottom: 2rem; max-width: 100%;}
    .metric-card {
        background: linear-gradient(180deg, #0f172a 0%, #111827 100%);
        border: 1px solid rgba(148,163,184,.18);
        border-radius: 18px;
        padding: 1rem 1rem 0.8rem 1rem;
        min-height: 128px;
        color: #e5e7eb;
        box-shadow: 0 10px 24px rgba(0,0,0,.16);
    }
    .metric-label {font-size: .75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .08em; font-weight: 700;}
    .metric-value {font-size: 2rem; font-weight: 800; line-height: 1.05; margin-top: .35rem; color: #f8fafc;}
    .metric-sub {font-size: .85rem; color: #cbd5e1; margin-top: .35rem;}
    .hero {
        background: linear-gradient(135deg, rgba(79,70,229,.12), rgba(16,185,129,.09));
        border: 1px solid rgba(99,102,241,.25);
        border-radius: 20px;
        padding: 1.2rem 1.4rem;
        margin-bottom: 1rem;
    }
    .hero h1 {margin: 0; font-size: 2rem;}
    .hero p {margin: .25rem 0 0 0; color: #475569;}
    </style>
    """,
    unsafe_allow_html=True,
)

PLOT_CONFIG = {"displayModeBar": False}
TZ_NAME = "Asia/Kolkata"

COLORS = {
    "indigo": "#6366F1",
    "emerald": "#10B981",
    "amber": "#F59E0B",
    "rose": "#F43F5E",
    "sky": "#38BDF8",
    "slate": "#64748B",
}

# =========================================================
# CONFIG
# =========================================================
AWS_REGION = st.secrets.get("AWS_REGION", "ap-south-1")
ATHENA_DATABASE = st.secrets.get("ATHENA_DATABASE", "nstags_analytics")
ATHENA_WORKGROUP = st.secrets.get("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT = st.secrets.get("ATHENA_OUTPUT", "s3://nstags-datalake-hq-2026/athena-results/")
AWS_ACCESS_KEY_ID = st.secrets.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = st.secrets.get("AWS_SECRET_ACCESS_KEY")

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    st.error("Missing AWS credentials in Streamlit secrets.")
    st.stop()

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
athena_client = session.client("athena")
s3_client = session.client("s3")

# =========================================================
# HELPERS
# =========================================================
def validate_store_id(store_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", str(store_id)):
        raise ValueError("Invalid store_id")
    return store_id


def s3_uri_to_bucket_key(s3_uri: str):
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip("/")


def fmt_int(x) -> str:
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "0"


def fmt_pct(x) -> str:
    try:
        return f"{float(x) * 100:.1f}%"
    except Exception:
        return "0.0%"


def fmt_pct_100(x) -> str:
    try:
        return f"{float(x):.1f}%"
    except Exception:
        return "0.0%"


def fmt_seconds(x) -> str:
    try:
        total = int(round(float(x)))
    except Exception:
        return "-"
    mins, secs = divmod(total, 60)
    hrs, mins = divmod(mins, 60)
    if hrs > 0:
        return f"{hrs}h {mins}m"
    if mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def safe_div(a, b) -> float:
    try:
        a = float(a)
        b = float(b)
        if b == 0:
            return 0.0
        return a / b
    except Exception:
        return 0.0


def score_band(score: float):
    try:
        score = float(score)
    except Exception:
        score = 0.0
    if score >= 75:
        return "Strong"
    if score >= 50:
        return "Moderate"
    return "Weak"


def benchmark_maturity_label(population):
    try:
        population = int(population)
    except Exception:
        return "Unknown", "Benchmark population unavailable."
    if population >= 100:
        return "Stable", f"Benchmark built on {population:,} store-day records."
    if population >= 30:
        return "Growing", f"Benchmark built on {population:,} store-day records. Directionally useful, still maturing."
    return "Early", f"Benchmark built on only {population:,} store-day records. Scores are provisional."


def metric_card(label: str, value: str, sub: str = ""):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-sub">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def style_chart(fig: go.Figure) -> go.Figure:
    fig.update_layout(
        margin=dict(l=10, r=10, t=40, b=40),
        hovermode="x unified",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=-0.2),
        font=dict(size=12),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(148,163,184,0.20)")
    return fig


def infer_trend_grain(start_date: date, end_date: date) -> str:
    span_days = (end_date - start_date).days + 1
    if span_days <= 14:
        return "day"
    if span_days <= 120:
        return "week"
    return "month"


def build_period_trend(daily_df: pd.DataFrame, grain: str) -> pd.DataFrame:
    if daily_df.empty:
        return pd.DataFrame()
    df = daily_df.copy()
    df["metric_date"] = pd.to_datetime(df["metric_date"])

    if grain == "day":
        df["period_start"] = df["metric_date"].dt.normalize()
        label_fmt = "%d %b"
    elif grain == "week":
        df["period_start"] = df["metric_date"] - pd.to_timedelta(df["metric_date"].dt.weekday, unit="D")
        label_fmt = "%d %b"
    else:
        df["period_start"] = df["metric_date"].dt.to_period("M").dt.to_timestamp()
        label_fmt = "%b %Y"

    trend = (
        df.groupby("period_start", as_index=False)
        .agg(
            walk_by_traffic=("walk_by_traffic", "mean"),
            store_interest=("store_interest", "mean"),
            near_store=("near_store", "mean"),
            store_visits=("store_visits", "sum"),
            qualified_footfall=("qualified_footfall", "sum"),
            engaged_visits=("engaged_visits", "sum"),
            avg_dwell_seconds=("avg_dwell_seconds", "mean"),
        )
        .sort_values("period_start")
    )
    trend["period_label"] = trend["period_start"].dt.strftime(label_fmt)
    return trend


def weighted_score(parts):
    total_weight = sum(w for _, w in parts if w > 0)
    if total_weight <= 0:
        return 0.0
    return round(sum(score * weight for score, weight in parts if weight > 0) / total_weight, 1)


def clamp_0_100(x):
    try:
        x = float(x)
    except Exception:
        return 0.0
    return max(0.0, min(100.0, x))


def normalize_ratio_to_100(value, cap):
    try:
        value = float(value)
        cap = float(cap)
    except Exception:
        return 0.0
    if cap <= 0:
        return 0.0
    return clamp_0_100((value / cap) * 100.0)


def normalize_index_to_100(value, cap):
    return normalize_ratio_to_100(value, cap)


def compute_local_fallback_indices(kpi: pd.Series, brand_df: pd.DataFrame | None = None) -> dict:
    walk_by_traffic = float(kpi.get("walk_by_traffic", 0) or 0)
    store_interest = float(kpi.get("store_interest", 0) or 0)
    near_store = float(kpi.get("near_store", 0) or 0)
    store_visits = float(kpi.get("store_visits", 0) or 0)
    qualified_visits = float(kpi.get("qualified_footfall", 0) or 0)
    engaged_visits = float(kpi.get("engaged_visits", 0) or 0)
    avg_dwell_seconds = float(kpi.get("avg_dwell_seconds", 0) or 0)

    walk_by_score = normalize_index_to_100(walk_by_traffic, cap=20.0)
    interest_score = normalize_index_to_100(store_interest, cap=12.0)
    near_store_score = normalize_index_to_100(near_store, cap=8.0)

    qualified_rate = safe_div(qualified_visits, store_visits)
    engaged_rate = safe_div(engaged_visits, store_visits)
    qualified_score = normalize_ratio_to_100(qualified_rate, cap=0.60)
    engaged_score = normalize_ratio_to_100(engaged_rate, cap=0.40)
    dwell_score = normalize_ratio_to_100(avg_dwell_seconds, cap=180.0)

    store_magnet_ratio = safe_div(store_interest, walk_by_traffic)
    window_capture_ratio = safe_div(store_visits, store_interest)
    entry_efficiency_ratio = safe_div(qualified_visits, store_visits)

    store_magnet_score = normalize_ratio_to_100(store_magnet_ratio, cap=0.60)
    window_capture_score = normalize_ratio_to_100(window_capture_ratio, cap=8.0)
    entry_efficiency_score = normalize_ratio_to_100(entry_efficiency_ratio, cap=0.70)
    dwell_quality_score = weighted_score([(engaged_score, 0.6), (dwell_score, 0.4)])

    premium_device_mix_score = 50.0
    if brand_df is not None and not brand_df.empty:
        apple = float(brand_df["avg_apple_devices"].sum())
        samsung = float(brand_df["avg_samsung_devices"].sum())
        other = float(brand_df["avg_other_devices"].sum())
        total = apple + samsung + other
        premium_share = safe_div(apple, total) * 1.0 + safe_div(samsung, total) * 0.7
        premium_device_mix_score = normalize_ratio_to_100(premium_share, cap=0.75)

    volume_confidence_score = normalize_ratio_to_100(store_visits, cap=500.0)
    visit_quality_index = weighted_score([(qualified_score, 0.45), (engaged_score, 0.35), (dwell_score, 0.20)])
    store_attraction_index = weighted_score([(store_magnet_score, 0.40), (window_capture_score, 0.35), (entry_efficiency_score, 0.25)])
    audience_quality_index = weighted_score([(premium_device_mix_score, 0.60), (engaged_score, 0.40)])
    traffic_intelligence_index = weighted_score([
        (walk_by_score, 0.18),
        (interest_score, 0.18),
        (near_store_score, 0.12),
        (visit_quality_index, 0.22),
        (dwell_quality_score, 0.18),
        (volume_confidence_score, 0.12),
    ])

    return {
        "traffic_intelligence_index": round(traffic_intelligence_index, 1),
        "visit_quality_index": round(visit_quality_index, 1),
        "store_attraction_index": round(store_attraction_index, 1),
        "audience_quality_index": round(audience_quality_index, 1),
        "walk_by_score": round(walk_by_score, 1),
        "interest_score": round(interest_score, 1),
        "near_store_score": round(near_store_score, 1),
        "qualified_score": round(qualified_score, 1),
        "engaged_score": round(engaged_score, 1),
        "dwell_score": round(dwell_score, 1),
        "store_magnet_percentile_score": round(store_magnet_score, 1),
        "window_capture_score": round(window_capture_score, 1),
        "entry_efficiency_percentile_score": round(entry_efficiency_score, 1),
        "dwell_quality_score": round(dwell_quality_score, 1),
        "premium_device_mix_score": round(premium_device_mix_score, 1),
        "volume_confidence_score": round(volume_confidence_score, 1),
        "benchmark_population": 0,
        "is_fallback": True,
    }

# =========================================================
# ATHENA EXECUTION
# =========================================================
def run_athena_query(query: str, database: str = ATHENA_DATABASE, timeout_sec: int = 60) -> pd.DataFrame:
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
            WorkGroup=ATHENA_WORKGROUP,
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        msg = e.response.get("Error", {}).get("Message", str(e))
        raise RuntimeError(f"Athena start error [{code}]: {msg}") from e

    execution_id = response["QueryExecutionId"]
    start_ts = time.time()

    while True:
        execution = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = execution["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - start_ts > timeout_sec:
            raise RuntimeError("Athena query timed out")
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Unknown Athena error")
        raise RuntimeError(f"Athena query failed: {reason}")

    output_location = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    bucket, key = s3_uri_to_bucket_key(output_location)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))

# =========================================================
# DATA LOADERS - ONLY CANONICAL IST VIEWS
# =========================================================
@st.cache_data(ttl=300)
def load_store_list() -> pd.DataFrame:
    return run_athena_query(
        """
        SELECT DISTINCT store_id
        FROM nstags_dashboard_metrics_canonical
        ORDER BY store_id
        """
    )


@st.cache_data(ttl=300)
def load_available_dates(store_id: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        WITH all_dates AS (
            SELECT DISTINCT metric_date FROM nstags_dashboard_metrics_canonical WHERE store_id = '{sid}'
            UNION
            SELECT DISTINCT metric_date FROM nstags_hourly_traffic_pretty_canonical WHERE store_id = '{sid}'
            UNION
            SELECT DISTINCT metric_date FROM nstags_dwell_buckets_canonical WHERE store_id = '{sid}'
            UNION
            SELECT DISTINCT DATE(from_unixtime(ts) AT TIME ZONE '{TZ_NAME}') AS metric_date
            FROM nstags_live_analytics
            WHERE store_id = '{sid}'
            UNION
            SELECT DISTINCT DATE(from_unixtime(session_start_ts) AT TIME ZONE '{TZ_NAME}') AS metric_date
            FROM nstags_session_flat
            WHERE store_id = '{sid}'
        )
        SELECT metric_date
        FROM all_dates
        WHERE metric_date IS NOT NULL
        ORDER BY metric_date DESC
        """
    )


@st.cache_data(ttl=300)
def load_dashboard_daily_rows(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        SELECT
            metric_date,
            CAST(walk_by_traffic AS double) AS walk_by_traffic,
            CAST(store_interest AS double) AS store_interest,
            CAST(near_store AS double) AS near_store,
            CAST(store_visits AS double) AS store_visits,
            CAST(qualified_footfall AS double) AS qualified_footfall,
            CAST(engaged_visits AS double) AS engaged_visits,
            CAST(avg_dwell_seconds AS double) AS avg_dwell_seconds,
            CAST(median_dwell_seconds AS double) AS median_dwell_seconds
        FROM nstags_dashboard_metrics_canonical
        WHERE store_id = '{sid}'
          AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        ORDER BY metric_date
        """
    )


@st.cache_data(ttl=300)
def load_hourly_traffic_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        SELECT
            hour_of_day,
            format('%02d:00', hour_of_day) AS hour_label,
            ROUND(AVG(avg_far_devices), 2) AS avg_far_devices,
            ROUND(AVG(avg_mid_devices), 2) AS avg_mid_devices,
            ROUND(AVG(avg_near_devices), 2) AS avg_near_devices
        FROM nstags_hourly_traffic_pretty_canonical
        WHERE store_id = '{sid}'
          AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """
    )


@st.cache_data(ttl=300)
def load_dwell_buckets_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        SELECT
            dwell_bucket,
            ROUND(SUM(visits), 2) AS visits
        FROM nstags_dwell_buckets_canonical
        WHERE store_id = '{sid}'
          AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        GROUP BY dwell_bucket
        """
    )


@st.cache_data(ttl=300)
def load_brand_mix_hourly_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        SELECT
            hour(from_unixtime(ts) AT TIME ZONE '{TZ_NAME}') AS hour_of_day,
            format('%02d:00', hour(from_unixtime(ts) AT TIME ZONE '{TZ_NAME}')) AS hour_label,
            ROUND(AVG(apple_devices), 2) AS avg_apple_devices,
            ROUND(AVG(samsung_devices), 2) AS avg_samsung_devices,
            ROUND(AVG(other_devices), 2) AS avg_other_devices
        FROM nstags_live_analytics
        WHERE store_id = '{sid}'
          AND DATE(from_unixtime(ts) AT TIME ZONE '{TZ_NAME}') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        GROUP BY hour(from_unixtime(ts) AT TIME ZONE '{TZ_NAME}')
        ORDER BY hour_of_day
        """
    )


@st.cache_data(ttl=300)
def load_intelligence_scores_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    try:
        return run_athena_query(
            f"""
            SELECT
                ROUND(AVG(store_magnet_score), 4) AS store_magnet_score,
                ROUND(AVG(window_capture_index), 4) AS window_capture_index,
                ROUND(AVG(entry_efficiency_score), 4) AS entry_efficiency_score,
                ROUND(AVG(dwell_quality_index), 4) AS dwell_quality_index
            FROM nstags_intelligence_scores_canonical
            WHERE store_id = '{sid}'
              AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
            """
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_dynamic_index_scores_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    try:
        return run_athena_query(
            f"""
            SELECT
                ROUND(AVG(traffic_intelligence_index), 2) AS traffic_intelligence_index,
                ROUND(AVG(visit_quality_index), 2) AS visit_quality_index,
                ROUND(AVG(store_attraction_index), 2) AS store_attraction_index,
                ROUND(AVG(audience_quality_index), 2) AS audience_quality_index,
                ROUND(AVG(walk_by_score), 2) AS walk_by_score,
                ROUND(AVG(interest_score), 2) AS interest_score,
                ROUND(AVG(near_store_score), 2) AS near_store_score,
                ROUND(AVG(qualified_score), 2) AS qualified_score,
                ROUND(AVG(engaged_score), 2) AS engaged_score,
                ROUND(AVG(dwell_score), 2) AS dwell_score,
                ROUND(AVG(store_magnet_percentile_score), 2) AS store_magnet_percentile_score,
                ROUND(AVG(window_capture_score), 2) AS window_capture_score,
                ROUND(AVG(entry_efficiency_percentile_score), 2) AS entry_efficiency_percentile_score,
                ROUND(AVG(dwell_quality_score), 2) AS dwell_quality_score,
                ROUND(AVG(premium_device_mix_score), 2) AS premium_device_mix_score,
                ROUND(AVG(volume_confidence_score), 2) AS volume_confidence_score,
                MAX(COALESCE(benchmark_population, 0)) AS benchmark_population
            FROM nstags_index_scores_dynamic_canonical
            WHERE store_id = '{sid}'
              AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
            """
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_raw_date_diagnostics(store_id: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        WITH live_raw AS (
            SELECT
                store_id,
                year,
                month,
                day,
                DATE(from_unixtime(ts) AT TIME ZONE '{TZ_NAME}') AS metric_date_ist,
                COUNT(*) AS live_rows
            FROM nstags_live_analytics
            WHERE store_id = '{sid}'
            GROUP BY 1,2,3,4,5
        ),
        sess_raw AS (
            SELECT
                store_id,
                year,
                month,
                day,
                DATE(from_unixtime(session_start_ts) AT TIME ZONE '{TZ_NAME}') AS metric_date_ist,
                COUNT(*) AS session_rows
            FROM nstags_session_flat
            WHERE store_id = '{sid}'
            GROUP BY 1,2,3,4,5
        )
        SELECT 'live' AS source, store_id, year, month, day, metric_date_ist, live_rows AS rows
        FROM live_raw
        UNION ALL
        SELECT 'session' AS source, store_id, year, month, day, metric_date_ist, session_rows AS rows
        FROM sess_raw
        ORDER BY metric_date_ist DESC, source
        """
    )


# =========================================================
# HEADER
# =========================================================
st.markdown(
    """
    <div class="hero">
        <h1>nsTags Intelligence</h1>
        <p>Retail Operations · Retail Media Measurement · India-time canonical analytics</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# SIDEBAR
# =========================================================
with st.sidebar:
    st.subheader("Configuration")
    app_mode = st.radio("Business Mode", ["Retail Ops", "Retail Media"], horizontal=True)

    try:
        stores_df = load_store_list()
    except Exception as e:
        st.error(f"Failed to load store list: {e}")
        st.stop()

    if stores_df.empty:
        st.warning("No stores found in nstags_dashboard_metrics_canonical.")
        st.stop()

    selected_store = st.selectbox(
        "Active Store",
        stores_df["store_id"].dropna().astype(str).tolist(),
    )

    try:
        dates_df = load_available_dates(selected_store)
    except Exception as e:
        st.error(f"Failed to load available dates: {e}")
        st.stop()

    if dates_df.empty:
        st.warning("No dates found for this store.")
        st.stop()

    dates_df["metric_date"] = pd.to_datetime(dates_df["metric_date"]).dt.date
    available_dates = sorted(set(dates_df["metric_date"].dropna().tolist()))
    min_available_date = min(available_dates)
    max_available_date = max(available_dates)

    st.subheader("Period Selection")
    period_mode = st.radio("Analysis Window", ["Daily", "Weekly", "Monthly", "Yearly", "Custom"])

    if period_mode == "Daily":
        selected_day = st.selectbox(
            "Select Date",
            options=list(reversed(available_dates)),
            index=0,
            format_func=lambda d: d.strftime("%d %b %Y"),
        )
        start_date = selected_day
        end_date = selected_day
    elif period_mode == "Weekly":
        end_date = st.date_input("Week End Date", value=max_available_date, min_value=min_available_date, max_value=max_available_date)
        start_date = max(min_available_date, end_date - timedelta(days=6))
    elif period_mode == "Monthly":
        end_date = st.date_input("Month End Date", value=max_available_date, min_value=min_available_date, max_value=max_available_date)
        start_date = max(min_available_date, end_date - timedelta(days=29))
    elif period_mode == "Yearly":
        end_date = st.date_input("Year End Date", value=max_available_date, min_value=min_available_date, max_value=max_available_date)
        start_date = max(min_available_date, end_date - timedelta(days=364))
    else:
        default_start = max(min_available_date, max_available_date - timedelta(days=29))
        selected_range = st.date_input(
            "Custom Date Range",
            value=(default_start, max_available_date),
            min_value=min_available_date,
            max_value=max_available_date,
        )
        if isinstance(selected_range, tuple) and len(selected_range) == 2:
            start_date, end_date = selected_range
        elif isinstance(selected_range, list) and len(selected_range) == 2:
            start_date, end_date = selected_range[0], selected_range[1]
        else:
            start_date, end_date = default_start, max_available_date

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    st.subheader("Commercial Inputs")
    transactions = st.number_input("Transactions", min_value=0, value=35, step=1)
    business_value = st.number_input(
        "Revenue / Campaign Value (₹)", min_value=0.0, value=45000.0, step=1000.0
    )

# =========================================================
# DATA LOAD
# =========================================================
start_date_str = start_date.isoformat()
end_date_str = end_date.isoformat()

try:
    daily_df = load_dashboard_daily_rows(selected_store, start_date_str, end_date_str)
    hourly_df = load_hourly_traffic_range(selected_store, start_date_str, end_date_str)
    dwell_df = load_dwell_buckets_range(selected_store, start_date_str, end_date_str)
    brand_df = load_brand_mix_hourly_range(selected_store, start_date_str, end_date_str)
    intel_df = load_intelligence_scores_range(selected_store, start_date_str, end_date_str)
    dynamic_idx_df = load_dynamic_index_scores_range(selected_store, start_date_str, end_date_str)
except Exception as e:
    st.error(f"Failed to load Athena data: {e}")
    st.stop()

if daily_df.empty:
    st.error("No dashboard metrics were found for the selected period.")
    with st.expander("Debug: raw date diagnostics"):
        try:
            diag = load_raw_date_diagnostics(selected_store)
            st.dataframe(diag, use_container_width=True)
        except Exception as e:
            st.write(f"Diagnostics failed: {e}")
    st.stop()

# =========================================================
# AGGREGATION
# =========================================================
daily_df["metric_date"] = pd.to_datetime(daily_df["metric_date"]).dt.date
for col in [
    "walk_by_traffic", "store_interest", "near_store", "store_visits",
    "qualified_footfall", "engaged_visits", "avg_dwell_seconds", "median_dwell_seconds"
]:
    daily_df[col] = pd.to_numeric(daily_df[col], errors="coerce").fillna(0)

kpi = pd.Series({
    "walk_by_traffic": daily_df["walk_by_traffic"].mean(),
    "store_interest": daily_df["store_interest"].mean(),
    "near_store": daily_df["near_store"].mean(),
    "store_visits": daily_df["store_visits"].sum(),
    "qualified_footfall": daily_df["qualified_footfall"].sum(),
    "engaged_visits": daily_df["engaged_visits"].sum(),
    "avg_dwell_seconds": daily_df["avg_dwell_seconds"].mean(),
    "median_dwell_seconds": daily_df["median_dwell_seconds"].mean(),
})

qualified_rate = safe_div(kpi["qualified_footfall"], kpi["store_visits"])
engaged_rate = safe_div(kpi["engaged_visits"], kpi["store_visits"])
sales_conversion = safe_div(transactions, kpi["store_visits"])

if dynamic_idx_df.empty or dynamic_idx_df.isna().all(axis=None):
    idx_row = compute_local_fallback_indices(kpi, brand_df)
else:
    idx_row = dynamic_idx_df.iloc[0].fillna(0).to_dict()

if intel_df.empty or intel_df.isna().all(axis=None):
    intelligence_row = {
        "store_magnet_score": safe_div(kpi["store_interest"], kpi["walk_by_traffic"]),
        "window_capture_index": safe_div(kpi["store_visits"], kpi["store_interest"]),
        "entry_efficiency_score": qualified_rate,
        "dwell_quality_index": (engaged_rate * 0.6) + (min(safe_div(kpi["median_dwell_seconds"], 120), 1.0) * 0.4),
    }
else:
    intelligence_row = intel_df.iloc[0].fillna(0).to_dict()

benchmark_population = int(idx_row.get("benchmark_population", 0) or 0)
benchmark_stage, benchmark_text = benchmark_maturity_label(benchmark_population)
trend_grain = infer_trend_grain(start_date, end_date)
trend_df = build_period_trend(daily_df, trend_grain)

# =========================================================
# TOP SUMMARY
# =========================================================
st.caption(
    f"Active period: {start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')} | "
    f"Trend grain: {trend_grain.title()} | India timezone: {TZ_NAME}"
)

c1, c2, c3, c4 = st.columns(4)
with c1:
    metric_card("Traffic Intelligence Index", f"{idx_row.get('traffic_intelligence_index', 0):.0f}", score_band(idx_row.get("traffic_intelligence_index", 0)))
with c2:
    metric_card("Visit Quality Index", f"{idx_row.get('visit_quality_index', 0):.0f}", score_band(idx_row.get("visit_quality_index", 0)))
with c3:
    metric_card("Store Attraction Index", f"{idx_row.get('store_attraction_index', 0):.0f}", score_band(idx_row.get("store_attraction_index", 0)))
with c4:
    metric_card("Audience Quality Index", f"{idx_row.get('audience_quality_index', 0):.0f}", score_band(idx_row.get("audience_quality_index", 0)))

st.info(f"Benchmark maturity: **{benchmark_stage}** — {benchmark_text}")

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    metric_card("Walk-by Traffic", f"{kpi['walk_by_traffic']:.2f}", "Traffic intensity index")
with k2:
    metric_card("Store Interest", f"{kpi['store_interest']:.2f}", "Mid-zone attention intensity")
with k3:
    metric_card("Store Visits", fmt_int(kpi["store_visits"]), "Validated visit sessions")
with k4:
    metric_card("Qualified Rate", fmt_pct(qualified_rate), "Share of visits ≥ 30s")
with k5:
    if app_mode == "Retail Ops":
        metric_card("Sales Conversion", fmt_pct(sales_conversion), "Transactions / visits")
    else:
        cost_per_engaged = safe_div(business_value, max(kpi["engaged_visits"], 1))
        metric_card("Cost Per Engaged", f"₹{cost_per_engaged:,.0f}", "Value / engaged visits")

# =========================================================
# DIAGNOSTIC SECTION
# =========================================================
st.subheader("Diagnostics")
d1, d2, d3 = st.columns(3)
with d1:
    st.write(
        f"**Live traffic**  
"
        f"Walk-by: **{kpi['walk_by_traffic']:.2f}**  
"
        f"Interest: **{kpi['store_interest']:.2f}**  
"
        f"Near-store: **{kpi['near_store']:.2f}**"
    )
with d2:
    st.write(
        f"**Visit quality**  
"
        f"Qualified visits: **{fmt_int(kpi['qualified_footfall'])}**  
"
        f"Engaged visits: **{fmt_int(kpi['engaged_visits'])}**  
"
        f"Average dwell: **{fmt_seconds(kpi['avg_dwell_seconds'])}**"
    )
with d3:
    st.write(
        f"**Commercial**  
"
        f"Transactions: **{transactions:,}**  
"
        f"Value: **₹{business_value:,.0f}**  
"
        f"Visit-to-sale: **{fmt_pct(sales_conversion)}**"
    )

primary_bottleneck_scores = {
    "Store Magnet": float(idx_row.get("store_magnet_percentile_score", 0) or 0),
    "Window Capture": float(idx_row.get("window_capture_score", 0) or 0),
    "Entry Efficiency": float(idx_row.get("entry_efficiency_percentile_score", 0) or 0),
    "Dwell Quality": float(idx_row.get("dwell_quality_score", 0) or 0),
    "Floor Conversion": min(sales_conversion * 100 * 4, 100),
}
primary_bottleneck = min(primary_bottleneck_scores, key=primary_bottleneck_scores.get)
st.warning(f"Primary bottleneck detected: **{primary_bottleneck}**")

# =========================================================
# TABS
# =========================================================
tab0, tab1, tab2, tab3, tab4 = st.tabs([
    "Period Trend", "Index Breakdown", "Visits Funnel", "Traffic Trends", "Dwell & Brand Mix"
])

with tab0:
    if not trend_df.empty:
        trend_fig = go.Figure()
        trend_fig.add_trace(go.Scatter(x=trend_df["period_label"], y=trend_df["store_visits"], name="Store Visits", mode="lines+markers", line=dict(color=COLORS["indigo"], width=3)))
        trend_fig.add_trace(go.Scatter(x=trend_df["period_label"], y=trend_df["qualified_footfall"], name="Qualified Visits", mode="lines+markers", line=dict(color=COLORS["emerald"], width=2)))
        trend_fig.add_trace(go.Scatter(x=trend_df["period_label"], y=trend_df["engaged_visits"], name="Engaged Visits", mode="lines+markers", line=dict(color=COLORS["amber"], width=2)))
        style_chart(trend_fig)
        st.plotly_chart(trend_fig, use_container_width=True, config=PLOT_CONFIG)
        st.dataframe(trend_df, use_container_width=True)
    else:
        st.info("No trend rows available for the selected period.")

with tab1:
    score_items = pd.DataFrame([
        ["Walk-by", idx_row.get("walk_by_score", 0)],
        ["Interest", idx_row.get("interest_score", 0)],
        ["Near-store", idx_row.get("near_store_score", 0)],
        ["Qualified", idx_row.get("qualified_score", 0)],
        ["Engaged", idx_row.get("engaged_score", 0)],
        ["Dwell", idx_row.get("dwell_score", 0)],
        ["Store Magnet", idx_row.get("store_magnet_percentile_score", 0)],
        ["Window Capture", idx_row.get("window_capture_score", 0)],
        ["Entry Efficiency", idx_row.get("entry_efficiency_percentile_score", 0)],
        ["Dwell Quality", idx_row.get("dwell_quality_score", 0)],
        ["Premium Device Mix", idx_row.get("premium_device_mix_score", 0)],
        ["Volume Confidence", idx_row.get("volume_confidence_score", 0)],
    ], columns=["Metric", "Score"])
    fig = px.bar(score_items, x="Score", y="Metric", orientation="h", title="Normalized score components (/100)")
    style_chart(fig)
    st.plotly_chart(fig, use_container_width=True, config=PLOT_CONFIG)

    s1, s2, s3, s4 = st.columns(4)
    with s1:
        metric_card("Store Magnet", f"{float(intelligence_row.get('store_magnet_score', 0))*100:.0f}" if float(intelligence_row.get('store_magnet_score', 0) or 0) <= 1.5 else f"{float(intelligence_row.get('store_magnet_score', 0)):.0f}", "Interest ÷ walk-by")
    with s2:
        metric_card("Window Capture", f"{float(intelligence_row.get('window_capture_index', 0))*100:.0f}" if float(intelligence_row.get('window_capture_index', 0) or 0) <= 1.5 else f"{float(intelligence_row.get('window_capture_index', 0)):.0f}", "Visits ÷ interest")
    with s3:
        metric_card("Entry Efficiency", f"{float(intelligence_row.get('entry_efficiency_score', 0))*100:.0f}" if float(intelligence_row.get('entry_efficiency_score', 0) or 0) <= 1.5 else f"{float(intelligence_row.get('entry_efficiency_score', 0)):.0f}", "Qualified ÷ visits")
    with s4:
        metric_card("Dwell Quality", f"{float(intelligence_row.get('dwell_quality_index', 0))*100:.0f}" if float(intelligence_row.get('dwell_quality_index', 0) or 0) <= 1.5 else f"{float(intelligence_row.get('dwell_quality_index', 0)):.0f}", "Engagement + dwell")

with tab2:
    funnel_labels = ["Walk-by", "Interest", "Near-store", "Store Visits", "Qualified", "Engaged"]
    funnel_values = [
        float(kpi["walk_by_traffic"]),
        float(kpi["store_interest"]),
        float(kpi["near_store"]),
        float(kpi["store_visits"]),
        float(kpi["qualified_footfall"]),
        float(kpi["engaged_visits"]),
    ]
    fig = go.Figure(go.Funnel(y=funnel_labels, x=funnel_values, textposition="inside", textinfo="value+percent previous"))
    fig.update_layout(title="Visits funnel (transactions remain a manual business input)")
    style_chart(fig)
    st.plotly_chart(fig, use_container_width=True, config=PLOT_CONFIG)

with tab3:
    if not hourly_df.empty:
        hourly_fig = go.Figure()
        hourly_fig.add_trace(go.Scatter(x=hourly_df["hour_label"], y=hourly_df["avg_far_devices"], name="Walk-by", mode="lines+markers", line=dict(color=COLORS["sky"], width=2)))
        hourly_fig.add_trace(go.Scatter(x=hourly_df["hour_label"], y=hourly_df["avg_mid_devices"], name="Interest", mode="lines+markers", line=dict(color=COLORS["indigo"], width=2)))
        hourly_fig.add_trace(go.Scatter(x=hourly_df["hour_label"], y=hourly_df["avg_near_devices"], name="Near-store", mode="lines+markers", line=dict(color=COLORS["emerald"], width=2)))
        hourly_fig.update_layout(title="Hourly traffic in IST")
        style_chart(hourly_fig)
        st.plotly_chart(hourly_fig, use_container_width=True, config=PLOT_CONFIG)
    else:
        st.info("No hourly traffic rows for the selected period.")

with tab4:
    left, right = st.columns(2)
    with left:
        if not dwell_df.empty:
            dwell_order = ["00-10s", "10-30s", "30-60s", "01-03m", "03-05m", "05m+"]
            dwell_df["dwell_bucket"] = pd.Categorical(dwell_df["dwell_bucket"], categories=dwell_order, ordered=True)
            dwell_df = dwell_df.sort_values("dwell_bucket")
            dwell_fig = px.bar(dwell_df, x="dwell_bucket", y="visits", title="Dwell buckets")
            style_chart(dwell_fig)
            st.plotly_chart(dwell_fig, use_container_width=True, config=PLOT_CONFIG)
        else:
            st.info("No dwell data for the selected period.")

    with right:
        if not brand_df.empty:
            brand_fig = go.Figure()
            brand_fig.add_trace(go.Bar(x=brand_df["hour_label"], y=brand_df["avg_apple_devices"], name="Apple"))
            brand_fig.add_trace(go.Bar(x=brand_df["hour_label"], y=brand_df["avg_samsung_devices"], name="Samsung"))
            brand_fig.add_trace(go.Bar(x=brand_df["hour_label"], y=brand_df["avg_other_devices"], name="Other"))
            brand_fig.update_layout(barmode="stack", title="Hourly brand mix (proxy)")
            style_chart(brand_fig)
            st.plotly_chart(brand_fig, use_container_width=True, config=PLOT_CONFIG)
        else:
            st.info("No brand mix data for the selected period.")

# =========================================================
# DEBUG SECTION
# =========================================================
with st.expander("Debug: raw date diagnostics and loaded daily rows"):
    try:
        st.write("Daily rows loaded from nstags_dashboard_metrics_canonical")
        st.dataframe(daily_df, use_container_width=True)
        st.write("Raw partition-date vs IST-date diagnostics")
        st.dataframe(load_raw_date_diagnostics(selected_store), use_container_width=True)
    except Exception as e:
        st.write(f"Diagnostics unavailable: {e}")

st.caption("nsTags Intelligence · Canonical IST views only · Athena + Streamlit")
