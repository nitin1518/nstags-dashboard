import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from io import StringIO
from typing import Any

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from botocore.exceptions import ClientError

try:
    from google import genai
except Exception:
    genai = None


# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="nsTags Retail Intelligence",
    page_icon="📈",
    layout="wide",
)


# =========================================================
# APP CONFIG
# =========================================================
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "nstags_analytics")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT = os.environ.get(
    "ATHENA_OUTPUT",
    "s3://nstags-datalake-hq-2026/athena-results/",
)
TIMEZONE = os.environ.get("TIMEZONE", "Asia/Kolkata")
ATHENA_RESULT_REUSE_MINUTES = int(os.environ.get("ATHENA_RESULT_REUSE_MINUTES", "30"))
APP_QUERY_TIMEOUT = int(os.environ.get("APP_QUERY_TIMEOUT", "35"))
APP_HEAVY_QUERY_TIMEOUT = int(os.environ.get("APP_HEAVY_QUERY_TIMEOUT", "20"))
AI_CACHE_TTL = int(os.environ.get("AI_CACHE_TTL", "900"))
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
SHOW_DEBUG = os.environ.get("SHOW_DEBUG", "0") == "1"

AVAILABLE_DATES_TABLES = ["nstags_available_dates_curated_inc"]
DASHBOARD_TABLES = ["nstags_dashboard_daily_curated_inc"]
HOURLY_TABLES = ["nstags_hourly_traffic_curated_inc"]
DWELL_TABLES = ["nstags_dwell_buckets_curated_inc"]

athena_client = boto3.client("athena", region_name=AWS_REGION)


# =========================================================
# STYLE
# =========================================================
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

:root {
    --bg: #F8FAFC;
    --bg-soft: #EEF2FF;
    --panel: #FFFFFF;
    --panel-2: #F8FAFC;
    --border: rgba(99,102,241,0.14);
    --border-strong: rgba(99,102,241,0.28);
    --text: #0F172A;
    --text-2: #334155;
    --text-3: #475569;
    --text-muted: #64748B;
    --accent: #6366F1;
    --accent-2: #8B5CF6;
    --good: #10B981;
    --warn: #F59E0B;
    --bad: #F43F5E;
    --shadow: 0 12px 36px rgba(15,23,42,0.08);
    --shadow-soft: 0 8px 24px rgba(15,23,42,0.06);
    --card-grad: linear-gradient(145deg, #FFFFFF 0%, #F8FAFC 100%);
}

html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    background: var(--bg) !important;
    color: var(--text) !important;
}

.main .block-container {
    max-width: 100% !important;
    padding: 1rem 1.1rem 2rem 1.1rem !important;
}

section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, var(--panel) 0%, var(--bg-soft) 100%) !important;
    border-right: 1px solid var(--border) !important;
}

.hero {
    background:
        radial-gradient(circle at top right, rgba(99,102,241,0.13), transparent 35%),
        linear-gradient(135deg, rgba(99,102,241,0.11) 0%, rgba(16,185,129,0.05) 55%, rgba(99,102,241,0.02) 100%);
    border: 1px solid var(--border-strong);
    border-radius: 24px;
    padding: 1.2rem 1.3rem 1.1rem 1.3rem;
    margin-bottom: 0.95rem;
    box-shadow: var(--shadow-soft);
}

.hero h1 {
    font-size: 1.85rem;
    margin: 0;
    line-height: 1.05;
    letter-spacing: -0.03em;
    color: var(--text);
}

.hero p {
    margin: .42rem 0 0 0;
    color: var(--text-3);
    line-height: 1.55;
    font-size: 0.95rem;
}

.eyebrow {
    text-transform: uppercase;
    letter-spacing: 0.14em;
    font-size: .72rem;
    font-weight: 800;
    color: var(--accent);
    margin-bottom: .45rem;
}

.section-title {
    margin: 1.05rem 0 .65rem 0;
    font-size: .76rem;
    text-transform: uppercase;
    letter-spacing: .13em;
    color: var(--accent);
    font-weight: 800;
}

.panel, .info-card, .story-card, .priority-card, .kpi-card, .summary-strip, .simple-callout {
    background: var(--card-grad);
    border: 1px solid var(--border);
    border-radius: 18px;
    box-shadow: var(--shadow-soft);
}

.panel {
    padding: 1rem 1rem .9rem 1rem;
    margin-bottom: 1rem;
}

.kpi-card {
    padding: 1rem 1rem .92rem 1rem;
    min-height: 165px;
    position: relative;
    overflow: hidden;
    margin-bottom: .9rem;
}

.kpi-card::after {
    content:'';
    position:absolute;
    top:-36px;
    right:-36px;
    width:92px;
    height:92px;
    background: radial-gradient(circle, rgba(99,102,241,0.08), transparent 70%);
    border-radius: 50%;
}

.kpi-label {
    font-size: .7rem;
    text-transform: uppercase;
    letter-spacing: .1em;
    font-weight: 800;
    color: var(--text-muted);
    margin-bottom: .35rem;
}

.kpi-value {
    font-size: 2rem;
    font-weight: 800;
    letter-spacing: -0.03em;
    color: var(--text);
    line-height: 1.02;
    margin-bottom: .25rem;
}

.kpi-sub {
    color: var(--text-3);
    font-size: .84rem;
    line-height: 1.5;
    margin-top: .35rem;
}

.kpi-formula {
    margin-top: .62rem;
    padding-top: .55rem;
    border-top: 1px dashed rgba(99,102,241,0.16);
    color: var(--text-muted);
    font-size: .76rem;
    line-height: 1.45;
}

.priority-card {
    border-left: 4px solid var(--accent);
    padding: 1rem 1rem .95rem 1rem;
    min-height: 184px;
    margin-bottom: .9rem;
}

.priority-label, .story-label, .info-title {
    font-size: .7rem;
    text-transform: uppercase;
    letter-spacing: .11em;
    font-weight: 800;
    color: var(--accent);
    margin-bottom: .45rem;
}

.priority-title, .story-title {
    font-size: 1.08rem;
    font-weight: 800;
    color: var(--text);
    line-height: 1.25;
    margin-bottom: .5rem;
}

.priority-body, .story-body, .info-body {
    color: var(--text-3);
    font-size: .88rem;
    line-height: 1.6;
}

.info-card {
    padding: .95rem 1rem;
    margin-bottom: .9rem;
}

.summary-strip, .simple-callout {
    padding: .9rem 1rem;
    margin-bottom: 1rem;
    color: var(--text-3);
    font-size: .9rem;
    line-height: 1.55;
}

.metric-pill {
    display: inline-block;
    padding: .2rem .52rem;
    border-radius: 999px;
    background: rgba(99,102,241,0.08);
    color: var(--accent);
    font-size: .72rem;
    font-weight: 700;
    margin-right: .35rem;
    margin-bottom: .38rem;
}

.badge-good, .badge-warn, .badge-bad, .badge-info {
    display:inline-block;
    padding:.12rem .48rem;
    border-radius:8px;
    font-size:.74rem;
    font-weight:800;
}

.badge-good { background: rgba(16,185,129,0.10); color: var(--good); }
.badge-warn { background: rgba(245,158,11,0.10); color: var(--warn); }
.badge-bad  { background: rgba(244,63,94,0.10); color: var(--bad); }
.badge-info { background: rgba(99,102,241,0.10); color: var(--accent); }

.small-muted {
    color: var(--text-muted);
    font-size: .78rem;
}

div[data-testid="stMetric"] {
    background: var(--card-grad);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: .6rem .7rem;
}

code {
    white-space: pre-wrap;
}
</style>
""",
    unsafe_allow_html=True,
)


# =========================================================
# DATA MODELS
# =========================================================
@dataclass
class Scope:
    label: str
    period_key: str
    start_date: date
    end_date: date
    days_in_scope: int


@dataclass
class DashboardMetrics:
    impressions: float = 0.0
    interest: float = 0.0
    near_store: float = 0.0
    store_visits: float = 0.0
    total_sessions: float = 0.0
    qualified_visits: float = 0.0
    engaged_visits: float = 0.0
    deeply_engaged_visits: float = 0.0
    avg_dwell_seconds: float = 0.0
    median_dwell_seconds: float = 0.0
    transactions: float = 0.0
    revenue: float = 0.0
    commercial_ratio: float = 0.0
    traffic_health_band: str = "Early"
    visit_quality_band: str = "Early"
    audience_quality_index: float = 0.0
    benchmark_days: int = 0
    source_table: str = ""
    notes: list[str] | None = None


# =========================================================
# HELPERS
# =========================================================
def q(s: str) -> str:
    return s.replace("'", "''")


def fmt_int(v: Any) -> str:
    try:
        return f"{int(round(float(v))):,}"
    except Exception:
        return "0"


def fmt_float(v: Any, digits: int = 1) -> str:
    try:
        return f"{float(v):,.{digits}f}"
    except Exception:
        return f"{0:.{digits}f}"


def fmt_currency(v: Any) -> str:
    try:
        return f"₹{float(v):,.0f}"
    except Exception:
        return "₹0"


def fmt_pct(v: Any) -> str:
    try:
        return f"{float(v):.1f}%"
    except Exception:
        return "0.0%"


def fmt_pct_from_ratio(v: Any) -> str:
    try:
        return f"{float(v) * 100:.1f}%"
    except Exception:
        return "0.0%"


def fmt_seconds(seconds: Any) -> str:
    try:
        seconds = float(seconds)
    except Exception:
        return "0s"
    if seconds < 60:
        return f"{int(round(seconds))}s"
    mins = int(seconds // 60)
    secs = int(round(seconds % 60))
    if mins < 60:
        return f"{mins}m {secs}s"
    hrs = mins // 60
    mins = mins % 60
    return f"{hrs}h {mins}m"


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def choose_text_mode(series: pd.Series, default: str = "") -> str:
    try:
        s = series.dropna().astype(str)
        if s.empty:
            return default
        return s.mode().iloc[0]
    except Exception:
        return default


def badge_class_for_label(label: str) -> str:
    label = str(label).strip().lower()
    if label in {"strong", "excellent", "high"}:
        return "badge-good"
    if label in {"moderate", "average", "early"}:
        return "badge-warn"
    return "badge-bad"


# =========================================================
# ATHENA
# =========================================================
def run_athena_query(
    query: str,
    database: str = ATHENA_DATABASE,
    timeout_sec: int = APP_QUERY_TIMEOUT,
) -> pd.DataFrame:
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
            WorkGroup=ATHENA_WORKGROUP,
            ResultReuseConfiguration={
                "ResultReuseByAgeConfiguration": {
                    "Enabled": True,
                    "MaxAgeInMinutes": ATHENA_RESULT_REUSE_MINUTES,
                }
            },
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
            try:
                athena_client.stop_query_execution(QueryExecutionId=execution_id)
            except Exception:
                pass
            raise RuntimeError("Athena query timed out")

        time.sleep(1)

    if state != "SUCCEEDED":
        reason = execution["QueryExecution"]["Status"].get(
            "StateChangeReason",
            "Unknown error",
        )
        raise RuntimeError(f"Athena query failed: {reason}")

    result_path = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    if not result_path.startswith("s3://"):
        raise RuntimeError("Athena result path is invalid")

    bucket_key = result_path.replace("s3://", "", 1)
    bucket, _, key = bucket_key.partition("/")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read().decode("utf-8")

    if not raw.strip():
        return pd.DataFrame()

    return pd.read_csv(StringIO(raw))


@st.cache_data(ttl=300, show_spinner=False)
def cached_athena_query(query: str, timeout_sec: int = APP_QUERY_TIMEOUT) -> pd.DataFrame:
    return run_athena_query(query, timeout_sec=timeout_sec)


def try_query_candidates(
    candidates: list[str],
    timeout_sec: int = APP_QUERY_TIMEOUT,
) -> tuple[pd.DataFrame, str]:
    last_error = None
    for query in candidates:
        try:
            return cached_athena_query(query, timeout_sec=timeout_sec), query
        except Exception as e:
            last_error = e
    raise RuntimeError(str(last_error) if last_error else "No query candidates succeeded")


# =========================================================
# SQL BUILDERS
# =========================================================
def build_available_dates_queries() -> list[str]:
    queries: list[str] = []
    for table in AVAILABLE_DATES_TABLES:
        queries.append(
            f"""
            SELECT DISTINCT store_id, CAST(metric_date AS DATE) AS metric_date
            FROM {table}
            WHERE metric_date IS NOT NULL
            ORDER BY metric_date DESC, store_id
            LIMIT 5000
            """
        )
    return queries


def build_dashboard_queries(store_id: str, scope: Scope) -> list[str]:
    queries: list[str] = []
    for table in DASHBOARD_TABLES:
        queries.append(
            f"""
            SELECT *
            FROM {table}
            WHERE store_id = '{q(store_id)}'
              AND CAST(metric_date AS DATE) BETWEEN DATE '{scope.start_date.isoformat()}' AND DATE '{scope.end_date.isoformat()}'
            ORDER BY metric_date
            """
        )
    return queries


def build_hourly_queries(store_id: str, scope: Scope) -> list[str]:
    queries: list[str] = []
    for table in HOURLY_TABLES:
        queries.append(
            f"""
            SELECT *
            FROM {table}
            WHERE store_id = '{q(store_id)}'
              AND CAST(metric_date AS DATE) BETWEEN DATE '{scope.start_date.isoformat()}' AND DATE '{scope.end_date.isoformat()}'
            ORDER BY metric_date, hour_of_day
            """
        )
    return queries


def build_dwell_queries(store_id: str, scope: Scope) -> list[str]:
    queries: list[str] = []
    for table in DWELL_TABLES:
        queries.append(
            f"""
            SELECT *
            FROM {table}
            WHERE store_id = '{q(store_id)}'
              AND CAST(metric_date AS DATE) BETWEEN DATE '{scope.start_date.isoformat()}' AND DATE '{scope.end_date.isoformat()}'
            ORDER BY metric_date, dwell_bucket
            """
        )
    return queries


# =========================================================
# DATE / PERIOD LOGIC
# =========================================================
def get_latest_available_dates() -> pd.DataFrame:
    df, _ = try_query_candidates(
        build_available_dates_queries(),
        timeout_sec=APP_HEAVY_QUERY_TIMEOUT,
    )
    if df.empty:
        return pd.DataFrame(columns=["store_id", "metric_date"])

    df["metric_date"] = pd.to_datetime(df["metric_date"]).dt.date
    return df


def compute_scope(
    period_key: str,
    selected_date: date,
    custom_start: date | None = None,
    custom_end: date | None = None,
) -> Scope:
    if period_key == "Daily":
        return Scope("Daily snapshot", "Daily", selected_date, selected_date, 1)

    if period_key == "Weekly":
        start = selected_date - timedelta(days=6)
        return Scope(
            f"Last 7 days · {start.strftime('%d %b')} → {selected_date.strftime('%d %b %Y')}",
            "Weekly",
            start,
            selected_date,
            7,
        )

    if period_key == "Monthly":
        start = selected_date - timedelta(days=29)
        return Scope(
            f"Last 30 days · {start.strftime('%d %b')} → {selected_date.strftime('%d %b %Y')}",
            "Monthly",
            start,
            selected_date,
            30,
        )

    if period_key == "Yearly":
        start = selected_date - timedelta(days=364)
        return Scope(
            f"Last 365 days · {start.strftime('%d %b %Y')} → {selected_date.strftime('%d %b %Y')}",
            "Yearly",
            start,
            selected_date,
            365,
        )

    custom_start = custom_start or selected_date
    custom_end = custom_end or selected_date
    if custom_start > custom_end:
        custom_start, custom_end = custom_end, custom_start

    return Scope(
        f"Custom · {custom_start.strftime('%d %b %Y')} → {custom_end.strftime('%d %b %Y')}",
        "Custom",
        custom_start,
        custom_end,
        (custom_end - custom_start).days + 1,
    )


# =========================================================
# METRIC DERIVATION
# =========================================================
def aggregate_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    for col in out.columns:
        try:
            out[col] = pd.to_numeric(out[col])
        except Exception:
            pass
    return out


def compute_overall_brand_mix(hourly_df: pd.DataFrame) -> pd.DataFrame:
    if hourly_df.empty:
        return pd.DataFrame(columns=["brand", "value"])

    agg = {
        "Apple": safe_float(hourly_df["avg_apple_devices"].sum()) if "avg_apple_devices" in hourly_df.columns else 0.0,
        "Samsung": safe_float(hourly_df["avg_samsung_devices"].sum()) if "avg_samsung_devices" in hourly_df.columns else 0.0,
        "Other": safe_float(hourly_df["avg_other_devices"].sum()) if "avg_other_devices" in hourly_df.columns else 0.0,
    }
    return pd.DataFrame({"brand": list(agg.keys()), "value": list(agg.values())})


def derive_from_dashboard_table(dash_df: pd.DataFrame) -> dict[str, float | str]:
    if dash_df.empty:
        return {}

    df = aggregate_frame(dash_df)

    return {
        "impressions": safe_float(df["walk_by_traffic"].sum()) if "walk_by_traffic" in df.columns else 0.0,
        "interest": safe_float(df["store_interest"].sum()) if "store_interest" in df.columns else 0.0,
        "near_store": safe_float(df["near_store"].sum()) if "near_store" in df.columns else 0.0,
        "store_visits": safe_float(df["store_visits"].sum()) if "store_visits" in df.columns else 0.0,
        "total_sessions": safe_float(df["total_sessions"].sum()) if "total_sessions" in df.columns else 0.0,
        "qualified_visits": safe_float(df["qualified_footfall"].sum()) if "qualified_footfall" in df.columns else 0.0,
        "engaged_visits": safe_float(df["engaged_visits"].sum()) if "engaged_visits" in df.columns else 0.0,
        "deeply_engaged_visits": safe_float(df["deeply_engaged_visits"].sum()) if "deeply_engaged_visits" in df.columns else 0.0,
        "avg_dwell_seconds": safe_float(df["avg_dwell_seconds"].mean()) if "avg_dwell_seconds" in df.columns else 0.0,
        "median_dwell_seconds": safe_float(df["median_dwell_seconds"].mean()) if "median_dwell_seconds" in df.columns else 0.0,
        "traffic_health_band": choose_text_mode(df["traffic_health_band"], "Early") if "traffic_health_band" in df.columns else "Early",
        "visit_quality_band": choose_text_mode(df["visit_quality_band"], "Early") if "visit_quality_band" in df.columns else "Early",
    }


def build_metrics(dash_df: pd.DataFrame, transactions: float, revenue: float) -> DashboardMetrics:
    notes: list[str] = []
    dash = derive_from_dashboard_table(dash_df)

    impressions = safe_float(dash.get("impressions", 0.0))
    store_visits = safe_float(dash.get("store_visits", 0.0))
    qualified_visits = safe_float(dash.get("qualified_visits", 0.0))
    engaged_visits = safe_float(dash.get("engaged_visits", 0.0))
    deeply_engaged_visits = safe_float(dash.get("deeply_engaged_visits", 0.0))
    avg_dwell_seconds = safe_float(dash.get("avg_dwell_seconds", 0.0))
    median_dwell_seconds = safe_float(dash.get("median_dwell_seconds", 0.0))
    interest = safe_float(dash.get("interest", 0.0))
    near_store = safe_float(dash.get("near_store", 0.0))
    total_sessions = safe_float(dash.get("total_sessions", 0.0))

    commercial_ratio = transactions / store_visits if store_visits > 0 else 0.0
    qualified_rate = qualified_visits / store_visits if store_visits > 0 else 0.0
    engaged_rate = engaged_visits / store_visits if store_visits > 0 else 0.0
    audience_quality_index = ((qualified_rate * 0.6) + (engaged_rate * 0.4)) * 100

    notes.append(
        "Impressions are session-based BLE storefront exposure sessions for the selected period. "
        "They should be interpreted as retail audience opportunity, not exact human headcount."
    )
    notes.append(
        "Store visits, qualified visits, engaged visits, and deep engagement are all session-derived layers, "
        "so funnel comparisons are now directionally consistent."
    )

    return DashboardMetrics(
        impressions=impressions,
        interest=interest,
        near_store=near_store,
        store_visits=store_visits,
        total_sessions=total_sessions,
        qualified_visits=qualified_visits,
        engaged_visits=engaged_visits,
        deeply_engaged_visits=deeply_engaged_visits,
        avg_dwell_seconds=avg_dwell_seconds,
        median_dwell_seconds=median_dwell_seconds,
        transactions=transactions,
        revenue=revenue,
        commercial_ratio=commercial_ratio,
        traffic_health_band=str(dash.get("traffic_health_band", "Early")),
        visit_quality_band=str(dash.get("visit_quality_band", "Early")),
        audience_quality_index=audience_quality_index,
        benchmark_days=len(dash_df),
        source_table="nstags_dashboard_daily_curated_inc",
        notes=notes,
    )


# =========================================================
# NARRATIVE HELPERS
# =========================================================
def commercial_ratio_label(mode: str) -> str:
    return "Commercial Response Ratio" if mode == "Retail Media" else "Transactions / Visit Ratio"


def commercial_ratio_description(mode: str) -> str:
    if mode == "Retail Media":
        return (
            "Business response generated against the selected period's in-store demand. "
            "Read as response intensity, not a literal person-to-response conversion."
        )
    return (
        "Business outcome relative to store visits. Read this with visit quality and dwell, "
        "not as a stand-alone judgment."
    )


def build_store_summary_sentence(metrics: DashboardMetrics) -> str:
    capture_idx = metrics.store_visits / metrics.impressions if metrics.impressions > 0 else 0.0
    return (
        f"The store recorded {fmt_int(metrics.store_visits)} visit sessions during the selected period, "
        f"against {fmt_int(metrics.impressions)} impressions and average dwell of {fmt_seconds(metrics.avg_dwell_seconds)}. "
        f"Visit conversion is {fmt_pct_from_ratio(capture_idx)}, which now reflects visit sessions against session-based impressions."
    )


def identify_primary_bottleneck(metrics: DashboardMetrics) -> str:
    capture = metrics.store_visits / metrics.impressions if metrics.impressions > 0 else 0.0
    qual = metrics.qualified_visits / metrics.store_visits if metrics.store_visits > 0 else 0.0
    engage = metrics.engaged_visits / metrics.store_visits if metrics.store_visits > 0 else 0.0
    close = metrics.transactions / metrics.store_visits if metrics.store_visits > 0 else 0.0

    layers = {
        "Store Magnet": capture,
        "Entry Efficiency": qual,
        "Dwell Quality": engage,
        "Floor Conversion": close,
    }
    return min(layers, key=layers.get)


def get_priority_narratives(mode: str, primary_bottleneck: str) -> dict[str, str]:
    if mode == "Retail Media":
        traffic_title = "Attention quality is the first commercial filter"
        traffic_body = (
            "For a media-led view, surrounding traffic alone is not enough. What matters first is how much of that audience "
            "slows down, notices the storefront, and becomes measurable store exposure."
        )
        visit_title = "Audience depth determines campaign usefulness"
        visit_body = (
            "A strong audience window is only valuable when visits become meaningful interactions. "
            "Dwell and engagement show whether the location is delivering shallow exposure or stronger brand consideration."
        )
        commercial_title = "Commercial response should be read as outcome intensity"
        commercial_body = (
            "Transactions / responses represent the business reaction captured against in-store demand. "
            "Treat this as commercial response intensity for the selected period."
        )
    else:
        traffic_title = "Storefront pull is the first business gate"
        traffic_body = (
            "The first question for store owners is whether the store is converting impressions into store visits. "
            "If people are present around the storefront but do not transition into visits, later selling excellence cannot fully compensate."
        )
        visit_title = "Visit quality is the clearest indicator of store health"
        visit_body = (
            "High-quality visits are the real operating opportunity. Qualified and engaged visits show whether entry is turning into "
            "serious browsing, assistance, and purchase intent rather than shallow walk-ins."
        )
        commercial_title = "Commercial outcome must be read with context"
        commercial_body = (
            "Transactions reflect business outcome for the selected period. This should be interpreted alongside visit quality, "
            "because closure depends on both entry quality and in-store experience."
        )

    priority_map = {
        "Store Magnet": f"{traffic_title} Current impression-to-visit conversion is the weakest layer, so the biggest opportunity is improving storefront stopping power and front-of-store attention.",
        "Entry Efficiency": f"{visit_title} Entry is happening, but too many visits are not becoming meaningful store interactions.",
        "Dwell Quality": f"{visit_title} Visitors are entering, but not staying long enough to reflect stronger browsing or deeper engagement.",
        "Floor Conversion": f"{commercial_title} The store is generating traffic and visit quality, but business closure is the weakest layer right now.",
    }

    return {
        "traffic_title": traffic_title,
        "traffic_body": traffic_body,
        "visit_title": visit_title,
        "visit_body": visit_body,
        "commercial_title": commercial_title,
        "commercial_body": commercial_body,
        "primary_bottleneck_story": priority_map.get(primary_bottleneck, "This is currently the weakest business layer in the store journey."),
    }


# =========================================================
# AI BRIEF
# =========================================================
@st.cache_data(ttl=AI_CACHE_TTL, show_spinner=False)
def generate_ai_brief(ai_payload: dict) -> str:
    if not GEMINI_API_KEY or genai is None:
        return """
<div class="simple-callout">
    <strong>AI Brief currently unavailable</strong><br><br>
    The dashboard is working normally, but the AI summary is not configured right now.
    Please add a valid Gemini API key in deployment secrets to enable this feature.
</div>
"""

    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        prompt = f"""
You are a senior retail strategy consultant writing for business leaders.
Do not mention sensors, BLE, Athena, pipelines, device scanning, or any backend system.

BUSINESS CONTEXT
Mode: {ai_payload['mode']}
Scope: {ai_payload['scope']}

PERFORMANCE METRICS
Impressions: {ai_payload['impressions']}
Storefront interest: {ai_payload['interest']}
Near-zone traffic: {ai_payload['near_store']}
Store visits: {ai_payload['visits']}
Qualified visits: {ai_payload['qualified_visits']}
Engaged visits: {ai_payload['engaged_visits']}
Qualified visit rate: {ai_payload['qualified_rate']}%
Engaged visit rate: {ai_payload['engaged_rate']}%
Average dwell: {ai_payload['avg_dwell']}
Transactions / response events: {ai_payload['transactions']}
Revenue / campaign value: {ai_payload['value']}
Commercial ratio: {ai_payload['commercial_ratio']}%
Audience quality index: {ai_payload['aqi']}
Primary opportunity: {ai_payload['primary_bottleneck']}

Important:
Impressions are directional session-based storefront audience exposure, not literal unique human count.
Do not call it exact footfall conversion.

Return exactly in this format:

**Executive Summary**
[2-3 sentences]

**Top Priority**
[1 short paragraph]

**Traffic Interpretation**
[1 short paragraph]

**Visit Quality Interpretation**
[1 short paragraph]

**Commercial Interpretation**
[1 short paragraph]

**Recommended Action**
- [bullet 1]
- [bullet 2]
- [bullet 3]
"""
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )
        return response.text

    except Exception as e:
        err = str(e).lower()

        if "429" in err or "resource_exhausted" in err or "quota" in err:
            return """
<div class="simple-callout">
    <strong>AI Brief temporarily unavailable</strong><br><br>
    The AI summary limit has been reached for now. Please try again a little later.
    Your dashboard metrics and charts are still fully available.
</div>
"""

        return """
<div class="simple-callout">
    <strong>AI Brief unavailable right now</strong><br><br>
    Something went wrong while generating the summary. Please try again shortly.
    The dashboard metrics are still available below.
</div>
"""


# =========================================================
# UI HELPERS
# =========================================================
def render_card(label: str, value: str, sub_html: str, formula: str = "") -> None:
    st.markdown(
        f"""
        <div class='kpi-card'>
            <div class='kpi-label'>{label}</div>
            <div class='kpi-value'>{value}</div>
            <div class='kpi-sub'>{sub_html}</div>
            {f"<div class='kpi-formula'>{formula}</div>" if formula else ""}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_priority_card(label: str, title: str, body: str) -> None:
    st.markdown(
        f"""
        <div class='priority-card'>
            <div class='priority-label'>{label}</div>
            <div class='priority-title'>{title}</div>
            <div class='priority-body'>{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_info_card(title: str, body: str) -> None:
    st.markdown(
        f"""
        <div class='info-card'>
            <div class='info-title'>{title}</div>
            <div class='info-body'>{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =========================================================
# CHARTS
# =========================================================
def make_exposure_funnel_figure(metrics: DashboardMetrics) -> go.Figure:
    fig = go.Figure(
        go.Funnel(
            y=[
                "Impressions",
                "Storefront Interest",
                "Near-Zone Traffic",
            ],
            x=[
                metrics.impressions,
                metrics.interest,
                metrics.near_store,
            ],
            textinfo="value+percent initial",
            opacity=0.92,
        )
    )
    fig.update_layout(
        title="Exposure Funnel",
        margin=dict(l=10, r=10, t=40, b=10),
        height=420,
    )
    return fig

def make_visit_quality_funnel_figure(metrics: DashboardMetrics, transactions: float) -> go.Figure:
    fig = go.Figure(
        go.Funnel(
            y=[
                "Store Visits",
                "Qualified Visits",
                "Engaged Visits",
                "Deeply Engaged Visits",
                "Transactions",
            ],
            x=[
                metrics.store_visits,
                metrics.qualified_visits,
                metrics.engaged_visits,
                metrics.deeply_engaged_visits,
                transactions,
            ],
            textinfo="value+percent initial",
            opacity=0.92,
        )
    )
    fig.update_layout(
        title="Visit Quality Funnel",
        margin=dict(l=10, r=10, t=40, b=10),
        height=420,
    )
    return fig


def make_hourly_figure(hourly_df: pd.DataFrame, scope: Scope) -> go.Figure:
    if hourly_df.empty:
        return go.Figure()

    df = aggregate_frame(hourly_df).copy()

    required_cols = ["hour_of_day"]
    for col in required_cols:
        if col not in df.columns:
            return go.Figure()

    if scope.period_key == "Daily":
        chart_df = df.copy()
        chart_df = chart_df.sort_values("hour_of_day")
    else:
        agg_map = {}
        for col in [
            "avg_walkby_exposure",
            "avg_near_devices",
            "avg_estimated_people",
            "avg_far_devices",
            "avg_mid_devices",
        ]:
            if col in df.columns:
                agg_map[col] = "mean"

        chart_df = (
            df.groupby("hour_of_day", as_index=False)
            .agg(agg_map)
            .sort_values("hour_of_day")
        )

    if "avg_walkby_exposure" not in chart_df.columns:
        if {"avg_far_devices", "avg_mid_devices", "avg_near_devices"}.issubset(chart_df.columns):
            chart_df["avg_walkby_exposure"] = (
                chart_df["avg_far_devices"] +
                chart_df["avg_mid_devices"] +
                chart_df["avg_near_devices"]
            )
        else:
            return go.Figure()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=chart_df["hour_of_day"],
            y=chart_df["avg_walkby_exposure"],
            mode="lines+markers",
            name="Impressions Intensity",
        )
    )

    if "avg_near_devices" in chart_df.columns:
        fig.add_trace(
            go.Scatter(
                x=chart_df["hour_of_day"],
                y=chart_df["avg_near_devices"],
                mode="lines+markers",
                name="Near-Zone Intensity",
            )
        )

    if "avg_estimated_people" in chart_df.columns:
        fig.add_trace(
            go.Scatter(
                x=chart_df["hour_of_day"],
                y=chart_df["avg_estimated_people"],
                mode="lines+markers",
                name="Estimated People",
            )
        )

    fig.update_layout(
        title="Peak Hours & Trends",
        height=420,
        margin=dict(l=10, r=10, t=50, b=10),
        xaxis_title="Hour of Day",
        yaxis_title="Average Intensity",
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(24)),
            ticktext=[f"{h:02d}:00" for h in range(24)],
            tickangle=-45,
        ),
        legend_title="Metric",
    )
    return fig


def make_dwell_figure(dwell_df: pd.DataFrame) -> go.Figure:
    if dwell_df.empty:
        return go.Figure()

    df = aggregate_frame(dwell_df).copy()
    if "dwell_bucket" not in df.columns or "visits" not in df.columns:
        return go.Figure()

    order = [
        "0-10s",
        "10-30s",
        "30-60s",
        "1-3m",
        "3-5m",
        "5-10m",
        "10-20m",
        "20m+",
    ]
    fig = px.bar(
        df,
        x="dwell_bucket",
        y="visits",
        category_orders={"dwell_bucket": order},
    )
    fig.update_layout(
        title="Customer Engagement / Dwell Depth",
        height=360,
        margin=dict(l=10, r=10, t=40, b=10),
        xaxis_title="Dwell bucket",
        yaxis_title="Visits",
    )
    return fig


def make_brand_mix_figure(hourly_df: pd.DataFrame) -> go.Figure:
    brand_df = compute_overall_brand_mix(hourly_df)
    if brand_df.empty:
        return go.Figure()

    fig = px.pie(
        brand_df,
        names="brand",
        values="value",
        hole=0.52,
    )
    fig.update_layout(
        title="Audience Mix",
        height=360,
        margin=dict(l=10, r=10, t=40, b=10),
        showlegend=True,
    )
    return fig


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    st.markdown(
        """
        <div class="hero">
            <div class="eyebrow">nsTags Retail Intelligence</div>
            <h1>Store Performance Dashboard</h1>
            <p>Retail Ops helps store teams improve visit quality and conversion. Retail Media helps prove storefront advertising value to partner brands and rank stores across the nsTags platform.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    try:
        available_dates = get_latest_available_dates()
    except Exception as e:
        st.error(f"Failed to load available dates: {e}")
        st.stop()

    if available_dates.empty:
        st.warning("No curated dashboard data is currently available.")
        st.stop()

    stores = sorted(available_dates["store_id"].dropna().astype(str).unique().tolist())
    latest_date = max(available_dates["metric_date"].tolist())

    with st.sidebar:
        st.markdown("### Dashboard Mode")
        mode = st.radio("Select mode", ["Retail Ops", "Retail Media"], label_visibility="collapsed")

        st.markdown("### Filters")
        store_id = st.selectbox("Store ID", stores, index=0)

        period_key = st.radio(
            "Analysis Window",
            ["Daily", "Weekly", "Monthly", "Yearly", "Custom"],
            index=1,
            horizontal=False,
        )

        store_dates = sorted(
            available_dates.loc[
                available_dates["store_id"].astype(str) == str(store_id),
                "metric_date",
            ].tolist()
        )
        effective_latest = max(store_dates) if store_dates else latest_date

        if period_key == "Custom":
            custom_start = st.date_input("Start Date", value=effective_latest - timedelta(days=6))
            custom_end = st.date_input("End Date", value=effective_latest)
            selected_date = custom_end
        else:
            custom_start = None
            custom_end = None
            selected_date = st.date_input("End Date", value=effective_latest)

        st.markdown("### Business Inputs")
        transactions = float(st.number_input("Transactions", value=35, step=1))
        revenue = float(st.number_input("Revenue", value=45000, step=1000))

        show_debug = st.checkbox("Show diagnostics", value=False or SHOW_DEBUG)

    scope = compute_scope(period_key, selected_date, custom_start, custom_end)

    try:
        dash_df, dash_query = try_query_candidates(build_dashboard_queries(store_id, scope))
        hourly_df, hourly_query = try_query_candidates(build_hourly_queries(store_id, scope))
        dwell_df, dwell_query = try_query_candidates(build_dwell_queries(store_id, scope))
    except Exception as e:
        st.error(f"Failed to load dashboard data: {e}")
        st.stop()

    if dash_df.empty:
        st.warning("No dashboard metrics were found for the selected period.")
        st.stop()

    metrics = build_metrics(dash_df, transactions, revenue)

    st.markdown(
        f"""
        <div class="summary-strip">
            <strong>Store:</strong> {store_id} &nbsp; • &nbsp;
            <strong>Period:</strong> {scope.label} &nbsp; • &nbsp;
            <strong>View:</strong> {mode} &nbsp; • &nbsp;
            <strong>Days in scope:</strong> {scope.days_in_scope}
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### Quick Summary")
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        render_card(
            "Impressions",
            fmt_int(metrics.impressions),
            "Total BLE storefront exposure sessions detected during the selected period.",
            "Source: walk_by_traffic (session-based impressions)",
        )

    with c2:
        render_card(
            "Store Visits",
            fmt_int(metrics.store_visits),
            "Attended in-store visit sessions.",
            "Formula: store_visits",
        )

    with c3:
        visit_conversion = metrics.store_visits / metrics.impressions if metrics.impressions > 0 else 0.0
        render_card(
            "Visit Conversion",
            fmt_pct_from_ratio(visit_conversion),
            "Share of impressions that became store visits.",
            "Formula: Store Visits / Impressions",
        )

    with c4:
        render_card(
            "Avg Dwell Time",
            fmt_seconds(metrics.avg_dwell_seconds),
            "Average dwell of visit sessions.",
            "Formula: mean(avg_dwell_seconds) across selected days",
        )

    st.markdown(
        f"""
        <div class="simple-callout">
            <strong>In simple terms:</strong> {build_store_summary_sentence(metrics)}
        </div>
        """,
        unsafe_allow_html=True,
    )

    primary_bottleneck = identify_primary_bottleneck(metrics)
    narratives = get_priority_narratives(mode, primary_bottleneck)

    p1, p2, p3 = st.columns(3)
    with p1:
        render_priority_card("Traffic Reading", narratives["traffic_title"], narratives["traffic_body"])
    with p2:
        render_priority_card("Experience Reading", narratives["visit_title"], narratives["visit_body"])
    with p3:
        render_priority_card("Business Reading", narratives["commercial_title"], narratives["commercial_body"])

    st.markdown("### Advanced Summary")
    a1, a2, a3, a4 = st.columns(4)

    with a1:
        cls = badge_class_for_label(metrics.traffic_health_band)
        render_info_card(
            "Traffic Health",
            f"<span class='{cls}'>{metrics.traffic_health_band}</span><br><br>"
            "Curated directional health band for impression-to-visit behavior.",
        )

    with a2:
        cls = badge_class_for_label(metrics.visit_quality_band)
        render_info_card(
            "Visit Quality",
            f"<span class='{cls}'>{metrics.visit_quality_band}</span><br><br>"
            "Curated directional quality band for visit depth.",
        )

    with a3:
        deep_rate = metrics.deeply_engaged_visits / metrics.store_visits if metrics.store_visits > 0 else 0.0
        render_info_card(
            "Deep Engagement Rate",
            f"<strong>{fmt_pct_from_ratio(deep_rate)}</strong><br><br>"
            "Share of visits that reached the deepest engagement layer.",
        )

    with a4:
        render_info_card(
            "Benchmark Depth",
            f"<strong>{fmt_int(metrics.benchmark_days)} day(s)</strong><br><br>"
            "Benchmark confidence improves as more store-day records accumulate.",
        )

    tabs = st.tabs(
        [
            "Executive AI Brief",
            "Store Funnel",
            "Peak Hours & Trends",
            "Customer Engagement",
            "Audience Mix",
            "Deep Diagnostics",
        ]
    )

    with tabs[0]:
        st.markdown("### Executive AI Brief")
        if st.button("Generate Intelligence Summary"):
            ai_payload = {
                "mode": mode,
                "scope": scope.label,
                "impressions": fmt_int(metrics.impressions),
                "interest": fmt_int(metrics.interest),
                "near_store": fmt_int(metrics.near_store),
                "visits": fmt_int(metrics.store_visits),
                "qualified_visits": fmt_int(metrics.qualified_visits),
                "engaged_visits": fmt_int(metrics.engaged_visits),
                "qualified_rate": round((metrics.qualified_visits / metrics.store_visits) * 100, 1) if metrics.store_visits > 0 else 0,
                "engaged_rate": round((metrics.engaged_visits / metrics.store_visits) * 100, 1) if metrics.store_visits > 0 else 0,
                "avg_dwell": fmt_seconds(metrics.avg_dwell_seconds),
                "transactions": fmt_int(transactions),
                "value": fmt_currency(revenue),
                "commercial_ratio": round(metrics.commercial_ratio * 100, 1),
                "aqi": round(metrics.audience_quality_index, 1),
                "primary_bottleneck": primary_bottleneck,
            }
            with st.spinner("Generating AI brief..."):
                summary = generate_ai_brief(ai_payload)
            st.markdown(summary)

        st.markdown(
            f"""
            <div class="panel">
                <div class="info-title">Primary Opportunity</div>
                <div class="info-body">{narratives['primary_bottleneck_story']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with tabs[1]:
        f1, f2 = st.columns(2)

        with f1:
            st.plotly_chart(
                make_exposure_funnel_figure(metrics),
                use_container_width=True,
            )

        with f2:
            st.plotly_chart(
                make_visit_quality_funnel_figure(metrics, transactions),
                use_container_width=True,
            )

        st.markdown(
            """
            <div class="simple-callout">
                <strong>How to read the funnels:</strong>
                The Exposure Funnel shows how storefront audience opportunity narrows into actual visits.
                The Visit Quality Funnel shows how visits deepen into qualified engagement and business outcome.
            </div>
            """,
            unsafe_allow_html=True,
        )

    with tabs[2]:
        hourly_fig = make_hourly_figure(hourly_df, scope)
        if len(hourly_fig.data) == 0:
            st.info("No hourly traffic data available for the selected period.")
        else:
            # 1. First, render the chart
            st.plotly_chart(hourly_fig, use_container_width=True)
            
            # 2. Add your new explanation immediately after the chart
            st.markdown(
                f"""
                <div class="simple-callout">
                    <strong>How to read this chart:</strong>
                    For <strong>{scope.label}</strong> view, the chart shows the hourly audience pattern for the selected filter window.
                    <strong>Impressions Intensity</strong> reflects storefront exposure,
                    <strong>Near-Zone Intensity</strong> reflects stronger close-range presence,
                    and <strong>Estimated People</strong> shows directional live crowd pattern.
                </div>
                """,
                unsafe_allow_html=True,
            )

    with tabs[3]:
        dwell_fig = make_dwell_figure(dwell_df)
        if len(dwell_fig.data) == 0:
            st.info("No dwell distribution data available for the selected period.")
        else:
            st.plotly_chart(dwell_fig, use_container_width=True)

    with tabs[4]:
        brand_fig = make_brand_mix_figure(hourly_df)
        if len(brand_fig.data) == 0:
            st.info("No audience mix data available for the selected period.")
        else:
            st.plotly_chart(brand_fig, use_container_width=True)

    with tabs[5]:
        d1, d2 = st.columns(2)

        with d1:
            render_info_card(
                "How to Read This Dashboard",
                """
                1. <strong>Impressions</strong> reflect session-based storefront audience opportunity.<br><br>
                2. <strong>Store Visits</strong>, <strong>Qualified Visits</strong>, and <strong>Engaged Visits</strong> show visit depth.<br><br>
                3. <strong>Peak hours</strong> and <strong>audience mix</strong> show when to staff and where the strongest audience windows exist.
                """,
            )

        with d2:
            render_info_card(
                "Commercial Reading",
                f"""
                <strong>{commercial_ratio_label(mode)}</strong><br><br>
                {commercial_ratio_description(mode)}<br><br>
                Current value: <strong>{fmt_pct_from_ratio(metrics.commercial_ratio)}</strong>
                """,
            )

        if metrics.notes:
            st.markdown("### Interpretation Notes")
            for note in metrics.notes:
                st.markdown(
                    f"<div class='simple-callout'>{note}</div>",
                    unsafe_allow_html=True,
                )

        if show_debug:
            st.markdown("### Debug / Diagnostics")
            st.code(dash_query, language="sql")
            st.code(hourly_query, language="sql")
            st.code(dwell_query, language="sql")

            st.markdown("#### Dashboard Data")
            st.dataframe(dash_df, use_container_width=True)

            st.markdown("#### Hourly Data")
            st.dataframe(hourly_df, use_container_width=True)

            st.markdown("#### Dwell Data")
            st.dataframe(dwell_df, use_container_width=True)


if __name__ == "__main__":
    main()
