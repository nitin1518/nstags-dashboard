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
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT", "s3://nstagsmasterdevices3bucket/athena-results/")
ATHENA_RESULT_REUSE_MINUTES = int(os.environ.get("ATHENA_RESULT_REUSE_MINUTES", "30"))
TIMEZONE = os.environ.get("TIMEZONE", "Asia/Kolkata")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
AI_CACHE_TTL = int(os.environ.get("AI_CACHE_TTL", "900"))
APP_QUERY_TIMEOUT = int(os.environ.get("APP_QUERY_TIMEOUT", "35"))
APP_HEAVY_QUERY_TIMEOUT = int(os.environ.get("APP_HEAVY_QUERY_TIMEOUT", "20"))
SHOW_DEBUG = os.environ.get("SHOW_DEBUG", "0") == "1"

# Curated / fast sources only
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
@media (prefers-color-scheme: dark) {
    :root {
        --bg: #06080F;
        --bg-soft: #0A0F1E;
        --panel: #0D1117;
        --panel-2: #111827;
        --border: rgba(99,102,241,0.16);
        --border-strong: rgba(99,102,241,0.32);
        --text: #F8FAFC;
        --text-2: #CBD5E1;
        --text-3: #94A3B8;
        --text-muted: #94A3B8;
        --accent: #818CF8;
        --accent-2: #A78BFA;
        --good: #34D399;
        --warn: #FBBF24;
        --bad: #FB7185;
        --shadow: 0 18px 44px rgba(0,0,0,0.35);
        --shadow-soft: 0 10px 28px rgba(0,0,0,0.26);
        --card-grad: linear-gradient(145deg, #0D1117 0%, #111827 100%);
    }
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
.hero h1 {font-size: 1.85rem; margin: 0; line-height: 1.05; letter-spacing: -0.03em; color: var(--text);}
.hero p {margin: .42rem 0 0 0; color: var(--text-3); line-height: 1.55; font-size: 0.95rem;}
.eyebrow {text-transform: uppercase; letter-spacing: 0.14em; font-size: .72rem; font-weight: 800; color: var(--accent); margin-bottom: .45rem;}
.section-title {margin: 1.05rem 0 .65rem 0; font-size: .76rem; text-transform: uppercase; letter-spacing: .13em; color: var(--accent); font-weight: 800;}
.panel, .info-card, .story-card, .priority-card, .kpi-card, .summary-strip, .simple-callout {
    background: var(--card-grad); border: 1px solid var(--border); border-radius: 18px; box-shadow: var(--shadow-soft);
}
.panel {padding: 1rem 1rem .9rem 1rem; margin-bottom: 1rem;}
.kpi-card {padding: 1rem 1rem .92rem 1rem; min-height: 158px; position: relative; overflow: hidden; margin-bottom: .9rem;}
.kpi-card::after {content:''; position:absolute; top:-36px; right:-36px; width:92px; height:92px; background: radial-gradient(circle, rgba(99,102,241,0.08), transparent 70%); border-radius: 50%;}
.kpi-label {font-size: .7rem; text-transform: uppercase; letter-spacing: .1em; font-weight: 800; color: var(--text-muted); margin-bottom: .35rem;}
.kpi-value {font-size: 2rem; font-weight: 800; letter-spacing: -0.03em; color: var(--text); line-height: 1.02; margin-bottom: .25rem;}
.kpi-sub {color: var(--text-3); font-size: .84rem; line-height: 1.5; margin-top: .35rem;}
.kpi-formula {margin-top: .62rem; padding-top: .55rem; border-top: 1px dashed rgba(99,102,241,0.16); color: var(--text-muted); font-size: .76rem; line-height: 1.45;}
.priority-card {border-left: 4px solid var(--accent); padding: 1rem 1rem .95rem 1rem; min-height: 180px; margin-bottom: .9rem;}
.priority-label, .story-label, .info-title {font-size: .7rem; text-transform: uppercase; letter-spacing: .11em; font-weight: 800; color: var(--accent); margin-bottom: .45rem;}
.priority-title, .story-title {font-size: 1.08rem; font-weight: 800; color: var(--text); line-height: 1.25; margin-bottom: .5rem;}
.priority-body, .story-body, .info-body {color: var(--text-3); font-size: .88rem; line-height: 1.6;}
.info-card {padding: .95rem 1rem; margin-bottom: .9rem;}
.summary-strip, .simple-callout {padding: .9rem 1rem; margin-bottom: 1rem; color: var(--text-3); font-size: .9rem; line-height: 1.55;}
.metric-pill {display: inline-block; padding: .2rem .52rem; border-radius: 999px; background: rgba(99,102,241,0.08); color: var(--accent); font-size: .72rem; font-weight: 700; margin-right: .35rem; margin-bottom: .38rem;}
.badge-good, .badge-warn, .badge-bad, .badge-info {display:inline-block; padding:.12rem .48rem; border-radius:8px; font-size:.74rem; font-weight:800;}
.badge-good { background: rgba(16,185,129,0.10); color: var(--good); }
.badge-warn { background: rgba(245,158,11,0.10); color: var(--warn); }
.badge-bad  { background: rgba(244,63,94,0.10); color: var(--bad); }
.badge-info { background: rgba(99,102,241,0.10); color: var(--accent); }
.small-muted {color: var(--text-muted); font-size: .78rem;}
code {white-space: pre-wrap;}
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
    walk_by: float = 0.0
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
    walk_by_source: str = ""
    notes: list[str] | None = None


# =========================================================
# GENERIC HELPERS
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


def choose_col_name(df: pd.DataFrame, names: list[str]) -> str:
    for name in names:
        if name in df.columns:
            return name
    return ""


def choose_text_mode(series: pd.Series, default: str = "") -> str:
    try:
        s = series.dropna().astype(str)
        if s.empty:
            return default
        return s.mode().iloc[0]
    except Exception:
        return default


# =========================================================
# ATHENA
# =========================================================
def run_athena_query(query: str, database: str = ATHENA_DATABASE, timeout_sec: int = APP_QUERY_TIMEOUT) -> pd.DataFrame:
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
        reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
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


def try_query_candidates(candidates: list[str], timeout_sec: int = APP_QUERY_TIMEOUT) -> tuple[pd.DataFrame, str]:
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
def sql_scope_predicate(store_id: str, start_date: date, end_date: date) -> str:
    return (
        f"store_id = '{q(store_id)}' "
        f"AND CAST(metric_date AS DATE) BETWEEN DATE '{start_date.isoformat()}' AND DATE '{end_date.isoformat()}'"
    )


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
    pred = sql_scope_predicate(store_id, scope.start_date, scope.end_date)
    queries: list[str] = []
    for table in DASHBOARD_TABLES:
        queries.append(
            f"""
            SELECT *
            FROM {table}
            WHERE {pred}
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
            ORDER BY metric_date
            """
        )
    return queries


# =========================================================
# DATE / PERIOD LOGIC
# =========================================================
def get_latest_available_dates() -> pd.DataFrame:
    df, _ = try_query_candidates(build_available_dates_queries(), timeout_sec=APP_HEAVY_QUERY_TIMEOUT)
    if df.empty:
        return pd.DataFrame(columns=["store_id", "metric_date"])
    if "metric_date" in df.columns:
        df["metric_date"] = pd.to_datetime(df["metric_date"]).dt.date
    return df


def compute_scope(period_key: str, selected_date: date, custom_start: date | None = None, custom_end: date | None = None) -> Scope:
    if period_key == "Daily":
        return Scope("Daily snapshot", "Daily", selected_date, selected_date, 1)
    if period_key == "Weekly":
        start = selected_date - timedelta(days=6)
        return Scope(f"Last 7 days · {start.strftime('%d %b')} → {selected_date.strftime('%d %b %Y')}", "Weekly", start, selected_date, 7)
    if period_key == "Monthly":
        start = selected_date - timedelta(days=29)
        return Scope(f"Last 30 days · {start.strftime('%d %b')} → {selected_date.strftime('%d %b %Y')}", "Monthly", start, selected_date, 30)
    if period_key == "Yearly":
        start = selected_date - timedelta(days=364)
        return Scope(f"Last 365 days · {start.strftime('%d %b %Y')} → {selected_date.strftime('%d %b %Y')}", "Yearly", start, selected_date, 365)
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
        "Apple": safe_float(hourly_df["avg_apple_devices"].sum()) if "avg_apple_devices" in hourly_df.columns else 0,
        "Samsung": safe_float(hourly_df["avg_samsung_devices"].sum()) if "avg_samsung_devices" in hourly_df.columns else 0,
        "Other": safe_float(hourly_df["avg_other_devices"].sum()) if "avg_other_devices" in hourly_df.columns else 0,
    }
    return pd.DataFrame({"brand": list(agg.keys()), "value": list(agg.values())})


def derive_from_dashboard_table(dash_df: pd.DataFrame) -> dict[str, float | str]:
    if dash_df.empty:
        return {}
    df = aggregate_frame(dash_df)
    return {
        "walk_by": safe_float(df["walk_by_traffic"].sum()) if "walk_by_traffic" in df.columns else 0.0,
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

    walk_by = safe_float(dash.get("walk_by", 0.0))
    store_visits = safe_float(dash.get("store_visits", 0.0))
    qualified_visits = safe_float(dash.get("qualified_visits", 0.0))
    engaged_visits = safe_float(dash.get("engaged_visits", 0.0))
    deeply_engaged_visits = safe_float(dash.get("deeply_engaged_visits", 0.0))
    avg_dwell_seconds = safe_float(dash.get("avg_dwell_seconds", 0.0))
    median_dwell_seconds = safe_float(dash.get("median_dwell_seconds", 0.0))
    interest = safe_float(dash.get("interest", 0.0))
    near_store = safe_float(dash.get("near_store", 0.0))
    total_sessions = safe_float(dash.get("total_sessions", 0.0))

    commercial_ratio = 0.0
    if store_visits > 0:
        commercial_ratio = transactions / store_visits

    qualified_rate = qualified_visits / store_visits if store_visits > 0 else 0.0
    engaged_rate = engaged_visits / store_visits if store_visits > 0 else 0.0
    audience_quality_index = ((qualified_rate * 0.6) + (engaged_rate * 0.4)) * 100

    if walk_by > 0 and store_visits > walk_by:
        notes.append(
            "Nearby exposure is a directional audience-exposure proxy derived from live zone averages, while store visits are session counts. "
            "The visit-to-exposure index should be read directionally, not as a literal human conversion percentage."
        )
    notes.append(
        "Nearby exposure is best used to compare relative traffic intensity across dates, hours, and stores rather than exact unique human footfall."
    )

    return DashboardMetrics(
        walk_by=walk_by,
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
        walk_by_source="curated walk_by_traffic",
        notes=notes,
    )


# =========================================================
# LABELS / NARRATIVES
# =========================================================
def commercial_ratio_label(mode: str) -> str:
    return "Commercial Response Ratio" if mode == "Retail Media" else "Transactions / Visit Ratio"


def commercial_ratio_description(mode: str) -> str:
    if mode == "Retail Media":
        return "Business response generated against the selected period's in-store demand. Read as response intensity, not a literal human conversion rate."
    return "Business outcome relative to store visits. Read with context because transaction or response counts can exceed visit counts in some setups."


def performance_label(score: float) -> str:
    if score >= 75:
        return "Strong"
    if score >= 50:
        return "Moderate"
    return "Weak"


def audience_label(score: float) -> str:
    if score < 40:
        return "Low"
    if score < 60:
        return "Average"
    if score < 80:
        return "High"
    return "Excellent"


def store_health_label(exposure_index_proxy: float, engaged_rate: float, avg_dwell_sec: float) -> tuple[str, str]:
    score = 0
    if exposure_index_proxy >= 0.25:
        score += 1
    if engaged_rate >= 0.35:
        score += 1
    if avg_dwell_sec >= 120:
        score += 1
    if score == 3:
        return "Strong", "badge-good"
    if score == 2:
        return "Moderate", "badge-warn"
    return "Needs attention", "badge-bad"


def build_store_summary_sentence(metrics: DashboardMetrics) -> str:
    idx = metrics.store_visits / metrics.walk_by if metrics.walk_by > 0 else 0
    return (
        f"The store recorded {fmt_int(metrics.store_visits)} visit sessions in this period, with nearby exposure measured at "
        f"{fmt_float(metrics.walk_by, 1)} and average dwell of {fmt_seconds(metrics.avg_dwell_seconds)}. "
        f"The visit-to-exposure index is {fmt_float(idx, 2)}, which should be read directionally rather than as a literal people conversion rate."
    )


def identify_primary_bottleneck(metrics: DashboardMetrics) -> str:
    capture = min((metrics.store_visits / metrics.walk_by), 1.0) if metrics.walk_by > 0 else 0
    qual = metrics.qualified_visits / metrics.store_visits if metrics.store_visits > 0 else 0
    engage = metrics.engaged_visits / metrics.store_visits if metrics.store_visits > 0 else 0
    close = metrics.transactions / metrics.store_visits if metrics.store_visits > 0 else 0
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
            "slows down, notices the storefront, and becomes measurable attention near the store."
        )
        visit_title = "Audience depth determines campaign usefulness"
        visit_body = (
            "A strong audience window is only valuable when visits become meaningful interactions. Dwell and engagement show "
            "whether the location is delivering shallow exposure or stronger brand consideration."
        )
        commercial_title = "Commercial response should be read as outcome intensity"
        commercial_body = (
            "Transactions / responses represent the business reaction captured against in-store demand. Treat this as commercial "
            "response intensity for the selected period, not as a strict person-to-sale conversion rate."
        )
    else:
        traffic_title = "Storefront pull is the first business gate"
        traffic_body = (
            "The first question for store owners is whether the store is converting nearby exposure into serious attention. "
            "If people pass by but do not slow down or approach, later selling excellence cannot fully compensate."
        )
        visit_title = "Visit quality is the clearest indicator of store health"
        visit_body = (
            "High-quality visits are the real operating opportunity. Qualified and engaged visits show whether entry is turning "
            "into browsing, assistance, and purchase intent rather than shallow walk-ins."
        )
        commercial_title = "Commercial outcome must be read with context"
        commercial_body = (
            "Transactions / responses reflect business outcome for the selected period. This should be interpreted alongside "
            "visit quality, because response counts can exceed visit counts in some business setups."
        )

    priority_map = {
        "Store Magnet": f"{traffic_title} Current exposure-to-visit relationship is weak, so the biggest opportunity is improving storefront noticeability and stopping power.",
        "Entry Efficiency": f"{visit_title} Entry is happening, but too many visits are not becoming meaningful store interactions.",
        "Dwell Quality": f"{visit_title} Visitors are entering, but not staying long enough to reflect stronger browsing or deeper engagement.",
        "Floor Conversion": f"{commercial_title} The store is generating visit quality, but business closure is the weakest layer right now.",
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
        return "⚠️ AI unavailable: GEMINI_API_KEY not configured."
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        prompt = f"""
You are a senior retail strategy consultant writing for business leaders.
Do not mention sensors, BLE, Athena, pipelines, device scanning, or any backend system.

BUSINESS CONTEXT
Mode: {ai_payload['mode']}
Scope: {ai_payload['scope']}

PERFORMANCE METRICS
Nearby exposure: {ai_payload['walk_by']}
Store interest: {ai_payload['interest']}
Near-store traffic: {ai_payload['near_store']}
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
Nearby exposure is a directional audience-exposure measure, not a literal unique human count.
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
        response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        return response.text
    except Exception as e:
        return f"⚠️ AI unavailable: {str(e)}"


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


def render_story_card(label: str, title: str, pills: list[str], body: str) -> None:
    pills_html = "".join([f"<span class='metric-pill'>{p}</span>" for p in pills])
    st.markdown(
        f"""
        <div class='story-card'>
            <div class='story-label'>{label}</div>
            <div class='story-title'>{title}</div>
            <div style='margin-bottom:.5rem;'>{pills_html}</div>
            <div class='story-body'>{body}</div>
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


def make_funnel_figure(metrics: DashboardMetrics, transactions: float, mode: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Funnel(
            y=[
                "Nearby exposure",
                "Store interest",
                "Near entrance",
            ],
            x=[metrics.walk_by, metrics.interest, metrics.near_store],
            textposition="inside",
        )
    )
    fig.update_layout(height=380, margin=dict(l=10, r=10, t=30, b=10), title="Attention Funnel")
    return fig


def make_visit_funnel_figure(metrics: DashboardMetrics, transactions: float, mode: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Funnel(
            y=[
                "Store visits",
                "Qualified visits",
                "Engaged visits",
                "Transactions / responses",
            ],
            x=[metrics.store_visits, metrics.qualified_visits, metrics.engaged_visits, transactions],
            textposition="inside",
        )
    )
    fig.update_layout(height=380, margin=dict(l=10, r=10, t=30, b=10), title="Visit & Engagement Funnel")
    return fig


def make_hourly_figure(hourly_df: pd.DataFrame) -> go.Figure:
    if hourly_df.empty:
        return go.Figure()
    df = hourly_df.copy()
    xcol = choose_col_name(df, ["hour_label", "hour_of_day", "hour"])
    ycol = choose_col_name(df, ["avg_walkby_exposure", "avg_estimated_people", "estimated_people", "avg_total_devices", "total_devices"])
    if not xcol or not ycol:
        return go.Figure()
    fig = px.line(df, x=xcol, y=ycol, markers=True)
    fig.update_layout(height=340, margin=dict(l=10, r=10, t=30, b=10), title="Peak Hours & Trends")
    return fig


def make_dwell_figure(dwell_df: pd.DataFrame) -> go.Figure:
    if dwell_df.empty:
        return go.Figure()
    df = dwell_df.copy()
    xcol = choose_col_name(df, ["bucket_label", "dwell_bucket", "bucket"])
    ycol = choose_col_name(df, ["visits", "session_count", "sessions", "value", "count"])
    if not xcol or not ycol:
        return go.Figure()
    fig = px.bar(df, x=xcol, y=ycol)
    fig.update_layout(height=340, margin=dict(l=10, r=10, t=30, b=10), title="Customer Engagement")
    return fig


def make_brand_mix_figure(brand_df: pd.DataFrame) -> go.Figure:
    if brand_df.empty:
        return go.Figure()
    fig = px.pie(brand_df, names="brand", values="value")
    fig.update_layout(height=340, margin=dict(l=10, r=10, t=30, b=10), title="Audience Mix")
    return fig


# =========================================================
# MAIN APP
# =========================================================
def main() -> None:
    st.markdown(
        """
        <div class='hero'>
            <div class='eyebrow'>nsTags Retail Intelligence</div>
            <h1>Store Performance Dashboard</h1>
            <p>A store-led dashboard to understand nearby exposure, visit quality, engagement, and the biggest operating opportunity.</p>
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
        st.warning("No store/date data available yet.")
        st.stop()

    stores = sorted(available_dates["store_id"].dropna().astype(str).unique().tolist())
    latest_date = max(available_dates["metric_date"].tolist())

    with st.sidebar:
        st.markdown("## Dashboard Mode")
        mode = st.radio("Business Mode", ["Retail Ops", "Retail Media"], label_visibility="collapsed", horizontal=True)

        st.markdown("## Filters")
        store_id = st.selectbox("Store ID", stores, index=0)

        st.markdown("## Time Period")
        period_key = st.radio("Analysis Window", ["Daily", "Weekly", "Monthly", "Yearly", "Custom"], index=1)

        store_dates = sorted(available_dates.loc[available_dates["store_id"] == store_id, "metric_date"].tolist())
        max_store_date = max(store_dates) if store_dates else latest_date

        if period_key == "Daily":
            selected_date = st.selectbox("Date", sorted(store_dates, reverse=True), index=0)
            scope = compute_scope(period_key, selected_date)
        elif period_key == "Weekly":
            selected_date = st.date_input("Week End Date", value=max_store_date)
            scope = compute_scope(period_key, selected_date)
        elif period_key == "Monthly":
            selected_date = st.date_input("Month End Date", value=max_store_date)
            scope = compute_scope(period_key, selected_date)
        elif period_key == "Yearly":
            selected_date = st.date_input("Year End Date", value=max_store_date)
            scope = compute_scope(period_key, selected_date)
        else:
            custom_end = st.date_input("End Date", value=max_store_date)
            custom_start = st.date_input("Start Date", value=max_store_date - timedelta(days=6))
            scope = compute_scope(period_key, custom_end, custom_start, custom_end)

        st.markdown("## Business Inputs")
        transactions = st.number_input("Transactions", min_value=0, value=35, step=1)
        revenue = st.number_input("Revenue", min_value=0, value=45000, step=1000)
        show_tz_diag = st.checkbox("Show timezone diagnostics", value=False)
        refresh = st.button("Refresh Dashboard", type="primary")
        if refresh:
            st.cache_data.clear()
            st.rerun()

    query_error: str | None = None
    dash_df = pd.DataFrame()
    hourly_df = pd.DataFrame()
    dwell_df = pd.DataFrame()

    try:
        dash_df, dash_sql = try_query_candidates(build_dashboard_queries(store_id, scope), timeout_sec=APP_QUERY_TIMEOUT)

        try:
            hourly_df, hourly_sql = try_query_candidates(build_hourly_queries(store_id, scope), timeout_sec=APP_HEAVY_QUERY_TIMEOUT)
        except Exception:
            hourly_df, hourly_sql = pd.DataFrame(), ""
        try:
            dwell_df, dwell_sql = try_query_candidates(build_dwell_queries(store_id, scope), timeout_sec=APP_HEAVY_QUERY_TIMEOUT)
        except Exception:
            dwell_df, dwell_sql = pd.DataFrame(), ""
    except Exception as e:
        query_error = str(e)

    if query_error:
        st.error(f"Failed to load dashboard data: {query_error}")
        st.stop()

    metrics = build_metrics(dash_df, transactions=float(transactions), revenue=float(revenue))

    st.markdown(
        f"""
        <div class='summary-strip'>
            <strong>Store:</strong> {store_id} &nbsp;&nbsp;•&nbsp;&nbsp;
            <strong>Period:</strong> {scope.label} &nbsp;&nbsp;•&nbsp;&nbsp;
            <strong>View:</strong> {mode} &nbsp;&nbsp;•&nbsp;&nbsp;
            <strong>Days in scope:</strong> {scope.days_in_scope}
        </div>
        """,
        unsafe_allow_html=True,
    )

    qualified_rate = metrics.qualified_visits / metrics.store_visits if metrics.store_visits > 0 else 0
    engaged_rate = metrics.engaged_visits / metrics.store_visits if metrics.store_visits > 0 else 0
    deep_rate = metrics.deeply_engaged_visits / metrics.store_visits if metrics.store_visits > 0 else 0
    visit_exposure_index = metrics.store_visits / metrics.walk_by if metrics.walk_by > 0 else 0
    health_label, health_badge = store_health_label(min(visit_exposure_index, 1.0), engaged_rate, metrics.avg_dwell_seconds)
    primary_bottleneck = identify_primary_bottleneck(metrics)
    priority_narratives = get_priority_narratives(mode, primary_bottleneck)

    st.markdown("<div class='section-title'>Quick Summary</div>", unsafe_allow_html=True)
    quick_cols = st.columns(4)

    with quick_cols[0]:
        render_card("Nearby Exposure", fmt_float(metrics.walk_by, 1), "Directional audience exposure around the store perimeter.")
    with quick_cols[1]:
        render_card("Store Visits", fmt_int(metrics.store_visits), "People who moved into store visit behavior.")
    with quick_cols[2]:
        render_card("Visit / Exposure Index", fmt_float(visit_exposure_index, 2), "Directional index, not literal human conversion.")
    with quick_cols[3]:
        render_card("Avg Dwell Time", fmt_seconds(metrics.avg_dwell_seconds), "Average time spent by store visits.")

    st.markdown(
        f"<div class='simple-callout'><strong>In simple terms:</strong> {build_store_summary_sentence(metrics)}</div>",
        unsafe_allow_html=True,
    )

    second_cols = st.columns(4)
    with second_cols[0]:
        render_card(
            "Store Health",
            health_label,
            f"<span class='{health_badge}'>{health_label}</span><br>Simple health view based on visit depth, engagement, and dwell time.",
            "This is a quick guide for store teams, not a strict benchmark score.",
        )
    with second_cols[1]:
        render_card(
            "Qualified Visit Rate",
            fmt_pct_from_ratio(qualified_rate),
            "Share of visits that became serious product visits.",
            f"Formula: {fmt_int(metrics.qualified_visits)} / {fmt_int(metrics.store_visits)}",
        )
    with second_cols[2]:
        render_card(
            "Engaged Visit Rate",
            fmt_pct_from_ratio(engaged_rate),
            "Share of visits that moved into deeper interaction.",
            f"Formula: {fmt_int(metrics.engaged_visits)} / {fmt_int(metrics.store_visits)}",
        )
    with second_cols[3]:
        render_card(
            "Audience Quality",
            audience_label(metrics.audience_quality_index),
            f"Current audience profile is {audience_label(metrics.audience_quality_index).lower()}.",
            f"Underlying score: {fmt_float(metrics.audience_quality_index, 0)}",
        )

    st.markdown("<div class='section-title'>What the Team Should Know</div>", unsafe_allow_html=True)
    pri_cols = st.columns(3)
    with pri_cols[0]:
        render_priority_card("Top Priority", "Biggest operational bottleneck", priority_narratives["primary_bottleneck_story"])
    with pri_cols[1]:
        render_priority_card(
            "Visit Quality",
            "Meaningful visits vs shallow visits",
            f"The store generated {fmt_int(metrics.store_visits)} visits, of which {fmt_int(metrics.qualified_visits)} were meaningful and {fmt_int(metrics.engaged_visits)} were deeper interactions. Average dwell of {fmt_seconds(metrics.avg_dwell_seconds)} helps explain whether people are browsing seriously or leaving too quickly.",
        )
    with pri_cols[2]:
        render_priority_card(
            "Trading Read",
            "Commercial outcome in context",
            f"Commercial ratio is {fmt_pct_from_ratio(metrics.commercial_ratio)}. This should be read with visit quality, not in isolation. Transactions / responses can exceed person-count conversions in some business models.",
        )

    st.markdown("<div class='section-title'>Business Reading</div>", unsafe_allow_html=True)
    story_cols = st.columns(3)
    with story_cols[0]:
        render_story_card(
            "Traffic Reading",
            priority_narratives["traffic_title"],
            [f"Exposure {fmt_float(metrics.walk_by, 1)}", f"Interest {fmt_float(metrics.interest, 1)}", f"Near-store {fmt_float(metrics.near_store, 1)}"],
            priority_narratives["traffic_body"],
        )
    with story_cols[1]:
        render_story_card(
            "Experience Reading",
            priority_narratives["visit_title"],
            [f"Visits {fmt_int(metrics.store_visits)}", f"Qualified {fmt_int(metrics.qualified_visits)}", f"Engaged {fmt_int(metrics.engaged_visits)}"],
            priority_narratives["visit_body"],
        )
    with story_cols[2]:
        render_story_card(
            "Business Outcome",
            priority_narratives["commercial_title"],
            [f"Responses {fmt_int(transactions)}", f"Value {fmt_currency(revenue)}", f"Index {fmt_pct_from_ratio(metrics.commercial_ratio)}"],
            priority_narratives["commercial_body"],
        )

    st.markdown("<div class='section-title'>Advanced Summary</div>", unsafe_allow_html=True)
    adv_cols = st.columns(4)
    maturity_label = "Early" if metrics.benchmark_days < 50 else "Developing" if metrics.benchmark_days < 200 else "Established"
    maturity_class = "badge-warn" if maturity_label == "Early" else "badge-info" if maturity_label == "Developing" else "badge-good"
    maturity_text = f"Benchmark built on only {metrics.benchmark_days or 21} store-day records. Scores are provisional." if maturity_label == "Early" else "Benchmark quality is improving as more store-day data accumulates."

    with adv_cols[0]:
        render_card("Traffic Health", metrics.traffic_health_band, "Curated directional health band for nearby exposure vs visit behavior.", "Use as a relative guide across stores and dates.")
    with adv_cols[1]:
        render_card("Visit Quality", metrics.visit_quality_band, "Curated directional quality band for visit depth.", "Higher visit depth means stronger browsing and deeper interaction.")
    with adv_cols[2]:
        render_card("Deep Engagement Rate", fmt_pct_from_ratio(deep_rate), "Share of visits that reached the deepest engagement layer.", f"Formula: {fmt_int(metrics.deeply_engaged_visits)} / {fmt_int(metrics.store_visits)}")
    with adv_cols[3]:
        render_card("Benchmark Depth", maturity_label, f"<span class='{maturity_class}'>{maturity_label}</span><br>{maturity_text}", "Benchmark quality improves as more store-day data accumulates.")

    st.markdown("<div class='section-title'>How to Read This Dashboard</div>", unsafe_allow_html=True)
    how_cols = st.columns(3)
    with how_cols[0]:
        render_info_card("1. Nearby exposure", "Nearby exposure is a directional audience-exposure proxy derived from far + mid zone behavior.")
    with how_cols[1]:
        render_info_card("2. People entering and browsing", "Visits, qualified visits, and dwell time show how serious the store traffic is.")
    with how_cols[2]:
        render_info_card("3. Busy hours and shopper mix", "Hourly charts and engagement charts show when to staff better and what audience is strongest.")

    tabs = st.tabs(["Executive AI Brief", "Store Funnel", "Peak Hours & Trends", "Customer Engagement", "Audience Mix", "Deep Diagnostics"])

    with tabs[0]:
        ai_payload = {
            "mode": mode,
            "scope": scope.label,
            "walk_by": fmt_float(metrics.walk_by, 1),
            "interest": fmt_float(metrics.interest, 1),
            "near_store": fmt_float(metrics.near_store, 1),
            "visits": fmt_int(metrics.store_visits),
            "qualified_visits": fmt_int(metrics.qualified_visits),
            "engaged_visits": fmt_int(metrics.engaged_visits),
            "qualified_rate": round(qualified_rate * 100, 1),
            "engaged_rate": round(engaged_rate * 100, 1),
            "avg_dwell": fmt_seconds(metrics.avg_dwell_seconds),
            "transactions": fmt_int(transactions),
            "value": fmt_currency(revenue),
            "commercial_ratio": round(metrics.commercial_ratio * 100, 1),
            "aqi": fmt_float(metrics.audience_quality_index, 0),
            "primary_bottleneck": primary_bottleneck,
        }
        st.markdown(generate_ai_brief(ai_payload))

    with tabs[1]:
        st.markdown("<div class='note'>This shows how audience exposure becomes attention, near-store approach, and then stronger visit behavior inside the store.</div>", unsafe_allow_html=True)
        fcols = st.columns(2)
        with fcols[0]:
            st.plotly_chart(make_funnel_figure(metrics, transactions, mode), use_container_width=True)
        with fcols[1]:
            st.plotly_chart(make_visit_funnel_figure(metrics, transactions, mode), use_container_width=True)

    with tabs[2]:
        if hourly_df.empty:
            st.info("Hourly data not available for this selection.")
        else:
            st.plotly_chart(make_hourly_figure(hourly_df), use_container_width=True)

    with tabs[3]:
        if dwell_df.empty:
            st.info("Dwell bucket data not available for this selection.")
        else:
            st.plotly_chart(make_dwell_figure(dwell_df), use_container_width=True)

    with tabs[4]:
        brand_df = compute_overall_brand_mix(hourly_df)
        if brand_df.empty or brand_df["value"].sum() <= 0:
            st.info("Audience mix data not available for this selection.")
        else:
            st.plotly_chart(make_brand_mix_figure(brand_df), use_container_width=True)

    with tabs[5]:
        st.markdown("<div class='section-title'>Metric Diagnostics</div>", unsafe_allow_html=True)
        diag = pd.DataFrame(
            [
                ["Nearby Exposure", metrics.walk_by, metrics.walk_by_source or "curated dashboard"],
                ["Store Interest", metrics.interest, "curated dashboard"],
                ["Near-store", metrics.near_store, "curated dashboard"],
                ["Store Visits", metrics.store_visits, "curated dashboard"],
                ["Total Sessions", metrics.total_sessions, "curated dashboard"],
                ["Qualified Visits", metrics.qualified_visits, "curated dashboard"],
                ["Engaged Visits", metrics.engaged_visits, "curated dashboard"],
                ["Deeply Engaged Visits", metrics.deeply_engaged_visits, "curated dashboard"],
                ["Avg Dwell (sec)", metrics.avg_dwell_seconds, "curated dashboard"],
                ["Traffic Health", metrics.traffic_health_band, "curated dashboard"],
                ["Visit Quality", metrics.visit_quality_band, "curated dashboard"],
                ["Transactions", transactions, "business input"],
                ["Revenue", revenue, "business input"],
            ],
            columns=["Metric", "Value", "Source"],
        )
        st.dataframe(diag, use_container_width=True, hide_index=True)

        if metrics.notes:
            st.markdown("<div class='section-title'>Important Notes</div>", unsafe_allow_html=True)
            for n in metrics.notes:
                st.markdown(f"- {n}")

        if show_tz_diag:
            st.markdown("<div class='section-title'>Timezone Diagnostics</div>", unsafe_allow_html=True)
            tz_diag = {
                "App timezone": TIMEZONE,
                "Selected scope start": scope.start_date.isoformat(),
                "Selected scope end": scope.end_date.isoformat(),
                "Dashboard rows": len(dash_df),
                "Hourly rows": len(hourly_df),
                "Dwell rows": len(dwell_df),
            }
            st.json(tz_diag)

        if SHOW_DEBUG:
            with st.expander("SQL debug"):
                st.code(dash_sql or "")
                st.code(hourly_sql or "")
                st.code(dwell_sql or "")


if __name__ == "__main__":
    main()
