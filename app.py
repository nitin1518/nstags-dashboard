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

try:
    from google import genai
except Exception:
    genai = None


st.set_page_config(page_title="nsTags Retail Intelligence", page_icon="📈", layout="wide")

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

section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] div,
section[data-testid="stSidebar"] span {
    color: var(--text) !important;
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

.panel {
    background: var(--card-grad);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 1rem 1rem .9rem 1rem;
    box-shadow: var(--shadow-soft);
    margin-bottom: 1rem;
}

.note, .panel, .summary-strip, .simple-callout, .info-body, .priority-body, .story-body, .kpi-sub {
    color: var(--text-3);
}

.kpi-card, .priority-card, .story-card, .info-card, .summary-strip, .simple-callout, .alert-panel {
    background: var(--card-grad);
    border: 1px solid var(--border);
    border-radius: 18px;
    box-shadow: var(--shadow-soft);
}

.kpi-card {
    padding: 1rem 1rem .92rem 1rem;
    min-height: 158px;
    position: relative;
    overflow: hidden;
    margin-bottom: .9rem;
}

.kpi-card::after {
    content: '';
    position: absolute;
    top: -36px;
    right: -36px;
    width: 92px;
    height: 92px;
    background: radial-gradient(circle, rgba(99,102,241,0.08), transparent 70%);
    border-radius: 50%;
}

.kpi-label, .story-label, .priority-label, .info-title {
    font-size: .7rem;
    text-transform: uppercase;
    letter-spacing: .1em;
    font-weight: 800;
    color: var(--accent);
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

.kpi-formula {
    margin-top: .62rem;
    padding-top: .55rem;
    border-top: 1px dashed rgba(99,102,241,0.16);
    color: var(--text-muted);
    font-size: .76rem;
    line-height: 1.45;
}

.priority-card, .story-card, .info-card, .summary-strip, .simple-callout, .alert-panel {
    padding: 1rem;
    margin-bottom: .9rem;
}

.priority-title, .story-title {
    font-size: 1.08rem;
    font-weight: 800;
    color: var(--text);
    line-height: 1.25;
    margin-bottom: .45rem;
}

.alert-title {
    font-size: .72rem;
    text-transform: uppercase;
    letter-spacing: .12em;
    font-weight: 800;
    color: var(--warn);
    margin-bottom: .3rem;
}

.alert-headline {
    font-size: 1.22rem;
    font-weight: 800;
    color: var(--text);
    margin-bottom: .35rem;
}

.alert-body {
    color: var(--text-3);
    font-size: .9rem;
    line-height: 1.6;
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

.stTabs [data-baseweb="tab-list"],
div[data-testid="stTabs"] div[role="tablist"] {
    background: rgba(99,102,241,0.04) !important;
    border: 1px solid var(--border) !important;
    border-radius: 14px !important;
    padding: 5px !important;
    gap: 4px !important;
    display: flex !important;
    flex-wrap: nowrap !important;
    overflow-x: auto !important;
}

.stTabs [data-baseweb="tab"],
div[data-testid="stTabs"] button[role="tab"] {
    border-radius: 10px !important;
    color: var(--text-muted) !important;
    font-weight: 700 !important;
    white-space: nowrap !important;
    min-width: max-content !important;
    flex: 0 0 auto !important;
    padding: .48rem .92rem !important;
}

.stTabs [aria-selected="true"],
div[data-testid="stTabs"] button[aria-selected="true"] {
    background: rgba(99,102,241,0.14) !important;
    color: var(--accent) !important;
    box-shadow: 0 0 0 1px rgba(99,102,241,0.14) inset !important;
}
</style>
""",
    unsafe_allow_html=True,
)

PLOT_CONFIG = {"displayModeBar": False, "responsive": True}
COLORS = {
    "indigo": "#6366F1",
    "violet": "#8B5CF6",
    "emerald": "#10B981",
    "amber": "#F59E0B",
    "rose": "#F43F5E",
    "sky": "#38BDF8",
    "slate": "#64748B",
    "teal": "#14B8A6",
    "blue": "#1976D2",
    "red": "#FF2D2D",
    "orange": "#F59E0B",
    "light_blue": "#75B0DE",
    "grey": "#7B8794",
}
DAYS_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
DWELL_LABEL_MAP = {
    "00-10s": "Walk-by only",
    "10-30s": "Quick look",
    "30-60s": "Interested",
    "01-03m": "Browsing",
    "03-05m": "Engaged",
    "05m+": "Highly engaged",
}

AWS_REGION = st.secrets.get("AWS_REGION", "ap-south-1")
ATHENA_DATABASE = st.secrets.get("ATHENA_DATABASE", "nstags_analytics")
ATHENA_WORKGROUP = st.secrets.get("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT = st.secrets.get("ATHENA_OUTPUT", "s3://nstags-datalake-hq-2026/athena-results/")
AWS_ACCESS_KEY_ID = st.secrets.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = st.secrets.get("AWS_SECRET_ACCESS_KEY")
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY")

META_CACHE_TTL = 1800
DATA_CACHE_TTL = 900
AI_CACHE_TTL = 300
ATHENA_RESULT_REUSE_MINUTES = 60

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    st.error("Missing AWS credentials in Streamlit secrets.")
    st.stop()

if "loaded_bundle" not in st.session_state:
    st.session_state.loaded_bundle = None
if "last_loaded_filters" not in st.session_state:
    st.session_state.last_loaded_filters = None


@st.cache_resource
def get_aws_clients():
    aws_session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    return aws_session.client("athena"), aws_session.client("s3")


athena_client, s3_client = get_aws_clients()


def validate_store_id(store_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", str(store_id)):
        raise ValueError("Invalid store_id")
    return store_id


def s3_uri_to_bucket_key(s3_uri: str):
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip("/")


def safe_div(a, b):
    try:
        a = float(a)
        b = float(b)
        if b == 0:
            return 0.0
        return a / b
    except Exception:
        return 0.0


def clamp_0_100(x):
    try:
        return max(0.0, min(100.0, float(x)))
    except Exception:
        return 0.0


def fmt_int(x):
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "0"


def fmt_float(x, digits: int = 2):
    try:
        return f"{float(x):,.{digits}f}"
    except Exception:
        return "0"


def fmt_currency(x):
    try:
        return f"₹{float(x):,.0f}"
    except Exception:
        return "₹0"


def fmt_pct_from_ratio(x, digits: int = 1):
    try:
        return f"{100 * float(x):.{digits}f}%"
    except Exception:
        return "0.0%"


def fmt_seconds(x):
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


def fmt_minutes_simple(seconds):
    try:
        return f"{float(seconds)/60:.1f} min"
    except Exception:
        return "-"


def score_band(score: float):
    try:
        score = float(score)
    except Exception:
        return "badge-bad", "Weak"
    if score >= 75:
        return "badge-good", "Strong"
    if score >= 50:
        return "badge-warn", "Moderate"
    return "badge-bad", "Weak"


def benchmark_maturity_label(population):
    try:
        population = int(population)
    except Exception:
        return "Unknown", "badge-bad", "Benchmark population unavailable."
    if population >= 100:
        return "Stable", "badge-good", f"Benchmark built on {population:,} store-day records."
    if population >= 30:
        return "Growing", "badge-warn", f"Benchmark built on {population:,} store-day records. Directionally useful, still maturing."
    return "Early", "badge-bad", f"Benchmark built on only {population:,} store-day records. Scores are provisional."


def safe_pct(numerator, denominator):
    try:
        numerator = float(numerator)
        denominator = float(denominator)
        if denominator <= 0:
            return 0.0
        return numerator / denominator
    except Exception:
        return 0.0


def compute_reporting_confidence(days_in_scope: int, benchmark_population: int) -> float:
    score = 0.0
    try:
        if days_in_scope >= 30:
            score += 60
        elif days_in_scope >= 14:
            score += 45
        elif days_in_scope >= 7:
            score += 30
        elif days_in_scope >= 3:
            score += 18
        elif days_in_scope >= 1:
            score += 10
    except Exception:
        pass

    try:
        if benchmark_population >= 100:
            score += 40
        elif benchmark_population >= 50:
            score += 28
        elif benchmark_population >= 30:
            score += 18
        elif benchmark_population > 0:
            score += 8
    except Exception:
        pass

    return max(0.0, min(100.0, score))


def compute_smv_score(metrics: dict) -> float:
    walk_by_score = float(metrics.get("walk_by_score", 0) or 0)
    store_magnet_score = float(metrics.get("store_magnet_percentile_score", 0) or 0)
    premium_mix_score = float(metrics.get("premium_device_mix_score", 0) or 0)
    days_in_scope = int(metrics.get("days_in_scope", 0) or 0)
    benchmark_population = int(metrics.get("benchmark_population", 0) or 0)

    reporting_confidence = compute_reporting_confidence(days_in_scope, benchmark_population)

    if days_in_scope <= 0:
        freshness_score = 0.0
    elif days_in_scope == 1:
        freshness_score = 100.0
    elif days_in_scope <= 7:
        freshness_score = 92.0
    elif days_in_scope <= 30:
        freshness_score = 84.0
    else:
        freshness_score = 72.0

    smv = (
        0.30 * walk_by_score
        + 0.25 * store_magnet_score
        + 0.20 * premium_mix_score
        + 0.15 * reporting_confidence
        + 0.10 * freshness_score
    )
    return round(max(0.0, min(100.0, smv)), 1)


def smv_tier(smv: float) -> str:
    if smv >= 85:
        return "Platinum"
    if smv >= 70:
        return "Gold"
    if smv >= 55:
        return "Silver"
    return "Emerging"


def benchmark_directional_note(benchmark_population: int) -> str:
    if benchmark_population < 50:
        return "Directional only: network benchmark base is still light for this comparison."
    if benchmark_population < 100:
        return "Reasonably directional: benchmark base is growing but not yet fully mature."
    return "Benchmark confidence is stable for store-to-network comparison."


def infer_trend_grain(start_date: date, end_date: date) -> str:
    span_days = (end_date - start_date).days + 1
    if span_days <= 14:
        return "day"
    if span_days <= 120:
        return "week"
    return "month"


def scope_title(period_mode: str, start_date: date, end_date: date) -> str:
    if period_mode == "Daily":
        return f"Daily snapshot · {start_date.strftime('%d %b %Y')}"
    if period_mode == "Weekly":
        return f"Last 7 days · {start_date.strftime('%d %b')} → {end_date.strftime('%d %b %Y')}"
    if period_mode == "Monthly":
        return f"Last 30 days · {start_date.strftime('%d %b')} → {end_date.strftime('%d %b %Y')}"
    if period_mode == "Yearly":
        return f"Last 365 days · {start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}"
    return f"Custom period · {start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}"


def style_chart(fig):
    fig.update_layout(
        hovermode="x unified",
        margin=dict(l=12, r=12, t=44, b=55),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=11),
        legend=dict(orientation="h", yanchor="top", y=-0.16, xanchor="center", x=0.5, bgcolor="rgba(0,0,0,0)"),
    )
    fig.update_xaxes(showgrid=False, zeroline=False, title_text="")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(99,102,241,0.10)", zeroline=False, title_text="")
    return fig


def render_card(label: str, value: str, sub: str, formula: str = ""):
    formula_html = f"<div class='kpi-formula'>{formula}</div>" if formula else ""
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-sub">{sub}</div>
            {formula_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_story_card(label: str, title: str, pills: list[str], body: str):
    pills_html = "".join([f"<span class='metric-pill'>{p}</span>" for p in pills])
    st.markdown(
        f"""
        <div class="story-card">
            <div class="story-label">{label}</div>
            <div class="story-title">{title}</div>
            <div class="story-body">{pills_html}<br><br>{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_priority_card(label: str, title: str, body: str, pills: list[str] | None = None):
    pills_html = "".join([f"<span class='metric-pill'>{p}</span>" for p in pills or []])
    sep = "<br><br>" if pills_html else ""
    st.markdown(
        f"""
        <div class="priority-card">
            <div class="priority-label">{label}</div>
            <div class="priority-title">{title}</div>
            <div class="priority-body">{pills_html}{sep}{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_info_card(title: str, body: str):
    st.markdown(
        f"""
        <div class="info-card">
            <div class="info-title">{title}</div>
            <div class="info-body">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


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
            walk_by_traffic=("walk_by_traffic", "sum"),
            store_interest=("store_interest", "sum"),
            near_store=("near_store", "sum"),
            store_visits=("store_visits", "sum"),
            qualified_footfall=("qualified_footfall", "sum"),
            engaged_visits=("engaged_visits", "sum"),
            avg_dwell_seconds=("avg_dwell_seconds", "mean"),
            avg_estimated_people=("avg_estimated_people", "mean"),
            avg_detected_devices=("avg_detected_devices", "mean"),
        )
        .sort_values("period_start")
    )
    trend["period_label"] = trend["period_start"].dt.strftime(label_fmt)
    trend["qualified_rate"] = trend.apply(lambda x: safe_div(x["qualified_footfall"], x["store_visits"]), axis=1)
    trend["engaged_rate"] = trend.apply(lambda x: safe_div(x["engaged_visits"], x["store_visits"]), axis=1)
    trend["traffic_capture_ratio"] = trend.apply(lambda x: safe_div(x["store_interest"], x["walk_by_traffic"]), axis=1)
    trend["near_store_ratio"] = trend.apply(lambda x: safe_div(x["near_store"], x["walk_by_traffic"]), axis=1)
    trend["conversion_rate"] = trend.apply(lambda x: safe_div(x["store_visits"], x["walk_by_traffic"]), axis=1)
    return trend


def build_weekday_trend(daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df.empty:
        return pd.DataFrame()

    df = daily_df.copy()
    df["metric_date"] = pd.to_datetime(df["metric_date"])
    df["weekday_num"] = df["metric_date"].dt.weekday
    df["weekday"] = df["metric_date"].dt.day_name()

    out = (
        df.groupby(["weekday_num", "weekday"], as_index=False)
        .agg(
            walk_by_traffic=("walk_by_traffic", "sum"),
            store_interest=("store_interest", "sum"),
            near_store=("near_store", "sum"),
            store_visits=("store_visits", "sum"),
            qualified_footfall=("qualified_footfall", "sum"),
            engaged_visits=("engaged_visits", "sum"),
            avg_dwell_seconds=("avg_dwell_seconds", "mean"),
        )
        .sort_values("weekday_num")
    )
    out["weekday"] = pd.Categorical(out["weekday"], categories=DAYS_ORDER, ordered=True)
    out["qualified_rate"] = out.apply(lambda x: safe_div(x["qualified_footfall"], x["store_visits"]), axis=1)
    out["engaged_rate"] = out.apply(lambda x: safe_div(x["engaged_visits"], x["store_visits"]), axis=1)
    out["conversion_rate"] = out.apply(lambda x: safe_div(x["store_visits"], x["walk_by_traffic"]), axis=1)
    out["attention_rate"] = out.apply(lambda x: safe_div(x["store_interest"], x["walk_by_traffic"]), axis=1)
    out["consideration_rate"] = out.apply(lambda x: safe_div(x["near_store"], x["walk_by_traffic"]), axis=1)
    return out.sort_values("weekday")


def prepare_dwell_plot_df(source_df: pd.DataFrame) -> pd.DataFrame:
    dwell_order = ["00-10s", "10-30s", "30-60s", "01-03m", "03-05m", "05m+"]
    plot_df = source_df.copy()
    if plot_df.empty or "dwell_bucket" not in plot_df.columns:
        return plot_df
    plot_df["dwell_bucket"] = pd.Categorical(plot_df["dwell_bucket"], categories=dwell_order, ordered=True)
    plot_df = plot_df.sort_values("dwell_bucket")
    total = plot_df["visits"].sum()
    plot_df["share"] = plot_df["visits"].apply(lambda v: safe_div(v, total))
    plot_df["display_label"] = plot_df["dwell_bucket"].astype(str).map(DWELL_LABEL_MAP)
    return plot_df


def classify_band(value: float, good_cutoff: float, warn_cutoff: float) -> tuple[str, str]:
    try:
        v = float(value)
    except Exception:
        return "badge-bad", "Weak"
    if v >= good_cutoff:
        return "badge-good", "Strong"
    if v >= warn_cutoff:
        return "badge-warn", "Moderate"
    return "badge-bad", "Weak"


def compute_ai_confidence(metrics: dict) -> tuple[int, str]:
    base = 45
    benchmark_population = int(metrics.get("benchmark_population", 0) or 0)
    days_in_scope = int(metrics.get("days_in_scope", 0) or 0)
    volume = float(metrics.get("base_volume", 0) or 0)
    spread_inputs = [
        float(metrics.get("traffic_intelligence_index", 0) or 0),
        float(metrics.get("visit_quality_index", 0) or 0),
        float(metrics.get("store_attraction_index", 0) or 0),
        float(metrics.get("audience_quality_index", 0) or 0),
    ]

    if benchmark_population >= 100:
        base += 18
    elif benchmark_population >= 30:
        base += 10
    elif benchmark_population > 0:
        base += 4

    if days_in_scope >= 30:
        base += 10
    elif days_in_scope >= 7:
        base += 7
    elif days_in_scope >= 2:
        base += 4
    else:
        base += 1

    if volume >= 1000:
        base += 12
    elif volume >= 300:
        base += 8
    elif volume >= 100:
        base += 5
    elif volume > 0:
        base += 2

    score_spread = max(spread_inputs) - min(spread_inputs)
    if score_spread <= 15:
        base += 8
    elif score_spread <= 30:
        base += 5
    else:
        base += 2

    confidence = max(40, min(96, int(round(base))))
    band = "High" if confidence >= 85 else "Moderate" if confidence >= 70 else "Directional"
    return confidence, band


def compute_scope_metrics(daily_df: pd.DataFrame, active_transactions: int, active_value: int) -> dict:
    score_row = daily_df.iloc[-1].to_dict() if not daily_df.empty else {}

    metrics = {
        "walk_by": float(daily_df["walk_by_traffic"].sum()),
        "interest": float(daily_df["store_interest"].sum()),
        "near_store": float(daily_df["near_store"].sum()),
        "store_visits": float(daily_df["store_visits"].sum()),
        "qualified_visits": float(daily_df["qualified_footfall"].sum()),
        "engaged_visits": float(daily_df["engaged_visits"].sum()),
        "avg_dwell_seconds": float(daily_df["avg_dwell_seconds"].mean()),
        "avg_estimated_people": float(daily_df["avg_estimated_people"].mean()) if "avg_estimated_people" in daily_df.columns else 0.0,
        "avg_detected_devices": float(daily_df["avg_detected_devices"].mean()) if "avg_detected_devices" in daily_df.columns else 0.0,
        "peak_estimated_people": float(daily_df["peak_estimated_people"].max()) if "peak_estimated_people" in daily_df.columns else 0.0,
        "peak_detected_devices": float(daily_df["peak_detected_devices"].max()) if "peak_detected_devices" in daily_df.columns else 0.0,
        "transactions": float(active_transactions),
        "value": float(active_value),
        "traffic_intelligence_index": float(score_row.get("traffic_intelligence_index", 0) or 0),
        "visit_quality_index": float(score_row.get("visit_quality_index", 0) or 0),
        "store_attraction_index": float(score_row.get("store_attraction_index", 0) or 0),
        "audience_quality_index": float(score_row.get("audience_quality_index", 0) or 0),
        "store_magnet_percentile_score": float(score_row.get("store_magnet_percentile_score", 0) or 0),
        "window_capture_score": float(score_row.get("window_capture_score", 0) or 0),
        "entry_efficiency_percentile_score": float(score_row.get("entry_efficiency_percentile_score", 0) or 0),
        "dwell_quality_score": float(score_row.get("dwell_quality_score", 0) or 0),
        "walk_by_score": float(score_row.get("walk_by_score", 0) or 0),
        "interest_score": float(score_row.get("interest_score", 0) or 0),
        "near_store_score": float(score_row.get("near_store_score", 0) or 0),
        "premium_device_mix_score": float(score_row.get("premium_device_mix_score", 0) or 0),
        "benchmark_population": int(score_row.get("benchmark_population", 0) or 0),
    }

    metrics["qualified_rate"] = safe_div(metrics["qualified_visits"], metrics["store_visits"])
    metrics["engaged_rate"] = safe_div(metrics["engaged_visits"], metrics["store_visits"])
    metrics["conversion_rate"] = safe_div(metrics["store_visits"], metrics["walk_by"])
    metrics["traffic_capture_ratio"] = safe_div(metrics["interest"], metrics["walk_by"])
    metrics["near_store_ratio"] = safe_div(metrics["near_store"], metrics["walk_by"])
    metrics["commercial_ratio"] = safe_div(metrics["transactions"], metrics["store_visits"])

    metrics["attention_rate"] = metrics["traffic_capture_ratio"]
    metrics["consideration_rate"] = safe_div(metrics["near_store"], metrics["interest"])
    metrics["storefront_engagement_rate"] = safe_div(metrics["near_store"], metrics["walk_by"])
    metrics["response_per_1k_walkby"] = safe_div(metrics["transactions"] * 1000, metrics["walk_by"])
    metrics["value_per_1k_walkby"] = safe_div(metrics["value"] * 1000, metrics["walk_by"])
    metrics["value_per_attention"] = safe_div(metrics["value"], metrics["interest"])
    metrics["value_per_near_store"] = safe_div(metrics["value"], metrics["near_store"])
    metrics["response_per_100_attention"] = safe_div(metrics["transactions"] * 100, metrics["interest"])
    metrics["response_per_100_near_store"] = safe_div(metrics["transactions"] * 100, metrics["near_store"])

    media_response_efficiency_score = clamp_0_100(metrics["response_per_100_attention"] * 5)
    media_value_efficiency_score = clamp_0_100(metrics["value_per_1k_walkby"] / 10)
    media_index = (
        0.25 * metrics["store_magnet_percentile_score"]
        + 0.20 * metrics["window_capture_score"]
        + 0.20 * metrics["audience_quality_index"]
        + 0.20 * media_response_efficiency_score
        + 0.15 * media_value_efficiency_score
    )
    metrics["media_response_efficiency_score"] = media_response_efficiency_score
    metrics["media_value_efficiency_score"] = media_value_efficiency_score
    metrics["retail_media_index"] = round(media_index, 1)
    metrics["retail_media_rank_band"] = (
        "Top 10%" if media_index >= 90 else
        "Top 25%" if media_index >= 75 else
        "Top 50%" if media_index >= 50 else
        "Bottom 50%"
    )
    metrics["retail_media_positioning"] = (
        "Premium media store"
        if media_index >= 80 else
        "Competitive media store"
        if media_index >= 60 else
        "Emerging media store"
        if media_index >= 40 else
        "Needs media improvement"
    )
    return metrics


def get_priority_narratives(mode, primary_bottleneck):
    if mode == "Retail Media":
        return {
            "traffic_title": "Storefront audience scale is the first media asset",
            "traffic_body": (
                "For a partner brand, the first question is whether this storefront consistently delivers enough passing audience "
                "to justify media placement. Read this as audience opportunity at the storefront, not as in-store demand."
            ),
            "visit_title": "Stopping power matters more than deep in-store browsing",
            "visit_body": (
                "The more important media question is how much surrounding audience slows down and becomes measurable storefront attention. "
                "This is a better signal of display effectiveness than deeper in-store engagement alone."
            ),
            "commercial_title": "Premium mix and network rank define partner attractiveness",
            "commercial_body": (
                "For partner evaluation, premium device mix and network ranking help determine whether this store attracts the right audience "
                "profile and deserves stronger media priority within the nsTags platform."
            ),
            "primary_bottleneck_story": {
                "Store Magnet": "The store is not converting enough surrounding movement into visible storefront attention. Improve hero creatives, brightness, signage angle, and first-look impact.",
                "Window Capture": "People notice the store, but too few move closer. The campaign needs stronger stopping power, sharper visual hierarchy, and clearer product message.",
                "Entry Efficiency": "Entry efficiency matters less for retail media than storefront attention. The commercial story should stay focused on attention and response, not in-store browsing.",
                "Dwell Quality": "Dwell is directionally useful, but not the core proof point for storefront media. Strengthen attention quality and response storytelling first.",
                "Floor Conversion": "Commercial response is the weakest current layer. The shop owner needs a cleaner proof of how attention is translating into value for the partner brand.",
            }.get(primary_bottleneck, "The weakest layer currently reduces the store's ability to prove media value."),
        }
    return {
        "traffic_title": "Storefront pull is the first business gate",
        "traffic_body": (
            "The first question for store owners is whether the store is converting nearby traffic into serious attention. "
            "If people pass by but do not slow down or approach, later selling excellence cannot fully compensate."
        ),
        "visit_title": "Visit quality is the clearest indicator of store health",
        "visit_body": (
            "High-quality visits are the real operating opportunity. Qualified and engaged visits show whether entry is turning "
            "into browsing, assistance, and purchase intent rather than shallow walk-ins."
        ),
        "commercial_title": "Commercial outcome must be read with context",
        "commercial_body": (
            "Transactions / responses reflect business outcome for the selected period. This should be interpreted alongside "
            "visit quality, because response counts can exceed visit counts in some business setups."
        ),
        "primary_bottleneck_story": {
            "Store Magnet": "Current capture is weak, so the biggest opportunity is improving storefront noticeability and stopping power.",
            "Window Capture": "Attention exists, but it is not translating strongly into closer approach or entry.",
            "Entry Efficiency": "Entry is happening, but too many visits are not becoming meaningful store interactions.",
            "Dwell Quality": "Visitors are entering, but not staying long enough to reflect stronger browsing or deeper engagement.",
            "Floor Conversion": "The store is generating visit quality, but business closure is the weakest layer right now.",
        }.get(primary_bottleneck, "This is currently the weakest business layer in the store journey."),
    }


def build_top_insights(mode, metrics: dict, weekday_peak: str | None):
    if mode == "Retail Media":
        return [
            {
                "label": "Value To Partner Brand",
                "title": "The storefront sells measurable attention, not just raw traffic",
                "body": (
                    f"The store delivered a safe storefront audience base of {fmt_int(metrics['walk_by'])}, converted it into "
                    f"{fmt_int(metrics['interest'])} measurable attention events, and moved {fmt_int(metrics['near_store'])} people "
                    f"into stronger near-store consideration. That is the clearest partner-facing value proof today."
                ),
                "pills": [
                    f"Attention {fmt_pct_from_ratio(metrics['attention_rate'])}",
                    f"Near-store {fmt_pct_from_ratio(metrics['storefront_engagement_rate'])}",
                    f"Best day {weekday_peak or 'N/A'}",
                ],
            },
            {
                "label": "Value To Shop Owner",
                "title": "Campaign performance should link back to business value",
                "body": (
                    f"With {fmt_int(metrics['transactions'])} response events and {fmt_currency(metrics['value'])} of reported value, "
                    f"the owner can show both partner-facing media delivery and owner-facing business impact from the campaign window."
                ),
                "pills": [
                    f"Resp / 1K audience {fmt_float(metrics['response_per_1k_walkby'], 1)}",
                    f"Value / 1K audience {fmt_currency(metrics['value_per_1k_walkby'])}",
                ],
            },
            {
                "label": "Platform Position",
                "title": "Use one store media value score to compare stores",
                "body": (
                    f"The SMV score is {fmt_float(metrics['smv_score'], 1)}, placing the store in the {metrics['smv_tier']} tier. "
                    f"This gives partner teams a fast way to compare storefront media attractiveness across the nsTags platform."
                ),
                "pills": [
                    metrics["retail_media_positioning"],
                    f"Premium mix {fmt_float(metrics['premium_device_mix_score'], 0)}",
                ],
            },
        ]
    return [
        {
            "label": "Top Priority",
            "title": "Biggest operational bottleneck",
            "body": (
                "The biggest current improvement area is the weakest layer between surrounding traffic and final store outcome. "
                "That is the business layer creating the largest drag in the store journey."
            ),
            "pills": [weekday_peak or "Weekday pattern unavailable"],
        },
        {
            "label": "Visit Quality",
            "title": "Meaningful visits vs shallow visits",
            "body": (
                f"The store generated {fmt_int(metrics['store_visits'])} visits, of which {fmt_int(metrics['qualified_visits'])} were meaningful and "
                f"{fmt_int(metrics['engaged_visits'])} were deeper interactions. Average dwell of {fmt_seconds(metrics['avg_dwell_seconds'])} helps explain whether "
                f"people are browsing seriously or leaving too quickly."
            ),
            "pills": [f"Qualified {fmt_pct_from_ratio(metrics['qualified_rate'])}", f"Engaged {fmt_pct_from_ratio(metrics['engaged_rate'])}"],
        },
        {
            "label": "Trading Read",
            "title": "Commercial outcome in context",
            "body": (
                f"Commercial response should be read with visit quality, not in isolation. The strongest operating days currently lean toward "
                f"{weekday_peak or 'unavailable'}, which is useful for staffing, display planning, and offer timing."
            ),
            "pills": [f"Commercial ratio {fmt_pct_from_ratio(metrics['commercial_ratio'])}"],
        },
    ]


def normalize_weekday_table(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return out
    for c in ["walk_by_traffic", "store_interest", "near_store", "store_visits", "qualified_footfall", "engaged_visits"]:
        if c in out.columns:
            out[c] = out[c].round(0).astype(int)
    if "avg_dwell_seconds" in out.columns:
        out["avg_dwell_seconds"] = out["avg_dwell_seconds"].round(1)
    if "attention_rate" in out.columns:
        out["attention_rate"] = (out["attention_rate"] * 100).round(1)
    if "consideration_rate" in out.columns:
        out["consideration_rate"] = (out["consideration_rate"] * 100).round(1)
    if "conversion_rate" in out.columns:
        out["conversion_rate"] = (out["conversion_rate"] * 100).round(1)
    return out


def compute_overall_brand_mix(hourly_df: pd.DataFrame) -> pd.DataFrame:
    if hourly_df.empty:
        return pd.DataFrame(columns=["brand", "value"])
    agg = {
        "Apple": hourly_df["avg_apple_devices"].sum() if "avg_apple_devices" in hourly_df.columns else 0,
        "Samsung": hourly_df["avg_samsung_devices"].sum() if "avg_samsung_devices" in hourly_df.columns else 0,
        "Other": hourly_df["avg_other_devices"].sum() if "avg_other_devices" in hourly_df.columns else 0,
    }
    return pd.DataFrame({"brand": list(agg.keys()), "value": list(agg.values())})


def performance_label(score: float) -> str:
    try:
        score = float(score)
    except Exception:
        return "Needs attention"
    if score >= 75:
        return "Strong"
    if score >= 50:
        return "Average"
    return "Needs attention"


def audience_label(score: float) -> str:
    try:
        score = float(score)
    except Exception:
        return "Unknown"
    if score < 40:
        return "Low"
    if score < 60:
        return "Average"
    if score < 80:
        return "High"
    return "Excellent"


def store_health_label(conversion_rate: float, engaged_rate: float, avg_dwell_sec: float) -> tuple[str, str]:
    score = 0
    if conversion_rate >= 0.08:
        score += 1
    if engaged_rate >= 0.30:
        score += 1
    if avg_dwell_sec >= 90:
        score += 1
    if score == 3:
        return "Strong", "badge-good"
    if score == 2:
        return "Average", "badge-warn"
    return "Needs attention", "badge-bad"


def media_index_label(score: float) -> tuple[str, str]:
    if score >= 80:
        return "Premium media store", "badge-good"
    if score >= 60:
        return "Competitive media store", "badge-warn"
    return "Developing media store", "badge-bad"


def build_store_summary_sentence(metrics: dict, mode: str):
    if mode == "Retail Media":
        return (
            f"This store delivered a safe storefront audience base of {fmt_int(metrics['walk_by'])}, captured "
            f"{fmt_int(metrics['interest'])} measurable attention events, and moved {fmt_int(metrics['near_store'])} people "
            f"into stronger near-store consideration. It currently sits in the {metrics['smv_tier']} media tier with an "
            f"SMV score of {fmt_float(metrics['smv_score'], 1)}."
        )
    return (
        f"This store recorded {fmt_int(metrics['store_visits'])} visits in the selected period, with "
        f"{fmt_pct_from_ratio(metrics['qualified_rate'])} qualifying beyond 30 seconds and average dwell of "
        f"{fmt_minutes_simple(metrics['avg_dwell_seconds'])}."
    )


@st.cache_data(ttl=AI_CACHE_TTL)
def generate_ai_brief(ai_payload: dict) -> str:
    if not GEMINI_API_KEY or genai is None:
        return "⚠️ AI unavailable: GEMINI_API_KEY not configured."
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)

        if ai_payload["mode"] == "Retail Media":
            prompt = f"""
You are writing a short executive insight for a retail media dashboard.

METRICS
Safe storefront audience: {ai_payload['walk_by']}
Storefront interest: {ai_payload['interest']}
Near-store audience: {ai_payload['near_store']}
Attention capture rate: {ai_payload['attention_rate']}%
Premium audience mix score: {ai_payload['premium_mix_score']}
SMV score: {ai_payload['smv_score']}
SMV tier: {ai_payload['smv_tier']}
Benchmark population: {ai_payload['benchmark_population']}
Partner response events: {ai_payload['transactions']}
Revenue / campaign value: {ai_payload['value']}
Value / 1k audience: {ai_payload['value_per_1k_reach']}
Response / 1k audience: {ai_payload['response_per_1k_reach']}

RULES
1. Treat safe storefront audience as a business-safe top-of-funnel proxy, not a literal measured impression count.
2. Focus on partner value: audience scale, stopping power, premium audience mix, and store attractiveness.
3. Do not emphasize in-store qualified visits, engaged visits, or dwell as hero metrics.
4. If benchmark population is below 50, clearly say the comparison is directional only.
5. Keep the output crisp, executive-friendly, and commercially useful.

Return exactly in this format:

**Executive Summary**
[2-3 sentences]

**Top Priority**
[1 short paragraph]

**Audience Interpretation**
[1 short paragraph]

**Partner Fit**
[1 short paragraph]

**Recommended Action**
- [bullet 1]
- [bullet 2]
- [bullet 3]
"""
        else:
            prompt = f"""
You are a senior retail strategy consultant writing for business leaders.
Do not mention sensors, device scanning, BLE, ingestion, Athena, SQL, pipelines, or backend systems.

BUSINESS CONTEXT
Mode: {ai_payload['mode']}
Scope: {ai_payload['scope']}
AI Confidence Score: {ai_payload['ai_confidence']}% ({ai_payload['ai_confidence_band']})

SELECTED METRICS
Safe storefront audience: {ai_payload['walk_by']}
Storefront interest: {ai_payload['interest']}
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
Traffic Intelligence Index: {ai_payload['tii']}
Visit Quality Index: {ai_payload['vqi']}
Store Attraction Index: {ai_payload['sai']}
Audience Quality Index: {ai_payload['aqi']}

PRIMARY OPPORTUNITY
{ai_payload['primary_bottleneck']}

INTERPRETATION RULES
1. Focus on storefront pull, visit quality, and trading outcome.
2. Do not describe commercial ratio as strict person conversion.
3. Keep the writing crisp, executive-friendly, and decisive.
4. Do not invent metrics not provided here.

Return exactly in this format:

**Executive Summary**
[2-3 sentences]

**Top Priority**
[1 short paragraph]

**Traffic Interpretation**
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


def run_athena_query(query: str, database: str = ATHENA_DATABASE, timeout_sec: int = 45) -> pd.DataFrame:
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
            raise RuntimeError("Athena query timed out")
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Unknown Athena error")
        raise RuntimeError(f"Athena query failed: {reason}")

    output_location = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    bucket, key = s3_uri_to_bucket_key(output_location)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))


@st.cache_data(ttl=META_CACHE_TTL)
def load_store_list() -> pd.DataFrame:
    return run_athena_query("SELECT DISTINCT store_id FROM nstags_available_dates_curated_inc ORDER BY store_id")


@st.cache_data(ttl=META_CACHE_TTL)
def load_available_dates(store_id: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        SELECT metric_date
        FROM nstags_available_dates_curated_inc
        WHERE store_id = '{sid}'
        ORDER BY metric_date DESC
        """
    )


@st.cache_data(ttl=DATA_CACHE_TTL)
def load_dashboard_daily_rows(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        SELECT
            metric_date,
            walk_by_traffic,
            store_interest,
            near_store,
            store_visits,
            qualified_footfall,
            engaged_visits,
            avg_dwell_seconds,
            median_dwell_seconds,
            avg_estimated_people,
            peak_estimated_people,
            avg_detected_devices,
            peak_detected_devices,
            qualified_visit_rate,
            engaged_visit_rate,
            walkby_to_visit_index,
            traffic_intelligence_index,
            visit_quality_index,
            store_attraction_index,
            audience_quality_index,
            walk_by_score,
            interest_score,
            near_store_score,
            qualified_score,
            engaged_score,
            dwell_score,
            store_magnet_percentile_score,
            window_capture_score,
            entry_efficiency_percentile_score,
            dwell_quality_score,
            premium_device_mix_score,
            volume_confidence_score,
            benchmark_population,
            store_id
        FROM nstags_dashboard_metrics_canonical_enriched
        WHERE store_id = '{sid}'
          AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        ORDER BY metric_date
        """
    )


@st.cache_data(ttl=DATA_CACHE_TTL)
def load_hourly_traffic_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        SELECT
            hour_of_day,
            hour_label,
            ROUND(AVG(avg_far_devices), 2) AS avg_far_devices,
            ROUND(AVG(avg_mid_devices), 2) AS avg_mid_devices,
            ROUND(AVG(avg_near_devices), 2) AS avg_near_devices,
            ROUND(AVG(avg_apple_devices), 2) AS avg_apple_devices,
            ROUND(AVG(avg_samsung_devices), 2) AS avg_samsung_devices,
            ROUND(AVG(avg_other_devices), 2) AS avg_other_devices
        FROM nstags_hourly_traffic_curated_inc
        WHERE store_id = '{sid}'
          AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        GROUP BY hour_of_day, hour_label
        ORDER BY hour_of_day
        """
    )


@st.cache_data(ttl=DATA_CACHE_TTL)
def load_dwell_buckets_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        SELECT dwell_bucket, ROUND(SUM(visits), 2) AS visits
        FROM nstags_dwell_buckets_curated_inc
        WHERE store_id = '{sid}'
          AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        GROUP BY dwell_bucket
        """
    )


@st.cache_data(ttl=DATA_CACHE_TTL)
def load_debug_partition_vs_ist(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        SELECT
            store_id,
            year,
            month,
            day,
            DATE(from_unixtime(ts) AT TIME ZONE 'Asia/Kolkata') AS metric_date_ist,
            COUNT(*) AS rows
        FROM nstags_live_analytics
        WHERE store_id = '{sid}'
          AND DATE(from_unixtime(ts) AT TIME ZONE 'Asia/Kolkata') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        GROUP BY 1,2,3,4,5
        ORDER BY metric_date_ist, year, month, day
        """
    )


def load_dashboard_bundle(store_id: str, start_date_str: str, end_date_str: str, show_debug: bool):
    bundle = {
        "daily_df": load_dashboard_daily_rows(store_id, start_date_str, end_date_str),
        "hourly_df": load_hourly_traffic_range(store_id, start_date_str, end_date_str),
        "dwell_df": load_dwell_buckets_range(store_id, start_date_str, end_date_str),
        "debug_df": pd.DataFrame(),
    }
    if show_debug:
        bundle["debug_df"] = load_debug_partition_vs_ist(store_id, start_date_str, end_date_str)
    return bundle


with st.sidebar:
    st.markdown("### Dashboard Mode")
    app_mode = st.radio("Business Mode", ["Retail Ops", "Retail Media"], horizontal=True, key="business_mode")

    try:
        stores_df = load_store_list()
    except Exception as e:
        st.error(f"Failed to load store list: {e}")
        st.stop()

    if stores_df.empty:
        st.warning("No stores found.")
        st.stop()

    store_options = stores_df["store_id"].dropna().astype(str).tolist()
    default_store = store_options[0]
    if st.session_state.last_loaded_filters and st.session_state.last_loaded_filters.get("selected_store") in store_options:
        default_store = st.session_state.last_loaded_filters["selected_store"]

    try:
        dates_df = load_available_dates(default_store)
    except Exception as e:
        st.error(f"Failed to load available dates: {e}")
        st.stop()

    if dates_df.empty:
        st.warning("No dates found for this store.")
        st.stop()

    dates_df["metric_date"] = pd.to_datetime(dates_df["metric_date"]).dt.date
    st.markdown("### Filters")

    with st.form("dashboard_filters_form", clear_on_submit=False):
        selected_store = st.selectbox("Store ID", options=store_options, index=store_options.index(default_store))
        selected_store_dates_df = load_available_dates(selected_store)
        selected_store_dates_df["metric_date"] = pd.to_datetime(selected_store_dates_df["metric_date"]).dt.date
        available_dates = sorted(set(selected_store_dates_df["metric_date"].dropna().tolist()))
        min_available_date = min(available_dates)
        max_available_date = max(available_dates)

        last_filters = st.session_state.last_loaded_filters or {}
        last_period_mode = last_filters.get("period_mode", "Daily")
        last_start_str = last_filters.get("start_date_str")
        last_end_str = last_filters.get("end_date_str")
        last_transactions = int(last_filters.get("transactions", 35) or 35)
        last_value = int(last_filters.get("value", 45000) or 45000)
        last_show_debug = bool(last_filters.get("show_debug", False))

        parsed_last_start = pd.to_datetime(last_start_str).date() if last_start_str else max_available_date
        parsed_last_end = pd.to_datetime(last_end_str).date() if last_end_str else max_available_date
        parsed_last_start = min(max(parsed_last_start, min_available_date), max_available_date)
        parsed_last_end = min(max(parsed_last_end, min_available_date), max_available_date)

        st.markdown("### Time Period")
        period_mode = st.radio("Analysis Window", ["Daily", "Weekly", "Monthly", "Yearly", "Custom"], index=["Daily", "Weekly", "Monthly", "Yearly", "Custom"].index(last_period_mode))

        if period_mode == "Daily":
            default_daily = parsed_last_end if parsed_last_end in available_dates else max_available_date
            rev_dates = list(reversed(available_dates))
            selected_day = st.selectbox("Date", options=rev_dates, index=rev_dates.index(default_daily), format_func=lambda d: d.strftime("%Y/%m/%d"))
            start_date = selected_day
            end_date = selected_day
        elif period_mode == "Weekly":
            end_date = st.date_input("Week End Date", value=parsed_last_end, min_value=min_available_date, max_value=max_available_date)
            start_date = max(min_available_date, end_date - timedelta(days=6))
        elif period_mode == "Monthly":
            end_date = st.date_input("Month End Date", value=parsed_last_end, min_value=min_available_date, max_value=max_available_date)
            start_date = max(min_available_date, end_date - timedelta(days=29))
        elif period_mode == "Yearly":
            end_date = st.date_input("Year End Date", value=parsed_last_end, min_value=min_available_date, max_value=max_available_date)
            start_date = max(min_available_date, end_date - timedelta(days=364))
        else:
            selected_range = st.date_input("Custom Date Range", value=(parsed_last_start, parsed_last_end), min_value=min_available_date, max_value=max_available_date)
            if isinstance(selected_range, (tuple, list)) and len(selected_range) == 2:
                start_date, end_date = selected_range[0], selected_range[1]
            else:
                start_date, end_date = parsed_last_start, parsed_last_end

        st.markdown("### Business Inputs")
        response_label = "Transactions" if app_mode == "Retail Ops" else "Partner campaign response events"
        value_label = "Revenue" if app_mode == "Retail Ops" else "Revenue / campaign value influenced"
        transactions = st.number_input(response_label, min_value=0, value=last_transactions, step=1)
        value = st.number_input(value_label, min_value=0, value=last_value, step=1000)
        show_debug = st.checkbox("Show timezone diagnostics", value=last_show_debug)
        submitted = st.form_submit_button("Refresh Dashboard", type="primary", use_container_width=True)

start_date_str = start_date.isoformat()
end_date_str = end_date.isoformat()

requested_filters = {
    "selected_store": selected_store,
    "period_mode": period_mode,
    "start_date_str": start_date_str,
    "end_date_str": end_date_str,
    "show_debug": show_debug,
    "transactions": int(transactions),
    "value": int(value),
    "app_mode": app_mode,
}

first_load = st.session_state.loaded_bundle is None
if first_load or submitted:
    try:
        with st.spinner("Loading dashboard data from Athena..."):
            st.session_state.loaded_bundle = load_dashboard_bundle(selected_store, start_date_str, end_date_str, show_debug)
            st.session_state.last_loaded_filters = requested_filters
    except Exception as e:
        st.error(f"Failed to load dashboard data: {e}")
        st.stop()

bundle = st.session_state.loaded_bundle
loaded_filters = st.session_state.last_loaded_filters or {}
daily_df = bundle["daily_df"].copy()
hourly_df = bundle["hourly_df"].copy()
dwell_df = bundle["dwell_df"].copy()
debug_df = bundle["debug_df"].copy()

if daily_df.empty:
    st.warning("No dashboard metrics were found for the selected period.")
    st.stop()

for col in daily_df.columns:
    if col != "metric_date":
        daily_df[col] = pd.to_numeric(daily_df[col], errors="coerce").fillna(0)
daily_df["metric_date"] = pd.to_datetime(daily_df["metric_date"]).dt.date

loaded_store = loaded_filters.get("selected_store")
loaded_start = loaded_filters.get("start_date_str")
loaded_end = loaded_filters.get("end_date_str")
loaded_mode = loaded_filters.get("period_mode")
active_app_mode = loaded_filters.get("app_mode", app_mode)
active_transactions = int(loaded_filters.get("transactions", transactions))
active_value = int(loaded_filters.get("value", value))
loaded_start_date = pd.to_datetime(loaded_start).date()
loaded_end_date = pd.to_datetime(loaded_end).date()
days_in_scope = (loaded_end_date - loaded_start_date).days + 1

st.markdown(
    f"""
    <div class="hero">
        <div class="eyebrow">nsTags Retail Intelligence</div>
        <h1>{"Store Performance Dashboard" if active_app_mode == "Retail Ops" else "Retail Media Performance Dashboard"}</h1>
        <p>
            {"A store-led dashboard to understand nearby traffic, visit quality, engagement, and the biggest operating opportunity."
             if active_app_mode == "Retail Ops"
             else "A partner-led dashboard to evaluate storefront audience opportunity, stopping power, premium audience mix, and network ranking across nsTags stores."}
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

metrics = compute_scope_metrics(daily_df, active_transactions, active_value)
metrics["days_in_scope"] = days_in_scope
metrics["attention_capture_rate"] = safe_pct(metrics.get("interest", 0), metrics.get("walk_by", 0))
metrics["near_store_rate"] = safe_pct(metrics.get("near_store", 0), metrics.get("walk_by", 0))
metrics["visit_yield_rate"] = safe_pct(metrics.get("store_visits", 0), metrics.get("walk_by", 0))
metrics["premium_audience_share_proxy"] = safe_div(metrics.get("premium_device_mix_score", 0), 100.0)
metrics["smv_score"] = compute_smv_score(metrics)
metrics["smv_tier"] = smv_tier(metrics["smv_score"])
metrics["benchmark_directional_note"] = benchmark_directional_note(int(metrics.get("benchmark_population", 0) or 0))

trend_grain = infer_trend_grain(loaded_start_date, loaded_end_date)
scope = scope_title(loaded_mode, loaded_start_date, loaded_end_date)
trend_df = build_period_trend(daily_df, trend_grain)
weekday_df = build_weekday_trend(daily_df)
dwell_plot_df = prepare_dwell_plot_df(dwell_df)
brand_mix_df = compute_overall_brand_mix(hourly_df)

floor_conversion_score = min(metrics["commercial_ratio"] * 400, 100)
bottlenecks = {
    "Store Magnet": metrics["store_magnet_percentile_score"],
    "Window Capture": metrics["window_capture_score"],
    "Entry Efficiency": metrics["entry_efficiency_percentile_score"],
    "Dwell Quality": metrics["dwell_quality_score"],
    "Floor Conversion": floor_conversion_score,
}
primary_bottleneck = min(bottlenecks, key=bottlenecks.get)
weekday_peak = None
if not weekday_df.empty:
    weekday_peak = str(weekday_df.sort_values("store_visits", ascending=False).iloc[0]["weekday"])
priority_narratives = get_priority_narratives(active_app_mode, primary_bottleneck)
top_insights = build_top_insights(active_app_mode, metrics, weekday_peak)

badge_tii, label_tii = score_band(metrics["traffic_intelligence_index"])
badge_vqi, label_vqi = score_band(metrics["visit_quality_index"])
badge_sai, label_sai = score_band(metrics["store_attraction_index"])
badge_aqi, label_aqi = score_band(metrics["audience_quality_index"])
maturity_label, maturity_class, maturity_text = benchmark_maturity_label(metrics["benchmark_population"])
traffic_capture_class, traffic_capture_label = classify_band(metrics["traffic_capture_ratio"], 0.45, 0.25)
engagement_depth_class, engagement_depth_label = classify_band(safe_div(metrics["engaged_visits"], metrics["qualified_visits"]), 0.60, 0.30)
commercial_class, commercial_label = classify_band(metrics["commercial_ratio"], 0.20, 0.08)
store_health_text, store_health_badge = store_health_label(metrics["conversion_rate"], metrics["engaged_rate"], metrics["avg_dwell_seconds"])
media_label, media_badge = media_index_label(metrics["retail_media_index"])

ai_confidence, ai_confidence_band = compute_ai_confidence({
    "benchmark_population": metrics["benchmark_population"],
    "days_in_scope": days_in_scope,
    "base_volume": metrics["walk_by"] if active_app_mode == "Retail Media" else metrics["store_visits"],
    "traffic_intelligence_index": metrics["traffic_intelligence_index"],
    "visit_quality_index": metrics["visit_quality_index"],
    "store_attraction_index": metrics["store_attraction_index"],
    "audience_quality_index": metrics["audience_quality_index"],
})

ai_payload = {
    "scope": scope,
    "mode": active_app_mode,
    "walk_by": fmt_int(metrics["walk_by"]),
    "interest": fmt_int(metrics["interest"]),
    "near_store": fmt_int(metrics["near_store"]),
    "visits": fmt_int(metrics["store_visits"]),
    "qualified_visits": fmt_int(metrics["qualified_visits"]),
    "engaged_visits": fmt_int(metrics["engaged_visits"]),
    "qualified_rate": round(metrics["qualified_rate"] * 100, 1),
    "engaged_rate": round(metrics["engaged_rate"] * 100, 1),
    "avg_dwell": fmt_seconds(metrics["avg_dwell_seconds"]),
    "transactions": fmt_int(metrics["transactions"]),
    "value": fmt_currency(metrics["value"]),
    "commercial_ratio": round(metrics["commercial_ratio"] * 100, 1),
    "tii": round(metrics["traffic_intelligence_index"], 1),
    "vqi": round(metrics["visit_quality_index"], 1),
    "sai": round(metrics["store_attraction_index"], 1),
    "aqi": round(metrics["audience_quality_index"], 1),
    "rmi": round(metrics["retail_media_index"], 1),
    "attention_rate": round(metrics["attention_rate"] * 100, 1),
    "near_store_rate": round(metrics["storefront_engagement_rate"] * 100, 1),
    "value_per_1k_reach": fmt_currency(metrics["value_per_1k_walkby"]),
    "response_per_1k_reach": fmt_float(metrics["response_per_1k_walkby"], 1),
    "premium_mix_score": round(metrics["premium_device_mix_score"], 1),
    "smv_score": round(metrics["smv_score"], 1),
    "smv_tier": metrics["smv_tier"],
    "benchmark_population": int(metrics["benchmark_population"]),
    "ai_confidence": ai_confidence,
    "ai_confidence_band": ai_confidence_band,
    "primary_bottleneck": primary_bottleneck,
}

st.markdown(
    f"""
    <div class="summary-strip">
        <b>Store:</b> {loaded_store} &nbsp;&nbsp;•&nbsp;&nbsp;
        <b>Period:</b> {scope} &nbsp;&nbsp;•&nbsp;&nbsp;
        <b>View:</b> {active_app_mode} &nbsp;&nbsp;•&nbsp;&nbsp;
        <b>Days in scope:</b> {days_in_scope}
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("<div class='section-title'>Quick Summary</div>", unsafe_allow_html=True)
summary_cols = st.columns(4)

if active_app_mode == "Retail Media":
    with summary_cols[0]:
        st.metric("Safe Storefront Audience", fmt_int(metrics["walk_by"]), help="Business-safe top-of-funnel audience base for the selected period.")
    with summary_cols[1]:
        st.metric("Attention Captured", fmt_pct_from_ratio(metrics["attention_capture_rate"]), help="Share of safe storefront audience that became measurable storefront attention.")
    with summary_cols[2]:
        st.metric("Premium Audience Mix", fmt_pct_from_ratio(metrics["premium_audience_share_proxy"]), help="Proxy based on premium device mix score.")
    with summary_cols[3]:
        st.metric("SMV Score", f"{fmt_float(metrics['smv_score'], 1)} · {metrics['smv_tier']}", help="Partner-facing store media value score.")
else:
    with summary_cols[0]:
        st.metric("Safe Walk-by Traffic", fmt_int(metrics["walk_by"]), help="Business-safe top-of-funnel audience base for the selected period.")
    with summary_cols[1]:
        st.metric("Store Visits", fmt_int(metrics["store_visits"]), help="Visitors who stayed long enough to count as a meaningful store visit.")
    with summary_cols[2]:
        st.metric("Visit Conversion", fmt_pct_from_ratio(metrics["conversion_rate"]), help="Store visits ÷ safe walk-by traffic.")
    with summary_cols[3]:
        st.metric("Avg Dwell Time", fmt_minutes_simple(metrics["avg_dwell_seconds"]), help="Average time visitors spent in the store area.")

st.markdown(
    f"""<div class="simple-callout"><strong>In simple terms:</strong> {build_store_summary_sentence(metrics, active_app_mode)}</div>""",
    unsafe_allow_html=True,
)

health_cols = st.columns(4)
if active_app_mode == "Retail Media":
    with health_cols[0]:
        render_card(
            "Safe Storefront Audience",
            fmt_int(metrics["walk_by"]),
            "Business-safe top-of-funnel audience base for the selected period.",
            "Interim safe metric = max(avg far-zone live traffic, store visits).",
        )
    with health_cols[1]:
        render_card(
            "Attention Capture",
            fmt_pct_from_ratio(metrics["attention_capture_rate"]),
            "Share of storefront audience that moved into measurable interest near the storefront.",
            f"Formula: {fmt_int(metrics['interest'])} / {fmt_int(metrics['walk_by'])}",
        )
    with health_cols[2]:
        render_card(
            "Premium Audience Mix",
            fmt_pct_from_ratio(metrics["premium_audience_share_proxy"]),
            "Proxy based on premium device mix score across the nsTags benchmark.",
            f"Underlying premium mix score: {fmt_float(metrics['premium_device_mix_score'], 1)}",
        )
    with health_cols[3]:
        render_card(
            "SMV Score",
            f"{fmt_float(metrics['smv_score'], 1)} · {metrics['smv_tier']}",
            metrics["benchmark_directional_note"],
            "Store Media Value score for partner-facing comparison.",
        )
else:
    with health_cols[0]:
        render_card("Store Health", f"<span class='{store_health_badge}'>{store_health_text}</span>", "Simple health view based on visit conversion, engagement, and dwell time.", "This is a quick guide for store teams, not a strict benchmark score.")
    with health_cols[1]:
        render_card("Qualified Visit Rate", fmt_pct_from_ratio(metrics["qualified_rate"]), "Share of visits that became serious product visits.", f"Formula: {fmt_int(metrics['qualified_visits'])} / {fmt_int(metrics['store_visits'])}")
    with health_cols[2]:
        render_card("Engaged Visit Rate", fmt_pct_from_ratio(metrics["engaged_rate"]), "Share of visits that moved into deeper interaction.", f"Formula: {fmt_int(metrics['engaged_visits'])} / {fmt_int(metrics['store_visits'])}")
    with health_cols[3]:
        render_card("Audience Quality", audience_label(metrics["audience_quality_index"]), f"Current audience profile is {audience_label(metrics['audience_quality_index']).lower()}.", f"Underlying score: {metrics['audience_quality_index']:.0f}")

if active_app_mode == "Retail Media":
    st.markdown('<div class="section-title">Retail Media View</div>', unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class="alert-panel">
            <div class="alert-title">Partner Readout</div>
            <div class="alert-headline">{metrics['smv_tier']} Tier Store · SMV {fmt_float(metrics['smv_score'], 1)}</div>
            <div class="alert-body">
                This store should be evaluated as a storefront media asset first, not as a pure in-store operations case.
                The strongest partner signals here are safe audience scale, attention capture, premium audience mix, and
                comparative network standing. {metrics['benchmark_directional_note']}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<div class='section-title'>What the Team Should Know</div>", unsafe_allow_html=True)
priority_cols = st.columns(3)
for i, card in enumerate(top_insights):
    with priority_cols[i]:
        render_priority_card(card["label"], card["title"], card["body"], card["pills"])

st.markdown("<div class='section-title'>Business Reading</div>", unsafe_allow_html=True)
story_cols = st.columns(3)
with story_cols[0]:
    render_story_card("Traffic Reading", priority_narratives["traffic_title"], [f"Audience {fmt_int(metrics['walk_by'])}", f"Interest {fmt_int(metrics['interest'])}", f"Near-store {fmt_int(metrics['near_store'])}"], priority_narratives["traffic_body"])
with story_cols[1]:
    render_story_card("Experience Reading" if active_app_mode == "Retail Ops" else "Attention Reading", priority_narratives["visit_title"], [f"Attention {fmt_pct_from_ratio(metrics['attention_rate'])}", f"Near-store {fmt_pct_from_ratio(metrics['storefront_engagement_rate'])}", f"Best day {weekday_peak or 'N/A'}"], priority_narratives["visit_body"])
with story_cols[2]:
    render_story_card("Business Outcome", priority_narratives["commercial_title"], [f"Responses {fmt_int(metrics['transactions'])}", f"Value {fmt_currency(metrics['value'])}", f"Index {fmt_float(metrics['smv_score'] if active_app_mode == 'Retail Media' else metrics['commercial_ratio']*100, 1)}"], priority_narratives["commercial_body"])

st.markdown("<div class='section-title'>Advanced Summary</div>", unsafe_allow_html=True)
advanced_cols = st.columns(4)
with advanced_cols[0]:
    render_card("Traffic Health Score", f"{metrics['traffic_intelligence_index']:.0f}", f"<span class='{badge_tii}'>{label_tii}</span><br>Overall traffic quality score.", "Use as a relative guide across stores and dates.")
with advanced_cols[1]:
    render_card("Visit Quality Score", f"{metrics['visit_quality_index']:.0f}", f"<span class='{badge_vqi}'>{label_vqi}</span><br>How strong visits are after entry.", "Higher means more serious browsing and deeper interaction.")
with advanced_cols[2]:
    render_card("Storefront Pull Score", f"{metrics['store_attraction_index']:.0f}", f"<span class='{badge_sai}'>{label_sai}</span><br>How well the storefront converts traffic into stronger approach.", "Higher means stronger stopping power and entrance pull.")
with advanced_cols[3]:
    if active_app_mode == "Retail Media":
        render_card("Platform Rank Proxy", metrics["smv_tier"], f"<span class='{media_badge}'>{media_label}</span><br>{metrics['benchmark_directional_note']}", "Tiered partner-facing score, not an exact absolute rank number.")
    else:
        render_card("Benchmark Depth", maturity_label, f"<span class='{maturity_class}'>{maturity_label}</span><br>{maturity_text}", "Benchmark quality improves as more store-day data accumulates.")

with st.expander("Executive AI Brief", expanded=False):
    conf_cols = st.columns(2)
    with conf_cols[0]:
        render_card("AI Confidence Score", f"{ai_confidence}%", f"{ai_confidence_band} confidence in narrative stability.", "Built from benchmark maturity, days in scope, scope volume, and score consistency.")
    with conf_cols[1]:
        render_card("AI Narrative Scope", scope, f"Mode: {active_app_mode}", "The AI brief uses only the selected store and selected dashboard period.")
    if st.button("Generate Executive Intelligence Brief", type="primary", use_container_width=True, key="generate_ai_brief_button"):
        with st.spinner("Generating executive narrative..."):
            st.markdown(generate_ai_brief(ai_payload))
    else:
        st.info("Click to generate a management summary for the selected store and time period.")

if active_app_mode == "Retail Media":
    tab_overview, tab_trend, tab_behaviour, tab_audience, tab_deep = st.tabs(
        ["Media Value Funnel", "Campaign Reach & Timing", "Partner Value Proof", "Audience Quality", "Store Media Ranking"]
    )
else:
    tab_overview, tab_trend, tab_behaviour, tab_audience, tab_deep = st.tabs(
        ["Store Funnel", "Peak Hours & Trends", "Customer Engagement", "Audience Mix", "Deep Diagnostics"]
    )

with tab_overview:
    if active_app_mode == "Retail Media":
        st.markdown("<div class='panel'><b>Media Value Funnel</b><div class='note'>This view is designed for partner brands and shop owners. It focuses on storefront advertising value rather than deeper in-store browsing.</div></div>", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            media_funnel = go.Figure(go.Funnel(
                y=["Safe storefront audience", "Attention captured", "Near-store consideration", "Response events"],
                x=[metrics["walk_by"], metrics["interest"], metrics["near_store"], metrics["transactions"]],
                texttemplate="%{value:.0f}",
                textposition="inside",
                opacity=0.94,
                marker={"color": [COLORS["grey"], COLORS["orange"], COLORS["indigo"], COLORS["emerald"]]},
                connector={"line": {"color": "rgba(99,102,241,0.25)", "width": 1.2}},
            ))
            media_funnel.update_layout(title="Partner Brand Media Funnel", height=420, margin=dict(l=20, r=20, t=55, b=20), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(media_funnel, use_container_width=True, config=PLOT_CONFIG)
        with c2:
            proof_fig = go.Figure()
            proof_fig.add_trace(go.Bar(x=["Attention rate", "Near-store rate", "Resp / 1K audience"], y=[metrics["attention_rate"] * 100, metrics["storefront_engagement_rate"] * 100, metrics["response_per_1k_walkby"]], name="Storefront performance"))
            proof_fig.update_layout(title="Storefront Performance Proof")
            proof_fig.update_yaxes(title="")
            st.plotly_chart(style_chart(proof_fig), use_container_width=True, config=PLOT_CONFIG)

        p1, p2 = st.columns(2)
        with p1:
            render_info_card("What the partner brand should see", f"This store delivered a safe storefront audience base of <strong>{fmt_int(metrics['walk_by'])}</strong>, converted it into <strong>{fmt_int(metrics['interest'])}</strong> attention events, and moved <strong>{fmt_int(metrics['near_store'])}</strong> people into stronger consideration near the store.")
        with p2:
            render_info_card("What the shop owner should show", f"The campaign contributed <strong>{fmt_int(metrics['transactions'])}</strong> response events and <strong>{fmt_currency(metrics['value'])}</strong> of business value, equal to <strong>{fmt_currency(metrics['value_per_1k_walkby'])}</strong> per 1,000 safe storefront audience.")
    else:
        st.markdown("<div class='panel'><b>Store Funnel</b><div class='note'>This shows how people move from passing the store to noticing it, coming near it, entering it, and then becoming meaningful or deeply engaged visitors.</div></div>", unsafe_allow_html=True)
        funnel_cols = st.columns(2)
        with funnel_cols[0]:
            signal_fig = go.Figure(go.Funnel(y=["Passing nearby", "Looked at store", "Came near entrance"], x=[metrics["walk_by"], metrics["interest"], metrics["near_store"]], texttemplate="%{value:.0f}", textposition="inside", opacity=0.94, marker={"color": [COLORS["grey"], COLORS["orange"], COLORS["indigo"]]}, connector={"line": {"color": "rgba(99,102,241,0.25)", "width": 1.2}}))
            signal_fig.update_layout(title="Attention Funnel", height=380, margin=dict(l=20, r=20, t=55, b=20), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(signal_fig, use_container_width=True, config=PLOT_CONFIG)
        with funnel_cols[1]:
            visit_fig = go.Figure(go.Funnel(y=["Store visits", "Qualified visits", "Engaged visits", "Transactions / responses"], x=[metrics["store_visits"], metrics["qualified_visits"], metrics["engaged_visits"], metrics["transactions"]], texttemplate="%{value:.0f}", textposition="inside", opacity=0.94, marker={"color": [COLORS["sky"], COLORS["amber"], COLORS["violet"], COLORS["emerald"]]}, connector={"line": {"color": "rgba(99,102,241,0.25)", "width": 1.2}}))
            visit_fig.update_layout(title="Visit & Engagement Funnel", height=380, margin=dict(l=20, r=20, t=55, b=20), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(visit_fig, use_container_width=True, config=PLOT_CONFIG)

with tab_trend:
    if active_app_mode == "Retail Media":
        st.markdown("<div class='panel'><b>Campaign Reach & Timing</b><div class='note'>Use this section to show when the storefront ad had the strongest reach, attention, and partner value opportunity.</div></div>", unsafe_allow_html=True)
        if loaded_mode == "Daily":
            full_hours = pd.DataFrame({"hour_of_day": list(range(24)), "hour_label": [f"{h:02d}:00" for h in range(24)]})
            hourly_plot_df = full_hours.merge(hourly_df, on=["hour_of_day", "hour_label"], how="left")
            for col in ["avg_far_devices", "avg_mid_devices", "avg_near_devices", "avg_apple_devices", "avg_samsung_devices", "avg_other_devices"]:
                if col in hourly_plot_df.columns:
                    hourly_plot_df[col] = pd.to_numeric(hourly_plot_df[col], errors="coerce").fillna(0)
            t1, t2 = st.columns(2)
            with t1:
                fig_hourly = go.Figure()
                fig_hourly.add_trace(go.Scatter(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_far_devices"], mode="lines+markers", name="Walk-by reach"))
                fig_hourly.add_trace(go.Scatter(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_mid_devices"], mode="lines+markers", name="Attention"))
                fig_hourly.add_trace(go.Scatter(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_near_devices"], mode="lines+markers", name="Near-store"))
                fig_hourly.update_layout(title="Hourly Media Opportunity")
                st.plotly_chart(style_chart(fig_hourly), use_container_width=True, config=PLOT_CONFIG)
            with t2:
                peak_label = hourly_plot_df.assign(total_signal=hourly_plot_df["avg_mid_devices"] + hourly_plot_df["avg_near_devices"]).sort_values("total_signal", ascending=False).iloc[0]["hour_label"]
                render_card("Best campaign hour", peak_label, "Hour with the strongest combined attention + near-store signal.", "Use this hour for premium partner placements, live demos, or high-value creatives.")
                render_card("Attention rate", fmt_pct_from_ratio(metrics["attention_rate"]), "Share of safe storefront audience that became measurable attention.")
                render_card("Near-store rate", fmt_pct_from_ratio(metrics["storefront_engagement_rate"]), "Share of safe storefront audience that moved closer to the store.")
        else:
            t1, t2 = st.columns(2)
            with t1:
                fig_signal_trend = go.Figure()
                fig_signal_trend.add_trace(go.Bar(x=trend_df["period_label"], y=trend_df["walk_by_traffic"], name="Safe audience"))
                fig_signal_trend.add_trace(go.Bar(x=trend_df["period_label"], y=trend_df["store_interest"], name="Attention"))
                fig_signal_trend.add_trace(go.Bar(x=trend_df["period_label"], y=trend_df["near_store"], name="Near-store"))
                fig_signal_trend.update_layout(title="Audience, Attention, and Consideration Trend", barmode="group")
                st.plotly_chart(style_chart(fig_signal_trend), use_container_width=True, config=PLOT_CONFIG)
            with t2:
                fig_rate_trend = go.Figure()
                fig_rate_trend.add_trace(go.Scatter(x=trend_df["period_label"], y=trend_df["traffic_capture_ratio"] * 100, name="Attention rate", mode="lines+markers"))
                fig_rate_trend.add_trace(go.Scatter(x=trend_df["period_label"], y=trend_df["near_store_ratio"] * 100, name="Near-store rate", mode="lines+markers"))
                fig_rate_trend.update_layout(title="Media Efficiency Trend")
                fig_rate_trend.update_yaxes(ticksuffix="%")
                st.plotly_chart(style_chart(fig_rate_trend), use_container_width=True, config=PLOT_CONFIG)

            fig_weekday = go.Figure()
            fig_weekday.add_trace(go.Bar(x=weekday_df["weekday"], y=weekday_df["walk_by_traffic"], name="Safe audience"))
            fig_weekday.add_trace(go.Scatter(x=weekday_df["weekday"], y=weekday_df["attention_rate"] * 100, name="Attention rate", mode="lines+markers", yaxis="y2"))
            fig_weekday.update_layout(title="Best Days: Audience vs Attention Rate", yaxis=dict(title="Audience"), yaxis2=dict(title="Attention %", overlaying="y", side="right", showgrid=False, ticksuffix="%"))
            st.plotly_chart(style_chart(fig_weekday), use_container_width=True, config=PLOT_CONFIG)
    else:
        st.markdown("<div class='panel'><b>Peak Hours & Trends</b><div class='note'>Use this section to identify busy hours, strong traffic windows, and the days or periods where the store performs best.</div></div>", unsafe_allow_html=True)
        if loaded_mode == "Daily":
            full_hours = pd.DataFrame({"hour_of_day": list(range(24)), "hour_label": [f"{h:02d}:00" for h in range(24)]})
            hourly_plot_df = full_hours.merge(hourly_df, on=["hour_of_day", "hour_label"], how="left")
            for col in ["avg_far_devices", "avg_mid_devices", "avg_near_devices", "avg_apple_devices", "avg_samsung_devices", "avg_other_devices"]:
                hourly_plot_df[col] = pd.to_numeric(hourly_plot_df[col], errors="coerce").fillna(0)
            daily_cols = st.columns(2)
            with daily_cols[0]:
                fig_hourly = go.Figure()
                fig_hourly.add_trace(go.Scatter(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_far_devices"], mode="lines+markers", name="Passing nearby"))
                fig_hourly.add_trace(go.Scatter(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_mid_devices"], mode="lines+markers", name="Looked at store"))
                fig_hourly.add_trace(go.Scatter(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_near_devices"], mode="lines+markers", name="Near entrance"))
                fig_hourly.update_layout(title="Hourly Store Attention")
                st.plotly_chart(style_chart(fig_hourly), use_container_width=True, config=PLOT_CONFIG)
            with daily_cols[1]:
                fig_hourly_brand = go.Figure()
                fig_hourly_brand.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_apple_devices"], name="Apple"))
                fig_hourly_brand.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_samsung_devices"], name="Samsung"))
                fig_hourly_brand.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_other_devices"], name="Other"))
                fig_hourly_brand.update_layout(title="Hourly Audience Mix", barmode="stack")
                st.plotly_chart(style_chart(fig_hourly_brand), use_container_width=True, config=PLOT_CONFIG)
        else:
            trend_cols_1 = st.columns(2)
            with trend_cols_1[0]:
                fig_visit_trend = go.Figure()
                fig_visit_trend.add_trace(go.Scatter(x=trend_df["period_label"], y=trend_df["store_visits"], name="Visits", mode="lines+markers", line=dict(width=3)))
                fig_visit_trend.add_trace(go.Scatter(x=trend_df["period_label"], y=trend_df["qualified_footfall"], name="Qualified visits", mode="lines+markers"))
                fig_visit_trend.add_trace(go.Scatter(x=trend_df["period_label"], y=trend_df["engaged_visits"], name="Engaged visits", mode="lines+markers"))
                fig_visit_trend.update_layout(title="Visit Volume Trend")
                st.plotly_chart(style_chart(fig_visit_trend), use_container_width=True, config=PLOT_CONFIG)
            with trend_cols_1[1]:
                fig_rate_trend = go.Figure()
                fig_rate_trend.add_trace(go.Scatter(x=trend_df["period_label"], y=trend_df["conversion_rate"] * 100, name="Visit conversion", mode="lines+markers"))
                fig_rate_trend.add_trace(go.Scatter(x=trend_df["period_label"], y=trend_df["qualified_rate"] * 100, name="Qualified visit rate", mode="lines+markers"))
                fig_rate_trend.add_trace(go.Scatter(x=trend_df["period_label"], y=trend_df["engaged_rate"] * 100, name="Engaged visit rate", mode="lines+markers"))
                fig_rate_trend.update_layout(title="Quality & Conversion Trend")
                fig_rate_trend.update_yaxes(ticksuffix="%")
                st.plotly_chart(style_chart(fig_rate_trend), use_container_width=True, config=PLOT_CONFIG)

with tab_behaviour:
    if active_app_mode == "Retail Media":
        st.markdown("<div class='panel'><b>Partner Value Proof</b><div class='note'>Use these measures to prove why the location deserves partner ad budgets and how the campaign helped the store's business outcome.</div></div>", unsafe_allow_html=True)
        top = st.columns(4)
        with top[0]:
            render_card("Campaign value", fmt_currency(metrics["value"]), "Reported revenue / campaign value linked to the selected period.")
        with top[1]:
            render_card("Responses", fmt_int(metrics["transactions"]), "Partner campaign response events.")
        with top[2]:
            render_card("Value / attention", fmt_currency(metrics["value_per_attention"]), "Reported value for every measured attention event.")
        with top[3]:
            render_card("Value / near-store", fmt_currency(metrics["value_per_near_store"]), "Reported value for every stronger storefront consideration event.")
        proof_cols = st.columns(2)
        with proof_cols[0]:
            proof_df = pd.DataFrame({
                "Metric": ["Value / 1K audience", "Response / 1K audience", "Response / 100 attention", "Response / 100 near-store"],
                "Value": [metrics["value_per_1k_walkby"], metrics["response_per_1k_walkby"], metrics["response_per_100_attention"], metrics["response_per_100_near_store"]],
            })
            fig = px.bar(proof_df, x="Metric", y="Value", title="Store-to-Partner Commercial Proof")
            st.plotly_chart(style_chart(fig), use_container_width=True, config=PLOT_CONFIG)
        with proof_cols[1]:
            render_info_card("How to sell this to the partner brand", f"The store can say: this location created <strong>{fmt_int(metrics['interest'])}</strong> attention events, <strong>{fmt_int(metrics['near_store'])}</strong> high-consideration passes, and <strong>{fmt_currency(metrics['value'])}</strong> of value impact. That makes it a stronger ad location than stores that only show raw traffic.")
            render_info_card("How the partner helps the shop owner", f"The partner campaign did not just create visibility. It delivered a reported <strong>{fmt_currency(metrics['value_per_1k_walkby'])}</strong> of value per 1,000 safe storefront audience, helping the owner justify premium media pricing.")
    else:
        st.markdown("<div class='panel'><b>Customer Engagement</b><div class='note'>This section shows how long people stayed. It helps separate quick passersby from real product browsers and highly engaged shoppers.</div></div>", unsafe_allow_html=True)
        behaviour_top = st.columns(3)
        with behaviour_top[0]:
            render_card("Avg Dwell Time", fmt_seconds(metrics["avg_dwell_seconds"]), "Average time spent by visitors.")
        with behaviour_top[1]:
            render_card("Qualified Visit Rate", fmt_pct_from_ratio(metrics["qualified_rate"]), "Visitors who stayed long enough to count as serious visits.")
        with behaviour_top[2]:
            render_card("Engaged Visit Rate", fmt_pct_from_ratio(metrics["engaged_rate"]), "Visitors who moved into deeper interaction.")
        if not dwell_plot_df.empty:
            beh_cols = st.columns(2)
            with beh_cols[0]:
                fig_dwell = px.bar(dwell_plot_df, x="display_label", y="visits", title="Customer Engagement by Volume")
                fig_dwell.update_xaxes(title="")
                st.plotly_chart(style_chart(fig_dwell), use_container_width=True, config=PLOT_CONFIG)
            with beh_cols[1]:
                share_df = dwell_plot_df.copy()
                share_df["share_pct"] = share_df["share"] * 100
                fig_dwell_share = px.bar(share_df, x="display_label", y="share_pct", title="Engagement Mix Share")
                fig_dwell_share.update_yaxes(ticksuffix="%")
                fig_dwell_share.update_xaxes(title="")
                st.plotly_chart(style_chart(fig_dwell_share), use_container_width=True, config=PLOT_CONFIG)

with tab_audience:
    if active_app_mode == "Retail Media":
        st.markdown("<div class='panel'><b>Audience Quality</b><div class='note'>This section helps explain whether the storefront is attracting a stronger audience mix and when that audience is most visible. This mix represents concurrent device presence, not unique people share.</div></div>", unsafe_allow_html=True)
    else:
        st.markdown("<div class='panel'><b>Audience Mix</b><div class='note'>This section shows the overall audience profile and when that audience is strongest during the day.</div></div>", unsafe_allow_html=True)

    audience_top = st.columns(3)
    with audience_top[0]:
        render_card("Average Audience Level", fmt_float(metrics["avg_estimated_people"], 1), "Average nearby audience level in the selected period.")
    with audience_top[1]:
        render_card("Peak Audience Level", fmt_int(metrics["peak_estimated_people"]), "Highest audience level seen in the selected period.")
    with audience_top[2]:
        render_card("Average Detected Devices", fmt_float(metrics["avg_detected_devices"], 1), "Average detected devices across the selected period.")

    if not hourly_df.empty:
        aud_cols = st.columns(2)
        with aud_cols[0]:
            if not brand_mix_df.empty and brand_mix_df["value"].sum() > 0:
                fig_brand_mix = px.pie(brand_mix_df, values="value", names="brand", title="Concurrent Device Presence Mix", hole=0.55)
                fig_brand_mix.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", legend=dict(orientation="h", y=-0.1))
                st.plotly_chart(fig_brand_mix, use_container_width=True, config=PLOT_CONFIG)
        with aud_cols[1]:
            hourly_plot_df = hourly_df.copy()
            for col in ["avg_apple_devices", "avg_samsung_devices", "avg_other_devices"]:
                hourly_plot_df[col] = pd.to_numeric(hourly_plot_df[col], errors="coerce").fillna(0)
            brand_fig = go.Figure()
            brand_fig.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_apple_devices"], name="Apple"))
            brand_fig.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_samsung_devices"], name="Samsung"))
            brand_fig.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_other_devices"], name="Other"))
            brand_fig.update_layout(title="Hourly Concurrent Presence Mix", barmode="stack")
            st.plotly_chart(style_chart(brand_fig), use_container_width=True, config=PLOT_CONFIG)

with tab_deep:
    if active_app_mode == "Retail Media":
        st.markdown("<div class='panel'><b>Store Media Ranking</b><div class='note'>This section creates a partner-facing store media value view. It combines storefront reach, stopping power, premium audience mix, reporting confidence, and freshness.</div></div>", unsafe_allow_html=True)
        index_breakdown_df = pd.DataFrame(
            {
                "Metric": [
                    "Walk-by Score",
                    "Store Magnet Score",
                    "Premium Mix Score",
                    "Reporting Confidence",
                    "Freshness Proxy",
                    "SMV Score",
                ],
                "Score": [
                    metrics["walk_by_score"],
                    metrics["store_magnet_percentile_score"],
                    metrics["premium_device_mix_score"],
                    compute_reporting_confidence(days_in_scope, metrics["benchmark_population"]),
                    100 if days_in_scope == 1 else 92 if days_in_scope <= 7 else 84 if days_in_scope <= 30 else 72,
                    metrics["smv_score"],
                ],
            }
        ).sort_values("Score", ascending=True)

        c1, c2 = st.columns(2)
        with c1:
            fig_index = px.bar(index_breakdown_df, x="Score", y="Metric", orientation="h", title="SMV Ranking Inputs")
            fig_index.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(style_chart(fig_index), use_container_width=True, config=PLOT_CONFIG)
        with c2:
            render_card("SMV Score", fmt_float(metrics["smv_score"], 1), f"<span class='{media_badge}'>{metrics['smv_tier']}</span><br>{metrics['benchmark_directional_note']}", "Partner-facing score for comparing stores across the nsTags platform.")
            render_card("Partner fit", metrics["retail_media_positioning"], "How the store should be positioned to partner brands.", "Higher-scoring stores deserve premium ad rates and stronger partner allocation.")
        st.dataframe(index_breakdown_df.sort_values("Score", ascending=False), use_container_width=True, hide_index=True)
    else:
        st.markdown("<div class='panel'><b>Deep Diagnostics</b><div class='note'>This section keeps the advanced score system for analysts and leadership. It is useful for comparing stores, dates, and quality signals.</div></div>", unsafe_allow_html=True)
        index_breakdown_df = pd.DataFrame(
            {
                "Metric": [
                    "Traffic Health Score",
                    "Visit Quality Score",
                    "Storefront Pull Score",
                    "Audience Quality Score",
                    "Store Magnet Score",
                    "Window Capture Score",
                    "Entry Efficiency Score",
                    "Dwell Quality Score",
                    "Floor Conversion Proxy",
                ],
                "Score": [
                    metrics["traffic_intelligence_index"],
                    metrics["visit_quality_index"],
                    metrics["store_attraction_index"],
                    metrics["audience_quality_index"],
                    metrics["store_magnet_percentile_score"],
                    metrics["window_capture_score"],
                    metrics["entry_efficiency_percentile_score"],
                    metrics["dwell_quality_score"],
                    floor_conversion_score,
                ],
            }
        ).sort_values("Score", ascending=True)
        deep_top = st.columns(2)
        with deep_top[0]:
            fig_index = px.bar(index_breakdown_df, x="Score", y="Metric", orientation="h", title="Score Breakdown")
            fig_index.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(style_chart(fig_index), use_container_width=True, config=PLOT_CONFIG)
        with deep_top[1]:
            radar_df = pd.DataFrame({"Metric": ["Traffic", "Visit Quality", "Storefront Pull", "Audience"], "Score": [metrics["traffic_intelligence_index"], metrics["visit_quality_index"], metrics["store_attraction_index"], metrics["audience_quality_index"]]})
            fig_radar = go.Figure()
            fig_radar.add_trace(go.Scatterpolar(r=radar_df["Score"], theta=radar_df["Metric"], fill="toself", name="Core Strategic Scores"))
            fig_radar.update_layout(title="Strategic Score Shape", polar=dict(radialaxis=dict(visible=True, range=[0, 100])), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=20, r=20, t=55, b=20))
            st.plotly_chart(fig_radar, use_container_width=True, config=PLOT_CONFIG)
        st.dataframe(index_breakdown_df.sort_values("Score", ascending=False), use_container_width=True, hide_index=True)
        if not weekday_df.empty:
            st.markdown("<div class='section-title'>Weekday Diagnostic Table</div>", unsafe_allow_html=True)
            st.dataframe(normalize_weekday_table(weekday_df), use_container_width=True, hide_index=True)

if loaded_filters.get("show_debug", False) and not debug_df.empty:
    st.markdown("### Timezone Diagnostics")
    st.dataframe(debug_df, use_container_width=True)

st.caption("nsTags Retail Intelligence · Store-ready dashboard · Powered by AWS Athena · Streamlit")
