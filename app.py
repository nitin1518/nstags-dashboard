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


# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="Retail Intelligence Command Center",
    page_icon="📈",
    layout="wide",
)

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

.note {
    color: var(--text-3);
    font-size: .88rem;
    line-height: 1.6;
}

.kpi-card {
    background: var(--card-grad);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 1rem 1rem .92rem 1rem;
    box-shadow: var(--shadow-soft);
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
    background: var(--card-grad);
    border: 1px solid var(--border);
    border-left: 4px solid var(--accent);
    border-radius: 18px;
    padding: 1rem 1rem .95rem 1rem;
    box-shadow: var(--shadow-soft);
    min-height: 180px;
    margin-bottom: .9rem;
}

.priority-label {
    font-size: .68rem;
    text-transform: uppercase;
    letter-spacing: .12em;
    color: var(--accent);
    font-weight: 800;
    margin-bottom: .35rem;
}

.priority-title {
    font-size: 1.08rem;
    font-weight: 800;
    color: var(--text);
    margin-bottom: .45rem;
    line-height: 1.25;
}

.priority-body {
    color: var(--text-3);
    font-size: .88rem;
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

.story-card {
    background: var(--card-grad);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 1rem;
    box-shadow: var(--shadow-soft);
    min-height: 200px;
    margin-bottom: .9rem;
}

.story-label {
    font-size: .7rem;
    text-transform: uppercase;
    letter-spacing: .11em;
    font-weight: 800;
    color: var(--accent);
    margin-bottom: .45rem;
}

.story-title {
    font-size: 1.08rem;
    font-weight: 800;
    color: var(--text);
    line-height: 1.25;
    margin-bottom: .5rem;
}

.story-body {
    color: var(--text-3);
    font-size: .88rem;
    line-height: 1.62;
}

.alert-panel {
    background:
        radial-gradient(circle at top right, rgba(245,158,11,0.08), transparent 35%),
        var(--card-grad);
    border: 1px solid rgba(245,158,11,0.28);
    border-left: 4px solid var(--warn);
    border-radius: 18px;
    padding: 1rem 1rem .95rem 1rem;
    box-shadow: var(--shadow-soft);
    margin-bottom: 1rem;
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

.summary-strip {
    background: var(--card-grad);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: .9rem 1rem;
    box-shadow: var(--shadow-soft);
    margin-bottom: 1rem;
    color: var(--text-3);
    font-size: .9rem;
    line-height: 1.55;
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
    overflow-y: hidden !important;
    scrollbar-width: thin;
    -webkit-overflow-scrolling: touch;
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

div[data-testid="stExpander"] {
    border-radius: 16px !important;
    overflow: hidden;
    border: 1px solid var(--border) !important;
    box-shadow: var(--shadow-soft);
}

div[data-testid="stExpander"] details {
    background: var(--card-grad) !important;
}

div[data-testid="stAlert"] {
    border-radius: 14px !important;
}

[data-testid="stDataFrame"] {
    border: 1px solid var(--border);
    border-radius: 16px;
    overflow: hidden;
}

@media (max-width: 900px) {
    .hero h1 { font-size: 1.7rem; }
}

@media (max-width: 768px) {
    .main .block-container {
        padding: .9rem .72rem 1.6rem .72rem !important;
    }
    .hero {
        padding: 1.05rem .95rem .95rem .95rem;
        border-radius: 20px;
    }
    .hero h1 { font-size: 1.5rem; }
    .hero p { font-size: 0.91rem; }
    .panel { padding: .9rem .9rem .75rem .9rem !important; }
    .kpi-card, .story-card, .priority-card { min-height: auto; }
}
</style>
""",
    unsafe_allow_html=True,
)

# =========================================================
# CONSTANTS
# =========================================================
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

# =========================================================
# CONFIG
# =========================================================
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

# =========================================================
# SESSION STATE
# =========================================================
if "loaded_bundle" not in st.session_state:
    st.session_state.loaded_bundle = None

if "last_loaded_filters" not in st.session_state:
    st.session_state.last_loaded_filters = None


# =========================================================
# AWS CLIENTS
# =========================================================
@st.cache_resource
def get_aws_clients():
    aws_session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    return aws_session.client("athena"), aws_session.client("s3")


athena_client, s3_client = get_aws_clients()

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


def safe_div(a, b):
    try:
        a = float(a)
        b = float(b)
        if b == 0:
            return 0.0
        return a / b
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
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.16,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(0,0,0,0)",
        ),
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
            <div class="story-body">
                {pills_html}
                <br><br>
                {body}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_priority_card(label: str, title: str, body: str, pills: list[str] | None = None):
    pills_html = ""
    if pills:
        pills_html = "".join([f"<span class='metric-pill'>{p}</span>" for p in pills])
    st.markdown(
        f"""
        <div class="priority-card">
            <div class="priority-label">{label}</div>
            <div class="priority-title">{title}</div>
            <div class="priority-body">
                {pills_html}
                {("<br><br>" if pills_html else "")}
                {body}
            </div>
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
            walk_by_traffic=("walk_by_traffic", "mean"),
            store_interest=("store_interest", "mean"),
            near_store=("near_store", "mean"),
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
            walk_by_traffic=("walk_by_traffic", "mean"),
            store_interest=("store_interest", "mean"),
            near_store=("near_store", "mean"),
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
    visits = float(metrics.get("store_visits", 0) or 0)
    tii = float(metrics.get("traffic_intelligence_index", 0) or 0)
    vqi = float(metrics.get("visit_quality_index", 0) or 0)
    sai = float(metrics.get("store_attraction_index", 0) or 0)
    aqi = float(metrics.get("audience_quality_index", 0) or 0)

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

    if visits >= 1000:
        base += 12
    elif visits >= 300:
        base += 8
    elif visits >= 100:
        base += 5
    elif visits > 0:
        base += 2

    score_spread = max(tii, vqi, sai, aqi) - min(tii, vqi, sai, aqi)
    if score_spread <= 15:
        base += 8
    elif score_spread <= 30:
        base += 5
    else:
        base += 2

    confidence = max(40, min(96, int(round(base))))

    if confidence >= 85:
        band = "High"
    elif confidence >= 70:
        band = "Moderate"
    else:
        band = "Directional"

    return confidence, band


def get_priority_narratives(mode, primary_bottleneck, traffic_capture_ratio, engagement_depth_ratio, commercial_ratio, avg_dwell_seconds):
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
            "The first question for store owners is whether the store is converting nearby traffic into serious attention. "
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
        "Store Magnet": f"{traffic_title} Current capture is weak, so the biggest opportunity is improving storefront noticeability and stopping power.",
        "Window Capture": f"{traffic_title} Attention exists, but it is not translating strongly into closer approach or entry.",
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


def build_top_insights(mode, walk_by, interest, near_store, store_visits, qualified_visits, engaged_visits, qualified_rate, engaged_rate,
                       commercial_ratio, avg_dwell_seconds, primary_bottleneck, weekday_peak):
    insights = []

    if mode == "Retail Media":
        insights.append({
            "label": "Top Priority",
            "title": "Monetizable attention quality",
            "body": (
                f"The strongest commercial question here is whether the location is producing usable attention, not just foot presence. "
                f"The current audience path shows walk-by at {fmt_float(walk_by)} and interest at {fmt_float(interest)}, which means "
                f"attention capture remains a key filter before media value scales."
            ),
            "pills": [f"Walk-by {fmt_float(walk_by)}", f"Interest {fmt_float(interest)}", f"Near-store {fmt_float(near_store)}"],
        })
        insights.append({
            "label": "Engagement Signal",
            "title": "Audience depth after entry",
            "body": (
                f"{fmt_int(qualified_visits)} qualified visits and {fmt_int(engaged_visits)} engaged visits indicate whether the audience "
                f"is simply exposed or meaningfully interacting. Average dwell at {fmt_seconds(avg_dwell_seconds)} suggests the quality of that exposure."
            ),
            "pills": [f"Qualified {fmt_pct_from_ratio(qualified_rate)}", f"Engaged {fmt_pct_from_ratio(engaged_rate)}"],
        })
        insights.append({
            "label": "Commercial Read",
            "title": "Response intensity for partners",
            "body": (
                f"The current transactions / response outcome should be read as period-level commercial response intensity, not a strict "
                f"one-person-one-sale conversion. Peak weekday pattern currently leans toward {weekday_peak or 'unavailable'}."
            ),
            "pills": [f"Commercial ratio {fmt_pct_from_ratio(commercial_ratio)}"],
        })
    else:
        insights.append({
            "label": "Top Priority",
            "title": "Biggest operational bottleneck",
            "body": (
                f"The biggest current improvement area is {primary_bottleneck}. That is the business layer creating the largest drag "
                f"between surrounding traffic and final store outcome."
            ),
            "pills": [primary_bottleneck],
        })
        insights.append({
            "label": "Visit Quality",
            "title": "Meaningful visits vs shallow visits",
            "body": (
                f"The store generated {fmt_int(store_visits)} visits, of which {fmt_int(qualified_visits)} were meaningful and "
                f"{fmt_int(engaged_visits)} were deeper interactions. Average dwell of {fmt_seconds(avg_dwell_seconds)} helps explain whether "
                f"people are browsing seriously or leaving too quickly."
            ),
            "pills": [f"Qualified {fmt_pct_from_ratio(qualified_rate)}", f"Engaged {fmt_pct_from_ratio(engaged_rate)}"],
        })
        insights.append({
            "label": "Trading Read",
            "title": "Commercial outcome in context",
            "body": (
                f"Commercial response should be read with visit quality, not in isolation. The strongest operating days currently lean toward "
                f"{weekday_peak or 'unavailable'}, which is useful for staffing, display planning, and offer timing."
            ),
            "pills": [f"Commercial ratio {fmt_pct_from_ratio(commercial_ratio)}"],
        })

    return insights


def normalize_weekday_table(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return out
    for c in ["store_visits", "qualified_footfall", "engaged_visits"]:
        if c in out.columns:
            out[c] = out[c].round(0).astype(int)
    if "avg_dwell_seconds" in out.columns:
        out["avg_dwell_seconds"] = out["avg_dwell_seconds"].round(1)
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


def commercial_ratio_label(mode: str) -> str:
    return "Commercial Response Ratio" if mode == "Retail Media" else "Transactions / Visit Ratio"


def commercial_ratio_description(mode: str) -> str:
    if mode == "Retail Media":
        return "Business response generated against the selected period's in-store demand. Read as response intensity, not a literal human conversion rate."
    return "Business outcome relative to store visits. Read with context because transaction or response counts can exceed visit counts in some setups."


# =========================================================
# AI BRIEF
# =========================================================
@st.cache_data(ttl=AI_CACHE_TTL)
def generate_ai_brief(ai_payload: dict) -> str:
    if not GEMINI_API_KEY or genai is None:
        return "⚠️ AI unavailable: GEMINI_API_KEY not configured."

    try:
        client = genai.Client(api_key=GEMINI_API_KEY)

        prompt = f"""
You are a senior retail strategy consultant writing for business leaders.

Your job is to interpret store performance clearly, commercially, and without technical language.
Do not mention sensors, device scanning, tracking logic, BLE, pipelines, Athena, or any backend system.

BUSINESS CONTEXT
Mode: {ai_payload['mode']}
Scope: {ai_payload['scope']}
AI Confidence Score: {ai_payload['ai_confidence']}% ({ai_payload['ai_confidence_band']})

PERFORMANCE METRICS
Walk-by traffic: {ai_payload['walk_by']}
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

Traffic Intelligence Index: {ai_payload['tii']}
Visit Quality Index: {ai_payload['vqi']}
Store Attraction Index: {ai_payload['sai']}
Audience Quality Index: {ai_payload['aqi']}

PRIMARY OPPORTUNITY
{ai_payload['primary_bottleneck']}

INTERPRETATION RULES
1. Start from what matters most to a business owner or media partner.
2. Prioritize the biggest business constraint first.
3. If Mode is "Retail Ops", focus on storefront pull, visit quality, and commercial closure.
4. If Mode is "Retail Media", focus on audience attention, engagement quality, and commercial response.
5. Do not call the commercial ratio a strict conversion rate. It may exceed 100% and should be described carefully.
6. Keep the writing crisp, executive-friendly, and insight-led.
7. Avoid repeating raw numbers too often. Use them selectively to support conclusions.
8. Be decisive, but do not invent facts.

Return exactly in this format:

**Executive Summary**
[2-3 sentences summarizing the biggest takeaway]

**Top Priority**
[1 short paragraph on the most important issue or opportunity]

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
            contents=prompt
        )

        return response.text

    except Exception as e:
        return f"⚠️ AI unavailable: {str(e)}"


# =========================================================
# ATHENA
# =========================================================
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


# =========================================================
# LOADERS
# =========================================================
@st.cache_data(ttl=META_CACHE_TTL)
def load_store_list() -> pd.DataFrame:
    return run_athena_query(
        "SELECT DISTINCT store_id FROM nstags_available_dates_curated_v2 ORDER BY store_id"
    )


@st.cache_data(ttl=META_CACHE_TTL)
def load_available_dates(store_id: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(
        f"""
        SELECT metric_date
        FROM nstags_available_dates_curated_v2
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
        FROM nstags_dashboard_daily_curated_v2
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
        FROM nstags_hourly_traffic_curated_v2
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
        SELECT
            dwell_bucket,
            ROUND(SUM(visits), 2) AS visits
        FROM nstags_dwell_buckets_curated_v2
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


# =========================================================
# HEADER
# =========================================================
st.markdown(
    """
    <div class="hero">
        <div class="eyebrow">Retail Intelligence Command Center</div>
        <h1>Retail Intelligence Command Center</h1>
        <p>
            A business-first dashboard that explains what happened, where the biggest opportunity sits,
            and how to interpret traffic, visit quality, audience, and commercial outcome with clarity.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# SIDEBAR
# =========================================================
with st.sidebar:
    st.markdown("### Configuration")
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
    available_dates_default_store = sorted(set(dates_df["metric_date"].dropna().tolist()))

    st.markdown("### Filters")

    with st.form("dashboard_filters_form", clear_on_submit=False):
        selected_store = st.selectbox(
            "Active Store",
            options=store_options,
            index=store_options.index(default_store),
        )

        try:
            selected_store_dates_df = load_available_dates(selected_store)
        except Exception as e:
            st.error(f"Failed to load available dates for selected store: {e}")
            st.stop()

        if selected_store_dates_df.empty:
            st.warning("No dates found for this selected store.")
            st.stop()

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

        st.markdown("### Period Selection")
        period_mode = st.radio(
            "Analysis Window",
            ["Daily", "Weekly", "Monthly", "Yearly", "Custom"],
            index=["Daily", "Weekly", "Monthly", "Yearly", "Custom"].index(last_period_mode),
        )

        if period_mode == "Daily":
            default_daily = parsed_last_end if parsed_last_end in available_dates else max_available_date
            rev_dates = list(reversed(available_dates))
            selected_day = st.selectbox(
                "Select Date",
                options=rev_dates,
                index=rev_dates.index(default_daily),
                format_func=lambda d: d.strftime("%Y/%m/%d"),
            )
            start_date = selected_day
            end_date = selected_day

        elif period_mode == "Weekly":
            week_end_default = parsed_last_end if parsed_last_end in available_dates else max_available_date
            end_date = st.date_input(
                "Week End Date",
                value=week_end_default,
                min_value=min_available_date,
                max_value=max_available_date,
            )
            start_date = max(min_available_date, end_date - timedelta(days=6))

        elif period_mode == "Monthly":
            month_end_default = parsed_last_end if parsed_last_end in available_dates else max_available_date
            end_date = st.date_input(
                "Month End Date",
                value=month_end_default,
                min_value=min_available_date,
                max_value=max_available_date,
            )
            start_date = max(min_available_date, end_date - timedelta(days=29))

        elif period_mode == "Yearly":
            year_end_default = parsed_last_end if parsed_last_end in available_dates else max_available_date
            end_date = st.date_input(
                "Year End Date",
                value=year_end_default,
                min_value=min_available_date,
                max_value=max_available_date,
            )
            start_date = max(min_available_date, end_date - timedelta(days=364))

        else:
            default_start = parsed_last_start
            default_end = parsed_last_end
            selected_range = st.date_input(
                "Custom Date Range",
                value=(default_start, default_end),
                min_value=min_available_date,
                max_value=max_available_date,
            )
            if isinstance(selected_range, tuple) and len(selected_range) == 2:
                start_date, end_date = selected_range
            elif isinstance(selected_range, list) and len(selected_range) == 2:
                start_date, end_date = selected_range[0], selected_range[1]
            else:
                start_date, end_date = default_start, default_end

        st.markdown("### Commercial Inputs")
        transactions = st.number_input("Transactions / Response Events", min_value=0, value=last_transactions, step=1)
        value = st.number_input("Revenue / Campaign Value", min_value=0, value=last_value, step=1000)
        show_debug = st.checkbox("Show timezone diagnostics", value=last_show_debug)

        submitted = st.form_submit_button("Refresh Dashboard Data", type="primary", use_container_width=True)

# =========================================================
# DATA LOAD CONTROL
# =========================================================
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

if first_load:
    try:
        with st.spinner("Loading dashboard data from Athena..."):
            st.session_state.loaded_bundle = load_dashboard_bundle(
                selected_store,
                start_date_str,
                end_date_str,
                show_debug,
            )
            st.session_state.last_loaded_filters = requested_filters
    except Exception as e:
        st.error(f"Failed to load dashboard data: {e}")
        st.stop()

elif submitted:
    try:
        with st.spinner("Refreshing dashboard data from Athena..."):
            st.session_state.loaded_bundle = load_dashboard_bundle(
                selected_store,
                start_date_str,
                end_date_str,
                show_debug,
            )
            st.session_state.last_loaded_filters = requested_filters
    except Exception as e:
        st.error(f"Failed to refresh dashboard data: {e}")
        st.stop()

bundle = st.session_state.loaded_bundle
if bundle is None:
    st.error("No dashboard data is loaded.")
    st.stop()

loaded_filters = st.session_state.last_loaded_filters or {}
loaded_store = loaded_filters.get("selected_store")
loaded_start = loaded_filters.get("start_date_str")
loaded_end = loaded_filters.get("end_date_str")
loaded_mode = loaded_filters.get("period_mode")
loaded_debug = loaded_filters.get("show_debug")
active_app_mode = loaded_filters.get("app_mode", app_mode)
active_transactions = int(loaded_filters.get("transactions", transactions))
active_value = int(loaded_filters.get("value", value))

current_filters_differ = (
    loaded_store != selected_store
    or loaded_start != start_date_str
    or loaded_end != end_date_str
    or loaded_mode != period_mode
    or loaded_debug != show_debug
    or active_app_mode != app_mode
    or active_transactions != int(transactions)
    or active_value != int(value)
)

if current_filters_differ and not submitted:
    st.info(
        f"Showing loaded dashboard for **{loaded_store}** · **{loaded_mode}** · "
        f"**{loaded_start} → {loaded_end}**. "
        f"Click **Refresh Dashboard Data** in the sidebar to apply the current filter changes."
    )

daily_df = bundle["daily_df"].copy()
hourly_df = bundle["hourly_df"].copy()
dwell_df = bundle["dwell_df"].copy()
debug_df = bundle["debug_df"].copy()

if daily_df.empty:
    st.warning("No dashboard metrics were found for the selected period.")
    if loaded_filters.get("show_debug", False) and not debug_df.empty:
        st.dataframe(debug_df, use_container_width=True)
    st.stop()

# =========================================================
# PREP
# =========================================================
for col in daily_df.columns:
    if col != "metric_date" and col in daily_df.columns:
        daily_df[col] = pd.to_numeric(daily_df[col], errors="coerce").fillna(0)

daily_df["metric_date"] = pd.to_datetime(daily_df["metric_date"]).dt.date

score_row = daily_df.iloc[-1].to_dict() if not daily_df.empty else {}

walk_by = daily_df["walk_by_traffic"].mean()
interest = daily_df["store_interest"].mean()
near_store = daily_df["near_store"].mean()
store_visits = daily_df["store_visits"].sum()
qualified_visits = daily_df["qualified_footfall"].sum()
engaged_visits = daily_df["engaged_visits"].sum()
avg_dwell_seconds = daily_df["avg_dwell_seconds"].mean()
avg_estimated_people = daily_df["avg_estimated_people"].mean() if "avg_estimated_people" in daily_df.columns else 0
avg_detected_devices = daily_df["avg_detected_devices"].mean() if "avg_detected_devices" in daily_df.columns else 0

qualified_rate = safe_div(qualified_visits, store_visits)
engaged_rate = safe_div(engaged_visits, store_visits)
commercial_ratio = safe_div(active_transactions, store_visits)

traffic_intelligence_index = float(score_row.get("traffic_intelligence_index", 0) or 0)
visit_quality_index = float(score_row.get("visit_quality_index", 0) or 0)
store_attraction_index = float(score_row.get("store_attraction_index", 0) or 0)
audience_quality_index = float(score_row.get("audience_quality_index", 0) or 0)

store_magnet_percentile_score = float(score_row.get("store_magnet_percentile_score", 0) or 0)
window_capture_score = float(score_row.get("window_capture_score", 0) or 0)
entry_efficiency_percentile_score = float(score_row.get("entry_efficiency_percentile_score", 0) or 0)
dwell_quality_score = float(score_row.get("dwell_quality_score", 0) or 0)
floor_conversion_score = min(commercial_ratio * 400, 100)
benchmark_population = int(score_row.get("benchmark_population", 0) or 0)

peak_estimated_people = daily_df["peak_estimated_people"].max() if "peak_estimated_people" in daily_df.columns else 0
peak_detected_devices = daily_df["peak_detected_devices"].max() if "peak_detected_devices" in daily_df.columns else 0

bottlenecks = {
    "Store Magnet": store_magnet_percentile_score,
    "Window Capture": window_capture_score,
    "Entry Efficiency": entry_efficiency_percentile_score,
    "Dwell Quality": dwell_quality_score,
    "Floor Conversion": floor_conversion_score,
}
primary_bottleneck = min(bottlenecks, key=bottlenecks.get)

loaded_start_date = pd.to_datetime(loaded_start).date() if loaded_start else start_date
loaded_end_date = pd.to_datetime(loaded_end).date() if loaded_end else end_date

trend_grain = infer_trend_grain(loaded_start_date, loaded_end_date)
scope = scope_title(loaded_mode, loaded_start_date, loaded_end_date)
trend_df = build_period_trend(daily_df, trend_grain)
weekday_df = build_weekday_trend(daily_df)
dwell_plot_df = prepare_dwell_plot_df(dwell_df)

badge_tii, label_tii = score_band(traffic_intelligence_index)
badge_vqi, label_vqi = score_band(visit_quality_index)
badge_sai, label_sai = score_band(store_attraction_index)
badge_aqi, label_aqi = score_band(audience_quality_index)
maturity_label, maturity_class, maturity_text = benchmark_maturity_label(benchmark_population)

traffic_capture_ratio = safe_div(interest, walk_by)
engagement_depth_ratio = safe_div(engaged_visits, qualified_visits)

weekday_peak = None
if not weekday_df.empty:
    weekday_peak = str(weekday_df.sort_values("store_visits", ascending=False).iloc[0]["weekday"])

traffic_capture_class, traffic_capture_label = classify_band(traffic_capture_ratio, 0.45, 0.25)
engagement_depth_class, engagement_depth_label = classify_band(engagement_depth_ratio, 0.60, 0.30)
commercial_class, commercial_label = classify_band(commercial_ratio, 0.20, 0.08)

days_in_scope = (loaded_end_date - loaded_start_date).days + 1

ai_confidence, ai_confidence_band = compute_ai_confidence({
    "benchmark_population": benchmark_population,
    "days_in_scope": days_in_scope,
    "store_visits": store_visits,
    "traffic_intelligence_index": traffic_intelligence_index,
    "visit_quality_index": visit_quality_index,
    "store_attraction_index": store_attraction_index,
    "audience_quality_index": audience_quality_index,
})

priority_narratives = get_priority_narratives(
    active_app_mode,
    primary_bottleneck,
    traffic_capture_ratio,
    engagement_depth_ratio,
    commercial_ratio,
    avg_dwell_seconds,
)

top_insights = build_top_insights(
    active_app_mode,
    walk_by,
    interest,
    near_store,
    store_visits,
    qualified_visits,
    engaged_visits,
    qualified_rate,
    engaged_rate,
    commercial_ratio,
    avg_dwell_seconds,
    primary_bottleneck,
    weekday_peak,
)

brand_mix_df = compute_overall_brand_mix(hourly_df)

# =========================================================
# AI BRIEF
# =========================================================
ai_payload = {
    "scope": scope,
    "mode": active_app_mode,
    "walk_by": round(walk_by, 2),
    "interest": round(interest, 2),
    "near_store": round(near_store, 2),
    "visits": int(round(store_visits)),
    "qualified_visits": int(round(qualified_visits)),
    "engaged_visits": int(round(engaged_visits)),
    "qualified_rate": round(qualified_rate * 100, 1),
    "engaged_rate": round(engaged_rate * 100, 1),
    "avg_dwell": fmt_seconds(avg_dwell_seconds),
    "transactions": int(active_transactions),
    "value": fmt_currency(active_value),
    "commercial_ratio": round(commercial_ratio * 100, 1),
    "tii": round(traffic_intelligence_index, 1),
    "vqi": round(visit_quality_index, 1),
    "sai": round(store_attraction_index, 1),
    "aqi": round(audience_quality_index, 1),
    "ai_confidence": ai_confidence,
    "ai_confidence_band": ai_confidence_band,
    "primary_bottleneck": primary_bottleneck,
}

with st.expander("Executive AI Brief", expanded=True):
    conf_cols = st.columns(2)
    with conf_cols[0]:
        render_card(
            "AI Confidence Score",
            f"{ai_confidence}%",
            f"{ai_confidence_band} confidence in narrative stability.",
            "Built from benchmark maturity, days in scope, visit volume, and score consistency."
        )
    with conf_cols[1]:
        render_card(
            "AI Narrative Scope",
            scope,
            f"Mode: {active_app_mode}",
            "The AI brief uses only the selected store and selected dashboard period."
        )

    if st.button("Generate Executive Intelligence Brief", type="primary", use_container_width=True, key="generate_ai_brief_button"):
        with st.spinner("Generating executive narrative..."):
            st.markdown(generate_ai_brief(ai_payload))
    else:
        st.info("Click the button to generate the AI brief.")

# =========================================================
# CONTEXT STRIP
# =========================================================
st.markdown(
    f"""
    <div class="summary-strip">
        <b>Store:</b> {loaded_store} &nbsp;&nbsp;•&nbsp;&nbsp;
        <b>Period:</b> {scope} &nbsp;&nbsp;•&nbsp;&nbsp;
        <b>View:</b> {active_app_mode} &nbsp;&nbsp;•&nbsp;&nbsp;
        <b>Trend grain:</b> {trend_grain.title()} &nbsp;&nbsp;•&nbsp;&nbsp;
        <b>Days in scope:</b> {days_in_scope}
    </div>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# PRIMARY OPPORTUNITY
# =========================================================
alert_map = {
    "Store Magnet": "Surrounding traffic exists, but the storefront is not slowing enough people down. Improve storefront visibility, display language, and first-glance stopping power.",
    "Window Capture": "The storefront is being noticed, but that attention is not translating strongly into closer approach or entry. Improve persuasion in the first few seconds.",
    "Entry Efficiency": "Visitors are entering, but too many remain shallow visits. Improve welcome experience, navigation, relevance, and early in-store discovery.",
    "Dwell Quality": "Visitors are entering, but not staying long enough to show stronger browsing or deeper engagement. Improve in-store discovery value and assisted interaction.",
    "Floor Conversion": "Visit quality exists, but business closure is the weakest layer. Focus on assisted selling, pricing, offer communication, and conversion friction.",
}

st.markdown(
    f"""
    <div class="alert-panel">
        <div class="alert-title">Primary Opportunity</div>
        <div class="alert-headline">{primary_bottleneck}</div>
        <div class="alert-body">{alert_map.get(primary_bottleneck, "This is currently the weakest business layer in the store journey.")}</div>
    </div>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# TOP INSIGHTS
# =========================================================
st.markdown("<div class='section-title'>Top Business Takeaways</div>", unsafe_allow_html=True)
priority_cols = st.columns(3)
for i, card in enumerate(top_insights):
    with priority_cols[i]:
        render_priority_card(card["label"], card["title"], card["body"], card["pills"])

# =========================================================
# EXECUTIVE SUMMARY
# =========================================================
st.markdown("<div class='section-title'>Executive Summary</div>", unsafe_allow_html=True)

summary_row_1 = st.columns(4)
with summary_row_1[0]:
    render_card(
        "Store Visits",
        fmt_int(store_visits),
        "Total visits recorded in the selected period.",
        "This is period demand entering the store journey."
    )
with summary_row_1[1]:
    render_card(
        "Qualified Visit Rate",
        fmt_pct_from_ratio(qualified_rate),
        "Share of visits that became meaningful visits.",
        f"Formula: {fmt_int(qualified_visits)} / {fmt_int(store_visits)} = {fmt_pct_from_ratio(qualified_rate)}"
    )
with summary_row_1[2]:
    render_card(
        "Engaged Visit Rate",
        fmt_pct_from_ratio(engaged_rate),
        "Share of visits showing deeper interaction.",
        f"Formula: {fmt_int(engaged_visits)} / {fmt_int(store_visits)} = {fmt_pct_from_ratio(engaged_rate)}"
    )
with summary_row_1[3]:
    render_card(
        commercial_ratio_label(active_app_mode),
        fmt_pct_from_ratio(commercial_ratio),
        commercial_ratio_description(active_app_mode),
        f"Formula: {fmt_int(active_transactions)} / {fmt_int(store_visits)} = {fmt_pct_from_ratio(commercial_ratio)}"
    )

summary_row_2 = st.columns(4)
with summary_row_2[0]:
    render_card(
        "Walk-by Traffic",
        fmt_float(walk_by, 2),
        "Average surrounding traffic opportunity.",
        "Use together with store interest to understand storefront pull."
    )
with summary_row_2[1]:
    render_card(
        "Store Interest",
        fmt_float(interest, 2),
        "Average traffic showing stronger attention to the store.",
        "This is the first attention filter inside the traffic journey."
    )
with summary_row_2[2]:
    render_card(
        "Average Dwell",
        fmt_seconds(avg_dwell_seconds),
        "Average time spent by visitors.",
        "Higher dwell generally indicates stronger browse quality or deeper engagement."
    )
with summary_row_2[3]:
    render_card(
        "Peak Audience Level",
        fmt_int(peak_estimated_people),
        "Highest audience level observed in the period.",
        "Useful for staffing, display planning, and media timing."
    )

# =========================================================
# BENCHMARK SNAPSHOT
# =========================================================
st.markdown("<div class='section-title'>Benchmark Snapshot</div>", unsafe_allow_html=True)

benchmark_row = st.columns(4)
with benchmark_row[0]:
    render_card(
        "Traffic Capture",
        fmt_pct_from_ratio(traffic_capture_ratio),
        f"<span class='{traffic_capture_class}'>{traffic_capture_label}</span><br>How much surrounding traffic becomes strong attention.",
        f"Formula: {fmt_float(interest, 2)} / {fmt_float(walk_by, 2)}"
    )
with benchmark_row[1]:
    render_card(
        "Engagement Depth",
        fmt_pct_from_ratio(engagement_depth_ratio),
        f"<span class='{engagement_depth_class}'>{engagement_depth_label}</span><br>How many qualified visits become deeper interactions.",
        f"Formula: {fmt_int(engaged_visits)} / {fmt_int(qualified_visits) if qualified_visits else 0}"
    )
with benchmark_row[2]:
    weekday_text = f"Peak weekday: {weekday_peak}" if weekday_peak else "Peak weekday unavailable"
    render_card(
        "Commercial Strength",
        fmt_pct_from_ratio(commercial_ratio),
        f"<span class='{commercial_class}'>{commercial_label}</span><br>{weekday_text}",
        "Interpret together with visit quality, not in isolation."
    )
with benchmark_row[3]:
    render_card(
        "Benchmark Maturity",
        maturity_label,
        f"<span class='{maturity_class}'>{maturity_label}</span><br>{maturity_text}",
        "More benchmark depth improves score stability."
    )

# =========================================================
# STORY SECTION
# =========================================================
st.markdown("<div class='section-title'>Business Reading</div>", unsafe_allow_html=True)
story_cols = st.columns(3)

with story_cols[0]:
    render_story_card(
        "Traffic Reading",
        priority_narratives["traffic_title"],
        [
            f"Walk-by {fmt_float(walk_by, 2)}",
            f"Interest {fmt_float(interest, 2)}",
            f"Near-store {fmt_float(near_store, 2)}",
        ],
        priority_narratives["traffic_body"]
    )

with story_cols[1]:
    render_story_card(
        "Visit Quality Reading",
        priority_narratives["visit_title"],
        [
            f"Visits {fmt_int(store_visits)}",
            f"Qualified {fmt_int(qualified_visits)}",
            f"Engaged {fmt_int(engaged_visits)}",
        ],
        priority_narratives["visit_body"]
    )

with story_cols[2]:
    render_story_card(
        "Commercial Reading",
        priority_narratives["commercial_title"],
        [
            f"Transactions / responses {fmt_int(active_transactions)}",
            f"Value {fmt_currency(active_value)}",
            f"Ratio {fmt_pct_from_ratio(commercial_ratio)}",
        ],
        priority_narratives["commercial_body"]
    )

# =========================================================
# DIAGNOSTIC INDICES
# =========================================================
st.markdown("<div class='section-title'>Diagnostic Indices</div>", unsafe_allow_html=True)
diag_row = st.columns(4)

with diag_row[0]:
    render_card(
        "Traffic Intelligence",
        f"{traffic_intelligence_index:.0f}",
        f"<span class='{badge_tii}'>{label_tii}</span><br>Overall traffic health score."
    )
with diag_row[1]:
    render_card(
        "Visit Quality",
        f"{visit_quality_index:.0f}",
        f"<span class='{badge_vqi}'>{label_vqi}</span><br>Quality of visits after entry."
    )
with diag_row[2]:
    render_card(
        "Store Attraction",
        f"{store_attraction_index:.0f}",
        f"<span class='{badge_sai}'>{label_sai}</span><br>Ability to convert attention into entry."
    )
with diag_row[3]:
    render_card(
        "Audience Quality",
        f"{audience_quality_index:.0f}",
        f"<span class='{badge_aqi}'>{label_aqi}</span><br>Directional audience quality score."
    )

# =========================================================
# MAIN TABS
# =========================================================
tab_funnels, tab_trend, tab_behaviour, tab_audience, tab_deep = st.tabs(
    ["Funnels", "Trend Intelligence", "Behaviour", "Audience", "Deep Diagnostics"]
)

# =========================================================
# TAB 1 — FUNNELS
# =========================================================
with tab_funnels:
    st.markdown(
        f"""
        <div class='panel'>
            <b>Store Journey Funnels</b>
            <div class='note'>
                This section shows the business journey from surrounding traffic to attention, then from visits to meaningful visits,
                deeper interaction, and finally commercial response.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    funnel_cols = st.columns(2)

    with funnel_cols[0]:
        signal_fig = go.Figure(go.Funnel(
            y=["Walk-by", "Interest", "Near Store"],
            x=[float(walk_by), float(interest), float(near_store)],
            texttemplate="%{value:.2f}",
            textposition="inside",
            opacity=0.94,
            marker={"color": [COLORS["grey"], COLORS["orange"], COLORS["indigo"]]},
            connector={"line": {"color": "rgba(99,102,241,0.25)", "width": 1.2}},
        ))
        signal_fig.update_layout(
            title="Attention Capture Funnel",
            height=380,
            margin=dict(l=20, r=20, t=55, b=20),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(signal_fig, use_container_width=True, config=PLOT_CONFIG, key="funnels_attention_capture_funnel")

        drop_attention = max(0, walk_by - interest)
        st.markdown(
            f"""
            <div class="panel note">
                <b>What this means</b><br><br>
                Out of average surrounding traffic of <b>{fmt_float(walk_by, 2)}</b>, only <b>{fmt_float(interest, 2)}</b> is becoming strong store attention.
                The biggest attention gap is <b>{fmt_float(drop_attention, 2)}</b>, which is why storefront pull remains critical here.
            </div>
            """,
            unsafe_allow_html=True,
        )

    with funnel_cols[1]:
        visit_fig = go.Figure(go.Funnel(
            y=["Visits", "Qualified", "Engaged", "Transactions / Responses"],
            x=[float(store_visits), float(qualified_visits), float(engaged_visits), float(active_transactions)],
            texttemplate=[
                f"{fmt_int(store_visits)}",
                f"{fmt_int(qualified_visits)} · {fmt_pct_from_ratio(qualified_rate)}",
                f"{fmt_int(engaged_visits)} · {fmt_pct_from_ratio(engaged_rate)}",
                f"{fmt_int(active_transactions)} · {fmt_pct_from_ratio(commercial_ratio)}",
            ],
            textposition="inside",
            opacity=0.94,
            marker={"color": [COLORS["sky"], COLORS["amber"], COLORS["violet"], COLORS["emerald"]]},
            connector={"line": {"color": "rgba(99,102,241,0.25)", "width": 1.2}},
        ))
        visit_fig.update_layout(
            title="Visit Quality & Commercial Funnel",
            height=380,
            margin=dict(l=20, r=20, t=55, b=20),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(visit_fig, use_container_width=True, config=PLOT_CONFIG, key="funnels_visit_quality_commercial_funnel")

        st.markdown(
            f"""
            <div class="panel note">
                <b>What this means</b><br><br>
                The store produced <b>{fmt_int(store_visits)}</b> visits, of which <b>{fmt_int(qualified_visits)}</b> were meaningful and
                <b>{fmt_int(engaged_visits)}</b> showed deeper interaction. The final business response should be interpreted as
                <b>{commercial_ratio_label(active_app_mode).lower()}</b>, not a literal person-by-person conversion rate.
            </div>
            """,
            unsafe_allow_html=True,
        )

# =========================================================
# TAB 2 — TREND INTELLIGENCE
# =========================================================
with tab_trend:
    st.markdown(
        """
        <div class='panel'>
            <b>Trend Intelligence</b>
            <div class='note'>
                Multi-day views are split into volume, rates, and weekday pattern so it is easier to see what changed,
                whether the issue is traffic, visit quality, or commercial depth.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if loaded_mode == "Daily":
        full_hours = pd.DataFrame({
            "hour_of_day": list(range(24)),
            "hour_label": [f"{h:02d}:00" for h in range(24)],
        })
        hourly_plot_df = full_hours.merge(hourly_df, on=["hour_of_day", "hour_label"], how="left")
        for col in ["avg_far_devices", "avg_mid_devices", "avg_near_devices", "avg_apple_devices", "avg_samsung_devices", "avg_other_devices"]:
            if col in hourly_plot_df.columns:
                hourly_plot_df[col] = pd.to_numeric(hourly_plot_df[col], errors="coerce").fillna(0)

        daily_cols = st.columns(2)

        with daily_cols[0]:
            fig_hourly = go.Figure()
            fig_hourly.add_trace(go.Scatter(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_far_devices"], mode="lines+markers", name="Walk-by"))
            fig_hourly.add_trace(go.Scatter(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_mid_devices"], mode="lines+markers", name="Interest"))
            fig_hourly.add_trace(go.Scatter(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_near_devices"], mode="lines+markers", name="Near-store"))
            fig_hourly.update_layout(title="Hourly Attention Shape")
            st.plotly_chart(style_chart(fig_hourly), use_container_width=True, config=PLOT_CONFIG, key="trend_daily_hourly_attention_shape")

        with daily_cols[1]:
            fig_hourly_brand = go.Figure()
            fig_hourly_brand.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_apple_devices"], name="Apple"))
            fig_hourly_brand.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_samsung_devices"], name="Samsung"))
            fig_hourly_brand.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_other_devices"], name="Other"))
            fig_hourly_brand.update_layout(title="Hourly Audience Mix", barmode="stack")
            st.plotly_chart(style_chart(fig_hourly_brand), use_container_width=True, config=PLOT_CONFIG, key="trend_daily_hourly_audience_mix")

    else:
        trend_cols_1 = st.columns(2)

        with trend_cols_1[0]:
            fig_visit_trend = go.Figure()
            fig_visit_trend.add_trace(go.Scatter(
                x=trend_df["period_label"], y=trend_df["store_visits"], name="Visits", mode="lines+markers", line=dict(width=3)
            ))
            fig_visit_trend.add_trace(go.Scatter(
                x=trend_df["period_label"], y=trend_df["qualified_footfall"], name="Qualified Visits", mode="lines+markers"
            ))
            fig_visit_trend.add_trace(go.Scatter(
                x=trend_df["period_label"], y=trend_df["engaged_visits"], name="Engaged Visits", mode="lines+markers"
            ))
            fig_visit_trend.update_layout(title="Volume Trend Across the Selected Period")
            st.plotly_chart(style_chart(fig_visit_trend), use_container_width=True, config=PLOT_CONFIG, key="trend_period_volume_trend")

        with trend_cols_1[1]:
            fig_rate_trend = go.Figure()
            fig_rate_trend.add_trace(go.Scatter(
                x=trend_df["period_label"], y=trend_df["qualified_rate"] * 100, name="Qualified Rate", mode="lines+markers"
            ))
            fig_rate_trend.add_trace(go.Scatter(
                x=trend_df["period_label"], y=trend_df["engaged_rate"] * 100, name="Engaged Rate", mode="lines+markers"
            ))
            fig_rate_trend.add_trace(go.Scatter(
                x=trend_df["period_label"], y=trend_df["traffic_capture_ratio"] * 100, name="Traffic Capture", mode="lines+markers"
            ))
            fig_rate_trend.update_layout(title="Quality Trend Across the Selected Period")
            fig_rate_trend.update_yaxes(ticksuffix="%")
            st.plotly_chart(style_chart(fig_rate_trend), use_container_width=True, config=PLOT_CONFIG, key="trend_period_quality_trend")

        trend_cols_2 = st.columns(2)

        with trend_cols_2[0]:
            fig_signal_trend = go.Figure()
            fig_signal_trend.add_trace(go.Bar(x=trend_df["period_label"], y=trend_df["walk_by_traffic"], name="Walk-by"))
            fig_signal_trend.add_trace(go.Bar(x=trend_df["period_label"], y=trend_df["store_interest"], name="Interest"))
            fig_signal_trend.add_trace(go.Bar(x=trend_df["period_label"], y=trend_df["near_store"], name="Near-store"))
            fig_signal_trend.update_layout(title="Attention Layer Trend", barmode="group")
            st.plotly_chart(style_chart(fig_signal_trend), use_container_width=True, config=PLOT_CONFIG, key="trend_period_attention_layer_trend")

        with trend_cols_2[1]:
            fig_weekday = go.Figure()
            fig_weekday.add_trace(go.Bar(x=weekday_df["weekday"], y=weekday_df["store_visits"], name="Visits"))
            fig_weekday.add_trace(go.Scatter(
                x=weekday_df["weekday"], y=weekday_df["engaged_rate"] * 100, name="Engaged Rate", mode="lines+markers", yaxis="y2"
            ))
            fig_weekday.update_layout(
                title="Weekday Pattern: Volume vs Depth",
                yaxis=dict(title="Visits"),
                yaxis2=dict(title="Engaged Rate %", overlaying="y", side="right", showgrid=False, ticksuffix="%"),
            )
            st.plotly_chart(style_chart(fig_weekday), use_container_width=True, config=PLOT_CONFIG, key="trend_weekday_pattern_volume_depth")

        st.markdown(
            f"""
            <div class="panel note">
                <b>How to read this</b><br><br>
                Use the first chart to see whether the selected period changed mainly because of <b>volume</b>.
                Use the second chart to see whether the change was really about <b>quality</b>.
                Use the weekday pattern to identify the best days for trading, staffing, promotion, or media activation.
            </div>
            """,
            unsafe_allow_html=True,
        )

# =========================================================
# TAB 3 — BEHAVIOUR
# =========================================================
with tab_behaviour:
    st.markdown(
        """
        <div class='panel'>
            <b>Visitor Behaviour</b>
            <div class='note'>
                This section makes dwell easier to read by showing both raw volume and share mix.
                That helps separate “many quick visits” from “fewer but more valuable visits.”
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    behaviour_top = st.columns(3)
    with behaviour_top[0]:
        render_card("Average Dwell", fmt_seconds(avg_dwell_seconds), "Average time spent by visitors.")
    with behaviour_top[1]:
        render_card("Qualified Visit Rate", fmt_pct_from_ratio(qualified_rate), "Share of visits that became meaningful visits.")
    with behaviour_top[2]:
        render_card("Engaged Visit Rate", fmt_pct_from_ratio(engaged_rate), "Share of visits that moved into deeper interaction.")

    if not dwell_plot_df.empty:
        beh_cols = st.columns(2)

        with beh_cols[0]:
            fig_dwell = px.bar(
                dwell_plot_df,
                x="dwell_bucket",
                y="visits",
                title="Dwell Time Distribution by Volume"
            )
            st.plotly_chart(style_chart(fig_dwell), use_container_width=True, config=PLOT_CONFIG, key="behaviour_dwell_distribution_volume")

        with beh_cols[1]:
            fig_dwell_share = px.bar(
                dwell_plot_df,
                x="dwell_bucket",
                y=dwell_plot_df["share"] * 100,
                title="Dwell Mix Share"
            )
            fig_dwell_share.update_yaxes(ticksuffix="%")
            st.plotly_chart(style_chart(fig_dwell_share), use_container_width=True, config=PLOT_CONFIG, key="behaviour_dwell_distribution_share")

        long_dwell_share = dwell_plot_df.loc[dwell_plot_df["dwell_bucket"].isin(["01-03m", "03-05m", "05m+"]), "share"].sum()
        short_dwell_share = dwell_plot_df.loc[dwell_plot_df["dwell_bucket"].isin(["00-10s", "10-30s", "30-60s"]), "share"].sum()

        st.markdown(
            f"""
            <div class="panel note">
                <b>Interpretation</b><br><br>
                Short-stay visits account for <b>{fmt_pct_from_ratio(short_dwell_share)}</b> of the current mix, while longer stays
                account for <b>{fmt_pct_from_ratio(long_dwell_share)}</b>. This helps explain whether the store is mostly creating
                quick pass-throughs or stronger browse-quality sessions.
            </div>
            """,
            unsafe_allow_html=True,
        )

# =========================================================
# TAB 4 — AUDIENCE
# =========================================================
with tab_audience:
    st.markdown(
        """
        <div class='panel'>
            <b>Audience</b>
            <div class='note'>
                This section separates overall audience mix from time-of-day patterns.
                That makes weekly and monthly views easier to interpret than a single crowded chart.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    audience_top = st.columns(3)
    with audience_top[0]:
        render_card(
            "Average Audience Level",
            fmt_float(avg_estimated_people, 2),
            "Average nearby audience level in the selected period."
        )
    with audience_top[1]:
        render_card(
            "Peak Audience Level",
            fmt_int(peak_estimated_people),
            "Highest audience level observed in the selected period."
        )
    with audience_top[2]:
        render_card(
            "Average Detected Devices",
            fmt_float(avg_detected_devices, 2),
            "Average audience signal level across the selected period."
        )

    if not hourly_df.empty:
        aud_cols = st.columns(2)

        with aud_cols[0]:
            if not brand_mix_df.empty and brand_mix_df["value"].sum() > 0:
                fig_brand_mix = px.pie(
                    brand_mix_df,
                    values="value",
                    names="brand",
                    title="Overall Audience Mix",
                    hole=0.55
                )
                fig_brand_mix.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    legend=dict(orientation="h", y=-0.1)
                )
                st.plotly_chart(fig_brand_mix, use_container_width=True, config=PLOT_CONFIG, key="audience_overall_mix_donut")

        with aud_cols[1]:
            hourly_plot_df = hourly_df.copy()
            for col in ["avg_apple_devices", "avg_samsung_devices", "avg_other_devices"]:
                if col in hourly_plot_df.columns:
                    hourly_plot_df[col] = pd.to_numeric(hourly_plot_df[col], errors="coerce").fillna(0)

            brand_fig = go.Figure()
            brand_fig.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_apple_devices"], name="Apple"))
            brand_fig.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_samsung_devices"], name="Samsung"))
            brand_fig.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_other_devices"], name="Other"))
            brand_fig.update_layout(title="Hourly Audience Mix", barmode="stack")
            st.plotly_chart(
                style_chart(brand_fig),
                use_container_width=True,
                config=PLOT_CONFIG,
                key="audience_hourly_brand_mix_chart"
            )

        st.markdown(
            """
            <div class="panel note">
                <b>How to use this</b><br><br>
                Use the overall mix to understand the broad audience profile across the selected period.
                Use the hourly chart to see when that audience becomes strongest, which is useful for campaign timing,
                staffing, premium-category display planning, and partner discussions.
            </div>
            """,
            unsafe_allow_html=True,
        )

# =========================================================
# TAB 5 — DEEP DIAGNOSTICS
# =========================================================
with tab_deep:
    st.markdown(
        """
        <div class='panel'>
            <b>Deep Diagnostics</b>
            <div class='note'>
                This section keeps the advanced score system, but presents it in a cleaner order:
                first the strategic score picture, then the detailed table, then supporting context.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    index_breakdown_df = pd.DataFrame(
        {
            "Metric": [
                "Traffic Intelligence",
                "Visit Quality",
                "Store Attraction",
                "Audience Quality",
                "Store Magnet",
                "Window Capture",
                "Entry Efficiency",
                "Dwell Quality",
                "Floor Conversion Proxy",
            ],
            "Score": [
                traffic_intelligence_index,
                visit_quality_index,
                store_attraction_index,
                audience_quality_index,
                store_magnet_percentile_score,
                window_capture_score,
                entry_efficiency_percentile_score,
                dwell_quality_score,
                floor_conversion_score,
            ],
        }
    ).sort_values("Score", ascending=True)

    deep_top = st.columns(2)

    with deep_top[0]:
        fig_index = px.bar(
            index_breakdown_df,
            x="Score",
            y="Metric",
            orientation="h",
            title="Score Breakdown"
        )
        fig_index.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(style_chart(fig_index), use_container_width=True, config=PLOT_CONFIG, key="deep_score_breakdown_bar")

    with deep_top[1]:
        radar_df = pd.DataFrame({
            "Metric": ["Traffic", "Visit Quality", "Attraction", "Audience"],
            "Score": [traffic_intelligence_index, visit_quality_index, store_attraction_index, audience_quality_index],
        })
        fig_radar = go.Figure()
        fig_radar.add_trace(go.Scatterpolar(
            r=radar_df["Score"],
            theta=radar_df["Metric"],
            fill="toself",
            name="Core Strategic Scores"
        ))
        fig_radar.update_layout(
            title="Strategic Score Shape",
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=20, r=20, t=55, b=20),
        )
        st.plotly_chart(fig_radar, use_container_width=True, config=PLOT_CONFIG, key="deep_strategic_score_shape_radar")

    st.dataframe(index_breakdown_df.sort_values("Score", ascending=False), use_container_width=True, hide_index=True)

    deep_cards = st.columns(2)
    with deep_cards[0]:
        render_card(
            "Traffic-to-Visit Signal Index",
            fmt_float(daily_df["walkby_to_visit_index"].mean(), 2) if "walkby_to_visit_index" in daily_df.columns else "0.00",
            "Directional index showing how visit volume compares to surrounding traffic.",
            "Use as a relative capture indicator, not as a literal human conversion rate."
        )
    with deep_cards[1]:
        render_card(
            "Average Detected Devices",
            fmt_float(avg_detected_devices, 2),
            "Average audience signal level around the store.",
            "Selected period average based on curated daily metrics."
        )

    if not weekday_df.empty:
        st.markdown("<div class='section-title'>Weekday Diagnostic Table</div>", unsafe_allow_html=True)
        weekday_table = normalize_weekday_table(weekday_df)
        st.dataframe(weekday_table, use_container_width=True, hide_index=True)

# =========================================================
# DEBUG
# =========================================================
if loaded_filters.get("show_debug", False):
    st.markdown("### Timezone Diagnostics")
    st.markdown(
        """
        <div class='panel note'>
            Use this only when validating date alignment or ingestion timing issues.
            The business dashboard itself already reads from curated daily tables.
        </div>
        """,
        unsafe_allow_html=True,
    )
    if not debug_df.empty:
        st.dataframe(debug_df, use_container_width=True)

# =========================================================
# FOOTER
# =========================================================
st.caption("Retail Intelligence Command Center · Retail Operations & Retail Media Measurement · Powered by AWS Athena · Streamlit")
