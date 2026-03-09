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
    }
}

html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    background: var(--bg) !important;
    color: var(--text) !important;
}

.main .block-container {
    max-width: 100% !important;
    padding: 1.05rem 1.1rem 2rem 1.1rem !important;
}

section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, var(--panel) 0%, var(--bg-soft) 100%) !important;
    border-right: 1px solid var(--border) !important;
}

section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] div {
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
    font-size: 1.9rem;
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
    margin: 1.05rem 0 .7rem 0;
    font-size: .76rem;
    text-transform: uppercase;
    letter-spacing: .13em;
    color: var(--accent);
    font-weight: 800;
}

.kpi-card {
    background:
        radial-gradient(circle at top right, rgba(99,102,241,0.06), transparent 30%),
        linear-gradient(145deg, var(--panel) 0%, var(--panel-2) 100%);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 1rem 1rem .95rem 1rem;
    box-shadow: var(--shadow-soft);
    min-height: 164px;
    position: relative;
    overflow: hidden;
    margin-bottom: .9rem;
}

.kpi-card::after {
    content: '';
    position: absolute;
    top: -35px;
    right: -35px;
    width: 90px;
    height: 90px;
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
    margin-top: .6rem;
    padding-top: .55rem;
    border-top: 1px dashed rgba(99,102,241,0.16);
    color: var(--text-muted);
    font-size: .76rem;
    line-height: 1.45;
}

.panel {
    background:
        radial-gradient(circle at top right, rgba(99,102,241,0.06), transparent 32%),
        linear-gradient(145deg, var(--panel) 0%, var(--panel-2) 100%);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 1rem 1rem .85rem 1rem;
    box-shadow: var(--shadow-soft);
    margin-bottom: 1rem;
}

.note {
    color: var(--text-3);
    font-size: .86rem;
    line-height: 1.6;
}

.alert-panel {
    background:
        radial-gradient(circle at top right, rgba(245,158,11,0.08), transparent 35%),
        linear-gradient(145deg, var(--panel) 0%, var(--panel-2) 100%);
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

.story-card {
    background:
        radial-gradient(circle at top right, rgba(99,102,241,0.06), transparent 32%),
        linear-gradient(145deg, var(--panel) 0%, var(--panel-2) 100%);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 1rem;
    box-shadow: var(--shadow-soft);
    min-height: 205px;
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

.badge-good, .badge-warn, .badge-bad {
    display:inline-block;
    padding:.12rem .48rem;
    border-radius:8px;
    font-size:.74rem;
    font-weight:800;
}
.badge-good { background: rgba(16,185,129,0.10); color: var(--good); }
.badge-warn { background: rgba(245,158,11,0.10); color: var(--warn); }
.badge-bad  { background: rgba(244,63,94,0.10); color: var(--bad); }

.small-muted {
    color: var(--text-muted);
    font-size: .78rem;
}

.stTabs [data-baseweb="tab-list"],
div[data-testid="stTabs"] div[role="tablist"] {
    background: rgba(99,102,241,0.04) !important;
    border: 1px solid var(--border) !important;
    border-radius: 12px !important;
    padding: 4px !important;
    gap: 4px !important;
    display: flex !important;
    flex-wrap: nowrap !important;
    justify-content: flex-start !important;
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

.stTabs [data-baseweb="tab-panel"] {
    padding-top: 1rem !important;
}

div[data-testid="stExpander"] {
    border-radius: 16px !important;
    overflow: hidden;
    border: 1px solid var(--border) !important;
    box-shadow: var(--shadow-soft);
}

div[data-testid="stExpander"] details {
    background: linear-gradient(145deg, var(--panel) 0%, var(--panel-2) 100%) !important;
}

div[data-testid="stAlert"] {
    border-radius: 14px !important;
}

@media (max-width: 900px) {
    .hero h1 {
        font-size: 1.72rem;
    }
}

@media (max-width: 768px) {
    .main .block-container {
        padding: .92rem .72rem 1.6rem .72rem !important;
    }

    .hero {
        padding: 1.05rem .95rem .95rem .95rem;
        border-radius: 20px;
    }

    .hero h1 {
        font-size: 1.52rem;
    }

    .hero p {
        font-size: 0.91rem;
    }

    .panel {
        padding: .9rem .9rem .75rem .9rem !important;
    }

    .kpi-card, .story-card {
        min-height: auto;
    }

    .stTabs [data-baseweb="tab"],
    div[data-testid="stTabs"] button[role="tab"] {
        font-size: .84rem !important;
        padding: .42rem .8rem !important;
    }
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
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY")

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


def fmt_pct(x, digits: int = 1):
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
            y=-0.18,
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


def build_weekday_trend(daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df.empty:
        return pd.DataFrame()

    df = daily_df.copy()
    df["metric_date"] = pd.to_datetime(df["metric_date"])
    df["weekday_num"] = df["metric_date"].dt.weekday
    df["weekday"] = df["metric_date"].dt.day_name()

    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

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

    out["weekday"] = pd.Categorical(out["weekday"], categories=weekday_order, ordered=True)
    return out.sort_values("weekday")


def prepare_dwell_plot_df(source_df: pd.DataFrame) -> pd.DataFrame:
    dwell_order = ["00-10s", "10-30s", "30-60s", "01-03m", "03-05m", "05m+"]
    plot_df = source_df.copy()
    if plot_df.empty or "dwell_bucket" not in plot_df.columns:
        return plot_df
    plot_df["dwell_bucket"] = pd.Categorical(plot_df["dwell_bucket"], categories=dwell_order, ordered=True)
    return plot_df.sort_values("dwell_bucket")


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


# =========================================================
# AI BRIEF
# =========================================================
@st.cache_data(ttl=300)
def generate_ai_brief(ai_payload: dict) -> str:
    if not GEMINI_API_KEY or genai is None:
        return "⚠️ AI unavailable: GEMINI_API_KEY not configured."

    try:
        client = genai.Client(api_key=GEMINI_API_KEY)

        prompt = f"""
You are a retail analytics strategy consultant.

Analyze the following store intelligence metrics.

Scope: {ai_payload['scope']}
Mode: {ai_payload['mode']}
AI Confidence Score: {ai_payload['ai_confidence']}% ({ai_payload['ai_confidence_band']})

TRAFFIC SIGNALS
Walk-by traffic index: {ai_payload['walk_by']}
Store interest index: {ai_payload['interest']}
Near-store index: {ai_payload['near_store']}

VISIT METRICS
Store visits: {ai_payload['visits']}
Qualified visits: {ai_payload['qualified_visits']}
Engaged visits: {ai_payload['engaged_visits']}
Qualified visit rate: {ai_payload['qualified_rate']}%
Engaged visit rate: {ai_payload['engaged_rate']}%
Average dwell: {ai_payload['avg_dwell']}

COMMERCIAL SIGNALS
Transactions: {ai_payload['transactions']}
Revenue: {ai_payload['value']}
Sales conversion: {ai_payload['sales_conversion']}%

INDEX METRICS
Traffic Intelligence Index: {ai_payload['tii']}
Visit Quality Index: {ai_payload['vqi']}
Store Attraction Index: {ai_payload['sai']}
Audience Quality Index: {ai_payload['aqi']}

Write a short executive insight.

Format exactly as:

**What happened**
[1 short paragraph]

**Traffic interpretation**
[1 short paragraph]

**Visit behavior**
[1 short paragraph]

**Commercial outcome**
[1 short paragraph]

**Primary bottleneck**
[1 short paragraph]

**Recommended action**
[1 short paragraph]
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
# LOADERS - IST CANONICAL VIEWS
# =========================================================
@st.cache_data(ttl=300)
def load_store_list() -> pd.DataFrame:
    return run_athena_query(
        "SELECT DISTINCT store_id FROM nstags_dashboard_metrics_canonical ORDER BY store_id"
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
            SELECT DISTINCT DATE(from_unixtime(ts) AT TIME ZONE 'Asia/Kolkata') AS metric_date
            FROM nstags_live_analytics
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
            walkby_to_visit_index
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
            ROUND(AVG(avg_near_devices), 2) AS avg_near_devices,
            ROUND(AVG(avg_apple_devices), 2) AS avg_apple_devices,
            ROUND(AVG(avg_samsung_devices), 2) AS avg_samsung_devices,
            ROUND(AVG(avg_other_devices), 2) AS avg_other_devices
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
def load_intelligence_scores_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
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


@st.cache_data(ttl=300)
def load_dynamic_index_scores_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
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


@st.cache_data(ttl=300)
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


# =========================================================
# HEADER
# =========================================================
st.markdown(
    """
    <div class="hero">
        <div class="eyebrow">Retail Intelligence Command Center</div>
        <h1>Retail Intelligence Command Center</h1>
        <p>
            A self-explanatory store performance dashboard that shows what happened,
            why it happened, and where the biggest improvement opportunity lies.
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

    selected_store = st.selectbox(
        "Active Store",
        stores_df["store_id"].dropna().astype(str).tolist(),
        key="active_store"
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

    st.markdown("### Period Selection")
    period_mode = st.radio(
        "Analysis Window",
        ["Daily", "Weekly", "Monthly", "Yearly", "Custom"],
        key="analysis_window",
    )

    if period_mode == "Daily":
        selected_day = st.selectbox(
            "Select Date",
            options=list(reversed(available_dates)),
            index=0,
            format_func=lambda d: d.strftime("%d %b %Y"),
            key="daily_date",
        )
        start_date = selected_day
        end_date = selected_day
    elif period_mode == "Weekly":
        end_date = st.date_input(
            "Week End Date",
            value=max_available_date,
            min_value=min_available_date,
            max_value=max_available_date,
            key="week_end_date",
        )
        start_date = max(min_available_date, end_date - timedelta(days=6))
    elif period_mode == "Monthly":
        end_date = st.date_input(
            "Month End Date",
            value=max_available_date,
            min_value=min_available_date,
            max_value=max_available_date,
            key="month_end_date",
        )
        start_date = max(min_available_date, end_date - timedelta(days=29))
    elif period_mode == "Yearly":
        end_date = st.date_input(
            "Year End Date",
            value=max_available_date,
            min_value=min_available_date,
            max_value=max_available_date,
            key="year_end_date",
        )
        start_date = max(min_available_date, end_date - timedelta(days=364))
    else:
        default_start = max(min_available_date, max_available_date - timedelta(days=29))
        selected_range = st.date_input(
            "Custom Date Range",
            value=(default_start, max_available_date),
            min_value=min_available_date,
            max_value=max_available_date,
            key="custom_date_range",
        )
        if isinstance(selected_range, tuple) and len(selected_range) == 2:
            start_date, end_date = selected_range
        elif isinstance(selected_range, list) and len(selected_range) == 2:
            start_date, end_date = selected_range[0], selected_range[1]
        else:
            start_date, end_date = default_start, max_available_date

    st.markdown("### Commercial Inputs")
    transactions = st.number_input("Transactions", min_value=0, value=35, step=1, key="transactions_input")
    value = st.number_input("Revenue / Campaign Value", min_value=0, value=45000, step=1000, key="revenue_input")
    show_debug = st.checkbox("Show timezone diagnostics", value=False, key="show_debug")

# =========================================================
# DATA LOAD
# =========================================================
start_date_str = start_date.isoformat()
end_date_str = end_date.isoformat()

try:
    daily_df = load_dashboard_daily_rows(selected_store, start_date_str, end_date_str)
    hourly_df = load_hourly_traffic_range(selected_store, start_date_str, end_date_str)
    dwell_df = load_dwell_buckets_range(selected_store, start_date_str, end_date_str)
    intelligence_df = load_intelligence_scores_range(selected_store, start_date_str, end_date_str)
    dynamic_df = load_dynamic_index_scores_range(selected_store, start_date_str, end_date_str)
except Exception as e:
    st.error(f"Failed to load dashboard data: {e}")
    st.stop()

if daily_df.empty:
    st.warning("No dashboard metrics were found for the selected period.")
    if show_debug:
        try:
            debug_df = load_debug_partition_vs_ist(selected_store, start_date_str, end_date_str)
            st.dataframe(debug_df, use_container_width=True, key="debug_empty_metrics")
        except Exception:
            pass
    st.stop()

# =========================================================
# PREP
# =========================================================
for col in daily_df.columns:
    if col != "metric_date":
        daily_df[col] = pd.to_numeric(daily_df[col], errors="coerce").fillna(0)

daily_df["metric_date"] = pd.to_datetime(daily_df["metric_date"]).dt.date

score_row = dynamic_df.iloc[0].to_dict() if not dynamic_df.empty else {}
_ = intelligence_df.iloc[0].to_dict() if not intelligence_df.empty else {}

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
sales_conversion = safe_div(transactions, store_visits)

traffic_intelligence_index = float(score_row.get("traffic_intelligence_index", 0) or 0)
visit_quality_index = float(score_row.get("visit_quality_index", 0) or 0)
store_attraction_index = float(score_row.get("store_attraction_index", 0) or 0)
audience_quality_index = float(score_row.get("audience_quality_index", 0) or 0)

store_magnet_percentile_score = float(score_row.get("store_magnet_percentile_score", 0) or 0)
window_capture_score = float(score_row.get("window_capture_score", 0) or 0)
entry_efficiency_percentile_score = float(score_row.get("entry_efficiency_percentile_score", 0) or 0)
dwell_quality_score = float(score_row.get("dwell_quality_score", 0) or 0)
floor_conversion_score = min(sales_conversion * 400, 100)
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

trend_grain = infer_trend_grain(start_date, end_date)
scope = scope_title(period_mode, start_date, end_date)
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
conversion_class, conversion_label = classify_band(sales_conversion, 0.20, 0.08)

days_in_scope = (end_date - start_date).days + 1
ai_confidence, ai_confidence_band = compute_ai_confidence({
    "benchmark_population": benchmark_population,
    "days_in_scope": days_in_scope,
    "store_visits": store_visits,
    "traffic_intelligence_index": traffic_intelligence_index,
    "visit_quality_index": visit_quality_index,
    "store_attraction_index": store_attraction_index,
    "audience_quality_index": audience_quality_index,
})

# =========================================================
# AI BRIEF
# =========================================================
ai_payload = {
    "scope": scope,
    "mode": app_mode,
    "walk_by": round(walk_by, 2),
    "interest": round(interest, 2),
    "near_store": round(near_store, 2),
    "visits": int(round(store_visits)),
    "qualified_visits": int(round(qualified_visits)),
    "engaged_visits": int(round(engaged_visits)),
    "qualified_rate": round(qualified_rate * 100, 1),
    "engaged_rate": round(engaged_rate * 100, 1),
    "avg_dwell": fmt_seconds(avg_dwell_seconds),
    "transactions": int(transactions),
    "value": fmt_currency(value),
    "sales_conversion": round(sales_conversion * 100, 1),
    "tii": round(traffic_intelligence_index, 1),
    "vqi": round(visit_quality_index, 1),
    "sai": round(store_attraction_index, 1),
    "aqi": round(audience_quality_index, 1),
    "ai_confidence": ai_confidence,
    "ai_confidence_band": ai_confidence_band,
}

with st.expander("Executive AI Brief", expanded=True):
    conf_cols = st.columns(2)
    with conf_cols[0]:
        render_card(
            "AI Confidence Score",
            f"{ai_confidence}%",
            f"{ai_confidence_band} confidence in AI narrative stability.",
            f"Built from benchmark maturity, days in scope, visit volume, and score consistency."
        )
    with conf_cols[1]:
        render_card(
            "AI Narrative Scope",
            scope,
            f"Mode: {app_mode}",
            "The AI brief uses only dashboard metrics from the selected store and selected period."
        )

    if st.button("Generate Executive Intelligence Brief", type="primary", use_container_width=True, key="generate_ai_brief_button"):
        with st.spinner("Generating executive narrative..."):
            st.markdown(generate_ai_brief(ai_payload))
    else:
        st.info("Click the button to generate the AI brief.")

# =========================================================
# SCOPE STRIP
# =========================================================
st.caption(
    f"Store: {selected_store} · Active period: {scope} · Trend grain: {trend_grain.title()} · Days in scope: {days_in_scope}"
)

# =========================================================
# PRIMARY OPPORTUNITY
# =========================================================
alert_map = {
    "Store Magnet": "Surrounding traffic exists, but the storefront is not slowing enough people down. Improve storefront visibility, display language, or exterior communication.",
    "Window Capture": "The storefront is being noticed, but attention is not translating into closer approach or meaningful entry. Improve the first 3 seconds of visual persuasion.",
    "Entry Efficiency": "Visitors are entering, but too many remain shallow visits. The issue may lie in store navigation, welcome experience, or relevance immediately after entry.",
    "Dwell Quality": "Visitors are entering, but not staying long enough to show serious browsing or assisted engagement. Improve inside-store engagement and discovery value.",
    "Floor Conversion": "Visit quality exists, but conversion into transactions is weak. Focus on assisted selling, pricing, offer communication, and closing friction.",
}

st.markdown(
    f"""
    <div class="alert-panel">
        <div class="alert-title">Primary Opportunity</div>
        <div class="alert-headline">{primary_bottleneck}</div>
        <div class="alert-body">{alert_map.get(primary_bottleneck, "This is currently the weakest operating layer in the store journey.")}</div>
    </div>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# EXECUTIVE SUMMARY
# =========================================================
st.markdown("<div class='section-title'>Executive Summary</div>", unsafe_allow_html=True)

summary_row_1 = st.columns(2)
with summary_row_1[0]:
    render_card(
        "Store Visits",
        fmt_int(store_visits),
        "Detected visit sessions for the selected period.",
        "Interpretation: these are validated visit sessions, not billing receipts."
    )
with summary_row_1[1]:
    render_card(
        "Qualified Visit Rate",
        fmt_pct(qualified_rate),
        "Share of visits that stayed long enough to count as meaningful.",
        f"Formula: {fmt_int(qualified_visits)} / {fmt_int(store_visits)} = {fmt_pct(qualified_rate)}"
    )

summary_row_2 = st.columns(2)
with summary_row_2[0]:
    render_card(
        "Engaged Visit Rate",
        fmt_pct(engaged_rate),
        "Share of visits showing deeper in-store interaction.",
        f"Formula: {fmt_int(engaged_visits)} / {fmt_int(store_visits)} = {fmt_pct(engaged_rate)}"
    )
with summary_row_2[1]:
    render_card(
        "Sales Conversion",
        fmt_pct(sales_conversion),
        "Share of visits converting into transactions.",
        f"Formula: {fmt_int(transactions)} / {fmt_int(store_visits)} = {fmt_pct(sales_conversion)}"
    )

# =========================================================
# BENCHMARK SNAPSHOT
# =========================================================
st.markdown("<div class='section-title'>Benchmark Snapshot</div>", unsafe_allow_html=True)

benchmark_row = st.columns(3)
with benchmark_row[0]:
    render_card(
        "Traffic Capture",
        fmt_pct(traffic_capture_ratio),
        f"<span class='{traffic_capture_class}'>{traffic_capture_label}</span><br>How much attention is being captured from surrounding traffic.",
        f"Formula: {fmt_float(interest,2)} / {fmt_float(walk_by,2)}"
    )
with benchmark_row[1]:
    render_card(
        "Engagement Depth",
        fmt_pct(engagement_depth_ratio),
        f"<span class='{engagement_depth_class}'>{engagement_depth_label}</span><br>How many qualified visits become engaged visits.",
        f"Formula: {fmt_int(engaged_visits)} / {fmt_int(qualified_visits) if qualified_visits else 0}"
    )
with benchmark_row[2]:
    weekday_text = f"Peak weekday: {weekday_peak}" if weekday_peak is not None else "Peak weekday unavailable"
    render_card(
        "Commercial Strength",
        fmt_pct(sales_conversion),
        f"<span class='{conversion_class}'>{conversion_label}</span><br>{weekday_text}",
        "Use together with visit quality, not as a standalone performance signal."
    )

# =========================================================
# STORE PERFORMANCE STORY
# =========================================================
st.markdown("<div class='section-title'>Store Performance Story</div>", unsafe_allow_html=True)

story_row_1 = st.columns(1)
with story_row_1[0]:
    render_story_card(
        "Traffic Story",
        "How strong was traffic around the store?",
        [
            f"Walk-by {fmt_float(walk_by, 2)}",
            f"Interest {fmt_float(interest, 2)}",
            f"Near Store {fmt_float(near_store, 2)}",
        ],
        (
            "Walk-by signal reflects nearby passing traffic, store interest reflects slowing or attention, "
            "and near-store signal reflects very close proximity to the storefront. Together, they describe "
            "how effectively traffic is being pulled inward."
        )
    )

story_row_2 = st.columns(1)
with story_row_2[0]:
    render_story_card(
        "Visit Story",
        "Did visitors stay and engage?",
        [
            f"Visits {fmt_int(store_visits)}",
            f"Qualified {fmt_int(qualified_visits)}",
            f"Engaged {fmt_int(engaged_visits)}",
        ],
        (
            f"Average dwell time was <b>{fmt_seconds(avg_dwell_seconds)}</b>. "
            "This shows whether visits were shallow pass-through events or more serious store interactions."
        )
    )

story_row_3 = st.columns(1)
with story_row_3[0]:
    render_story_card(
        "Commercial Story",
        "How much visit demand became sales?",
        [
            f"Transactions {fmt_int(transactions)}",
            f"Revenue {fmt_currency(value)}",
            f"Conversion {fmt_pct(sales_conversion)}",
        ],
        (
            "This is where store traffic, engagement quality, and commercial closure come together. "
            "Strong traffic but weak conversion usually points to floor execution or offer friction."
        )
    )

# =========================================================
# DIAGNOSTIC INDICES
# =========================================================
st.markdown("<div class='section-title'>Diagnostic Indices</div>", unsafe_allow_html=True)

diag_row_1 = st.columns(2)
with diag_row_1[0]:
    render_card(
        "Traffic Intelligence Index",
        f"{traffic_intelligence_index:.0f}",
        f"<span class='{badge_tii}'>{label_tii}</span><br>Overall traffic health score out of 100."
    )
with diag_row_1[1]:
    render_card(
        "Visit Quality Index",
        f"{visit_quality_index:.0f}",
        f"<span class='{badge_vqi}'>{label_vqi}</span><br>Quality of visits based on qualification, engagement, and dwell."
    )

diag_row_2 = st.columns(2)
with diag_row_2[0]:
    render_card(
        "Store Attraction Index",
        f"{store_attraction_index:.0f}",
        f"<span class='{badge_sai}'>{label_sai}</span><br>Ability of the storefront to convert pass-by traffic into entry."
    )
with diag_row_2[1]:
    render_card(
        "Audience Quality Index",
        f"{audience_quality_index:.0f}",
        f"<span class='{badge_aqi}'>{label_aqi}</span><br>Directional quality of audience/device environment."
    )

st.markdown(
    f"""
    <div class="panel note">
        <b>Benchmark maturity</b><br><br>
        <span class="{maturity_class}">{maturity_label}</span><br><br>
        {maturity_text}
    </div>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# MAIN TABS
# =========================================================
tab_funnels, tab_trend, tab_behaviour, tab_audience, tab_deep = st.tabs(
    ["Funnels", "Trend Intelligence", "Behaviour", "Audience", "Deep Diagnostics"]
)

with tab_funnels:
    st.markdown(
        "<div class='panel'><b>Store Journey Funnels</b><div class='note'>The first funnel explains attraction. The second explains visit quality and commercial conversion.</div></div>",
        unsafe_allow_html=True,
    )

    funnel_cols = st.columns(2)

    with funnel_cols[0]:
        signal_fig = go.Figure(go.Funnel(
            y=["Walk-by", "Interest", "Near Store"],
            x=[float(walk_by), float(interest), float(near_store)],
            texttemplate="%{value:.2f}",
            textposition="inside",
            opacity=0.92,
            marker={"color": [COLORS["slate"], COLORS["amber"], COLORS["indigo"]]},
            connector={"line": {"color": "rgba(99,102,241,0.25)", "width": 1.2}},
        ))
        signal_fig.update_layout(
            title="Traffic Capture Funnel",
            height=380,
            margin=dict(l=20, r=20, t=55, b=20),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(signal_fig, use_container_width=True, config=PLOT_CONFIG, key="traffic_capture_funnel")

    with funnel_cols[1]:
        visit_fig = go.Figure(go.Funnel(
            y=["Visits", "Qualified", "Engaged", "Sales"],
            x=[float(store_visits), float(qualified_visits), float(engaged_visits), float(transactions)],
            texttemplate=[
                f"{fmt_int(store_visits)}",
                f"{fmt_int(qualified_visits)} · {qualified_rate*100:.1f}%",
                f"{fmt_int(engaged_visits)} · {engaged_rate*100:.1f}%",
                f"{fmt_int(transactions)} · {sales_conversion*100:.1f}%",
            ],
            textposition="inside",
            opacity=0.92,
            marker={"color": [COLORS["sky"], COLORS["amber"], COLORS["violet"], COLORS["emerald"]]},
            connector={"line": {"color": "rgba(99,102,241,0.25)", "width": 1.2}},
        ))
        visit_fig.update_layout(
            title="Visit to Sale Funnel",
            height=380,
            margin=dict(l=20, r=20, t=55, b=20),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(visit_fig, use_container_width=True, config=PLOT_CONFIG, key="visit_to_sale_funnel")

with tab_trend:
    st.markdown(
        "<div class='panel'><b>Trend Intelligence</b><div class='note'>Daily view focuses on hourly shape. Longer ranges focus on selected-period trend plus weekday benchmarking.</div></div>",
        unsafe_allow_html=True,
    )

    if period_mode == "Daily":
        full_hours = pd.DataFrame({
            "hour_of_day": list(range(24)),
            "hour_label": [f"{h:02d}:00" for h in range(24)],
        })
        hourly_plot_df = full_hours.merge(hourly_df, on=["hour_of_day", "hour_label"], how="left")

        if not hourly_plot_df.empty:
            for col in [
                "avg_far_devices", "avg_mid_devices", "avg_near_devices",
                "avg_apple_devices", "avg_samsung_devices", "avg_other_devices"
            ]:
                if col in hourly_plot_df.columns:
                    hourly_plot_df[col] = pd.to_numeric(hourly_plot_df[col], errors="coerce").fillna(0)

            fig_hourly = go.Figure()
            fig_hourly.add_trace(go.Scatter(
                x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_far_devices"], mode="lines+markers", name="Walk-by"
            ))
            fig_hourly.add_trace(go.Scatter(
                x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_mid_devices"], mode="lines+markers", name="Interest"
            ))
            fig_hourly.add_trace(go.Scatter(
                x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_near_devices"], mode="lines+markers", name="Near Store"
            ))
            fig_hourly.update_layout(title="Hourly Traffic Signals")
            st.plotly_chart(style_chart(fig_hourly), use_container_width=True, config=PLOT_CONFIG, key="daily_hourly_traffic_signals")

            fig_hourly_brand = go.Figure()
            fig_hourly_brand.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_apple_devices"], name="Apple"))
            fig_hourly_brand.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_samsung_devices"], name="Samsung"))
            fig_hourly_brand.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_other_devices"], name="Other"))
            fig_hourly_brand.update_layout(title="Hourly Device Brand Mix", barmode="stack")
            st.plotly_chart(style_chart(fig_hourly_brand), use_container_width=True, config=PLOT_CONFIG, key="daily_hourly_device_mix")
    else:
        if not trend_df.empty:
            fig_visit_trend = go.Figure()
            fig_visit_trend.add_trace(go.Scatter(
                x=trend_df["period_label"], y=trend_df["store_visits"], name="Store Visits", mode="lines+markers"
            ))
            fig_visit_trend.add_trace(go.Scatter(
                x=trend_df["period_label"], y=trend_df["qualified_footfall"], name="Qualified Visits", mode="lines+markers"
            ))
            fig_visit_trend.add_trace(go.Scatter(
                x=trend_df["period_label"], y=trend_df["engaged_visits"], name="Engaged Visits", mode="lines+markers"
            ))
            fig_visit_trend.update_layout(title="Selected-Period Visit Trend")
            st.plotly_chart(style_chart(fig_visit_trend), use_container_width=True, config=PLOT_CONFIG, key="selected_period_visit_trend")

            fig_signal_trend = go.Figure()
            fig_signal_trend.add_trace(go.Bar(x=trend_df["period_label"], y=trend_df["walk_by_traffic"], name="Walk-by"))
            fig_signal_trend.add_trace(go.Bar(x=trend_df["period_label"], y=trend_df["store_interest"], name="Interest"))
            fig_signal_trend.add_trace(go.Bar(x=trend_df["period_label"], y=trend_df["near_store"], name="Near-store"))
            fig_signal_trend.update_layout(title="Selected-Period Traffic Trend", barmode="group")
            st.plotly_chart(style_chart(fig_signal_trend), use_container_width=True, config=PLOT_CONFIG, key="selected_period_signal_trend")

        if not weekday_df.empty:
            fig_weekday = go.Figure()
            fig_weekday.add_trace(go.Bar(x=weekday_df["weekday"], y=weekday_df["store_visits"], name="Store Visits"))
            fig_weekday.add_trace(go.Scatter(
                x=weekday_df["weekday"], y=weekday_df["engaged_visits"], name="Engaged Visits", mode="lines+markers", yaxis="y2"
            ))
            fig_weekday.update_layout(
                title="Weekday Performance Pattern",
                yaxis=dict(title="Visits"),
                yaxis2=dict(title="Engaged Visits", overlaying="y", side="right", showgrid=False),
            )
            st.plotly_chart(style_chart(fig_weekday), use_container_width=True, config=PLOT_CONFIG, key="weekday_performance_pattern")

with tab_behaviour:
    st.markdown(
        "<div class='panel'><b>Visitor Behaviour</b><div class='note'>Dwell time reveals how serious the visit was. Short stays usually indicate pass-through, while longer stays indicate stronger browsing or assisted engagement.</div></div>",
        unsafe_allow_html=True,
    )

    if not dwell_plot_df.empty:
        fig_dwell = px.bar(dwell_plot_df, x="dwell_bucket", y="visits", title="Dwell Time Distribution")
        st.plotly_chart(style_chart(fig_dwell), use_container_width=True, config=PLOT_CONFIG, key="dwell_time_distribution")

    behaviour_cols_1 = st.columns(1)
    with behaviour_cols_1[0]:
        render_card("Average Dwell", fmt_seconds(avg_dwell_seconds), "Average time visitors spent inside the store.")

    behaviour_cols_2 = st.columns(2)
    with behaviour_cols_2[0]:
        render_card(
            "Qualified Rate",
            fmt_pct(qualified_rate),
            "Visitors crossing minimum quality threshold.",
            f"Formula: {fmt_int(qualified_visits)} / {fmt_int(store_visits)}"
        )
    with behaviour_cols_2[1]:
        render_card(
            "Engaged Rate",
            fmt_pct(engaged_rate),
            "Visitors showing deeper engagement.",
            f"Formula: {fmt_int(engaged_visits)} / {fmt_int(store_visits)}"
        )

with tab_audience:
    st.markdown(
        "<div class='panel'><b>Audience & Device Signals</b><div class='note'>These are directional device signals around the store and are best used for pattern comparison, not deterministic demographic claims.</div></div>",
        unsafe_allow_html=True,
    )

    if period_mode == "Daily":
        full_hours = pd.DataFrame({
            "hour_of_day": list(range(24)),
            "hour_label": [f"{h:02d}:00" for h in range(24)],
        })
        hourly_plot_df = full_hours.merge(hourly_df, on=["hour_of_day", "hour_label"], how="left")
    else:
        hourly_plot_df = hourly_df.copy()

    if not hourly_plot_df.empty:
        for col in [
            "avg_far_devices", "avg_mid_devices", "avg_near_devices",
            "avg_apple_devices", "avg_samsung_devices", "avg_other_devices"
        ]:
            if col in hourly_plot_df.columns:
                hourly_plot_df[col] = pd.to_numeric(hourly_plot_df[col], errors="coerce").fillna(0)

        brand_fig = go.Figure()
        brand_fig.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_apple_devices"], name="Apple"))
        brand_fig.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_samsung_devices"], name="Samsung"))
        brand_fig.add_trace(go.Bar(x=hourly_plot_df["hour_label"], y=hourly_plot_df["avg_other_devices"], name="Other"))
        brand_fig.update_layout(title="Hourly Device Brand Mix", barmode="stack")
        st.plotly_chart(style_chart(brand_fig), use_container_width=True, config=PLOT_CONFIG, key="audience_hourly_device_brand_mix")

    audience_cols_1 = st.columns(1)
    with audience_cols_1[0]:
        render_card(
            "Average Estimated People",
            fmt_float(avg_estimated_people, 2),
            "Average nearby people signal detected around the store."
        )

    audience_cols_2 = st.columns(2)
    with audience_cols_2[0]:
        render_card(
            "Peak Estimated People",
            fmt_int(peak_estimated_people),
            "Highest nearby people signal observed in the selected period."
        )
    with audience_cols_2[1]:
        render_card(
            "Average Detected Devices",
            fmt_float(avg_detected_devices, 2),
            "Average mobile devices detected around the store."
        )

with tab_deep:
    st.markdown(
        "<div class='panel'><b>Deep Diagnostics</b><div class='note'>This section is for advanced users who want to inspect the score system and detailed math behind the dashboard.</div></div>",
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
                "Floor Conversion",
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
    )

    fig_index = px.bar(index_breakdown_df, x="Score", y="Metric", orientation="h", title="Index Breakdown")
    fig_index.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(style_chart(fig_index), use_container_width=True, config=PLOT_CONFIG, key="deep_diagnostics_index_breakdown")
    st.dataframe(index_breakdown_df, use_container_width=True, hide_index=True, key="deep_diagnostics_index_table")

    deep_cols_1 = st.columns(2)
    with deep_cols_1[0]:
        render_card(
            "Traffic-to-Visit Signal Index",
            fmt_float(daily_df["walkby_to_visit_index"].mean(), 2) if "walkby_to_visit_index" in daily_df.columns else "0.00",
            "Directional index showing how visit volume compares to surrounding traffic signal.",
            "Use as a relative store-capture indicator, not as a literal conversion rate."
        )
    with deep_cols_1[1]:
        render_card(
            "Average Detected Devices",
            fmt_float(avg_detected_devices, 2),
            "Average device signal strength around the store.",
            "Selected period average based on daily canonical metrics."
        )

    if not weekday_df.empty:
        weekday_table = weekday_df.copy()
        weekday_table["store_visits"] = weekday_table["store_visits"].round(0).astype(int)
        weekday_table["qualified_footfall"] = weekday_table["qualified_footfall"].round(0).astype(int)
        weekday_table["engaged_visits"] = weekday_table["engaged_visits"].round(0).astype(int)
        weekday_table["avg_dwell_seconds"] = weekday_table["avg_dwell_seconds"].round(1)
        st.dataframe(weekday_table, use_container_width=True, hide_index=True, key="weekday_diagnostics_table")

# =========================================================
# DEBUG SECTION
# =========================================================
if show_debug:
    st.markdown("### Timezone Diagnostics")
    st.markdown(
        "<div class='panel note'>Use this to verify the exact issue that caused IST rows to sit inside earlier S3 partitions. The dashboard now reads canonical IST views directly.</div>",
        unsafe_allow_html=True,
    )
    try:
        debug_df = load_debug_partition_vs_ist(selected_store, start_date_str, end_date_str)
        st.dataframe(debug_df, use_container_width=True, key="timezone_debug_table")
    except Exception as e:
        st.error(f"Failed to load timezone diagnostics: {e}")

# =========================================================
# FOOTER
# =========================================================
st.caption("Retail Intelligence Command Center · Retail Operations & Media Measurement · Powered by AWS Athena · Streamlit")
