import re
import time
from io import StringIO
from urllib.parse import urlparse

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from botocore.exceptions import ClientError
from google import genai

# ==========================================
# PAGE CONFIG
# ==========================================
st.set_page_config(
    page_title="nsTags | Retail Intelligence",
    page_icon="📈",
    layout="wide",
)

# ==========================================
# PREMIUM UI
# ==========================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"]  { font-family: 'Inter', sans-serif; background-color: #f8f9fa; }

@keyframes slideUpFade {
    from { opacity: 0; transform: translateY(12px); }
    to { opacity: 1; transform: translateY(0); }
}
.animate-container { animation: slideUpFade 0.55s cubic-bezier(0.16, 1, 0.3, 1) forwards; }

.hero-shell {
    background: linear-gradient(135deg, rgba(26,115,232,0.10) 0%, rgba(52,168,83,0.05) 100%);
    border: 1px solid rgba(26,115,232,0.18);
    border-radius: 18px;
    padding: 1.2rem 1.4rem;
    margin-bottom: 1rem;
}
.hero-title { font-size: 1.8rem; font-weight: 800; margin-bottom: 0.2rem; color: #1f1f1f; }
.hero-sub { color: #5f6368; font-size: 0.98rem; margin-bottom: 0; }

.kpi-card {
    background: white; border: 1px solid rgba(95,99,104,0.12); border-radius: 16px;
    padding: 1rem 1rem 0.9rem 1rem; box-shadow: 0 2px 10px rgba(0,0,0,0.03); height: 100%;
}
.kpi-label {
    font-size: 0.78rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.08em; color: #5f6368; margin-bottom: 0.45rem;
}
.kpi-value { font-size: 2rem; font-weight: 800; color: #1f1f1f; line-height: 1.1; }
.kpi-sub { font-size: 0.9rem; color: #5f6368; margin-top: 0.3rem; }

.verdict-good { color: #188038; font-weight: 700; }
.verdict-warn { color: #b06000; font-weight: 700; }
.verdict-bad  { color: #d93025; font-weight: 700; }

.insight-card {
    background: white; border: 1px solid rgba(95,99,104,0.12); border-left: 5px solid #1a73e8;
    border-radius: 16px; padding: 1rem 1.1rem; box-shadow: 0 2px 10px rgba(0,0,0,0.03); min-height: 220px;
}
.insight-title {
    font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.08em;
    color: #5f6368; font-weight: 800; margin-bottom: 0.55rem;
}
.insight-headline {
    font-size: 1.35rem; font-weight: 800; color: #1f1f1f; line-height: 1.2; margin-bottom: 0.7rem;
}
.insight-body { font-size: 0.96rem; color: #3c4043; line-height: 1.55; }
.mono-box {
    background: rgba(95,99,104,0.06); border-radius: 10px; padding: 0.7rem 0.8rem;
    margin-top: 0.8rem; font-family: 'Courier New', monospace; font-size: 0.82rem; color: #3c4043;
}
.section-title { font-size: 1.15rem; font-weight: 800; color: #1f1f1f; margin-top: 0.4rem; margin-bottom: 0.8rem; }

div[data-testid="stInfo"] {
    background: linear-gradient(145deg, #f8fafd 0%, #ffffff 100%);
    border: 1px solid rgba(26, 115, 232, 0.18);
    border-left: 6px solid #1a73e8;
    border-radius: 14px;
    padding: 1rem 1.3rem;
    box-shadow: 0 4px 14px rgba(0,0,0,0.04);
}

.small-note {
    font-size: 0.82rem;
    color: #6b7280;
    margin-top: -0.3rem;
    margin-bottom: 0.8rem;
}
</style>
""", unsafe_allow_html=True)

PLOT_CONFIG = {"displayModeBar": False}

# ==========================================
# CONFIG
# ==========================================
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

# ==========================================
# HELPERS
# ==========================================
def safe_div(a, b):
    return a / b if b not in [0, None] else 0

def pct(a, b):
    return safe_div(a, b) * 100

def fmt_int(x):
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "0"

def fmt_currency(x):
    try:
        return f"₹{float(x):,.0f}"
    except Exception:
        return "₹0"

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

def verdict_class(value, good_threshold, warn_threshold, higher_is_better=True):
    if higher_is_better:
        if value >= good_threshold:
            return "verdict-good", "Healthy"
        elif value >= warn_threshold:
            return "verdict-warn", "Watch"
        return "verdict-bad", "Weak"
    else:
        if value <= good_threshold:
            return "verdict-good", "Efficient"
        elif value <= warn_threshold:
            return "verdict-warn", "Monitor"
        return "verdict-bad", "Expensive"

def validate_store_id(store_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", str(store_id)):
        raise ValueError("Invalid store_id")
    return store_id

def validate_date_part(value: str, field_name: str) -> str:
    if not re.fullmatch(r"\d{1,4}", str(value)):
        raise ValueError(f"Invalid {field_name}")
    return value

def s3_uri_to_bucket_key(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip("/")

def style_chart(fig):
    fig.update_layout(
        hovermode="x unified",
        margin=dict(l=10, r=10, t=35, b=50),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="top", y=-0.18, xanchor="center", x=0.5),
        font=dict(family="Inter, sans-serif", color="#5f6368"),
    )
    fig.update_xaxes(showgrid=False, zeroline=False, title_text="")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(95,99,104,0.10)", zeroline=False, title_text="")
    return fig

def prepare_dwell_plot_df(source_df: pd.DataFrame) -> pd.DataFrame:
    dwell_order = ["00-10s", "10-30s", "30-60s", "01-03m", "03-05m", "05m+"]
    plot_df = source_df.copy()
    plot_df["dwell_bucket"] = pd.Categorical(
        plot_df["dwell_bucket"], categories=dwell_order, ordered=True
    )
    return plot_df.sort_values("dwell_bucket")

def to_100_scale(value, cap=1.0):
    try:
        value = float(value)
    except Exception:
        return 0
    return max(0, min((value / cap) * 100, 100))

def detect_primary_bottleneck(score_row, transactions, store_visits):
    floor_conversion_strength = safe_div(transactions, store_visits)

    scores = {
        "Store Magnet": float(score_row.get("store_magnet_score", 0)),
        "Window Capture": min(float(score_row.get("window_capture_index", 0)) / 8.0, 1.0),
        "Entry Efficiency": float(score_row.get("entry_efficiency_score", 0)),
        "Dwell Quality": float(score_row.get("dwell_quality_index", 0)),
        "Floor Conversion": floor_conversion_strength,
    }

    primary = min(scores, key=scores.get)
    return primary, scores

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
    return clamp_0_100((value / cap) * 100)

def normalize_index_to_100(value, cap):
    try:
        value = float(value)
        cap = float(cap)
    except Exception:
        return 0.0
    if cap <= 0:
        return 0.0
    return clamp_0_100((value / cap) * 100)

def weighted_score(parts):
    total_weight = sum(weight for _, weight in parts if weight > 0)
    if total_weight <= 0:
        return 0.0
    weighted_sum = sum(score * weight for score, weight in parts if weight > 0)
    return round(weighted_sum / total_weight, 1)

def score_band(score: float):
    score = float(score)
    if score >= 75:
        return "verdict-good", "Strong"
    elif score >= 50:
        return "verdict-warn", "Moderate"
    return "verdict-bad", "Weak"

def compute_local_fallback_indices(
    walk_by_traffic,
    store_interest,
    near_store,
    store_visits,
    qualified_visits,
    engaged_visits,
    avg_dwell_seconds,
    score_row,
    brand_df=None,
):
    walk_by_score = normalize_index_to_100(walk_by_traffic, cap=20.0)
    interest_score = normalize_index_to_100(store_interest, cap=12.0)
    near_store_score = normalize_index_to_100(near_store, cap=8.0)

    qualified_rate = safe_div(qualified_visits, store_visits)
    engaged_rate = safe_div(engaged_visits, store_visits)

    qualified_score = normalize_ratio_to_100(qualified_rate, cap=0.60)
    engaged_score = normalize_ratio_to_100(engaged_rate, cap=0.40)
    dwell_score = normalize_ratio_to_100(avg_dwell_seconds, cap=180.0)

    visit_quality_score = weighted_score([
        (qualified_score, 0.45),
        (engaged_score, 0.35),
        (dwell_score, 0.20),
    ])

    store_magnet_ratio = safe_div(store_interest, walk_by_traffic)
    window_capture_ratio = safe_div(store_visits, store_interest)
    entry_efficiency_ratio = safe_div(qualified_visits, store_visits)

    store_magnet_score = normalize_ratio_to_100(store_magnet_ratio, cap=0.60)
    window_capture_score = normalize_ratio_to_100(window_capture_ratio, cap=8.0)
    entry_efficiency_score = normalize_ratio_to_100(entry_efficiency_ratio, cap=0.70)

    if score_row:
        dwell_quality_score = normalize_ratio_to_100(float(score_row.get("dwell_quality_index", 0)), cap=1.0)
    else:
        dwell_quality_score = visit_quality_score

    premium_device_mix_score = 50.0
    if brand_df is not None and not brand_df.empty:
        apple = float(brand_df["avg_apple_devices"].sum())
        samsung = float(brand_df["avg_samsung_devices"].sum())
        other = float(brand_df["avg_other_devices"].sum())
        total = apple + samsung + other
        apple_share = safe_div(apple, total)
        samsung_share = safe_div(samsung, total)
        premium_share = (apple_share * 1.0) + (samsung_share * 0.7)
        premium_device_mix_score = normalize_ratio_to_100(premium_share, cap=0.75)

    tii = weighted_score([
        (walk_by_score, 0.20),
        (interest_score, 0.20),
        (near_store_score, 0.15),
        (visit_quality_score, 0.25),
        (dwell_quality_score, 0.20),
    ])

    vqi = weighted_score([
        (qualified_score, 0.45),
        (engaged_score, 0.35),
        (dwell_score, 0.20),
    ])

    sai = weighted_score([
        (store_magnet_score, 0.40),
        (window_capture_score, 0.35),
        (entry_efficiency_score, 0.25),
    ])

    aqi = weighted_score([
        (premium_device_mix_score, 0.60),
        (engaged_score, 0.40),
    ])

    return {
        "traffic_intelligence_index": round(tii, 1),
        "visit_quality_index": round(vqi, 1),
        "store_attraction_index": round(sai, 1),
        "audience_quality_index": round(aqi, 1),
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
        "is_fallback": True,
    }

# ==========================================
# ATHENA
# ==========================================
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

# ==========================================
# DATA LOADERS
# ==========================================
@st.cache_data(ttl=300)
def load_store_list() -> pd.DataFrame:
    query = "SELECT DISTINCT store_id FROM nstags_dashboard_metrics ORDER BY store_id"
    return run_athena_query(query)

@st.cache_data(ttl=300)
def load_available_dates(store_id: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    query = f"""
    SELECT DISTINCT year, month, day
    FROM nstags_dashboard_metrics
    WHERE store_id = '{sid}'
    ORDER BY year DESC, month DESC, day DESC
    """
    return run_athena_query(query)

@st.cache_data(ttl=300)
def load_dashboard_metrics(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    y = validate_date_part(year, "year")
    m = validate_date_part(month, "month")
    d = validate_date_part(day, "day")
    query = f"""
    SELECT *
    FROM nstags_dashboard_metrics
    WHERE store_id = '{sid}'
      AND year = '{y}'
      AND month = '{m}'
      AND day = '{d}'
    """
    return run_athena_query(query)

@st.cache_data(ttl=300)
def load_hourly_traffic(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    y = validate_date_part(year, "year")
    m = validate_date_part(month, "month")
    d = validate_date_part(day, "day")
    query = f"""
    SELECT *
    FROM nstags_hourly_traffic_pretty
    WHERE store_id = '{sid}'
      AND year = '{y}'
      AND month = '{m}'
      AND day = '{d}'
    ORDER BY hour_of_day
    """
    return run_athena_query(query)

@st.cache_data(ttl=300)
def load_conversion_hourly(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    y = validate_date_part(year, "year")
    m = validate_date_part(month, "month")
    d = validate_date_part(day, "day")
    query = f"""
    SELECT *
    FROM nstags_conversion_hourly
    WHERE store_id = '{sid}'
      AND year = '{y}'
      AND month = '{m}'
      AND day = '{d}'
    ORDER BY hour_of_day
    """
    return run_athena_query(query)

@st.cache_data(ttl=300)
def load_dwell_buckets(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    y = validate_date_part(year, "year")
    m = validate_date_part(month, "month")
    d = validate_date_part(day, "day")
    query = f"""
    SELECT *
    FROM nstags_dwell_buckets
    WHERE store_id = '{sid}'
      AND year = '{y}'
      AND month = '{m}'
      AND day = '{d}'
    """
    return run_athena_query(query)

@st.cache_data(ttl=300)
def load_brand_mix_hourly(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    y = validate_date_part(year, "year")
    m = validate_date_part(month, "month")
    d = validate_date_part(day, "day")

    query = f"""
    SELECT
        hour(from_unixtime(ts) AT TIME ZONE 'Asia/Kolkata') AS hour_of_day,
        format('%02d:00', hour(from_unixtime(ts) AT TIME ZONE 'Asia/Kolkata')) AS hour_label,
        round(avg(apple_devices), 2) AS avg_apple_devices,
        round(avg(samsung_devices), 2) AS avg_samsung_devices,
        round(avg(other_devices), 2) AS avg_other_devices
    FROM nstags_live_analytics
    WHERE store_id = '{sid}'
      AND year = '{y}'
      AND month = '{m}'
      AND day = '{d}'
    GROUP BY hour(from_unixtime(ts) AT TIME ZONE 'Asia/Kolkata')
    ORDER BY hour_of_day
    """
    return run_athena_query(query)

@st.cache_data(ttl=300)
def load_intelligence_scores(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    y = validate_date_part(year, "year")
    m = validate_date_part(month, "month")
    d = validate_date_part(day, "day")

    query = f"""
    SELECT *
    FROM nstags_intelligence_scores
    WHERE store_id = '{sid}'
      AND year = '{y}'
      AND month = '{m}'
      AND day = '{d}'
    """
    return run_athena_query(query)

@st.cache_data(ttl=300)
def load_dynamic_index_scores(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    y = validate_date_part(year, "year")
    m = validate_date_part(month, "month")
    d = validate_date_part(day, "day")
    query = f"""
    SELECT *
    FROM nstags_index_scores_dynamic
    WHERE store_id = '{sid}'
      AND year = '{y}'
      AND month = '{m}'
      AND day = '{d}'
    """
    return run_athena_query(query)

# ==========================================
# AI
# ==========================================
@st.cache_data(ttl=300)
def generate_ai_brief(ai_payload):
    if not GEMINI_API_KEY:
        return "⚠️ **AI unavailable:** GEMINI_API_KEY not configured in Secrets."

    try:
        client = genai.Client(api_key=GEMINI_API_KEY)

        prompt = f"""
You are a retail analytics strategy consultant.

Analyze these metrics carefully. Only use the numbers below. Do not invent any extra data.

Mode: {ai_payload['mode']}

LIVE TRAFFIC INTENSITY
- Walk-by traffic index: {ai_payload['walk_by']}
- Store interest index: {ai_payload['interest']}
- Near-store index: {ai_payload['near_store']}

VISIT METRICS
- Store visits: {ai_payload['visits']}
- Qualified visits: {ai_payload['qualified_visits']}
- Engaged visits: {ai_payload['engaged_visits']}
- Qualified visit rate: {ai_payload['qualified_rate']}%
- Engaged visit rate: {ai_payload['engaged_rate']}%
- Average dwell: {ai_payload['avg_dwell']}

COMMERCIAL INPUTS
- Transactions: {ai_payload['transactions']}
- Revenue or campaign value: {ai_payload['value']}
- Sales conversion from visits: {ai_payload['sales_conversion']}%

INDEX LAYER
- Traffic Intelligence Index: {ai_payload['tii']}
- Visit Quality Index: {ai_payload['vqi']}
- Store Attraction Index: {ai_payload['sai']}
- Audience Quality Index: {ai_payload['aqi']}

Write an executive brief in Markdown with exactly this structure:
* **What happened:** [1 sentence]
* **What the traffic says:** [Interpret walk-by / interest / near-store correctly as traffic intensity, not literal counts]
* **What the visits say:** [Interpret visit quality and dwell]
* **What the index says:** [Interpret TII / VQI / SAI briefly]
* **Primary bottleneck:** [Choose one clear issue]
* **Recommended action:** [One concrete action]
"""
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )
        return response.text
    except Exception:
        return "⚠️ **AI unavailable:** connection or rate-limit issue."

# ==========================================
# APP HEADER
# ==========================================
st.markdown("""
<div class="hero-shell">
    <div class="hero-title">nsTags Intelligence</div>
    <p class="hero-sub">Retail Intelligence • DOOH Media Measurement • Conversion Diagnostics</p>
</div>
""", unsafe_allow_html=True)

# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("### Control Center")
    app_mode = st.radio("Business Mode", ["Retail Ops", "Retail Media"])

    try:
        stores_df = load_store_list()
    except Exception as e:
        st.error(f"Failed to load store list: {e}")
        st.stop()

    if stores_df.empty:
        st.warning("No stores found.")
        st.stop()

    selected_store = st.selectbox("Active Store", stores_df["store_id"].dropna().astype(str).tolist())

    try:
        dates_df = load_available_dates(selected_store)
    except Exception as e:
        st.error(f"Failed to load available dates: {e}")
        st.stop()

    if dates_df.empty:
        st.warning("No dates found.")
        st.stop()

    dates_df["date_str"] = (
        dates_df["year"].astype(str)
        + "-"
        + dates_df["month"].astype(str).str.zfill(2)
        + "-"
        + dates_df["day"].astype(str).str.zfill(2)
    )
    selected_date = st.selectbox("Date", dates_df["date_str"].tolist())
    selected_year, selected_month, selected_day = selected_date.split("-")

    st.markdown("### Commercial Inputs")
    if app_mode == "Retail Media":
        campaign_value = st.number_input("Campaign Revenue (₹)", min_value=0, value=15000, step=1000)
        transactions = st.number_input("Attributed Sales", min_value=0, value=12, step=1)
        daily_revenue = 0
        marketing_spend = 0
    else:
        marketing_spend = st.number_input("Marketing Spend (₹)", min_value=0, value=5000, step=500)
        daily_revenue = st.number_input("Store Revenue (₹)", min_value=0, value=45000, step=1000)
        transactions = st.number_input("Store Transactions", min_value=0, value=35, step=1)
        campaign_value = 0

    ai_enabled = st.checkbox("Enable AI executive brief", value=True)

# ==========================================
# LOAD DATA
# ==========================================
try:
    dashboard_df = load_dashboard_metrics(selected_store, selected_year, selected_month, selected_day)
    hourly_df = load_hourly_traffic(selected_store, selected_year, selected_month, selected_day)
    conversion_df = load_conversion_hourly(selected_store, selected_year, selected_month, selected_day)
    dwell_df = load_dwell_buckets(selected_store, selected_year, selected_month, selected_day)
    brand_df = load_brand_mix_hourly(selected_store, selected_year, selected_month, selected_day)
    scores_df = load_intelligence_scores(selected_store, selected_year, selected_month, selected_day)
except Exception as e:
    st.error(f"Failed to load Athena data: {e}")
    st.stop()

if dashboard_df.empty:
    st.warning("No metrics found for selected date.")
    st.stop()

dash = dashboard_df.iloc[0].to_dict()
score_row = scores_df.iloc[0].to_dict() if not scores_df.empty else {}

# ==========================================
# METRIC LAYER
# ==========================================
walk_by_traffic = float(dash.get("walk_by_traffic", 0))
store_interest = float(dash.get("store_interest", 0))
near_store = float(dash.get("near_store", 0))

store_visits = float(dash.get("store_visits", 0))
qualified_visits = float(dash.get("qualified_footfall", 0))
engaged_visits = float(dash.get("engaged_visits", 0))
avg_dwell_seconds = float(dash.get("avg_dwell_seconds", 0))
median_dwell_seconds = float(dash.get("median_dwell_seconds", 0))

sales_conversion = safe_div(transactions, store_visits)
qualified_visit_rate = safe_div(qualified_visits, store_visits)
engaged_visit_rate = safe_div(engaged_visits, store_visits)

if app_mode == "Retail Media":
    cost_per_engaged = safe_div(campaign_value, engaged_visits)
    cost_per_visit = safe_div(campaign_value, store_visits)
    effective_cpm_est = safe_div(campaign_value, walk_by_traffic) * 1000 if walk_by_traffic else 0
else:
    aov = safe_div(daily_revenue, transactions)
    indicative_attributed_revenue = store_visits * sales_conversion * aov
    roas = safe_div(indicative_attributed_revenue, marketing_spend)
    cost_per_visit = safe_div(marketing_spend, store_visits)

qual_class, qual_verdict = verdict_class(qualified_visit_rate * 100, 50, 30)
eng_class, eng_verdict = verdict_class(engaged_visit_rate * 100, 40, 20)
conv_class, conv_verdict = verdict_class(sales_conversion * 100, 20, 10)

# ==========================================
# INDEX LAYER
# ==========================================
index_source_note = "Dynamic Athena percentile scoring"
try:
    index_df = load_dynamic_index_scores(selected_store, selected_year, selected_month, selected_day)
except Exception:
    index_df = pd.DataFrame()

if not index_df.empty:
    idx = index_df.iloc[0].to_dict()
    tii = float(idx.get("traffic_intelligence_index", 0))
    vqi = float(idx.get("visit_quality_index", 0))
    sai = float(idx.get("store_attraction_index", 0))
    aqi = float(idx.get("audience_quality_index", 0))
    index_scores = idx
    index_scores["is_fallback"] = False
else:
    index_scores = compute_local_fallback_indices(
        walk_by_traffic=walk_by_traffic,
        store_interest=store_interest,
        near_store=near_store,
        store_visits=store_visits,
        qualified_visits=qualified_visits,
        engaged_visits=engaged_visits,
        avg_dwell_seconds=avg_dwell_seconds,
        score_row=score_row,
        brand_df=brand_df,
    )
    tii = float(index_scores["traffic_intelligence_index"])
    vqi = float(index_scores["visit_quality_index"])
    sai = float(index_scores["store_attraction_index"])
    aqi = float(index_scores["audience_quality_index"])
    index_source_note = "Local fallback scoring (Athena dynamic view not available)"

tii_class, tii_verdict = score_band(tii)
vqi_class, vqi_verdict = score_band(vqi)
sai_class, sai_verdict = score_band(sai)
aqi_class, aqi_verdict = score_band(aqi)

# ==========================================
# AI
# ==========================================
if ai_enabled:
    payload = {
        "mode": app_mode,
        "walk_by": round(walk_by_traffic, 2),
        "interest": round(store_interest, 2),
        "near_store": round(near_store, 2),
        "visits": int(store_visits),
        "qualified_visits": int(qualified_visits),
        "engaged_visits": int(engaged_visits),
        "qualified_rate": round(qualified_visit_rate * 100, 1),
        "engaged_rate": round(engaged_visit_rate * 100, 1),
        "avg_dwell": fmt_seconds(avg_dwell_seconds),
        "transactions": int(transactions),
        "value": fmt_currency(daily_revenue if app_mode == "Retail Ops" else campaign_value),
        "sales_conversion": round(sales_conversion * 100, 1),
        "tii": round(tii, 1),
        "vqi": round(vqi, 1),
        "sai": round(sai, 1),
        "aqi": round(aqi, 1),
    }

    with st.spinner("Analyzing Athena metrics..."):
        ai_text = generate_ai_brief(payload)
    st.info(ai_text, icon="✨")

st.markdown("<div class='animate-container'>", unsafe_allow_html=True)

# ==========================================
# INDEX RAIL
# ==========================================
st.markdown("<div class='section-title'>nsTags Index Rail</div>", unsafe_allow_html=True)
i1, i2, i3, i4 = st.columns(4)

with i1:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Traffic Intelligence Index</div>
            <div class="kpi-value">{tii:.0f}</div>
            <div class="kpi-sub"><span class="{tii_class}">{tii_verdict}</span> overall store traffic health</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with i2:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Visit Quality Index</div>
            <div class="kpi-value">{vqi:.0f}</div>
            <div class="kpi-sub"><span class="{vqi_class}">{vqi_verdict}</span> qualified + engaged + dwell</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with i3:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Store Attraction Index</div>
            <div class="kpi-value">{sai:.0f}</div>
            <div class="kpi-sub"><span class="{sai_class}">{sai_verdict}</span> pass-by to interest to entry</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with i4:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Audience Quality Index</div>
            <div class="kpi-value">{aqi:.0f}</div>
            <div class="kpi-sub"><span class="{aqi_class}">{aqi_verdict}</span> device-mix engagement proxy</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown(
    f"""
    <div class="small-note">
    Index scores are internal normalized indicators built on traffic, visit quality, dwell, and device-mix signals.
    Raw metrics are shown below for validation and operational analysis. Source: {index_source_note}.
    </div>
    """,
    unsafe_allow_html=True,
)

# ==========================================
# KPI RAIL
# ==========================================
st.markdown("<div class='section-title'>Executive KPI Rail</div>", unsafe_allow_html=True)
k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Walk-By Traffic</div>
            <div class="kpi-value">{walk_by_traffic:.2f}</div>
            <div class="kpi-sub">Live traffic intensity index</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with k2:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Store Interest</div>
            <div class="kpi-value">{store_interest:.2f}</div>
            <div class="kpi-sub">Mid-zone attention intensity</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with k3:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Store Visits</div>
            <div class="kpi-value">{fmt_int(store_visits)}</div>
            <div class="kpi-sub">Session-based visit count</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with k4:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Qualified Visit Rate</div>
            <div class="kpi-value">{qualified_visit_rate*100:.1f}%</div>
            <div class="kpi-sub"><span class="{qual_class}">{qual_verdict}</span> of visits ≥ 30s</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with k5:
    if app_mode == "Retail Media":
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-label">Cost per Engaged</div>
                <div class="kpi-value">{fmt_currency(cost_per_engaged)}</div>
                <div class="kpi-sub">Campaign value / engaged visits</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-label">Sales Conversion</div>
                <div class="kpi-value">{sales_conversion*100:.1f}%</div>
                <div class="kpi-sub"><span class="{conv_class}">{conv_verdict}</span> transactions / visits</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ==========================================
# INTELLIGENCE SCORES
# ==========================================
st.markdown("<div class='section-title'>nsTags Intelligence Scores</div>", unsafe_allow_html=True)

if score_row:
    floor_conversion_strength = safe_div(transactions, store_visits)

    store_magnet_100 = to_100_scale(score_row.get("store_magnet_score", 0), cap=0.60)
    window_capture_100 = to_100_scale(score_row.get("window_capture_index", 0), cap=8.0)
    entry_efficiency_100 = to_100_scale(score_row.get("entry_efficiency_score", 0), cap=0.70)
    dwell_quality_100 = to_100_scale(score_row.get("dwell_quality_index", 0), cap=1.0)
    floor_conversion_100 = to_100_scale(floor_conversion_strength, cap=0.25)

    s1, s2, s3, s4, s5 = st.columns(5)

    def render_score_card(col, label, score, subtitle):
        col.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value">{score:.0f}</div>
                <div class="kpi-sub">{subtitle}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    render_score_card(s1, "Store Magnet", store_magnet_100, "Are people stopping?")
    render_score_card(s2, "Window Capture", window_capture_100, "Are interested people entering?")
    render_score_card(s3, "Entry Efficiency", entry_efficiency_100, "Are visits meaningful?")
    render_score_card(s4, "Dwell Quality", dwell_quality_100, "Are visitors truly engaging?")
    render_score_card(s5, "Floor Conversion", floor_conversion_100, "Is store demand converting?")

    bottleneck, score_map = detect_primary_bottleneck(score_row, transactions, store_visits)

    bottleneck_message = {
        "Store Magnet": "Primary issue appears to be storefront attraction. People are walking by but not sufficiently slowing down.",
        "Window Capture": "Primary issue appears to be converting attention into entry. Window/storefront is noticed, but entry is weak.",
        "Entry Efficiency": "Primary issue appears to be visit quality. Many visits are too shallow to qualify.",
        "Dwell Quality": "Primary issue appears to be in-store engagement. Visitors enter but do not stay meaningfully.",
        "Floor Conversion": "Primary issue appears to be on-floor conversion. Store traffic exists, but sales closure is weak.",
    }

    st.markdown(
        f"""
        <div class="insight-card" style="margin-top: 1rem;">
            <div class="insight-title">Primary Bottleneck</div>
            <div class="insight-headline">{bottleneck}</div>
            <div class="insight-body">{bottleneck_message[bottleneck]}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ==========================================
# DIAGNOSTICS
# ==========================================
st.markdown("<div class='section-title'>Diagnostic Breakdown</div>", unsafe_allow_html=True)
d1, d2, d3 = st.columns(3)

with d1:
    st.markdown(
        f"""
        <div class="insight-card">
            <div class="insight-title">Live Traffic</div>
            <div class="insight-headline">{walk_by_traffic:.2f} walk-by vs {store_interest:.2f} interest</div>
            <div class="insight-body">
                Live Bluetooth traffic indicates a <b>{store_interest:.2f}</b> store-interest intensity
                against <b>{walk_by_traffic:.2f}</b> walk-by intensity.
            </div>
            <div class="mono-box">Traffic metrics are live intensity indices, not literal people totals.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with d2:
    st.markdown(
        f"""
        <div class="insight-card" style="border-left-color:#fbbc04;">
            <div class="insight-title">Visit Quality</div>
            <div class="insight-headline">{qualified_visit_rate*100:.1f}% qualified, {engaged_visit_rate*100:.1f}% engaged</div>
            <div class="insight-body">
                Out of <b>{fmt_int(store_visits)}</b> visits, <b>{fmt_int(qualified_visits)}</b> were qualified
                and <b>{fmt_int(engaged_visits)}</b> were engaged.
            </div>
            <div class="mono-box">Qualified = visits ≥ 30s • Engaged = visits ≥ 60s</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with d3:
    if app_mode == "Retail Media":
        st.markdown(
            f"""
            <div class="insight-card" style="border-left-color:#34a853;">
                <div class="insight-title">Commercial Efficiency</div>
                <div class="insight-headline">{fmt_currency(cost_per_visit)} per visit</div>
                <div class="insight-body">
                    Campaign value distributed across <b>{fmt_int(store_visits)}</b> visits and
                    <b>{fmt_int(engaged_visits)}</b> engaged visits.
                </div>
                <div class="mono-box">Estimated CPM shown elsewhere is intensity-based, not audited media billing.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        leakage = max(0, int(round(store_visits - transactions)))
        st.markdown(
            f"""
            <div class="insight-card" style="border-left-color:#34a853;">
                <div class="insight-title">Floor Conversion</div>
                <div class="insight-headline">{sales_conversion*100:.1f}% visit-to-sale</div>
                <div class="insight-body">
                    <b>{fmt_int(leakage)}</b> visits did not convert into transactions.
                    This may indicate floor execution, product fit, or merchandising issues.
                </div>
                <div class="mono-box">This uses manual transaction input against session-based visit counts.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ==========================================
# TABS
# ==========================================
tab0, tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Index Breakdown",
    "🎯 Visits Funnel",
    "🚦 Traffic Trends",
    "⏱️ Dwell",
    "📱 Audience Mix",
])

with tab0:
    st.markdown("<div class='section-title'>Index Breakdown</div>", unsafe_allow_html=True)

    breakdown_df = pd.DataFrame({
        "Component": [
            "Walk-By Score",
            "Interest Score",
            "Near-Store Score",
            "Qualified Visit Score",
            "Engaged Visit Score",
            "Dwell Score",
            "Store Magnet Score",
            "Window Capture Score",
            "Entry Efficiency Score",
            "Premium Device Mix Score",
        ],
        "Score": [
            float(index_scores.get("walk_by_score", 0)),
            float(index_scores.get("interest_score", 0)),
            float(index_scores.get("near_store_score", 0)),
            float(index_scores.get("qualified_score", 0)),
            float(index_scores.get("engaged_score", 0)),
            float(index_scores.get("dwell_score", 0)),
            float(index_scores.get("store_magnet_percentile_score", 0)),
            float(index_scores.get("window_capture_score", 0)),
            float(index_scores.get("entry_efficiency_percentile_score", 0)),
            float(index_scores.get("premium_device_mix_score", 0)),
        ]
    })

    fig_index = px.bar(
        breakdown_df,
        x="Component",
        y="Score",
        text_auto=".0f",
        color="Score",
        color_continuous_scale="Blues",
    )
    fig_index.update_layout(coloraxis_showscale=False)
    fig_index.update_xaxes(tickangle=-30)
    st.plotly_chart(style_chart(fig_index), width="stretch", config=PLOT_CONFIG)

    c1, c2 = st.columns(2)

    with c1:
        st.markdown(
            f"""
            <div class="insight-card">
                <div class="insight-title">Primary Summary</div>
                <div class="insight-headline">TII {tii:.0f} / VQI {vqi:.0f}</div>
                <div class="insight-body">
                    Traffic Intelligence Index summarizes overall store traffic health, while Visit Quality Index shows
                    how meaningful those visits were once people came close or entered.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c2:
        st.markdown(
            f"""
            <div class="insight-card" style="border-left-color:#34a853;">
                <div class="insight-title">Commercial Interpretation</div>
                <div class="insight-headline">SAI {sai:.0f} / AQI {aqi:.0f}</div>
                <div class="insight-body">
                    Store Attraction Index reflects how well traffic converts into store approach and entry.
                    Audience Quality Index is a device-profile proxy and should be used directionally, not as a demographic fact.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

with tab1:
    st.markdown("<div class='section-title'>Session-Based Visits Funnel</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='small-note'>This funnel uses only visit/session metrics, so all stages are on the same basis.</div>",
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div style="display: flex; flex-wrap: wrap; text-align: center; background: white; padding: 1.5rem; border-radius: 12px; border: 1px solid rgba(128,134,139,0.2); margin-bottom: 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.02);">
            <div style="flex: 1; border-right: 1px solid rgba(128,134,139,0.2);">
                <div style="font-size: 0.75rem; font-weight: 700; color: #1a73e8; text-transform: uppercase;">1. Visits</div>
                <div style="font-size: 1.8rem; font-weight: 800; color: #1f1f1f;">{fmt_int(store_visits)}</div>
                <div style="font-size: 0.85rem; color: #80868b;">Session visits</div>
            </div>
            <div style="flex: 1; border-right: 1px solid rgba(128,134,139,0.2);">
                <div style="font-size: 0.75rem; font-weight: 700; color: #fbbc04; text-transform: uppercase;">2. Qualified</div>
                <div style="font-size: 1.8rem; font-weight: 800; color: #1f1f1f;">{fmt_int(qualified_visits)}</div>
                <div style="font-size: 0.85rem; color: #80868b;"><b>{qualified_visit_rate*100:.1f}%</b> of visits</div>
            </div>
            <div style="flex: 1; border-right: 1px solid rgba(128,134,139,0.2);">
                <div style="font-size: 0.75rem; font-weight: 700; color: #8e24aa; text-transform: uppercase;">3. Engaged</div>
                <div style="font-size: 1.8rem; font-weight: 800; color: #1f1f1f;">{fmt_int(engaged_visits)}</div>
                <div style="font-size: 0.85rem; color: #80868b;"><b>{engaged_visit_rate*100:.1f}%</b> of visits</div>
            </div>
            <div style="flex: 1;">
                <div style="font-size: 0.75rem; font-weight: 700; color: #34a853; text-transform: uppercase;">4. Transactions</div>
                <div style="font-size: 1.8rem; font-weight: 800; color: #1f1f1f;">{fmt_int(transactions)}</div>
                <div style="font-size: 0.85rem; color: #80868b;"><b>{sales_conversion*100:.1f}%</b> of visits</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    fig_funnel = go.Figure(
        go.Funnel(
            y=[
                "<b>Visits</b><br>Session Count",
                "<b>Qualified</b><br>≥ 30s",
                "<b>Engaged</b><br>≥ 60s",
                "<b>Transactions</b><br>Manual Input",
            ],
            x=[store_visits, qualified_visits, engaged_visits, transactions],
            textposition="auto",
            texttemplate="%{value:,}<br>(%{percentPrevious} retained from prior)",
            marker={
                "color": [
                    "rgba(26,115,232,0.15)",
                    "rgba(251,188,4,0.15)",
                    "rgba(142,36,170,0.15)",
                    "rgba(52,168,83,0.15)",
                ],
                "line": {"width": 2, "color": ["#1a73e8", "#fbbc04", "#8e24aa", "#34a853"]},
            },
            connector={"line": {"color": "rgba(128,134,139,0.3)", "width": 1.5}},
        )
    )
    fig_funnel.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=13, color="#5f6368"),
    )
    st.plotly_chart(fig_funnel, width="stretch", config=PLOT_CONFIG)

with tab2:
    st.markdown("<div class='section-title'>Hourly Live Traffic Trend</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='small-note'>These hourly traffic and attention trends are aligned to Asia/Kolkata time.</div>",
        unsafe_allow_html=True,
    )

    if not hourly_df.empty:
        traffic_plot_df = hourly_df.copy()
        fig_hourly = go.Figure()
        fig_hourly.add_trace(
            go.Scatter(
                x=traffic_plot_df["hour_label"],
                y=traffic_plot_df["avg_far_devices"],
                mode="lines+markers",
                name="Walk-By",
                line=dict(color="#9aa0a6"),
            )
        )
        fig_hourly.add_trace(
            go.Scatter(
                x=traffic_plot_df["hour_label"],
                y=traffic_plot_df["avg_mid_devices"],
                mode="lines+markers",
                name="Interest",
                line=dict(color="#fbbc04"),
            )
        )
        fig_hourly.add_trace(
            go.Scatter(
                x=traffic_plot_df["hour_label"],
                y=traffic_plot_df["avg_near_devices"],
                mode="lines+markers",
                name="Near",
                line=dict(color="#1a73e8"),
            )
        )
        st.plotly_chart(style_chart(fig_hourly), width="stretch", config=PLOT_CONFIG)
    else:
        st.info("No hourly traffic data found.")

with tab3:
    st.markdown("<div class='section-title'>Dwell Distribution</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='small-note'>This chart uses raw dwell bucket counts from Athena with no UI-side scaling.</div>",
        unsafe_allow_html=True,
    )

    if not dwell_df.empty:
        plot_df = prepare_dwell_plot_df(dwell_df)
        fig_dwell = px.bar(
            plot_df,
            x="dwell_bucket",
            y="visits",
            text_auto=".0f",
            color="visits",
            color_continuous_scale="Blues",
        )
        fig_dwell.update_layout(coloraxis_showscale=False)
        st.plotly_chart(style_chart(fig_dwell), width="stretch", config=PLOT_CONFIG)
    else:
        st.info("No dwell bucket data found.")

with tab4:
    st.markdown("<div class='section-title'>Hourly Detected Device Mix</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='small-note'>Audience mix by hour is aligned to Asia/Kolkata time.</div>",
        unsafe_allow_html=True,
    )

    if not brand_df.empty:
        brand_plot_df = brand_df.copy()
        brand_long = brand_plot_df.melt(
            id_vars=["hour_label"],
            value_vars=["avg_apple_devices", "avg_samsung_devices", "avg_other_devices"],
            var_name="Brand",
            value_name="Count",
        )
        brand_long["Brand"] = brand_long["Brand"].map(
            {
                "avg_apple_devices": "Apple",
                "avg_samsung_devices": "Samsung",
                "avg_other_devices": "Other",
            }
        )
        fig_brand = px.bar(
            brand_long,
            x="hour_label",
            y="Count",
            color="Brand",
            barmode="stack",
            color_discrete_map={"Apple": "#5f6368", "Samsung": "#1a73e8", "Other": "#9aa0a6"},
        )
        st.plotly_chart(style_chart(fig_brand), width="stretch", config=PLOT_CONFIG)
    else:
        st.info("No brand data found.")

st.markdown("</div>", unsafe_allow_html=True)
