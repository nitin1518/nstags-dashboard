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
        hour(from_unixtime(ts)) AS hour_of_day,
        format('%02d:00', hour(from_unixtime(ts))) AS hour_label,
        round(avg(apple_devices), 2) AS avg_apple_devices,
        round(avg(samsung_devices), 2) AS avg_samsung_devices,
        round(avg(other_devices), 2) AS avg_other_devices
    FROM nstags_live_analytics
    WHERE store_id = '{sid}'
      AND year = '{y}'
      AND month = '{m}'
      AND day = '{d}'
    GROUP BY hour(from_unixtime(ts))
    ORDER BY hour_of_day
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

Write an executive brief in Markdown with exactly this structure:
* **What happened:** [1 sentence]
* **What the traffic says:** [Interpret walk-by / interest / near-store correctly as traffic intensity, not literal counts]
* **What the visits say:** [Interpret visit quality and dwell]
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
except Exception as e:
    st.error(f"Failed to load Athena data: {e}")
    st.stop()

if dashboard_df.empty:
    st.warning("No metrics found for selected date.")
    st.stop()

dash = dashboard_df.iloc[0].to_dict()

# ==========================================
# TRUSTWORTHY METRIC LAYER
# ==========================================
# LIVE TRAFFIC = INTENSITY / INDEX STYLE METRICS
walk_by_traffic = float(dash.get("walk_by_traffic", 0))
store_interest = float(dash.get("store_interest", 0))
near_store = float(dash.get("near_store", 0))

# SESSION METRICS = COUNTS
store_visits = float(dash.get("store_visits", 0))
qualified_visits = float(dash.get("qualified_footfall", 0))
engaged_visits = float(dash.get("engaged_visits", 0))
avg_dwell_seconds = float(dash.get("avg_dwell_seconds", 0))
median_dwell_seconds = float(dash.get("median_dwell_seconds", 0))

# COMMERCIAL / MODELED METRICS
sales_conversion = safe_div(transactions, store_visits)
qualified_visit_rate = safe_div(qualified_visits, store_visits)
engaged_visit_rate = safe_div(engaged_visits, store_visits)

if app_mode == "Retail Media":
    cost_per_engaged = safe_div(campaign_value, engaged_visits)
    cost_per_visit = safe_div(campaign_value, store_visits)
    # This is an index-like CPM estimate using walk-by intensity, not literal impression count.
    effective_cpm_est = safe_div(campaign_value, walk_by_traffic) * 1000 if walk_by_traffic else 0
else:
    aov = safe_div(daily_revenue, transactions)
    # Indicative attributed revenue model, not causal truth
    indicative_attributed_revenue = store_visits * sales_conversion * aov
    roas = safe_div(indicative_attributed_revenue, marketing_spend)
    cost_per_visit = safe_div(marketing_spend, store_visits)

# BENCHMARKS (placeholder / configurable later)
qual_class, qual_verdict = verdict_class(qualified_visit_rate * 100, 50, 30)
eng_class, eng_verdict = verdict_class(engaged_visit_rate * 100, 40, 20)
conv_class, conv_verdict = verdict_class(sales_conversion * 100, 20, 10)

# ==========================================
# AI COPILOT
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
    }

    with st.spinner("Analyzing Athena metrics..."):
        ai_text = generate_ai_brief(payload)
    st.info(ai_text, icon="✨")

st.markdown("<div class='animate-container'>", unsafe_allow_html=True)

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
# DIAGNOSTIC CARDS
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
                <div class="mono-box">Effective CPM shown elsewhere is an intensity-based estimate, not audited media billing.</div>
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
tab1, tab2, tab3, tab4 = st.tabs(["🎯 Visits Funnel", "🚦 Traffic Trends", "⏱️ Dwell", "📱 Audience Mix"])

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
        "<div class='small-note'>These are raw hourly live intensity metrics from Athena. No UI-side multipliers are applied.</div>",
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
        "<div class='small-note'>This chart uses the raw dwell bucket counts from Athena without calibration multipliers.</div>",
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
        "<div class='small-note'>This is live detected device mix, not necessarily the same as visit brand mix.</div>",
        unsafe_allow_html=True,
    )

    if not brand_df.empty:
        brand_long = brand_df.melt(
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
