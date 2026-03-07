import time
from io import StringIO
from urllib.parse import urlparse
from datetime import datetime

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
# PREMIUM UI / UX (V3)
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
.kpi-label { font-size: 0.78rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: #5f6368; margin-bottom: 0.45rem; }
.kpi-value { font-size: 2rem; font-weight: 800; color: #1f1f1f; line-height: 1.1; }
.kpi-sub { font-size: 0.9rem; color: #5f6368; margin-top: 0.3rem; }

.verdict-good { color: #188038; font-weight: 700; }
.verdict-warn { color: #b06000; font-weight: 700; }
.verdict-bad  { color: #d93025; font-weight: 700; }

.insight-card {
    background: white; border: 1px solid rgba(95,99,104,0.12); border-left: 5px solid #1a73e8;
    border-radius: 16px; padding: 1rem 1.1rem; box-shadow: 0 2px 10px rgba(0,0,0,0.03); min-height: 220px;
}
.insight-title { font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.08em; color: #5f6368; font-weight: 800; margin-bottom: 0.55rem; }
.insight-headline { font-size: 1.35rem; font-weight: 800; color: #1f1f1f; line-height: 1.2; margin-bottom: 0.7rem; }
.insight-body { font-size: 0.96rem; color: #3c4043; line-height: 1.55; }
.mono-box { background: rgba(95,99,104,0.06); border-radius: 10px; padding: 0.7rem 0.8rem; margin-top: 0.8rem; font-family: 'Courier New', monospace; font-size: 0.82rem; color: #3c4043; }
.section-title { font-size: 1.15rem; font-weight: 800; color: #1f1f1f; margin-top: 0.4rem; margin-bottom: 0.8rem; }

div[data-testid="stInfo"] {
    background: linear-gradient(145deg, #f8fafd 0%, #ffffff 100%);
    border: 1px solid rgba(26, 115, 232, 0.18); border-left: 6px solid #1a73e8;
    border-radius: 14px; padding: 1rem 1.3rem; box-shadow: 0 4px 14px rgba(0,0,0,0.04);
}
</style>
""", unsafe_allow_html=True)

PLOT_CONFIG = {"displayModeBar": False}

# ==========================================
# CONFIG & ATHENA
# ==========================================
AWS_REGION = st.secrets.get("AWS_REGION", "ap-south-1")
ATHENA_DATABASE = st.secrets.get("ATHENA_DATABASE", "nstags_analytics")
ATHENA_WORKGROUP = st.secrets.get("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT = st.secrets.get("ATHENA_OUTPUT", "s3://nstags-datalake-hq-2026/athena-results/")

AWS_ACCESS_KEY_ID = st.secrets.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = st.secrets.get("AWS_SECRET_ACCESS_KEY")
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY")

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    st.error("Missing AWS Credentials in Streamlit secrets.")
    st.stop()

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

athena_client = session.client("athena")
s3_client = session.client("s3")

# ==========================================
# HELPERS (Math & Viz)
# ==========================================
def safe_div(a, b): return a / b if b not in [0, None] else 0
def pct(a, b): return safe_div(a, b) * 100
def fmt_int(x): return f"{int(round(x)):,}"
def fmt_currency(x): return f"₹{x:,.0f}"

def verdict_class(value, good_threshold, warn_threshold, higher_is_better=True):
    if higher_is_better:
        if value >= good_threshold: return "verdict-good", "Healthy"
        elif value >= warn_threshold: return "verdict-warn", "Watch"
        return "verdict-bad", "Weak"
    else:
        if value <= good_threshold: return "verdict-good", "Efficient"
        elif value <= warn_threshold: return "verdict-warn", "Monitor"
        return "verdict-bad", "Expensive"

def s3_uri_to_bucket_key(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip("/")

def style_chart(fig):
    fig.update_layout(
        hovermode="x unified", margin=dict(l=10, r=10, t=35, b=50),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="top", y=-0.18, xanchor="center", x=0.5),
        font=dict(family="Inter, sans-serif", color="#5f6368")
    )
    fig.update_xaxes(showgrid=False, zeroline=False, title_text="")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(95,99,104,0.10)", zeroline=False, title_text="")
    return fig

# ==========================================
# ATHENA LOADERS
# ==========================================
def run_athena_query(query: str, database: str = ATHENA_DATABASE) -> pd.DataFrame:
    try:
        response = athena_client.start_query_execution(
            QueryString=query, QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}, WorkGroup=ATHENA_WORKGROUP,
        )
    except ClientError as e:
        st.error(f"Athena error: {e}")
        st.stop()

    execution_id = response["QueryExecutionId"]
    while True:
        execution = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = execution["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"): break
        time.sleep(1)

    if state != "SUCCEEDED": raise RuntimeError("Athena query failed")
    output_location = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    bucket, key = s3_uri_to_bucket_key(output_location)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))

@st.cache_data(ttl=300)
def load_store_list() -> pd.DataFrame:
    return run_athena_query("SELECT DISTINCT store_id FROM nstags_dashboard_metrics ORDER BY store_id")

@st.cache_data(ttl=300)
def load_available_dates(store_id: str) -> pd.DataFrame:
    query = f"SELECT DISTINCT year, month, day FROM nstags_dashboard_metrics WHERE store_id = '{store_id}' ORDER BY year DESC, month DESC, day DESC"
    return run_athena_query(query)

@st.cache_data(ttl=300)
def load_dashboard_metrics(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    return run_athena_query(f"SELECT * FROM nstags_dashboard_metrics WHERE store_id = '{store_id}' AND year = '{year}' AND month = '{month}' AND day = '{day}'")

@st.cache_data(ttl=300)
def load_hourly_traffic(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    return run_athena_query(f"SELECT * FROM nstags_hourly_traffic_pretty WHERE store_id = '{store_id}' AND year = '{year}' AND month = '{month}' AND day = '{day}' ORDER BY hour_of_day")

@st.cache_data(ttl=300)
def load_conversion_hourly(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    return run_athena_query(f"SELECT * FROM nstags_conversion_hourly WHERE store_id = '{store_id}' AND year = '{year}' AND month = '{month}' AND day = '{day}' ORDER BY hour_of_day")

@st.cache_data(ttl=300)
def load_dwell_buckets(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    return run_athena_query(f"SELECT * FROM nstags_dwell_buckets WHERE store_id = '{store_id}' AND year = '{year}' AND month = '{month}' AND day = '{day}'")

@st.cache_data(ttl=300)
def load_brand_mix_hourly(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    query = f"""
    SELECT hour(from_unixtime(ts)) AS hour_of_day, format('%02d:00', hour(from_unixtime(ts))) AS hour_label,
           round(avg(apple_devices), 2) AS avg_apple_devices, round(avg(samsung_devices), 2) AS avg_samsung_devices,
           round(avg(other_devices), 2) AS avg_other_devices
    FROM nstags_live_analytics
    WHERE store_id = '{store_id}' AND year = '{year}' AND month = '{month}' AND day = '{day}'
    GROUP BY hour(from_unixtime(ts)) ORDER BY hour_of_day
    """
    return run_athena_query(query)

# ==========================================
# AI ENGINE (V3)
# ==========================================
@st.cache_data(ttl=300)
def generate_ai_brief(ai_payload):
    if not GEMINI_API_KEY: return "⚠️ **AI unavailable:** GEMINI_API_KEY not configured in Secrets."
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        prompt = f"""
        You are a top-tier retail strategy consultant. Analyze this store's Athena Data Lake metrics:
        MODE: {ai_payload['mode']}
        - Exposure (Street): {ai_payload['exposed']}
        - Attention (Window): {ai_payload['attended']}
        - Entries (Walk-ins): {ai_payload['entered']}
        - Transactions: {ai_payload['transactions']}
        - Revenue: {ai_payload['revenue']}
        - Attention Rate: {ai_payload['attention_rate']}% (Target 15%)
        - Entry Rate: {ai_payload['entry_rate']}% (Target 35%)
        - Conversion Rate: {ai_payload['conversion_rate']}% (Target 20%)

        Generate a sharp executive brief with this exact structure (Use exact numbers. Only Markdown):
        * **What happened:** [1 sentence summary]
        * **Why it matters:** [Diagnosis of the Funnel Drop-off vs Benchmarks]
        * **Primary bottleneck:** [Identify awareness, attention, entry, or floor conversion problem]
        * **Recommended action:** [Concrete fix for store/merchandising/staff]
        """
        response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        return response.text
    except Exception:
        return "⚠️ **AI unavailable:** connection or rate-limit issue."

# ==========================================
# APP HEADER
# ==========================================
st.markdown("""
<div class="hero-shell">
    <div class="hero-title">nsTags Intelligence V3 (Athena)</div>
    <p class="hero-sub">Retail Intelligence • DOOH Media Measurement • Conversion Diagnostics</p>
</div>
""", unsafe_allow_html=True)

# ==========================================
# SIDEBAR CONTROLS
# ==========================================
with st.sidebar:
    st.markdown("### Control Center")
    app_mode = st.radio("Business Mode", ["Retail Ops", "Retail Media"])

    try: stores_df = load_store_list()
    except Exception: st.stop()

    if stores_df.empty: st.warning("No stores found."); st.stop()
    selected_store = st.selectbox("Active Store", stores_df["store_id"].dropna().astype(str).tolist())

    try: dates_df = load_available_dates(selected_store)
    except Exception: st.stop()
    
    if dates_df.empty: st.warning("No dates found."); st.stop()
    dates_df["date_str"] = dates_df["year"].astype(str) + "-" + dates_df["month"].astype(str).str.zfill(2) + "-" + dates_df["day"].astype(str).str.zfill(2)
    selected_date = st.selectbox("Date", dates_df["date_str"].tolist())
    selected_year, selected_month, selected_day = selected_date.split("-")

    st.markdown("### Commercial Inputs")
    if app_mode == "Retail Media":
        campaign_value = st.number_input("Campaign Revenue (₹)", min_value=0, value=15000, step=1000)
        transactions = st.number_input("Attributed Sales", min_value=0, value=12, step=1)
    else:
        marketing_spend = st.number_input("Marketing Spend (₹)", min_value=0, value=5000, step=500)
        daily_revenue = st.number_input("Store Revenue (₹)", min_value=0, value=45000, step=1000)
        transactions = st.number_input("Store Transactions", min_value=0, value=35, step=1)

    ai_enabled = st.checkbox("Enable AI executive brief", value=True)

# ==========================================
# EXTRACT DATA FROM ATHENA
# ==========================================
try:
    dashboard_df = load_dashboard_metrics(selected_store, selected_year, selected_month, selected_day)
    hourly_df = load_hourly_traffic(selected_store, selected_year, selected_month, selected_day)
    conversion_df = load_conversion_hourly(selected_store, selected_year, selected_month, selected_day)
    dwell_df = load_dwell_buckets(selected_store, selected_year, selected_month, selected_day)
    brand_df = load_brand_mix_hourly(selected_store, selected_year, selected_month, selected_day)
except Exception: st.stop()

if dashboard_df.empty: st.warning("No metrics found for selected date."); st.stop()

dash = dashboard_df.iloc[0].to_dict()

# Map Athena Data to V3 Logic
exposed = float(dash.get("walk_by_traffic", 0))
attended = float(dash.get("store_interest", 0))
entered = float(dash.get("store_visits", 0))
engaged_visitors = float(dash.get("engaged_visits", 0))

attention_rate = safe_div(attended, exposed)
entry_rate = safe_div(entered, attended)
conversion_rate = safe_div(transactions, entered)

# Pre-process DataFrames for charting
if not hourly_df.empty and "hour_label" not in hourly_df: hourly_df["hour_label"] = hourly_df["hour_of_day"].apply(lambda x: f"{int(x):02d}:00")
if not brand_df.empty and "hour_label" not in brand_df: brand_df["hour_label"] = brand_df["hour_of_day"].apply(lambda x: f"{int(x):02d}:00")

# ==========================================
# MONETIZATION MATH & BENCHMARKS
# ==========================================
if app_mode == "Retail Media":
    cost_per_engaged = safe_div(campaign_value, engaged_visitors)
    cost_per_entry = safe_div(campaign_value, entered)
    effective_cpm = safe_div(campaign_value, exposed) * 1000
else:
    aov = safe_div(daily_revenue, transactions)
    attributed_revenue = entered * conversion_rate * aov
    roas = safe_div(attributed_revenue, marketing_spend)
    cost_per_entry = safe_div(marketing_spend, entered)

att_class, att_verdict = verdict_class(attention_rate * 100, 15, 8)
ent_class, ent_verdict = verdict_class(entry_rate * 100, 35, 20)
conv_class, conv_verdict = verdict_class(conversion_rate * 100, 20, 10)

# ==========================================
# AI COPILOT
# ==========================================
if ai_enabled:
    payload = {
        "mode": app_mode, "exposed": int(exposed), "attended": int(attended), 
        "entered": int(entered), "transactions": transactions, 
        "revenue": fmt_currency(daily_revenue) if app_mode == "Retail Ops" else fmt_currency(campaign_value),
        "attention_rate": round(attention_rate * 100, 1), "entry_rate": round(entry_rate * 100, 1),
        "conversion_rate": round(conversion_rate * 100, 1)
    }
    with st.spinner("Analyzing Athena Data Lake..."):
        ai_text = generate_ai_brief(payload)
    st.info(ai_text, icon="✨")

st.markdown("<div class='animate-container'>", unsafe_allow_html=True)

# ==========================================
# KPI RAIL (V3)
# ==========================================
st.markdown("<div class='section-title'>Executive KPI Rail</div>", unsafe_allow_html=True)
k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    label = "Qualified Audience" if app_mode == "Retail Media" else "Qualified Footfall"
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">{label}</div><div class="kpi-value">{fmt_int(attended)}</div><div class="kpi-sub">Window-level attention pool</div></div>', unsafe_allow_html=True)
with k2:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Attention Rate</div><div class="kpi-value">{attention_rate*100:.1f}%</div><div class="kpi-sub"><span class="{att_class}">{att_verdict}</span> vs 15% bench</div></div>', unsafe_allow_html=True)
with k3:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Entry Rate</div><div class="kpi-value">{entry_rate*100:.1f}%</div><div class="kpi-sub"><span class="{ent_class}">{ent_verdict}</span> vs 35% bench</div></div>', unsafe_allow_html=True)

with k4:
    if app_mode == "Retail Media":
        st.markdown(f'<div class="kpi-card"><div class="kpi-label">Cost per Engaged</div><div class="kpi-value">{fmt_currency(cost_per_engaged)}</div><div class="kpi-sub">Campaign value / engaged</div></div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="kpi-card"><div class="kpi-label">Sales Conversion</div><div class="kpi-value">{conversion_rate*100:.1f}%</div><div class="kpi-sub"><span class="{conv_class}">{conv_verdict}</span> vs 20% bench</div></div>', unsafe_allow_html=True)

with k5:
    if app_mode == "Retail Media":
        st.markdown(f'<div class="kpi-card"><div class="kpi-label">Effective CPM</div><div class="kpi-value">{fmt_currency(effective_cpm)}</div><div class="kpi-sub">Media Value Delivered</div></div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="kpi-card"><div class="kpi-label">ROAS</div><div class="kpi-value">{roas:.1f}x</div><div class="kpi-sub">Attributed Rev / Spend</div></div>', unsafe_allow_html=True)

# ==========================================
# DIAGNOSTIC CARDS (V3)
# ==========================================
st.markdown("<div class='section-title'>Diagnostic Breakdown</div>", unsafe_allow_html=True)
d1, d2, d3 = st.columns(3)

with d1:
    st.markdown(f"""<div class="insight-card"><div class="insight-title">Acquisition</div><div class="insight-headline">{attention_rate*100:.1f}% attention rate</div>
    <div class="insight-body">Out of <b>{fmt_int(exposed)}</b> exposure events, <b>{fmt_int(attended)}</b> became attention-level interactions.</div>
    <div class="mono-box">Attention Rate = Window / Street = {fmt_int(attended)} / {fmt_int(exposed)}</div></div>""", unsafe_allow_html=True)

with d2:
    st.markdown(f"""<div class="insight-card" style="border-left-color:#fbbc04;"><div class="insight-title">Entry & Intent</div><div class="insight-headline">{entry_rate*100:.1f}% entry rate</div>
    <div class="insight-body">Of all qualified interactions, <b>{fmt_int(entered)}</b> became entries. Cleanest indicator of storefront strength.</div>
    <div class="mono-box">Entry Rate = InStore / Window = {fmt_int(entered)} / {fmt_int(attended)}</div></div>""", unsafe_allow_html=True)

with d3:
    if app_mode == "Retail Media":
        st.markdown(f"""<div class="insight-card" style="border-left-color:#34a853;"><div class="insight-title">Commercial Efficiency</div><div class="insight-headline">{fmt_currency(cost_per_entry)} per entry</div>
        <div class="insight-body">The campaign translated into <b>{fmt_int(entered)}</b> entries and <b>{fmt_int(engaged_visitors)}</b> engaged visitors.</div>
        <div class="mono-box">CPE = Campaign Value / Entries = {fmt_currency(campaign_value)} / {fmt_int(entered)}</div></div>""", unsafe_allow_html=True)
    else:
        leakage = max(0, entered - transactions)
        st.markdown(f"""<div class="insight-card" style="border-left-color:#34a853;"><div class="insight-title">Floor Conversion</div><div class="insight-headline">{conversion_rate*100:.1f}% close rate</div>
        <div class="insight-body"><b>{fmt_int(leakage)}</b> entry-level opportunities did not buy. Bottleneck is on-floor execution.</div>
        <div class="mono-box">Conversion = Transactions / Entries = {fmt_int(transactions)} / {fmt_int(entered)}</div></div>""", unsafe_allow_html=True)

# ==========================================
# DEEP DIVE TABS (Powered by Athena)
# ==========================================
tab1, tab2, tab3, tab4 = st.tabs(["🎯 Funnel", "🚦 Traffic Trends", "⏱️ Dwell", "📱 Audience Mix"])

with tab1:
    st.markdown("<div class='section-title'>The Shopper Journey Funnel</div>", unsafe_allow_html=True)
    
    # 1. The Horizontal Journey Map (Highly Intuitive Executive Breakdown)
    engaged_rate_pct = safe_div(engaged_visitors, entered) * 100
    
    st.markdown(f"""
    <div style="display: flex; flex-wrap: wrap; text-align: center; background: var(--secondary-background-color); padding: 1.5rem; border-radius: 12px; border: 1px solid rgba(128,134,139,0.2); margin-bottom: 1.5rem;">
        <div style="flex: 1; border-right: 1px solid rgba(128,134,139,0.2);">
            <div style="font-size: 0.75rem; font-weight: 700; color: #9aa0a6; text-transform: uppercase;">1. Street</div>
            <div style="font-size: 1.8rem; font-weight: 800; color: var(--text-color);">{fmt_int(exposed)}</div>
            <div style="font-size: 0.85rem; color: #80868b;">Total Impressions</div>
        </div>
        <div style="flex: 1; border-right: 1px solid rgba(128,134,139,0.2);">
            <div style="font-size: 0.75rem; font-weight: 700; color: #fbbc04; text-transform: uppercase;">2. Window</div>
            <div style="font-size: 1.8rem; font-weight: 800; color: var(--text-color);">{fmt_int(attended)}</div>
            <div style="font-size: 0.85rem; color: #80868b;"><b>{attention_rate*100:.1f}%</b> Stop Rate</div>
        </div>
        <div style="flex: 1; border-right: 1px solid rgba(128,134,139,0.2);">
            <div style="font-size: 0.75rem; font-weight: 700; color: #1a73e8; text-transform: uppercase;">3. Walk-ins</div>
            <div style="font-size: 1.8rem; font-weight: 800; color: var(--text-color);">{fmt_int(entered)}</div>
            <div style="font-size: 0.85rem; color: #80868b;"><b>{entry_rate*100:.1f}%</b> Entry Rate</div>
        </div>
        <div style="flex: 1; border-right: 1px solid rgba(128,134,139,0.2);">
            <div style="font-size: 0.75rem; font-weight: 700; color: #8e24aa; text-transform: uppercase;">4. Engaged</div>
            <div style="font-size: 1.8rem; font-weight: 800; color: var(--text-color);">{fmt_int(engaged_visitors)}</div>
            <div style="font-size: 0.85rem; color: #80868b;"><b>{engaged_rate_pct:.1f}%</b> of Walk-ins</div>
        </div>
        <div style="flex: 1;">
            <div style="font-size: 0.75rem; font-weight: 700; color: #34a853; text-transform: uppercase;">5. Purchased</div>
            <div style="font-size: 1.8rem; font-weight: 800; color: var(--text-color);">{fmt_int(transactions)}</div>
            <div style="font-size: 0.85rem; color: #80868b;"><b>{conversion_rate*100:.1f}%</b> Close Rate</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 2. The Visual Plotly Funnel (Reformatted for Clarity)
    fig_funnel = go.Figure(go.Funnel(
        y=[
            "<b>Exposure</b><br>Street Traffic", 
            "<b>Attention</b><br>Stopped to Look", 
            "<b>Visitation</b><br>Crossed Threshold", 
            "<b>Intent</b><br>Deep Dwell (>10m)", 
            "<b>Action</b><br>POS Transactions"
        ],
        x=[exposed, attended, entered, engaged_visitors, transactions],
        textposition="auto",
        texttemplate="%{value:,} People<br>(Retained %{percentPrevious} from prior stage)",
        marker={
            "color": ["rgba(154,160,166,0.15)", "rgba(251,188,4,0.15)", "rgba(26,115,232,0.15)", "rgba(142,36,170,0.15)", "rgba(52,168,83,0.15)"],
            "line": {"width": 2, "color": ["#9aa0a6", "#fbbc04", "#1a73e8", "#8e24aa", "#34a853"]}
        },
        connector={"line": {"color": "rgba(128,134,139,0.3)", "dash": "solid", "width": 1.5}}
    ))
    
    fig_funnel.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=13, color="#5f6368"),
        hoverlabel=dict(bgcolor="white", font_size=14, font_family="Inter")
    )
    
    st.plotly_chart(fig_funnel, width="stretch", config=PLOT_CONFIG)

with tab2:
    if not hourly_df.empty:
        traffic_plot_df = hourly_df.copy()
        fig_hourly = go.Figure()
        fig_hourly.add_trace(go.Scatter(x=traffic_plot_df["hour_label"], y=traffic_plot_df["avg_far_devices"], mode="lines+markers", name="Walk-By (Exposed)", line=dict(color="#9aa0a6")))
        fig_hourly.add_trace(go.Scatter(x=traffic_plot_df["hour_label"], y=traffic_plot_df["avg_mid_devices"], mode="lines+markers", name="Interest (Attended)", line=dict(color="#fbbc04")))
        fig_hourly.add_trace(go.Scatter(x=traffic_plot_df["hour_label"], y=traffic_plot_df["avg_near_devices"], mode="lines+markers", name="Near (Entered)", line=dict(color="#1a73e8")))
        st.plotly_chart(style_chart(fig_hourly), use_container_width=True, config=PLOT_CONFIG)
    else:
        st.info("No hourly traffic data found.")

with tab3:
    if not dwell_df.empty:
        dwell_order = ["00-10s", "10-30s", "30-60s", "01-03m", "03-05m", "05m+"]
        plot_df = dwell_df.copy()
        plot_df["dwell_bucket"] = pd.Categorical(plot_df["dwell_bucket"], categories=dwell_order, ordered=True)
        plot_df = plot_df.sort_values("dwell_bucket")
        fig_dwell = px.bar(plot_df, x="dwell_bucket", y="visits", text="visits", color="visits", color_continuous_scale="Blues")
        fig_dwell.update_layout(coloraxis_showscale=False)
        st.plotly_chart(style_chart(fig_dwell), use_container_width=True, config=PLOT_CONFIG)
    else:
        st.info("No dwell bucket data found.")

with tab4:
    if not brand_df.empty:
        brand_long = brand_df.melt(
            id_vars=["hour_label"], value_vars=["avg_apple_devices", "avg_samsung_devices", "avg_other_devices"],
            var_name="Brand", value_name="Count"
        )
        brand_long["Brand"] = brand_long["Brand"].map({"avg_apple_devices": "Apple", "avg_samsung_devices": "Samsung", "avg_other_devices": "Other"})
        fig_brand = px.bar(brand_long, x="hour_label", y="Count", color="Brand", barmode="stack", color_discrete_map={"Apple": "#5f6368", "Samsung": "#1a73e8", "Other": "#9aa0a6"})
        st.plotly_chart(style_chart(fig_brand), use_container_width=True, config=PLOT_CONFIG)
    else:
        st.info("No brand data found.")

st.markdown("</div>", unsafe_allow_html=True)
