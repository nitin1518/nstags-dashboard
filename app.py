import time
import re
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
# PAGE CONFIG & STYLES
# ==========================================
st.set_page_config(page_title="nsTags | Retail Intelligence", page_icon="📈", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
html, body, [class*="css"]  { font-family: 'Inter', sans-serif; background-color: #f8f9fa; }
@keyframes slideUpFade { from { opacity: 0; transform: translateY(12px); } to { opacity: 1; transform: translateY(0); } }
.animate-container { animation: slideUpFade 0.55s cubic-bezier(0.16, 1, 0.3, 1) forwards; }
.hero-shell { background: linear-gradient(135deg, rgba(26,115,232,0.10) 0%, rgba(52,168,83,0.05) 100%); border: 1px solid rgba(26,115,232,0.18); border-radius: 18px; padding: 1.2rem 1.4rem; margin-bottom: 1rem; }
.hero-title { font-size: 1.8rem; font-weight: 800; margin-bottom: 0.2rem; color: #1f1f1f; }
.hero-sub { color: #5f6368; font-size: 0.98rem; margin-bottom: 0; }
.kpi-card { background: white; border: 1px solid rgba(95,99,104,0.12); border-radius: 16px; padding: 1rem 1rem 0.9rem 1rem; box-shadow: 0 2px 10px rgba(0,0,0,0.03); height: 100%; }
.kpi-label { font-size: 0.78rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: #5f6368; margin-bottom: 0.45rem; }
.kpi-value { font-size: 2rem; font-weight: 800; color: #1f1f1f; line-height: 1.1; }
.kpi-sub { font-size: 0.9rem; color: #5f6368; margin-top: 0.3rem; }
.insight-card { background: white; border: 1px solid rgba(95,99,104,0.12); border-left: 5px solid #1a73e8; border-radius: 16px; padding: 1rem 1.1rem; box-shadow: 0 2px 10px rgba(0,0,0,0.03); min-height: 220px; }
.insight-title { font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.08em; color: #5f6368; font-weight: 800; margin-bottom: 0.55rem; }
.insight-headline { font-size: 1.35rem; font-weight: 800; color: #1f1f1f; line-height: 1.2; margin-bottom: 0.7rem; }
.insight-body { font-size: 0.96rem; color: #3c4043; line-height: 1.55; }
.mono-box { background: rgba(95,99,104,0.06); border-radius: 10px; padding: 0.7rem 0.8rem; margin-top: 0.8rem; font-family: 'Courier New', monospace; font-size: 0.82rem; color: #3c4043; }
.section-title { font-size: 1.15rem; font-weight: 800; color: #1f1f1f; margin-top: 0.4rem; margin-bottom: 0.8rem; }
div[data-testid="stInfo"] { background: linear-gradient(145deg, #f8fafd 0%, #ffffff 100%); border: 1px solid rgba(26, 115, 232, 0.18); border-left: 6px solid #1a73e8; border-radius: 14px; padding: 1rem 1.3rem; box-shadow: 0 4px 14px rgba(0,0,0,0.04); }
</style>
""", unsafe_allow_html=True)

PLOT_CONFIG = {"displayModeBar": False}

# ==========================================
# CONFIG & SECURITY
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

# --- SANITIZATION HELPERS ---
def sanitize_alnum(val: str) -> str:
    if not re.match(r'^[a-zA-Z0-9_:-]+$', str(val)): raise ValueError(f"Invalid input: {val}")
    return str(val)

def sanitize_num(val: str) -> str:
    if not re.match(r'^\d+$', str(val)): raise ValueError(f"Invalid input: {val}")
    return str(val)

# --- MATH HELPERS ---
def safe_div(a, b): return a / b if b not in [0, None] else 0
def fmt_int(x): return f"{int(round(x)):,}" if pd.notnull(x) else "0"
def fmt_currency(x): return f"₹{x:,.0f}" if pd.notnull(x) else "₹0"

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
# ROBUST ATHENA LOADERS
# ==========================================
def run_athena_query(query: str, database: str = ATHENA_DATABASE) -> pd.DataFrame:
    try:
        response = athena_client.start_query_execution(
            QueryString=query, QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}, WorkGroup=ATHENA_WORKGROUP,
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        msg = e.response.get("Error", {}).get("Message", str(e))
        raise RuntimeError(f"Athena ClientError [{code}]: {msg}") from e

    execution_id = response["QueryExecutionId"]
    start_time = time.time()
    
    # 60-Second Timeout Loop
    while True:
        execution = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = execution["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"): 
            break
        if time.time() - start_time > 60:
            raise TimeoutError("Athena query timed out after 60 seconds.")
        time.sleep(1)

    if state != "SUCCEEDED": 
        reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Unknown Athena error")
        raise RuntimeError(f"Athena Query Failed: {reason}")
        
    output_location = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    bucket, key = s3_uri_to_bucket_key(output_location)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))

@st.cache_data(ttl=300)
def load_store_list() -> pd.DataFrame:
    return run_athena_query("SELECT DISTINCT store_id FROM nstags_dashboard_metrics ORDER BY store_id")

@st.cache_data(ttl=300)
def load_available_dates(store_id: str) -> pd.DataFrame:
    s_id = sanitize_alnum(store_id)
    return run_athena_query(f"SELECT DISTINCT year, month, day FROM nstags_dashboard_metrics WHERE store_id = '{s_id}' ORDER BY year DESC, month DESC, day DESC")

@st.cache_data(ttl=300)
def load_dashboard_metrics(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    s_id, y, m, d = sanitize_alnum(store_id), sanitize_num(year), sanitize_num(month), sanitize_num(day)
    return run_athena_query(f"SELECT * FROM nstags_dashboard_metrics WHERE store_id = '{s_id}' AND year = '{y}' AND month = '{m}' AND day = '{d}'")

@st.cache_data(ttl=300)
def load_hourly_traffic(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    s_id, y, m, d = sanitize_alnum(store_id), sanitize_num(year), sanitize_num(month), sanitize_num(day)
    return run_athena_query(f"SELECT * FROM nstags_hourly_traffic_pretty WHERE store_id = '{s_id}' AND year = '{y}' AND month = '{m}' AND day = '{d}' ORDER BY hour_of_day")

@st.cache_data(ttl=300)
def load_conversion_hourly(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    s_id, y, m, d = sanitize_alnum(store_id), sanitize_num(year), sanitize_num(month), sanitize_num(day)
    return run_athena_query(f"SELECT * FROM nstags_conversion_hourly WHERE store_id = '{s_id}' AND year = '{y}' AND month = '{m}' AND day = '{d}' ORDER BY hour_of_day")

# ==========================================
# AI ENGINE
# ==========================================
@st.cache_data(ttl=300)
def generate_ai_brief(ai_payload):
    if not GEMINI_API_KEY: return "⚠️ **AI unavailable:** GEMINI_API_KEY not configured in Secrets."
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        prompt = f"""
        Act as an elite retail strategy consultant. Analyze this store's strictly reconciled daily metrics.
        MODE: {ai_payload['mode']}
        
        ENVIRONMENTAL INTENSITY (Live scan averages):
        - Walk-By Intensity: {ai_payload['walk_by_intensity']}
        - Store Interest Intensity: {ai_payload['interest_intensity']}
        
        VISIT FUNNEL (Cumulative Session Counts):
        - Total Store Visits: {ai_payload['visits']}
        - Qualified Visits: {ai_payload['qualified']}
        - Engaged Visits: {ai_payload['engaged']}
        - Attributed Transactions (Model Input): {ai_payload['transactions']}
        
        Generate a crisp executive brief using EXACT numbers. Output strictly as Markdown:
        * **Environment Context:** [1 sentence summarizing walk-by vs interest intensity]
        * **Visit Quality:** [Analyze drop-off from Total Visits to Qualified/Engaged]
        * **Commercial Execution:** [Diagnose modeled transaction conversion against engaged traffic]
        * **Strategic Action:** [One concrete operational directive based on the bottleneck]
        """
        response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        return response.text
    except Exception as e:
        return f"⚠️ **AI unavailable:** {str(e)}"

# ==========================================
# APP HEADER
# ==========================================
st.markdown("""
<div class="hero-shell">
    <div class="hero-title">nsTags Intelligence V3</div>
    <p class="hero-sub">Retail Session Analytics & Floor Execution (Athena Validated)</p>
</div>
""", unsafe_allow_html=True)

# ==========================================
# SIDEBAR CONTROLS
# ==========================================
with st.sidebar:
    st.markdown("### Control Center")
    app_mode = st.radio("Business Mode", ["Retail Ops", "Retail Media"])

    try: 
        stores_df = load_store_list()
    except Exception as e: 
        st.error(f"Initialization Failed: {e}"); st.stop()

    if stores_df.empty: st.warning("No stores found."); st.stop()
    selected_store = st.selectbox("Active Store", stores_df["store_id"].dropna().astype(str).tolist())

    try: 
        dates_df = load_available_dates(selected_store)
    except Exception as e: 
        st.error(f"Date Fetch Failed: {e}"); st.stop()
    
    if dates_df.empty: st.warning("No dates found."); st.stop()
    dates_df["date_str"] = dates_df["year"].astype(str) + "-" + dates_df["month"].astype(str).str.zfill(2) + "-" + dates_df["day"].astype(str).str.zfill(2)
    selected_date = st.selectbox("Date", dates_df["date_str"].tolist())
    selected_year, selected_month, selected_day = selected_date.split("-")

    st.markdown("### Financial Modeling")
    st.caption("Inputs used to calculate indicative scenario outcomes.")
    if app_mode == "Retail Media":
        campaign_value = st.number_input("Campaign Revenue (₹)", min_value=0, value=15000, step=1000)
    else:
        marketing_spend = st.number_input("Marketing Spend (₹)", min_value=0, value=5000, step=500)
        daily_revenue = st.number_input("Store Revenue (₹)", min_value=0, value=45000, step=1000)
        
    transactions = st.number_input("Modeled Transactions", min_value=0, value=35, step=1)
    ai_enabled = st.checkbox("Enable AI executive brief", value=True)

# ==========================================
# EXACT DATA EXTRACTION (NO SCALING/STACKING)
# ==========================================
try:
    dashboard_df = load_dashboard_metrics(selected_store, selected_year, selected_month, selected_day)
    hourly_df = load_hourly_traffic(selected_store, selected_year, selected_month, selected_day)
except Exception as e: 
    st.error(f"Dashboard Data Failed to Load: {e}"); st.stop()

if dashboard_df.empty: st.warning("No metrics found for selected date."); st.stop()
dash = dashboard_df.iloc[0].to_dict()

# 1. Environmental Averages (Intensity metrics from live scans)
walk_by_intensity = float(dash.get("walk_by_traffic", 0))
interest_intensity = float(dash.get("store_interest", 0))

# 2. Cumulative Session Counts (True Funnel Data)
total_visits = float(dash.get("store_visits", 0))
qualified_visits = float(dash.get("qualified_footfall", 0))
engaged_visits = float(dash.get("engaged_visits", 0))

# 3. Honest Business Math
qualified_rate = safe_div(qualified_visits, total_visits)
engaged_rate = safe_div(engaged_visits, total_visits)
floor_conversion = safe_div(transactions, total_visits)

if app_mode == "Retail Media":
    cost_per_visit = safe_div(campaign_value, total_visits)
    cost_per_engaged = safe_div(campaign_value, engaged_visits)
else:
    aov = safe_div(daily_revenue, transactions)
    indicative_roas = safe_div((total_visits * floor_conversion * aov), marketing_spend)
    cost_per_visit = safe_div(marketing_spend, total_visits)

# Pre-process charting data
if not hourly_df.empty and "hour_label" not in hourly_df: 
    hourly_df["hour_label"] = hourly_df["hour_of_day"].apply(lambda x: f"{int(x):02d}:00")

# ==========================================
# AI COPILOT
# ==========================================
if ai_enabled:
    payload = {
        "mode": app_mode, "walk_by_intensity": round(walk_by_intensity, 1), "interest_intensity": round(interest_intensity, 1),
        "visits": int(total_visits), "qualified": int(qualified_visits), "engaged": int(engaged_visits), "transactions": transactions
    }
    with st.spinner("Analyzing verified Athena Data..."):
        ai_text = generate_ai_brief(payload)
    st.info(ai_text, icon="✨")

st.markdown("<div class='animate-container'>", unsafe_allow_html=True)

# ==========================================
# HONEST KPI RAIL
# ==========================================
st.markdown("<div class='section-title'>Session & Financial Outcomes</div>", unsafe_allow_html=True)
k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Total Visits</div><div class="kpi-value">{fmt_int(total_visits)}</div><div class="kpi-sub">Total detected sessions</div></div>', unsafe_allow_html=True)
with k2:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Qualified Rate</div><div class="kpi-value">{qualified_rate*100:.1f}%</div><div class="kpi-sub">Met base dwell criteria</div></div>', unsafe_allow_html=True)
with k3:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Engaged Rate</div><div class="kpi-value">{engaged_rate*100:.1f}%</div><div class="kpi-sub">High-intent sessions</div></div>', unsafe_allow_html=True)

with k4:
    if app_mode == "Retail Media":
        st.markdown(f'<div class="kpi-card"><div class="kpi-label">Cost per Visit</div><div class="kpi-value">{fmt_currency(cost_per_visit)}</div><div class="kpi-sub">Campaign value / visit</div></div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="kpi-card"><div class="kpi-label">Est. Floor Conv.</div><div class="kpi-value">{floor_conversion*100:.1f}%</div><div class="kpi-sub">Modeled against visits</div></div>', unsafe_allow_html=True)

with k5:
    if app_mode == "Retail Media":
        st.markdown(f'<div class="kpi-card"><div class="kpi-label">Cost per Engaged</div><div class="kpi-value">{fmt_currency(cost_per_engaged)}</div><div class="kpi-sub">Value per deep intent</div></div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="kpi-card"><div class="kpi-label">Indicative ROAS</div><div class="kpi-value">{indicative_roas:.1f}x</div><div class="kpi-sub">Attributed model / spend</div></div>', unsafe_allow_html=True)

# ==========================================
# DIAGNOSTIC CARDS
# ==========================================
st.markdown("<div class='section-title'>Environment vs. Execution</div>", unsafe_allow_html=True)
d1, d2, d3 = st.columns(3)

with d1:
    st.markdown(f"""<div class="insight-card"><div class="insight-title">Traffic Intensity</div><div class="insight-headline">{walk_by_intensity:.1f} Avg</div>
    <div class="insight-body">The location maintained an average concurrent walk-by intensity of <b>{walk_by_intensity:.1f}</b> devices, generating <b>{interest_intensity:.1f}</b> concurrent devices at the window.</div>
    <div class="mono-box">Note: These are environmental averages, not total audience counts.</div></div>""", unsafe_allow_html=True)

with d2:
    st.markdown(f"""<div class="insight-card" style="border-left-color:#fbbc04;"><div class="insight-title">Session Quality</div><div class="insight-headline">{engaged_rate*100:.1f}% Engaged</div>
    <div class="insight-body">Out of <b>{fmt_int(total_visits)}</b> finalized visits, <b>{fmt_int(engaged_visits)}</b> became highly engaged.</div>
    <div class="mono-box">Engaged Share = Engaged Visits / Total Visits</div></div>""", unsafe_allow_html=True)

with d3:
    if app_mode == "Retail Media":
        st.markdown(f"""<div class="insight-card" style="border-left-color:#34a853;"><div class="insight-title">Media Accountability</div><div class="insight-headline">{fmt_currency(cost_per_engaged)} / Engaged</div>
        <div class="insight-body">The campaign secured <b>{fmt_int(engaged_visits)}</b> deep engagements at this cost factor.</div>
        <div class="mono-box">CPE = Modeled Campaign Value / Engaged Sessions</div></div>""", unsafe_allow_html=True)
    else:
        st.markdown(f"""<div class="insight-card" style="border-left-color:#34a853;"><div class="insight-title">Commercial Model</div><div class="insight-headline">{floor_conversion*100:.1f}% Est. Close</div>
        <div class="insight-body">Based on <b>{fmt_int(transactions)}</b> modeled transactions against <b>{fmt_int(total_visits)}</b> detected visits.</div>
        <div class="mono-box">Floor Conversion = Transactions / Total Visits</div></div>""", unsafe_allow_html=True)

# ==========================================
# TRUE SESSION FUNNEL
# ==========================================
st.markdown("<div class='section-title'>Verified Session Funnel</div>", unsafe_allow_html=True)
st.caption("This funnel strictly measures cumulative completed sessions and modeled transactions.")

fig_funnel = go.Figure(go.Funnel(
    y=[
        "<b>Total Visits</b><br>Detected Sessions", 
        "<b>Qualified Visits</b><br>Met Dwell Baseline", 
        "<b>Engaged Visits</b><br>Deep Intent Dwell", 
        "<b>Transactions</b><br>Modeled Sales"
    ],
    x=[total_visits, qualified_visits, engaged_visits, transactions],
    textposition="auto",
    texttemplate="%{value:,} <br>(Retained %{percentPrevious} from prior stage)",
    marker={
        "color": ["rgba(26,115,232,0.15)", "rgba(251,188,4,0.15)", "rgba(142,36,170,0.15)", "rgba(52,168,83,0.15)"],
        "line": {"width": 2, "color": ["#1a73e8", "#fbbc04", "#8e24aa", "#34a853"]}
    },
    connector={"line": {"color": "rgba(128,134,139,0.3)", "dash": "solid", "width": 1.5}}
))

fig_funnel.update_layout(
    margin=dict(l=10, r=10, t=10, b=10), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", size=13, color="#5f6368")
)
st.plotly_chart(fig_funnel, width="stretch", config=PLOT_CONFIG)

st.markdown("</div>", unsafe_allow_html=True)
