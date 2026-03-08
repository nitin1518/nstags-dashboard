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
    page_title="nsTags | Retail Intelligence",
    page_icon="📈",
    layout="wide",
)

# =========================================================
# GOOGLE MATERIAL 3 DESIGN SYSTEM
# =========================================================
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Google+Sans:wght@400;500;700&display=swap');

:root {
    --google-blue: #4285F4;
    --google-red: #EA4335;
    --google-yellow: #FBBC04;
    --google-green: #34A853;
    --surface: #FFFFFF;
    --surface-variant: #F1F3F4;
    --on-surface: #202124;
    --on-surface-variant: #5F6368;
    --outline: #DADCE0;
    --primary-container: #D3E3FD;
    --transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
}

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background-color: #F8F9FA !important;
}

h1, h2, h3, .kpi-value {
    font-family: 'Google Sans', sans-serif !important;
}

.main .block-container {
    padding: 2rem 3rem !important;
    max-width: 1600px;
}

@keyframes fadeIn {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
}
.stApp { animation: fadeIn 0.4s ease-out; }

.hero {
    background: var(--surface);
    border: 1px solid var(--outline);
    border-radius: 28px;
    padding: 2.5rem;
    margin-bottom: 2rem;
    box-shadow: 0 1px 2px rgba(60,64,67,0.1);
}
.hero h1 { color: var(--google-blue); font-size: 2.6rem; font-weight: 700; margin: 0; }
.hero p { color: var(--on-surface-variant); font-size: 1.1rem; margin-top: 5px; }

.kpi-card {
    background: var(--surface);
    border: 1px solid var(--outline);
    border-radius: 20px;
    padding: 1.5rem;
    transition: var(--transition);
    display: flex;
    flex-direction: column;
    height: 100%;
    min-width: 180px;
}
.kpi-card:hover {
    box-shadow: 0 4px 12px rgba(60,64,67,0.1);
    border-color: var(--google-blue);
    transform: translateY(-2px);
}
.kpi-label {
    font-size: 0.7rem;
    font-weight: 700;
    color: var(--on-surface-variant);
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 8px;
}
.kpi-value {
    font-size: 2.2rem;
    font-weight: 700;
    color: var(--on-surface);
    line-height: 1.1;
}
.kpi-sub {
    font-size: 0.8rem;
    color: var(--on-surface-variant);
    margin-top: 8px;
    line-height: 1.4;
}

.progress-container {
    background-color: var(--surface-variant);
    border-radius: 10px;
    height: 6px;
    width: 100%;
    margin-top: 12px;
    overflow: hidden;
}
.progress-bar { height: 100%; border-radius: 10px; transition: width 0.8s ease-in-out; }

.panel {
    background: var(--surface);
    border: 1px solid var(--outline);
    border-radius: 20px;
    padding: 1.5rem;
    margin-bottom: 1rem;
}

.stTabs [data-baseweb="tab-list"] { background: transparent; gap: 8px; }
.stTabs [data-baseweb="tab"] {
    border-radius: 25px !important;
    border: 1px solid var(--outline) !important;
    padding: 8px 20px !important;
    background: var(--surface) !important;
}
.stTabs [aria-selected="true"] {
    background: var(--primary-container) !important;
    color: var(--google-blue) !important;
    border-color: var(--google-blue) !important;
}

.badge-good { color: var(--google-green); font-weight: 700; }
.badge-warn { color: var(--google-yellow); font-weight: 700; }
.badge-bad  { color: var(--google-red); font-weight: 700; }

@media (max-width: 768px) {
    .main .block-container { padding: 1rem !important; }
}
</style>
""",
    unsafe_allow_html=True,
)

# =========================================================
# CONFIG & AWS SESSION
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
        a, b = float(a), float(b)
        return a / b if b != 0 else 0.0
    except: return 0.0

def fmt_int(x): return f"{int(round(float(x))):,}" if x else "0"
def fmt_float(x, d=2): return f"{float(x):,.{d}f}" if x else "0"
def fmt_currency(x): return f"₹{float(x):,.0f}" if x else "₹0"
def fmt_pct(x, d=1): return f"{100 * float(x):.{d}f}%" if x else "0.0%"
def fmt_seconds(x):
    try:
        total = int(round(float(x)))
        mins, secs = divmod(total, 60)
        hrs, mins = divmod(mins, 60)
        return f"{hrs}h {mins}m" if hrs > 0 else f"{mins}m {secs}s"
    except: return "-"

def score_band(score):
    s = float(score or 0)
    if s >= 75: return "badge-good", "Strong", "#34A853"
    if s >= 50: return "badge-warn", "Moderate", "#FBBC04"
    return "badge-bad", "Weak", "#EA4335"

def render_card(label, value, sub, score=None):
    progress_html = ""
    if score is not None:
        _, _, color = score_band(score)
        progress_html = f"""
        <div class="progress-container">
            <div class="progress-bar" style="width: {score}%; background-color: {color};"></div>
        </div>
        """
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-sub">{sub}</div>
            {progress_html}
        </div>
        """,
        unsafe_allow_html=True,
    )

def style_chart(fig):
    fig.update_layout(
        font_family="Inter",
        font_color="#5F6368",
        title_font_family="Google Sans",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=50, b=50, l=20, r=20),
        colorway=["#4285F4", "#34A853", "#FBBC04", "#EA4335", "#5F6368"]
    )
    fig.update_xaxes(showgrid=False, linecolor="#DADCE0")
    fig.update_yaxes(showgrid=True, gridcolor="#F1F3F4", linecolor="#DADCE0")
    return fig

# =========================================================
# ATHENA RUNNER
# =========================================================
def run_athena_query(query: str, database: str = ATHENA_DATABASE, timeout_sec: int = 45) -> pd.DataFrame:
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP,
    )
    execution_id = response["QueryExecutionId"]
    start_ts = time.time()
    while True:
        execution = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = execution["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"): break
        if time.time() - start_ts > timeout_sec: raise RuntimeError("Query Timeout")
        time.sleep(1)
    if state != "SUCCEEDED": raise RuntimeError(f"Athena error: {state}")
    bucket, key = s3_uri_to_bucket_key(execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"])
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))

# =========================================================
# DATA LOADERS
# =========================================================
@st.cache_data(ttl=300)
def load_store_list():
    return run_athena_query("SELECT DISTINCT store_id FROM nstags_dashboard_metrics_canonical ORDER BY store_id")

@st.cache_data(ttl=300)
def load_available_dates(sid):
    return run_athena_query(f"SELECT DISTINCT metric_date FROM nstags_dashboard_metrics_canonical WHERE store_id = '{validate_store_id(sid)}' ORDER BY metric_date DESC")

@st.cache_data(ttl=300)
def load_dashboard_data(sid, start, end):
    return run_athena_query(f"SELECT * FROM nstags_dashboard_metrics_canonical WHERE store_id = '{sid}' AND metric_date BETWEEN DATE '{start}' AND DATE '{end}' ORDER BY metric_date")

@st.cache_data(ttl=300)
def load_hourly_traffic(sid, start, end):
    return run_athena_query(f"SELECT hour_of_day, format('%02d:00', hour_of_day) as hour_label, AVG(avg_far_devices) as far, AVG(avg_near_devices) as near FROM nstags_hourly_traffic_pretty_canonical WHERE store_id = '{sid}' AND metric_date BETWEEN DATE '{start}' AND DATE '{end}' GROUP BY hour_of_day ORDER BY hour_of_day")

@st.cache_data(ttl=300)
def load_index_scores(sid, start, end):
    return run_athena_query(f"SELECT AVG(traffic_intelligence_index) as tii, AVG(visit_quality_index) as vqi, AVG(store_attraction_index) as sai, AVG(audience_quality_index) as aqi FROM nstags_index_scores_dynamic_canonical WHERE store_id = '{sid}' AND metric_date BETWEEN DATE '{start}' AND DATE '{end}'")

# =========================================================
# AI ANALYTICS
# =========================================================
def generate_ai_brief(payload):
    if not GEMINI_API_KEY or genai is None: return "AI Insight unavailable."
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        prompt = f"Analyze these retail metrics for store {payload['sid']}: Visits: {payload['visits']}, Conv: {payload['conv']}%. Provide 3 bullet points on performance and bottlenecks."
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return response.text
    except: return "AI currently offline."

# =========================================================
# MAIN APP
# =========================================================
st.markdown("<div class='hero'><h1>nsTags Intelligence</h1><p>Retail Performance & Media Attribution Dashboard</p></div>", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### Configuration")
    stores = load_store_list()
    selected_store = st.selectbox("Store", stores["store_id"].tolist())
    dates = load_available_dates(selected_store)
    date_range = st.date_input("Period", [pd.to_datetime(dates.iloc[-1][0]).date(), pd.to_datetime(dates.iloc[0][0]).date()])
    
    st.divider()
    trans = st.number_input("Transactions", value=35)
    rev = st.number_input("Revenue", value=45000)

if len(date_range) == 2:
    start_str, end_str = date_range[0].isoformat(), date_range[1].isoformat()
    
    # Load Data
    daily_df = load_dashboard_data(selected_store, start_str, end_str)
    hourly_df = load_hourly_traffic(selected_store, start_str, end_str)
    idx_df = load_index_scores(selected_store, start_str, end_str)
    
    if not daily_df.empty:
        # Prep Metrics
        visits = daily_df["store_visits"].sum()
        qual = daily_df["qualified_footfall"].sum()
        conv = safe_div(trans, visits)
        idx_row = idx_df.iloc[0]

        # AI BRIEF
        with st.expander("✨ Executive AI Brief", expanded=True):
            st.markdown(generate_ai_brief({"sid": selected_store, "visits": visits, "conv": conv*100}))

        # INDEX RAIL
        st.markdown("### nsTags Index Rail")
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_card("Traffic Intel", f"{idx_row['tii']:.0f}", "Network Health", idx_row['tii'])
        with c2: render_card("Visit Quality", f"{idx_row['vqi']:.0f}", "Dwell Depth", idx_row['vqi'])
        with c3: render_card("Store Attraction", f"{idx_row['sai']:.0f}", "Capture Rate", idx_row['sai'])
        with c4: render_card("Audience Quality", f"{idx_row['aqi']:.0f}", "Premium Mix", idx_row['aqi'])

        # KPI RAIL
        st.markdown("### Executive KPIs")
        k1, k2, k3, k4, k5 = st.columns(5)
        with k1: render_card("Walk-by", fmt_float(daily_df["walk_by_traffic"].mean()), "Signal Index")
        with k2: render_card("Visits", fmt_int(visits), "Total Entries")
        with k3: render_card("Qualified", fmt_pct(safe_div(qual, visits)), "Visits > 30s")
        with k4: render_card("Avg Dwell", fmt_seconds(daily_df["avg_dwell_seconds"].mean()), "Engagement Time")
        with k5: render_card("Conversion", fmt_pct(conv), "Visit-to-Sale")

        # CHARTS
        st.markdown("---")
        t1, t2, t3 = st.tabs(["📈 Trends", "🎯 Funnel", "🚦 Patterns"])
        
        with t1:
            fig = px.line(daily_df, x="metric_date", y="store_visits", title="Daily Visit Trend", markers=True)
            st.plotly_chart(style_chart(fig), use_container_width=True)

        with t2:
            st.markdown("#### Conversion Stages")
            
            f_fig = go.Figure(go.Funnel(
                y=["Visits", "Qualified", "Sales"],
                x=[visits, qual, trans],
                textinfo="value+percent initial",
                marker={"color": ["#4285F4", "#34A853", "#FBBC04"]}
            ))
            st.plotly_chart(style_chart(f_fig), use_container_width=True)

        with t3:
            h_fig = px.area(hourly_df, x="hour_label", y=["far", "near"], title="Intraday Signal Intensity", line_shape='spline')
            st.plotly_chart(style_chart(h_fig), use_container_width=True)

st.markdown("<div style='text-align:center; padding:40px; color:#5f6368; font-size:0.8rem;'>nsTags Analytics Platform • v2026.1</div>", unsafe_allow_html=True)
