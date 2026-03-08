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
    page_title="nsTags | Command Center",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================================================
# PREMIUM ENTERPRISE DARK THEME
# =========================================================
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

:root {
    --bg-main: #0B0F19;
    --bg-surface: #111827;
    --bg-surface-2: #1F2937;
    --border: #374151;
    --text-primary: #F9FAFB;
    --text-secondary: #9CA3AF;
    --text-muted: #6B7280;
    
    --accent-blue: #3B82F6;
    --accent-indigo: #6366F1;
    --accent-green: #10B981;
    --accent-yellow: #F59E0B;
    --accent-red: #EF4444;
}

/* Base resets */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background-color: var(--bg-main) !important;
    color: var(--text-primary) !important;
}

/* Main Container Spacing */
.main .block-container {
    padding: 2rem 3rem 4rem 3rem !important;
    max-width: 1600px;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: var(--bg-surface) !important;
    border-right: 1px solid var(--border) !important;
}

/* Header */
.cc-header {
    margin-bottom: 2rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid var(--border);
}
.cc-header h1 {
    font-size: 2.2rem;
    font-weight: 700;
    color: var(--text-primary);
    margin: 0 0 0.5rem 0;
    letter-spacing: -0.02em;
}
.cc-header p {
    color: var(--text-secondary);
    font-size: 1.05rem;
    margin: 0;
}

/* Primary Alert */
.cc-alert {
    background: rgba(245, 158, 11, 0.1);
    border-left: 4px solid var(--accent-yellow);
    padding: 1rem 1.5rem;
    border-radius: 0 8px 8px 0;
    margin-bottom: 2rem;
    display: flex;
    align-items: center;
    gap: 12px;
}
.cc-alert strong { color: var(--accent-yellow); }

/* KPI Cards Flex Grid - Prevents Overlap on Mobile */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 1.2rem;
    margin-bottom: 2rem;
}

.cc-card {
    background-color: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.25rem;
    transition: transform 0.2s, border-color 0.2s;
}
.cc-card:hover {
    border-color: var(--accent-indigo);
    transform: translateY(-2px);
}
.cc-card.outcome {
    background: linear-gradient(180deg, rgba(31,41,55,0.4) 0%, var(--bg-surface) 100%);
    border-top: 2px solid var(--accent-blue);
}
.cc-card.diagnostic {
    border-top: 2px solid var(--text-muted);
}

.card-title {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-weight: 600;
    color: var(--text-secondary);
    margin-bottom: 0.5rem;
}
.card-value {
    font-size: 2rem;
    font-weight: 700;
    color: var(--text-primary);
    line-height: 1.2;
}
.card-sub {
    font-size: 0.8rem;
    color: var(--text-muted);
    margin-top: 0.5rem;
}

/* Badges */
.badge {
    display: inline-block;
    padding: 0.15rem 0.5rem;
    border-radius: 6px;
    font-size: 0.7rem;
    font-weight: 600;
    margin-bottom: 0.5rem;
}
.badge.good { background: rgba(16,185,129,0.15); color: var(--accent-green); }
.badge.warn { background: rgba(245,158,11,0.15); color: var(--accent-yellow); }
.badge.bad  { background: rgba(239,68,68,0.15); color: var(--accent-red); }

/* Section Titles */
.section-title {
    font-size: 1.2rem;
    font-weight: 600;
    color: var(--text-primary);
    margin: 2.5rem 0 1rem 0;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid var(--border);
}

/* Mobile Scrollable Tabs */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 1px solid var(--border);
    gap: 2rem;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    white-space: nowrap;
    padding-bottom: 0px;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    border: none !important;
    color: var(--text-secondary) !important;
    font-weight: 500 !important;
    padding: 1rem 0 !important;
}
.stTabs [aria-selected="true"] {
    color: var(--accent-blue) !important;
    border-bottom: 2px solid var(--accent-blue) !important;
}

@media (max-width: 768px) {
    .main .block-container { padding: 1rem !important; }
    .kpi-grid { grid-template-columns: repeat(2, 1fr); }
    .card-value { font-size: 1.5rem; }
}
@media (max-width: 480px) {
    .kpi-grid { grid-template-columns: 1fr; }
}
</style>
""",
    unsafe_allow_html=True,
)

# =========================================================
# CONFIG & AWS
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
        return float(a) / float(b) if float(b) != 0 else 0.0
    except: return 0.0

def fmt_int(x): return f"{int(round(float(x))):,}" if pd.notnull(x) else "0"
def fmt_float(x, d=2): return f"{float(x):,.{d}f}" if pd.notnull(x) else "0"
def fmt_currency(x): return f"₹{float(x):,.0f}" if pd.notnull(x) else "₹0"
def fmt_pct(x, d=1): return f"{100 * float(x):.{d}f}%" if pd.notnull(x) else "0.0%"

def score_band(score):
    try: s = float(score)
    except: return "bad", "Weak", "#EF4444"
    if s >= 75: return "good", "Strong", "#10B981"
    if s >= 50: return "warn", "Moderate", "#F59E0B"
    return "bad", "Weak", "#EF4444"

def render_html_card(title, value, sub, style_class="outcome", score=None):
    badge_html = ""
    if score is not None:
        b_class, b_text, _ = score_band(score)
        badge_html = f"<div class='badge {b_class}'>{b_text}</div>"
        
    return f"""
    <div class="cc-card {style_class}">
        {badge_html}
        <div class="card-title">{title}</div>
        <div class="card-value">{value}</div>
        <div class="card-sub">{sub}</div>
    </div>
    """

def style_dark_chart(fig):
    fig.update_layout(
        font_family="Inter",
        font_color="#9CA3AF",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=40, b=40, l=20, r=20),
        hoverlabel=dict(bgcolor="#1F2937", font_size=12, font_family="Inter"),
        colorway=["#3B82F6", "#10B981", "#8B5CF6", "#F59E0B", "#EF4444", "#6366F1"]
    )
    fig.update_xaxes(showgrid=False, linecolor="#374151", gridcolor="#1F2937")
    fig.update_yaxes(showgrid=True, linecolor="#374151", gridcolor="#1F2937", zerolinecolor="#374151")
    return fig

# =========================================================
# ATHENA RUNNER & LOADERS (Preserved from original)
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
        raise RuntimeError(f"Athena start error: {e}")

    execution_id = response["QueryExecutionId"]
    start_ts = time.time()

    while True:
        execution = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = execution["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"): break
        if time.time() - start_ts > timeout_sec: raise RuntimeError("Athena query timed out")
        time.sleep(1)

    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena query failed: {state}")

    bucket, key = s3_uri_to_bucket_key(execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"])
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))

@st.cache_data(ttl=300)
def load_store_list() -> pd.DataFrame:
    return run_athena_query("SELECT DISTINCT store_id FROM nstags_dashboard_metrics_canonical ORDER BY store_id")

@st.cache_data(ttl=300)
def load_available_dates(store_id: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(f"""
        WITH all_dates AS (
            SELECT DISTINCT metric_date FROM nstags_dashboard_metrics_canonical WHERE store_id = '{sid}'
            UNION SELECT DISTINCT metric_date FROM nstags_hourly_traffic_pretty_canonical WHERE store_id = '{sid}'
            UNION SELECT DISTINCT metric_date FROM nstags_dwell_buckets_canonical WHERE store_id = '{sid}'
        )
        SELECT metric_date FROM all_dates WHERE metric_date IS NOT NULL ORDER BY metric_date DESC
    """)

@st.cache_data(ttl=300)
def load_dashboard_daily_rows(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(f"SELECT * FROM nstags_dashboard_metrics_canonical WHERE store_id = '{sid}' AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}' ORDER BY metric_date")

@st.cache_data(ttl=300)
def load_hourly_traffic_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(f"""
        SELECT hour_of_day, format('%02d:00', hour_of_day) AS hour_label,
            ROUND(AVG(avg_far_devices), 2) AS avg_far_devices,
            ROUND(AVG(avg_mid_devices), 2) AS avg_mid_devices,
            ROUND(AVG(avg_near_devices), 2) AS avg_near_devices,
            ROUND(AVG(avg_apple_devices), 2) AS avg_apple_devices,
            ROUND(AVG(avg_samsung_devices), 2) AS avg_samsung_devices,
            ROUND(AVG(avg_other_devices), 2) AS avg_other_devices
        FROM nstags_hourly_traffic_pretty_canonical
        WHERE store_id = '{sid}' AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        GROUP BY hour_of_day ORDER BY hour_of_day
    """)

@st.cache_data(ttl=300)
def load_dwell_buckets_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(f"SELECT dwell_bucket, ROUND(SUM(visits), 2) AS visits FROM nstags_dwell_buckets_canonical WHERE store_id = '{sid}' AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}' GROUP BY dwell_bucket")

@st.cache_data(ttl=300)
def load_dynamic_index_scores_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(f"SELECT AVG(traffic_intelligence_index) as traffic_intelligence_index, AVG(visit_quality_index) as visit_quality_index, AVG(store_attraction_index) as store_attraction_index, AVG(audience_quality_index) as audience_quality_index, AVG(store_magnet_percentile_score) as store_magnet_percentile_score, AVG(window_capture_score) as window_capture_score, AVG(entry_efficiency_percentile_score) as entry_efficiency_percentile_score, AVG(dwell_quality_score) as dwell_quality_score FROM nstags_index_scores_dynamic_canonical WHERE store_id = '{sid}' AND metric_date BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'")

@st.cache_data(ttl=300)
def generate_ai_brief(payload: dict) -> str:
    if not GEMINI_API_KEY or genai is None: return "AI Insight unavailable. Configure GEMINI_API_KEY."
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        prompt = f"""
        Act as an Executive Retail Director. Write a concise 3-bullet briefing on this store's performance.
        Metrics: Visits {payload['visits']}, Conversion {payload['sales_conversion']}%, Primary Bottleneck: {payload['bottleneck']}.
        Focus on the bottleneck and recommend one operational action. Use Markdown. Keep it strictly under 100 words.
        """
        response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        return response.text
    except: return "AI processing temporarily unavailable."

# =========================================================
# SIDEBAR CONTROLS
# =========================================================
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/c/c1/Google_%22G%22_logo.svg", width=40) # Mock logo
    st.markdown("### Parameters")
    
    try: stores_df = load_store_list()
    except Exception as e: st.error("Database connection failed."); st.stop()
    
    selected_store = st.selectbox("Operating Store", stores_df["store_id"].dropna().astype(str).tolist())
    
    try: dates_df = load_available_dates(selected_store)
    except: st.stop()
    
    if dates_df.empty: st.warning("No data for store."); st.stop()
    dates_df["metric_date"] = pd.to_datetime(dates_df["metric_date"]).dt.date
    avail_dates = sorted(set(dates_df["metric_date"].dropna().tolist()))
    
    period_mode = st.radio("Analysis Window", ["Daily", "Weekly", "Monthly", "Custom"])
    
    if period_mode == "Daily":
        end_date = st.selectbox("Select Date", list(reversed(avail_dates)), index=0)
        start_date = end_date
    elif period_mode == "Weekly":
        end_date = st.date_input("End Date", value=avail_dates[-1], min_value=avail_dates[0], max_value=avail_dates[-1])
        start_date = max(avail_dates[0], end_date - timedelta(days=6))
    else:
        # Default fallback logic for Monthly/Custom
        dr = st.date_input("Range", value=(max(avail_dates[0], avail_dates[-1] - timedelta(days=29)), avail_dates[-1]), max_value=avail_dates[-1])
        if len(dr) == 2: start_date, end_date = dr
        else: start_date, end_date = dr[0], avail_dates[-1]

    st.markdown("### Commercial Inputs")
    transactions = st.number_input("POS Transactions", min_value=0, value=35)
    value = st.number_input("Recognized Revenue", min_value=0, value=45000)

# =========================================================
# DATA FETCH & PREP
# =========================================================
start_str, end_str = start_date.isoformat(), end_date.isoformat()

try:
    daily_df = load_dashboard_daily_rows(selected_store, start_str, end_str)
    hourly_df = load_hourly_traffic_range(selected_store, start_str, end_date_str=end_str)
    dwell_df = load_dwell_buckets_range(selected_store, start_str, end_str)
    dyn_df = load_dynamic_index_scores_range(selected_store, start_str, end_str)
except Exception as e:
    st.error(f"Data loading failed: {e}")
    st.stop()

if daily_df.empty:
    st.warning("No metrics found for selected period.")
    st.stop()

# Aggregate Calculations
visits = daily_df["store_visits"].sum()
qual_visits = daily_df["qualified_footfall"].sum()
eng_visits = daily_df["engaged_visits"].sum()
walk_by = daily_df["walk_by_traffic"].mean()
interest = daily_df["store_interest"].mean()
near_store = daily_df["near_store"].mean()

sales_conv = safe_div(transactions, visits)
floor_conv = min(sales_conv * 400, 100) # Using original logic

score_row = dyn_df.iloc[0].to_dict() if not dyn_df.empty else {}
tii = float(score_row.get("traffic_intelligence_index", 0) or 0)
vqi = float(score_row.get("visit_quality_index", 0) or 0)
sai = float(score_row.get("store_attraction_index", 0) or 0)
aqi = float(score_row.get("audience_quality_index", 0) or 0)

bottlenecks = {
    "Store Magnet": float(score_row.get("store_magnet_percentile_score", 0) or 0),
    "Window Capture": float(score_row.get("window_capture_score", 0) or 0),
    "Entry Efficiency": float(score_row.get("entry_efficiency_percentile_score", 0) or 0),
    "Dwell Quality": float(score_row.get("dwell_quality_score", 0) or 0)
}
primary_bottleneck = min(bottlenecks, key=bottlenecks.get) if bottlenecks else "Traffic Volume"

# =========================================================
# 1. HEADER
# =========================================================
st.markdown(f"""
<div class="cc-header">
    <h1>Retail Intelligence Command Center</h1>
    <p>Operating Mode: Enterprise Operations | Store ID: {selected_store} | Scope: {start_date.strftime('%d %b')} - {end_date.strftime('%d %b %Y')}</p>
</div>
""", unsafe_allow_html=True)

# =========================================================
# 2. PRIMARY ALERT
# =========================================================
st.markdown(f"""
<div class="cc-alert">
    <div style="font-size: 1.2rem;">⚠️</div>
    <div><strong>Diagnostic Alert:</strong> The primary system bottleneck identified for this period is <strong>{primary_bottleneck}</strong>. Review Funnel Zone to optimize conversion stages.</div>
</div>
""", unsafe_allow_html=True)

# =========================================================
# 3. OUTCOME ROW
# =========================================================
st.markdown("<div class='section-title'>Business Outcomes</div>", unsafe_allow_html=True)
out_cols = st.columns(4)
with out_cols[0]: st.markdown(render_html_card("Recognized Revenue", fmt_currency(value), "Commercial Output", "outcome"), unsafe_allow_html=True)
with out_cols[1]: st.markdown(render_html_card("Sales Conversion", fmt_pct(sales_conv), "Transactions / Visits", "outcome"), unsafe_allow_html=True)
with out_cols[2]: st.markdown(render_html_card("Total Transactions", fmt_int(transactions), "Validated POS", "outcome"), unsafe_allow_html=True)
with out_cols[3]: st.markdown(render_html_card("Floor Conversion Score", f"{floor_conv:.0f}/100", "Efficiency Index", "outcome", floor_conv), unsafe_allow_html=True)

# =========================================================
# 4. DIAGNOSTIC ROW
# =========================================================
st.markdown("<div class='section-title'>Diagnostic Indices</div>", unsafe_allow_html=True)
diag_cols = st.columns(4)
with diag_cols[0]: st.markdown(render_html_card("Traffic Intel Index", f"{tii:.0f}", "Network Health", "diagnostic", tii), unsafe_allow_html=True)
with diag_cols[1]: st.markdown(render_html_card("Visit Quality Index", f"{vqi:.0f}", "Engagement Depth", "diagnostic", vqi), unsafe_allow_html=True)
with diag_cols[2]: st.markdown(render_html_card("Store Attraction", f"{sai:.0f}", "Capture Efficiency", "diagnostic", sai), unsafe_allow_html=True)
with diag_cols[3]: st.markdown(render_html_card("Audience Quality", f"{aqi:.0f}", "Premium Signal Mix", "diagnostic", aqi), unsafe_allow_html=True)

# =========================================================
# 5. FUNNEL ZONE (Side-by-Side Dual Funnels)
# =========================================================
st.markdown("<div class='section-title'>Friction Analysis (Dual Funnel Architecture)</div>", unsafe_allow_html=True)
f_col1, f_col2 = st.columns(2)

with f_col1:
    st.markdown("**1. Attraction Pipeline** (Signal Intensity)")
    fig_attr = go.Figure(go.Funnel(
        y=["Walk-by Zone", "Interest Zone", "Near Store", "Store Entry"],
        x=[walk_by, interest, near_store, visits],
        textinfo="value+percent initial",
        marker={"color": ["#374151", "#4B5563", "#6B7280", "#3B82F6"]},
        connector={"line": {"color": "#374151", "width": 2}}
    ))
    st.plotly_chart(style_dark_chart(fig_attr), use_container_width=True, config=PLOT_CONFIG)

with f_col2:
    st.markdown("**2. Conversion Pipeline** (Physical Output)")
    fig_conv = go.Figure(go.Funnel(
        y=["Store Entry", "Qualified (>30s)", "Engaged", "Transactions"],
        x=[visits, qual_visits, eng_visits, transactions],
        textinfo="value+percent initial",
        marker={"color": ["#3B82F6", "#10B981", "#8B5CF6", "#F59E0B"]},
        connector={"line": {"color": "#374151", "width": 2}}
    ))
    st.plotly_chart(style_dark_chart(fig_conv), use_container_width=True, config=PLOT_CONFIG)

# =========================================================
# 6. TREND ZONE & 7. WEEKDAY BENCHMARK
# =========================================================
st.markdown("<div class='section-title'>Temporal Diagnostics</div>", unsafe_allow_html=True)
t_col1, t_col2 = st.columns([3, 2])

with t_col1:
    if period_mode == "Daily" and not hourly_df.empty:
        st.markdown("**Intraday Traffic Signals**")
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(x=hourly_df["hour_label"], y=hourly_df["avg_far_devices"], name="Walk-by", fill='tozeroy', line=dict(color="#374151")))
        fig_trend.add_trace(go.Scatter(x=hourly_df["hour_label"], y=hourly_df["avg_near_devices"], name="Near Store", line=dict(color="#3B82F6", width=3)))
        st.plotly_chart(style_dark_chart(fig_trend), use_container_width=True, config=PLOT_CONFIG)
    else:
        st.markdown("**Period Visit Trend**")
        fig_trend = px.line(daily_df, x="metric_date", y="store_visits", markers=True)
        fig_trend.update_traces(line_color="#3B82F6", line_width=3, marker=dict(size=8))
        st.plotly_chart(style_dark_chart(fig_trend), use_container_width=True, config=PLOT_CONFIG)

with t_col2:
    st.markdown("**Weekday Benchmark** (Volume vs Signal)")
    if not daily_df.empty:
        df_wd = daily_df.copy()
        df_wd['metric_date'] = pd.to_datetime(df_wd['metric_date'])
        df_wd['weekday'] = df_wd['metric_date'].dt.day_name().str[:3]
        wd_grp = df_wd.groupby('weekday', as_index=False)[['store_visits', 'walk_by_traffic']].mean()
        # Sort by standard week
        cats = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        wd_grp['weekday'] = pd.Categorical(wd_grp['weekday'], categories=cats, ordered=True)
        wd_grp = wd_grp.sort_values('weekday')
        
        fig_wd = go.Figure()
        fig_wd.add_trace(go.Bar(x=wd_grp["weekday"], y=wd_grp["store_visits"], name="Avg Visits", marker_color="#3B82F6"))
        fig_wd.add_trace(go.Scatter(x=wd_grp["weekday"], y=wd_grp["walk_by_traffic"], name="Walk-by Idx", yaxis="y2", line=dict(color="#10B981", width=3)))
        fig_wd.update_layout(yaxis2=dict(overlaying="y", side="right", showgrid=False))
        st.plotly_chart(style_dark_chart(fig_wd), use_container_width=True, config=PLOT_CONFIG)

# =========================================================
# 8. VISIT QUALITY + AUDIENCE
# =========================================================
st.markdown("<div class='section-title'>Audience Profiling</div>", unsafe_allow_html=True)
q_col1, q_col2 = st.columns(2)

with q_col1:
    st.markdown("**Dwell Depth Distribution**")
    if not dwell_df.empty:
        dwell_order = ["00-10s", "10-30s", "30-60s", "01-03m", "03-05m", "05m+"]
        dwell_df['dwell_bucket'] = pd.Categorical(dwell_df['dwell_bucket'], categories=dwell_order, ordered=True)
        fig_dw = px.bar(dwell_df.sort_values("dwell_bucket"), x="dwell_bucket", y="visits", color_discrete_sequence=["#8B5CF6"])
        st.plotly_chart(style_dark_chart(fig_dw), use_container_width=True, config=PLOT_CONFIG)

with q_col2:
    st.markdown("**Device Mix Intelligence** (Hourly)")
    if not hourly_df.empty:
        fig_mix = go.Figure()
        fig_mix.add_trace(go.Bar(x=hourly_df["hour_label"], y=hourly_df["avg_apple_devices"], name="Apple", marker_color="#F9FAFB"))
        fig_mix.add_trace(go.Bar(x=hourly_df["hour_label"], y=hourly_df["avg_samsung_devices"], name="Samsung", marker_color="#3B82F6"))
        fig_mix.add_trace(go.Bar(x=hourly_df["hour_label"], y=hourly_df["avg_other_devices"], name="Other", marker_color="#374151"))
        fig_mix.update_layout(barmode="stack")
        st.plotly_chart(style_dark_chart(fig_mix), use_container_width=True, config=PLOT_CONFIG)

# =========================================================
# 9. EXECUTIVE AI BRIEF
# =========================================================
st.markdown("<div class='section-title'>Synthesized AI Directives</div>", unsafe_allow_html=True)
with st.expander("Expand Intelligence Brief", expanded=False):
    payload = {
        "visits": visits,
        "sales_conversion": round(sales_conv * 100, 1),
        "bottleneck": primary_bottleneck
    }
    st.markdown(f"<div style='color: var(--text-secondary); line-height: 1.6;'>{generate_ai_brief(payload)}</div>", unsafe_allow_html=True)

# Footer
st.markdown("<br><hr style='border-color: #374151;'><p style='text-align: center; color: #6B7280; font-size: 0.8rem;'>nsTags Enterprise Command Center v3.0 | Confidential Operations</p>", unsafe_allow_html=True)
