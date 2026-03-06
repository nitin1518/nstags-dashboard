import streamlit as st
import boto3
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from google import genai

# ==========================================
# PAGE CONFIG
# ==========================================
st.set_page_config(
    page_title="nsTags | Retail Intelligence",
    page_icon="📈",
    layout="wide"
)

# ==========================================
# PREMIUM UI / UX
# ==========================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"]  {
    font-family: 'Inter', sans-serif;
}

section.main > div {
    padding-top: 1rem;
}

.block-container {
    padding-top: 1.2rem;
    padding-bottom: 2rem;
    max-width: 1500px;
}

@keyframes slideUpFade {
    from { opacity: 0; transform: translateY(12px); }
    to { opacity: 1; transform: translateY(0); }
}
.animate-container {
    animation: slideUpFade 0.55s cubic-bezier(0.16, 1, 0.3, 1) forwards;
}

.hero-shell {
    background: linear-gradient(135deg, rgba(26,115,232,0.10) 0%, rgba(52,168,83,0.05) 100%);
    border: 1px solid rgba(26,115,232,0.18);
    border-radius: 18px;
    padding: 1.2rem 1.4rem;
    margin-bottom: 1rem;
}

.hero-title {
    font-size: 1.8rem;
    font-weight: 800;
    margin-bottom: 0.2rem;
    color: #1f1f1f;
}

.hero-sub {
    color: #5f6368;
    font-size: 0.98rem;
    margin-bottom: 0;
}

.kpi-card {
    background: white;
    border: 1px solid rgba(95,99,104,0.12);
    border-radius: 16px;
    padding: 1rem 1rem 0.9rem 1rem;
    box-shadow: 0 2px 10px rgba(0,0,0,0.03);
    height: 100%;
}

.kpi-label {
    font-size: 0.78rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #5f6368;
    margin-bottom: 0.45rem;
}

.kpi-value {
    font-size: 2rem;
    font-weight: 800;
    color: #1f1f1f;
    line-height: 1.1;
}

.kpi-sub {
    font-size: 0.9rem;
    color: #5f6368;
    margin-top: 0.3rem;
}

.verdict-good { color: #188038; font-weight: 700; }
.verdict-warn { color: #b06000; font-weight: 700; }
.verdict-bad  { color: #d93025; font-weight: 700; }

.insight-card {
    background: white;
    border: 1px solid rgba(95,99,104,0.12);
    border-left: 5px solid #1a73e8;
    border-radius: 16px;
    padding: 1rem 1.1rem;
    box-shadow: 0 2px 10px rgba(0,0,0,0.03);
    min-height: 220px;
}

.insight-title {
    font-size: 0.85rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #5f6368;
    font-weight: 800;
    margin-bottom: 0.55rem;
}

.insight-headline {
    font-size: 1.35rem;
    font-weight: 800;
    color: #1f1f1f;
    line-height: 1.2;
    margin-bottom: 0.7rem;
}

.insight-body {
    font-size: 0.96rem;
    color: #3c4043;
    line-height: 1.55;
}

.mono-box {
    background: rgba(95,99,104,0.06);
    border-radius: 10px;
    padding: 0.7rem 0.8rem;
    margin-top: 0.8rem;
    font-family: 'Courier New', monospace;
    font-size: 0.82rem;
    color: #3c4043;
}

.section-title {
    font-size: 1.15rem;
    font-weight: 800;
    color: #1f1f1f;
    margin-top: 0.4rem;
    margin-bottom: 0.8rem;
}

.chart-caption {
    font-size: 0.88rem;
    color: #5f6368;
    text-align: center;
    margin-top: -6px;
    margin-bottom: 14px;
    font-style: italic;
}

div[data-testid="stInfo"] {
    background: linear-gradient(145deg, #f8fafd 0%, #ffffff 100%);
    border: 1px solid rgba(26, 115, 232, 0.18);
    border-left: 6px solid #1a73e8;
    border-radius: 14px;
    padding: 1rem 1.3rem;
    box-shadow: 0 4px 14px rgba(0,0,0,0.04);
}

div[data-testid="stMetric"] {
    background: white;
    border: 1px solid rgba(95,99,104,0.10);
    border-radius: 14px;
    padding: 0.7rem 0.8rem;
}

[data-testid="stMetricValue"] {
    font-size: 1.6rem;
    font-weight: 800;
}

[data-testid="stMetricLabel"] {
    font-size: 0.82rem;
    text-transform: uppercase;
    letter-spacing: 0.07em;
}

div[role="radiogroup"] {
    background: rgba(95,99,104,0.04);
    border: 1px solid rgba(95,99,104,0.10);
    border-radius: 10px;
    padding: 5px;
}
</style>
""", unsafe_allow_html=True)

PLOT_CONFIG = {"displayModeBar": False}

# ==========================================
# AWS CONFIG
# ==========================================
BUCKET_NAME = "nstags-datalake-hq-2026"
REGION = "ap-south-1"

# ==========================================
# HELPERS
# ==========================================
def safe_div(a, b):
    return a / b if b not in [0, None] else 0

def pct(a, b):
    return safe_div(a, b) * 100

def fmt_int(x):
    return f"{int(round(x)):,}"

def fmt_currency(x):
    return f"₹{x:,.0f}"

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

def style_chart(fig):
    fig.update_layout(
        hovermode="x unified",
        margin=dict(l=10, r=10, t=35, b=50),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.18,
            xanchor="center",
            x=0.5
        ),
        font=dict(family="Inter, sans-serif", color="#5f6368")
    )
    fig.update_xaxes(showgrid=False, zeroline=False, title_text="")
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(95,99,104,0.10)",
        zeroline=False,
        title_text=""
    )
    return fig

# ==========================================
# DATA LOAD
# ==========================================
@st.cache_data(ttl=45)
def load_s3_data():
    try:
        s3 = boto3.client(
            "s3",
            region_name=REGION,
            aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"]
        )
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="footfall/")
        if "Contents" not in response:
            return pd.DataFrame()

        files = sorted(
            response["Contents"],
            key=lambda x: x["LastModified"],
            reverse=True
        )[:150]

    except Exception:
        return pd.DataFrame()

    all_records = []

    for file in files:
        try:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=file["Key"])
            lines = obj["Body"].read().decode("utf-8").strip().split("\n")

            for line in lines:
                if not line:
                    continue

                try:
                    data = json.loads(line)
                    end_time = datetime.fromtimestamp(data["timestamp"] / 1000.0) + timedelta(hours=5, minutes=30)
                    snapshots = data.get("D", [])
                    start_time = end_time - timedelta(seconds=len(snapshots) * 5)

                    for idx, snap in enumerate(snapshots):
                        if len(snap) < 20:
                            continue

                        ts = start_time + timedelta(seconds=idx * 5)

                        all_records.append({
                            "Store ID": data.get("S", "Unknown"),
                            "Time": ts,
                            "Date": ts.date(),
                            "Hour Label": ts.strftime("%H:00"),
                            "Weekday": ts.strftime("%A"),

                            "Street": snap[0],
                            "Window": snap[1],
                            "InStore": snap[2],

                            "Passersby (<10s)": snap[3],
                            "Window Shoppers (<30s)": snap[4],
                            "Explorers (<2m)": snap[5],
                            "Focused (<5m)": snap[6],
                            "Engaged (<10m)": snap[7],
                            "Potential (<20m)": snap[8],
                            "Committed (<30m)": snap[9],
                            "Enthusiasts (<45m)": snap[10],
                            "Deep (<1h)": snap[11],
                            "Loyal (>1h)": snap[12],

                            "Bounced": snap[3] + snap[4],
                            "Browsed": snap[5] + snap[6] + snap[7],
                            "Retained": sum(snap[8:13]),

                            "Apple": snap[17],
                            "Samsung": snap[18],
                            "Other": snap[19],
                        })

                except Exception:
                    pass

        except Exception:
            pass

    df = pd.DataFrame(all_records)
    if not df.empty:
        df = df.sort_values("Time").reset_index(drop=True)

    return df

# ==========================================
# BENCHMARK ENGINE
# ==========================================
def get_same_period_baseline(store_df, camp_start, camp_end, weeks_back=1):
    duration = camp_end - camp_start
    base_end = camp_end - timedelta(days=7 * weeks_back)
    base_start = base_end - duration
    base_df = store_df[(store_df["Time"] >= base_start) & (store_df["Time"] <= base_end)]
    return base_df, base_start, base_end

def aggregate_metrics(df_slice, transactions=0, revenue=0):
    if df_slice.empty:
        return {
            "street": 0,
            "window": 0,
            "instore": 0,
            "high_intent_activity": 0,
            "retained": 0,
            "apple": 0,
            "samsung": 0,
            "other": 0,
            "attention_rate": 0,
            "entry_rate": 0,
            "high_intent_rate": 0,
            "conversion_rate": 0,
            "aov": 0,
        }

    street = df_slice["Street"].sum()
    window = df_slice["Window"].sum()
    instore = df_slice["InStore"].sum()

    high_intent_activity = (
        df_slice["Focused (<5m)"].sum() +
        df_slice["Engaged (<10m)"].sum() +
        df_slice["Potential (<20m)"].sum() +
        df_slice["Committed (<30m)"].sum() +
        df_slice["Enthusiasts (<45m)"].sum() +
        df_slice["Deep (<1h)"].sum() +
        df_slice["Loyal (>1h)"].sum()
    )

    retained = df_slice["Retained"].sum()

    apple = df_slice["Apple"].sum()
    samsung = df_slice["Samsung"].sum()
    other = df_slice["Other"].sum()

    attention_rate = safe_div(window, street)
    entry_rate = safe_div(instore, window)
    high_intent_rate = safe_div(high_intent_activity, instore)
    conversion_rate = safe_div(transactions, instore)
    aov = safe_div(revenue, transactions)

    return {
        "street": street,
        "window": window,
        "instore": instore,
        "high_intent_activity": high_intent_activity,
        "retained": retained,
        "apple": apple,
        "samsung": samsung,
        "other": other,
        "attention_rate": attention_rate,
        "entry_rate": entry_rate,
        "high_intent_rate": high_intent_rate,
        "conversion_rate": conversion_rate,
        "aov": aov,
    }

# ==========================================
# AI ENGINE
# ==========================================
@st.cache_data(ttl=300)
def generate_ai_brief(ai_payload):
    try:
        api_key = st.secrets.get("GEMINI_API_KEY")
        if not api_key:
            return "⚠️ **AI unavailable:** GEMINI_API_KEY not configured in Streamlit Secrets."

        client = genai.Client(api_key=api_key)

        prompt = f"""
You are a top-tier retail strategy consultant analyzing storefront and in-store performance.

You are evaluating a campaign/store period with the following metrics:

MODE: {ai_payload['mode']}
DURATION_HOURS: {ai_payload['duration_hours']}

CURRENT PERIOD METRICS
- Exposure: {ai_payload['street']}
- Attention / Window Stops: {ai_payload['window']}
- Entries / Walk-ins: {ai_payload['instore']}
- High-Intent Activity: {ai_payload['high_intent_activity']}
- Transactions: {ai_payload['transactions']}
- Revenue: {ai_payload['revenue']}
- Attention Rate: {ai_payload['attention_rate']}%
- Entry Rate: {ai_payload['entry_rate']}%
- High-Intent Activity Rate: {ai_payload['high_intent_rate']}%
- Conversion Rate: {ai_payload['conversion_rate']}%
- AOV: {ai_payload['aov']}
- Dominant OS: {ai_payload['dominant_os']}

BENCHMARKS / BASELINE
- Baseline Exposure: {ai_payload['base_street']}
- Baseline Attention Rate: {ai_payload['base_attention_rate']}%
- Baseline Entry Rate: {ai_payload['base_entry_rate']}%
- Baseline High-Intent Activity Rate: {ai_payload['base_high_intent_rate']}%
- Baseline Conversion Rate: {ai_payload['base_conversion_rate']}%
- Baseline Available: {ai_payload['baseline_available']}

REFERENCE TARGETS
- Healthy Attention Rate Benchmark: 15%
- Healthy Entry Rate Benchmark: 35%
- Healthy Conversion Benchmark: 20%

TASK:
Classify the primary bottleneck into exactly one of these:
1. Awareness problem
2. Attention problem
3. Entry problem
4. Floor conversion problem
5. Healthy funnel

Then generate a sharp executive brief with this exact structure:

* **What happened:** [Use exact numbers]
* **Why it matters:** [Business diagnosis with comparison to baseline/benchmark]
* **Primary bottleneck:** [Choose one category above and justify]
* **Recommended action:** [Very concrete action for store / media / visual merchandising / floor staff]
* **OS-led optimization:** [Use dominant OS insight in a commercially meaningful way]

Rules:
- Keep it concise but premium.
- Use exact numbers from the payload.
- Do not describe High-Intent Activity as unique visitors unless explicitly stated.
- No HTML.
- Output only Markdown.
"""

        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        return response.text

    except Exception:
        return "⚠️ **AI unavailable:** temporary connection or rate-limit issue."

# ==========================================
# APP HEADER
# ==========================================
st.markdown("""
<div class="hero-shell">
    <div class="hero-title">nsTags Intelligence V3</div>
    <p class="hero-sub">Retail Intelligence • Storefront Media Measurement • Conversion Diagnostics</p>
</div>
""", unsafe_allow_html=True)

with st.spinner("Synchronizing with AWS Data Lake..."):
    df = load_s3_data()

if df.empty:
    st.warning("Awaiting telemetry. Ensure hardware nodes are online and S3 data is available.")
    st.stop()

# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("### Control Center")

    store_id = st.selectbox("Active Store", sorted(df["Store ID"].unique()))

    app_mode = st.radio(
        "Business Mode",
        ["Retail Ops", "Retail Media"],
        help="Retail Ops = store performance. Retail Media = campaign/storefront advertising measurement."
    )

    st.markdown("### Time Horizon")
    latest_time = df["Time"].max()

    time_preset = st.selectbox(
        "Quick Select",
        ["Today", "Yesterday", "Last 24 Hours", "This Week", "Custom Period"]
    )

    if time_preset == "Today":
        camp_start = latest_time.replace(hour=0, minute=0, second=0, microsecond=0)
        camp_end = latest_time
    elif time_preset == "Yesterday":
        y = latest_time - timedelta(days=1)
        camp_start = y.replace(hour=0, minute=0, second=0, microsecond=0)
        camp_end = y.replace(hour=23, minute=59, second=59, microsecond=0)
    elif time_preset == "Last 24 Hours":
        camp_start = latest_time - timedelta(hours=24)
        camp_end = latest_time
    elif time_preset == "This Week":
        camp_start = latest_time - timedelta(days=7)
        camp_end = latest_time
    else:
        c1, c2 = st.columns(2)
        start_date = c1.date_input("Start Date", latest_time.date() - timedelta(days=1))
        start_time = c2.time_input("Start Time", (latest_time - timedelta(hours=4)).time())

        c3, c4 = st.columns(2)
        end_date = c3.date_input("End Date", latest_time.date())
        end_time = c4.time_input("End Time", latest_time.time())

        camp_start = datetime.combine(start_date, start_time)
        camp_end = datetime.combine(end_date, end_time)

    duration_hours = max((camp_end - camp_start).total_seconds() / 3600, 0.01)

    st.markdown("### Commercial Inputs")

    if app_mode == "Retail Media":
        campaign_value = st.number_input("Campaign Value / Revenue (₹)", min_value=0, value=15000, step=1000)
        daily_revenue = st.number_input("Store Revenue (₹)", min_value=0, value=45000, step=1000)
        transactions = st.number_input("Store Transactions", min_value=0, value=35, step=1)
        compare_baseline = st.checkbox("Compare against prior same-period baseline", value=True)

    else:
        marketing_spend = st.number_input("Marketing Spend (₹)", min_value=0, value=5000, step=500)
        daily_revenue = st.number_input("Store Revenue (₹)", min_value=0, value=45000, step=1000)
        transactions = st.number_input("Store Transactions", min_value=0, value=35, step=1)
        compare_baseline = st.checkbox("Compare against prior same-period baseline", value=True)
        isolate_product = st.checkbox("Isolate product campaign revenue")
        if isolate_product:
            product_revenue = st.number_input("Attributed Product Revenue (₹)", min_value=0, value=12000, step=500)
            product_transactions = st.number_input("Attributed Product Transactions", min_value=0, value=8, step=1)

    st.markdown("### AI Layer")
    ai_enabled = st.checkbox("Enable AI executive brief", value=True)

# ==========================================
# FILTER DATA
# ==========================================
store_df = df[df["Store ID"] == store_id].copy()
camp_df = store_df[(store_df["Time"] >= camp_start) & (store_df["Time"] <= camp_end)].copy()

if camp_df.empty:
    st.warning("No records found for the selected store and time period.")
    st.stop()

if compare_baseline:
    base_df, base_start, base_end = get_same_period_baseline(store_df, camp_start, camp_end, weeks_back=1)
else:
    base_df = pd.DataFrame()
    base_start = None
    base_end = None

baseline_available = compare_baseline and not base_df.empty

curr = aggregate_metrics(camp_df, transactions=transactions, revenue=daily_revenue)

base = aggregate_metrics(base_df, transactions=transactions, revenue=daily_revenue) if baseline_available else {
    "street": 0,
    "window": 0,
    "instore": 0,
    "high_intent_activity": 0,
    "retained": 0,
    "apple": 0,
    "samsung": 0,
    "other": 0,
    "attention_rate": 0,
    "entry_rate": 0,
    "high_intent_rate": 0,
    "conversion_rate": 0,
    "aov": 0
}

# audience / os
if curr["apple"] > curr["samsung"] and curr["apple"] > curr["other"]:
    dominant_os = "Apple iOS"
elif curr["samsung"] >= curr["apple"] and curr["samsung"] > curr["other"]:
    dominant_os = "Samsung / Android"
else:
    dominant_os = "Mixed"

qualified_audience = curr["window"]
entries = curr["instore"]
high_intent_activity = curr["high_intent_activity"]

attention_rate = curr["attention_rate"]
entry_rate = curr["entry_rate"]
high_intent_rate = curr["high_intent_rate"]
conversion_rate = curr["conversion_rate"]
aov = curr["aov"]

baseline_entries = base["instore"] if baseline_available else 0
incremental_entries = max(0, entries - baseline_entries) if baseline_available else entries

if app_mode == "Retail Media":
    cost_per_high_intent = safe_div(campaign_value, high_intent_activity)
    cost_per_entry = safe_div(campaign_value, entries)
    effective_cpm = safe_div(campaign_value, curr["street"]) * 1000
else:
    attributed_revenue = product_revenue if ("isolate_product" in locals() and isolate_product) else (incremental_entries * conversion_rate * aov)
    roas = safe_div(attributed_revenue, marketing_spend)
    cost_per_entry = safe_div(marketing_spend, incremental_entries if incremental_entries > 0 else entries)
    cost_per_high_intent = safe_div(marketing_spend, high_intent_activity)

attention_class, attention_verdict = verdict_class(attention_rate * 100, 15, 8, higher_is_better=True)
entry_class, entry_verdict = verdict_class(entry_rate * 100, 35, 20, higher_is_better=True)
conversion_class, conversion_verdict = verdict_class(conversion_rate * 100, 20, 10, higher_is_better=True)

# ==========================================
# AI BRIEF
# ==========================================
if ai_enabled:
    ai_payload = {
        "mode": app_mode,
        "duration_hours": round(duration_hours, 1),
        "street": int(curr["street"]),
        "window": int(curr["window"]),
        "instore": int(curr["instore"]),
        "high_intent_activity": int(curr["high_intent_activity"]),
        "transactions": int(transactions),
        "revenue": fmt_currency(daily_revenue),
        "attention_rate": round(attention_rate * 100, 1),
        "entry_rate": round(entry_rate * 100, 1),
        "high_intent_rate": round(high_intent_rate * 100, 1),
        "conversion_rate": round(conversion_rate * 100, 1),
        "aov": fmt_currency(aov),
        "dominant_os": dominant_os,
        "base_street": int(base["street"]),
        "base_attention_rate": round(base["attention_rate"] * 100, 1),
        "base_entry_rate": round(base["entry_rate"] * 100, 1),
        "base_high_intent_rate": round(base["high_intent_rate"] * 100, 1),
        "base_conversion_rate": round(base["conversion_rate"] * 100, 1),
        "baseline_available": baseline_available,
    }

    with st.spinner("Generating executive synthesis..."):
        ai_text = generate_ai_brief(ai_payload)

    st.info(ai_text, icon="✨")

# ==========================================
# KPI RAIL
# ==========================================
st.markdown("<div class='animate-container'>", unsafe_allow_html=True)

st.markdown("<div class='section-title'>Executive KPI Rail</div>", unsafe_allow_html=True)
k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    label = "Qualified Audience" if app_mode == "Retail Media" else "Qualified Footfall"
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{fmt_int(qualified_audience)}</div>
        <div class="kpi-sub">Window-level attention pool</div>
    </div>
    """, unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Attention Rate</div>
        <div class="kpi-value">{attention_rate*100:.1f}%</div>
        <div class="kpi-sub"><span class="{attention_class}">{attention_verdict}</span> vs 15% benchmark</div>
    </div>
    """, unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Entry Rate</div>
        <div class="kpi-value">{entry_rate*100:.1f}%</div>
        <div class="kpi-sub"><span class="{entry_class}">{entry_verdict}</span> vs 35% benchmark</div>
    </div>
    """, unsafe_allow_html=True)

with k4:
    if app_mode == "Retail Media":
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Cost / High-Intent Activity</div>
            <div class="kpi-value">{fmt_currency(cost_per_high_intent)}</div>
            <div class="kpi-sub">Campaign value / high-intent activity</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Sales Conversion</div>
            <div class="kpi-value">{conversion_rate*100:.1f}%</div>
            <div class="kpi-sub"><span class="{conversion_class}">{conversion_verdict}</span> vs 20% benchmark</div>
        </div>
        """, unsafe_allow_html=True)

with k5:
    if app_mode == "Retail Media":
        if baseline_available and baseline_entries > 0:
            lift_pct = ((entries - baseline_entries) / baseline_entries) * 100
            lift_text = f"{lift_pct:.1f}%"
            lift_sub = "vs prior same-period baseline"
        else:
            lift_text = "N/A"
            lift_sub = "Baseline unavailable"
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Visit Lift</div>
            <div class="kpi-value">{lift_text}</div>
            <div class="kpi-sub">{lift_sub}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">ROAS</div>
            <div class="kpi-value">{roas:.1f}x</div>
            <div class="kpi-sub">Attributed revenue / marketing spend</div>
        </div>
        """, unsafe_allow_html=True)

# ==========================================
# DIAGNOSTIC INSIGHT ROW
# ==========================================
st.markdown("<div class='section-title'>Diagnostic Cards</div>", unsafe_allow_html=True)
d1, d2, d3 = st.columns(3)

with d1:
    st.markdown(f"""
    <div class="insight-card">
        <div class="insight-title">Acquisition</div>
        <div class="insight-headline">{attention_rate*100:.1f}% attention rate</div>
        <div class="insight-body">
            Out of <b>{fmt_int(curr["street"])}</b> exposure events, <b>{fmt_int(curr["window"])}</b> became attention-level interactions.
            This indicates how effectively the storefront converts pass-by visibility into stops.
        </div>
        <div class="mono-box">
            Attention Rate = Window / Street = {fmt_int(curr["window"])} / {fmt_int(curr["street"])}
        </div>
    </div>
    """, unsafe_allow_html=True)

with d2:
    st.markdown(f"""
    <div class="insight-card" style="border-left-color:#fbbc04;">
        <div class="insight-title">Entry & Intent</div>
        <div class="insight-headline">{entry_rate*100:.1f}% entry rate</div>
        <div class="insight-body">
            Of all qualified audience interactions, <b>{fmt_int(curr["instore"])}</b> became entries.
            This is the cleanest indicator of storefront proposition strength.
        </div>
        <div class="mono-box">
            Entry Rate = InStore / Window = {fmt_int(curr["instore"])} / {fmt_int(curr["window"])}
        </div>
    </div>
    """, unsafe_allow_html=True)

with d3:
    if app_mode == "Retail Media":
        st.markdown(f"""
        <div class="insight-card" style="border-left-color:#34a853;">
            <div class="insight-title">Commercial Efficiency</div>
            <div class="insight-headline">{fmt_currency(cost_per_entry)} per entry</div>
            <div class="insight-body">
                The campaign translated into <b>{fmt_int(entries)}</b> store entries and <b>{fmt_int(high_intent_activity)}</b> units of high-intent activity.
                This is the commercial cost of converting exposure into action.
            </div>
            <div class="mono-box">
                Cost per Entry = Campaign Value / Entries = {fmt_currency(campaign_value)} / {fmt_int(entries)}
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        leakage = max(0, entries - transactions)
        st.markdown(f"""
        <div class="insight-card" style="border-left-color:#34a853;">
            <div class="insight-title">Floor Conversion</div>
            <div class="insight-headline">{conversion_rate*100:.1f}% close rate</div>
            <div class="insight-body">
                <b>{fmt_int(leakage)}</b> entry-level opportunities did not convert into transactions.
                If entry is healthy but sales conversion is weak, the bottleneck is likely on-floor execution.
            </div>
            <div class="mono-box">
                Conversion = Transactions / Entries = {fmt_int(transactions)} / {fmt_int(entries)}
            </div>
        </div>
        """, unsafe_allow_html=True)

# ==========================================
# OPERATING METRICS
# ==========================================
st.markdown("<div class='section-title'>Operating Metrics</div>", unsafe_allow_html=True)
m1, m2, m3, m4 = st.columns(4)

with m1:
    st.metric("Exposure", fmt_int(curr["street"]), f"{fmt_int(curr['street'] / duration_hours)} / hr")

with m2:
    st.metric("Entries", fmt_int(entries), f"{fmt_int(entries / duration_hours)} / hr")

with m3:
    st.metric("High-Intent Activity", fmt_int(high_intent_activity), f"{high_intent_rate*100:.1f}% of entries")

with m4:
    st.metric("Dominant OS", dominant_os, f"Apple {fmt_int(curr['apple'])} • Samsung {fmt_int(curr['samsung'])}")

if compare_baseline:
    if baseline_available and base_start and base_end:
        st.caption(
            f"Baseline comparison window: {base_start.strftime('%d %b %Y, %H:%M')} → {base_end.strftime('%d %b %Y, %H:%M')}"
        )
    else:
        st.caption("Baseline unavailable for the selected comparison window.")

# ==========================================
# STATEFUL SECTION NAV
# ==========================================
st.markdown("<div class='section-title'>Analytics Views</div>", unsafe_allow_html=True)

if "active_section" not in st.session_state:
    st.session_state.active_section = "Funnel"

section = st.radio(
    "Select View",
    ["Funnel", "Traffic", "Dwell", "Audience", "Benchmarks"],
    horizontal=True,
    key="active_section",
    label_visibility="collapsed"
)

# ==========================================
# SECTION: FUNNEL
# ==========================================
if section == "Funnel":
    st.markdown("<div class='section-title'>Commercial Funnel</div>", unsafe_allow_html=True)

    funnel_labels = ["Exposed", "Attended", "Entered", "Purchased"]
    funnel_values = [
        curr["street"],
        curr["window"],
        curr["instore"],
        transactions
    ]

    fig_funnel = go.Figure(go.Funnel(
        y=funnel_labels,
        x=funnel_values,
        textinfo="value+percent previous",
        marker={"color": ["#9aa0a6", "#fbbc04", "#1a73e8", "#188038"]}
    ))

    st.plotly_chart(
        style_chart(fig_funnel),
        width="stretch",
        config=PLOT_CONFIG
    )

    st.markdown(
        "<div class='chart-caption'>Strict commercial funnel showing progressive drop-off from exposure to purchase.</div>",
        unsafe_allow_html=True
    )

    st.markdown("<div class='section-title'>Engagement Quality</div>", unsafe_allow_html=True)

    e1, e2, e3 = st.columns(3)

    browsed_activity = camp_df["Browsed"].sum()
    retained_activity = camp_df["Retained"].sum()

    with e1:
        st.metric("High-Intent Activity", fmt_int(high_intent_activity), "Summed dwell-based activity")

    with e2:
        st.metric("Browsed Activity", fmt_int(browsed_activity), "Mid-intent interactions")

    with e3:
        st.metric("Retained Activity", fmt_int(retained_activity), "Longer-duration activity")

    st.caption(
        "Note: Engagement metrics above are activity-based and should not be interpreted as strict funnel stages because they are aggregated across snapshots, not deduplicated people."
    )

# ==========================================
# SECTION: TRAFFIC
# ==========================================
elif section == "Traffic":
    st.markdown("<div class='section-title'>Traffic Timeline</div>", unsafe_allow_html=True)

    fig_t = go.Figure()
    fig_t.add_trace(go.Scatter(
        x=camp_df["Time"], y=camp_df["Street"],
        fill="tozeroy", mode="lines", name="Exposure",
        line=dict(color="#9aa0a6", width=1.5),
        fillcolor="rgba(154,160,166,0.18)"
    ))
    fig_t.add_trace(go.Scatter(
        x=camp_df["Time"], y=camp_df["Window"],
        fill="tozeroy", mode="lines", name="Attention",
        line=dict(color="#fbbc04", width=2),
        fillcolor="rgba(251,188,4,0.26)"
    ))
    fig_t.add_trace(go.Scatter(
        x=camp_df["Time"], y=camp_df["InStore"],
        fill="tozeroy", mode="lines", name="Entries",
        line=dict(color="#1a73e8", width=2.2),
        fillcolor="rgba(26,115,232,0.18)"
    ))

    if baseline_available and len(camp_df) > 0:
        baseline_avg = base["instore"] / len(camp_df)
        fig_t.add_hline(
            y=baseline_avg,
            line_dash="dot",
            line_color="#3c4043",
            annotation_text="Prior same-period entry baseline"
        )

    st.plotly_chart(style_chart(fig_t), width="stretch", config=PLOT_CONFIG)
    st.markdown(
        "<div class='chart-caption'>Tracks exposure, attention, and entry events over time to reveal daypart quality and drop-off behavior.</div>",
        unsafe_allow_html=True
    )

    st.markdown("<div class='section-title'>Hourly Entry Heatmap</div>", unsafe_allow_html=True)
    hourly = camp_df.groupby(["Date", "Hour Label"], as_index=False)["InStore"].sum()
    if not hourly.empty:
        heat = hourly.pivot(index="Date", columns="Hour Label", values="InStore").fillna(0)
        fig_hm = px.imshow(
            heat,
            aspect="auto",
            color_continuous_scale="Blues",
            labels=dict(x="Hour", y="Date", color="Entries")
        )
        fig_hm.update_layout(margin=dict(l=10, r=10, t=35, b=20))
        st.plotly_chart(fig_hm, width="stretch", config=PLOT_CONFIG)

# ==========================================
# SECTION: DWELL
# ==========================================
elif section == "Dwell":
    st.markdown("<div class='section-title'>Behavior & Dwell Distribution</div>", unsafe_allow_html=True)

    behavior_categories = [
        "Passersby (<10s)",
        "Window Shoppers (<30s)",
        "Explorers (<2m)",
        "Focused (<5m)",
        "Engaged (<10m)",
        "Potential (<20m)",
        "Committed (<30m)",
        "Enthusiasts (<45m)",
        "Deep (<1h)",
        "Loyal (>1h)"
    ]
    behavior_counts = [camp_df[c].sum() for c in behavior_categories]

    behavior_df = pd.DataFrame({
        "Behavioral Segment": behavior_categories,
        "Count": behavior_counts
    })

    fig_b = px.bar(
        behavior_df,
        x="Behavioral Segment",
        y="Count",
        color="Count",
        color_continuous_scale="Blues"
    )
    fig_b.update_layout(coloraxis_showscale=False)
    st.plotly_chart(style_chart(fig_b), width="stretch", config=PLOT_CONFIG)
    st.markdown(
        "<div class='chart-caption'>Uses summed dwell-segment activity for the selected period. This is an activity distribution, not a unique-visitor distribution.</div>",
        unsafe_allow_html=True
    )

    g1, g2, g3 = st.columns(3)
    g1.metric("Bounced", fmt_int(camp_df["Bounced"].sum()), "Low-intent traffic")
    g2.metric("Browsed", fmt_int(camp_df["Browsed"].sum()), "Mid-intent traffic")
    g3.metric("Retained", fmt_int(camp_df["Retained"].sum()), "High-intent traffic")

# ==========================================
# SECTION: AUDIENCE
# ==========================================
elif section == "Audience":
    st.markdown("<div class='section-title'>OS / Brand Ecosystem</div>", unsafe_allow_html=True)

    os_df = pd.DataFrame({
        "OS": ["Apple", "Samsung", "Other"],
        "Count": [curr["apple"], curr["samsung"], curr["other"]]
    })

    fig_os = px.pie(
        os_df,
        values="Count",
        names="OS",
        hole=0.62,
        color="OS",
        color_discrete_map={
            "Apple": "#5f6368",
            "Samsung": "#1a73e8",
            "Other": "#9aa0a6"
        }
    )
    fig_os.update_traces(textinfo="percent+label", marker=dict(line=dict(width=0)))
    st.plotly_chart(style_chart(fig_os), width="stretch", config=PLOT_CONFIG)
    st.markdown(
        "<div class='chart-caption'>A directional view of audience device-brand ecosystem using manufacturer broadcast data.</div>",
        unsafe_allow_html=True
    )

# ==========================================
# SECTION: BENCHMARKS
# ==========================================
elif section == "Benchmarks":
    st.markdown("<div class='section-title'>Benchmark Comparison</div>", unsafe_allow_html=True)

    bench_df = pd.DataFrame({
        "Metric": ["Attention Rate", "Entry Rate", "High-Intent Activity Rate", "Conversion Rate"],
        "Current": [
            attention_rate * 100,
            entry_rate * 100,
            high_intent_rate * 100,
            conversion_rate * 100
        ],
        "Baseline": [
            base["attention_rate"] * 100 if baseline_available else 0,
            base["entry_rate"] * 100 if baseline_available else 0,
            base["high_intent_rate"] * 100 if baseline_available else 0,
            base["conversion_rate"] * 100 if baseline_available else 0
        ],
        "Target": [15, 35, 0, 20]
    })

    fig_bench = go.Figure()
    fig_bench.add_trace(go.Bar(name="Current", x=bench_df["Metric"], y=bench_df["Current"]))
    if baseline_available:
        fig_bench.add_trace(go.Bar(name="Baseline", x=bench_df["Metric"], y=bench_df["Baseline"]))
    fig_bench.add_trace(go.Scatter(
        name="Target",
        x=bench_df["Metric"],
        y=bench_df["Target"],
        mode="lines+markers"
    ))

    fig_bench.update_layout(barmode="group")
    st.plotly_chart(style_chart(fig_bench), width="stretch", config=PLOT_CONFIG)

    display_df = bench_df.copy()
    if not baseline_available:
        display_df["Baseline"] = "N/A"

    st.dataframe(
        display_df,
        width="stretch"
    )

    if not baseline_available:
        st.caption("Baseline metrics are unavailable for the selected comparison window.")

st.markdown("</div>", unsafe_allow_html=True)
