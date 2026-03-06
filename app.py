import streamlit as st
import boto3
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from google import genai

# ==========================================
# 🎨 V3 ENTERPRISE UI/UX (GOOGLE MATERIAL 3)
# ==========================================
st.set_page_config(page_title="nsTags | Retail Intelligence", page_icon="🏢", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; background-color: #f8f9fa; }
    
    /* Fluid Fade-in Animation */
    @keyframes slideUpFade {
        from { opacity: 0; transform: translateY(15px); }
        to { opacity: 1; transform: translateY(0); }
    }
    .animate-container { animation: slideUpFade 0.6s cubic-bezier(0.16, 1, 0.3, 1) forwards; }
    
    /* 🤖 V3 AI Consulting Copilot Box */
    .ai-copilot-box {
        background: #ffffff;
        border: 1px solid #e8eaed;
        border-top: 4px solid #1a73e8;
        border-radius: 8px;
        padding: 1.5rem 2rem;
        margin-bottom: 2rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        display: flex;
        gap: 2rem;
    }
    .ai-column { flex: 1; }
    .ai-column h4 { color: #5f6368; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; margin-top: 0; margin-bottom: 0.8rem; }
    .ai-column p { font-size: 0.95rem; color: #202124; line-height: 1.5; margin: 0; }
    .ai-column strong { color: #1a73e8; }
    
    /* Diagnostic Row Cards */
    .diag-card {
        background-color: #ffffff;
        border: 1px solid #e8eaed;
        border-radius: 8px;
        padding: 1.5rem;
        display: flex; flex-direction: column; justify-content: space-between;
        height: 100%; min-height: 220px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    }
    .diag-header { font-size: 0.85rem; font-weight: 600; color: #5f6368; text-transform: uppercase; margin-bottom: 0.5rem; }
    .diag-metric { font-size: 2.2rem; font-weight: 700; color: #202124; line-height: 1; margin-bottom: 0.5rem; }
    .diag-benchmark { font-size: 0.85rem; color: #80868b; margin-bottom: 1rem; }
    
    .verdict-pass { display: inline-block; padding: 4px 8px; background: #e6f4ea; color: #137333; font-size: 0.75rem; font-weight: 700; border-radius: 4px; text-transform: uppercase; }
    .verdict-fail { display: inline-block; padding: 4px 8px; background: #fce8e6; color: #c5221f; font-size: 0.75rem; font-weight: 700; border-radius: 4px; text-transform: uppercase; }
    
    .diag-action { margin-top: 1rem; padding-top: 1rem; border-top: 1px dashed #e8eaed; font-size: 0.85rem; color: #3c4043; font-weight: 500; }
    
    /* KPI Rail Customization */
    div[data-testid="metric-container"] { border-left: 3px solid #1a73e8; padding-left: 1rem; background: #ffffff; padding: 1rem; border-radius: 6px; border: 1px solid #e8eaed; box-shadow: 0 1px 2px rgba(0,0,0,0.02); }
    [data-testid="stMetricValue"] { font-size: 1.8rem; font-weight: 700; color: #202124; }
    [data-testid="stMetricLabel"] { font-size: 0.8rem; text-transform: uppercase; color: #5f6368; font-weight: 600; }
    
    /* Global Semantic Colors */
    .g-blue { color: #1a73e8; } .g-green { color: #34a853; } .g-yellow { color: #fbbc04; } .g-red { color: #ea4335; }
    </style>
""", unsafe_allow_html=True)

MOBILE_CONFIG = {'displayModeBar': False}

# ==========================================
# ☁️ AWS S3 NETWORK ENGINE 
# ==========================================
BUCKET_NAME = 'nstags-datalake-hq-2026'
REGION = 'ap-south-1'

@st.cache_data(ttl=45)
def load_s3_data():
    try:
        s3 = boto3.client('s3', region_name=REGION, aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"], aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"])
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='footfall/')
        if 'Contents' not in response: return pd.DataFrame()
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[:150]
    except Exception as e: 
        return pd.DataFrame()

    all_records = []
    for file in files:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=file['Key'])
        lines = obj['Body'].read().decode('utf-8').strip().split('\n')
        for line in lines:
            if not line: continue
            try:
                data = json.loads(line)
                end_time = datetime.fromtimestamp(data['timestamp'] / 1000.0) + timedelta(hours=5, minutes=30)
                snapshots = data.get('D', [])
                start_time = end_time - timedelta(seconds=len(snapshots)*5)
                for idx, snap in enumerate(snapshots):
                    if len(snap) < 20: continue
                    all_records.append({
                        'Store ID': data.get('S', 'Unknown'), 
                        'Time': start_time + timedelta(seconds=idx*5),
                        'Hour': (start_time + timedelta(seconds=idx*5)).strftime('%H:00'),
                        'Street': snap[0], 'Window': snap[1], 'InStore': snap[2],
                        'Bounced': snap[3] + snap[4], 'Browsed': snap[5] + snap[6] + snap[7], 'Retained': sum(snap[8:13]),
                        'Apple': snap[17], 'Samsung': snap[18], 'Other': snap[19]
                    })
            except Exception: pass
                
    df = pd.DataFrame(all_records)
    if not df.empty: df = df.sort_values('Time').reset_index(drop=True)
    return df

def style_chart(fig):
    fig.update_layout(
        hovermode="x unified", margin=dict(l=0, r=0, t=20, b=20), 
        legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#5f6368")
    )
    fig.update_xaxes(showgrid=False, zeroline=False, title_text="")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(128,134,139,0.1)", zeroline=False, title_text="")
    return fig

# ==========================================
# 🤖 V3 AI CONSULTING COPILOT
# ==========================================
@st.cache_data(ttl=300) 
def generate_ai_copilot(metrics):
    try:
        api_key = st.secrets.get("GEMINI_API_KEY")
        if not api_key: return "<div class='ai-column'><p>⚠️ API Key Missing in Secrets.</p></div>"
        
        client = genai.Client(api_key=api_key)
        
        prompt = f"""
        Act as an elite retail strategy consultant (ex-McKinsey/Apple). Analyze this data for a {metrics['mode']} dashboard:
        - Exposed (Street): {metrics['exposed']}
        - Attended (Window): {metrics['attended']} (Stop Rate: {metrics['stop_rate']}%) [Benchmark: {metrics['bench_stop']}%]
        - Entered (Walk-ins): {metrics['entered']} (Capture Rate: {metrics['capture_rate']}%) [Benchmark: {metrics['bench_cap']}%]
        - Converted (Sales): {metrics['sales']} (Floor Close Rate: {metrics['conversion_rate']}%) [Benchmark: {metrics['bench_conv']}%]
        
        Write a hyper-specific, data-driven brief. Output STRICTLY as 3 HTML columns with this exact structure (no markdown, no extra text):
        <div class="ai-column"><h4>📊 What Happened</h4><p>[1 sentence analyzing the overall funnel volume and the most notable metric]</p></div>
        <div class="ai-column"><h4>⚠️ Why It Happened</h4><p>[1 sentence diagnosing the primary bottleneck or success against benchmarks]</p></div>
        <div class="ai-column"><h4>💡 What To Do Next</h4><p>[1 actionable retail directive based on the bottleneck]</p></div>
        """
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        return response.text.replace("```html", "").replace("```", "").strip()
    except Exception:
        return "<div class='ai-column'><p>⚠️ AI Analysis momentarily unavailable.</p></div>"

# ==========================================
# 🧠 V3 PLATFORM INITIALIZATION & SIDEBAR
# ==========================================
st.markdown("<h2 style='margin-bottom: 0.2rem;'>nsTags Intelligence</h2>", unsafe_allow_html=True)

with st.spinner("Synchronizing with AWS Data Lake..."):
    df = load_s3_data()

if df.empty:
    st.warning("Awaiting telemetry. Ensure hardware nodes are online.")
else:
    with st.sidebar:
        # V3 SAAS MODE SWITCHER
        st.markdown("### ⚙️ Platform Mode")
        platform_mode = st.selectbox("Select Operating Framework", ["🛍️ Retail Operations", "📢 Retail Media Measurement"])
        
        st.markdown("### 📍 Node & Horizon")
        store_id = st.selectbox("Active Store", df['Store ID'].unique())
        time_preset = st.selectbox("Timeframe", ["Today", "Yesterday", "This Week", "Custom"])
        
        latest_time = df['Time'].max()
        if time_preset == "Today":
            camp_start = latest_time.replace(hour=0, minute=0, second=0)
            camp_end = latest_time
        elif time_preset == "Yesterday":
            camp_start = (latest_time - timedelta(days=1)).replace(hour=0, minute=0, second=0)
            camp_end = camp_start.replace(hour=23, minute=59, second=59)
        elif time_preset == "This Week":
            camp_start = latest_time - timedelta(days=7)
            camp_end = latest_time
        else:
            c_d1, c_t1 = st.columns(2)
            start_date = c_d1.date_input("Start", latest_time.date() - timedelta(days=1))
            start_time = c_t1.time_input("Time", (latest_time - timedelta(hours=4)).time())
            c_d2, c_t2 = st.columns(2)
            end_date = c_d2.date_input("End", latest_time.date())
            end_time = c_t2.time_input("Time", latest_time.time())
            camp_start = datetime.combine(start_date, start_time)
            camp_end = datetime.combine(end_date, end_time)

        duration_hours = max((camp_end - camp_start).total_seconds() / 3600, 0.01)

        st.markdown("### 🎯 System Benchmarks")
        bench_stop = st.number_input("Target Stop Rate (%)", value=15.0, step=1.0)
        bench_cap = st.number_input("Target Entry Rate (%)", value=35.0, step=1.0)
        bench_conv = st.number_input("Target Close Rate (%)", value=20.0, step=1.0)

        st.markdown("### 💰 Financial Integration")
        if platform_mode == "📢 Retail Media Measurement":
            revenue_input = st.number_input("Ad Contract Revenue (₹)", min_value=0, value=25000, step=1000)
            daily_transactions = st.number_input("Attributed Campaign Sales", min_value=0, value=12, step=1)
        else:
            revenue_input = st.number_input("Total Store Revenue (₹)", min_value=0, value=85000, step=1000)
            daily_transactions = st.number_input("Total Store Transactions", min_value=0, value=65, step=1)

        ai_enabled = st.checkbox("✨ Run AI Copilot", value=True)

    # --- CORE MATH & FUNNEL LOGIC ---
    store_df = df[df['Store ID'] == store_id]
    camp_df = store_df[(store_df['Time'] >= camp_start) & (store_df['Time'] <= camp_end)]
    
    # The 5-Layer Physical Funnel
    exposed = int(camp_df['Street'].sum())
    attended = int(camp_df['Window'].sum())
    entered = int(camp_df['InStore'].sum())
    engaged = int(camp_df['Retained'].sum()) # Proxied via deep dwell (>10m)
    purchased = daily_transactions

    # The True Conversion Rates
    stop_rate = (attended / exposed) if exposed > 0 else 0
    entry_rate = (entered / attended) if attended > 0 else 0
    close_rate = (purchased / entered) if entered > 0 else 0
    
    aov = (revenue_input / purchased) if purchased > 0 else 0

    st.markdown("<div class='animate-container'>", unsafe_allow_html=True)

    # ==========================================
    # 🤖 AI COPILOT COMMAND CENTER
    # ==========================================
    if ai_enabled:
        payload = {
            'mode': platform_mode, 'duration': round(duration_hours, 1),
            'exposed': exposed, 'attended': attended, 'entered': entered, 'sales': purchased,
            'stop_rate': round(stop_rate*100, 1), 'capture_rate': round(entry_rate*100, 1), 'conversion_rate': round(close_rate*100, 1),
            'bench_stop': bench_stop, 'bench_cap': bench_cap, 'bench_conv': bench_conv,
            'roi': f"₹{revenue_input:,}", 'dominant_os': "Calculated"
        }
        with st.spinner("Copilot Analyzing Funnel..."):
            ai_html = generate_ai_copilot(payload)
        st.markdown(f"<div class='ai-copilot-box'>{ai_html}</div>", unsafe_allow_html=True)

    # ==========================================
    # 📈 THE 5-METRIC KPI RAIL (CONTEXT AWARE)
    # ==========================================
    c1, c2, c3, c4, c5 = st.columns(5)
    
    if platform_mode == "🛍️ Retail Operations":
        c1.metric("Qualified Exposure", f"{exposed:,}", f"{int(exposed/duration_hours):,} / hr")
        c2.metric("Window Attended", f"{attended:,}", f"{stop_rate*100:.1f}% Stop Rate")
        c3.metric("Store Entered", f"{entered:,}", f"{entry_rate*100:.1f}% Entry Rate")
        c4.metric("Floor Closed", f"{purchased:,}", f"{close_rate*100:.1f}% Win Rate")
        c5.metric("Rev / Walk-in", f"₹{(revenue_input/entered) if entered>0 else 0:,.0f}", "Yield per entrant")
    else:
        cpm = (revenue_input / exposed * 1000) if exposed > 0 else 0
        cpe = (revenue_input / attended) if attended > 0 else 0
        c1.metric("Audience Delivered", f"{exposed:,}", "Total Opportunity")
        c2.metric("Attention Rate", f"{stop_rate*100:.1f}%", f"{attended:,} Stops")
        c3.metric("Entry Lift", f"{entry_rate*100:.1f}%", f"{entered:,} Walk-ins")
        c4.metric("Cost per Engaged", f"₹{cpe:,.0f}", "Value of Attention")
        c5.metric("Effective CPM", f"₹{cpm:,.2f}", "Media Value")

    st.markdown("<br>", unsafe_allow_html=True)

    # ==========================================
    # 📊 THE HERO PANEL (1 MASSIVE CHART)
    # ==========================================
    if platform_mode == "🛍️ Retail Operations":
        st.markdown("#### Traffic Heatmap (Peak Hour Operations)")
        hourly_df = camp_df.groupby('Hour')[['Street', 'Window', 'InStore']].sum().reset_index()
        hourly_df = hourly_df.melt(id_vars='Hour', var_name='Zone', value_name='Traffic Volume')
        fig_hero = px.density_heatmap(hourly_df, x="Hour", y="Zone", z="Traffic Volume", color_continuous_scale="Blues")
        st.plotly_chart(style_chart(fig_hero), width="stretch", theme="streamlit", config=MOBILE_CONFIG)
    
    else:
        st.markdown("#### The DOOH Media Accountability Funnel")
        fig_hero = go.Figure(go.Funnel(
            y=["Exposure (Street)", "Attention (Window)", "Visitation (In-Store)", "Commercial Impact"],
            x=[exposed, attended, entered, purchased],
            textinfo="value+percent previous",
            marker={"color": ["#9aa0a6", "#fbbc04", "#1a73e8", "#34a853"]}
        ))
        st.plotly_chart(style_chart(fig_hero), width="stretch", theme="streamlit", config=MOBILE_CONFIG)

    st.markdown("<br>", unsafe_allow_html=True)

    # ==========================================
    # 🩺 DIAGNOSTIC ROW (ACQUISITION, ENGAGEMENT, CONVERSION)
    # ==========================================
    st.markdown("#### Funnel Diagnostics vs. Benchmarks")
    d1, d2, d3 = st.columns(3)

    # Function to generate card HTML
    def build_diag_card(title, current, benchmark, metric_name, good_is_higher=True):
        diff = current - benchmark
        passed = (diff >= 0) if good_is_higher else (diff <= 0)
        verdict_class = "verdict-pass" if passed else "verdict-fail"
        verdict_text = "Exceeds Benchmark" if passed else "Below Benchmark"
        
        action = ""
        if title == "Acquisition (Stop Rate)": action = "Optimize exterior display lighting and high-contrast creative." if not passed else "Current window creative is arresting. Maintain."
        if title == "Engagement (Entry Rate)": action = "Remove entrance friction; add compelling mobile CTAs on glass." if not passed else "Storefront proposition is highly effective."
        if title == "Conversion (Close Rate)": action = "Increase active floor staff during peak hours." if not passed else "Staff execution is elite."

        return f"""
        <div class="diag-card">
            <div>
                <div class="diag-header">{title}</div>
                <div class="diag-metric">{current:.1f}%</div>
                <div class="diag-benchmark">Target: {benchmark:.1f}% {metric_name}</div>
                <div class="{verdict_class}">{verdict_text}</div>
            </div>
            <div class="diag-action"><strong>Fix:</strong> {action}</div>
        </div>
        """

    with d1: st.markdown(build_diag_card("Acquisition (Stop Rate)", stop_rate*100, bench_stop, "Window / Street"), unsafe_allow_html=True)
    with d2: st.markdown(build_diag_card("Engagement (Entry Rate)", entry_rate*100, bench_cap, "InStore / Window"), unsafe_allow_html=True)
    with d3: st.markdown(build_diag_card("Conversion (Close Rate)", close_rate*100, bench_conv, "Sales / InStore"), unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True) # End animation
