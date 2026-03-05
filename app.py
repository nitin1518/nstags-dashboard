import streamlit as st
import boto3
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ==========================================
# 🎨 MOBILE-FIRST, THEME-ADAPTIVE UI
# ==========================================
st.set_page_config(page_title="nsTags Analytics", page_icon="📈", layout="wide")

# Native CSS variables automatically adapt to user's Dark/Light mode device settings
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    
    /* Mobile-first Executive Cards */
    .consultant-card {
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 134, 139, 0.2);
        border-radius: 12px;
        padding: 1.25rem;
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    .card-header {
        font-size: 0.85rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-color);
        opacity: 0.7;
        margin-bottom: 0.5rem;
    }
    
    .card-value {
        font-size: 1.8rem;
        font-weight: 600;
        color: var(--text-color);
        line-height: 1.2;
        margin-bottom: 0.5rem;
    }
    
    .card-insight {
        font-size: 0.95rem;
        color: var(--text-color);
        opacity: 0.9;
        line-height: 1.5;
    }
    
    .highlight-red { color: #ff4b4b; font-weight: 600; }
    .highlight-green { color: #09ab3b; font-weight: 600; }
    .highlight-blue { color: #1a73e8; font-weight: 600; }
    
    /* Clean up native Streamlit elements for mobile */
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { padding-left: 10px; padding-right: 10px; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# ☁️ AWS S3 DATA ENGINE
# ==========================================
BUCKET_NAME = 'nstags-datalake-hq-2026'
REGION = 'ap-south-1'

@st.cache_data(ttl=45)
def load_s3_data():
    try:
        s3 = boto3.client('s3', region_name=REGION,
                          aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
                          aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"])
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='footfall/')
        if 'Contents' not in response: return pd.DataFrame()
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[:48]
    except: return pd.DataFrame()

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
                        'Street': snap[0], 'Window': snap[1], 'InStore': snap[2]
                    })
            except: pass
    df = pd.DataFrame(all_records)
    if not df.empty: df = df.sort_values('Time').reset_index(drop=True)
    return df

# ==========================================
# 🧠 EXECUTIVE INTELLIGENCE & UI
# ==========================================
st.markdown("<h3 style='text-align: center; margin-bottom: 0;'>nsTags Intelligence</h3>", unsafe_allow_html=True)
st.caption("<p style='text-align: center;'>Storefront Revenue Optimization Engine</p>", unsafe_allow_html=True)

with st.spinner("Analyzing location economics..."):
    df = load_s3_data()

if df.empty:
    st.warning("Awaiting telemetry. Ensure ESP32 is online.")
else:
    # --- MOBILE OPTIMIZED SIDEBAR ---
    with st.sidebar:
        st.markdown("### ⚙️ Parameters")
        store_id = st.selectbox("Active Node", df['Store ID'].unique(), label_visibility="collapsed")
        time_filter = st.selectbox("Timeframe", ["Full Day", "Last 3 Hours", "Last 1 Hour"])
        
        st.markdown("### 💰 POS Data (Today)")
        daily_revenue = st.number_input("Revenue (₹)", min_value=0, value=35000, step=1000)
        daily_transactions = st.number_input("Transactions", min_value=0, value=40, step=1)

    # --- DATA FILTERING ---
    now = df['Time'].max()
    if time_filter == "Last 1 Hour": plot_df = df[(df['Store ID'] == store_id) & (df['Time'] >= (now - timedelta(hours=1)))]
    elif time_filter == "Last 3 Hours": plot_df = df[(df['Store ID'] == store_id) & (df['Time'] >= (now - timedelta(hours=3)))]
    else: plot_df = df[df['Store ID'] == store_id]

    # --- THE CONSULTANT MATH (THE BAIT) ---
    t_street = plot_df['Street'].sum()
    t_window = plot_df['Window'].sum()
    t_instore = plot_df['InStore'].sum()
    
    # 1. Visual Merchandising (Street to Window)
    window_rate = (t_window / t_street) if t_street > 0 else 0
    # 2. Storefront Friction (Window to Walk-in)
    walkin_rate = (t_instore / t_window) if t_window > 0 else 0
    # 3. Staff Performance (Walk-in to Sale)
    staff_close_rate = (daily_transactions / t_instore) if t_instore > 0 else 0
    # Basic POS metrics for math
    aov = (daily_revenue / daily_transactions) if daily_transactions > 0 else 0

    st.markdown("<hr style='margin: 1rem 0; opacity: 0.3;'>", unsafe_allow_html=True)

    # ==========================================
    # 🏆 TIER 1: THE REVENUE LEAKAGE REPORT
    # ==========================================
    st.markdown("#### The Opportunity Cost")
    
    # AI Insight: Window Abandonment Leakage
    industry_window_rate = 0.25 # 25% is a healthy retail benchmark
    if window_rate < industry_window_rate:
        lost_browsers = int(t_street * (industry_window_rate - window_rate))
        lost_revenue = int(lost_browsers * walkin_rate * staff_close_rate * aov)
        st.markdown(f"""
            <div class="consultant-card" style="border-left: 4px solid #ff4b4b;">
                <div class="card-header">Visual Merchandising Leakage</div>
                <div class="card-value">₹{lost_revenue:,} Lost Today</div>
                <div class="card-insight">Only <b>{window_rate*100:.1f}%</b> of passersby look at your window (Target: 25%). Your displays are failing to stop traffic. <br><br><b>Consultant Action:</b> Rotate window mannequins or introduce high-contrast digital signage facing the street.</div>
            </div>
        """, unsafe_allow_html=True)

    # AI Insight: Staff Closing Power
    target_close_rate = 0.20 # 20% is a standard retail walk-in close rate
    if staff_close_rate < target_close_rate and t_instore > daily_transactions:
        lost_sales = int(t_instore * (target_close_rate - staff_close_rate))
        st.markdown(f"""
            <div class="consultant-card" style="border-left: 4px solid #fbbc04;">
                <div class="card-header">Sales Floor Performance</div>
                <div class="card-value">{int(t_instore - daily_transactions)} Unconverted Walk-ins</div>
                <div class="card-insight">Your staff's closing rate is <b>{staff_close_rate*100:.1f}%</b>. Over {int(t_instore - daily_transactions)} people walked in, browsed, and left without buying. <br><br><b>Consultant Action:</b> Increase floor staff during peak hours or train staff on immediate customer engagement.</div>
            </div>
        """, unsafe_allow_html=True)
        
    # AI Insight: High Transit DOOH
    if t_street > 5000 and (t_instore / t_street) < 0.02:
        st.markdown(f"""
            <div class="consultant-card" style="border-left: 4px solid #09ab3b;">
                <div class="card-header">DOOH Ad Real Estate</div>
                <div class="card-value">High Transit Location</div>
                <div class="card-insight">You have massive street exposure (<b>{int(t_street):,}</b> passersby) but extreme low intent (<b>{(t_instore/t_street)*100:.1f}%</b> walk-in rate). Your location acts as a transit corridor. <br><br><b>Consultant Action:</b> Monetize your window space by installing screens for 3rd-party advertising.</div>
            </div>
        """, unsafe_allow_html=True)

    # ==========================================
    # 📊 TIER 2: MOBILE-FIRST METRICS GRID
    # ==========================================
    st.markdown("#### Core Conversion Metrics")
    # Using 2 columns for perfect mobile readability
    c1, c2 = st.columns(2)
    with c1:
        st.metric("Total Street Traffic", f"{int(t_street):,}", "Addressable Market")
        st.metric("Window Stop Rate", f"{window_rate*100:.1f}%", "Merchandising ROI")
    with c2:
        st.metric("Walk-In Conversions", f"{int(t_instore):,}", "Total Entries")
        st.metric("Staff Close Rate", f"{staff_close_rate*100:.1f}%", "Sales Efficiency")

    # ==========================================
    # 🔬 TIER 3: THEME-ADAPTIVE CHARTS
    # ==========================================
    st.markdown("<br>", unsafe_allow_html=True)
    tab1, tab2 = st.tabs(["🚦 Traffic Flow", "🎯 The Leakage Funnel"])

    with tab1:
        # Replaced ugly filled area chart with sleek, hyper-modern spline lines
        fig_lines = go.Figure()
        fig_lines.add_trace(go.Scatter(x=plot_df['Time'], y=plot_df['Street'], mode='lines', name='Street', line=dict(color='#9aa0a6', width=1, shape='spline')))
        fig_lines.add_trace(go.Scatter(x=plot_df['Time'], y=plot_df['Window'], mode='lines', name='Window', line=dict(color='#fbbc04', width=2, shape='spline')))
        fig_lines.add_trace(go.Scatter(x=plot_df['Time'], y=plot_df['InStore'], mode='lines', name='Walk-in', line=dict(color='#1a73e8', width=3, shape='spline')))
        
        fig_lines.update_layout(
            hovermode="x unified",
            margin=dict(l=0, r=0, t=20, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        fig_lines.update_xaxes(showgrid=False)
        fig_lines.update_yaxes(showgrid=True, gridcolor="rgba(128,134,139,0.2)")
        # theme="streamlit" guarantees dark/light mode compatibility natively
        st.plotly_chart(fig_lines, use_container_width=True, theme="streamlit")

    with tab2:
        # A true consultant funnel
        fig_funnel = go.Figure(go.Funnel(
            y=["Total Street", "Stopped at Window", "Walked Inside", "Purchased (POS)"],
            x=[t_street, t_window, t_instore, daily_transactions],
            textinfo="value+percent previous",
            marker={"color": ["rgba(154,160,166,0.6)", "rgba(251,188,4,0.8)", "rgba(26,115,232,0.9)", "rgba(15,157,88,1)"]}
        ))
        fig_funnel.update_layout(margin=dict(l=0, r=0, t=20, b=0))
        st.plotly_chart(fig_funnel, use_container_width=True, theme="streamlit")
