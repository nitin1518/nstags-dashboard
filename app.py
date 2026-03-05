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
st.set_page_config(page_title="nsTags | Retail Intelligence", page_icon="📈", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    
    /* Executive Consultant Cards - Adapts to Dark/Light Mode automatically */
    .consultant-card {
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 134, 139, 0.2);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        height: 100%;
        box-shadow: 0 4px 6px rgba(0,0,0,0.02);
    }
    
    .card-header {
        font-size: 0.85rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: var(--text-color);
        opacity: 0.6;
        margin-bottom: 0.8rem;
        display: flex;
        align-items: center;
        gap: 6px;
    }
    
    .card-headline {
        font-size: 1.5rem;
        font-weight: 600;
        color: var(--text-color);
        line-height: 1.2;
        margin-bottom: 0.8rem;
    }
    
    .card-body {
        font-size: 0.95rem;
        color: var(--text-color);
        opacity: 0.85;
        line-height: 1.5;
    }
    
    /* Dynamic Highlights */
    .hl-blue { color: #1a73e8; font-weight: 600; }
    .hl-green { color: #0f9d58; font-weight: 600; }
    .hl-orange { color: #f4b400; font-weight: 600; }
    .hl-red { color: #db4437; font-weight: 600; }
    
    /* Clean Metric overrides */
    div[data-testid="metric-container"] {
        background-color: transparent;
        border-left: 2px solid rgba(128, 134, 139, 0.2);
        padding-left: 1rem;
    }
    [data-testid="stMetricValue"] { font-size: 2rem; font-weight: 600; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem; text-transform: uppercase; letter-spacing: 0.5px; opacity: 0.7; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# ☁️ AWS S3 NETWORK ENGINE
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
                        'Street': snap[0], 'Window': snap[1], 'InStore': snap[2],
                        'Bounced': snap[3] + snap[4], 
                        'Browsed': snap[5] + snap[6] + snap[7],
                        'Retained': sum(snap[8:13])
                    })
            except: pass
    df = pd.DataFrame(all_records)
    if not df.empty: df = df.sort_values('Time').reset_index(drop=True)
    return df

# ==========================================
# 🧠 EXECUTIVE INTELLIGENCE & UI
# ==========================================
st.markdown("<h2 style='margin-bottom: 0;'>nsTags Intelligence</h2>", unsafe_allow_html=True)
st.caption("Storefront Monetization & Benchmarking Engine")

with st.spinner("Analyzing cross-store network data..."):
    df = load_s3_data()

if df.empty:
    st.warning("Awaiting telemetry. Ensure ESP32 is online.")
else:
    # --- MOBILE OPTIMIZED SIDEBAR ---
    with st.sidebar:
        st.markdown("### ⚙️ Engine Parameters")
        store_id = st.selectbox("Active Node", df['Store ID'].unique(), label_visibility="collapsed")
        time_filter = st.selectbox("Intelligence Window", ["Full Day", "Last 3 Hours", "Last 1 Hour"])
        
        st.markdown("### 📢 Ad Campaign & Monetization")
        ad_type = st.radio("Exterior Display Type:", ["Partner Brand Ad (e.g., Oppo)", "Own Store Promotion"])
        if ad_type == "Partner Brand Ad (e.g., Oppo)":
            ad_value = st.number_input("Ad Revenue Received (₹)", min_value=0, value=15000, step=1000, help="What the brand paid you to put their ad in your window.")
        else:
            ad_value = st.number_input("Marketing Spend (₹)", min_value=0, value=5000, step=500, help="What you spent on this window display/promotion.")

        st.markdown("### 💰 POS Integration")
        daily_revenue = st.number_input("Today's POS Revenue (₹)", min_value=0, value=45000, step=1000)
        daily_transactions = st.number_input("Total Transactions", min_value=0, value=35, step=1)

    # --- DATA FILTERING & NETWORK MATH ---
    now = df['Time'].max()
    if time_filter == "Last 1 Hour": time_delta = timedelta(hours=1)
    elif time_filter == "Last 3 Hours": time_delta = timedelta(hours=3)
    else: time_delta = timedelta(hours=24)

    # Network Data (All Stores)
    net_df = df[df['Time'] >= (now - time_delta)]
    n_street, n_instore = net_df['Street'].sum(), net_df['InStore'].sum()
    n_capture = (n_instore / n_street) if n_street > 0 else 0

    # Selected Store Data
    store_df = net_df[net_df['Store ID'] == store_id]
    s_street, s_window, s_instore = store_df['Street'].sum(), store_df['Window'].sum(), store_df['InStore'].sum()
    
    # Store Math
    s_capture = (s_instore / s_street) if s_street > 0 else 0
    s_conversion = (daily_transactions / s_instore) if s_instore > 0 else 0
    aov = (daily_revenue / daily_transactions) if daily_transactions > 0 else 0

    st.markdown("<hr style='margin: 1rem 0; opacity: 0.2;'>", unsafe_allow_html=True)

    # ==========================================
    # 🏆 TIER 1: EXECUTIVE STRATEGY BOARD
    # ==========================================
    st.markdown("#### Strategic Insights")
    e1, e2, e3 = st.columns(3)

    # INSIGHT 1: EXTERIOR MONETIZATION (The Game Changer)
    with e1:
        if ad_type == "Partner Brand Ad (e.g., Oppo)":
            # Calculate CPM (Cost Per 1000 Impressions)
            cpm = (ad_value / s_street * 1000) if s_street > 0 else 0
            if cpm < 150:
                negotiation = f"You are undercharging. Standard retail DOOH CPM is ₹150-₹300. Use this verified data to negotiate a <span class='hl-green'>higher payout</span> next month."
                color = "#f4b400" # Warning Orange
            else:
                negotiation = f"You are providing excellent value to the brand at this premium CPM. Send them this report as <span class='hl-blue'>Proof of Performance</span>."
                color = "#1a73e8" # Blue
                
            st.markdown(f"""
                <div class="consultant-card" style="border-top: 4px solid {color};">
                    <div class="card-header">📢 Brand Ad Monetization</div>
                    <div class="card-headline">₹{cpm:,.2f} Effective CPM</div>
                    <div class="card-body">Your storefront delivered <span class='hl-blue'>{int(s_street):,}</span> verified impressions and <span class='hl-blue'>{int(s_window):,}</span> direct window engagements for the partner brand. <br><br>{negotiation}</div>
                </div>
            """, unsafe_allow_html=True)
            
        else: # Own Store Promotion
            cac = (ad_value / s_instore) if s_instore > 0 else 0
            roas = (daily_revenue / ad_value) if ad_value > 0 else 0
            st.markdown(f"""
                <div class="consultant-card" style="border-top: 4px solid #0f9d58;">
                    <div class="card-header">📢 Store Promo ROI</div>
                    <div class="card-headline">₹{cac:,.2f} Cost Per Walk-in</div>
                    <div class="card-body">Your investment of ₹{ad_value:,} generated <span class='hl-green'>{int(s_instore):,}</span> walk-ins today. With an AOV of ₹{aov:,.2f}, this campaign is generating a Return on Ad Spend (ROAS) of <span class='hl-green'>{roas:.1f}x</span>.</div>
                </div>
            """, unsafe_allow_html=True)

    # INSIGHT 2: CROSS-STORE BENCHMARKING
    with e2:
        diff = (s_capture - n_capture) * 100
        if diff > 0:
            rank = "Outperforming Market"
            bench = f"Your window merchandising is highly effective. You capture <span class='hl-green'>+{diff:.1f}% more</span> street traffic than the nsTags network average."
            color = "#0f9d58"
        else:
            rank = "Below Market Average"
            bench = f"Your storefront is underperforming the network average by <span class='hl-red'>{abs(diff):.1f}%</span>. Consider updating window lighting or clearing physical friction at the entrance."
            color = "#db4437"
            
        st.markdown(f"""
            <div class="consultant-card" style="border-top: 4px solid {color};">
                <div class="card-header">🌐 Network Benchmark</div>
                <div class="card-headline">{rank}</div>
                <div class="card-body">Your capture rate is <span class='hl-blue'>{s_capture*100:.2f}%</span> vs the network average of <span class='hl-blue'>{n_capture*100:.2f}%</span>. <br><br>{bench}</div>
            </div>
        """, unsafe_allow_html=True)

    # INSIGHT 3: SALES FLOOR LEAKAGE
    with e3:
        target_close = 0.20
        lost_opps = int(s_instore - daily_transactions)
        if s_conversion < target_close and s_instore > daily_transactions:
            diag = f"High Floor Abandonment"
            fix = f"Your staff is closing <span class='hl-red'>{s_conversion*100:.1f}%</span> of walk-ins. <span class='hl-red'>{lost_opps}</span> people walked in but left empty-handed. <br><br><b>Action:</b> Deploy more staff to the floor or adjust pricing visibility."
        else:
            diag = "Strong Sales Execution"
            fix = f"Your staff is efficiently closing <span class='hl-green'>{s_conversion*100:.1f}%</span> of walk-ins. Focus your efforts on driving more street traffic into the store, as your floor team handles them well."

        st.markdown(f"""
            <div class="consultant-card" style="border-top: 4px solid #9aa0a6;">
                <div class="card-header">🛍️ Staff Diagnostics</div>
                <div class="card-headline">{diag}</div>
                <div class="card-body">{fix}</div>
            </div>
        """, unsafe_allow_html=True)

    # ==========================================
    # 📊 TIER 2: MACRO METRICS GRID
    # ==========================================
    st.markdown("<br>", unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Street Exposure", f"{int(s_street):,}", "Total Impressions")
    c2.metric("Window Engagements", f"{int(s_window):,}", f"{(s_window/s_street*100) if s_street > 0 else 0:.1f}% Stop Rate")
    c3.metric("Store Walk-ins", f"{int(s_instore):,}", f"{s_capture*100:.2f}% Capture Rate")
    c4.metric("Avg Order Value", f"₹{aov:,.0f}", f"₹{daily_revenue:,} Gross")

    # ==========================================
    # 🔬 TIER 3: THEME-ADAPTIVE CHARTS
    # ==========================================
    st.markdown("<br>", unsafe_allow_html=True)
    tab1, tab2, tab3 = st.tabs(["🚦 Real-time Traffic Flow", "🎯 The Conversion Funnel", "⏱️ Behavior Matrix"])

    with tab1:
        # Modern Spline curves that adapt to Dark/Light mode natively via theme="streamlit"
        fig_lines = go.Figure()
        fig_lines.add_trace(go.Scatter(x=store_df['Time'], y=store_df['Street'], mode='lines', name='Street (Far)', line=dict(color='#9aa0a6', width=1.5, shape='spline')))
        fig_lines.add_trace(go.Scatter(x=store_df['Time'], y=store_df['Window'], mode='lines', name='Window (Mid)', line=dict(color='#fbbc04', width=2, shape='spline')))
        fig_lines.add_trace(go.Scatter(x=store_df['Time'], y=store_df['InStore'], mode='lines', name='Walk-ins (Near)', line=dict(color='#1a73e8', width=3, shape='spline')))
        
        fig_lines.update_layout(
            hovermode="x unified", margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        fig_lines.update_xaxes(showgrid=False)
        fig_lines.update_yaxes(showgrid=True, gridcolor="rgba(128,134,139,0.1)")
        st.plotly_chart(fig_lines, use_container_width=True, theme="streamlit")

    with tab2:
        col1, col2 = st.columns([1, 1])
        with col1:
            fig_funnel = go.Figure(go.Funnel(
                y=["Street Exposure", "Window Browsers", "Store Walk-ins", "POS Transactions"],
                x=[s_street, s_window, s_instore, daily_transactions],
                textinfo="value+percent previous",
                marker={"color": ["rgba(154,160,166,0.6)", "rgba(251,188,4,0.8)", "rgba(26,115,232,0.9)", "rgba(15,157,88,1)"]}
            ))
            fig_funnel.update_layout(margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig_funnel, use_container_width=True, theme="streamlit")
            
        with col2:
            cat_df = pd.DataFrame({
                'Category': ['Bounced (<30s)', 'Browsed (<10m)', 'Retained (>10m)'],
                'Count': [store_df['Bounced'].max(), store_df['Browsed'].max(), store_df['Retained'].max()]
            })
            fig_bar = px.bar(cat_df, x='Count', y='Category', orientation='h', color='Category',
                             color_discrete_map={'Bounced (<30s)': '#ea4335', 'Browsed (<10m)': '#fbbc04', 'Retained (>10m)': '#1a73e8'})
            fig_bar.update_layout(showlegend=False, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig_bar, use_container_width=True, theme="streamlit")

    with tab3:
        hourly_df = store_df.groupby('Hour')[['Street', 'Window', 'InStore']].mean().reset_index()
        hourly_df = hourly_df.melt(id_vars='Hour', var_name='Zone', value_name='Avg Traffic')
        fig_heat = px.density_heatmap(hourly_df, x="Hour", y="Zone", z="Avg Traffic", color_continuous_scale="Blues")
        fig_heat.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig_heat, use_container_width=True, theme="streamlit")
