import streamlit as st
import boto3
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ==========================================
# 🎨 GOOGLE MATERIAL 3 EXECUTIVE UI
# ==========================================
st.set_page_config(page_title="nsTags - Analytics", page_icon="🛍️", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Google+Sans:wght@400;500;700&display=swap');
    
    html, body, [class*="css"] { font-family: 'Google Sans', sans-serif; }
    
    /* Executive Insight Cards (Top Level) */
    .exec-card {
        background: #ffffff;
        border: 1px solid #dadce0;
        border-radius: 12px;
        padding: 1.5rem;
        height: 100%;
        box-shadow: 0 4px 6px rgba(0,0,0,0.04);
        border-top: 4px solid #1a73e8;
    }
    .exec-card.revenue { border-top-color: #34a853; }
    .exec-card.benchmark { border-top-color: #fbbc04; }
    
    .exec-title { font-size: 0.95rem; font-weight: 700; color: #5f6368; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 0.8rem; }
    .exec-headline { font-size: 1.4rem; font-weight: 500; color: #202124; margin-bottom: 0.5rem; line-height: 1.3; }
    .exec-body { font-size: 1rem; color: #5f6368; line-height: 1.5; }
    .exec-highlight { font-weight: 700; color: #1a73e8; }
    
    /* Clean Metric Cards */
    div[data-testid="metric-container"] {
        background-color: transparent;
        border-bottom: 1px solid #dadce0;
        padding: 0.5rem 0 1rem 0;
    }
    [data-testid="stMetricValue"] { color: #202124; font-weight: 500; font-size: 2rem; }
    [data-testid="stMetricLabel"] { font-weight: 500; color: #5f6368; font-size: 0.95rem; }
    </style>
""", unsafe_allow_html=True)

G_BLUE, G_RED, G_YELLOW, G_GREEN, G_GREY = '#1a73e8', '#ea4335', '#fbbc04', '#34a853', '#9aa0a6'

# ==========================================
# ☁️ AWS S3 DATA ENGINE (WITH NETWORK CONTEXT)
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
                        'Total': snap[0] + snap[1] + snap[2],
                        'Bounced': snap[3] + snap[4], 
                        'Browsed': snap[5] + snap[6] + snap[7],
                        'Retained': sum(snap[8:13])
                    })
            except: pass
    df = pd.DataFrame(all_records)
    if not df.empty: df = df.sort_values('Time').reset_index(drop=True)
    return df

def apply_clean_style(fig):
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Google Sans, sans-serif", color="#5f6368", size=12),
        hovermode="x unified", margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1)
    )
    fig.update_xaxes(showgrid=False, zeroline=False, title_text="")
    fig.update_yaxes(showgrid=True, gridcolor="#f1f3f4", zeroline=False, title_text="")
    return fig

# ==========================================
# 🧠 AI STRATEGY & RENDERING
# ==========================================
st.markdown("<h2 style='color:#202124; font-weight:400; margin-bottom: 2rem;'>nsTags - Analytics</h2>", unsafe_allow_html=True)

with st.spinner("Compiling Network Intelligence..."):
    df = load_s3_data()

if df.empty:
    st.warning("Awaiting telemetry from nodes.")
else:
    # --- SIDEBAR ---
    with st.sidebar:
        st.markdown("### ⚙️ Engine Parameters")
        store_id = st.selectbox("Active Retail Node", df['Store ID'].unique())
        time_filter = st.radio("Intelligence Window:", ["Last 1 Hour", "Last 3 Hours", "Full Day"])
        
        st.markdown("### 💰 POS Integration")
        daily_revenue = st.number_input("Revenue (₹)", min_value=0, value=25000, step=1000)
        daily_transactions = st.number_input("Transactions", min_value=0, value=45, step=1)

    # --- DATA FILTERING (STORE VS NETWORK) ---
    now = df['Time'].max()
    if time_filter == "Last 1 Hour": time_delta = timedelta(hours=1)
    elif time_filter == "Last 3 Hours": time_delta = timedelta(hours=3)
    else: time_delta = timedelta(hours=24) # Approx full day

    # Filtered Network (All stores)
    net_df = df[df['Time'] >= (now - time_delta)]
    
    # Filtered Selected Store
    store_df = net_df[net_df['Store ID'] == store_id]
    
    # Mathematical Foundations
    s_street, s_window, s_instore = store_df['Street'].sum(), store_df['Window'].sum(), store_df['InStore'].sum()
    s_capture = (s_instore / s_street * 100) if s_street > 0 else 0
    s_conversion = (daily_transactions / s_instore * 100) if s_instore > 0 else 0
    s_aov = (daily_revenue / daily_transactions) if daily_transactions > 0 else 0

    n_street, n_instore = net_df['Street'].sum(), net_df['InStore'].sum()
    n_capture = (n_instore / n_street * 100) if n_street > 0 else 0

    # ==========================================
    # 🏆 TIER 1: EXECUTIVE INTELLIGENCE (THE "BAIT")
    # ==========================================
    st.markdown("<h4 style='color:#5f6368;'>Executive Briefing</h4>", unsafe_allow_html=True)
    e1, e2, e3 = st.columns(3)
    
    # INSIGHT 1: Location Setup Strategy
    with e1:
        if s_capture < 2.0 and s_street > 1000:
            status = "High-Volume Transit Corridor"
            # Assuming ₹300 CPM for a digital window display
            ad_rev = int((s_street / 1000) * 300) 
            action = f"Your storefront has massive street visibility but low walk-ins. We recommend installing a Digital Out-Of-Home (DOOH) screen facing the street to generate an estimated <span class='exec-highlight'>₹{ad_rev} in daily ad revenue</span>."
        else:
            status = "Destination Location"
            action = f"Your store successfully pulls people inside. Focus budget on <span class='exec-highlight'>In-Store Marketing</span> and visual merchandising to increase your ₹{int(s_aov):,} Average Order Value."
            
        st.markdown(f"""
            <div class="exec-card">
                <div class="exec-title">📍 Location Classification</div>
                <div class="exec-headline">{status}</div>
                <div class="exec-body">{action}</div>
            </div>
        """, unsafe_allow_html=True)

    # INSIGHT 2: Network Benchmarking
    with e2:
        diff = s_capture - n_capture
        if diff > 0:
            rank = "Outperforming Network"
            bench = f"Your window displays are highly effective. You capture <span class='exec-highlight'>+{diff:.1f}% more</span> street traffic than the nsTags network average."
        else:
            rank = "Below Network Average"
            bench = f"Your storefront is underperforming the network average by <span class='exec-highlight'>{abs(diff):.1f}%</span>. Consider updating window signage or lighting to attract passersby."
            
        st.markdown(f"""
            <div class="exec-card benchmark">
                <div class="exec-title">🌐 Cross-Store Benchmark</div>
                <div class="exec-headline">{rank}</div>
                <div class="exec-body">{bench}</div>
            </div>
        """, unsafe_allow_html=True)

    # INSIGHT 3: Funnel Diagnostics
    with e3:
        if s_window > 0 and (s_instore / s_window) < 0.2:
            diag = "High Window Abandonment"
            fix = "People look at the window but do not enter. The barrier is likely psychological: a closed door, intimidating layout, or lack of clear entry pricing."
        else:
            diag = "Healthy Walk-In Flow"
            fix = f"Window browsers are converting smoothly into walk-ins. Your bottleneck is at the register. Train floor staff to convert the <span class='exec-highlight'>{int(s_instore - daily_transactions)} non-buying</span> visitors currently inside."

        st.markdown(f"""
            <div class="exec-card revenue">
                <div class="exec-title">🎯 Funnel Diagnostics</div>
                <div class="exec-headline">{diag}</div>
                <div class="exec-body">{fix}</div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("<br><br>", unsafe_allow_html=True)

    # ==========================================
    # 📊 TIER 2: MACRO METRICS
    # ==========================================
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Street Exposure", f"{int(s_street):,}", "Potential Audience")
    c2.metric("Walk-In Capture", f"{s_capture:.2f}%", f"{s_capture - n_capture:.2f}% vs Network")
    c3.metric("POS Conversion", f"{s_conversion:.2f}%", f"{int(daily_transactions)} Sales")
    c4.metric("Avg Order Value", f"₹{s_aov:,.2f}", f"₹{int(daily_revenue):,} Revenue")

    st.markdown("<br>", unsafe_allow_html=True)

    # ==========================================
    # 🔬 TIER 3: DEEP DIVE CHARTS
    # ==========================================
    tab1, tab2, tab3 = st.tabs(["Spatial Proximity", "Conversion Funnel", "Hourly Heatmap"])

    with tab1:
        fig_traffic = px.area(
            store_df, x='Time', y=['InStore', 'Window', 'Street'], 
            color_discrete_map={'Street': '#e8eaed', 'Window': '#fce8b2', 'InStore': G_BLUE}
        )
        fig_traffic.data = fig_traffic.data[::-1] # Near on top
        st.plotly_chart(apply_clean_style(fig_traffic), use_container_width=True)

    with tab2:
        col1, col2 = st.columns([1, 1])
        with col1:
            fig_funnel = go.Figure(go.Funnel(
                y=["Street Exposure", "Window Browsers", "Walk-ins", "Transactions"],
                x=[s_street, s_window, s_instore, daily_transactions],
                textinfo="value+percent previous",
                marker={"color": [G_GREY, G_YELLOW, G_BLUE, G_GREEN]}
            ))
            st.plotly_chart(apply_clean_style(fig_funnel), use_container_width=True)
            
        with col2:
            cat_df = pd.DataFrame({
                'Category': ['Bounced (<30s)', 'Browsed (<10m)', 'Retained (>10m)'],
                'Count': [store_df['Bounced'].max(), store_df['Browsed'].max(), store_df['Retained'].max()]
            })
            fig_bar = px.bar(cat_df, x='Count', y='Category', orientation='h', color='Category',
                             color_discrete_map={'Bounced (<30s)': G_RED, 'Browsed (<10m)': G_YELLOW, 'Retained (>10m)': G_BLUE})
            fig_bar.update_layout(showlegend=False)
            st.plotly_chart(apply_clean_style(fig_bar), use_container_width=True)

    with tab3:
        hourly_df = store_df.groupby('Hour')[['Street', 'Window', 'InStore']].mean().reset_index()
        hourly_df = hourly_df.melt(id_vars='Hour', var_name='Zone', value_name='Avg Traffic')
        fig_heat = px.density_heatmap(
            hourly_df, x="Hour", y="Zone", z="Avg Traffic", color_continuous_scale="Blues"
        )
        st.plotly_chart(apply_clean_style(fig_heat), use_container_width=True)
