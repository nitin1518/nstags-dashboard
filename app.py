import streamlit as st
import boto3
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ==========================================
# 🎨 GOOGLE MATERIAL UI/UX CONFIGURATION
# ==========================================
st.set_page_config(page_title="nsTags Analytics | HQ", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

# Injecting Custom CSS for Google Material Design feel (Clean, Card-based, Roboto-esque)
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');
    html, body, [class*="css"] { font-family: 'Roboto', sans-serif; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.1); border: 1px solid #e0e0e0; }
    [data-testid="stMetricValue"] { color: #1a73e8; font-weight: 500; font-size: 2.2rem; }
    h1, h2, h3 { color: #202124; font-weight: 400; }
    .insight-card { background-color: #e8f0fe; padding: 15px; border-radius: 8px; border-left: 4px solid #1a73e8; color: #1967d2; margin-bottom: 1rem; font-size: 0.95rem; }
    .warning-card { background-color: #fef7e0; padding: 15px; border-radius: 8px; border-left: 4px solid #fbbc04; color: #b08d00; margin-bottom: 1rem; }
    </style>
""", unsafe_allow_html=True)

# Google Material Hex Codes for Charts
G_BLUE, G_RED, G_YELLOW, G_GREEN = '#4285F4', '#EA4335', '#FBBC05', '#34A853'
G_GREY = '#9AA0A6'

# ==========================================
# ☁️ AWS S3 DATA ENGINE
# ==========================================
BUCKET_NAME = 'nstags-datalake-hq-2026'
REGION = 'ap-south-1'

@st.cache_data(ttl=45) # Fast 45-second refresh
def load_s3_data():
    try:
        s3 = boto3.client('s3', region_name=REGION,
                          aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
                          aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"])
    except: return pd.DataFrame()
    
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='footfall/')
        if 'Contents' not in response: return pd.DataFrame()
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[:12]
    except: return pd.DataFrame()

    all_records = []
    for file in files:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=file['Key'])
        lines = obj['Body'].read().decode('utf-8').strip().split('\n')
        
        for line in lines:
            if not line: continue
            try:
                data = json.loads(line)
                store_id = data.get('S', 'Unknown')
                # 🇮🇳 UTC to IST Conversion (+5h 30m)
                end_time = datetime.fromtimestamp(data['timestamp'] / 1000.0) + timedelta(hours=5, minutes=30)
                snapshots = data.get('D', [])
                start_time = end_time - timedelta(seconds=len(snapshots)*5)
                
                for idx, snap in enumerate(snapshots):
                    if len(snap) < 20: continue
                    snap_time = start_time + timedelta(seconds=idx*5)
                    all_records.append({
                        'Store ID': store_id, 'Time': snap_time,
                        'Far': snap[0], 'Mid': snap[1], 'Near': snap[2],
                        'Total': snap[0] + snap[1] + snap[2],
                        'Passersby': snap[3], 'Window': snap[4], 'Explorers': snap[5],
                        'Focused': snap[6], 'Engaged': snap[7], 'Potential': snap[8],
                        'Committed': snap[9], 'Enthusiasts': snap[10], 'Deep': snap[11], 'Loyal': snap[12],
                        'Temp': snap[13], 'Humidity': snap[14], 'Pressure': snap[15], 'Alt': snap[16],
                        'Apple': snap[17], 'Samsung': snap[18], 'Other': snap[19]
                    })
            except: pass
                
    df = pd.DataFrame(all_records)
    if not df.empty: df = df.sort_values('Time').reset_index(drop=True)
    return df

# Helper to cleanly style Plotly charts in Material Design
def apply_material_style(fig):
    fig.update_layout(
        template="plotly_white", margin=dict(l=20, r=20, t=40, b=20),
        font=dict(family="Roboto, sans-serif", color="#3c4043"),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#f1f3f4", zeroline=False)
    return fig

# ==========================================
# 📊 UI RENDERING & INSIGHTS
# ==========================================
st.markdown("## 📊 Tanuvansh Intelligence Console")

with st.spinner("Synchronizing with AWS Data Lake..."):
    df = load_s3_data()

if df.empty:
    st.warning("Awaiting telemetry from nodes.")
else:
    # --- GLOBAL FILTERS ---
    store_id = st.selectbox("Active Retail Node", df['Store ID'].unique(), label_visibility="collapsed")
    store_df = df[df['Store ID'] == store_id]
    
    # Calculate Time Windows for Velocity
    now = store_df['Time'].max()
    df_15m = store_df[store_df['Time'] >= (now - timedelta(minutes=15))]
    df_1h = store_df[store_df['Time'] >= (now - timedelta(hours=1))]

    # --- KPI HEADER ---
    st.markdown("<br>", unsafe_allow_html=True)
    k1, k2, k3, k4 = st.columns(4)
    
    # Traffic Velocity
    cur_traffic = int(df_15m['Total'].mean()) if not df_15m.empty else 0
    hist_traffic = int(df_1h['Total'].mean()) if not df_1h.empty else 0
    k1.metric("Live Traffic (15m Avg)", cur_traffic, f"{cur_traffic - hist_traffic} vs 1H Baseline", delta_color="normal")
    
    # Store Capture Rate Insight
    total_far = store_df['Far'].sum()
    total_near = store_df['Near'].sum()
    capture_rate = (total_near / total_far * 100) if total_far > 0 else 0
    k2.metric("Store Capture Rate", f"{capture_rate:.1f}%", "Industry Avg: 8-12%", delta_color="off")
    
    # Environmental Status
    cur_temp = df_15m['Temp'].mean() if not df_15m.empty else 0
    k3.metric("Ambient Temperature", f"{cur_temp:.1f}°C", "BME280 Sensor")
    
    # Brand Dominance
    apple_tot, sam_tot = store_df['Apple'].sum(), store_df['Samsung'].sum()
    dom_brand = "Apple" if apple_tot > sam_tot else "Samsung"
    k4.metric("Dominant Ecosystem", dom_brand, f"{(max(apple_tot, sam_tot) / (apple_tot+sam_tot+0.001)*100):.1f}% Share", delta_color="off")

    st.markdown("<hr style='border:1px solid #f1f3f4'>", unsafe_allow_html=True)

    # --- LAYER 1: TRAFFIC & CONVERSION ---
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.markdown("### 🌊 Real-Time Footfall Velocity")
        fig_traffic = px.area(store_df, x='Time', y=['Far', 'Mid', 'Near'], 
                              color_discrete_sequence=[G_GREY, G_YELLOW, G_BLUE],
                              labels={'value': 'Detected Devices', 'variable': 'Zone'})
        st.plotly_chart(apply_material_style(fig_traffic), use_container_width=True)
        
    with c2:
        st.markdown("### 🎯 Conversion Funnel")
        # Google Analytics 4 Style Funnel
        fig_funnel = go.Figure(go.Funnel(
            y=["Street Traffic", "Window Browsers", "In-Store Walk-ins"],
            x=[store_df['Far'].sum(), store_df['Mid'].sum(), store_df['Near'].sum()],
            textinfo="value+percent initial",
            marker={"color": [G_GREY, G_YELLOW, G_BLUE], "line": {"width": [0,0,0]}}
        ))
        st.plotly_chart(apply_material_style(fig_funnel), use_container_width=True)
        
        # Generative Insight
        st.markdown(f"<div class='insight-card'><b>Insight:</b> Out of {store_df['Far'].sum()} devices detected on the street today, {store_df['Mid'].sum()} slowed down at the window, yielding a <b>{int(store_df['Mid'].sum()/(store_df['Far'].sum()+0.001)*100)}%</b> window-engagement rate.</div>", unsafe_allow_html=True)

    # --- LAYER 2: BEHAVIOR & ENVIRONMENT ---
    c3, c4 = st.columns([1, 1])
    
    with c3:
        st.markdown("### 🧠 Customer Retention Matrix")
        # Group the 10 micro-categories into 3 Macro Retail Metrics
        bounced = store_df['Passersby'].max() + store_df['Window'].max()
        browsed = store_df['Explorers'].max() + store_df['Focused'].max() + store_df['Engaged'].max()
        retained = store_df['Potential'].max() + store_df['Committed'].max() + store_df['Enthusiasts'].max() + store_df['Deep'].max() + store_df['Loyal'].max()
        
        cat_df = pd.DataFrame({
            'Category': ['Bounced (<30s)', 'Browsed (<10m)', 'Retained (>10m)'],
            'Count': [bounced, browsed, retained]
        })
        
        fig_bar = px.bar(cat_df, x='Count', y='Category', orientation='h', color='Category',
                         color_discrete_sequence=[G_RED, G_YELLOW, G_GREEN])
        fig_bar.update_traces(texttemplate='%{x}', textposition='outside')
        st.plotly_chart(apply_material_style(fig_bar), use_container_width=True)
        
        if bounced == 0 and browsed == 0:
            st.markdown("<div class='warning-card'><b>Note:</b> Sessions populate here only after a device leaves the 1-hour Bluetooth radius.</div>", unsafe_allow_html=True)

    with c4:
        st.markdown("### 🌤️ Environmental Impact Engine")
        # Dual Axis Plot: Traffic vs Temperature
        fig_env = go.Figure()
        fig_env.add_trace(go.Scatter(x=store_df['Time'], y=store_df['Total'], name="Total Traffic", fill='tozeroy', line=dict(color=G_GREY, width=1)))
        fig_env.add_trace(go.Scatter(x=store_df['Time'], y=store_df['Temp'], name="Temperature (°C)", yaxis="y2", line=dict(color=G_RED, width=3)))
        
        fig_env.update_layout(
            yaxis2=dict(title="Temperature (°C)", overlaying="y", side="right", range=[0, 50]),
            yaxis=dict(title="Footfall"),
        )
        st.plotly_chart(apply_material_style(fig_env), use_container_width=True)
        
        st.markdown("<div class='insight-card'><b>Predictive Engine:</b> Awaiting live BME280 sensor data stream to establish weather-to-sales correlations.</div>", unsafe_allow_html=True)
