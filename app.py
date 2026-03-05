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
st.set_page_config(page_title="nsTags - Analytics", page_icon="🛍️", layout="wide", initial_sidebar_state="expanded")

# Master-level CSS for true Material Design aesthetics & Looker Studio vibes
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Google+Sans:wght@400;500;700&display=swap');
    
    html, body, [class*="css"] { font-family: 'Google Sans', sans-serif; }
    
    /* Clean, elevated Metric Cards */
    div[data-testid="metric-container"] {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        padding: 1.2rem;
        border-radius: 8px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }
    
    [data-testid="stMetricValue"] { color: #1A73E8; font-weight: 500; font-size: 2.2rem; }
    [data-testid="stMetricLabel"] { font-weight: 500; color: #5f6368; font-size: 1rem; }
    
    /* Insight Callout Cards */
    .insight-box {
        background-color: #e8f0fe;
        border-left: 4px solid #1A73E8;
        padding: 1rem 1.5rem;
        border-radius: 0 8px 8px 0;
        margin: 1rem 0;
        color: #1967d2;
        font-size: 0.95rem;
        line-height: 1.5;
    }
    .insight-title { font-weight: 700; margin-bottom: 0.25rem; }
    
    .finance-box { background-color: #e6f4ea; border-left: 4px solid #34A853; color: #137333; }
    </style>
""", unsafe_allow_html=True)

# Exact Google Material Palette
G_BLUE, G_RED, G_YELLOW, G_GREEN, G_GREY = '#1A73E8', '#EA4335', '#FBBC04', '#34A853', '#9AA0A6'

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
    except: return pd.DataFrame()
    
    try:
        # Pulling up to 48 files (~8 hours of data) to allow for historical heatmap trends
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
                store_id = data.get('S', 'Unknown')
                # UTC to IST Conversion
                end_time = datetime.fromtimestamp(data['timestamp'] / 1000.0) + timedelta(hours=5, minutes=30)
                snapshots = data.get('D', [])
                start_time = end_time - timedelta(seconds=len(snapshots)*5)
                
                for idx, snap in enumerate(snapshots):
                    if len(snap) < 20: continue
                    snap_time = start_time + timedelta(seconds=idx*5)
                    all_records.append({
                        'Store ID': store_id, 'Time': snap_time,
                        'Hour': snap_time.strftime('%H:00'),
                        'Street (Far)': snap[0], 'Window (Mid)': snap[1], 'In-Store (Near)': snap[2],
                        'Total': snap[0] + snap[1] + snap[2],
                        'Bounced': snap[3] + snap[4], 
                        'Browsed': snap[5] + snap[6] + snap[7],
                        'Retained': snap[8] + snap[9] + snap[10] + snap[11] + snap[12],
                        'Temp': snap[13], 'Apple': snap[17], 'Samsung': snap[18], 'Other': snap[19]
                    })
            except: pass
                
    df = pd.DataFrame(all_records)
    if not df.empty: df = df.sort_values('Time').reset_index(drop=True)
    return df

def apply_clean_style(fig):
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Google Sans, sans-serif", color="#3c4043"),
        hovermode="x unified", margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1)
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#f1f3f4", zeroline=False)
    return fig

# ==========================================
# 📊 UI RENDERING & LOGIC
# ==========================================
st.markdown("## 📊 nsTags - Analytics")

with st.spinner("Connecting to AWS Data Lake..."):
    df = load_s3_data()

if df.empty:
    st.warning("Awaiting telemetry from nodes. Ensure ESP32 is online.")
else:
    # --- SIDEBAR CONTROLS ---
    with st.sidebar:
        st.markdown("### ⚙️ System Controls")
        store_id = st.selectbox("Active Node", df['Store ID'].unique())
        
        st.markdown("### ⏱️ Timeframe")
        time_filter = st.radio("Select View:", ["Last 1 Hour", "Last 3 Hours", "Full Day (All Data)"])
        
        st.markdown("### 💰 Financial Integration")
        st.caption("Input POS register data to calculate real-time conversion.")
        daily_revenue = st.number_input("Today's Revenue (₹)", min_value=0, value=25000, step=1000)
        daily_transactions = st.number_input("Total Transactions", min_value=0, value=45, step=1)

    # Apply Time Filter
    store_df = df[df['Store ID'] == store_id]
    now = store_df['Time'].max()
    
    if time_filter == "Last 1 Hour":
        plot_df = store_df[store_df['Time'] >= (now - timedelta(hours=1))]
    elif time_filter == "Last 3 Hours":
        plot_df = store_df[store_df['Time'] >= (now - timedelta(hours=3))]
    else:
        plot_df = store_df

    # --- TOP KPI METRICS ---
    st.markdown("#### Live Store Pulse")
    c1, c2, c3, c4 = st.columns(4)
    
    # Footfall Metrics
    cur_traffic = int(plot_df['Total'].iloc[-1]) if not plot_df.empty else 0
    avg_traffic = int(plot_df['Total'].mean()) if not plot_df.empty else 0
    total_in_store = plot_df['In-Store (Near)'].sum()
    total_street = plot_df['Street (Far)'].sum()
    
    # Financial Metrics Calculations
    capture_rate = (total_in_store / total_street * 100) if total_street > 0 else 0
    conversion_rate = (daily_transactions / total_in_store * 100) if total_in_store > 0 else 0
    rev_per_visitor = (daily_revenue / total_in_store) if total_in_store > 0 else 0

    c1.metric("Live Traffic Density", cur_traffic, f"{(cur_traffic - avg_traffic)} vs timeframe avg", delta_color="normal")
    c2.metric("Walk-in Capture Rate", f"{capture_rate:.1f}%", f"{int(total_in_store)} Total Walk-ins", delta_color="off")
    c3.metric("Sales Conversion", f"{conversion_rate:.1f}%", f"{daily_transactions} POS Transactions", delta_color="off")
    c4.metric("Revenue per Visitor", f"₹{int(rev_per_visitor)}", f"₹{daily_revenue} Total Revenue", delta_color="off")

    st.markdown("<br>", unsafe_allow_html=True)

    # --- TABBED INTERFACE ---
    tab1, tab2, tab3 = st.tabs(["🌊 Live Operations", "💰 Retail Conversion", "📅 Heatmaps & Trends"])

    with tab1:
        st.markdown("#### Spatial Proximity Timeline")
        fig_traffic = px.area(
            plot_df, x='Time', y=['In-Store (Near)', 'Window (Mid)', 'Street (Far)'], 
            color_discrete_map={'Street (Far)': G_GREY, 'Window (Mid)': G_YELLOW, 'In-Store (Near)': G_BLUE}
        )
        st.plotly_chart(apply_clean_style(fig_traffic), use_container_width=True)

    with tab2:
        col1, col2 = st.columns([1, 1])
        with col1:
            st.markdown("#### The Retail Funnel")
            fig_funnel = go.Figure(go.Funnel(
                y=["Street Walkbys", "Window Browsers", "In-Store Walk-ins", "Paying Customers"],
                x=[total_street, plot_df['Window (Mid)'].sum(), total_in_store, daily_transactions],
                textinfo="value+percent previous",
                marker={"color": [G_GREY, G_YELLOW, G_BLUE, G_GREEN]}
            ))
            st.plotly_chart(apply_clean_style(fig_funnel), use_container_width=True)
            
        with col2:
            st.markdown("#### Customer Retention Categories")
            cat_df = pd.DataFrame({
                'Category': ['Bounced (<30s)', 'Browsed (<10m)', 'Retained (>10m)'],
                'Count': [plot_df['Bounced'].max(), plot_df['Browsed'].max(), plot_df['Retained'].max()]
            })
            fig_bar = px.bar(cat_df, x='Count', y='Category', orientation='h', color='Category',
                             color_discrete_map={'Bounced (<30s)': G_RED, 'Browsed (<10m)': G_YELLOW, 'Retained (>10m)': G_BLUE})
            fig_bar.update_layout(showlegend=False)
            st.plotly_chart(apply_clean_style(fig_bar), use_container_width=True)
            
            # Automated Financial Insight
            st.markdown(f"""
                <div class="insight-box finance-box">
                    <div class="insight-title">Financial Insight</div>
                    For every <b>100 people</b> walking past your store, <b>{int(capture_rate)}</b> walk inside, and <b>{int(capture_rate * (conversion_rate/100))}</b> make a purchase. <br><br>
                    Increasing your Window-to-Walk-in capture rate by just 2% could theoretically increase today's revenue by <b>₹{int(daily_revenue * 0.15)}</b>.
                </div>
            """, unsafe_allow_html=True)

    with tab3:
        st.markdown("#### Store Activity Heatmap (By Hour)")
        st.caption("Visualizing peak performance hours to optimize staff scheduling.")
        
        # Aggregate data by Hour for the Heatmap
        hourly_df = store_df.groupby('Hour')[['Street (Far)', 'Window (Mid)', 'In-Store (Near)']].mean().reset_index()
        hourly_df = hourly_df.melt(id_vars='Hour', var_name='Zone', value_name='Average Traffic')
        
        # Plotly Density Heatmap
        fig_heat = px.density_heatmap(
            hourly_df, x="Hour", y="Zone", z="Average Traffic",
            color_continuous_scale="Blues",
            labels={'Zone': 'Store Proximity', 'Hour': 'Time of Day'}
        )
        fig_heat.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Google Sans, sans-serif"),
            margin=dict(l=10, r=10, t=30, b=10)
        )
        st.plotly_chart(fig_heat, use_container_width=True)
