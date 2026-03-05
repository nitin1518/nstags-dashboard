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
st.set_page_config(page_title="Data | Retail Intelligence", page_icon="📈", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    
    .consultant-card {
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 134, 139, 0.2);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        height: 100%;
        box-shadow: 0 4px 6px rgba(0,0,0,0.02);
    }
    
    .portfolio-card {
        background: linear-gradient(135deg, rgba(142, 36, 170, 0.05) 0%, rgba(26, 115, 232, 0.05) 100%);
        border: 1px solid rgba(142, 36, 170, 0.2);
        border-top: 4px solid #8e24aa;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 2rem;
    }
    
    .card-header {
        font-size: 0.85rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: var(--text-color);
        opacity: 0.6;
        margin-bottom: 0.8rem;
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
    
    .hl-blue { color: #1a73e8; font-weight: 600; }
    .hl-green { color: #0f9d58; font-weight: 600; }
    .hl-red { color: #db4437; font-weight: 600; }
    .hl-purple { color: #8e24aa; font-weight: 600; }
    .hl-orange { color: #f4b400; font-weight: 600; }
    
    div[data-testid="metric-container"] {
        background-color: transparent;
        border-left: 2px solid rgba(128, 134, 139, 0.2);
        padding-left: 1rem;
    }
    [data-testid="stMetricValue"] { font-size: 2rem; font-weight: 600; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem; text-transform: uppercase; opacity: 0.7; }
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
        # Pulling more files to ensure we have historical baselines
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='footfall/')
        if 'Contents' not in response: return pd.DataFrame()
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[:100]
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
st.markdown("<h2 style='margin-bottom: 0;'>Data Intelligence</h2>", unsafe_allow_html=True)
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
        
        st.markdown("### 📢 Campaign Strategy")
        ad_type = st.radio("Campaign Type:", ["Partner Brand Ad", "Own Store Promotion"])
        
        track_product = False
        hist_baseline = False
        camp_hours = 0
        
        if ad_type == "Partner Brand Ad (e.g., Oppo)":
            ad_value = st.number_input("Ad Revenue Received (₹)", min_value=0, value=15000, step=1000)
            time_filter = st.selectbox("Intelligence Window", ["Full Day", "Last 3 Hours", "Last 1 Hour"])
        else:
            ad_value = st.number_input("Total Marketing Spend (₹)", min_value=0, value=5000, step=500)
            camp_hours = st.number_input("Campaign Duration (Hours)", min_value=1, value=4)
            hist_baseline = st.checkbox("Calculate Incremental Lift (Historic Baseline)")
            
            track_product = st.checkbox("Track Specific Product Sales?")
            if track_product:
                st.info("Tracking ad impact for this specific product only.")
                prod_revenue = st.number_input("Product Revenue (₹)", min_value=0, value=12000, step=500)
                prod_transactions = st.number_input("Product Transactions", min_value=0, value=8, step=1)
            time_filter = "Custom Campaign" # Override window for internal promos

        st.markdown("### 💰 Internal POS Integration")
        daily_revenue = st.number_input("Total POS Revenue (₹)", min_value=0, value=45000, step=1000)
        daily_transactions = st.number_input("Total Store Transactions", min_value=0, value=35, step=1)

    # --- DATA FILTERING & NETWORK MATH ---
    now = df['Time'].max()
    
    # Define active window based on campaign type
    if time_filter == "Last 1 Hour": time_delta = timedelta(hours=1)
    elif time_filter == "Last 3 Hours": time_delta = timedelta(hours=3)
    elif time_filter == "Full Day": time_delta = timedelta(hours=24)
    else: time_delta = timedelta(hours=camp_hours) # Custom for internal promos

    net_df = df[df['Time'] >= (now - time_delta)]
    n_street, n_instore = net_df['Street'].sum(), net_df['InStore'].sum()
    n_capture = (n_instore / n_street) if n_street > 0 else 0

    store_df = net_df[net_df['Store ID'] == store_id]
    s_street, s_window, s_instore = store_df['Street'].sum(), store_df['Window'].sum(), store_df['InStore'].sum()
    
    s_capture = (s_instore / s_street) if s_street > 0 else 0
    s_conversion = (daily_transactions / s_instore) if s_instore > 0 else 0
    aov = (daily_revenue / daily_transactions) if daily_transactions > 0 else 0

    # ==========================================
    # 👑 TIER 0: MULTI-STORE PORTFOLIO ANALYSIS
    # ==========================================
    store_stats = net_df.groupby('Store ID')[['Street', 'InStore']].sum().reset_index()
    store_stats['Capture_Rate'] = store_stats['InStore'] / store_stats['Street']
    valid_stores = store_stats[store_stats['Street'] > 50]
    
    if len(valid_stores) > 1:
        best_store = valid_stores.loc[valid_stores['Capture_Rate'].idxmax()]
        best_id = best_store['Store ID']
        best_cap_rate = best_store['Capture_Rate'] * 100
        best_ins = int(best_store['InStore'])
        
        st.markdown(f"""
            <div class="portfolio-card">
                <div class="card-header">👑 Multi-Store Portfolio Analysis</div>
                <div class="card-headline">Top Performer: Store ID {best_id}</div>
                <div class="card-body">This location is currently leading your retail network with a <span class='hl-purple'>{best_cap_rate:.1f}%</span> Walk-in Capture Rate ({best_ins} total walk-ins). <br><br><b>Consultant Action:</b> Identify the specific window displays or exterior signage currently deployed at Store {best_id} and replicate them across your underperforming locations.</div>
            </div>
        """, unsafe_allow_html=True)
    elif len(valid_stores) == 1:
        st.markdown("<hr style='margin: 1rem 0; opacity: 0.2;'>", unsafe_allow_html=True)

    # ==========================================
    # 🏆 TIER 1: EXECUTIVE STRATEGY BOARD
    # ==========================================
    st.markdown("#### Store-Level Strategic Insights")
    e1, e2, e3 = st.columns(3)

    # INSIGHT 1: EXTERIOR CAMPAIGN LOGIC
    with e1:
        if ad_type == "Partner Brand Ad (e.g., Oppo)":
            cpm = (ad_value / s_street * 1000) if s_street > 0 else 0
            stop_rate = (s_window / s_street * 100) if s_street > 0 else 0
            
            st.markdown(f"""
                <div class="consultant-card" style="border-top: 4px solid #1a73e8;">
                    <div class="card-header">📢 Brand Ad Visibility (DOOH)</div>
                    <div class="card-headline">₹{cpm:,.2f} Effective CPM</div>
                    <div class="card-body">Your storefront provided the partner brand with <span class='hl-blue'>{int(s_street):,}</span> verified street impressions.<br><br><b>Value Delivered:</b> <span class='hl-blue'>{int(s_window):,}</span> people explicitly stopped to view the ad (a <b>{stop_rate:.1f}%</b> engagement rate). Share this data with the brand to negotiate future placements.</div>
                </div>
            """, unsafe_allow_html=True)
            
        else: 
            # INCREMENTALITY TESTING LOGIC
            base_walkins = 0
            incremental_walkins = s_instore
            lift_pct = 0
            
            if hist_baseline:
                # Calculate historic baseline based on previous equivalent timeframe
                full_store_df = df[df['Store ID'] == store_id]
                baseline_end = now - timedelta(hours=camp_hours)
                baseline_start = baseline_end - timedelta(hours=camp_hours)
                base_df = full_store_df[(full_store_df['Time'] >= baseline_start) & (full_store_df['Time'] < baseline_end)]
                base_walkins = base_df['InStore'].sum()
                
                incremental_walkins = s_instore - base_walkins
                if incremental_walkins < 0: incremental_walkins = 0 # Cannot have negative new customers
                lift_pct = ((s_instore - base_walkins) / base_walkins * 100) if base_walkins > 0 else 0

            if incremental_walkins <= 0:
                st.markdown(f"""
                    <div class="consultant-card" style="border-top: 4px solid #db4437;">
                        <div class="card-header">📢 Campaign Incrementality</div>
                        <div class="card-headline" style="color: #db4437;">0% Lift (Ad Failure)</div>
                        <div class="card-body">Your investment of ₹{ad_value:,} generated <span class='hl-red'>0 incremental walk-ins</span> compared to your historic baseline.<br><br><b>Consultant Action:</b> The promotion is not altering consumer behavior. Terminate the campaign or alter the signage immediately.</div>
                    </div>
                """, unsafe_allow_html=True)
            else:
                # Calculate TRUE CAC using strictly incremental customers
                true_cac = (ad_value / incremental_walkins)
                
                # Dynamic Logic: Full Store vs Specific Product Track
                if track_product:
                    true_roas = (prod_revenue / ad_value) if ad_value > 0 else 0
                    focus_text = f"Using <b>product-specific</b> revenue"
                else:
                    true_roas = ((incremental_walkins * aov) / ad_value) if ad_value > 0 else 0
                    focus_text = f"Using your store AOV"

                baseline_text = f" This represents a <span class='hl-green'>+{lift_pct:.1f}%</span> incremental lift over your historic baseline." if hist_baseline else ""

                st.markdown(f"""
                    <div class="consultant-card" style="border-top: 4px solid #0f9d58;">
                        <div class="card-header">📢 Campaign Incrementality</div>
                        <div class="card-headline">₹{true_cac:,.2f} True CAC</div>
                        <div class="card-body">Your ₹{ad_value:,} spend successfully generated <span class='hl-green'>{int(incremental_walkins):,}</span> <b>incremental</b> walk-ins.{baseline_text} <br><br>{focus_text}, this campaign yields a True Return on Ad Spend (ROAS) of <span class='hl-green'>{true_roas:.1f}x</span>.</div>
                    </div>
                """, unsafe_allow_html=True)

    # INSIGHT 2: CROSS-STORE BENCHMARKING
    with e2:
        diff = (s_capture - n_capture) * 100
        if diff > 0:
            rank = "Outperforming Market"
            bench = f"Your storefront pulls <span class='hl-green'>+{diff:.1f}% more</span> street traffic inside compared to the network average."
            color = "#0f9d58"
        else:
            rank = "Below Market Average"
            bench = f"Your storefront underperforms the network average by <span class='hl-red'>{abs(diff):.1f}%</span>. Consider updating window lighting."
            color = "#db4437"
            
        st.markdown(f"""
            <div class="consultant-card" style="border-top: 4px solid {color};">
                <div class="card-header">🌐 Walk-in Network Benchmark</div>
                <div class="card-headline">{rank}</div>
                <div class="card-body">Your Capture Rate is <span class='hl-blue'>{s_capture*100:.2f}%</span> vs the network average of <span class='hl-blue'>{n_capture*100:.2f}%</span>. <br><br>{bench}</div>
            </div>
        """, unsafe_allow_html=True)

    # INSIGHT 3: INTERNAL STAFF & FLOOR LEAKAGE
    with e3:
        target_close = 0.20
        lost_opps = int(s_instore - daily_transactions)
        if s_conversion < target_close and s_instore > daily_transactions:
            diag = f"High Floor Abandonment"
            fix = f"Staff is closing <span class='hl-red'>{s_conversion*100:.1f}%</span> of Walk-ins. <span class='hl-red'>{lost_opps}</span> people left empty-handed. <br><br><b>Action:</b> Deploy more staff to the floor or adjust pricing visibility."
        else:
            diag = "Strong Sales Execution"
            fix = f"Staff is closing <span class='hl-green'>{s_conversion*100:.1f}%</span> of Walk-ins. Focus your efforts on driving more street traffic into the store, as your team converts well."

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
    mobile_config = {'displayModeBar': False}

    with tab1:
        fig_lines = go.Figure()
        fig_lines.add_trace(go.Scatter(x=store_df['Time'], y=store_df['Street'], mode='lines', name='Street (Far)', line=dict(color='#9aa0a6', width=1.5, shape='spline')))
        fig_lines.add_trace(go.Scatter(x=store_df['Time'], y=store_df['Window'], mode='lines', name='Window (Mid)', line=dict(color='#fbbc04', width=2, shape='spline')))
        fig_lines.add_trace(go.Scatter(x=store_df['Time'], y=store_df['InStore'], mode='lines', name='Walk-ins (Near)', line=dict(color='#1a73e8', width=3, shape='spline')))
        
        fig_lines.update_layout(
            hovermode="x unified", margin=dict(l=0, r=0, t=40, b=60),
            legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5)
        )
        fig_lines.update_xaxes(showgrid=False)
        fig_lines.update_yaxes(showgrid=True, gridcolor="rgba(128,134,139,0.1)")
        st.plotly_chart(fig_lines, use_container_width=True, theme="streamlit", config=mobile_config)

    with tab2:
        col1, col2 = st.columns([1, 1])
        with col1:
            fig_funnel = go.Figure(go.Funnel(
                y=["Street Exposure", "Window Browsers", "Store Walk-ins", "POS Transactions"],
                x=[s_street, s_window, s_instore, daily_transactions],
                textinfo="value+percent previous",
                marker={"color": ["rgba(154,160,166,0.6)", "rgba(251,188,4,0.8)", "rgba(26,115,232,0.9)", "rgba(15,157,88,1)"]}
            ))
            fig_funnel.update_layout(margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_funnel, use_container_width=True, theme="streamlit", config=mobile_config)
            
        with col2:
            cat_df = pd.DataFrame({
                'Category': ['Bounced (<30s)', 'Browsed (<10m)', 'Retained (>10m)'],
                'Count': [store_df['Bounced'].max(), store_df['Browsed'].max(), store_df['Retained'].max()]
            })
            fig_bar = px.bar(cat_df, x='Count', y='Category', orientation='h', color='Category',
                             color_discrete_map={'Bounced (<30s)': '#ea4335', 'Browsed (<10m)': '#fbbc04', 'Retained (>10m)': '#1a73e8'})
            fig_bar.update_layout(showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_bar, use_container_width=True, theme="streamlit", config=mobile_config)

    with tab3:
        hourly_df = store_df.groupby('Hour')[['Street', 'Window', 'InStore']].mean().reset_index()
        hourly_df = hourly_df.melt(id_vars='Hour', var_name='Zone', value_name='Avg Traffic')
        fig_heat = px.density_heatmap(hourly_df, x="Hour", y="Zone", z="Avg Traffic", color_continuous_scale="Blues")
        fig_heat.update_layout(margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_heat, use_container_width=True, theme="streamlit", config=mobile_config)
