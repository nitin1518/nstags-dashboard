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
    
    .consultant-card { background-color: var(--secondary-background-color); border: 1px solid rgba(128, 134, 139, 0.2); border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem; height: 100%; box-shadow: 0 4px 6px rgba(0,0,0,0.02); }
    .portfolio-card { background: linear-gradient(135deg, rgba(142, 36, 170, 0.05) 0%, rgba(26, 115, 232, 0.05) 100%); border: 1px solid rgba(142, 36, 170, 0.2); border-top: 4px solid #8e24aa; border-radius: 12px; padding: 1.5rem; margin-bottom: 2rem; }
    .card-header { font-size: 0.85rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; color: var(--text-color); opacity: 0.6; margin-bottom: 0.8rem; }
    .card-headline { font-size: 1.5rem; font-weight: 600; color: var(--text-color); line-height: 1.2; margin-bottom: 0.8rem; }
    .card-body { font-size: 0.95rem; color: var(--text-color); opacity: 0.85; line-height: 1.5; }
    
    .hl-blue { color: #1a73e8; font-weight: 600; }
    .hl-green { color: #0f9d58; font-weight: 600; }
    .hl-red { color: #db4437; font-weight: 600; }
    .hl-purple { color: #8e24aa; font-weight: 600; }
    .hl-orange { color: #f4b400; font-weight: 600; }
    
    div[data-testid="metric-container"] { background-color: transparent; border-left: 2px solid rgba(128, 134, 139, 0.2); padding-left: 1rem; }
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
        s3 = boto3.client('s3', region_name=REGION, aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"], aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"])
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

df = load_s3_data()

if df.empty:
    st.warning("Awaiting telemetry. Ensure ESP32 is online.")
else:
    # --- MOBILE OPTIMIZED SIDEBAR ---
    with st.sidebar:
        st.markdown("### ⚙️ Engine Parameters")
        store_id = st.selectbox("Active Node", df['Store ID'].unique(), label_visibility="collapsed")
        
        st.markdown("### 📢 Campaign Strategy")
        ad_type = st.radio("Campaign Type:", ["Partner Brand Ad (Media Matrix)", "Own Store Promotion (Retail Matrix)"])
        
        st.markdown("### 📅 Campaign Schedule")
        c_d1, c_t1 = st.columns(2)
        start_date = c_d1.date_input("Start Date", datetime.now() - timedelta(days=1))
        start_time = c_t1.time_input("Start Time", (datetime.now() - timedelta(hours=4)).time())
        
        c_d2, c_t2 = st.columns(2)
        end_date = c_d2.date_input("End Date", datetime.now())
        end_time = c_t2.time_input("End Time", datetime.now().time())
        
        # Combine Date & Time into single datetime objects
        camp_start = datetime.combine(start_date, start_time)
        camp_end = datetime.combine(end_date, end_time)
        
        ad_value = st.number_input("Total Marketing Spend / Ad Revenue (₹)", min_value=0, value=5000, step=500)

        # Show Retail POS inputs ONLY if it's an Own Store Promotion
        if ad_type == "Own Store Promotion (Retail Matrix)":
            st.markdown("### 💰 POS Integration")
            daily_revenue = st.number_input("Total POS Revenue in Window (₹)", min_value=0, value=45000, step=1000)
            daily_transactions = st.number_input("Total Store Transactions in Window", min_value=0, value=35, step=1)
            
            track_product = st.checkbox("Track Specific Product Sales?")
            if track_product:
                prod_revenue = st.number_input("Product Revenue (₹)", min_value=0, value=12000, step=500)
                prod_transactions = st.number_input("Product Transactions", min_value=0, value=8, step=1)

    # --- DATA FILTERING ---
    net_df = df[(df['Time'] >= camp_start) & (df['Time'] <= camp_end)]
    n_street = net_df['Street'].sum()
    n_instore = net_df['InStore'].sum()
    n_capture = (n_instore / n_street) if n_street > 0 else 0

    store_df = df[df['Store ID'] == store_id]
    camp_df = store_df[(store_df['Time'] >= camp_start) & (store_df['Time'] <= camp_end)]
    
    # Calculate baseline exactly 7 days prior (or 1 day if data is short) to match footfall trends
    baseline_start = camp_start - timedelta(days=1)
    baseline_end = camp_end - timedelta(days=1)
    base_df = store_df[(store_df['Time'] >= baseline_start) & (store_df['Time'] <= baseline_end)]

    # Metrics
    c_street = camp_df['Street'].sum()
    c_window = camp_df['Window'].sum()
    c_instore = camp_df['InStore'].sum()
    b_instore = base_df['InStore'].sum() if not base_df.empty else 0
    c_capture = (c_instore / c_street) if c_street > 0 else 0

    # ==========================================
    # 👑 TIER 0: MULTI-STORE PORTFOLIO
    # ==========================================
    store_stats = net_df.groupby('Store ID')[['Street', 'InStore']].sum().reset_index()
    store_stats['Capture_Rate'] = store_stats['InStore'] / store_stats['Street']
    valid_stores = store_stats[store_stats['Street'] > 50]
    
    if len(valid_stores) > 1:
        best_store = valid_stores.loc[valid_stores['Capture_Rate'].idxmax()]
        st.markdown(f"""
            <div class="portfolio-card">
                <div class="card-header">👑 Network Portfolio Analysis</div>
                <div class="card-headline">Top Performer: Store {best_store['Store ID']}</div>
                <div class="card-body">This location led your network during this time window with a <span class='hl-purple'>{best_store['Capture_Rate']*100:.1f}%</span> Capture Rate.</div>
            </div>
        """, unsafe_allow_html=True)
    elif len(valid_stores) == 1:
        st.markdown("<hr style='margin: 1rem 0; opacity: 0.2;'>", unsafe_allow_html=True)

    # ==========================================
    # 🚀 DYNAMIC MATRIX RENDERING
    # ==========================================
    if ad_type == "Partner Brand Ad (Media Matrix)":
        # ----------------------------------------------------
        # THE MEDIA MATRIX (For Partner Ads)
        # Focuses entirely on Impressions, Window Engagements, and CPM
        # ----------------------------------------------------
        cpm = (ad_value / c_street * 1000) if c_street > 0 else 0
        stop_rate = (c_window / c_street * 100) if c_street > 0 else 0
        cpe = (ad_value / c_window) if c_window > 0 else 0 # Cost Per Engagement
        
        st.markdown("#### Media Performance Insights")
        e1, e2, e3 = st.columns(3)
        with e1:
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #1a73e8;"><div class="card-header">DOOH Valuation</div><div class="card-headline">₹{cpm:,.2f} Effective CPM</div><div class="card-body">You delivered <span class='hl-blue'>{int(c_street):,}</span> total verified impressions to the brand during this window.</div></div>""", unsafe_allow_html=True)
        with e2:
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #fbbc04;"><div class="card-header">Engagement Value</div><div class="card-headline">₹{cpe:,.2f} Cost Per Stop</div><div class="card-body">The partner paid ₹{cpe:,.2f} for every person who explicitly slowed down to view their ad (<span class='hl-orange'>{int(c_window):,}</span> engagements).</div></div>""", unsafe_allow_html=True)
        with e3:
            diff = (c_capture - n_capture) * 100
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #9aa0a6;"><div class="card-header">Location Premium</div><div class="card-headline">{"Premium" if diff > 0 else "Standard"} Tier</div><div class="card-body">Your storefront captures attention <span class='hl-blue'>{abs(diff):.1f}%</span> {"better" if diff > 0 else "worse"} than the network average. Use this to negotiate rates.</div></div>""", unsafe_allow_html=True)

        st.markdown("<br>#### Ad Visibility Metrics", unsafe_allow_html=True)
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Street Impressions", f"{int(c_street):,}", "Addressable Audience")
        m2.metric("Window Engagements", f"{int(c_window):,}", f"{stop_rate:.1f}% Stop Rate")
        m3.metric("Cost Per Mille (CPM)", f"₹{cpm:,.2f}")
        m4.metric("Total Ad Revenue", f"₹{ad_value:,.0f}")

        st.markdown("<br>", unsafe_allow_html=True)
        tab1, tab2 = st.tabs(["🚦 Impression Flow", "🎯 Media Engagement Funnel"])
        mobile_config = {'displayModeBar': False}
        with tab1:
            fig_lines = go.Figure()
            fig_lines.add_trace(go.Scatter(x=camp_df['Time'], y=camp_df['Street'], mode='lines', name='Street Exposure (Impressions)', line=dict(color='#9aa0a6', width=2, shape='spline')))
            fig_lines.add_trace(go.Scatter(x=camp_df['Time'], y=camp_df['Window'], mode='lines', name='Window Stops (Engagements)', line=dict(color='#1a73e8', width=3, shape='spline')))
            fig_lines.update_layout(hovermode="x unified", margin=dict(l=0, r=0, t=40, b=60), legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5))
            fig_lines.update_xaxes(showgrid=False)
            fig_lines.update_yaxes(showgrid=True, gridcolor="rgba(128,134,139,0.1)")
            st.plotly_chart(fig_lines, use_container_width=True, theme="streamlit", config=mobile_config)
        with tab2:
            fig_funnel = go.Figure(go.Funnel(
                y=["Total Exposure (Street)", "Direct Engagements (Window)"],
                x=[c_street, c_window],
                textinfo="value+percent previous",
                marker={"color": ["#9aa0a6", "#1a73e8"]}
            ))
            fig_funnel.update_layout(margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_funnel, use_container_width=True, theme="streamlit", config=mobile_config)

    else:
        # ----------------------------------------------------
        # THE RETAIL MATRIX (For Own Store Promotions)
        # Focuses entirely on Incremental Lift, Walk-ins, and POS
        # ----------------------------------------------------
        incremental_walkins = c_instore - b_instore
        lift_pct = (incremental_walkins / b_instore * 100) if b_instore > 0 else 0
        aov = (daily_revenue / daily_transactions) if daily_transactions > 0 else 0
        s_conversion = (daily_transactions / c_instore) if c_instore > 0 else 0

        st.markdown("#### Store Promotion Insights")
        e1, e2, e3 = st.columns(3)
        with e1:
            if incremental_walkins <= 0:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #db4437;"><div class="card-header">Campaign Lift</div><div class="card-headline" style="color: #db4437;">No Incremental Lift</div><div class="card-body">Your investment of ₹{ad_value:,} generated <span class='hl-red'>0 extra walk-ins</span> compared to your historic baseline.</div></div>""", unsafe_allow_html=True)
            else:
                true_cac = (ad_value / incremental_walkins)
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #0f9d58;"><div class="card-header">Campaign Lift</div><div class="card-headline">+{lift_pct:.1f}% Traffic Uplift</div><div class="card-body">This ad brought in <span class='hl-green'>{int(incremental_walkins):,}</span> extra people vs your historic baseline. Your True Cost Per Acquisition is <span class='hl-green'>₹{true_cac:,.2f}</span>.</div></div>""", unsafe_allow_html=True)
        
        with e2:
            rev_to_use = prod_revenue if track_product else (max(0, incremental_walkins) * aov)
            roas = (rev_to_use / ad_value) if ad_value > 0 else 0
            label = "Product-Specific" if track_product else "Incremental"
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #fbbc04;"><div class="card-header">{label} ROAS</div><div class="card-headline">{roas:.1f}x Return</div><div class="card-body">Every rupee spent on this campaign returned <span class='hl-orange'>₹{roas:.1f}</span> in {label.lower()} sales.</div></div>""", unsafe_allow_html=True)
        
        with e3:
            lost_opps = int(c_instore - daily_transactions)
            if s_conversion < 0.20 and c_instore > daily_transactions:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #db4437;"><div class="card-header">Sales Conversion Leakage</div><div class="card-headline">{s_conversion*100:.1f}% Close Rate</div><div class="card-body"><span class='hl-red'>{lost_opps}</span> people walked in during the ad but left empty-handed. Deploy more floor staff to capitalize on ad traffic.</div></div>""", unsafe_allow_html=True)
            else:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #0f9d58;"><div class="card-header">Sales Execution</div><div class="card-headline">{s_conversion*100:.1f}% Close Rate</div><div class="card-body">Your staff efficiently converted the walk-ins driven by this campaign.</div></div>""", unsafe_allow_html=True)

        st.markdown("<br>#### Retail & Sales Metrics", unsafe_allow_html=True)
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Walk-ins", f"{int(c_instore):,}", f"{int(b_instore)} Baseline (Prior)", delta_color="normal")
        m2.metric("Incremental Walk-ins", f"{int(max(0, incremental_walkins)):,}", "Driven specifically by Ad", delta_color="off")
        m3.metric("POS Conversion", f"{s_conversion*100:.1f}%", f"{int(daily_transactions)} Sales")
        m4.metric("Avg Order Value", f"₹{aov:,.0f}", "Based on POS Input")

        st.markdown("<br>", unsafe_allow_html=True)
        tab1, tab2 = st.tabs(["🚦 Traffic & Baseline Comparison", "🎯 Sales Funnel"])
        mobile_config = {'displayModeBar': False}
        with tab1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=camp_df['Time'], y=camp_df['InStore'], name='Active Walk-ins', line=dict(color='#1a73e8', width=3, shape='spline')))
            # Overlay historical baseline average to visually prove lift
            baseline_avg = b_instore / len(camp_df) if not camp_df.empty and len(camp_df) > 0 else 0
            fig.add_hline(y=baseline_avg, line_dash="dot", annotation_text="Historic Baseline Average", line_color="#9aa0a6")
            fig.update_layout(hovermode="x unified", margin=dict(l=0, r=0, t=40, b=60), legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5))
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=True, gridcolor="rgba(128,134,139,0.1)")
            st.plotly_chart(fig, use_container_width=True, theme="streamlit", config=mobile_config)
        with tab2:
            fig_f = go.Figure(go.Funnel(
                y=["Street Visibility", "Window Browsers", "Store Walk-ins", "POS Transactions"],
                x=[c_street, c_window, c_instore, daily_transactions],
                textinfo="value+percent previous",
                marker={"color": ["#9aa0a6", "#fbbc04", "#1a73e8", "#0f9d58"]}
            ))
            fig_f.update_layout(margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_f, use_container_width=True, theme="streamlit", config=mobile_config)
