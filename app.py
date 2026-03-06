import streamlit as st
import boto3
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ==========================================
# 🎨 GOOGLE MATERIAL 3 UI/UX CONFIGURATION
# ==========================================
st.set_page_config(page_title="nsTags | Retail Intelligence", page_icon="📊", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Google+Sans:wght@400;500;700&display=swap');
    
    html, body, [class*="css"] { font-family: 'Google Sans', sans-serif; }
    
    .consultant-card {
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 134, 139, 0.2);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        height: 100%;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    
    .portfolio-card {
        background: linear-gradient(135deg, rgba(26, 115, 232, 0.05) 0%, rgba(52, 168, 83, 0.05) 100%);
        border: 1px solid rgba(26, 115, 232, 0.2);
        border-top: 4px solid #1a73e8;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 2rem;
    }
    
    .card-header { font-size: 0.85rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; color: #5f6368; margin-bottom: 0.5rem; }
    .card-headline { font-size: 1.5rem; font-weight: 500; color: var(--text-color); line-height: 1.2; margin-bottom: 0.5rem; }
    .card-body { font-size: 0.95rem; color: #5f6368; line-height: 1.5; }
    
    .hl-blue { color: #1a73e8; font-weight: 600; }
    .hl-green { color: #34a853; font-weight: 600; }
    .hl-red { color: #ea4335; font-weight: 600; }
    .hl-orange { color: #fbbc04; font-weight: 600; }
    
    div[data-testid="metric-container"] { border-left: 2px solid rgba(128, 134, 139, 0.2); padding-left: 1rem; }
    [data-testid="stMetricValue"] { font-size: 2rem; font-weight: 500; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem; text-transform: uppercase; color: #5f6368; }
    </style>
""", unsafe_allow_html=True)

# Google Material Palette
G_BLUE, G_RED, G_YELLOW, G_GREEN, G_GREY = '#1a73e8', '#ea4335', '#fbbc04', '#34a853', '#9aa0a6'

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
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[:150] # Increased file pull for historical views
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

# Global Plotly styling to fix mobile overlaps (Legend Top-Left, space for Modebar Top-Right)
def style_chart(fig):
    fig.update_layout(
        hovermode="x unified",
        margin=dict(l=0, r=0, t=50, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Google Sans, sans-serif", color="#5f6368")
    )
    fig.update_xaxes(showgrid=False, zeroline=False, title_text="")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(128,134,139,0.1)", zeroline=False, title_text="")
    return fig

# ==========================================
# 🧠 UI RENDERING
# ==========================================
st.markdown("<h2 style='margin-bottom: 0;'>nsTags Intelligence</h2>", unsafe_allow_html=True)
st.caption("Storefront Monetization & Behavioral Engine")

with st.spinner("Synchronizing with AWS Data Lake..."):
    df = load_s3_data()

if df.empty:
    st.warning("Awaiting telemetry. Ensure ESP32 is online.")
else:
    # --- SMART SIDEBAR ---
    with st.sidebar:
        st.markdown("### ⚙️ Intelligence Node")
        store_id = st.selectbox("Active Store", df['Store ID'].unique(), label_visibility="collapsed")
        
        st.markdown("### 📢 Strategy Context")
        ad_type = st.radio("What are we measuring?", ["Partner Ad (Media Impact)", "Own Promotion (Retail ROI)"])
        
        st.markdown("### 📅 Time Horizon")
        time_preset = st.selectbox("Select Period", ["Today", "Yesterday", "This Week", "This Month", "Custom Date Range"])
        
        # Dynamic Time Calculation
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
        elif time_preset == "This Month":
            camp_start = latest_time - timedelta(days=30)
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

        st.markdown("### 💰 Financial Integration")
        if ad_type == "Partner Ad (Media Impact)":
            ad_value = st.number_input("Ad Revenue Received from Brand (₹)", min_value=0, value=15000, step=1000)
            st.info("Tracking Media Impressions and DOOH Value.")
        else:
            ad_value = st.number_input("Marketing Campaign Spend (₹)", min_value=0, value=5000, step=500)
            hist_baseline = st.checkbox("Calculate Incremental Lift (vs Baseline)", value=True)
            
            track_product = st.checkbox("Isolate Specific Product Sales?")
            if track_product:
                prod_revenue = st.number_input("Product Revenue (₹)", min_value=0, value=12000, step=500)
                prod_transactions = st.number_input("Product Transactions", min_value=0, value=8, step=1)
                
            daily_revenue = st.number_input("Total POS Revenue in Period (₹)", min_value=0, value=45000, step=1000)
            daily_transactions = st.number_input("Total Store Transactions", min_value=0, value=35, step=1)

    # --- DATA FILTERING ---
    net_df = df[(df['Time'] >= camp_start) & (df['Time'] <= camp_end)]
    n_street, n_instore = net_df['Street'].sum(), net_df['InStore'].sum()
    n_capture = (n_instore / n_street) if n_street > 0 else 0

    store_df = df[df['Store ID'] == store_id]
    camp_df = store_df[(store_df['Time'] >= camp_start) & (store_df['Time'] <= camp_end)]
    s_street, s_window, s_instore = camp_df['Street'].sum(), camp_df['Window'].sum(), camp_df['InStore'].sum()
    s_capture = (s_instore / s_street) if s_street > 0 else 0

    # ==========================================
    # 👑 TIER 0: PORTFOLIO INTELLIGENCE
    # ==========================================
    store_stats = net_df.groupby('Store ID')[['Street', 'InStore']].sum().reset_index()
    store_stats['Capture_Rate'] = store_stats['InStore'] / store_stats['Street']
    valid_stores = store_stats[store_stats['Street'] > 50]
    
    if len(valid_stores) > 1:
        best_store = valid_stores.loc[valid_stores['Capture_Rate'].idxmax()]
        st.markdown(f"""
            <div class="portfolio-card">
                <div class="card-header">👑 Network Portfolio Analysis</div>
                <div class="card-headline">Crown Jewel Location: Store {best_store['Store ID']}</div>
                <div class="card-body">This location led your entire network during this timeframe with a <span class='hl-blue'>{best_store['Capture_Rate']*100:.1f}%</span> walk-in capture rate. <b>Consultant Action:</b> Audit this location's current visual merchandising and implement its layout at underperforming stores.</div>
            </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("<hr style='margin: 1rem 0; opacity: 0.2;'>", unsafe_allow_html=True)

    # ==========================================
    # 🚀 DYNAMIC MATRIX RENDERING (CONTEXT AWARE)
    # ==========================================
    if ad_type == "Partner Ad (Media Impact)":
        # ----------------------------------------------------
        # 🏢 THE MEDIA MATRIX
        # ----------------------------------------------------
        cpm = (ad_value / s_street * 1000) if s_street > 0 else 0
        stop_rate = (s_window / s_street * 100) if s_street > 0 else 0
        cpe = (ad_value / s_window) if s_window > 0 else 0 

        st.markdown("#### Media Performance Insights")
        e1, e2, e3 = st.columns(3)
        with e1:
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #1a73e8;"><div class="card-header">Billboard Value (DOOH)</div><div class="card-headline">₹{cpm:,.2f} Effective CPM</div><div class="card-body">You delivered <span class='hl-blue'>{int(s_street):,}</span> verified street impressions to the partner brand. Share this with them as Proof of Performance.</div></div>""", unsafe_allow_html=True)
        with e2:
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #fbbc04;"><div class="card-header">Engagement Leverage</div><div class="card-headline">₹{cpe:,.2f} Cost Per Stop</div><div class="card-body">The partner paid ₹{cpe:,.2f} for every single person who explicitly slowed down to view their ad (<span class='hl-orange'>{int(s_window):,}</span> visual engagements).</div></div>""", unsafe_allow_html=True)
        with e3:
            diff = (s_capture - n_capture) * 100
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #9aa0a6;"><div class="card-header">Location Premium</div><div class="card-headline">{"Premium" if diff > 0 else "Standard"} Tier</div><div class="card-body">Your storefront captures attention <span class='hl-blue'>{abs(diff):.1f}%</span> {"better" if diff > 0 else "worse"} than the network average.</div></div>""", unsafe_allow_html=True)

        st.markdown("<br>#### Ad Visibility Metrics", unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Street Impressions", f"{int(s_street):,}", "Addressable Audience")
        c2.metric("Window Engagements", f"{int(s_window):,}", f"{stop_rate:.1f}% Stop Rate")
        c3.metric("Effective CPM", f"₹{cpm:,.2f}", "Target: ₹150-300", delta_color="off")
        c4.metric("Ad Revenue", f"₹{ad_value:,}", "Generated")

        # CONTEXTUAL CHARTS: Only showing Media metrics
        st.markdown("<br>", unsafe_allow_html=True)
        tab1, tab2 = st.tabs(["🚦 Visibility Timeline", "🎯 Media Engagement Funnel"])
        with tab1:
            fig = px.area(camp_df, x='Time', y=['Window', 'Street'], 
                          color_discrete_map={'Street': 'rgba(154,160,166,0.3)', 'Window': G_YELLOW},
                          labels={'value': 'Detected Devices', 'variable': 'Exposure'})
            # Area chart stacking adjustment
            fig.update_traces(fill='tozeroy')
            st.plotly_chart(style_chart(fig), use_container_width=True, theme="streamlit")
        with tab2:
            fig_f = go.Figure(go.Funnel(
                y=["Total Exposure (Street)", "Direct Engagements (Window)"],
                x=[s_street, s_window],
                textinfo="value+percent previous",
                marker={"color": [G_GREY, G_YELLOW]}
            ))
            st.plotly_chart(style_chart(fig_f), use_container_width=True, theme="streamlit")

    else:
        # ----------------------------------------------------
        # 🛍️ THE RETAIL MATRIX 
        # ----------------------------------------------------
        s_conversion = (daily_transactions / s_instore) if s_instore > 0 else 0
        aov = (daily_revenue / daily_transactions) if daily_transactions > 0 else 0

        # Calculate Baseline Duration (match the exact time length, shifted by a week to account for day-of-week trends, or a day if data is short)
        duration = camp_end - camp_start
        baseline_start = camp_start - timedelta(days=1)
        baseline_end = camp_end - timedelta(days=1)
        base_df = store_df[(store_df['Time'] >= baseline_start) & (store_df['Time'] <= baseline_end)]
        
        base_walkins = base_df['InStore'].sum() if hist_baseline and not base_df.empty else 0
        incremental_walkins = s_instore - base_walkins if hist_baseline else s_instore
        if incremental_walkins < 0: incremental_walkins = 0 
        lift_pct = ((s_instore - base_walkins) / base_walkins * 100) if base_walkins > 0 and hist_baseline else 0

        st.markdown("#### Store Promotion Insights")
        e1, e2, e3 = st.columns(3)
        with e1:
            if incremental_walkins <= 0 and hist_baseline:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #ea4335;"><div class="card-header">Campaign Lift</div><div class="card-headline" style="color: #ea4335;">0% Lift (Ad Failure)</div><div class="card-body">Your investment generated <span class='hl-red'>0 incremental walk-ins</span> compared to your historic baseline. The promotion did not alter consumer behavior.</div></div>""", unsafe_allow_html=True)
            else:
                true_cac = (ad_value / incremental_walkins) if incremental_walkins > 0 else 0
                txt = f"This ad brought in <span class='hl-green'>{int(incremental_walkins):,}</span> extra people vs your historic baseline." if hist_baseline else f"The campaign yielded <span class='hl-green'>{int(incremental_walkins):,}</span> total walk-ins."
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #34a853;"><div class="card-header">Customer Acquisition</div><div class="card-headline">₹{true_cac:,.2f} True CAC</div><div class="card-body">{txt}</div></div>""", unsafe_allow_html=True)
        
        with e2:
            rev_to_use = prod_revenue if track_product else (incremental_walkins * aov)
            roas = (rev_to_use / ad_value) if ad_value > 0 else 0
            label = "Product-Specific" if track_product else "Incremental"
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #fbbc04;"><div class="card-header">{label} ROAS</div><div class="card-headline">{roas:.1f}x Return</div><div class="card-body">Every rupee spent on this campaign returned <span class='hl-orange'>₹{roas:.1f}</span> in {label.lower()} sales at the register.</div></div>""", unsafe_allow_html=True)
        
        with e3:
            if s_conversion < 0.20 and s_instore > daily_transactions:
                lost_opps = int(s_instore - daily_transactions)
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #ea4335;"><div class="card-header">Floor Leakage</div><div class="card-headline">{s_conversion*100:.1f}% Close Rate</div><div class="card-body"><span class='hl-red'>{lost_opps}</span> people walked in but left empty-handed. Your bottleneck is sales staff, not the ad.</div></div>""", unsafe_allow_html=True)
            else:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #34a853;"><div class="card-header">Sales Execution</div><div class="card-headline">{s_conversion*100:.1f}% Close Rate</div><div class="card-body">Your staff efficiently converted the walk-ins driven by this campaign.</div></div>""", unsafe_allow_html=True)

        st.markdown("<br>#### Retail & Sales Metrics", unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Store Walk-ins", f"{int(s_instore):,}", f"{s_capture*100:.2f}% Capture Rate")
        if hist_baseline: c2.metric("Incremental Walk-ins", f"{int(incremental_walkins):,}", f"{int(base_walkins)} Historic Baseline", delta_color="normal")
        else: c2.metric("Marketing Spend", f"₹{ad_value:,}", "Invested")
        c3.metric("POS Conversion", f"{s_conversion*100:.1f}%", f"{daily_transactions} Sales")
        c4.metric("Avg Order Value", f"₹{aov:,.0f}", f"₹{daily_revenue:,} Gross")

        # CONTEXTUAL CHARTS: Reverting to Area charts with Legends properly styled
        st.markdown("<br>", unsafe_allow_html=True)
        tab1, tab2, tab3 = st.tabs(["🚦 Walk-in Uplift Chart", "🎯 Retail Funnel", "⏱️ Behavior Matrix"])
        
        with tab1:
            # Clean Area chart showing all 3 zones for the Retailer
            fig = px.area(camp_df, x='Time', y=['InStore', 'Window', 'Street'], 
                          color_discrete_map={'Street': 'rgba(154,160,166,0.2)', 'Window': 'rgba(251,188,4,0.4)', 'InStore': G_BLUE},
                          labels={'value': 'Detected Devices', 'variable': 'Zone'})
            fig.data = fig.data[::-1] # Ensure InStore draws on top
            
            # Draw Historic Baseline if enabled
            if hist_baseline and not base_df.empty:
                baseline_avg = base_walkins / len(camp_df) if len(camp_df) > 0 else 0
                fig.add_hline(y=baseline_avg, line_dash="dot", annotation_text="Historic Baseline (Walk-ins)", line_color="#3c4043")
            
            st.plotly_chart(style_chart(fig), use_container_width=True, theme="streamlit")

        with tab2:
            fig_f = go.Figure(go.Funnel(
                y=["Street Visibility", "Window Browsers", "Store Walk-ins", "POS Transactions"],
                x=[s_street, s_window, s_instore, daily_transactions],
                textinfo="value+percent previous",
                marker={"color": [G_GREY, G_YELLOW, G_BLUE, G_GREEN]}
            ))
            st.plotly_chart(style_chart(fig_f), use_container_width=True, theme="streamlit")
            
        with tab3:
            cat_df = pd.DataFrame({
                'Category': ['Bounced (<30s)', 'Browsed (<10m)', 'Retained (>10m)'],
                'Count': [camp_df['Bounced'].max(), camp_df['Browsed'].max(), camp_df['Retained'].max()]
            })
            fig_bar = px.bar(cat_df, x='Count', y='Category', orientation='h', color='Category',
                             color_discrete_map={'Bounced (<30s)': G_RED, 'Browsed (<10m)': G_YELLOW, 'Retained (>10m)': G_BLUE})
            fig_bar.update_layout(showlegend=False)
            st.plotly_chart(style_chart(fig_bar), use_container_width=True, theme="streamlit")
