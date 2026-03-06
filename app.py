import streamlit as st
import boto3
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ==========================================
# 🎨 UI/UX & FLUID ANIMATIONS (MATERIAL 3)
# ==========================================
st.set_page_config(page_title="nsTags | Retail Intelligence", page_icon="📈", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    
    /* Fluid Fade-in Animation for Data Loads */
    @keyframes slideUpFade {
        from { opacity: 0; transform: translateY(15px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    .animate-container {
        animation: slideUpFade 0.6s cubic-bezier(0.16, 1, 0.3, 1) forwards;
    }
    
    /* Interactive Consultant Cards */
    .consultant-card {
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 134, 139, 0.2);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        height: 100%;
        box-shadow: 0 2px 4px rgba(0,0,0,0.02);
        transition: all 0.3s ease;
    }
    .consultant-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 8px 16px rgba(0,0,0,0.08);
    }
    
    .portfolio-card {
        background: linear-gradient(135deg, rgba(26, 115, 232, 0.05) 0%, rgba(52, 168, 83, 0.05) 100%);
        border: 1px solid rgba(26, 115, 232, 0.2);
        border-top: 4px solid #1a73e8;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 2rem;
    }
    
    .card-header { font-size: 0.85rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; color: var(--text-color); opacity: 0.6; margin-bottom: 0.8rem; }
    .card-headline { font-size: 1.5rem; font-weight: 600; color: var(--text-color); line-height: 1.2; margin-bottom: 0.8rem; }
    .card-body { font-size: 0.95rem; color: var(--text-color); opacity: 0.85; line-height: 1.5; }
    
    /* Brand Colors */
    .hl-blue { color: #1a73e8; font-weight: 600; }
    .hl-green { color: #0f9d58; font-weight: 600; }
    .hl-red { color: #db4437; font-weight: 600; }
    .hl-orange { color: #f4b400; font-weight: 600; }
    .hl-purple { color: #8e24aa; font-weight: 600; }
    
    /* Clean Metric overrides */
    div[data-testid="metric-container"] { border-left: 2px solid rgba(128, 134, 139, 0.2); padding-left: 1rem; }
    [data-testid="stMetricValue"] { font-size: 2rem; font-weight: 600; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem; text-transform: uppercase; opacity: 0.7; }
    
    /* Custom Radio Buttons to act as Sticky Tabs */
    div[role="radiogroup"] {
        background: var(--secondary-background-color);
        padding: 4px;
        border-radius: 8px;
        border: 1px solid rgba(128,134,139,0.2);
    }
    
    /* Clean brief text under charts */
    .chart-brief { font-size: 0.85rem; color: #80868b; text-align: center; margin-top: -10px; margin-bottom: 20px; font-style: italic; }
    </style>
""", unsafe_allow_html=True)

# Global Config for Mobile Plotly rendering
MOBILE_CONFIG = {'displayModeBar': False}

# ==========================================
# ☁️ AWS S3 NETWORK ENGINE & DATA MAPPING
# ==========================================
BUCKET_NAME = 'nstags-datalake-hq-2026'
REGION = 'ap-south-1'

@st.cache_data(ttl=45)
def load_s3_data():
    try:
        s3 = boto3.client('s3', region_name=REGION, aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"], aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"])
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='footfall/')
        if 'Contents' not in response: return pd.DataFrame()
        # Pull 150 files to ensure rich historical baseline data is available
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
                    
                    # Full 20-Data-Point Extraction Array
                    all_records.append({
                        'Store ID': data.get('S', 'Unknown'), 
                        'Time': start_time + timedelta(seconds=idx*5),
                        'Hour': (start_time + timedelta(seconds=idx*5)).strftime('%H:00'),
                        
                        # Spatial Mapping
                        'Street': snap[0], 'Window': snap[1], 'InStore': snap[2],
                        
                        # 10-Tier Behavioral Depth Mapping
                        'Passersby (<10s)': snap[3], 
                        'Window Shoppers (<30s)': snap[4],
                        'Explorers (<2m)': snap[5], 
                        'Focused (<5m)': snap[6],
                        'Engaged (<10m)': snap[7], 
                        'Potential (<20m)': snap[8],
                        'Committed (<30m)': snap[9], 
                        'Enthusiasts (<45m)': snap[10],
                        'Deep (<1h)': snap[11], 
                        'Loyal (>1h)': snap[12],
                        
                        # Brand Ecosystem Mapping
                        'Apple': snap[17], 'Samsung': snap[18], 'Other': snap[19]
                    })
            except Exception as e: 
                pass
                
    df = pd.DataFrame(all_records)
    if not df.empty: df = df.sort_values('Time').reset_index(drop=True)
    return df

# Global Plotly Style function to fix Mobile overlaps natively
def style_chart(fig):
    fig.update_layout(
        hovermode="x unified", 
        margin=dict(l=0, r=0, t=20, b=50), 
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#5f6368")
    )
    fig.update_xaxes(showgrid=False, zeroline=False, title_text="")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(128,134,139,0.1)", zeroline=False, title_text="")
    return fig

# ==========================================
# 🧠 APP INITIALIZATION & SIDEBAR
# ==========================================
st.markdown("<h2 style='margin-bottom: 0;'>nsTags Intelligence</h2>", unsafe_allow_html=True)
st.caption("Enterprise Storefront Analytics & Monetization Engine")

with st.spinner("Synchronizing with AWS Data Lake..."):
    df = load_s3_data()

if df.empty:
    st.warning("Awaiting telemetry. Ensure hardware nodes are online.")
else:
    with st.sidebar:
        st.markdown("### ⚙️ Engine Parameters")
        store_id = st.selectbox("Active Node", df['Store ID'].unique(), label_visibility="collapsed")
        
        st.markdown("### 📢 Campaign Strategy")
        ad_type = st.radio("Analytics Matrix:", ["Partner Brand Ad (Media)", "Own Store Promotion (Retail)"])
        
        st.markdown("### 📅 Time Horizon")
        time_preset = st.selectbox("Quick Select", ["Today", "Yesterday", "This Week", "Custom Period"])
        
        # Smart Time Logic
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
            start_date = c_d1.date_input("Start Date", latest_time.date() - timedelta(days=1))
            start_time = c_t1.time_input("Start Time", (latest_time - timedelta(hours=4)).time())
            c_d2, c_t2 = st.columns(2)
            end_date = c_d2.date_input("End Date", latest_time.date())
            end_time = c_t2.time_input("End Time", latest_time.time())
            camp_start = datetime.combine(start_date, start_time)
            camp_end = datetime.combine(end_date, end_time)

        st.markdown("### 💰 Financial Integration")
        if ad_type == "Partner Brand Ad (Media)":
            ad_value = st.number_input("Ad Revenue Received from Brand (₹)", min_value=0, value=15000, step=1000)
            daily_revenue = st.number_input("Store POS Revenue (₹)", min_value=0, value=45000, step=1000)
            daily_transactions = st.number_input("Store Transactions", min_value=0, value=35, step=1)
        else:
            ad_value = st.number_input("Marketing Spend (₹)", min_value=0, value=5000, step=500)
            hist_baseline = st.checkbox("Track Historic Incrementality", value=True)
            
            track_product = st.checkbox("Isolate Specific Product Sales?")
            if track_product:
                prod_revenue = st.number_input("Product Revenue (₹)", min_value=0, value=12000, step=500)
                prod_transactions = st.number_input("Product Transactions", min_value=0, value=8, step=1)
                
            daily_revenue = st.number_input("Total POS Revenue (₹)", min_value=0, value=45000, step=1000)
            daily_transactions = st.number_input("Total Store Transactions", min_value=0, value=35, step=1)

    # --- DATA WRANGLING ---
    net_df = df[(df['Time'] >= camp_start) & (df['Time'] <= camp_end)]
    store_df = df[df['Store ID'] == store_id]
    camp_df = store_df[(store_df['Time'] >= camp_start) & (store_df['Time'] <= camp_end)]
    
    s_street, s_window, s_instore = camp_df['Street'].sum(), camp_df['Window'].sum(), camp_df['InStore'].sum()
    s_capture = (s_instore / s_street) if s_street > 0 else 0
    s_conversion = (daily_transactions / s_instore) if s_instore > 0 else 0
    aov = (daily_revenue / daily_transactions) if daily_transactions > 0 else 0

    # Wrap main UI in CSS animation wrapper
    st.markdown("<div class='animate-container'>", unsafe_allow_html=True)

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
                <div class="card-body">This location led your entire network during this timeframe with a <span class='hl-purple'>{best_store['Capture_Rate']*100:.1f}%</span> walk-in capture rate. <b>Consultant Action:</b> Audit this location's current visual merchandising.</div>
            </div>
        """, unsafe_allow_html=True)

    # ==========================================
    # 🧠 TIER 1: FOUR-PILLAR INTELLIGENCE 
    # ==========================================
    st.markdown("#### Strategic Insights")
    e1, e2, e3, e4 = st.columns(4)

    # Pillar 1: Media/Lift
    with e1:
        if ad_type == "Partner Brand Ad (Media)":
            cpm = (ad_value / s_street * 1000) if s_street > 0 else 0
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #1a73e8;"><div class="card-header">Media Performance</div><div class="card-headline">₹{cpm:,.2f} CPM</div><div class="card-body">Delivered <span class='hl-blue'>{int(s_street):,}</span> impressions.</div></div>""", unsafe_allow_html=True)
        else:
            baseline_start = camp_start - timedelta(days=7) # Lookback exactly 1 week
            baseline_end = camp_end - timedelta(days=7)
            base_df = store_df[(store_df['Time'] >= baseline_start) & (store_df['Time'] <= baseline_end)]
            base_walkins = base_df['InStore'].sum() if hist_baseline and not base_df.empty else 0
            incremental = s_instore - base_walkins if hist_baseline else s_instore
            
            if incremental <= 0 and hist_baseline:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #ea4335;"><div class="card-header">Campaign Lift</div><div class="card-headline" style="color: #ea4335;">0% Lift</div><div class="card-body">The ad generated <span class='hl-red'>0 incremental</span> walk-ins.</div></div>""", unsafe_allow_html=True)
            else:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #34a853;"><div class="card-header">Campaign Lift</div><div class="card-headline">₹{(ad_value/incremental) if incremental>0 else 0:,.0f} True CAC</div><div class="card-body">Secured <span class='hl-green'>{int(incremental):,}</span> incremental walk-ins.</div></div>""", unsafe_allow_html=True)

    # Pillar 2: ROAS / Value
    with e2:
        if ad_type == "Partner Brand Ad (Media)":
            cpe = (ad_value / s_window) if s_window > 0 else 0 
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #fbbc04;"><div class="card-header">Engagement Value</div><div class="card-headline">₹{cpe:,.2f} Cost/Stop</div><div class="card-body">Brand paid this amount for every visual engagement.</div></div>""", unsafe_allow_html=True)
        else:
            roas = ((max(0, incremental) * aov) / ad_value) if ad_value > 0 else 0
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #fbbc04;"><div class="card-header">Incremental ROAS</div><div class="card-headline">{roas:.1f}x Return</div><div class="card-body">Return on ad spend based on historical uplift.</div></div>""", unsafe_allow_html=True)

    # Pillar 3: Floor Leakage (Universal)
    with e3:
        lost_opps = int(s_instore - daily_transactions)
        if s_conversion < 0.20 and s_instore > daily_transactions:
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #ea4335;"><div class="card-header">Floor Leakage</div><div class="card-headline">{s_conversion*100:.1f}% Close</div><div class="card-body"><span class='hl-red'>{lost_opps}</span> people left empty-handed. Deploy staff.</div></div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #34a853;"><div class="card-header">Floor Execution</div><div class="card-headline">{s_conversion*100:.1f}% Close</div><div class="card-body">Staff is converting highly effectively.</div></div>""", unsafe_allow_html=True)

    # Pillar 4: Brand/Ecosystem Insights
    with e4:
        apple, samsung, other = camp_df['Apple'].sum(), camp_df['Samsung'].sum(), camp_df['Other'].sum()
        total_phones = apple + samsung + other
        if total_phones > 0:
            if apple / total_phones > 0.50:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #8e24aa;"><div class="card-header">Audience Matrix</div><div class="card-headline">Premium Tilt</div><div class="card-body">Apple leads with <span class='hl-purple'>{(apple/total_phones)*100:.0f}%</span>. Tailor window to high-ticket items.</div></div>""", unsafe_allow_html=True)
            elif samsung / total_phones > 0.50:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #1a73e8;"><div class="card-header">Audience Matrix</div><div class="card-headline">Android Core</div><div class="card-body">Samsung dominates at <span class='hl-blue'>{(samsung/total_phones)*100:.0f}%</span>. Focus on tech-forward display.</div></div>""", unsafe_allow_html=True)
            else:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #9aa0a6;"><div class="card-header">Audience Matrix</div><div class="card-headline">Mixed Market</div><div class="card-body">Audience OS is highly fragmented. Maintain broad appeal.</div></div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class="consultant-card"><div class="card-header">Audience Matrix</div><div class="card-headline">Pending Data</div><div class="card-body">Awaiting demographic resolution.</div></div>""", unsafe_allow_html=True)

    # ==========================================
    # 📊 STATEFUL CHART ENGINE
    # ==========================================
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Session state ensures the selected chart doesn't reset when date changes
    if "active_chart" not in st.session_state:
        st.session_state.active_chart = "🚦 Traffic Timeline"
        
    chart_view = st.radio("Select Analytical View:", 
                          ["🚦 Traffic Timeline", "🎯 Universal Funnel", "📱 Brand Ecosystem", "⏱️ 10-Tier Behavior Matrix"], 
                          horizontal=True, key="active_chart")

    if chart_view == "🚦 Traffic Timeline":
        if ad_type == "Partner Brand Ad (Media)":
            fig = px.area(camp_df, x='Time', y=['Window', 'Street'], 
                          color_discrete_map={'Street': 'rgba(154,160,166,0.3)', 'Window': '#fbbc04'})
            fig.update_traces(fill='tozeroy')
            st.plotly_chart(style_chart(fig), use_container_width=True, theme="streamlit", config=MOBILE_CONFIG)
            st.markdown("<div class='chart-brief'>Tracks total addressable street audience against direct window engagements over time.</div>", unsafe_allow_html=True)
        else:
            fig = px.area(camp_df, x='Time', y=['InStore', 'Window', 'Street'], 
                          color_discrete_map={'Street': 'rgba(154,160,166,0.3)', 'Window': 'rgba(251,188,4,0.5)', 'InStore': '#1a73e8'})
            fig.data = fig.data[::-1] # Layer order fix
            if hist_baseline and not base_df.empty:
                baseline_avg = base_walkins / len(camp_df) if len(camp_df) > 0 else 0
                fig.add_hline(y=baseline_avg, line_dash="dot", annotation_text="Historic Walk-in Baseline", line_color="#3c4043")
            st.plotly_chart(style_chart(fig), use_container_width=True, theme="streamlit", config=MOBILE_CONFIG)
            st.markdown("<div class='chart-brief'>Visualizes the physical proximity of detected devices. Click items in the legend to toggle specific zones.</div>", unsafe_allow_html=True)

    elif chart_view == "🎯 Universal Funnel":
        fig_f = go.Figure(go.Funnel(
            y=["Street Visibility", "Window Browsers", "Store Walk-ins", "POS Transactions"],
            x=[s_street, s_window, s_instore, daily_transactions],
            textinfo="value+percent previous",
            marker={"color": ["#9aa0a6", "#fbbc04", "#1a73e8", "#0f9d58"]}
        ))
        st.plotly_chart(style_chart(fig_f), use_container_width=True, theme="streamlit", config=MOBILE_CONFIG)
        st.markdown("<div class='chart-brief'>Illustrates the complete drop-off rate from initial street exposure down to the final POS transaction.</div>", unsafe_allow_html=True)

    elif chart_view == "📱 Brand Ecosystem":
        brand_df = pd.DataFrame({'OS': ['Apple', 'Samsung', 'Other'], 'Count': [apple, samsung, other]})
        fig_brands = px.pie(brand_df, values='Count', names='OS', hole=0.6, color_discrete_map={'Apple':'#5F6368', 'Samsung':'#1a73e8', 'Other':'#9aa0a6'})
        fig_brands.update_traces(textinfo='percent+label', marker=dict(line=dict(width=0)))
        st.plotly_chart(style_chart(fig_brands), use_container_width=True, theme="streamlit", config=MOBILE_CONFIG)
        st.markdown("<div class='chart-brief'>Analyzes the smartphone operating system distribution of your audience based on broadcasted manufacturer data.</div>", unsafe_allow_html=True)

    elif chart_view == "⏱️ 10-Tier Behavior Matrix":
        categories = ['Passersby (<10s)', 'Window Shoppers (<30s)', 'Explorers (<2m)', 'Focused (<5m)', 
                      'Engaged (<10m)', 'Potential (<20m)', 'Committed (<30m)', 'Enthusiasts (<45m)', 
                      'Deep (<1h)', 'Loyal (>1h)']
        counts = [camp_df[cat].max() for cat in categories]
        
        cat_df = pd.DataFrame({'Behavioral Segment': categories, 'Device Count': counts})
        fig_bar = px.bar(cat_df, x='Behavioral Segment', y='Device Count', color='Device Count', color_continuous_scale="Blues")
        fig_bar.update_layout(coloraxis_showscale=False)
        st.plotly_chart(style_chart(fig_bar), use_container_width=True, theme="streamlit", config=MOBILE_CONFIG)
        st.markdown("<div class='chart-brief'>Categorizes visitors by the exact continuous duration they spent within your store's Bluetooth radius.</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True) # Close animation div
