import streamlit as st
import boto3
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from google import genai

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
    
    .animate-container { animation: slideUpFade 0.6s cubic-bezier(0.16, 1, 0.3, 1) forwards; }
    
    /* 🤖 AI Synthesis Box */
    .ai-synthesis-box {
        background: linear-gradient(145deg, #f8f9fa 0%, #ffffff 100%);
        border: 1px solid rgba(26, 115, 232, 0.3);
        border-left: 5px solid #1a73e8;
        border-radius: 12px;
        padding: 1.5rem 2rem;
        margin-bottom: 2rem;
        box-shadow: 0 8px 24px rgba(26, 115, 232, 0.08);
    }
    .ai-synthesis-box::before {
        content: "✨ AI EXECUTIVE BRIEF";
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 1px;
        color: #1a73e8;
        text-transform: uppercase;
        display: block;
        margin-bottom: 0.5rem;
    }
    .ai-text { font-size: 1.05rem; color: #202124; line-height: 1.6; font-weight: 500; }
    
    .consultant-card {
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 134, 139, 0.2);
        border-radius: 12px;
        padding: 1.5rem;
        display: flex; flex-direction: column; justify-content: space-between;
        height: 100%; min-height: 280px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.02); transition: all 0.3s ease;
    }
    .consultant-card:hover { transform: translateY(-4px); box-shadow: 0 8px 16px rgba(0,0,0,0.08); }
    
    .portfolio-card { background: linear-gradient(135deg, rgba(26, 115, 232, 0.05) 0%, rgba(52, 168, 83, 0.05) 100%); border: 1px solid rgba(26, 115, 232, 0.2); border-top: 4px solid #1a73e8; border-radius: 12px; padding: 1.5rem; margin-bottom: 2rem; }
    
    .card-header { font-size: 0.85rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; color: var(--text-color); opacity: 0.6; margin-bottom: 0.8rem; }
    .card-headline { font-size: 1.5rem; font-weight: 600; color: var(--text-color); line-height: 1.2; margin-bottom: 0.8rem; }
    .card-body { font-size: 0.95rem; color: var(--text-color); opacity: 0.85; line-height: 1.5; flex-grow: 1; }
    
    .math-box { background: rgba(128, 134, 139, 0.08); border-radius: 6px; padding: 0.75rem; margin-top: 1rem; font-family: 'Courier New', monospace; font-size: 0.8rem; border-left: 3px solid #9aa0a6; }
    
    .hl-blue { color: #1a73e8; font-weight: 600; }
    .hl-green { color: #0f9d58; font-weight: 600; }
    .hl-red { color: #db4437; font-weight: 600; }
    .hl-orange { color: #f4b400; font-weight: 600; }
    .hl-purple { color: #8e24aa; font-weight: 600; }
    
    div[data-testid="metric-container"] { border-left: 2px solid rgba(128, 134, 139, 0.2); padding-left: 1rem; }
    [data-testid="stMetricValue"] { font-size: 2rem; font-weight: 600; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem; text-transform: uppercase; opacity: 0.7; }
    div[role="radiogroup"] { background: var(--secondary-background-color); padding: 4px; border-radius: 8px; border: 1px solid rgba(128,134,139,0.2); }
    .chart-brief { font-size: 0.85rem; color: #80868b; text-align: center; margin-top: -10px; margin-bottom: 20px; font-style: italic; }
    </style>
""", unsafe_allow_html=True)

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
                        'Passersby (<10s)': snap[3], 'Window Shoppers (<30s)': snap[4],
                        'Explorers (<2m)': snap[5], 'Focused (<5m)': snap[6],
                        'Engaged (<10m)': snap[7], 'Potential (<20m)': snap[8],
                        'Committed (<30m)': snap[9], 'Enthusiasts (<45m)': snap[10],
                        'Deep (<1h)': snap[11], 'Loyal (>1h)': snap[12],
                        'Bounced': snap[3] + snap[4], 'Browsed': snap[5] + snap[6] + snap[7], 'Retained': sum(snap[8:13]),
                        'Apple': snap[17], 'Samsung': snap[18], 'Other': snap[19]
                    })
            except Exception as e: 
                pass
                
    df = pd.DataFrame(all_records)
    if not df.empty: df = df.sort_values('Time').reset_index(drop=True)
    return df

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
# 🤖 NEW AI GENERATION ENGINE
# ==========================================
@st.cache_data(ttl=300) 
def generate_ai_brief(metrics_dict):
    try:
        api_key = st.secrets.get("GEMINI_API_KEY")
        if not api_key:
            return "AI Insights locked. Please configure 'GEMINI_API_KEY' in Streamlit secrets."
        
        # Future-proofed SDK client implementation
        client = genai.Client(api_key=api_key)
        
        prompt = f"""
        Act as an expert retail strategy consultant. Analyze the following live storefront metrics for a {metrics_dict['campaign_type']} over a {metrics_dict['duration']} hour window.
        
        DATA:
        - Street Impressions: {metrics_dict['street']}
        - Window Engagements: {metrics_dict['window']}
        - Walk-ins: {metrics_dict['walkins']}
        - Conversion Rate: {metrics_dict['conversion_rate']}%
        - ROI/ROAS Indicator: {metrics_dict['roi']}
        - Dominant Phone OS: {metrics_dict['dominant_os']}
        
        Provide a highly professional, 3-sentence executive brief.
        Sentence 1: Summarize the overarching performance and traffic flow.
        Sentence 2: Identify the primary bottleneck or greatest success (e.g., floor leakage, high window stopping power).
        Sentence 3: Provide one specific, actionable recommendation to improve commercial yield based strictly on this data.
        
        Do not use bolding or markdown. Keep it concise and authoritative.
        """
        
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
        )
        return response.text
    except Exception as e:
        return f"AI Analysis momentarily unavailable. Please check system logs."

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

        duration_hours = max((camp_end - camp_start).total_seconds() / 3600, 0.01)

        st.markdown("### 💰 Financials")
        if ad_type == "Partner Brand Ad (Media)":
            ad_value = st.number_input("Ad Revenue Received from Brand (₹)", min_value=0, value=15000, step=1000)
            daily_revenue = st.number_input("Store POS Revenue (₹)", min_value=0, value=45000, step=1000)
            daily_transactions = st.number_input("Store Transactions", min_value=0, value=35, step=1)
            track_product = False
            hist_baseline = False
        else:
            ad_value = st.number_input("Marketing Spend (₹)", min_value=0, value=5000, step=500)
            hist_baseline = st.checkbox("Track Historic Incrementality", value=True)
            track_product = st.checkbox("Isolate Specific Product Sales?")
            if track_product:
                prod_revenue = st.number_input("Product Revenue (₹)", min_value=0, value=12000, step=500)
                prod_transactions = st.number_input("Product Transactions", min_value=0, value=8, step=1)
            daily_revenue = st.number_input("Total POS Revenue (₹)", min_value=0, value=45000, step=1000)
            daily_transactions = st.number_input("Total Store Transactions", min_value=0, value=35, step=1)

        ai_enabled = st.checkbox("✨ Enable AI Executive Synthesis", value=True)

    # --- DATA WRANGLING ---
    net_df = df[(df['Time'] >= camp_start) & (df['Time'] <= camp_end)]
    store_df = df[df['Store ID'] == store_id]
    camp_df = store_df[(store_df['Time'] >= camp_start) & (store_df['Time'] <= camp_end)]
    
    s_street, s_window, s_instore = camp_df['Street'].sum(), camp_df['Window'].sum(), camp_df['InStore'].sum()
    s_capture = (s_instore / s_street) if s_street > 0 else 0
    s_conversion = (daily_transactions / s_instore) if s_instore > 0 else 0
    aov = (daily_revenue / daily_transactions) if daily_transactions > 0 else 0
    
    baseline_start = camp_start - timedelta(days=7)
    baseline_end = camp_end - timedelta(days=7)
    base_df = store_df[(store_df['Time'] >= baseline_start) & (store_df['Time'] <= baseline_end)]
    base_walkins = base_df['InStore'].sum() if hist_baseline and not base_df.empty else 0
    incremental = s_instore - base_walkins if hist_baseline else s_instore
    
    apple, samsung, other = camp_df['Apple'].sum(), camp_df['Samsung'].sum(), camp_df['Other'].sum()
    top_os = "Apple iOS" if apple > samsung else "Android/Samsung" if samsung > apple else "Mixed"

    st.markdown("<div class='animate-container'>", unsafe_allow_html=True)

    # ==========================================
    # 🤖 TIER 0: AI EXECUTIVE BRIEF
    # ==========================================
    if ai_enabled:
        roi_metric = f"₹{(ad_value/s_street*1000) if s_street>0 else 0:.2f} CPM" if ad_type == "Partner Brand Ad (Media)" else f"{((max(0, incremental) * aov) / ad_value) if ad_value > 0 else 0:.1f}x ROAS"
        
        metrics_payload = {
            'campaign_type': ad_type,
            'duration': round(duration_hours, 1),
            'street': int(s_street),
            'window': int(s_window),
            'walkins': int(s_instore),
            'conversion_rate': round(s_conversion * 100, 1),
            'roi': roi_metric,
            'dominant_os': top_os
        }
        
        with st.spinner("Generating AI Analysis..."):
            ai_text = generate_ai_brief(metrics_payload)
            
        st.markdown(f"""
            <div class="ai-synthesis-box">
                <div class="ai-text">{ai_text}</div>
            </div>
        """, unsafe_allow_html=True)

    # ==========================================
    # 🧠 TIER 1: FOUR-PILLAR INTELLIGENCE 
    # ==========================================
    st.markdown("#### Strategic Executive Insights")
    e1, e2, e3, e4 = st.columns(4)

    with e1:
        if ad_type == "Partner Brand Ad (Media)":
            cpm = (ad_value / s_street * 1000) if s_street > 0 else 0
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #1a73e8;"><div class="card-header">Media Performance</div><div class="card-headline">₹{cpm:,.2f} CPM</div><div class="card-body">Your storefront generated <span class='hl-blue'>{int(s_street):,}</span> impressions over {duration_hours:.1f} hours.<div class='math-box'>Math: (₹{ad_value:,} / {int(s_street)}) × 1000</div></div></div>""", unsafe_allow_html=True)
        else:
            if incremental <= 0 and hist_baseline:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #ea4335;"><div class="card-header">Campaign Lift</div><div class="card-headline" style="color: #ea4335;">0% Lift</div><div class="card-body">The ad generated <span class='hl-red'>0 incremental</span> walk-ins vs baseline.<div class='math-box'>Math: {int(s_instore)} Active - {int(base_walkins)} Base</div></div></div>""", unsafe_allow_html=True)
            else:
                cac = (ad_value / incremental) if incremental > 0 else 0
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #34a853;"><div class="card-header">Acquisition Cost</div><div class="card-headline">₹{cac:,.0f} CAC</div><div class="card-body">Secured <span class='hl-green'>{int(incremental):,}</span> net-new walk-ins during this period.<div class='math-box'>Math: ₹{ad_value:,} / {int(incremental)} New Walk-ins</div></div></div>""", unsafe_allow_html=True)

    with e2:
        if ad_type == "Partner Brand Ad (Media)":
            cpe = (ad_value / s_window) if s_window > 0 else 0 
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #fbbc04;"><div class="card-header">Engagement Value</div><div class="card-headline">₹{cpe:,.2f} Cost/Stop</div><div class="card-body">The partner effectively paid this amount for every direct visual engagement.<div class='math-box'>Math: ₹{ad_value:,} / {int(s_window)} Window Stops</div></div></div>""", unsafe_allow_html=True)
        else:
            rev_pool = prod_revenue if track_product else (max(0, incremental) * aov)
            roas = (rev_pool / ad_value) if ad_value > 0 else 0
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #fbbc04;"><div class="card-header">Campaign ROAS</div><div class="card-headline">{roas:.1f}x Return</div><div class="card-body">Return on Ad Spend based directly on incremental footfall.<div class='math-box'>Math: ₹{rev_pool:,.0f} Attrib Rev / ₹{ad_value:,}</div></div></div>""", unsafe_allow_html=True)

    with e3:
        lost_opps = int(s_instore - daily_transactions)
        if s_conversion < 0.20 and s_instore > daily_transactions:
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #ea4335;"><div class="card-header">Floor Leakage</div><div class="card-headline">{s_conversion*100:.1f}% Close</div><div class="card-body"><span class='hl-red'>{lost_opps}</span> people left empty-handed. Your bottleneck is floor execution.<div class='math-box'>Math: {daily_transactions} Sales / {int(s_instore)} Walk-ins</div></div></div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #34a853;"><div class="card-header">Floor Execution</div><div class="card-headline">{s_conversion*100:.1f}% Close</div><div class="card-body">Staff is converting highly effectively. Push more traffic.<div class='math-box'>Math: {daily_transactions} Sales / {int(s_instore)} Walk-ins</div></div></div>""", unsafe_allow_html=True)

    with e4:
        total_phones = apple + samsung + other
        if total_phones > 0:
            if top_os == "Apple iOS":
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #8e24aa;"><div class="card-header">Audience Matrix</div><div class="card-headline">Premium Tilt</div><div class="card-body">Apple leads with <span class='hl-purple'>{(apple/total_phones)*100:.0f}%</span>. Tailor displays to high-ticket demographics.<div class='math-box'>Vol: {apple} iOS / {total_phones} Total</div></div></div>""", unsafe_allow_html=True)
            elif top_os == "Android/Samsung":
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #1a73e8;"><div class="card-header">Audience Matrix</div><div class="card-headline">Android Core</div><div class="card-body">Samsung dominates at <span class='hl-blue'>{(samsung/total_phones)*100:.0f}%</span>. Focus on tech-forward display.<div class='math-box'>Vol: {samsung} AOS / {total_phones} Total</div></div></div>""", unsafe_allow_html=True)
            else:
                st.markdown(f"""<div class="consultant-card" style="border-top: 4px solid #9aa0a6;"><div class="card-header">Audience Matrix</div><div class="card-headline">Mixed Market</div><div class="card-body">Audience OS is highly fragmented. Maintain broad appeal.<div class='math-box'>Top Segment: {max(apple, samsung, other)}</div></div></div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class="consultant-card"><div class="card-header">Audience Matrix</div><div class="card-headline">Pending Data</div><div class="card-body">Awaiting demographic resolution.</div></div>""", unsafe_allow_html=True)

    # ==========================================
    # 📈 MACRO METRICS
    # ==========================================
    st.markdown(f"#### Period Diagnostics ({duration_hours:.1f} Hours)", unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    if ad_type == "Partner Brand Ad (Media)":
        c1.metric("Street Impressions", f"{int(s_street):,}", f"{int(s_street/duration_hours):,} / hr velocity")
        c2.metric("Window Engagements", f"{int(s_window):,}", f"{(s_window/s_street*100) if s_street>0 else 0:.1f}% Stop Rate")
        c3.metric("Effective CPM", f"₹{cpm:,.2f}", "Industry Benchmark: ₹200", delta_color="off")
        c4.metric("Ad Revenue", f"₹{ad_value:,}", "Generated")
    else:
        c1.metric("Store Walk-ins", f"{int(s_instore):,}", f"{int(s_instore/duration_hours):,} / hr velocity")
        if hist_baseline: c2.metric("Incremental Walk-ins", f"{int(incremental):,}", f"vs {int(base_walkins)} Historic", delta_color="normal")
        else: c2.metric("Marketing Spend", f"₹{ad_value:,}", "Invested")
        c3.metric("POS Conversion", f"{s_conversion*100:.1f}%", f"{daily_transactions} Sales")
        c4.metric("Avg Order Value", f"₹{aov:,.0f}", f"₹{daily_revenue:,} Gross")

    # ==========================================
    # 📊 STATEFUL CHART ENGINE
    # ==========================================
    st.markdown("<br>", unsafe_allow_html=True)
    
    if "active_chart" not in st.session_state:
        st.session_state.active_chart = "🚦 Traffic Timeline"
        
    chart_view = st.radio("Select Analytical View:", 
                          ["🚦 Traffic Timeline", "🎯 Universal Funnel", "📱 Brand Ecosystem", "⏱️ 10-Tier Behavior Matrix"], 
                          horizontal=True, key="active_chart")

    # FIX: Replaced use_container_width=True with width="stretch" to clear all Streamlit deprecation warnings
    if chart_view == "🚦 Traffic Timeline":
        if ad_type == "Partner Brand Ad (Media)":
            fig = px.area(camp_df, x='Time', y=['Window', 'Street'], color_discrete_map={'Street': 'rgba(154,160,166,0.3)', 'Window': '#fbbc04'})
            fig.update_traces(fill='tozeroy')
            st.plotly_chart(style_chart(fig), width="stretch", theme="streamlit", config=MOBILE_CONFIG)
            st.markdown(f"<div class='chart-brief'>Tracks the flow of {int(s_street)} total street impressions over the {duration_hours:.1f}-hour campaign window.</div>", unsafe_allow_html=True)
        else:
            fig = px.area(camp_df, x='Time', y=['InStore', 'Window', 'Street'], color_discrete_map={'Street': 'rgba(154,160,166,0.3)', 'Window': 'rgba(251,188,4,0.5)', 'InStore': '#1a73e8'})
            fig.data = fig.data[::-1] 
            if hist_baseline and not base_df.empty:
                baseline_avg = base_walkins / len(camp_df) if len(camp_df) > 0 else 0
                fig.add_hline(y=baseline_avg, line_dash="dot", annotation_text="Historic Walk-in Baseline", line_color="#3c4043")
            st.plotly_chart(style_chart(fig), width="stretch", theme="streamlit", config=MOBILE_CONFIG)
            st.markdown(f"<div class='chart-brief'>Visualizes traffic proximity over {duration_hours:.1f} hours. Click items in the legend to isolate specific zones.</div>", unsafe_allow_html=True)

    elif chart_view == "🎯 Universal Funnel":
        fig_f = go.Figure(go.Funnel(
            y=["Street Visibility", "Window Browsers", "Store Walk-ins", "POS Transactions"],
            x=[s_street, s_window, s_instore, daily_transactions],
            textinfo="value+percent previous",
            marker={"color": ["#9aa0a6", "#fbbc04", "#1a73e8", "#0f9d58"]}
        ))
        st.plotly_chart(style_chart(fig_f), width="stretch", theme="streamlit", config=MOBILE_CONFIG)
        st.markdown("<div class='chart-brief'>Illustrates the complete drop-off rate from initial street exposure down to the final POS transaction.</div>", unsafe_allow_html=True)

    elif chart_view == "📱 Brand Ecosystem":
        brand_df = pd.DataFrame({'OS': ['Apple', 'Samsung', 'Other'], 'Count': [apple, samsung, other]})
        fig_brands = px.pie(brand_df, values='Count', names='OS', hole=0.6, color_discrete_map={'Apple':'#5F6368', 'Samsung':'#1a73e8', 'Other':'#9aa0a6'})
        fig_brands.update_traces(textinfo='percent+label', marker=dict(line=dict(width=0)))
        st.plotly_chart(style_chart(fig_brands), width="stretch", theme="streamlit", config=MOBILE_CONFIG)
        st.markdown("<div class='chart-brief'>Analyzes the smartphone operating system distribution of your audience based on broadcasted manufacturer data.</div>", unsafe_allow_html=True)

    elif chart_view == "⏱️ 10-Tier Behavior Matrix":
        categories = ['Passersby (<10s)', 'Window Shoppers (<30s)', 'Explorers (<2m)', 'Focused (<5m)', 
                      'Engaged (<10m)', 'Potential (<20m)', 'Committed (<30m)', 'Enthusiasts (<45m)', 
                      'Deep (<1h)', 'Loyal (>1h)']
        counts = [camp_df[cat].max() if cat in camp_df.columns else 0 for cat in categories]
        
        cat_df = pd.DataFrame({'Behavioral Segment': categories, 'Device Count': counts})
        fig_bar = px.bar(cat_df, x='Behavioral Segment', y='Device Count', color='Device Count', color_continuous_scale="Blues")
        fig_bar.update_layout(coloraxis_showscale=False)
        st.plotly_chart(style_chart(fig_bar), width="stretch", theme="streamlit", config=MOBILE_CONFIG)
        st.markdown("<div class='chart-brief'>Categorizes visitors by the exact continuous duration they spent within your store's Bluetooth radius.</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)
