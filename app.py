import time
from io import StringIO
from urllib.parse import urlparse

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ==========================================
# PAGE CONFIG
# ==========================================
st.set_page_config(
    page_title="nsTags Retail Intelligence",
    page_icon="📈",
    layout="wide",
)

# ==========================================
# STYLING
# ==========================================
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    .main-title {
        font-size: 2rem;
        font-weight: 700;
        margin-bottom: 0.25rem;
    }

    .sub-title {
        font-size: 0.95rem;
        color: #6b7280;
        margin-bottom: 1rem;
    }

    .section-title {
        font-size: 1.15rem;
        font-weight: 700;
        margin-top: 0.75rem;
        margin-bottom: 0.75rem;
    }

    div[data-testid="metric-container"] {
        border: 1px solid rgba(128,128,128,0.18);
        border-radius: 14px;
        padding: 14px 16px;
        background: linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01));
    }

    [data-testid="stMetricLabel"] {
        font-size: 0.82rem;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }

    [data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
    }

    .info-box {
        border: 1px solid rgba(59,130,246,0.2);
        border-left: 4px solid #3b82f6;
        border-radius: 12px;
        padding: 12px 14px;
        background: rgba(59,130,246,0.06);
        margin-bottom: 12px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ==========================================
# CONFIG
# ==========================================
AWS_REGION = st.secrets.get("AWS_REGION", "ap-south-1")
ATHENA_DATABASE = st.secrets.get("ATHENA_DATABASE", "nstags_analytics")
ATHENA_WORKGROUP = st.secrets.get("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT = st.secrets.get(
    "ATHENA_OUTPUT", "s3://nstags-datalake-hq-2026/athena-results/"
)

AWS_ACCESS_KEY_ID = st.secrets.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = st.secrets.get("AWS_SECRET_ACCESS_KEY")

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    st.error("Missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in Streamlit secrets.")
    st.stop()

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

athena_client = session.client("athena")
s3_client = session.client("s3")

# ==========================================
# HELPERS
# ==========================================
def s3_uri_to_bucket_key(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip("/")


def format_seconds(seconds_value: float | int | None) -> str:
    if seconds_value is None or pd.isna(seconds_value):
        return "-"
    seconds_value = int(round(float(seconds_value)))
    mins, secs = divmod(seconds_value, 60)
    hrs, mins = divmod(mins, 60)

    if hrs > 0:
        return f"{hrs}h {mins}m"
    if mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def style_fig(fig: go.Figure, title: str | None = None) -> go.Figure:
    fig.update_layout(
        title=title,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=55, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        font=dict(family="Inter"),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(148,163,184,0.15)", zeroline=False)
    return fig


def run_athena_query(query: str, database: str = ATHENA_DATABASE) -> pd.DataFrame:
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP,
    )

    execution_id = response["QueryExecutionId"]

    while True:
        execution = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = execution["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Unknown Athena error")
        raise RuntimeError(f"Athena query failed: {reason}")

    output_location = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    bucket, key = s3_uri_to_bucket_key(output_location)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    csv_bytes = obj["Body"].read()
    df = pd.read_csv(StringIO(csv_bytes.decode("utf-8")))
    return df


@st.cache_data(ttl=300)
def load_store_list() -> pd.DataFrame:
    query = """
    SELECT DISTINCT store_id
    FROM nstags_dashboard_metrics
    ORDER BY store_id
    """
    return run_athena_query(query)


@st.cache_data(ttl=300)
def load_available_dates(store_id: str) -> pd.DataFrame:
    query = f"""
    SELECT DISTINCT
        year,
        month,
        day
    FROM nstags_dashboard_metrics
    WHERE store_id = '{store_id}'
    ORDER BY year DESC, month DESC, day DESC
    """
    return run_athena_query(query)


@st.cache_data(ttl=300)
def load_dashboard_metrics(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    query = f"""
    SELECT *
    FROM nstags_dashboard_metrics
    WHERE store_id = '{store_id}'
      AND year = '{year}'
      AND month = '{month}'
      AND day = '{day}'
    """
    return run_athena_query(query)


@st.cache_data(ttl=300)
def load_hourly_traffic(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    query = f"""
    SELECT *
    FROM nstags_hourly_traffic
    WHERE store_id = '{store_id}'
      AND year = '{year}'
      AND month = '{month}'
      AND day = '{day}'
    ORDER BY hour_of_day
    """
    return run_athena_query(query)


@st.cache_data(ttl=300)
def load_conversion_hourly(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    query = f"""
    SELECT *
    FROM nstags_conversion_hourly
    WHERE store_id = '{store_id}'
      AND year = '{year}'
      AND month = '{month}'
      AND day = '{day}'
    ORDER BY hour_of_day
    """
    return run_athena_query(query)


@st.cache_data(ttl=300)
def load_dwell_buckets(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    query = f"""
    SELECT *
    FROM nstags_dwell_buckets
    WHERE store_id = '{store_id}'
      AND year = '{year}'
      AND month = '{month}'
      AND day = '{day}'
    ORDER BY dwell_bucket
    """
    return run_athena_query(query)


@st.cache_data(ttl=300)
def load_brand_mix_hourly(store_id: str, year: str, month: str, day: str) -> pd.DataFrame:
    query = f"""
    SELECT
        store_id,
        year,
        month,
        day,
        hour(from_unixtime(ts)) AS hour_of_day,
        round(avg(apple_devices), 2) AS avg_apple_devices,
        round(avg(samsung_devices), 2) AS avg_samsung_devices,
        round(avg(other_devices), 2) AS avg_other_devices
    FROM nstags_live_analytics
    WHERE store_id = '{store_id}'
      AND year = '{year}'
      AND month = '{month}'
      AND day = '{day}'
    GROUP BY store_id, year, month, day, hour(from_unixtime(ts))
    ORDER BY hour_of_day
    """
    return run_athena_query(query)


# ==========================================
# SIDEBAR
# ==========================================
st.markdown('<div class="main-title">nsTags Retail Intelligence</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-title">BLE-powered retail traffic, session, dwell, and conversion analytics</div>',
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Controls")

    mode = st.radio("Dashboard Mode", ["Basic", "Advanced"], index=0)

    stores_df = load_store_list()
    if stores_df.empty:
        st.error("No stores found in Athena view: nstags_dashboard_metrics")
        st.stop()

    store_options = stores_df["store_id"].dropna().tolist()
    selected_store = st.selectbox("Store ID", store_options)

    dates_df = load_available_dates(selected_store)
    if dates_df.empty:
        st.error("No dates found for selected store.")
        st.stop()

    dates_df["date_str"] = (
        dates_df["year"].astype(str)
        + "-"
        + dates_df["month"].astype(str).str.zfill(2)
        + "-"
        + dates_df["day"].astype(str).str.zfill(2)
    )

    selected_date = st.selectbox("Date", dates_df["date_str"].tolist())

    selected_year, selected_month, selected_day = selected_date.split("-")

    st.markdown("---")
    st.caption(f"Database: `{ATHENA_DATABASE}`")
    st.caption(f"Workgroup: `{ATHENA_WORKGROUP}`")

# ==========================================
# LOAD DATA
# ==========================================
try:
    dashboard_df = load_dashboard_metrics(selected_store, selected_year, selected_month, selected_day)
    hourly_df = load_hourly_traffic(selected_store, selected_year, selected_month, selected_day)
    conversion_df = load_conversion_hourly(selected_store, selected_year, selected_month, selected_day)
    dwell_df = load_dwell_buckets(selected_store, selected_year, selected_month, selected_day)
    brand_df = load_brand_mix_hourly(selected_store, selected_year, selected_month, selected_day)
except Exception as e:
    st.error(f"Failed to load Athena data: {e}")
    st.stop()

if dashboard_df.empty:
    st.warning("No dashboard metrics found for selected filters.")
    st.stop()

dashboard_row = dashboard_df.iloc[0].to_dict()

# Prepare helper labels
if not hourly_df.empty and "hour_of_day" in hourly_df.columns:
    hourly_df["hour_label"] = hourly_df["hour_of_day"].apply(lambda x: f"{int(x):02d}:00")
if not conversion_df.empty and "hour_of_day" in conversion_df.columns:
    conversion_df["hour_label"] = conversion_df["hour_of_day"].apply(lambda x: f"{int(x):02d}:00")
if not brand_df.empty and "hour_of_day" in brand_df.columns:
    brand_df["hour_label"] = brand_df["hour_of_day"].apply(lambda x: f"{int(x):02d}:00")

# ==========================================
# BASIC VIEW
# ==========================================
def render_basic_view() -> None:
    st.markdown('<div class="section-title">Executive Summary</div>', unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    c4, c5, c6 = st.columns(3)

    c1.metric("Walk-By Traffic", f"{dashboard_row.get('walk_by_traffic', 0):.2f}")
    c2.metric("Store Interest", f"{dashboard_row.get('store_interest', 0):.2f}")
    c3.metric("Store Visits", f"{int(dashboard_row.get('store_visits', 0)):,}")

    c4.metric("Qualified Footfall", f"{int(dashboard_row.get('qualified_footfall', 0)):,}")
    c5.metric("Engaged Visits", f"{int(dashboard_row.get('engaged_visits', 0)):,}")
    c6.metric("Avg Dwell", format_seconds(dashboard_row.get("avg_dwell_seconds")))

    st.markdown('<div class="section-title">Retail Funnel</div>', unsafe_allow_html=True)
    funnel_df = pd.DataFrame(
        {
            "stage": [
                "Walk-By",
                "Interest",
                "Visits",
                "Qualified",
                "Engaged",
            ],
            "value": [
                float(dashboard_row.get("walk_by_traffic", 0)),
                float(dashboard_row.get("store_interest", 0)),
                float(dashboard_row.get("store_visits", 0)),
                float(dashboard_row.get("qualified_footfall", 0)),
                float(dashboard_row.get("engaged_visits", 0)),
            ],
        }
    )

    fig_funnel = go.Figure(
        go.Funnel(
            y=funnel_df["stage"],
            x=funnel_df["value"],
            textinfo="value",
        )
    )
    st.plotly_chart(style_fig(fig_funnel, "Retail Funnel"), width="stretch")

    left, right = st.columns([1.3, 1])

    with left:
        st.markdown('<div class="section-title">Hourly Traffic Trend</div>', unsafe_allow_html=True)
        if not hourly_df.empty:
            traffic_long = hourly_df.melt(
                id_vars=["hour_label"],
                value_vars=["avg_far_devices", "avg_mid_devices", "avg_near_devices"],
                var_name="metric",
                value_name="value",
            )
            traffic_long["metric"] = traffic_long["metric"].map(
                {
                    "avg_far_devices": "Walk-By",
                    "avg_mid_devices": "Interest",
                    "avg_near_devices": "Near Store",
                }
            )
            fig_traffic = px.line(
                traffic_long,
                x="hour_label",
                y="value",
                color="metric",
                markers=True,
            )
            st.plotly_chart(style_fig(fig_traffic, "Hourly Traffic"), width="stretch")
        else:
            st.info("No hourly traffic data found.")

    with right:
        st.markdown('<div class="section-title">Dwell Distribution</div>', unsafe_allow_html=True)
        if not dwell_df.empty:
            dwell_order = ["00-10s", "10-30s", "30-60s", "01-03m", "03-05m", "05m+"]
            dwell_plot_df = dwell_df.copy()
            dwell_plot_df["dwell_bucket"] = pd.Categorical(
                dwell_plot_df["dwell_bucket"], categories=dwell_order, ordered=True
            )
            dwell_plot_df = dwell_plot_df.sort_values("dwell_bucket")
        
            fig_dwell = px.bar(
                dwell_plot_df,
                x="dwell_bucket",
                y="visits",
                text="visits",
            )
            st.plotly_chart(style_fig(fig_dwell, "Dwell Distribution"), width="stretch")
        else:
            st.info("No dwell bucket data found.")

    st.markdown('<div class="section-title">Hourly Conversion</div>', unsafe_allow_html=True)
    if not conversion_df.empty:
        conv_long = conversion_df.melt(
            id_vars=["hour_label"],
            value_vars=["store_visits", "qualified_visits", "engaged_visits"],
            var_name="metric",
            value_name="value",
        )
        conv_long["metric"] = conv_long["metric"].map(
            {
                "store_visits": "Visits",
                "qualified_visits": "Qualified",
                "engaged_visits": "Engaged",
            }
        )
        fig_conv = px.bar(
            conv_long,
            x="hour_label",
            y="value",
            color="metric",
            barmode="group",
        )
        st.plotly_chart(style_fig(fig_conv, "Hourly Visits / Qualified / Engaged"), width="stretch")
    else:
        st.info("No conversion data found.")


# ==========================================
# ADVANCED VIEW
# ==========================================
def render_advanced_view() -> None:
    st.markdown('<div class="section-title">Advanced Retail Analytics</div>', unsafe_allow_html=True)

    info_cols = st.columns(4)
    info_cols[0].metric("Store Visits", f"{int(dashboard_row.get('store_visits', 0)):,}")
    info_cols[1].metric("Qualified Rate", f"{float(dashboard_row.get('qualified_visit_rate', 0)):.2f}")
    info_cols[2].metric("Engaged Rate", f"{float(dashboard_row.get('engaged_visit_rate', 0)):.2f}")
    info_cols[3].metric("Median Dwell", format_seconds(dashboard_row.get("median_dwell_seconds")))

    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        st.markdown('<div class="section-title">Hourly Traffic Diagnostics</div>', unsafe_allow_html=True)
        if not hourly_df.empty:
            fig_hourly = go.Figure()
            fig_hourly.add_trace(go.Scatter(
                x=hourly_df["hour_label"],
                y=hourly_df["avg_far_devices"],
                mode="lines+markers",
                name="Walk-By"
            ))
            fig_hourly.add_trace(go.Scatter(
                x=hourly_df["hour_label"],
                y=hourly_df["avg_mid_devices"],
                mode="lines+markers",
                name="Interest"
            ))
            fig_hourly.add_trace(go.Scatter(
                x=hourly_df["hour_label"],
                y=hourly_df["avg_near_devices"],
                mode="lines+markers",
                name="Near Store"
            ))
            st.plotly_chart(style_fig(fig_hourly, "Traffic by Hour"), width="stretch")
        else:
            st.info("No hourly traffic data found.")

    with row1_col2:
        st.markdown('<div class="section-title">Brand Mix by Hour</div>', unsafe_allow_html=True)
        if not brand_df.empty:
            brand_long = brand_df.melt(
                id_vars=["hour_label"],
                value_vars=["avg_apple_devices", "avg_samsung_devices", "avg_other_devices"],
                var_name="metric",
                value_name="value",
            )
            brand_long["metric"] = brand_long["metric"].map(
                {
                    "avg_apple_devices": "Apple",
                    "avg_samsung_devices": "Samsung",
                    "avg_other_devices": "Other",
                }
            )
            fig_brand = px.bar(
                brand_long,
                x="hour_label",
                y="value",
                color="metric",
                barmode="stack",
            )
            st.plotly_chart(style_fig(fig_brand, "Hourly Brand Mix"), width="stretch")
        else:
            st.info("No brand data found.")

    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        st.markdown('<div class="section-title">Hourly Conversion Ratios</div>', unsafe_allow_html=True)
        if not conversion_df.empty:
            ratio_long = conversion_df.melt(
                id_vars=["hour_label"],
                value_vars=["qualified_share_of_visits", "engaged_share_of_visits"],
                var_name="metric",
                value_name="value",
            )
            ratio_long["metric"] = ratio_long["metric"].map(
                {
                    "qualified_share_of_visits": "Qualified Share",
                    "engaged_share_of_visits": "Engaged Share",
                }
            )
            fig_ratio = px.line(
                ratio_long,
                x="hour_label",
                y="value",
                color="metric",
                markers=True,
            )
            st.plotly_chart(style_fig(fig_ratio, "Hourly Conversion Rates"), width="stretch")
        else:
            st.info("No hourly conversion data found.")

    with row2_col2:
        st.markdown('<div class="section-title">Visits per Scan / Traffic Indices</div>', unsafe_allow_html=True)
        if not conversion_df.empty:
            index_long = conversion_df.melt(
                id_vars=["hour_label"],
                value_vars=[
                    "visits_per_scan",
                    "qualified_visits_per_scan",
                    "interest_to_visit_index",
                    "walkby_to_visit_index",
                ],
                var_name="metric",
                value_name="value",
            )
            index_long["metric"] = index_long["metric"].map(
                {
                    "visits_per_scan": "Visits / Scan",
                    "qualified_visits_per_scan": "Qualified / Scan",
                    "interest_to_visit_index": "Interest → Visit Index",
                    "walkby_to_visit_index": "Walk-By → Visit Index",
                }
            )
            fig_index = px.bar(
                index_long,
                x="hour_label",
                y="value",
                color="metric",
                barmode="group",
            )
            st.plotly_chart(style_fig(fig_index, "Hourly Indices"), width="stretch")
        else:
            st.info("No hourly index data found.")

    st.markdown('<div class="section-title">Dwell Distribution</div>', unsafe_allow_html=True)
    if not dwell_df.empty:
        dwell_order = ["00-10s", "10-30s", "30-60s", "01-03m", "03-05m", "05m+"]
        dwell_plot_df = dwell_df.copy()
        dwell_plot_df["dwell_bucket"] = pd.Categorical(
            dwell_plot_df["dwell_bucket"], categories=dwell_order, ordered=True
        )
        dwell_plot_df = dwell_plot_df.sort_values("dwell_bucket")
    
        fig_dwell = px.bar(
            dwell_plot_df,
            x="dwell_bucket",
            y="visits",
            text="visits",
        )
        st.plotly_chart(style_fig(fig_dwell, "Dwell Bucket Distribution"), width="stretch")
    else:
        st.info("No dwell bucket data found.")

    st.markdown('<div class="section-title">Raw Analytics Tables</div>', unsafe_allow_html=True)
    with st.expander("Show hourly traffic table"):
        st.dataframe(hourly_df, width="stretch")
    with st.expander("Show conversion table"):
        st.dataframe(conversion_df, width="stretch")
    with st.expander("Show dwell bucket table"):
        st.dataframe(dwell_df, width="stretch")


# ==========================================
# RENDER
# ==========================================
st.markdown(
    f"""
    <div class="info-box">
        <strong>Store:</strong> {selected_store} &nbsp;&nbsp;|&nbsp;&nbsp;
        <strong>Date:</strong> {selected_date} &nbsp;&nbsp;|&nbsp;&nbsp;
        <strong>Mode:</strong> {mode}
    </div>
    """,
    unsafe_allow_html=True,
)

if mode == "Basic":
    render_basic_view()
else:
    render_advanced_view()
