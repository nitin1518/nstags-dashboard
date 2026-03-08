import re
import time
from io import StringIO
from urllib.parse import urlparse
from datetime import timedelta, date

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from botocore.exceptions import ClientError
from google import genai

# ==========================================
# PAGE CONFIG
# ==========================================
st.set_page_config(
    page_title="nsTags | Retail Intelligence",
    page_icon="📈",
    layout="wide",
)

# ==========================================
# PREMIUM UI — EXECUTIVE GRADE
# ==========================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');
:root {
    --bg: #F8FAFC;
    --bg-soft: #EEF2FF;
    --panel: #FFFFFF;
    --panel-2: #F8FAFC;
    --border: rgba(99,102,241,0.16);
    --border-strong: rgba(99,102,241,0.28);
    --text: #0F172A;
    --text-2: #334155;
    --text-3: #475569;
    --text-muted: #64748B;
    --accent: #6366F1;
    --accent-2: #8B5CF6;
    --good: #10B981;
    --warn: #F59E0B;
    --bad: #F43F5E;
    --shadow: 0 12px 36px rgba(15,23,42,0.08);
    --shadow-soft: 0 6px 18px rgba(15,23,42,0.06);
}
@media (prefers-color-scheme: dark) {
    :root {
        --bg: #06080F;
        --bg-soft: #0A0F1E;
        --panel: #0D1117;
        --panel-2: #111827;
        --border: rgba(99,102,241,0.14);
        --border-strong: rgba(99,102,241,0.30);
        --text: #F8FAFC;
        --text-2: #CBD5E1;
        --text-3: #94A3B8;
        --text-muted: #94A3B8;
        --accent: #818CF8;
        --accent-2: #A78BFA;
        --good: #34D399;
        --warn: #FBBF24;
        --bad: #FB7185;
        --shadow: 0 14px 40px rgba(0,0,0,0.35);
        --shadow-soft: 0 8px 24px rgba(0,0,0,0.25);
    }
}
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    background: var(--bg) !important;
    color: var(--text);
    -webkit-font-smoothing: antialiased;
}
.main .block-container {
    padding: 1.5rem 2rem 3rem 2rem !important;
    max-width: 100% !important;
    background: var(--bg) !important;
}
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, var(--panel) 0%, var(--bg-soft) 100%) !important;
    border-right: 1px solid var(--border) !important;
}
section[data-testid="stSidebar"] > div:first-child {
    padding-top: 1.5rem !important;
}
.sidebar-logo {
    display: flex;
    align-items: center;
    gap: 0.65rem;
    padding: 0 0 1.2rem 0;
    border-bottom: 1px solid var(--border);
    margin-bottom: 1.2rem;
}
.sidebar-logo-mark {
    width: 32px; height: 32px;
    background: linear-gradient(135deg, var(--accent) 0%, var(--accent-2) 100%);
    border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1rem;
    flex-shrink: 0;
    box-shadow: 0 4px 12px rgba(99,102,241,0.28);
}
.sidebar-logo-text {
    font-size: 0.95rem;
    font-weight: 800;
    color: var(--text);
    letter-spacing: -0.01em;
}
.sidebar-logo-sub {
    font-size: 0.68rem;
    color: var(--text-muted);
    font-weight: 500;
}
section[data-testid="stSidebar"] .stMarkdown h3 {
    font-size: 0.68rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.12em !important;
    color: var(--accent) !important;
    padding: 0 0 0.5rem 0 !important;
    border-bottom: 1px solid var(--border) !important;
    margin-bottom: 0.8rem !important;
}
section[data-testid="stSidebar"] label {
    color: var(--text-3) !important;
    font-size: 0.82rem !important;
    font-weight: 600 !important;
}
section[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div,
section[data-testid="stSidebar"] .stMultiSelect [data-baseweb="select"] > div {
    background: var(--panel) !important;
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
    color: var(--text) !important;
}
section[data-testid="stSidebar"] .stNumberInput input {
    background: var(--panel) !important;
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
    color: var(--text) !important;
}
section[data-testid="stSidebar"] .stRadio [data-testid="stMarkdownContainer"] p,
section[data-testid="stSidebar"] .stCheckbox label {
    color: var(--text-3) !important;
    font-size: 0.83rem !important;
}
@keyframes fadeSlideUp {
    from { opacity: 0; transform: translateY(16px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes pulse-ring {
    0%   { transform: scale(0.92); opacity: 0.9; }
    50%  { transform: scale(1.04); opacity: 0.5; }
    100% { transform: scale(0.92); opacity: 0.9; }
}
.anim-1 { animation: fadeSlideUp 0.5s cubic-bezier(0.22,1,0.36,1) 0.05s both; }
.anim-2 { animation: fadeSlideUp 0.5s cubic-bezier(0.22,1,0.36,1) 0.12s both; }
.anim-3 { animation: fadeSlideUp 0.5s cubic-bezier(0.22,1,0.36,1) 0.19s both; }
.anim-4 { animation: fadeSlideUp 0.5s cubic-bezier(0.22,1,0.36,1) 0.26s both; }
.anim-5 { animation: fadeSlideUp 0.5s cubic-bezier(0.22,1,0.36,1) 0.33s both; }
.hero-shell {
    background: linear-gradient(135deg,
        rgba(99,102,241,0.12) 0%,
        rgba(16,185,129,0.06) 55%,
        rgba(99,102,241,0.02) 100%
    );
    border: 1px solid var(--border-strong);
    border-radius: 20px;
    padding: 1.4rem 1.8rem 1.3rem 1.8rem;
    margin-bottom: 1.4rem;
    position: relative;
    overflow: hidden;
    animation: fadeSlideUp 0.6s cubic-bezier(0.22,1,0.36,1) both;
    box-shadow: var(--shadow-soft);
}
.hero-shell::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 1px;
    background: linear-gradient(90deg,
        transparent 0%,
        rgba(99,102,241,0.55) 40%,
        rgba(16,185,129,0.35) 70%,
        transparent 100%
    );
}
.hero-shell::after {
    content: '';
    position: absolute;
    top: -60px; right: -60px;
    width: 200px; height: 200px;
    background: radial-gradient(circle, rgba(99,102,241,0.08) 0%, transparent 70%);
    border-radius: 50%;
    pointer-events: none;
}
.hero-eyebrow {
    font-size: 0.7rem;
    font-weight: 800;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: var(--accent);
    margin-bottom: 0.35rem;
    display: flex;
    align-items: center;
    gap: 0.4rem;
}
.hero-eyebrow-dot {
    width: 6px; height: 6px;
    background: var(--accent);
    border-radius: 50%;
    animation: pulse-ring 2s ease-in-out infinite;
    box-shadow: 0 0 0 4px rgba(99,102,241,0.18);
}
.hero-title {
    font-size: 2rem;
    font-weight: 800;
    line-height: 1.1;
    letter-spacing: -0.03em;
    background: linear-gradient(135deg, var(--text) 0%, var(--accent) 50%, var(--good) 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 0.3rem;
}
.hero-sub {
    font-size: 0.9rem;
    color: var(--text-3);
    font-weight: 500;
    letter-spacing: 0.01em;
}
.hero-badges {
    display: flex;
    gap: 0.5rem;
    margin-top: 0.65rem;
    flex-wrap: wrap;
}
.hero-badge {
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.06em;
    padding: 0.22rem 0.6rem;
    border-radius: 100px;
    border: 1px solid;
}
.badge-indigo {
    background: rgba(99,102,241,0.10);
    border-color: rgba(99,102,241,0.24);
    color: var(--accent);
}
.badge-emerald {
    background: rgba(16,185,129,0.08);
    border-color: rgba(16,185,129,0.22);
    color: var(--good);
}
.badge-amber {
    background: rgba(245,158,11,0.08);
    border-color: rgba(245,158,11,0.22);
    color: var(--warn);
}
.section-title {
    font-size: 0.72rem;
    font-weight: 800;
    letter-spacing: 0.13em;
    text-transform: uppercase;
    color: var(--accent);
    margin: 1.6rem 0 0.9rem 0;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
.section-title::after {
    content: '';
    flex: 1;
    height: 1px;
    background: linear-gradient(90deg, rgba(99,102,241,0.22) 0%, transparent 100%);
    border-radius: 1px;
}
.kpi-card {
    background: radial-gradient(circle at top right, rgba(99,102,241,0.07), transparent 30%), linear-gradient(145deg, var(--panel) 0%, var(--panel-2) 100%);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 1.12rem 1.15rem 1rem 1.15rem;
    height: 100%;
    min-height: 150px;
    position: relative;
    overflow: hidden;
    transition: transform 0.22s ease, border-color 0.22s ease, box-shadow 0.22s ease;
    cursor: default;
    box-shadow: var(--shadow-soft);
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 1px;
    background: linear-gradient(90deg, transparent, rgba(99,102,241,0.38), transparent);
    opacity: 0;
    transition: opacity 0.2s ease;
}
.kpi-card::after {
    content: '';
    position: absolute;
    top: -44px; right: -44px;
    width: 120px; height: 120px;
    background: radial-gradient(circle, rgba(99,102,241,0.06) 0%, transparent 72%);
    border-radius: 50%;
    pointer-events: none;
}
.kpi-card:hover {
    transform: translateY(-3px);
    border-color: var(--border-strong);
    box-shadow: var(--shadow);
}
.kpi-card:hover::before { opacity: 1; }
.kpi-label {
    font-size: 0.7rem;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--text-muted);
    margin-bottom: 0.4rem;
}
.kpi-value {
    font-size: 2.2rem;
    font-weight: 800;
    color: var(--text);
    line-height: 1.05;
    letter-spacing: -0.03em;
    font-variant-numeric: tabular-nums;
}
.kpi-value-sm {
    font-size: 1.6rem;
    font-weight: 800;
    color: var(--text);
    line-height: 1.05;
    letter-spacing: -0.02em;
    font-variant-numeric: tabular-nums;
}
.kpi-sub {
    font-size: 0.8rem;
    color: var(--text-3);
    margin-top: 0.35rem;
    line-height: 1.45;
}
.kpi-guidance {
    margin-top: 0.6rem;
    padding-top: 0.6rem;
    border-top: 1px dashed rgba(99,102,241,0.14);
    display: flex;
    gap: 0.5rem;
    align-items: flex-start;
}
.kpi-guidance-icon {
    font-size: 0.9rem;
    line-height: 1;
    margin-top: 0.05rem;
    flex-shrink: 0;
    opacity: 0.95;
}
.kpi-guidance-text {
    font-size: 0.76rem;
    line-height: 1.48;
    color: var(--text-muted);
}
.kpi-guidance-text b {
    color: var(--text-2);
    font-weight: 700;
}
.kpi-accent-bar {
    position: absolute;
    bottom: 0; left: 0;
    width: 100%; height: 2px;
    background: linear-gradient(90deg, var(--accent), var(--accent-2));
    border-radius: 0 0 16px 16px;
    opacity: 0.55;
}
.verdict-good, .verdict-warn, .verdict-bad {
    font-weight: 800;
    padding: 0.1rem 0.45rem;
    border-radius: 6px;
    font-size: 0.76rem;
    display: inline-block;
}
.verdict-good {
    color: var(--good);
    background: rgba(16,185,129,0.10);
}
.verdict-warn {
    color: var(--warn);
    background: rgba(245,158,11,0.10);
}
.verdict-bad {
    color: var(--bad);
    background: rgba(244,63,94,0.10);
}
.insight-card {
    background: radial-gradient(circle at top right, rgba(99,102,241,0.07), transparent 32%), linear-gradient(145deg, var(--panel) 0%, var(--panel-2) 100%);
    border: 1px solid var(--border);
    border-left: 3px solid var(--accent);
    border-radius: 18px;
    padding: 1.15rem 1.2rem 1.1rem 1.2rem;
    min-height: 215px;
    transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease;
    position: relative;
    overflow: hidden;
    box-shadow: var(--shadow-soft);
    margin-bottom: 0.95rem;
}
.insight-card::after {
    content: '';
    position: absolute;
    top: -40px; right: -40px;
    width: 120px; height: 120px;
    background: radial-gradient(circle, rgba(99,102,241,0.05) 0%, transparent 70%);
    border-radius: 50%;
    pointer-events: none;
}
.insight-card:hover {
    transform: translateY(-2px);
    box-shadow: var(--shadow);
    border-color: var(--border-strong);
}
.insight-title {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: var(--accent);
    font-weight: 800;
    margin-bottom: 0.5rem;
}
.insight-headline {
    font-size: 1.25rem;
    font-weight: 800;
    color: var(--text);
    line-height: 1.2;
    margin-bottom: 0.65rem;
    letter-spacing: -0.02em;
}
.insight-body {
    font-size: 0.9rem;
    color: var(--text-3);
    line-height: 1.62;
}
.insight-body b {
    color: var(--text-2);
    font-weight: 700;
}
.insight-meta {
    margin-top: 0.8rem;
    padding: 0.7rem 0.8rem;
    border-radius: 10px;
    background: rgba(99,102,241,0.05);
    border: 1px solid rgba(99,102,241,0.10);
    display: flex;
    align-items: flex-start;
    gap: 0.55rem;
}
.insight-meta-icon {
    font-size: 0.95rem;
    line-height: 1;
    margin-top: 0.08rem;
    flex-shrink: 0;
}
.insight-meta-text {
    font-size: 0.78rem;
    color: var(--text-3);
    line-height: 1.5;
}
.insight-meta-text b {
    color: var(--text-2);
}
.small-note {
    font-size: 0.78rem;
    color: var(--text-3);
    margin-top: -0.35rem;
    margin-bottom: 0.9rem;
    line-height: 1.5;
}
.benchmark-card {
    background: radial-gradient(circle at top right, rgba(99,102,241,0.07), transparent 32%), linear-gradient(145deg, var(--panel) 0%, var(--panel-2) 100%);
    border: 1px solid rgba(100,116,139,0.18);
    border-left: 3px solid #64748B;
    border-radius: 16px;
    padding: 0.95rem 1.1rem;
    margin-top: 0.8rem;
    margin-bottom: 1rem;
    display: flex;
    align-items: flex-start;
    gap: 0.9rem;
    box-shadow: var(--shadow-soft);
    position: relative;
    overflow: hidden;
}
.benchmark-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 1px;
    background: linear-gradient(90deg, transparent, rgba(99,102,241,0.26), transparent);
}
.benchmark-icon {
    font-size: 1.3rem;
    margin-top: 0.1rem;
    flex-shrink: 0;
}
.benchmark-body { flex: 1; }
.benchmark-title {
    font-size: 0.67rem;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--text-muted);
    margin-bottom: 0.3rem;
}
.benchmark-note {
    font-size: 0.84rem;
    color: var(--text-3);
    line-height: 1.55;
}
.index-source-note {
    font-size: 0.76rem;
    color: var(--text-3);
    background: rgba(99,102,241,0.05);
    border: 1px solid rgba(99,102,241,0.12);
    border-radius: 10px;
    padding: 0.65rem 0.85rem;
    margin-top: 0.2rem;
    margin-bottom: 0.4rem;
    line-height: 1.55;
}
.metric-explainer {
    background: rgba(99,102,241,0.05);
    border: 1px solid rgba(99,102,241,0.12);
    border-radius: 12px;
    padding: 0.85rem 0.95rem;
    margin-bottom: 0.85rem;
    display: flex;
    gap: 0.6rem;
    align-items: flex-start;
}
.metric-explainer-icon {
    font-size: 1rem;
    line-height: 1;
    margin-top: 0.1rem;
    flex-shrink: 0;
}
.metric-explainer-text {
    font-size: 0.8rem;
    color: var(--text-3);
    line-height: 1.55;
}
.metric-explainer-text b {
    color: var(--text-2);
}
.chart-shell {
    background: radial-gradient(circle at top right, rgba(99,102,241,0.05), transparent 28%), linear-gradient(145deg, var(--panel) 0%, var(--panel-2) 100%);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 0.8rem 0.85rem 0.25rem 0.85rem;
    box-shadow: var(--shadow-soft);
    margin-bottom: 1rem;
    position: relative;
    overflow: hidden;
}
.chart-shell::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(99,102,241,0.24), transparent);
}
div[data-testid="stInfo"] {
    background: radial-gradient(circle at top right, rgba(99,102,241,0.07), transparent 30%), linear-gradient(145deg, var(--panel) 0%, var(--panel-2) 100%) !important;
    border: 1px solid var(--border-strong) !important;
    border-left: 4px solid var(--accent) !important;
    border-radius: 16px !important;
    padding: 1.1rem 1.4rem !important;
    box-shadow: var(--shadow-soft) !important;
    color: var(--text-2) !important;
}
div[data-testid="stInfo"] p,
div[data-testid="stInfo"] li {
    color: var(--text-2) !important;
}
div[data-testid="stInfo"] strong {
    color: var(--text) !important;
}
.stTabs [data-baseweb="tab-list"] {
    background: rgba(99,102,241,0.04) !important;
    border: 1px solid var(--border) !important;
    border-radius: 12px !important;
    padding: 4px !important;
    gap: 2px !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    border-radius: 9px !important;
    color: var(--text-muted) !important;
    font-size: 0.83rem !important;
    font-weight: 700 !important;
    padding: 0.45rem 1rem !important;
    transition: all 0.18s ease !important;
    border: none !important;
}
.stTabs [data-baseweb="tab"]:hover {
    background: rgba(99,102,241,0.08) !important;
    color: var(--accent) !important;
}
.stTabs [aria-selected="true"] {
    background: rgba(99,102,241,0.14) !important;
    color: var(--accent) !important;
    box-shadow: 0 0 0 1px rgba(99,102,241,0.22) !important;
}
.stTabs [data-baseweb="tab-panel"] {
    background: transparent !important;
    padding-top: 1.2rem !important;
}
.stPlotlyChart {
    background: transparent !important;
    border-radius: 12px;
    overflow: hidden;
}
.stSpinner > div > div {
    border-color: var(--accent) transparent transparent transparent !important;
}
div[data-testid="stAlert"] {
    border-radius: 12px !important;
    font-size: 0.85rem !important;
}
div[data-testid="stHorizontalBlock"] {
    gap: 0.8rem;
}
.page-wrapper {
    animation: fadeSlideUp 0.4s ease both;
}
@media (max-width: 900px) {
    div[data-testid="stHorizontalBlock"] {
        gap: 1rem !important;
    }
    .kpi-card,
    .insight-card,
    .benchmark-card {
        margin-bottom: 0.95rem !important;
    }
    .hero-title {
        font-size: 1.6rem;
    }
    .main .block-container {
        padding: 1rem 1rem 2rem 1rem !important;
    }
}
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: rgba(99,102,241,0.25); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: rgba(99,102,241,0.45); }
</style>
""", unsafe_allow_html=True)

PLOT_CONFIG = {"displayModeBar": False}

CHART_COLORS = {
    "indigo":  "#6366F1",
    "violet":  "#8B5CF6",
    "emerald": "#10B981",
    "amber":   "#F59E0B",
    "rose":    "#F43F5E",
    "slate":   "#64748B",
    "sky":     "#38BDF8",
    "teal":    "#14B8A6",
}

# ==========================================
# CONFIG
# ==========================================
AWS_REGION       = st.secrets.get("AWS_REGION", "ap-south-1")
ATHENA_DATABASE  = st.secrets.get("ATHENA_DATABASE", "nstags_analytics")
ATHENA_WORKGROUP = st.secrets.get("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT    = st.secrets.get("ATHENA_OUTPUT", "s3://nstags-datalake-hq-2026/athena-results/")
AWS_ACCESS_KEY_ID     = st.secrets.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = st.secrets.get("AWS_SECRET_ACCESS_KEY")
GEMINI_API_KEY        = st.secrets.get("GEMINI_API_KEY")

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    st.error("Missing AWS credentials in Streamlit secrets.")
    st.stop()

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
athena_client = session.client("athena")
s3_client     = session.client("s3")

# ==========================================
# HELPERS
# ==========================================
def benchmark_maturity_label(population):
    try:
        population = int(population)
    except Exception:
        return "Unknown", "verdict-bad", "Benchmark population unavailable."
    
    if population >= 100:
        return "Stable", "verdict-good", f"Benchmark built on {population:,} store-day records."
    elif population >= 30:
        return "Growing", "verdict-warn", f"Benchmark built on {population:,} store-day records. Directionally useful, still maturing."
    else:
        return "Early", "verdict-bad", f"Benchmark built on only {population:,} store-day records. Scores are provisional."

def safe_div(a, b):
    return a / b if b not in [0, None] else 0

def fmt_int(x):
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "0"

def fmt_currency(x):
    try:
        return f"₹{float(x):,.0f}"
    except Exception:
        return "₹0"

def fmt_seconds(x):
    try:
        total = int(round(float(x)))
    except Exception:
        return "-"
    mins, secs = divmod(total, 60)
    hrs, mins = divmod(mins, 60)
    if hrs > 0:
        return f"{hrs}h {mins}m"
    if mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"

def verdict_class(value, good_threshold, warn_threshold, higher_is_better=True):
    if higher_is_better:
        if value >= good_threshold:
            return "verdict-good", "Healthy"
        elif value >= warn_threshold:
            return "verdict-warn", "Watch"
        return "verdict-bad", "Weak"
    else:
        if value <= good_threshold:
            return "verdict-good", "Efficient"
        elif value <= warn_threshold:
            return "verdict-warn", "Monitor"
        return "verdict-bad", "Expensive"

def validate_store_id(store_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", str(store_id)):
        raise ValueError("Invalid store_id")
    return store_id

def s3_uri_to_bucket_key(s3_uri: str):
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip("/")

def athena_date_expr():
    return (
        "CAST(date_parse("
        "CAST(year AS varchar) || '-' || lpad(CAST(month AS varchar), 2, '0') || '-' || lpad(CAST(day AS varchar), 2, '0'), "
        "'%Y-%m-%d') AS date)"
    )

SQL_DATE_EXPR = athena_date_expr()

def sql_date_range_filter(start_date_str: str, end_date_str: str) -> str:
    return f"{SQL_DATE_EXPR} BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'"

def style_chart(fig):
    fig.update_layout(
        hovermode="x unified",
        margin=dict(l=10, r=10, t=35, b=55),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.18,
            xanchor="center",
            x=0.5,
            font=dict(size=11, color="#94A3B8"),
            bgcolor="rgba(0,0,0,0)",
        ),
        font=dict(family="Inter, sans-serif", color="#CBD5E1", size=11),
        hoverlabel=dict(
            bgcolor="#111827",
            bordercolor="rgba(99,102,241,0.3)",
            font_color="#F8FAFC",
            font_size=12,
        ),
    )
    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        title_text="",
        color="#94A3B8",
        tickfont=dict(size=10, color="#94A3B8"),
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(99,102,241,0.10)",
        zeroline=False,
        title_text="",
        color="#94A3B8",
        tickfont=dict(size=10, color="#94A3B8"),
    )
    return fig

def prepare_dwell_plot_df(source_df: pd.DataFrame) -> pd.DataFrame:
    dwell_order = ["00-10s", "10-30s", "30-60s", "01-03m", "03-05m", "05m+"]
    plot_df = source_df.copy()
    if "dwell_bucket" not in plot_df.columns or plot_df.empty:
        return plot_df
    plot_df["dwell_bucket"] = pd.Categorical(
        plot_df["dwell_bucket"], categories=dwell_order, ordered=True
    )
    return plot_df.sort_values("dwell_bucket")

def to_100_scale(value, cap=1.0):
    try:
        value = float(value)
    except Exception:
        return 0
    return max(0, min((value / cap) * 100, 100))

def detect_primary_bottleneck(score_row, transactions, store_visits):
    floor_conversion_strength = safe_div(transactions, store_visits)
    scores = {
        "Store Magnet": float(score_row.get("store_magnet_score", 0) or 0),
        "Window Capture": min(float(score_row.get("window_capture_index", 0) or 0) / 8.0, 1.0),
        "Entry Efficiency": float(score_row.get("entry_efficiency_score", 0) or 0),
        "Dwell Quality": float(score_row.get("dwell_quality_index", 0) or 0),
        "Floor Conversion": floor_conversion_strength,
    }
    primary = min(scores, key=scores.get)
    return primary, scores

def clamp_0_100(x):
    try:
        x = float(x)
    except Exception:
        return 0.0
    return max(0.0, min(100.0, x))

def normalize_ratio_to_100(value, cap):
    try:
        value, cap = float(value), float(cap)
    except Exception:
        return 0.0
    if cap <= 0:
        return 0.0
    return clamp_0_100((value / cap) * 100)

def normalize_index_to_100(value, cap):
    try:
        value, cap = float(value), float(cap)
    except Exception:
        return 0.0
    if cap <= 0:
        return 0.0
    return clamp_0_100((value / cap) * 100)

def weighted_score(parts):
    total_weight = sum(w for _, w in parts if w > 0)
    if total_weight <= 0:
        return 0.0
    return round(sum(s * w for s, w in parts if w > 0) / total_weight, 1)

def score_band(score: float):
    score = float(score)
    if score >= 75:
        return "verdict-good", "Strong"
    elif score >= 50:
        return "verdict-warn", "Moderate"
    return "verdict-bad", "Weak"

def compute_local_fallback_indices(
    walk_by_traffic, store_interest, near_store,
    store_visits, qualified_visits, engaged_visits,
    avg_dwell_seconds, score_row, brand_df=None,
):
    walk_by_score = normalize_index_to_100(walk_by_traffic, cap=20.0)
    interest_score = normalize_index_to_100(store_interest, cap=12.0)
    near_store_score = normalize_index_to_100(near_store, cap=8.0)
    
    qualified_rate = safe_div(qualified_visits, store_visits)
    engaged_rate = safe_div(engaged_visits, store_visits)
    qualified_score = normalize_ratio_to_100(qualified_rate, cap=0.60)
    engaged_score = normalize_ratio_to_100(engaged_rate, cap=0.40)
    dwell_score = normalize_ratio_to_100(avg_dwell_seconds, cap=180.0)
    
    store_magnet_ratio = safe_div(store_interest, walk_by_traffic)
    window_capture_ratio = safe_div(store_visits, store_interest)
    entry_efficiency_ratio = safe_div(qualified_visits, store_visits)
    
    store_magnet_score = normalize_ratio_to_100(store_magnet_ratio, cap=0.60)
    window_capture_score = normalize_ratio_to_100(window_capture_ratio, cap=8.0)
    entry_efficiency_score = normalize_ratio_to_100(entry_efficiency_ratio, cap=0.70)
    
    if score_row:
        dwell_quality_score = normalize_ratio_to_100(float(score_row.get("dwell_quality_index", 0) or 0), cap=1.0)
    else:
        dwell_quality_score = weighted_score([(engaged_score, 0.6), (dwell_score, 0.4)])
        
    premium_device_mix_score = 50.0
    if brand_df is not None and not brand_df.empty:
        apple = float(brand_df["avg_apple_devices"].sum())
        samsung = float(brand_df["avg_samsung_devices"].sum())
        other = float(brand_df["avg_other_devices"].sum())
        total = apple + samsung + other
        premium_share = (safe_div(apple, total) * 1.0) + (safe_div(samsung, total) * 0.7)
        premium_device_mix_score = normalize_ratio_to_100(premium_share, cap=0.75)
        
    volume_confidence_score = normalize_ratio_to_100(store_visits, cap=500.0)
    
    visit_quality_index = weighted_score([
        (qualified_score, 0.45),
        (engaged_score, 0.35),
        (dwell_score, 0.20),
    ])
    
    store_attraction_index = weighted_score([
        (store_magnet_score, 0.40),
        (window_capture_score, 0.35),
        (entry_efficiency_score, 0.25),
    ])
    
    audience_quality_index = weighted_score([
        (premium_device_mix_score, 0.60),
        (engaged_score, 0.40),
    ])
    
    traffic_intelligence_index = weighted_score([
        (walk_by_score, 0.18),
        (interest_score, 0.18),
        (near_store_score, 0.12),
        (visit_quality_index, 0.22),
        (dwell_quality_score, 0.18),
        (volume_confidence_score, 0.12),
    ])
    
    return {
        "traffic_intelligence_index": round(traffic_intelligence_index, 1),
        "visit_quality_index": round(visit_quality_index, 1),
        "store_attraction_index": round(store_attraction_index, 1),
        "audience_quality_index": round(audience_quality_index, 1),
        "walk_by_score": round(walk_by_score, 1),
        "interest_score": round(interest_score, 1),
        "near_store_score": round(near_store_score, 1),
        "qualified_score": round(qualified_score, 1),
        "engaged_score": round(engaged_score, 1),
        "dwell_score": round(dwell_score, 1),
        "store_magnet_percentile_score": round(store_magnet_score, 1),
        "window_capture_score": round(window_capture_score, 1),
        "entry_efficiency_percentile_score": round(entry_efficiency_score, 1),
        "dwell_quality_score": round(dwell_quality_score, 1),
        "premium_device_mix_score": round(premium_device_mix_score, 1),
        "volume_confidence_score": round(volume_confidence_score, 1),
        "benchmark_population": 0,
        "is_fallback": True,
    }

def index_color(score):
    if score >= 75:
        return CHART_COLORS["emerald"]
    if score >= 50:
        return CHART_COLORS["amber"]
    return CHART_COLORS["rose"]

def render_metric_explainer(icon, title, body):
    st.markdown(
        f"""
        <div class="metric-explainer">
        <div class="metric-explainer-icon">{icon}</div>
        <div class="metric-explainer-text">
        <b>{title}</b><br>
        {body}
        </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def open_chart_shell():
    st.markdown('<div class="chart-shell">', unsafe_allow_html=True)

def close_chart_shell():
    st.markdown('</div>', unsafe_allow_html=True)

def guidance_html(icon, title, text):
    return (
        '<div class="kpi-guidance">'
        f'<div class="kpi-guidance-icon">{icon}</div>'
        f'<div class="kpi-guidance-text"><b>{title}</b> {text}</div>'
        '</div>'
    )

def meta_html(icon, title, text):
    return (
        '<div class="insight-meta">'
        f'<div class="insight-meta-icon">{icon}</div>'
        f'<div class="insight-meta-text"><b>{title}</b> {text}</div>'
        '</div>'
    )

def infer_trend_grain(start_date: date, end_date: date) -> str:
    span_days = (end_date - start_date).days + 1
    if span_days <= 14:
        return "day"
    if span_days <= 120:
        return "week"
    return "month"

def scope_title(period_mode: str, start_date: date, end_date: date) -> str:
    if period_mode == "Daily":
        return f"Daily snapshot · {start_date.strftime('%d %b %Y')}"
    if period_mode == "Weekly":
        return f"Last 7 days · {start_date.strftime('%d %b')} → {end_date.strftime('%d %b %Y')}"
    if period_mode == "Monthly":
        return f"Last 30 days · {start_date.strftime('%d %b')} → {end_date.strftime('%d %b %Y')}"
    if period_mode == "Yearly":
        return f"Last 365 days · {start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}"
    return f"Custom period · {start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}"

def build_period_trend(daily_df: pd.DataFrame, grain: str) -> pd.DataFrame:
    if daily_df.empty:
        return pd.DataFrame()
        
    df = daily_df.copy()
    df["metric_date"] = pd.to_datetime(df["metric_date"])
    
    if grain == "day":
        df["period_start"] = df["metric_date"].dt.normalize()
        label_fmt = "%d %b"
    elif grain == "week":
        df["period_start"] = df["metric_date"] - pd.to_timedelta(df["metric_date"].dt.weekday, unit="D")
        label_fmt = "%d %b"
    elif grain == "month":
        df["period_start"] = df["metric_date"].dt.to_period("M").dt.to_timestamp()
        label_fmt = "%b %Y"
    else:
        df["period_start"] = df["metric_date"].dt.to_period("Y").dt.to_timestamp()
        label_fmt = "%Y"
        
    trend = (
        df.groupby("period_start", as_index=False)
        .agg(
            walk_by_traffic=("walk_by_traffic", "mean"),
            store_interest=("store_interest", "mean"),
            near_store=("near_store", "mean"),
            store_visits=("store_visits", "sum"),
            qualified_visits=("qualified_footfall", "sum"),
            engaged_visits=("engaged_visits", "sum"),
            avg_dwell_seconds=("avg_dwell_seconds", "mean"),
        )
        .sort_values("period_start")
    )
    trend["period_label"] = trend["period_start"].dt.strftime(label_fmt)
    
    for c in ["walk_by_traffic", "store_interest", "near_store", "store_visits", "qualified_visits", "engaged_visits", "avg_dwell_seconds"]:
        trend[c] = trend[c].fillna(0)
        
    return trend

# ==========================================
# ATHENA
# ==========================================
def run_athena_query(query: str, database: str = ATHENA_DATABASE, timeout_sec: int = 45) -> pd.DataFrame:
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
            WorkGroup=ATHENA_WORKGROUP,
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        msg = e.response.get("Error", {}).get("Message", str(e))
        raise RuntimeError(f"Athena start error [{code}]: {msg}") from e
        
    execution_id = response["QueryExecutionId"]
    start_ts = time.time()
    
    while True:
        execution = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = execution["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - start_ts > timeout_sec:
            raise RuntimeError("Athena query timed out")
        time.sleep(1)
        
    if state != "SUCCEEDED":
        reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Unknown Athena error")
        raise RuntimeError(f"Athena query failed: {reason}")
        
    output_location = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    bucket, key = s3_uri_to_bucket_key(output_location)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))

# ==========================================
# DATA LOADERS
# ==========================================
@st.cache_data(ttl=300)
def load_store_list() -> pd.DataFrame:
    return run_athena_query("SELECT DISTINCT store_id FROM nstags_dashboard_metrics ORDER BY store_id")

@st.cache_data(ttl=300)
def load_available_dates(store_id: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(f"""
    WITH all_dates AS (
        SELECT DISTINCT {SQL_DATE_EXPR} AS metric_date
        FROM nstags_dashboard_metrics
        WHERE store_id = '{sid}'
        UNION
        SELECT DISTINCT {SQL_DATE_EXPR} AS metric_date
        FROM nstags_hourly_traffic_pretty
        WHERE store_id = '{sid}'
        UNION
        SELECT DISTINCT {SQL_DATE_EXPR} AS metric_date
        FROM nstags_dwell_buckets
        WHERE store_id = '{sid}'
        UNION
        SELECT DISTINCT DATE(from_unixtime(ts) AT TIME ZONE 'Asia/Kolkata') AS metric_date
        FROM nstags_live_analytics
        WHERE store_id = '{sid}'
    )
    SELECT metric_date
    FROM all_dates
    WHERE metric_date IS NOT NULL
    ORDER BY metric_date DESC
    """)

@st.cache_data(ttl=300)
def load_dashboard_daily_rows(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    date_filter = sql_date_range_filter(start_date_str, end_date_str)
    return run_athena_query(f"""
    SELECT
        {SQL_DATE_EXPR} AS metric_date,
        walk_by_traffic,
        store_interest,
        near_store,
        store_visits,
        qualified_footfall,
        engaged_visits,
        avg_dwell_seconds,
        median_dwell_seconds
    FROM nstags_dashboard_metrics
    WHERE store_id = '{sid}'
    AND {date_filter}
    ORDER BY metric_date
    """)

@st.cache_data(ttl=300)
def load_hourly_traffic_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    date_filter = sql_date_range_filter(start_date_str, end_date_str)
    return run_athena_query(f"""
    SELECT
        hour_of_day,
        format('%02d:00', hour_of_day) AS hour_label,
        ROUND(AVG(avg_far_devices), 2) AS avg_far_devices,
        ROUND(AVG(avg_mid_devices), 2) AS avg_mid_devices,
        ROUND(AVG(avg_near_devices), 2) AS avg_near_devices
    FROM nstags_hourly_traffic_pretty
    WHERE store_id = '{sid}'
    AND {date_filter}
    GROUP BY hour_of_day
    ORDER BY hour_of_day
    """)

@st.cache_data(ttl=300)
def load_dwell_buckets_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    date_filter = sql_date_range_filter(start_date_str, end_date_str)
    return run_athena_query(f"""
    SELECT
        dwell_bucket,
        ROUND(SUM(visits), 2) AS visits
    FROM nstags_dwell_buckets
    WHERE store_id = '{sid}'
    AND {date_filter}
    GROUP BY dwell_bucket
    """)

@st.cache_data(ttl=300)
def load_brand_mix_hourly_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    return run_athena_query(f"""
    SELECT
        hour(from_unixtime(ts) AT TIME ZONE 'Asia/Kolkata') AS hour_of_day,
        format('%02d:00', hour(from_unixtime(ts) AT TIME ZONE 'Asia/Kolkata')) AS hour_label,
        ROUND(AVG(apple_devices), 2) AS avg_apple_devices,
        ROUND(AVG(samsung_devices), 2) AS avg_samsung_devices,
        ROUND(AVG(other_devices), 2) AS avg_other_devices
    FROM nstags_live_analytics
    WHERE store_id = '{sid}'
    AND DATE(from_unixtime(ts) AT TIME ZONE 'Asia/Kolkata') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
    GROUP BY hour(from_unixtime(ts) AT TIME ZONE 'Asia/Kolkata')
    ORDER BY hour_of_day
    """)

@st.cache_data(ttl=300)
def load_intelligence_scores_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    date_filter = sql_date_range_filter(start_date_str, end_date_str)
    return run_athena_query(f"""
    SELECT
        ROUND(AVG(store_magnet_score), 4) AS store_magnet_score,
        ROUND(AVG(window_capture_index), 4) AS window_capture_index,
        ROUND(AVG(entry_efficiency_score), 4) AS entry_efficiency_score,
        ROUND(AVG(dwell_quality_index), 4) AS dwell_quality_index
    FROM nstags_intelligence_scores
    WHERE store_id = '{sid}'
    AND {date_filter}
    """)

@st.cache_data(ttl=300)
def load_dynamic_index_scores_range(store_id: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    sid = validate_store_id(store_id)
    date_filter = sql_date_range_filter(start_date_str, end_date_str)
    return run_athena_query(f"""
    SELECT
        ROUND(AVG(traffic_intelligence_index), 2) AS traffic_intelligence_index,
        ROUND(AVG(visit_quality_index), 2) AS visit_quality_index,
        ROUND(AVG(store_attraction_index), 2) AS store_attraction_index,
        ROUND(AVG(audience_quality_index), 2) AS audience_quality_index,
        ROUND(AVG(walk_by_score), 2) AS walk_by_score,
        ROUND(AVG(interest_score), 2) AS interest_score,
        ROUND(AVG(near_store_score), 2) AS near_store_score,
        ROUND(AVG(qualified_score), 2) AS qualified_score,
        ROUND(AVG(engaged_score), 2) AS engaged_score,
        ROUND(AVG(dwell_score), 2) AS dwell_score,
        ROUND(AVG(store_magnet_percentile_score), 2) AS store_magnet_percentile_score,
        ROUND(AVG(window_capture_score), 2) AS window_capture_score,
        ROUND(AVG(entry_efficiency_percentile_score), 2) AS entry_efficiency_percentile_score,
        ROUND(AVG(dwell_quality_score), 2) AS dwell_quality_score,
        ROUND(AVG(premium_device_mix_score), 2) AS premium_device_mix_score,
        ROUND(AVG(volume_confidence_score), 2) AS volume_confidence_score,
        MAX(COALESCE(benchmark_population, 0)) AS benchmark_population
    FROM nstags_index_scores_dynamic
    WHERE store_id = '{sid}'
    AND {date_filter}
    """)

# ==========================================
# AI
# ==========================================
@st.cache_data(ttl=300)
def generate_ai_brief(ai_payload):
    if not GEMINI_API_KEY:
        return "⚠️ **AI unavailable:** GEMINI_API_KEY not configured in Secrets."
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        prompt = f"""
        You are a retail analytics strategy consultant.
        Analyze these metrics carefully. Only use the numbers below. Do not invent any extra data.
        
        Scope: {ai_payload['scope']}
        Mode: {ai_payload['mode']}
        
        LIVE TRAFFIC INTENSITY
        - Walk-by traffic index: {ai_payload['walk_by']}
        - Store interest index: {ai_payload['interest']}
        - Near-store index: {ai_payload['near_store']}
        
        VISIT METRICS
        - Store visits: {ai_payload['visits']}
        - Qualified visits: {ai_payload['qualified_visits']}
        - Engaged visits: {ai_payload['engaged_visits']}
        - Qualified visit rate: {ai_payload['qualified_rate']}%
        - Engaged visit rate: {ai_payload['engaged_rate']}%
        - Average dwell: {ai_payload['avg_dwell']}
        
        COMMERCIAL INPUTS
        - Transactions: {ai_payload['transactions']}
        - Revenue or campaign value: {ai_payload['value']}
        - Sales conversion from visits: {ai_payload['sales_conversion']}%
        
        INDEX LAYER
        - Traffic Intelligence Index: {ai_payload['tii']}
        - Visit Quality Index: {ai_payload['vqi']}
        - Store Attraction Index: {ai_payload['sai']}
        - Audience Quality Index: {ai_payload['aqi']}
        
        Write an executive brief in Markdown with exactly this structure:
        * **What happened:** [1 sentence]
        * **What the traffic says:** [Interpret walk-by / interest / near-store correctly as traffic intensity, not literal counts]
        * **What the visits say:** [Interpret visit quality and dwell]
        * **What the index says:** [Interpret TII / VQI / SAI briefly]
        * **Primary bottleneck:** [Choose one clear issue]
        * **Recommended action:** [One concrete action]
        """
        response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        return response.text
    except Exception:
        return "⚠️ **AI unavailable:** connection or rate-limit issue."

# ==========================================
# APP HEADER
# ==========================================
st.markdown("""
<div class="hero-shell">
<div class="hero-eyebrow">
<span class="hero-eyebrow-dot"></span>
Live Intelligence
</div>
<div class="hero-title">nsTags Intelligence</div>
<p class="hero-sub">Retail Operations · Retail Media Measurement · Conversion Diagnostics</p>
<div class="hero-badges">
<span class="hero-badge badge-indigo">Proximity Intelligence</span>
<span class="hero-badge badge-emerald">Athena Analytics</span>
<span class="hero-badge badge-amber">Executive AI Briefing</span>
</div>
</div>
""", unsafe_allow_html=True)

# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("""
    <div class="sidebar-logo">
    <div class="sidebar-logo-mark">📈</div>
    <div>
    <div class="sidebar-logo-text">nsTags</div>
    <div class="sidebar-logo-sub">Retail Intelligence</div>
    </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### Configuration")
    app_mode = st.radio("Business Mode", ["Retail Ops", "Retail Media"], horizontal=True)
    
    try:
        stores_df = load_store_list()
    except Exception as e:
        st.error(f"Failed to load store list: {e}")
        st.stop()
        
    if stores_df.empty:
        st.warning("No stores found.")
        st.stop()
        
    st.markdown("### Store")
    selected_store = st.selectbox(
        "Active Store",
        stores_df["store_id"].dropna().astype(str).tolist(),
        help="Select the store to analyze",
    )
    
    try:
        dates_df = load_available_dates(selected_store)
    except Exception as e:
        st.error(f"Failed to load available dates: {e}")
        st.stop()
        
    if dates_df.empty:
        st.warning("No dates found for this store.")
        st.stop()
        
    dates_df["metric_date"] = pd.to_datetime(dates_df["metric_date"]).dt.date
    available_dates = sorted(set(dates_df["metric_date"].dropna().tolist()))
    min_available_date = min(available_dates)
    max_available_date = max(available_dates)
    
    st.markdown("### Period Selection")
    period_mode = st.radio(
        "Analysis Window",
        ["Daily", "Weekly", "Monthly", "Yearly", "Custom"],
        horizontal=False,
    )
    
    if period_mode == "Daily":
        selected_day = st.selectbox(
            "Select Date",
            options=list(reversed(available_dates)),
            index=0,
            format_func=lambda d: d.strftime("%d %b %Y"),
        )
        start_date = selected_day
        end_date = selected_day
    elif period_mode == "Weekly":
        end_date = st.date_input(
            "Week End Date",
            value=max_available_date,
            min_value=min_available_date,
            max_value=max_available_date,
        )
        start_date = max(min_available_date, end_date - timedelta(days=6))
    elif period_mode == "Monthly":
        end_date = st.date_input(
            "Month End Date",
            value=max_available_date,
            min_value=min_available_date,
            max_value=max_available_date,
        )
        start_date = max(min_available_date, end_date - timedelta(days=29))
    elif period_mode == "Yearly":
        end_date = st.date_input(
            "Year End Date",
            value=max_available_date,
            min_value=min_available_date,
            max_value=max_available_date,
        )
        start_date = max(min_available_date, end_date - timedelta(days=364))
    else:
        default_start = max(min_available_date, max_available_date - timedelta(days=29))
        selected_range = st.date_input(
            "Custom Date Range",
            value=(default_start, max_available_date),
            min_value=min_available_date,
            max_value=max_available_date,
        )
        if isinstance(selected_range, tuple) and len(selected_range) == 2:
            start_date, end_date = selected_range
        else:
            start_date = default_start
            end
