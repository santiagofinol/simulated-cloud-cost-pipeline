"""
Cloud Cost Monitoring Dashboard
Streamlit replacement for Power BI dashboard
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

st.set_page_config(
    page_title="Cloud Cost Monitor",
    page_icon="☁️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

/* Background */
.stApp {
    background: #0a0e1a;
    color: #e2e8f0;
}

[data-testid="stSidebar"] {
    background: #0f1629 !important;
    border-right: 1px solid #1e2d4a;
}

/* Title */
h1 { 
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 1.4rem !important;
    color: #38bdf8 !important;
    letter-spacing: 0.05em;
    border-bottom: 1px solid #1e3a5f;
    padding-bottom: 0.5rem;
}
h2, h3 {
    font-family: 'IBM Plex Mono', monospace !important;
    color: #94a3b8 !important;
    font-size: 0.85rem !important;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

/* Metric cards */
[data-testid="metric-container"] {
    background: #0f1629;
    border: 1px solid #1e3a5f;
    border-radius: 4px;
    padding: 1.2rem 1.4rem !important;
    position: relative;
    overflow: hidden;
}
[data-testid="metric-container"]::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, #38bdf8, #6366f1);
}
[data-testid="stMetricLabel"] {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.68rem !important;
    color: #64748b !important;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}
[data-testid="stMetricValue"] {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 1.8rem !important;
    color: #f1f5f9 !important;
}
[data-testid="stMetricDelta"] {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.72rem !important;
}

/* Charts */
.stPlotlyChart {
    background: #0f1629;
    border: 1px solid #1e3a5f;
    border-radius: 4px;
    padding: 0.5rem;
}

/* Sidebar labels */
[data-testid="stSidebar"] label {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.72rem !important;
    color: #64748b !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}

/* Dataframe */
[data-testid="stDataFrame"] {
    border: 1px solid #1e3a5f;
    border-radius: 4px;
}

/* Expander */
[data-testid="stExpander"] {
    background: #0f1629;
    border: 1px solid #1e3a5f;
    border-radius: 4px;
}

/* Divider */
hr { border-color: #1e2d4a; }

/* Caption */
.stCaption { 
    font-family: 'IBM Plex Mono', monospace !important;
    color: #334155 !important;
    font-size: 0.65rem !important;
}

/* Warning/info/success */
[data-testid="stAlert"] {
    background: #0f1629 !important;
    border: 1px solid #1e3a5f !important;
    color: #94a3b8 !important;
}
</style>
""", unsafe_allow_html=True)

try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=300000, key="dashboard_refresh")
except ImportError:
    pass

load_dotenv()
DB_URL = os.environ.get("DB_URL", "")

PLOTLY_THEME = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(15,22,41,1)",
    font=dict(family="IBM Plex Mono", color="#94a3b8", size=11),
    xaxis=dict(gridcolor="#1e2d4a", linecolor="#1e3a5f"),
    yaxis=dict(gridcolor="#1e2d4a", linecolor="#1e3a5f"),
    margin=dict(l=20, r=20, t=40, b=20),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1,
        font=dict(size=10),
    ),
)

ACCENT_COLORS = ["#38bdf8", "#6366f1", "#34d399", "#f59e0b", "#f43f5e"]


@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


engine = get_engine()

# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.markdown("### ☁️ CLOUD COST")
st.sidebar.markdown("---")

with engine.connect() as conn:
    services = pd.read_sql("SELECT service_name FROM dim_service ORDER BY service_name", conn)
    regions = pd.read_sql("SELECT region_code FROM dim_region ORDER BY region_code", conn)

selected_services = st.sidebar.multiselect(
    "Services",
    options=services["service_name"].tolist(),
    default=services["service_name"].tolist(),
)

selected_regions = st.sidebar.multiselect(
    "Regions",
    options=regions["region_code"].tolist(),
    default=regions["region_code"].tolist(),
)

col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input("From", datetime.now() - timedelta(days=7))
with col2:
    end_date = st.date_input("To", datetime.now() + timedelta(days=1))

st.sidebar.markdown("---")
st.sidebar.caption(f"v1.0 · {datetime.now().strftime('%H:%M:%S')}")


# ── Data loading ───────────────────────────────────────────────────────────────
def build_in_clause(items):
    return ", ".join([f"'{item}'" for item in items])


@st.cache_data(ttl=300)
def load_cost_data(services, regions, start, end):
    svc_filter = build_in_clause(services)
    reg_filter = build_in_clause(regions)
    query = f"""
    SELECT 
        fc.cost_id, fc.cost_usd, fc.resource_count, fc.ingested_at,
        dd.full_date, dd.year, dd.month, dd.day,
        dt.hour, dt.minute,
        ds.service_name, ds.service_type,
        dr.region_code, dr.geography,
        fa.is_anomaly, fa.anomaly_score, fa.severity
    FROM fact_cost fc
    JOIN dim_date dd ON dd.date_key = fc.date_key
    JOIN dim_time dt ON dt.time_key = fc.time_key
    JOIN dim_service ds ON ds.service_key = fc.service_key
    JOIN dim_region dr ON dr.region_key = fc.region_key
    LEFT JOIN fact_anomaly fa ON fa.cost_id = fc.cost_id
    WHERE ds.service_name IN ({svc_filter})
      AND dr.region_code IN ({reg_filter})
      AND dd.full_date BETWEEN '{start}' AND '{end}'
    ORDER BY fc.ingested_at
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    df["full_date"] = pd.to_datetime(df["full_date"]).dt.date
    return df


@st.cache_data(ttl=300)
def load_forecast_data(services, regions):
    svc_filter = build_in_clause(services)
    reg_filter = build_in_clause(regions)
    query = f"""
    SELECT ff.forecast_id, ff.forecast_ts, ff.yhat, ff.yhat_lower, ff.yhat_upper,
           ff.created_at, ds.service_name, dr.region_code
    FROM fact_forecast ff
    JOIN dim_service ds ON ds.service_key = ff.service_key
    JOIN dim_region dr ON dr.region_key = ff.region_key
    WHERE ds.service_name IN ({svc_filter})
      AND dr.region_code IN ({reg_filter})
      AND ff.forecast_ts >= NOW() - INTERVAL '7 days'
    ORDER BY ff.forecast_ts
    """
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


@st.cache_data(ttl=300)
def load_anomaly_data(services, regions, today):
    svc_filter = build_in_clause(services)
    reg_filter = build_in_clause(regions)
    query = f"""
    SELECT fc.ingested_at, ds.service_name, dr.region_code,
           fc.cost_usd, fa.severity, fa.anomaly_score
    FROM fact_anomaly fa
    JOIN fact_cost fc ON fc.cost_id = fa.cost_id
    JOIN dim_service ds ON ds.service_key = fc.service_key
    JOIN dim_region dr ON dr.region_key = fc.region_key
    WHERE fa.is_anomaly = TRUE
      AND ds.service_name IN ({svc_filter})
      AND dr.region_code IN ({reg_filter})
      AND fc.ingested_at::date = '{today}'
    ORDER BY fa.severity DESC, fc.ingested_at DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


# ── KPI measures (full_date is now datetime.date) ──────────────────────────────
def current_hour_cost(df):
    """
    Returns 0 if no rows exist for the current hour — usually because the
    pipeline hasn't run recently. Fix with:
        python scripts/generate_data.py && python scripts/transform.py

    Uncomment the fallback below to show the most recent hour instead
    (useful for demos with stale data).
    """
    now = datetime.now()
    mask = (df["full_date"] == now.date()) & (df["hour"] == now.hour)
    cost = df.loc[mask, "cost_usd"].sum()

    return cost


def mtd_actual_cost(df):
    start_of_month = datetime.now().replace(day=1).date()
    return df.loc[df["full_date"] >= start_of_month, "cost_usd"].sum()


def mtd_forecast_cost(forecast_df):
    if forecast_df.empty:
        return 0.0
    now = datetime.now()
    start_of_month = now.replace(day=1).date()
    fts = pd.to_datetime(forecast_df["forecast_ts"], utc=True)
    mask = (fts >= pd.Timestamp(start_of_month, tz="UTC")) & (fts <= pd.Timestamp(now.date(), tz="UTC"))
    return forecast_df.loc[mask.values, "yhat"].sum()


def mtd_variance_pct(actual, forecast):
    return 0.0 if forecast == 0 else (actual - forecast) / forecast


def anomalies_today(df):
    today = datetime.now().date()
    mask = (df["full_date"] == today) & (df["is_anomaly"] == True)
    return int(mask.sum())


def high_severity_anomalies(df):
    today = datetime.now().date()
    mask = (df["full_date"] == today) & (df["is_anomaly"] == True) & (df["severity"] == "high")
    return int(mask.sum())


def cost_wow_change(df):
    now = datetime.now().date()
    cur = df.loc[df["full_date"] >= (now - timedelta(days=7)), "cost_usd"].sum()
    prev = df.loc[
        (df["full_date"] >= (now - timedelta(days=14))) &
        (df["full_date"] < (now - timedelta(days=7))),
        "cost_usd",
    ].sum()
    return 0.0 if prev == 0 else (cur - prev) / prev


def cost_dod_change(df):
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    t = df.loc[df["full_date"] == today, "cost_usd"].sum()
    y = df.loc[df["full_date"] == yesterday, "cost_usd"].sum()
    return 0.0 if y == 0 else (t - y) / y


# ── Load data ──────────────────────────────────────────────────────────────────
df_cost = load_cost_data(
    tuple(selected_services), tuple(selected_regions), start_date, end_date
)
df_forecast = load_forecast_data(tuple(selected_services), tuple(selected_regions))
today_str = datetime.now().strftime("%Y-%m-%d")
df_anomalies = load_anomaly_data(
    tuple(selected_services), tuple(selected_regions), today_str
)

# ── Header ─────────────────────────────────────────────────────────────────────
st.title("☁ CLOUD COST MONITOR")
st.caption(f"LAST UPDATED {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · AUTO-REFRESH 5 MIN")

if df_cost.empty:
    st.warning("No data found. Adjust filters or date range.")
    st.stop()

# ── KPI row ────────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)

with k1:
    chc = current_hour_cost(df_cost)
    dod = cost_dod_change(df_cost)
    st.metric(
        "💰 Current Hour Cost",
        f"${chc:,.2f}",
        delta=f"{dod*100:+.1f}% vs yesterday" if dod != 0 else None,
        delta_color="inverse",
    )

with k2:
    anom = anomalies_today(df_cost)
    high = high_severity_anomalies(df_cost)
    st.metric(
        "🚨 Anomalies Today",
        str(anom),
        delta=f"{high} high severity" if high > 0 else None,
        delta_color="inverse",
    )

with k3:
    mtd_a = mtd_actual_cost(df_cost)
    mtd_f = mtd_forecast_cost(df_forecast)
    var = mtd_variance_pct(mtd_a, mtd_f)
    st.metric(
        "📈 MTD Variance",
        f"{var*100:+.1f}%",
        delta="over forecast" if var > 0 else "under forecast" if var < 0 else "on target",
        delta_color="inverse",
    )

with k4:
    total = df_cost["cost_usd"].sum()
    wow = cost_wow_change(df_cost)
    st.metric(
        "📅 Period Total",
        f"${total:,.2f}",
        delta=f"{wow*100:+.1f}% vs prev week" if wow != 0 else None,
        delta_color="inverse",
    )

with st.expander("Variance Details"):
    d1, d2 = st.columns(2)
    with d1:
        st.metric("MTD Actual Cost", f"${mtd_a:,.2f}")
    with d2:
        st.metric("MTD Forecast Cost", f"${mtd_f:,.2f}")
    st.caption(
        "A large variance % (e.g. +3000%) typically means the Prophet model was trained "
        "on flat/low data while the generator now produces higher costs. "
        "Retrain with: python scripts/train.py after a fresh backfill."
    )
    st.markdown("---")
    yesterday_hour_cost = df_cost.loc[
        (df_cost["full_date"] == (datetime.now().date() - timedelta(days=1))) &
        (df_cost["hour"] == datetime.now().hour),
        "cost_usd",
    ].sum()
    st.caption(
        f"Yesterday's same-hour cost was ${yesterday_hour_cost:,.2f}. "
        "A high DoD change (e.g. +1822%) due to yesterday's cost being zero or close to it "
        "while today's batch generated a normal-sized cost, expected because "
        "partial previous day data was deleted."
    )

st.markdown("---")

# ── Charts row ─────────────────────────────────────────────────────────────────
st.subheader("Cost Trends & Forecasting")
c1, c2 = st.columns(2)

with c1:
    st.markdown("**Actual Costs by Service**")
    agg = df_cost.groupby(["ingested_at", "service_name"])["cost_usd"].sum().reset_index()
    fig = px.line(
        agg,
        x="ingested_at",
        y="cost_usd",
        color="service_name",
        color_discrete_sequence=ACCENT_COLORS,
        labels={"cost_usd": "Cost (USD)", "ingested_at": "Time"},
        height=380,
    )
    fig.update_traces(line=dict(width=2))
    fig.update_layout(**PLOTLY_THEME)
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.markdown("**Forecast with Confidence Bands**")
    if not df_forecast.empty:
        fc_agg = df_forecast.groupby("forecast_ts").agg(
            yhat=("yhat", "sum"),
            yhat_lower=("yhat_lower", "sum"),
            yhat_upper=("yhat_upper", "sum"),
        ).reset_index()

        fig2 = go.Figure()
        ts_fwd = fc_agg["forecast_ts"].tolist()
        fig2.add_trace(go.Scatter(
            x=ts_fwd + ts_fwd[::-1],
            y=fc_agg["yhat_upper"].tolist() + fc_agg["yhat_lower"].tolist()[::-1],
            fill="toself",
            fillcolor="rgba(56,189,248,0.12)",
            line=dict(color="rgba(0,0,0,0)"),
            name="90% CI",
            hoverinfo="skip",
        ))
        fig2.add_trace(go.Scatter(
            x=fc_agg["forecast_ts"],
            y=fc_agg["yhat"],
            mode="lines",
            line=dict(color="#38bdf8", width=2),
            name="Forecast",
        ))
        fig2.update_layout(height=380, **PLOTLY_THEME)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No forecast data. Run `python scripts/score.py` first.")

st.markdown("---")

# ── Service breakdown ──────────────────────────────────────────────────────────
st.subheader("Service & Region Breakdown")
b1, b2 = st.columns(2)

with b1:
    svc_totals = df_cost.groupby("service_name")["cost_usd"].sum().reset_index()
    fig3 = px.bar(
        svc_totals,
        x="service_name",
        y="cost_usd",
        color="service_name",
        color_discrete_sequence=ACCENT_COLORS,
        labels={"cost_usd": "Total Cost (USD)", "service_name": "Service"},
        height=320,
    )
    fig3.update_layout(showlegend=False, **PLOTLY_THEME)
    st.plotly_chart(fig3, use_container_width=True)

with b2:
    reg_totals = df_cost.groupby("region_code")["cost_usd"].sum().reset_index()
    fig4 = px.pie(
        reg_totals,
        names="region_code",
        values="cost_usd",
        color_discrete_sequence=ACCENT_COLORS,
        height=320,
        hole=0.55,
    )
    fig4.update_traces(textfont_family="IBM Plex Mono", textfont_size=11)
    fig4.update_layout(**PLOTLY_THEME)
    st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")

# ── Anomaly table ──────────────────────────────────────────────────────────────
st.subheader("Anomalies Detected Today")

if not df_anomalies.empty:
    def severity_color(val):
        mapping = {
            "high": "background-color:#4c1010; color:#f87171; font-weight:600",
            "medium": "background-color:#3d2e00; color:#fbbf24",
            "low": "background-color:#3d3500; color:#fde68a",
        }
        return mapping.get(val, "")

    styled = df_anomalies.style.applymap(severity_color, subset=["severity"])
    st.dataframe(
        styled,
        column_config={
            "ingested_at": st.column_config.DatetimeColumn("Timestamp", format="YYYY-MM-DD HH:mm"),
            "service_name": "Service",
            "region_code": "Region",
            "cost_usd": st.column_config.NumberColumn("Cost (USD)", format="$%.2f"),
            "severity": "Severity",
            "anomaly_score": st.column_config.NumberColumn("Score", format="%.3f"),
        },
        hide_index=True,
        use_container_width=True,
    )

    sev_counts = df_anomalies["severity"].value_counts().reset_index()
    sev_counts.columns = ["severity", "count"]
    fig5 = px.bar(
        sev_counts,
        x="severity",
        y="count",
        color="severity",
        color_discrete_map={"high": "#f43f5e", "medium": "#f59e0b", "low": "#eab308"},
        labels={"count": "Count", "severity": "Severity"},
        height=260,
    )
    fig5.update_layout(showlegend=False, **PLOTLY_THEME)
    st.plotly_chart(fig5, use_container_width=True)
else:
    st.success("No anomalies detected today.")

st.markdown("---")

# ── Data explorer ──────────────────────────────────────────────────────────────
st.subheader("Data Explorer")

with st.expander("Raw Cost Data"):
    st.dataframe(
        df_cost,
        column_config={
            "cost_usd": st.column_config.NumberColumn("Cost (USD)", format="$%.4f"),
            "ingested_at": st.column_config.DatetimeColumn("Ingested At", format="YYYY-MM-DD HH:mm"),
            "full_date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
        },
        hide_index=True,
        use_container_width=True,
    )

with st.expander("Forecast Data"):
    if not df_forecast.empty:
        st.dataframe(
            df_forecast,
            column_config={
                "forecast_ts": st.column_config.DatetimeColumn("Forecast Time", format="YYYY-MM-DD HH:mm"),
                "yhat": st.column_config.NumberColumn("Forecast", format="$%.4f"),
                "yhat_lower": st.column_config.NumberColumn("Lower", format="$%.4f"),
                "yhat_upper": st.column_config.NumberColumn("Upper", format="$%.4f"),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No forecast data available.")

st.divider()
st.caption("STREAMLIT · SUPABASE POSTGRESQL · PROPHET · ISOLATION FOREST")
