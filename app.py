"""
Marine Weather Streaming Dashboard
  Live tab:    Kafka -> Streamlit (auto-refresh)
  History tab: MongoDB (written by Spark) -> Streamlit (filters, aggregation, CSV, PNG)
"""
import json
from collections import deque
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import plotly.io as pio
import streamlit as st
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="Marine Weather Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------------------ settings
DEFAULT_REFRESH_SECONDS = 15      # set to 900 for a 15-minute refresh
DISPLAY_TZ = "Asia/Manila"
BUFFER_SIZE = 1000                # live records kept in memory
BACKFILL_PER_PARTITION = 200      # recent messages read when the dashboard connects
MAX_HISTORY_ROWS = 200_000
REQUIRED_FIELDS = ["timestamp", "value", "metric_type", "sensor_id"]
METRICS = ["wave_height", "swell_height", "water_temperature", "wind_speed", "visibility"]
METRIC_LABELS = {
    "all": "All metrics",
    "wave_height": "Wave height",
    "swell_height": "Swell height",
    "water_temperature": "Water temperature",
    "wind_speed": "Wind speed",
    "visibility": "Visibility",
}
TIME_RANGES = {
    "1h": timedelta(hours=1),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
    "all": None,
}
AGG_FREQ = {"hourly": "1h", "daily": "1D", "weekly": "1W"}
PLOT_CONFIG = {"displaylogo": False, "toImageButtonOptions": {"format": "png", "scale": 2}}
LIVE_COLUMNS = ["timestamp", "value", "metric_type", "sensor_id", "location", "unit", "latitude", "longitude"]
COLORS = ["#0B6E99", "#1FAF9B", "#F2A541", "#D1495B", "#6C5B7B", "#3D8361"]

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .block-container { padding-top: 2.2rem; max-width: 1300px; }
    h1 { font-weight: 700; letter-spacing: -0.02em; color: #0B3954; }
    h2, h3 { font-weight: 600; letter-spacing: -0.01em; color: #0B3954; }
    section[data-testid="stSidebar"] { border-right: 1px solid #D5E3EE; }
    div[data-testid="stMetric"] {
        background: #FFFFFF; border: 1px solid #DCE7F0; border-radius: 14px;
        padding: 14px 18px; box-shadow: 0 1px 3px rgba(11, 57, 84, 0.06);
    }
    div[data-testid="stMetricLabel"] p {
        font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; color: #5B7A91;
    }
    div[data-testid="stMetricValue"] { color: #0B3954; font-weight: 600; }
    button[data-baseweb="tab"] { font-weight: 600; font-size: 1rem; }
    div[data-testid="stDataFrame"] { border: 1px solid #DCE7F0; border-radius: 12px; }
    .pill {
        display: inline-block; padding: 3px 12px; border-radius: 999px;
        font-size: 0.78rem; font-weight: 600; margin-right: 8px;
    }
    .pill.ok   { background: #DDF3EC; color: #176B52; }
    .pill.warn { background: #FCEFD6; color: #8A5A00; }
    .pill.bad  { background: #F8DDE1; color: #9B2335; }
    .muted { color: #5B7A91; font-size: 0.9rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


def style_fig(fig, height=None):
    """Apply one consistent look to every chart."""
    fig.update_layout(
        font_family="Inter, sans-serif",
        font_color="#10283B",
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="rgba(0,0,0,0)",
        title_font_size=16,
        margin=dict(l=10, r=10, t=60, b=10),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, title_text=""),
    )
    fig.update_xaxes(showgrid=False, linecolor="#C9D8E4")
    fig.update_yaxes(gridcolor="#E6EEF4", zeroline=False)
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1].replace("_", " ").title()))
    if height:
        fig.update_layout(height=height)
    return fig


# ------------------------------------------------------------------- sidebar
def setup_sidebar():
    st.sidebar.title("Controls")
    st.sidebar.subheader("Kafka (live stream)")
    kafka_broker = st.sidebar.text_input("Kafka Broker", value="localhost:9092")
    kafka_topic = st.sidebar.text_input("Kafka Topic", value="marine-weather")
    st.sidebar.subheader("MongoDB (history)")
    mongo_uri = st.sidebar.text_input("MongoDB URI", value="mongodb://localhost:27017")
    mongo_db = st.sidebar.text_input("Database", value="marine_db")
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": "MongoDB",
        "mongo_uri": mongo_uri,
        "mongo_db": mongo_db,
    }


# --------------------------------------------------------------------- Kafka
def _safe_json(raw):
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def _create_consumer(broker, topic):
    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=_safe_json,
        consumer_timeout_ms=2000,
    )
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        raise ValueError(f"Topic '{topic}' not found. Create it first (see README).")
    tps = [TopicPartition(topic, p) for p in sorted(partitions)]
    consumer.assign(tps)
    begin, end = consumer.beginning_offsets(tps), consumer.end_offsets(tps)
    for tp in tps:  # start a little behind the end so the chart isn't empty
        consumer.seek(tp, max(begin[tp], end[tp] - BACKFILL_PER_PARTITION))
    return consumer


def _parse_message(data):
    if not isinstance(data, dict) or not all(k in data for k in REQUIRED_FIELDS):
        return None
    try:
        ts = datetime.fromisoformat(str(data["timestamp"]).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return {
            "timestamp": ts,
            "value": float(data["value"]),
            "metric_type": str(data["metric_type"]),
            "sensor_id": str(data["sensor_id"]),
            "location": data.get("location") or str(data["sensor_id"]),
            "unit": data.get("unit", ""),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
        }
    except (ValueError, TypeError):
        return None


def buffer_to_df(buffer):
    if not buffer:
        return pd.DataFrame(columns=LIVE_COLUMNS)
    df = pd.DataFrame(list(buffer))
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(DISPLAY_TZ)
    return df.sort_values("timestamp")


def consume_kafka_data(config):
    """Poll Kafka for new messages and return the rolling buffer as a DataFrame."""
    key = (config["kafka_broker"], config["kafka_topic"])
    if st.session_state.get("kafka_key") != key:  # settings changed -> reset
        old = st.session_state.get("kafka_consumer")
        if old is not None:
            try:
                old.close()
            except Exception:
                pass
        st.session_state["kafka_consumer"] = None
        st.session_state["kafka_key"] = key
        st.session_state["live_buffer"] = deque(maxlen=BUFFER_SIZE)
    buffer = st.session_state.setdefault("live_buffer", deque(maxlen=BUFFER_SIZE))

    consumer = st.session_state.get("kafka_consumer")
    if consumer is None:
        try:
            consumer = _create_consumer(*key)
            st.session_state["kafka_consumer"] = consumer
        except NoBrokersAvailable:
            st.error(f"Kafka broker unavailable at {key[0]}. Is Kafka running?")
            return buffer_to_df(buffer)
        except (KafkaError, ValueError) as e:
            st.error(f"Kafka error: {e}")
            return buffer_to_df(buffer)
        except Exception as e:
            st.error(f"Unexpected error connecting to Kafka: {e}")
            return buffer_to_df(buffer)

    try:
        batches = consumer.poll(timeout_ms=2000, max_records=500)
    except KafkaError as e:
        st.error(f"Kafka poll error: {e}")
        return buffer_to_df(buffer)

    bad = 0
    for msgs in batches.values():
        for m in msgs:
            rec = _parse_message(m.value)
            if rec is None:
                bad += 1
            else:
                buffer.append(rec)
    if bad:
        st.warning(f"Skipped {bad} malformed message(s).")
    return buffer_to_df(buffer)


# ------------------------------------------------------------------- MongoDB
@st.cache_resource(show_spinner=False)
def get_mongo_client(uri):
    return MongoClient(uri, serverSelectionTimeoutMS=3000)


@st.cache_data(ttl=30, show_spinner=False)
def list_sensors(uri, db_name):
    try:
        return sorted(get_mongo_client(uri)[db_name]["readings"].distinct("sensor_id"))
    except PyMongoError:
        return []


def _utc_cutoff(time_range):
    delta = TIME_RANGES[time_range]
    if delta is None:
        return None
    return datetime.now(timezone.utc).replace(tzinfo=None) - delta


def _aggregate(df, aggregation):
    """Resample to hourly/daily/weekly bins: value = mean, plus min/max/count."""
    keys = ["sensor_id", "location", "metric_type", "unit"]
    out = (
        df.set_index("timestamp")
        .groupby(keys)["value"]
        .resample(AGG_FREQ[aggregation])
        .agg(["mean", "min", "max", "count"])
        .reset_index()
    )
    out = out[out["count"] > 0].rename(
        columns={"mean": "value", "min": "min_value", "max": "max_value", "count": "n_records"}
    )
    out["value"] = out["value"].round(3)
    return out.sort_values("timestamp")


def query_historical_data(config, time_range="24h", metric=None, sensors=None, aggregation="raw"):
    """Query MongoDB readings (filters + optional aggregation). Returns a DataFrame or None on error."""
    try:
        col = get_mongo_client(config["mongo_uri"])[config["mongo_db"]]["readings"]
        query = {}
        cutoff = _utc_cutoff(time_range)
        if cutoff is not None:
            query["timestamp"] = {"$gte": cutoff}
        if metric and metric != "all":
            query["metric_type"] = metric
        if sensors:
            query["sensor_id"] = {"$in": list(sensors)}
        rows = list(col.find(query, {"_id": 0, "ingested_at": 0}).sort("timestamp", 1).limit(MAX_HISTORY_ROWS))
    except PyMongoError as e:
        st.error(f"MongoDB error: {e}")
        return None

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(DISPLAY_TZ)
    df["location"] = df["location"].fillna(df["sensor_id"]) if "location" in df else df["sensor_id"]
    if len(rows) >= MAX_HISTORY_ROWS:
        st.warning(f"Showing the first {MAX_HISTORY_ROWS:,} rows. Narrow the time range or use an aggregation.")
    if aggregation != "raw":
        df = _aggregate(df, aggregation)
    return df


def query_spark_aggregates(config, time_range, metric):
    try:
        col = get_mongo_client(config["mongo_uri"])[config["mongo_db"]]["aggregates"]
        query = {}
        cutoff = _utc_cutoff(time_range)
        if cutoff is not None:
            query["window_start"] = {"$gte": cutoff}
        if metric and metric != "all":
            query["metric_type"] = metric
        rows = list(col.find(query, {"_id": 0}).sort("window_start", -1).limit(1000))
    except PyMongoError as e:
        st.error(f"MongoDB error: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for c in ("window_start", "window_end", "updated_at"):
        if c in df:
            df[c] = pd.to_datetime(df[c], utc=True).dt.tz_convert(DISPLAY_TZ)
    return df.round(3)


# ------------------------------------------------------------------- helpers
@st.cache_data(show_spinner=False, max_entries=8)
def _fig_to_png(fig_json, height):
    return pio.from_json(fig_json).to_image(format="png", width=1400, height=height, scale=2)


def png_download_button(fig, filename, key, height=600):
    try:
        st.download_button("Download chart (PNG)", _fig_to_png(fig.to_json(), height),
                           file_name=filename, mime="image/png", key=key)
    except Exception:
        st.caption("PNG button unavailable (needs kaleido==0.2.1). Use the camera icon on the chart instead.")


def metric_label(m):
    return METRIC_LABELS.get(m, m.replace("_", " ").title())


# --------------------------------------------------------------- live view
def display_real_time_view(config, refresh_interval):
    st.header("Live Marine Conditions")
    rs = st.session_state.refresh_state
    state = "on" if rs["auto_refresh"] else "off"
    st.markdown(
        f"<span class='muted'>Auto-refresh {state}, every {refresh_interval} seconds. "
        f"Times shown in {DISPLAY_TZ}.</span>",
        unsafe_allow_html=True,
    )

    with st.spinner("Reading from Kafka..."):
        df = consume_kafka_data(config)

    if df.empty:
        st.info("No messages received yet. Make sure producer.py is running and Kafka is up.")
        return

    latest_ts = df["timestamp"].max()
    age = (pd.Timestamp.now(tz=DISPLAY_TZ) - latest_ts).total_seconds()
    cls, label = ("ok", "Live") if age < 150 else ("warn", "Delayed") if age < 600 else ("bad", "Stale")
    st.markdown(
        f"<span class='pill {cls}'>{label}</span>"
        f"<span class='muted'>Latest message received {age:.0f} seconds ago "
        f"({latest_ts.strftime('%H:%M:%S')})</span>",
        unsafe_allow_html=True,
    )
    st.write("")

    latest = df.sort_values("timestamp").groupby(["sensor_id", "metric_type"]).tail(1)
    c1, c2, c3 = st.columns(3)
    c1.metric("Locations reporting", latest["sensor_id"].nunique())
    c2.metric("Messages in buffer", len(df))
    c3.metric("Metrics", latest["metric_type"].nunique())

    st.subheader("Latest readings by location")
    table = latest.pivot(index="location", columns="metric_type", values="value").round(2)
    table = table.rename(
        columns=lambda c: f"{metric_label(c)} ({latest.loc[latest.metric_type == c, 'unit'].iloc[0]})"
    )
    table.index.name = "Location"
    st.dataframe(table, width="stretch")

    map_df = latest.drop_duplicates("sensor_id")[["latitude", "longitude"]].dropna().astype(float)
    if not map_df.empty:
        st.subheader("Monitoring locations")
        st.map(map_df, latitude="latitude", longitude="longitude", size=8000, color="#0B6E99")

    st.subheader("Real-time trend")
    available = [m for m in METRICS if m in set(df["metric_type"])] or sorted(df["metric_type"].unique())
    metric = st.selectbox("Metric", available, format_func=metric_label, key="live_metric")
    mdf = df[df["metric_type"] == metric]
    unit = mdf["unit"].iloc[-1] if not mdf.empty else ""
    fig = px.line(mdf, x="timestamp", y="value", color="location", markers=True,
                  title=f"{metric_label(metric)} ({unit}), last {len(mdf)} readings",
                  labels={"value": f"Value ({unit})", "timestamp": "Time", "location": "Location"},
                  color_discrete_sequence=COLORS)
    st.plotly_chart(style_fig(fig, 420), width="stretch", config=PLOT_CONFIG)

    with st.expander("View raw live data"):
        st.dataframe(df.sort_values("timestamp", ascending=False), width="stretch", height=300)


# ------------------------------------------------------------- history view
def display_historical_view(config):
    st.header("Historical Data")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        time_range = st.selectbox("Time range", list(TIME_RANGES), index=1,
                                  format_func=lambda x: "All time" if x == "all" else f"Last {x}")
    with c2:
        metric = st.selectbox("Metric", ["all"] + METRICS, format_func=metric_label, key="hist_metric")
    with c3:
        sensors = st.multiselect("Locations", list_sensors(config["mongo_uri"], config["mongo_db"]),
                                 placeholder="All locations")
    with c4:
        aggregation = st.selectbox("Aggregation", ["raw", "hourly", "daily", "weekly"],
                                   format_func=str.capitalize)

    df = query_historical_data(config, time_range, metric, sensors, aggregation)
    if df is None:
        return
    if df.empty:
        st.info("No data for this selection yet. Is spark_stream.py running?")
        return

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Rows", f"{len(df):,}")
    k2.metric("Locations", df["sensor_id"].nunique())
    k3.metric("From", df["timestamp"].min().strftime("%Y-%m-%d %H:%M"))
    k4.metric("To", df["timestamp"].max().strftime("%Y-%m-%d %H:%M"))

    st.subheader("Historical trend")
    if aggregation == "raw" and len(df) > 20000:
        st.warning("Many raw points, so the chart may be slow. Try hourly or daily aggregation.")
    n_metrics = df["metric_type"].nunique()
    height = 400 if n_metrics == 1 else 270 * n_metrics
    title = f"{metric_label(metric)}, {aggregation} ({'all time' if time_range == 'all' else 'last ' + time_range})"
    fig = px.line(df, x="timestamp", y="value", color="location",
                  facet_row="metric_type" if n_metrics > 1 else None,
                  title=title, labels={"value": "Value", "timestamp": "Time", "location": "Location"},
                  color_discrete_sequence=COLORS)
    fig.update_yaxes(matches=None)
    style_fig(fig, height)
    st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG)
    png_download_button(fig, f"marine_chart_{aggregation}_{time_range}.png", "png_hist", height)

    st.subheader("Summary statistics")
    summary = (df.groupby(["location", "metric_type"])["value"]
                 .agg(["count", "mean", "min", "max", "std"]).round(2).reset_index())
    summary["metric_type"] = summary["metric_type"].map(metric_label)
    summary = summary.rename(columns={"location": "Location", "metric_type": "Metric", "count": "Count",
                                      "mean": "Mean", "min": "Min", "max": "Max", "std": "Std dev"})
    st.dataframe(summary, width="stretch", hide_index=True)

    with st.expander("Spark windowed aggregates (10-minute windows)"):
        sdf = query_spark_aggregates(config, time_range, metric)
        if sdf.empty:
            st.info("No Spark aggregates yet.")
        else:
            st.dataframe(sdf, width="stretch", hide_index=True, height=300)

    st.subheader("Data table")
    st.dataframe(df, width="stretch", hide_index=True, height=350)
    st.download_button("Download data (CSV)", df.to_csv(index=False).encode("utf-8"),
                       file_name=f"marine_history_{aggregation}_{time_range}.csv", mime="text/csv")


# ---------------------------------------------------------------------- main
def main():
    st.title("Marine Weather Dashboard")

    if "refresh_state" not in st.session_state:
        st.session_state.refresh_state = {"last_refresh": datetime.now(), "auto_refresh": True}

    config = setup_sidebar()

    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state["auto_refresh"] = st.sidebar.checkbox(
        "Enable Auto Refresh", value=st.session_state.refresh_state["auto_refresh"])
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", min_value=5, max_value=900,
                                         value=DEFAULT_REFRESH_SECONDS, step=5)
    if st.session_state.refresh_state["auto_refresh"]:
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    if st.sidebar.button("Manual Refresh"):
        st.rerun()

    st.session_state.refresh_state["last_refresh"] = datetime.now()
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state["last_refresh"].strftime("%H:%M:%S"))

    tab1, tab2 = st.tabs(["Real-time Streaming", "Historical Data"])
    with tab1:
        display_real_time_view(config, refresh_interval)
    with tab2:
        display_historical_view(config)


if __name__ == "__main__":
    main()