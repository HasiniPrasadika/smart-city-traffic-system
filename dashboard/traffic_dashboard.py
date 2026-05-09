import glob
import json
import os

import pandas as pd
import streamlit as st

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPORT_DIR = os.path.join(BASE_DIR, "reports")
ALERT_DIR = os.path.join(BASE_DIR, "output", "critical_alerts")
PEAK_REPORT_PATH = os.path.join(REPORT_DIR, "daily_peak_report.csv")
HOURLY_SUMMARY_PATH = os.path.join(REPORT_DIR, "hourly_summary.csv")
CHART_PATH = os.path.join(REPORT_DIR, "traffic_volume_chart.png")


def load_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


def load_alerts():
    records = []
    for path in glob.glob(os.path.join(ALERT_DIR, "*.json")):
        if os.path.getsize(path) == 0:
            continue
        with open(path, "r", encoding="utf-8") as alert_file:
            for line in alert_file:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return pd.DataFrame(records)


st.set_page_config(page_title="Smart City Traffic Dashboard", layout="wide")
st.title("Smart City Traffic & Congestion Dashboard")

peak_report = load_csv(PEAK_REPORT_PATH)
hourly_summary = load_csv(HOURLY_SUMMARY_PATH)
alerts = load_alerts()

metric_cols = st.columns(4)
metric_cols[0].metric("Monitored Junctions", 4)
metric_cols[1].metric("Peak Report Rows", len(peak_report))
metric_cols[2].metric("Hourly Summary Rows", len(hourly_summary))
metric_cols[3].metric("Critical Alerts", len(alerts))

st.subheader("Peak Congestion Report")
if peak_report.empty:
    st.info("Run the daily report generator first.")
else:
    st.dataframe(peak_report, use_container_width=True)

st.subheader("Hourly Congestion Index")
if hourly_summary.empty:
    st.info("No hourly summary found.")
else:
    selected_junction = st.selectbox(
        "Junction",
        sorted(hourly_summary["junction_name"].unique()),
    )
    filtered_summary = hourly_summary[hourly_summary["junction_name"] == selected_junction]
    st.line_chart(
        filtered_summary.set_index("hour")[["total_vehicle_count", "congestion_index"]]
    )

st.subheader("Traffic Volume Chart")
if os.path.exists(CHART_PATH):
    st.image(CHART_PATH)
else:
    st.info("No chart found.")

st.subheader("Critical Alert Records")
if alerts.empty:
    st.info("No critical alerts found. Run Spark streaming with the producer.")
else:
    display_columns = [
        column for column in [
            "sensor_id",
            "junction_name",
            "timestamp",
            "vehicle_count",
            "avg_speed",
            "congestion_severity",
            "weather",
            "road_condition",
        ]
        if column in alerts.columns
    ]
    st.dataframe(alerts[display_columns], use_container_width=True)
