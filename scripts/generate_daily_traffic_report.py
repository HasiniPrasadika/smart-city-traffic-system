import os

import matplotlib.pyplot as plt
import pandas as pd


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_PATH = os.path.join(BASE_DIR, "data", "traffic_data.csv")
REPORT_DIR = os.path.join(BASE_DIR, "reports")


def generate_daily_traffic_report(data_path=DATA_PATH, report_dir=REPORT_DIR):
    os.makedirs(report_dir, exist_ok=True)

    df = pd.read_csv(data_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"] = df["timestamp"].dt.hour

    hourly_summary = df.groupby(
        ["sensor_id", "junction_name", "hour"], as_index=False
    ).agg(
        total_vehicle_count=("vehicle_count", "sum"),
        avg_speed=("avg_speed", "mean"),
    )

    hourly_summary["congestion_index"] = (
        hourly_summary["total_vehicle_count"] / hourly_summary["avg_speed"]
    ).round(2)

    peak_report = hourly_summary.loc[
        hourly_summary.groupby("sensor_id")["congestion_index"].idxmax()
    ].copy()

    peak_report["avg_speed"] = peak_report["avg_speed"].round(2)
    peak_report["recommendation"] = peak_report.apply(
        lambda row: "Traffic police intervention required"
        if row["avg_speed"] < 10 or row["congestion_index"] > 15
        else "Monitor traffic condition",
        axis=1,
    )

    report_path = os.path.join(report_dir, "daily_peak_report.csv")
    peak_report.to_csv(report_path, index=False)

    chart_data = df.groupby("hour", as_index=False)["vehicle_count"].sum()

    plt.figure(figsize=(10, 6))
    plt.plot(chart_data["hour"], chart_data["vehicle_count"], marker="o")
    plt.xlabel("Time of Day Hour")
    plt.ylabel("Traffic Volume")
    plt.title("Traffic Volume vs Time of Day")
    plt.grid(True)
    plt.xticks(chart_data["hour"])
    plt.tight_layout()

    chart_path = os.path.join(report_dir, "traffic_volume_chart.png")
    plt.savefig(chart_path)
    plt.close()

    print("Daily traffic report generated successfully.")
    print("Report saved at:", report_path)
    print("Chart saved at:", chart_path)

    return report_path, chart_path


if __name__ == "__main__":
    generate_daily_traffic_report()
