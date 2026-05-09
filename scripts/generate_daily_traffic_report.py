import os

import matplotlib.pyplot as plt
import pandas as pd

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_PATH = os.path.join(BASE_DIR, "data", "traffic_data.csv")
REPORT_DIR = os.path.join(BASE_DIR, "reports")
HOURLY_SUMMARY_PATH = os.path.join(REPORT_DIR, "hourly_summary.csv")
PEAK_REPORT_PATH = os.path.join(REPORT_DIR, "daily_peak_report.csv")
CHART_PATH = os.path.join(REPORT_DIR, "traffic_volume_chart.png")

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_HOST_PORT", os.getenv("POSTGRES_PORT", "55432"))),
    "dbname": os.getenv("POSTGRES_DB", "traffic_db"),
    "user": os.getenv("POSTGRES_USER", "traffic_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "traffic_pass"),
}


def validate_input_data(data_path=DATA_PATH):
    df = pd.read_csv(data_path)
    required_columns = {
        "sensor_id",
        "junction_name",
        "timestamp",
        "vehicle_count",
        "avg_speed",
    }
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    valid_sensor_ids = {"J001", "J002", "J003", "J004"}

    invalid_rows = df[
        df["timestamp"].isna()
        | ~df["sensor_id"].isin(valid_sensor_ids)
        | (df["vehicle_count"] < 0)
        | (df["avg_speed"] < 0)
        | (df["avg_speed"] > 120)
    ]

    if not invalid_rows.empty:
        bad_record_path = os.path.join(REPORT_DIR, "bad_records.csv")
        os.makedirs(REPORT_DIR, exist_ok=True)
        invalid_rows.to_csv(bad_record_path, index=False)
        raise ValueError(f"Input validation failed. Bad records saved to {bad_record_path}")

    print(f"Validated {len(df)} traffic records from {data_path}")
    return data_path


def generate_hourly_summary(data_path=DATA_PATH, report_dir=REPORT_DIR):
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

    hourly_summary["avg_speed"] = hourly_summary["avg_speed"].round(2)
    hourly_summary["congestion_index"] = (
        hourly_summary["total_vehicle_count"] / hourly_summary["avg_speed"]
    ).round(2)

    hourly_summary.to_csv(HOURLY_SUMMARY_PATH, index=False)
    print("Hourly summary saved at:", HOURLY_SUMMARY_PATH)
    return HOURLY_SUMMARY_PATH


def generate_peak_report(hourly_summary_path=HOURLY_SUMMARY_PATH, report_dir=REPORT_DIR):
    os.makedirs(report_dir, exist_ok=True)

    hourly_summary = pd.read_csv(hourly_summary_path)
    peak_report = hourly_summary.loc[
        hourly_summary.groupby("sensor_id")["congestion_index"].idxmax()
    ].copy()

    peak_report["recommendation"] = peak_report.apply(
        lambda row: "Traffic police intervention required"
        if row["avg_speed"] < 10 or row["congestion_index"] > 15
        else "Monitor traffic condition",
        axis=1,
    )

    peak_report.to_csv(PEAK_REPORT_PATH, index=False)
    print("Peak traffic report saved at:", PEAK_REPORT_PATH)
    return PEAK_REPORT_PATH


def generate_traffic_volume_chart(data_path=DATA_PATH, report_dir=REPORT_DIR):
    os.makedirs(report_dir, exist_ok=True)

    df = pd.read_csv(data_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"] = df["timestamp"].dt.hour
    chart_data = df.groupby("hour", as_index=False)["vehicle_count"].sum()

    plt.figure(figsize=(10, 6))
    plt.plot(chart_data["hour"], chart_data["vehicle_count"], marker="o")
    plt.xlabel("Time of Day Hour")
    plt.ylabel("Traffic Volume")
    plt.title("Traffic Volume vs Time of Day")
    plt.grid(True)
    plt.xticks(chart_data["hour"])
    plt.tight_layout()
    plt.savefig(CHART_PATH)
    plt.close()

    print("Traffic volume chart saved at:", CHART_PATH)
    return CHART_PATH


def load_reports_to_postgres(
    hourly_summary_path=HOURLY_SUMMARY_PATH,
    peak_report_path=PEAK_REPORT_PATH,
):
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError as exc:
        raise RuntimeError("Install psycopg2-binary to load reports into PostgreSQL") from exc

    hourly_summary = pd.read_csv(hourly_summary_path)
    peak_report = pd.read_csv(peak_report_path)

    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE hourly_summary, daily_peak_report;")
            execute_values(
                cur,
                """
                INSERT INTO hourly_summary
                    (sensor_id, junction_name, hour, total_vehicle_count, avg_speed, congestion_index)
                VALUES %s
                """,
                list(hourly_summary.itertuples(index=False, name=None)),
            )
            execute_values(
                cur,
                """
                INSERT INTO daily_peak_report
                    (sensor_id, junction_name, hour, total_vehicle_count, avg_speed, congestion_index, recommendation)
                VALUES %s
                """,
                list(peak_report.itertuples(index=False, name=None)),
            )

    print("Hourly summary and peak report loaded into PostgreSQL.")
    return "postgres_load_complete"


def generate_daily_traffic_report(data_path=DATA_PATH, report_dir=REPORT_DIR):
    validate_input_data(data_path)
    hourly_summary_path = generate_hourly_summary(data_path, report_dir)
    peak_report_path = generate_peak_report(hourly_summary_path, report_dir)
    chart_path = generate_traffic_volume_chart(data_path, report_dir)

    print("Daily traffic report generated successfully.")
    print("Report saved at:", peak_report_path)
    print("Chart saved at:", chart_path)

    return peak_report_path, chart_path


if __name__ == "__main__":
    generate_daily_traffic_report()
