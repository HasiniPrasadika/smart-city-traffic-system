from datetime import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from scripts.generate_daily_traffic_report import (
    generate_hourly_summary,
    generate_peak_report,
    generate_traffic_volume_chart,
    load_reports_to_postgres,
    validate_input_data,
)


default_args = {
    "owner": "smart_city_group",
    "start_date": datetime(2026, 4, 24),
    "retries": 1,
}


with DAG(
    dag_id="daily_smart_city_traffic_report",
    default_args=default_args,
    description="Generate daily peak traffic hour report for smart city traffic system",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    validate_input_task = PythonOperator(
        task_id="validate_input_data",
        python_callable=validate_input_data,
    )

    hourly_summary_task = PythonOperator(
        task_id="generate_hourly_summary",
        python_callable=generate_hourly_summary,
    )

    peak_report_task = PythonOperator(
        task_id="generate_peak_report",
        python_callable=generate_peak_report,
    )

    chart_task = PythonOperator(
        task_id="generate_traffic_volume_chart",
        python_callable=generate_traffic_volume_chart,
    )

    postgres_load_task = PythonOperator(
        task_id="load_reports_to_postgres",
        python_callable=load_reports_to_postgres,
    )

    validate_input_task >> hourly_summary_task
    hourly_summary_task >> [peak_report_task, chart_task]
    [peak_report_task, chart_task] >> postgres_load_task
