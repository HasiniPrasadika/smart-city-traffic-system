from datetime import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from scripts.generate_daily_traffic_report import generate_daily_traffic_report


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
    generate_report_task = PythonOperator(
        task_id="generate_daily_traffic_report",
        python_callable=generate_daily_traffic_report,
    )
