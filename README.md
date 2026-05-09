# Smart City Traffic & Congestion System

Big Data pipeline for a Colombo smart city traffic scenario using Python, Kafka, Spark Structured Streaming, Airflow, and file-based reporting outputs.

## Project Structure

```text
producer/traffic_producer.py              Kafka traffic sensor simulator
spark/traffic_stream_processor.py         Spark streaming processor and alert detector
airflow/dags/daily_traffic_report.py      Airflow DAG for daily reporting
scripts/generate_daily_traffic_report.py  Local report generator used by the DAG
data/traffic_data.csv                     Sample stored traffic data
reports/                                  Generated CSV and chart outputs
docs/architecture_diagram.md              Mermaid architecture diagram
docs/analytical_report.md                 Analytical report
docs/project_report.md                    Project report
dashboard/traffic_dashboard.py            Streamlit dashboard
postgres/init.sql                         PostgreSQL schema
```

## Python Environment

This project has a local Python virtual environment in `.venv`.

Activate it before running manual commands:

```bash
source .venv/bin/activate
```

The project also has `.env` for local command settings. The shareable template is `.env.example`.

## Run Kafka

```bash
docker compose up -d
docker exec -it kafka kafka-topics --create --topic traffic-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

This also starts PostgreSQL using `postgres:14.20-trixie`.

PostgreSQL connection:

```text
host: localhost
host port: 55432
container port: 5432
database: traffic_db
user: traffic_user
password: traffic_pass
```

From the host machine, PostgreSQL is exposed on port `55432` to avoid conflicts with local PostgreSQL installs:

```bash
PGPASSWORD=traffic_pass psql -h localhost -p 55432 -U traffic_user -d traffic_db
```

## Run Producer

```bash
.venv/bin/python producer/traffic_producer.py
```

Check Kafka messages:

```bash
docker exec -it kafka kafka-console-consumer --topic traffic-data --bootstrap-server localhost:9092 --from-beginning
```

## Run Spark Streaming

```bash
.venv/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark/traffic_stream_processor.py
```

Critical alerts are saved to `output/critical_alerts`.

To also write streaming critical alerts to PostgreSQL, include the PostgreSQL JDBC driver and enable the sink:

```bash
POSTGRES_ENABLED=true .venv/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  spark/traffic_stream_processor.py
```

## Generate Daily Report Locally

```bash
.venv/bin/python scripts/generate_daily_traffic_report.py
```

Outputs:

```text
reports/hourly_summary.csv
reports/daily_peak_report.csv
reports/traffic_volume_chart.png
```

The Airflow DAG also loads `hourly_summary` and `daily_peak_report` into PostgreSQL.

## Run Dashboard

```bash
.venv/bin/streamlit run dashboard/traffic_dashboard.py
```

The dashboard shows peak congestion results, hourly congestion trends, the traffic volume chart, and saved critical alert records.

## Run Airflow DAG

Install Airflow using the official constraint file for your Python version, then place this repository on Airflow's DAG path or set `AIRFLOW__CORE__DAGS_FOLDER` to `airflow/dags`.

```bash
.venv/bin/airflow db migrate
.venv/bin/airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
.venv/bin/airflow webserver --port 8080
.venv/bin/airflow scheduler
```

Open `http://localhost:8080`, enable `daily_smart_city_traffic_report`, and trigger the DAG.
