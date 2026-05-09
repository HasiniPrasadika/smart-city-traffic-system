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
```

## Run Kafka

```bash
docker compose up -d
docker exec -it kafka kafka-topics --create --topic traffic-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

## Run Producer

```bash
python3 -m pip install kafka-python
python3 producer/traffic_producer.py
```

Check Kafka messages:

```bash
docker exec -it kafka kafka-console-consumer --topic traffic-data --bootstrap-server localhost:9092 --from-beginning
```

## Run Spark Streaming

```bash
python3 -m pip install pyspark
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark/traffic_stream_processor.py
```

Critical alerts are saved to `output/critical_alerts`.

## Generate Daily Report Locally

```bash
python3 -m pip install pandas matplotlib
python3 scripts/generate_daily_traffic_report.py
```

Outputs:

```text
reports/daily_peak_report.csv
reports/traffic_volume_chart.png
```

## Run Airflow DAG

Install Airflow using the official constraint file for your Python version, then place this repository on Airflow's DAG path or set `AIRFLOW__CORE__DAGS_FOLDER` to `airflow/dags`.

```bash
airflow db init
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
airflow webserver --port 8080
airflow scheduler
```

Open `http://localhost:8080`, enable `daily_smart_city_traffic_report`, and trigger the DAG.

