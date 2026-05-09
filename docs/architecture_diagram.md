# Smart City Traffic & Congestion System Architecture

```mermaid
flowchart TD
    A[Python Traffic Sensor Producer] -->|JSON events every second| B[Kafka Topic: traffic-data]
    B --> C[Spark Structured Streaming]
    C --> D[Window Aggregation<br/>1-minute tumbling windows]
    C --> E[Critical Alert Filter<br/>avg_speed below 10 km/h]
    D --> F[Processed Traffic Metrics]
    E --> G[output/critical_alerts JSON]
    F --> H[Stored Traffic Data<br/>CSV / JSON / Parquet]
    H --> I[Airflow Daily DAG]
    I --> J[Peak Hour Report CSV]
    I --> K[Traffic Volume vs Time Chart]
```

## Components

- **Producer:** `producer/traffic_producer.py` simulates four Colombo junction sensors and publishes JSON records to Kafka.
- **Kafka:** `traffic-data` is the event-streaming topic used between ingestion and stream processing.
- **Spark Structured Streaming:** `spark/traffic_stream_processor.py` consumes Kafka records, parses JSON, detects congestion, and performs windowed aggregation.
- **Alert Storage:** critical alerts are written to `output/critical_alerts` as JSON.
- **Airflow DAG:** `airflow/dags/daily_traffic_report.py` schedules the daily analytical report.
- **Reports:** `reports/daily_peak_report.csv` and `reports/traffic_volume_chart.png` are generated from stored traffic data.

