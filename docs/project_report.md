# Smart City Traffic & Congestion System Project Report

## Introduction

Urban traffic congestion is a major operational problem in Colombo. Busy junctions such as Borella, Town Hall, Maradana, and Nugegoda experience large changes in vehicle flow during school hours, office travel periods, shopping peaks, and evening return trips. A smart city traffic platform can help traffic managers observe these changes in near real time and make quicker decisions. This project implements a Big Data pipeline that simulates traffic sensors, ingests traffic events using Apache Kafka, processes live streams with Apache Spark Structured Streaming, stores congestion alerts, and generates a daily analytical report through Apache Airflow.

The system is based on four virtual traffic sensors. Each sensor sends a JSON message containing a sensor ID, junction name, timestamp, vehicle count, average speed, and traffic status. The producer generates one reading per junction every second. Normal traffic readings contain moderate vehicle counts and speeds, while controlled critical readings contain high vehicle counts and average speeds below 10 km/h. This makes the pipeline suitable for demonstrating real-time congestion detection.

## System Objectives

The main objective is to build an end-to-end data pipeline for a Smart City Traffic and Congestion System. The system must generate mock sensor data, publish that data into Kafka, process events as a live stream, detect congestion when average speed falls below 10 km/h, store critical alerts, and create a daily report showing peak traffic hours and traffic volume over time. The project also demonstrates the roles of ingestion, stream processing, batch orchestration, and analytics in a Big Data architecture.

The pipeline is intentionally simple enough to run on a laptop, but it follows the same structure as a production system. Kafka acts as the event backbone, Spark handles real-time processing and windowing, and Airflow manages scheduled reporting. CSV and JSON files are used as lightweight storage outputs for the assignment, but the same design can be extended to PostgreSQL, Parquet, or a data lake.

## Architecture

The architecture begins with `producer/traffic_producer.py`, a Python program that simulates four traffic sensors. The producer publishes records to the Kafka topic named `traffic-data`. Kafka decouples the producer from downstream processing, allowing traffic events to be consumed continuously without directly connecting the sensor program to the analytics logic.

Spark Structured Streaming reads from the Kafka topic and parses the Kafka value field as JSON. The parsed data is converted into typed columns such as `vehicle_count`, `avg_speed`, and `timestamp`. Spark then creates an `event_time` column from the original sensor timestamp. This distinction is important because traffic analysis should be based on when the traffic event occurred, not only when the processing engine received it.

The stream processing layer has two responsibilities. First, it filters records where `avg_speed` is below 10 km/h. These records are treated as critical congestion alerts and written to the `output/critical_alerts` folder as JSON. Second, Spark groups traffic records into one-minute tumbling windows by junction. Within each window, it calculates total vehicle count, average speed, and a congestion index.

The batch layer is implemented with an Airflow DAG in `airflow/dags/daily_traffic_report.py`. The DAG runs the shared reporting logic from `scripts/generate_daily_traffic_report.py`. It reads stored traffic data from `data/traffic_data.csv`, calculates hourly summaries, identifies the peak congestion hour for each junction, writes `reports/daily_peak_report.csv`, and creates the chart `reports/traffic_volume_chart.png`.

## Data Ingestion

The ingestion layer uses a Python Kafka producer. Four virtual sensors represent Borella Junction, Town Hall Junction, Maradana Junction, and Nugegoda Junction. Each loop generates one record per junction and sends it to Kafka. The output is JSON, which is widely used in event-driven systems because it is readable, flexible, and easy to parse in Spark.

Kafka was selected because it is suitable for high-throughput, real-time event streaming. In a real smart city deployment, many sensors could publish to the same Kafka cluster, while multiple consumers could read the same events for alerting, dashboards, historical storage, or machine learning. In this project, Kafka provides the ingestion boundary between the simulated traffic sensors and the stream processing layer.

The producer also creates occasional critical records. These records have higher vehicle counts and speeds below 10 km/h. This supports reliable testing because the Spark processor does not need to wait for random real-world congestion. The generated critical records prove that the alerting logic can detect low-speed traffic events.

## Stream Processing

The stream processing layer is implemented using Apache Spark Structured Streaming. Spark reads continuously from the Kafka topic `traffic-data`, casts the Kafka message value to a string, and parses it using a defined schema. The schema contains the expected fields from the producer: `sensor_id`, `junction_name`, `timestamp`, `vehicle_count`, `avg_speed`, and `traffic_status`.

Critical alert detection is performed by filtering records where average speed is below 10 km/h. These events are written to the console for live demonstration and also saved as JSON files under `output/critical_alerts`. This gives the system both visible real-time output and a stored record that can be reviewed later.

Spark also performs window-based aggregation. The implementation uses a one-minute tumbling window for demonstration, although a five-minute window would be suitable for a real deployment. For each sensor and window, Spark calculates total vehicle count and average speed. It then calculates congestion index as total vehicle count divided by average speed. A high vehicle count combined with low speed creates a high congestion index, which indicates severe congestion.

Event time is used for windowing. Event time is the timestamp generated by the traffic sensor, while processing time is the time when Spark processes the record. Event-time processing is more accurate for traffic analytics because records should be grouped according to when congestion actually occurred. Spark watermarking is included to handle slightly delayed records.

## Batch Processing and Reporting

The batch processing layer is orchestrated using Apache Airflow. Airflow is appropriate for this project because it schedules recurring data jobs, tracks execution status, and provides a UI for monitoring success or failure. The DAG `daily_smart_city_traffic_report` is designed to run once per day.

The report job reads stored traffic records from CSV and converts timestamps into hourly values. It groups records by sensor, junction, and hour, then calculates total vehicle count and average speed. A congestion index is calculated for every junction-hour group. The highest congestion index per junction is selected as that junction's peak traffic hour.

The final CSV report includes a recommendation field. If average speed is below 10 km/h or the congestion index is greater than 15, the report recommends traffic police intervention. Otherwise, it recommends monitoring the condition. The chart output shows traffic volume against time of day, giving a quick visual summary of when the city-wide traffic load is highest.

## Results

Using the sample data, Borella Junction has peak congestion around 08:00, Town Hall Junction around 09:00, Maradana Junction around 17:00, and Nugegoda Junction around 18:00. These periods match common urban traffic patterns: morning movement toward offices and schools, followed by evening return trips. The calculated congestion indexes for the peak hours are high enough to trigger intervention recommendations.

The generated outputs are `reports/daily_peak_report.csv` and `reports/traffic_volume_chart.png`. The CSV provides operational details per junction, while the chart gives a broader view of traffic volume over the day. Together, these outputs satisfy the assignment requirement for an analytical report showing peak traffic hour and traffic volume versus time.

## Limitations and Future Improvements

The current implementation is designed as an assignment prototype, so it uses simulated data and local file outputs. A production version would need more durable storage, stronger monitoring, and a larger data model. PostgreSQL could be used for structured reporting tables, while Parquet files in object storage could support long-term historical analysis. A dashboard could also be added so traffic officers can see current congestion levels without reading console logs or CSV files.

The congestion index is intentionally simple. It is useful for explaining the relationship between vehicle count and speed, but real traffic control would benefit from more features. Future versions could include road capacity, lane count, incident reports, weather, public events, school calendars, and bus movement data. Machine learning models could then predict congestion before it becomes critical, allowing traffic managers to adjust signal timing or deploy officers earlier.

## Ethics and Data Governance

Smart city traffic systems can improve road efficiency, but they can also raise privacy concerns. If cameras, vehicle registration numbers, GPS traces, or personal movement data are collected, citizens may be monitored without consent. This project avoids personally identifiable information and uses only anonymous sensor-level data such as vehicle count and average speed.

A real deployment should include access control, secure storage, data minimization, and clear retention rules. Only authorized traffic management staff should access reports and alerts. Historical data should be used for congestion management, transport planning, and signal optimization, not for tracking individuals.

## Conclusion

This project demonstrates a complete Big Data traffic pipeline using Python, Kafka, Spark Structured Streaming, file-based storage, Airflow, and reporting outputs. The ingestion layer produces real-time JSON traffic events, Kafka transports the events, Spark detects congestion and calculates windowed metrics, and Airflow generates daily peak-hour reports. The system meets the core requirements of the assignment and provides a practical foundation that can be extended with more sensors, dashboards, predictive models, PostgreSQL storage, or real traffic data sources.
