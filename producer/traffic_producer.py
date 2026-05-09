import json
import os
import random
import time
from datetime import datetime
from kafka import KafkaProducer

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic-data")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
PRODUCER_INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)

junctions = [
    {"sensor_id": "J001", "junction_name": "Borella Junction", "road_type": "arterial"},
    {"sensor_id": "J002", "junction_name": "Town Hall Junction", "road_type": "central_business"},
    {"sensor_id": "J003", "junction_name": "Maradana Junction", "road_type": "railway_access"},
    {"sensor_id": "J004", "junction_name": "Nugegoda Junction", "road_type": "suburban"}
]


def classify_severity(avg_speed):
    if avg_speed < 10:
        return "CRITICAL"
    if avg_speed < 15:
        return "HIGH"
    if avg_speed < 30:
        return "MODERATE"
    return "LOW"


def generate_traffic_data(junction):
    current_hour = datetime.now().hour
    is_peak_hour = current_hour in [7, 8, 9, 16, 17, 18]
    weather = random.choices(
        ["CLEAR", "LIGHT_RAIN", "HEAVY_RAIN"],
        weights=[0.70, 0.20, 0.10],
        k=1
    )[0]
    road_condition = "WET" if weather in ["LIGHT_RAIN", "HEAVY_RAIN"] else "DRY"
    is_school_time = current_hour in [6, 7, 12, 13]

    critical_chance = 0.18 if is_peak_hour else 0.08
    if weather == "HEAVY_RAIN":
        critical_chance += 0.08

    is_critical = random.random() < critical_chance

    if is_critical:
        vehicle_count = random.randint(70, 120)
        avg_speed = round(random.uniform(3, 9), 2)
    elif is_peak_hour:
        vehicle_count = random.randint(45, 90)
        avg_speed = round(random.uniform(10, 28), 2)
    else:
        vehicle_count = random.randint(10, 60)
        avg_speed = round(random.uniform(20, 60), 2)

    congestion_severity = classify_severity(avg_speed)
    traffic_status = "CRITICAL" if congestion_severity == "CRITICAL" else "NORMAL"

    return {
        "sensor_id": junction["sensor_id"],
        "junction_name": junction["junction_name"],
        "road_type": junction["road_type"],
        "timestamp": datetime.now().isoformat(),
        "vehicle_count": vehicle_count,
        "avg_speed": avg_speed,
        "traffic_status": traffic_status,
        "congestion_severity": congestion_severity,
        "weather": weather,
        "road_condition": road_condition,
        "is_peak_hour": is_peak_hour,
        "is_school_time": is_school_time
    }

print("Traffic sensor producer started...")
print("Sending data to Kafka topic:", KAFKA_TOPIC)

try:
    while True:
        for junction in junctions:
            data = generate_traffic_data(junction)

            producer.send(KAFKA_TOPIC, value=data)
            producer.flush()

            print(
                f"{data['timestamp']} | "
                f"{data['sensor_id']} | "
                f"{data['junction_name']} | "
                f"Vehicles: {data['vehicle_count']} | "
                f"Avg Speed: {data['avg_speed']} km/h | "
                f"Severity: {data['congestion_severity']} | "
                f"Weather: {data['weather']} | "
                f"Status: {data['traffic_status']}"
            )

        time.sleep(PRODUCER_INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("Producer stopped.")
    producer.close()
