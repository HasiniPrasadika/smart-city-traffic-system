import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_TOPIC = "traffic-data"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)

junctions = [
    {"sensor_id": "J001", "junction_name": "Borella Junction"},
    {"sensor_id": "J002", "junction_name": "Town Hall Junction"},
    {"sensor_id": "J003", "junction_name": "Maradana Junction"},
    {"sensor_id": "J004", "junction_name": "Nugegoda Junction"}
]

def generate_traffic_data(junction):
    # 10% chance to generate critical traffic data
    is_critical = random.random() < 0.10

    if is_critical:
        vehicle_count = random.randint(70, 120)
        avg_speed = round(random.uniform(3, 9), 2)
        traffic_status = "CRITICAL"
    else:
        vehicle_count = random.randint(10, 60)
        avg_speed = round(random.uniform(20, 60), 2)
        traffic_status = "NORMAL"

    return {
        "sensor_id": junction["sensor_id"],
        "junction_name": junction["junction_name"],
        "timestamp": datetime.now().isoformat(),
        "vehicle_count": vehicle_count,
        "avg_speed": avg_speed,
        "traffic_status": traffic_status
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
                f"Status: {data['traffic_status']}"
            )

        time.sleep(1)

except KeyboardInterrupt:
    print("Producer stopped.")
    producer.close()