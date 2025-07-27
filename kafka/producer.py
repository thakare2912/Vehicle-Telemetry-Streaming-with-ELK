import json
import time
import random
import logging
from confluent_kafka import Producer

# --------------------
# Configure Logging
# --------------------
logging.basicConfig(
    filename="telemetry_producer.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --------------------
# Kafka Configuration
# --------------------
producer_conf = {
    'bootstrap.servers': 'localhost:9092'


  # Use 'localhost:9092' if not in Docker
}
producer = Producer(producer_conf)

# --------------------
# Sample Data Config
# --------------------
vehicle_types = {
    "SUV": ["Innova", "XUV700", "Creta"],
    "Sedan": ["City", "Verna", "Ciaz"],
    "Truck": ["LPT 1618", "Ace", "Bolero Maxx"],
    "Hatchback": ["i20", "Tiago", "Baleno"]
}

manufacturers = ["Toyota", "Tata", "Hyundai", "Mahindra", "Ford"]
user_names = ["Ravi Kumar", "Priya Shah", "John Singh", "Amit Verma", "Sneha Joshi"]
number_prefixes = ["MH12", "DL08", "KA05", "GJ18", "TN22"]
print(" Producer script started...")
# --------------------
# Vehicle Profile Generator
# --------------------
def generate_vehicle_profiles(n=10):
    profiles = []
    for i in range(1, n+1):
        vtype = random.choice(list(vehicle_types.keys()))
        profile = {
            "vehicle_id": f"VHC-{i:03d}",
            "user_name": random.choice(user_names),
            "vehicle_number": f"{random.choice(number_prefixes)}{random.randint(1000, 9999)}",
            "vehicle_type": vtype,
            "manufacturer": random.choice(manufacturers),
            "model": random.choice(vehicle_types[vtype]),
            "year": random.randint(2015, 2023),
            "start_count": random.randint(1, 10)
            
        }
        profiles.append(profile)
    return profiles



vehicle_profiles = generate_vehicle_profiles(10)

def generate_tire_quality():
    return {
        "front_left": round(random.uniform(70, 100), 2),
        "front_right": round(random.uniform(70, 100), 2),
        "rear_left": round(random.uniform(70, 100), 2),
        "rear_right": round(random.uniform(70, 100), 2)
    }

# --------------------
# Kafka Callback
# --------------------
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Delivery failed: {err}")
    else:
        logging.info(f"Sent to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# --------------------
# Send Messages
# --------------------
logging.info(" Starting vehicle telemetry producer...")

while True:
    vehicle = random.choice(vehicle_profiles)

    telemetry_data = {
        **vehicle,
        "speed": round(random.uniform(0, 140), 2),
        "engine_quality": round(random.uniform(60, 100), 2),
        "fuel_level": round(random.uniform(10, 100), 2),
        "tire_quality": generate_tire_quality(),
        "lat": round(random.uniform(12.900000, 13.000000), 6),
        "lon": round(random.uniform(77.500000, 77.600000), 6),
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S')
    }
    print("Sent:", telemetry_data)

    json_data = json.dumps(telemetry_data).encode('utf-8')
    producer.produce("vehicle_telemetry", value=json_data, callback=delivery_report)
    producer.poll(0)
    time.sleep(1)

