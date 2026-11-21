#!/usr/bin/env python3
'''
FlowPulse AI - Greater Bengaluru IoT & GPS Kafka Telemetry Producer
Automatically streams to Apache Kafka (localhost:9092) if a broker is running,
or cleanly falls back to VS Code Console Streaming mode with zero errors.
'''
import json
import random
import time
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
    KAFKA_LIB_INSTALLED = True
except ImportError:
    KAFKA_LIB_INSTALLED = False

BENGALURU_HUBS = [
    {"code": "BLR-CE-01", "name": "MG Road - Trinity Circle", "zone": "Central", "base_speed": 34.0, "base_density": 88},
    {"code": "BLR-NO-05", "name": "Hebbal Flyover Interchange", "zone": "North", "base_speed": 28.0, "base_density": 135},
    {"code": "BLR-NO-07", "name": "Manyata Tech Park - Nagawara", "zone": "North", "base_speed": 26.0, "base_density": 142},
    {"code": "BLR-EA-10", "name": "KR Puram Hanging Bridge", "zone": "East IT", "base_speed": 29.1, "base_density": 145},
    {"code": "BLR-EA-11", "name": "Marathahalli Bridge Junction", "zone": "East IT", "base_speed": 22.0, "base_density": 156},
    {"code": "BLR-EA-12", "name": "Whitefield - ITPL Main Gate", "zone": "East IT", "base_speed": 27.0, "base_density": 124},
    {"code": "BLR-EA-13", "name": "Bellandur - Ecospace ORR", "zone": "East IT", "base_speed": 14.0, "base_density": 168},
    {"code": "BLR-SO-17", "name": "Central Silk Board Junction", "zone": "South", "base_speed": 18.0, "base_density": 174},
    {"code": "BLR-SO-18", "name": "Electronic City Phase 1 Toll", "zone": "South", "base_speed": 44.0, "base_density": 76},
    {"code": "BLR-WE-25", "name": "Yeshwantpur Circle", "zone": "West", "base_speed": 35.0, "base_density": 112},
    {"code": "BLR-WE-26", "name": "Rajajinagar - Navrang Junction", "zone": "West", "base_speed": 42.0, "base_density": 82},
    {"code": "BLR-OB-31", "name": "Kengeri Satellite Town", "zone": "Outer Belt", "base_speed": 57.0, "base_density": 52},
]


def generate_event(hub: dict) -> dict:
    hour = datetime.now().hour
    is_peak = 1 if (8 <= hour <= 11 or 17 <= hour <= 21) else 0
    peak_factor = 0.68 if is_peak else 1.0
    speed = round(max(6.5, hub["base_speed"] * peak_factor + random.uniform(-2.2, 2.2)), 1)
    density = int(max(20, hub["base_density"] * (1.28 if is_peak else 1.0) + random.randint(-7, 7)))
    rainfall = round(random.choice([0.0, 0.0, 0.0, 2.5, 11.0]), 1)
    return {
        "code": hub["code"],
        "junction": hub["name"],
        "zone": hub["zone"],
        "speed_kmph": speed,
        "density_veh_km": density,
        "rainfall_mm_hr": rainfall,
        "is_peak": is_peak,
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


def main():
    topic = "bengaluru.traffic.telemetry"
    producer = None

    if KAFKA_LIB_INSTALLED:
        try:
            producer = KafkaProducer(
                bootstrap_servers=["localhost:9092"],
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                request_timeout_ms=2000,
                max_block_ms=2000,
            )
            print(f"[Kafka Connected] Publishing Greater Bengaluru events to {topic}...")
        except Exception:
            producer = None
            print("[Console Mode] No local Kafka broker on localhost:9092 — streaming live JSON telemetry to console:")
    else:
        print("[Console Mode] Streaming live Greater Bengaluru JSON telemetry to console:")

    try:
        while True:
            for hub in BENGALURU_HUBS:
                payload = generate_event(hub)
                if producer:
                    producer.send(topic, key=hub["zone"], value=payload)
                else:
                    print(json.dumps(payload))
            time.sleep(1.5)
    except KeyboardInterrupt:
        print("\n[Stopped] Telemetry producer terminated cleanly.")


if __name__ == "__main__":
    main()
