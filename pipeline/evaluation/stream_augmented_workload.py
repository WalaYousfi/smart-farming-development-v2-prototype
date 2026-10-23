# Generates records reproducibly and streams them directly to Kafka


import argparse
import csv
import json
import random
import os
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

FIELD_PATH = ROOT / "data/source/Smart_Farming_Crop_Yield_2024.csv"
WEATHER_PATH = ROOT / "data/source/weather/weather_observations.json"


def shift_date(value, days):
    if not value:
        return value
    try:
        return (
            date.fromisoformat(str(value)[:10])
            + timedelta(days=days)
        ).isoformat()
    except ValueError:
        return value


def generate(source, count, seed):
    with FIELD_PATH.open(encoding="utf-8-sig", newline="") as f:
        fields = list(csv.DictReader(f))

    with WEATHER_PATH.open(encoding="utf-8") as f:
        weathers = json.load(f)

    weather_by_key = {
        (str(w["farm_id"]), str(w["observed_at"])[:10]): w
        for w in weathers
    }

    pairs = []

    for field in fields:
        key = (
            str(field["farm_id"]),
            str(field["timestamp"])[:10],
        )

        if key in weather_by_key:
            pairs.append((field, weather_by_key[key]))

    if not pairs:
        raise ValueError("No matching Field/Weather source records")

    rng = random.Random(seed)

    for i in range(count):
        field, weather = pairs[rng.randrange(len(pairs))]
        days = 400 * (i // len(pairs))
        farm_id = f"AUGFARM{i + 1:07d}"

        if source == "field":
            record = dict(field)
            record["farm_id"] = farm_id
            record["sensor_id"] = f"AUGSENS{i + 1:07d}"

            for name in ("timestamp", "sowing_date", "harvest_date"):
                record[name] = shift_date(record.get(name), days)

        else:
            record = dict(weather)
            record["farm_id"] = farm_id
            record["weather_station_id"] = f"AUGWS{i + 1:07d}"
            record["observed_at"] = shift_date(
                weather["observed_at"], days
            )

        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["field", "weather"], required=True)
    parser.add_argument("--count", type=int, default=100000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()

    if args.count <= 0 or args.batch_size <= 0:
        parser.error("Count and batch size must be positive")

    # Load evaluation configuration before publishing.
    if not args.check_only:
        if os.getenv("APP_ENV") != "evaluation":
            parser.error("APP_ENV=evaluation is required")

        from pipeline.common.config import (
            KAFKA_SERVER,
            KAFKA_TOPIC,
            WEATHER_KAFKA_TOPIC,
            MINIO_BUCKET,
        )

        if MINIO_BUCKET != "smart-farming-100000-records-evaluation":
            parser.error("Incorrect evaluation bucket")

        topic = (
            KAFKA_TOPIC if args.source == "field"
            else WEATHER_KAFKA_TOPIC
        )

        if "100000" not in topic:
            parser.error("Incorrect evaluation topic")

        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda record: json.dumps(
                record, separators=(",", ":")
            ).encode("utf-8"),
        )

    count = 0

    try:
        for record in generate(args.source, args.count, args.seed):
            if not args.check_only:
                producer.send(topic, record)

            count += 1

            if not args.check_only and count % args.batch_size == 0:
                producer.flush()

        if not args.check_only:
            producer.flush()

    finally:
        if not args.check_only:
            producer.close()

    print(
        f"{'Checked' if args.check_only else 'Published'} "
        f"{count} {args.source} records"
    )


if __name__ == "__main__":
    main()