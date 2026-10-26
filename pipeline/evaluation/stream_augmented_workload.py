# Role: Generate reproducible Field/Weather workloads,
# inject independently labeled anomalies, and stream to Kafka.
# Compatible with Python 3.7.

import argparse
import csv
import json
import os
import random

from datetime import date, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

FIELD_PATH = (
    ROOT / "data/source/Smart_Farming_Crop_Yield_2024.csv"
)

WEATHER_PATH = (
    ROOT / "data/source/weather/weather_observations.json"
)

DEFAULT_COUNT = 100000
DEFAULT_SEED = 42
DEFAULT_ANOMALY_RATE = 0.05


def shift_date(value, days):
    if not value:
        return value

    try:
        original = date.fromisoformat(str(value)[:10])
        return (original + timedelta(days=days)).isoformat()
    except ValueError:
        return value


def load_source_pairs():
    with FIELD_PATH.open(
        encoding="utf-8-sig",
        newline=""
    ) as file:
        fields = list(csv.DictReader(file))

    with WEATHER_PATH.open(
        encoding="utf-8"
    ) as file:
        weathers = json.load(file)

    weather_by_key = {}

    for weather in weathers:
        key = (
            str(weather["farm_id"]),
            str(weather["observed_at"])[:10],
        )

        if key in weather_by_key:
            raise ValueError(
                "Duplicate source Weather key: {}".format(key)
            )

        weather_by_key[key] = weather

    pairs = []

    for field in fields:
        key = (
            str(field["farm_id"]),
            str(field["timestamp"])[:10],
        )

        weather = weather_by_key.get(key)

        if weather is not None:
            pairs.append((field, weather))

    if not pairs:
        raise ValueError(
            "No matching Field/Weather source records"
        )

    print(
        "Matched source pairs: {}".format(len(pairs))
    )

    return pairs


def should_inject_anomaly(index, seed, anomaly_rate):
    """
    Deterministically choose approximately anomaly_rate
    of the generated Field observations.

    This decision does not depend on model predictions.
    """
    rng = random.Random(
        "{}:anomaly:{}".format(seed, index)
    )

    return rng.random() < anomaly_rate


def inject_anomaly(record, index, seed):
    """
    Inject a controlled, schema-valid numerical anomaly.

    The values remain within ordinary physical bounds.
    The injected patterns are synthetic experimental
    anomalies, not confirmed real-world faults.
    """
    rng = random.Random(
        "{}:perturbation:{}".format(seed, index)
    )

    anomaly_type = rng.choice([
        "low_moisture_high_temperature",
        "high_moisture_low_ndvi",
        "low_ndvi_high_pesticide",
    ])

    if anomaly_type == "low_moisture_high_temperature":
        record["soil_moisture_%"] = "8.0"
        record["temperature_C"] = "42.0"

    elif anomaly_type == "high_moisture_low_ndvi":
        record["soil_moisture_%"] = "85.0"
        record["NDVI_index"] = "0.12"

    else:
        record["NDVI_index"] = "0.15"
        record["pesticide_usage_ml"] = "95.0"

    return anomaly_type


def generate(source, count, seed, anomaly_rate=DEFAULT_ANOMALY_RATE):
    pairs = load_source_pairs()

    rng = random.Random(seed)

    for index in range(count):
        pair_index = rng.randrange(len(pairs))

        field, weather = pairs[pair_index]

        # Keep related synthetic records in the same
        # train/test augmentation family.
        family_id = "family-{:06d}".format(pair_index + 1)

        # Unique farm per generated pair prevents
        # accidental many-to-many integration.
        farm_id = "AUGFARM{:07d}".format(index + 1)

        # Both source streams use identical pairing,
        # date shifts, and farm identifiers.
        days = 400 * (index // len(pairs))

        if source == "field":
            record = dict(field)

            record["farm_id"] = farm_id
            record["sensor_id"] = (
                "AUGSENS{:07d}".format(index + 1)
            )

            for name in (
                "timestamp",
                "sowing_date",
                "harvest_date",
            ):
                record[name] = shift_date(
                    record.get(name),
                    days,
                )

            record["augmentation_family_id"] = family_id

            is_anomaly = should_inject_anomaly(
                index,
                seed,
                anomaly_rate,
            )

            record["ground_truth_anomaly"] = (
                1 if is_anomaly else 0
            )

            if is_anomaly:
                inject_anomaly(record, index, seed)

        else:
            record = dict(weather)

            record["farm_id"] = farm_id
            record["weather_station_id"] = (
                "AUGWS{:07d}".format(index + 1)
            )

            record["observed_at"] = shift_date(
                weather["observed_at"],
                days,
            )

            # Do not add augmentation metadata here:
            # the original Weather source schema does
            # not allow additional properties.

        yield record


def check_workload(count, seed, anomaly_rate):
    """
    Verify both streams without publishing to Kafka.
    Uses sets and counters, not large output files.
    """
    field_keys = set()
    weather_keys = set()
    families = set()

    field_count = 0
    weather_count = 0
    anomaly_count = 0

    for record in generate(
        "field",
        count,
        seed,
        anomaly_rate,
    ):
        field_count += 1

        key = (
            str(record["farm_id"]),
            str(record["timestamp"])[:10],
        )

        if key in field_keys:
            raise ValueError(
                "Duplicate Field integration key: {}".format(key)
            )

        field_keys.add(key)

        family_id = record["augmentation_family_id"]

        if not family_id:
            raise ValueError("Missing augmentation family")

        families.add(family_id)

        label = record["ground_truth_anomaly"]

        if label not in (0, 1):
            raise ValueError("Invalid ground-truth label")

        anomaly_count += label

    for record in generate(
        "weather",
        count,
        seed,
        anomaly_rate,
    ):
        weather_count += 1

        key = (
            str(record["farm_id"]),
            str(record["observed_at"])[:10],
        )

        if key in weather_keys:
            raise ValueError(
                "Duplicate Weather integration key: {}".format(key)
            )

        weather_keys.add(key)

    if field_count != count or weather_count != count:
        raise ValueError("Generated record count mismatch")

    if field_keys != weather_keys:
        raise ValueError(
            "Field and Weather integration keys do not match"
        )

    if count >= 20 and len(families) < 2:
        raise ValueError(
            "Insufficient augmentation families"
        )

    print("\nWorkload verification PASSED")
    print("Field records:", field_count)
    print("Weather records:", weather_count)
    print("Unique augmentation families:", len(families))
    print("Independent anomaly labels:", anomaly_count)
    print(
        "Actual anomaly rate: {:.4f}".format(
            anomaly_count / float(count)
        )
    )
    print("Matching Field/Weather keys:", len(field_keys))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source",
        choices=["field", "weather"],
        default=None,
    )

    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_COUNT,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
    )

    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=DEFAULT_ANOMALY_RATE,
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
    )

    parser.add_argument(
        "--check-only",
        action="store_true",
    )

    args = parser.parse_args()

    if args.count <= 0:
        parser.error("--count must be positive")

    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")

    if not 0 <= args.anomaly_rate <= 1:
        parser.error(
            "--anomaly-rate must be between 0 and 1"
        )

    if args.check_only:
        check_workload(
            args.count,
            args.seed,
            args.anomaly_rate,
        )
        return

    if args.source is None:
        parser.error(
            "--source is required when publishing"
        )

    if os.getenv("APP_ENV") != "evaluation":
        parser.error(
            "APP_ENV=evaluation is required"
        )

    from pipeline.common.config import (
        KAFKA_SERVER,
        KAFKA_TOPIC,
        WEATHER_KAFKA_TOPIC,
        MINIO_BUCKET,
    )

    if MINIO_BUCKET != (
        "smart-farming-100000-records-evaluation"
    ):
        parser.error("Incorrect evaluation bucket")

    topic = (
        KAFKA_TOPIC
        if args.source == "field"
        else WEATHER_KAFKA_TOPIC
    )

    if "100000" not in topic:
        parser.error("Incorrect evaluation topic")

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda record: json.dumps(
            record,
            separators=(",", ":"),
        ).encode("utf-8"),
    )

    published = 0

    try:
        for record in generate(
            args.source,
            args.count,
            args.seed,
            args.anomaly_rate,
        ):
            future = producer.send(topic, record)

            published += 1

            if published % args.batch_size == 0:
                # Surface asynchronous delivery errors.
                producer.flush()

                print(
                    "Published {} {} records".format(
                        published,
                        args.source,
                    )
                )

        producer.flush()

    finally:
        producer.close()

    print(
        "\nPublished {} {} records to {}".format(
            published,
            args.source,
            topic,
        )
    )


if __name__ == "__main__":
    main()