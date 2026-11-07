
# Generate reproducible noisy Field/Weather workloads.
# Compatible with Python 3.7.

import argparse
import csv
import json
import os
import random

from datetime import date, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

FIELD_PATH = ROOT / "data/source/Smart_Farming_Crop_Yield_2024.csv"
WEATHER_PATH = ROOT / "data/source/weather/weather_observations.json"

DEFAULT_COUNT = 1000000
DEFAULT_SEED = 42

DEFAULT_ANOMALY_RATE = 0.05
DEFAULT_FIELD_DEFECT_RATE = 0.04
DEFAULT_WEATHER_DEFECT_RATE = 0.03
DEFAULT_MISSING_WEATHER_RATE = 0.08
DEFAULT_MISMATCH_RATE = 0.05


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

    with WEATHER_PATH.open(encoding="utf-8") as file:
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

    print("Matched source pairs: {}".format(len(pairs)))
    return pairs


def selected(index, seed, category, rate):
    rng = random.Random(
        "{}:{}:{}".format(seed, category, index)
    )
    return rng.random() < rate


def should_inject_anomaly(index, seed, anomaly_rate):
    return selected(index, seed, "anomaly", anomaly_rate)


def inject_anomaly(record, index, seed):
    """
    Inject a schema-valid numerical anomaly for Gold.
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


def noise_plan(
    index,
    seed,
    field_defect_rate,
    weather_defect_rate,
    missing_weather_rate,
    mismatch_rate,
):
    missing = selected(
        index, seed, "missing_weather", missing_weather_rate
    )

    mismatch = (
        not missing
        and selected(
            index, seed, "mismatch_weather", mismatch_rate
        )
    )

    field_defect = selected(
        index, seed, "field_defect", field_defect_rate
    )

    weather_defect = (
        not missing
        and selected(
            index, seed, "weather_defect", weather_defect_rate
        )
    )

    return missing, mismatch, field_defect, weather_defect


def perturb_farm_identifier(farm_id, index, seed):
    """
    Produce a similar but non-identical farm ID.

    The integration layer is NOT modified to reconcile it.
    This allows testing the limitations of exact matching.
    """
    rng = random.Random(
        "{}:identifier_variant:{}".format(seed, index)
    )

    variant = rng.choice([
        "lowercase",
        "insert_hyphen",
        "insert_underscore",
        "insert_separator",
        "one_digit_typo",
    ])

    if variant == "lowercase":
        changed = farm_id.lower()

    elif variant == "insert_hyphen":
        changed = farm_id.replace(
            "AUGFARM", "AUG-FARM", 1
        )

    elif variant == "insert_underscore":
        changed = farm_id.replace(
            "AUGFARM", "AUG_FARM", 1
        )

    elif variant == "insert_separator":
        changed = farm_id.replace(
            "AUGFARM", "AUGFARM-", 1
        )

    else:
        # A visually similar substitution, with a suffix
        # to prevent collisions with other generated IDs.
        changed = (
            farm_id[:-1]
            + "O-{:07d}".format(index + 1)
        )

    assert changed != farm_id

    return changed, variant


def generate_with_truth(
    source,
    count,
    seed,
    anomaly_rate=DEFAULT_ANOMALY_RATE,
    field_defect_rate=DEFAULT_FIELD_DEFECT_RATE,
    weather_defect_rate=DEFAULT_WEATHER_DEFECT_RATE,
    missing_weather_rate=DEFAULT_MISSING_WEATHER_RATE,
    mismatch_rate=DEFAULT_MISMATCH_RATE,
):
    pairs = load_source_pairs()
    rng = random.Random(seed)

    for index in range(count):
        pair_index = rng.randrange(len(pairs))
        field, weather = pairs[pair_index]

        farm_id = "AUGFARM{:07d}".format(index + 1)

        # Bounded shifts to avoid extreme pandas dates.
        days = 400 * ((index // len(pairs)) % 100)

        missing, mismatch, field_defect, weather_defect = (
            noise_plan(
                index,
                seed,
                field_defect_rate,
                weather_defect_rate,
                missing_weather_rate,
                mismatch_rate,
            )
        )

        truth = {
            "index": index + 1,
            "source": source,
            "original_farm_id": farm_id,
            "injected_defect": "none",
            "expected_behavior": "silver_accept",
            "published": True,
            "gold_anomaly": False,
        }

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
                    record.get(name), days
                )

            record["augmentation_family_id"] = (
                "family-{:06d}".format(pair_index + 1)
            )

            anomaly = should_inject_anomaly(
                index, seed, anomaly_rate
            )

            record["ground_truth_anomaly"] = int(anomaly)
            truth["gold_anomaly"] = anomaly

            if anomaly:
                truth["gold_anomaly_type"] = (
                    inject_anomaly(record, index, seed)
                )

            if field_defect:
                record["soil_moisture_%"] = "-5.0"

                truth["injected_defect"] = (
                    "field_negative_soil_moisture"
                )
                truth["expected_behavior"] = (
                    "silver_quarantine"
                )

        else:
            if missing:
                truth["published"] = False
                truth["injected_defect"] = "weather_missing"
                truth["expected_behavior"] = (
                    "integration_unmatched"
                )

                yield None, truth
                continue

            record = dict(weather)

            record["farm_id"] = farm_id
            record["weather_station_id"] = (
                "AUGWS{:07d}".format(index + 1)
            )

            record["observed_at"] = shift_date(
                weather["observed_at"], days
            )

            if mismatch:
                changed_id, variant = (
                    perturb_farm_identifier(
                        farm_id, index, seed
                    )
                )

                record["farm_id"] = changed_id

                truth["injected_defect"] = (
                    "weather_similar_identifier_mismatch"
                )
                truth["identifier_variant"] = variant
                truth["weather_farm_id"] = changed_id
                truth["expected_behavior"] = (
                    "integration_unmatched"
                )

            if weather_defect:
                record["relative_humidity_percentage"] = 105.0

                if mismatch:
                    truth["injected_defect"] = (
                        "weather_humidity_and_"
                        "similar_identifier_mismatch"
                    )
                else:
                    truth["injected_defect"] = (
                        "weather_humidity_above_100"
                    )

                truth["expected_behavior"] = (
                    "silver_quarantine"
                )

        yield record, truth


def generate(
    source,
    count,
    seed,
    anomaly_rate=DEFAULT_ANOMALY_RATE,
    field_defect_rate=DEFAULT_FIELD_DEFECT_RATE,
    weather_defect_rate=DEFAULT_WEATHER_DEFECT_RATE,
    missing_weather_rate=DEFAULT_MISSING_WEATHER_RATE,
    mismatch_rate=DEFAULT_MISMATCH_RATE,
):
    for record, truth in generate_with_truth(
        source,
        count,
        seed,
        anomaly_rate,
        field_defect_rate,
        weather_defect_rate,
        missing_weather_rate,
        mismatch_rate,
    ):
        if record is not None:
            yield record


def check_workload(
    count,
    seed,
    anomaly_rate,
    field_defect_rate,
    weather_defect_rate,
    missing_weather_rate,
    mismatch_rate,
):
    field_keys = set()
    weather_keys = set()

    counts = {
        "field": 0,
        "weather": 0,
        "weather_missing": 0,
        "field_defect": 0,
        "weather_defect": 0,
        "weather_mismatch": 0,
        "gold_anomaly": 0,
    }

    for source in ("field", "weather"):
        for record, truth in generate_with_truth(
            source,
            count,
            seed,
            anomaly_rate,
            field_defect_rate,
            weather_defect_rate,
            missing_weather_rate,
            mismatch_rate,
        ):
            if record is None:
                counts["weather_missing"] += 1
                continue

            timestamp_field = (
                "timestamp"
                if source == "field"
                else "observed_at"
            )

            key = (
                str(record["farm_id"]),
                str(record[timestamp_field])[:10],
            )

            keys = (
                field_keys
                if source == "field"
                else weather_keys
            )

            if key in keys:
                raise ValueError(
                    "Duplicate {} key: {}".format(
                        source, key
                    )
                )

            keys.add(key)
            counts[source] += 1

            if source == "field":
                counts["gold_anomaly"] += int(
                    truth["gold_anomaly"]
                )
                counts["field_defect"] += int(
                    truth["injected_defect"] != "none"
                )

            else:
                counts["weather_defect"] += int(
                    "humidity" in truth["injected_defect"]
                )
                counts["weather_mismatch"] += int(
                    "mismatch" in truth["injected_defect"]
                )

    if counts["field"] != count:
        raise ValueError("Incorrect Field count")

    if (
        counts["weather"] + counts["weather_missing"]
        != count
    ):
        raise ValueError("Incorrect Weather count")

    print("Workload verification PASSED")

    print(
        json.dumps(
            counts,
            indent=2,
            sort_keys=True,
        )
    )

    print(
        "Pre-Silver exact matching keys:",
        len(field_keys & weather_keys),
    )

    print(
        "Note: post-Silver integration matches may "
        "be lower if records are quarantined."
    )


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
        "--field-defect-rate",
        type=float,
        default=DEFAULT_FIELD_DEFECT_RATE,
    )

    parser.add_argument(
        "--weather-defect-rate",
        type=float,
        default=DEFAULT_WEATHER_DEFECT_RATE,
    )

    parser.add_argument(
        "--missing-weather-rate",
        type=float,
        default=DEFAULT_MISSING_WEATHER_RATE,
    )

    parser.add_argument(
        "--mismatch-rate",
        type=float,
        default=DEFAULT_MISMATCH_RATE,
    )

    parser.add_argument(
        "--truth-output",
        type=str,
        default=None,
        help="Separate JSONL ground-truth file",
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

    for name in (
        "anomaly_rate",
        "field_defect_rate",
        "weather_defect_rate",
        "missing_weather_rate",
        "mismatch_rate",
    ):
        value = getattr(args, name)

        if not 0 <= value <= 1:
            parser.error(
                "--{} must be between 0 and 1".format(
                    name.replace("_", "-")
                )
            )

    if args.check_only:
        check_workload(
            args.count,
            args.seed,
            args.anomaly_rate,
            args.field_defect_rate,
            args.weather_defect_rate,
            args.missing_weather_rate,
            args.mismatch_rate,
        )
        return

    if args.source is None:
        parser.error(
            "--source is required when publishing"
        )

    if not args.truth_output:
        parser.error(
            "--truth-output is required when publishing"
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

    expected_bucket = os.getenv(
        "EVALUATION_EXPECTED_BUCKET", ""
    ).strip()

    topic_marker = os.getenv(
        "EVALUATION_TOPIC_MARKER", ""
    ).strip()

    if not expected_bucket or not topic_marker:
        parser.error(
            "Missing EVALUATION_EXPECTED_BUCKET "
            "or EVALUATION_TOPIC_MARKER"
        )

    if (
        not topic_marker.isdigit()
        or int(topic_marker) <= 0
    ):
        parser.error(
            "EVALUATION_TOPIC_MARKER must be positive"
        )

    if not expected_bucket.endswith("-evaluation"):
        parser.error(
            "Expected bucket must end with -evaluation"
        )

    if MINIO_BUCKET != expected_bucket:
        parser.error("Incorrect evaluation bucket")

    topic = (
        KAFKA_TOPIC
        if args.source == "field"
        else WEATHER_KAFKA_TOPIC
    )

    expected_topic = "eval-{}-{}-readings".format(
        topic_marker, args.source
    )

    if topic != expected_topic:
        parser.error(
            "Incorrect evaluation topic: expected {}".format(
                expected_topic
            )
        )

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda record: json.dumps(
            record,
            separators=(",", ":"),
        ).encode("utf-8"),
    )

    truth_path = Path(args.truth_output)

    if truth_path.exists():
        parser.error(
            "Ground truth already exists: {}".format(
                truth_path
            )
        )

    truth_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    published = 0

    try:
        with truth_path.open(
            "x",
            encoding="utf-8",
        ) as truth_file:

            for record, truth in generate_with_truth(
                args.source,
                args.count,
                args.seed,
                args.anomaly_rate,
                args.field_defect_rate,
                args.weather_defect_rate,
                args.missing_weather_rate,
                args.mismatch_rate,
            ):
                truth_file.write(
                    json.dumps(
                        truth,
                        sort_keys=True,
                    ) + "\n"
                )

                if record is None:
                    continue

                producer.send(topic, record)
                published += 1

                if published % args.batch_size == 0:
                    producer.flush()

                    print(
                        "Published {} {} records".format(
                            published, args.source
                        )
                    )

        producer.flush()

    finally:
        producer.close()

    print(
        "Published {} {} records to {}".format(
            published,
            args.source,
            topic,
        )
    )

    print("Ground truth:", truth_path)


if __name__ == "__main__":
    main()
