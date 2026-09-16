import argparse
import csv
import json
from io import BytesIO
from pathlib import Path

import pandas as pd

from pipeline.common.config import (
    BRONZE_PREFIX,
    MINIO_BUCKET,
    PROJECT_ROOT,
    SOURCE_SYSTEM,
)
from pipeline.common.minio_client import create_minio_client


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Evaluate quality robustness experiment."
    )

    parser.add_argument(
        "--silver-run-id",
        required=True,
    )

    parser.add_argument(
        "--ground-truth",
        required=True,
    )

    return parser.parse_args()


def read_jsonl_object(client, object_name):
    response = client.get_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
    )

    try:
        content = response.read().decode("utf-8")
    finally:
        response.close()
        response.release_conn()

    return [
        json.loads(line)
        for line in content.splitlines()
        if line.strip()
    ]


def read_parquet_object(client, object_name):
    response = client.get_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
    )

    try:
        content = response.read()
    finally:
        response.close()
        response.release_conn()

    return pd.read_parquet(BytesIO(content))


def read_ground_truth(path):
    records = {}

    with path.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:
            test_id = str(
                row["quality_test_id"]
            )

            expected_invalid = (
                str(row["expected_invalid"])
                .strip()
                .lower()
                == "true"
            )

            records[test_id] = {
                "expected_invalid": expected_invalid,
                "defect_type": row["defect_type"],
            }

    return records


def find_silver_objects(
    client,
    silver_run_id,
):
    run_marker = (
        "run_id={}/".format(
            silver_run_id
        )
    )

    accepted = []
    quarantine = []

    objects = client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix="silver/",
        recursive=True,
    )

    for obj in objects:
        name = obj.object_name

        if run_marker not in name:
            continue

        if (
            name.startswith(
                "silver/accepted/"
            )
            and name.endswith(".parquet")
        ):
            accepted.append(name)

        elif (
            name.startswith(
                "silver/quarantine/"
            )
            and name.endswith(".jsonl")
        ):
            quarantine.append(name)

    if not accepted:
        raise RuntimeError(
            "No accepted Silver object found."
        )

    if not quarantine:
        raise RuntimeError(
            "No quarantine Silver object found."
        )

    return sorted(accepted), sorted(quarantine)


def determine_bronze_run_id(
    client,
    quarantine_objects,
):
    run_ids = set()

    for object_name in quarantine_objects:
        records = read_jsonl_object(
            client,
            object_name,
        )

        for record in records:
            bronze_run_id = record.get(
                "bronze_run_id"
            )

            if bronze_run_id:
                run_ids.add(
                    str(bronze_run_id)
                )

    if len(run_ids) != 1:
        raise RuntimeError(
            "Expected exactly one Bronze parent "
            "run, found: {}".format(run_ids)
        )

    return next(iter(run_ids))


def find_bronze_objects(
    client,
    bronze_run_id,
):
    prefix = (
        "{}/source={}/".format(
            BRONZE_PREFIX,
            SOURCE_SYSTEM,
        )
    )

    marker = (
        "run_id={}/".format(
            bronze_run_id
        )
    )

    objects = client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    result = [
        obj.object_name
        for obj in objects
        if (
            marker in obj.object_name
            and obj.object_name.endswith(
                ".jsonl"
            )
        )
    ]

    if not result:
        raise RuntimeError(
            "No Bronze objects found."
        )

    return sorted(result)


def build_test_event_map(
    client,
    bronze_objects,
):
    mapping = {}

    for object_name in bronze_objects:
        records = read_jsonl_object(
            client,
            object_name,
        )

        for record in records:
            metadata = record.get(
                "metadata",
                {},
            )

            payload = record.get(
                "payload",
                {},
            )

            test_id = payload.get(
                "_quality_test_id"
            )

            event_id = metadata.get(
                "event_id"
            )

            if (
                test_id is None
                or event_id is None
            ):
                continue

            test_id = str(test_id)

            if test_id in mapping:
                raise RuntimeError(
                    "Duplicate test ID in Bronze: "
                    + test_id
                )

            mapping[test_id] = str(
                event_id
            )

    return mapping


def get_accepted_event_ids(
    client,
    objects,
):
    event_ids = set()

    for object_name in objects:
        dataframe = read_parquet_object(
            client,
            object_name,
        )

        values = (
            dataframe["source_event_id"]
            .dropna()
            .astype(str)
            .tolist()
        )

        event_ids.update(values)

    return event_ids


def get_quarantined_event_ids(
    client,
    objects,
):
    event_ids = set()

    for object_name in objects:
        records = read_jsonl_object(
            client,
            object_name,
        )

        for record in records:
            event_id = record.get(
                "event_id"
            )

            if event_id is not None:
                event_ids.add(
                    str(event_id)
                )

    return event_ids


def safe_divide(numerator, denominator):
    if denominator == 0:
        return 0.0

    return numerator / denominator


def main():
    args = parse_arguments()

    ground_truth_path = Path(
        args.ground_truth
    )

    ground_truth = read_ground_truth(
        ground_truth_path
    )

    client = create_minio_client()

    accepted_objects, quarantine_objects = (
        find_silver_objects(
            client,
            args.silver_run_id,
        )
    )

    bronze_run_id = determine_bronze_run_id(
        client,
        quarantine_objects,
    )

    bronze_objects = find_bronze_objects(
        client,
        bronze_run_id,
    )

    test_event_map = build_test_event_map(
        client,
        bronze_objects,
    )

    accepted_event_ids = (
        get_accepted_event_ids(
            client,
            accepted_objects,
        )
    )

    quarantined_event_ids = (
        get_quarantined_event_ids(
            client,
            quarantine_objects,
        )
    )

    tp = 0
    tn = 0
    fp = 0
    fn = 0

    missing = []
    record_results = []

    defect_statistics = {}

    for test_id, truth in ground_truth.items():

        event_id = test_event_map.get(
            test_id
        )

        if event_id is None:
            missing.append(test_id)
            continue

        if event_id in quarantined_event_ids:
            actual_invalid = True
            actual_result = "quarantined"

        elif event_id in accepted_event_ids:
            actual_invalid = False
            actual_result = "accepted"

        else:
            missing.append(test_id)
            continue

        expected_invalid = truth[
            "expected_invalid"
        ]

        defect_type = truth[
            "defect_type"
        ]

        if expected_invalid:
            if actual_invalid:
                tp += 1
            else:
                fn += 1
        else:
            if actual_invalid:
                fp += 1
            else:
                tn += 1

        if defect_type != "none":
            if defect_type not in defect_statistics:
                defect_statistics[
                    defect_type
                ] = {
                    "total": 0,
                    "detected": 0,
                    "missed": 0,
                }

            stats = defect_statistics[
                defect_type
            ]

            stats["total"] += 1

            if actual_invalid:
                stats["detected"] += 1
            else:
                stats["missed"] += 1

        record_results.append(
            {
                "quality_test_id": test_id,
                "event_id": event_id,
                "defect_type": defect_type,
                "expected_invalid": (
                    expected_invalid
                ),
                "actual_result": (
                    actual_result
                ),
            }
        )

    precision = safe_divide(
        tp,
        tp + fp,
    )

    recall = safe_divide(
        tp,
        tp + fn,
    )

    specificity = safe_divide(
        tn,
        tn + fp,
    )

    accuracy = safe_divide(
        tp + tn,
        tp + tn + fp + fn,
    )

    f1 = safe_divide(
        2 * precision * recall,
        precision + recall,
    )

    for defect_type, stats in (
        defect_statistics.items()
    ):
        stats["recall"] = round(
            safe_divide(
                stats["detected"],
                stats["total"],
            ),
            4,
        )

    metrics = {
        "accuracy": round(
            accuracy,
            4,
        ),
        "precision": round(
            precision,
            4,
        ),
        "recall": round(
            recall,
            4,
        ),
        "specificity": round(
            specificity,
            4,
        ),
        "f1_score": round(
            f1,
            4,
        ),
    }

    report = {
        "experiment": (
            "quality_robustness_pilot"
        ),
        "bucket": MINIO_BUCKET,
        "silver_run_id": (
            args.silver_run_id
        ),
        "bronze_run_id": (
            bronze_run_id
        ),
        "ground_truth_records": len(
            ground_truth
        ),
        "bronze_test_ids_found": len(
            test_event_map
        ),
        "accepted_event_ids": len(
            accepted_event_ids
        ),
        "quarantined_event_ids": len(
            quarantined_event_ids
        ),
        "confusion_matrix": {
            "true_positive": tp,
            "true_negative": tn,
            "false_positive": fp,
            "false_negative": fn,
        },
        "metrics": metrics,
        "per_defect": defect_statistics,
        "missing_records": missing,
        "record_results": record_results,
    }

    output_directory = (
        PROJECT_ROOT
        / "evaluation"
        / "results"
        / "quality_robustness"
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_directory
        / (
            "quality_robustness_"
            + args.silver_run_id
            + ".json"
        )
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            report,
            file,
            indent=2,
        )

    print()
    print("Quality robustness evaluation")
    print("-----------------------------")
    print(
        "Ground-truth records:",
        len(ground_truth),
    )
    print(
        "Bronze test IDs:",
        len(test_event_map),
    )
    print(
        "Accepted:",
        len(accepted_event_ids),
    )
    print(
        "Quarantined:",
        len(quarantined_event_ids),
    )

    print()
    print("Confusion matrix")
    print("----------------")
    print("TP:", tp)
    print("TN:", tn)
    print("FP:", fp)
    print("FN:", fn)

    print()
    print("Metrics")
    print("-------")

    for name, value in metrics.items():
        print(
            "{}: {:.4f}".format(
                name,
                value,
            )
        )

    print()
    print("Per-defect detection")
    print("--------------------")

    for defect_type in sorted(
        defect_statistics
    ):
        stats = defect_statistics[
            defect_type
        ]

        print(
            "{}: {}/{} detected "
            "(recall={:.4f})".format(
                defect_type,
                stats["detected"],
                stats["total"],
                stats["recall"],
            )
        )

    print()
    print(
        "Missing records:",
        len(missing),
    )

    print()
    print("Report:")
    print(output_path)


if __name__ == "__main__":
    main()