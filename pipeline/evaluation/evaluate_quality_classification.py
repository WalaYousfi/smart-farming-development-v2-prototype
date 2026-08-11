import argparse
from io import BytesIO
import json
from pathlib import Path
from typing import Any, Dict, List, Set

import pandas as pd

from pipeline.common.config import (
    BRONZE_PREFIX,
    MINIO_BUCKET,
    PROJECT_ROOT,
    SOURCE_SYSTEM,
)
from pipeline.common.minio_client import (
    create_minio_client,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate Silver quality classification "
            "against controlled ground truth using "
            "Bronze event lineage."
        )
    )

    parser.add_argument(
        "--silver-run-id",
        required=True,
        help="Silver run being evaluated.",
    )

    parser.add_argument(
        "--ground-truth",
        required=True,
        help="Controlled ground-truth JSON file.",
    )

    return parser.parse_args()


def read_local_json(
    path: Path,
) -> Dict[str, Any]:
    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def read_jsonl_object(
    minio_client: Any,
    object_name: str,
) -> List[Dict[str, Any]]:
    """
    Read a JSONL object from MinIO.
    """

    response = minio_client.get_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
    )

    try:
        content = response.read().decode(
            "utf-8"
        )

    finally:
        response.close()
        response.release_conn()

    records = []

    for line in content.splitlines():

        if line.strip():
            records.append(
                json.loads(line)
            )

    return records


def read_parquet_object(
    minio_client: Any,
    object_name: str,
) -> pd.DataFrame:
    """
    Read a Parquet object from MinIO.
    """

    response = minio_client.get_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
    )

    try:
        content = response.read()

    finally:
        response.close()
        response.release_conn()

    return pd.read_parquet(
        BytesIO(content)
    )


def find_silver_run_objects(
    minio_client: Any,
    silver_run_id: str,
) -> Dict[str, List[str]]:
    """
    Locate accepted and quarantine objects for one
    Silver run.
    """

    objects = minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix="silver/",
        recursive=True,
    )

    accepted_objects = []
    quarantine_objects = []

    run_marker = (
        f"run_id={silver_run_id}/"
    )

    for obj in objects:

        object_name = obj.object_name

        if run_marker not in object_name:
            continue

        if (
            object_name.startswith(
                "silver/accepted/"
            )
            and object_name.endswith(
                ".parquet"
            )
        ):
            accepted_objects.append(
                object_name
            )

        elif (
            object_name.startswith(
                "silver/quarantine/"
            )
            and object_name.endswith(
                ".jsonl"
            )
        ):
            quarantine_objects.append(
                object_name
            )

    return {
        "accepted": sorted(
            accepted_objects
        ),
        "quarantine": sorted(
            quarantine_objects
        ),
    }


def determine_bronze_run_id(
    minio_client: Any,
    quarantine_objects: List[str],
) -> str:
    """
    Determine the Bronze parent run from quarantine data.

    In this controlled mixed experiment at least one
    quarantine record is expected.
    """

    if not quarantine_objects:
        raise RuntimeError(
            "No quarantine object was found, so the "
            "Bronze parent run could not be identified "
            "using quarantine lineage."
        )

    records = read_jsonl_object(
        minio_client,
        quarantine_objects[0],
    )

    bronze_run_ids = {
        record.get("bronze_run_id")
        for record in records
        if record.get("bronze_run_id")
    }

    if len(bronze_run_ids) != 1:
        raise RuntimeError(
            "Expected exactly one Bronze parent run. "
            f"Found: {bronze_run_ids}"
        )

    return next(
        iter(bronze_run_ids)
    )


def find_bronze_objects(
    minio_client: Any,
    bronze_run_id: str,
) -> List[str]:
    """
    Locate Field Bronze JSONL objects belonging to
    the selected quality-test run.
    """

    prefix = (
        f"{BRONZE_PREFIX}/"
        f"source={SOURCE_SYSTEM}/"
    )

    objects = minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    run_marker = (
        f"run_id={bronze_run_id}/"
    )

    bronze_objects = [
        obj.object_name
        for obj in objects
        if (
            run_marker
            in obj.object_name
            and obj.object_name.endswith(
                ".jsonl"
            )
        )
    ]

    if not bronze_objects:
        raise RuntimeError(
            "No Bronze objects were found for "
            f"run: {bronze_run_id}"
        )

    return sorted(
        bronze_objects
    )


def build_test_id_event_map(
    minio_client: Any,
    bronze_objects: List[str],
) -> Dict[str, str]:
    """
    Map experiment test IDs to immutable Bronze event IDs.

    Example:
        valid-01 -> UUID
        soil-moisture-01 -> UUID
    """

    mapping = {}

    for object_name in bronze_objects:

        records = read_jsonl_object(
            minio_client,
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
            event_id = str(event_id)

            if test_id in mapping:
                raise RuntimeError(
                    "Duplicate quality test ID "
                    f"found in Bronze: {test_id}"
                )

            mapping[test_id] = event_id

    return mapping


def get_accepted_event_ids(
    minio_client: Any,
    accepted_objects: List[str],
) -> Set[str]:
    """
    Extract source_event_id values from accepted
    canonical Silver data.
    """

    event_ids = set()

    for object_name in accepted_objects:

        dataframe = read_parquet_object(
            minio_client,
            object_name,
        )

        if (
            "source_event_id"
            not in dataframe.columns
        ):
            raise RuntimeError(
                "Accepted Silver data does not "
                "contain source_event_id."
            )

        values = (
            dataframe[
                "source_event_id"
            ]
            .dropna()
            .astype(str)
            .tolist()
        )

        event_ids.update(values)

    return event_ids


def get_quarantined_event_ids(
    minio_client: Any,
    quarantine_objects: List[str],
) -> Set[str]:
    """
    Extract original Bronze event IDs from
    quarantine records.
    """

    event_ids = set()

    for object_name in quarantine_objects:

        records = read_jsonl_object(
            minio_client,
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


def calculate_metrics(
    tp: int,
    tn: int,
    fp: int,
    fn: int,
) -> Dict[str, float]:

    total = (
        tp + tn + fp + fn
    )

    accuracy = (
        (tp + tn) / total
        if total
        else 0.0
    )

    precision = (
        tp / (tp + fp)
        if tp + fp
        else 0.0
    )

    recall = (
        tp / (tp + fn)
        if tp + fn
        else 0.0
    )

    specificity = (
        tn / (tn + fp)
        if tn + fp
        else 0.0
    )

    false_positive_rate = (
        fp / (fp + tn)
        if fp + tn
        else 0.0
    )

    f1_score = (
        (
            2
            * precision
            * recall
        )
        / (
            precision
            + recall
        )
        if (
            precision
            + recall
        )
        else 0.0
    )

    return {
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
        "false_positive_rate": round(
            false_positive_rate,
            4,
        ),
        "f1_score": round(
            f1_score,
            4,
        ),
    }


def main() -> None:

    args = parse_arguments()

    ground_truth_path = Path(
        args.ground_truth
    )

    if not ground_truth_path.exists():
        raise FileNotFoundError(
            "Ground-truth file not found: "
            f"{ground_truth_path}"
        )

    ground_truth = read_local_json(
        ground_truth_path
    )

    expected_results = {}

    for record in ground_truth[
        "records"
    ]:

        expected_results[
            str(record["test_id"])
        ] = record[
            "expected_result"
        ]

    minio_client = (
        create_minio_client()
    )

    # ------------------------------------------------
    # Locate Silver outputs
    # ------------------------------------------------

    silver_objects = (
        find_silver_run_objects(
            minio_client=minio_client,
            silver_run_id=(
                args.silver_run_id
            ),
        )
    )

    print("\nSilver objects")
    print("--------------")

    print("\nAccepted:")

    for object_name in (
        silver_objects[
            "accepted"
        ]
    ):
        print(
            f"  {object_name}"
        )

    print("\nQuarantine:")

    for object_name in (
        silver_objects[
            "quarantine"
        ]
    ):
        print(
            f"  {object_name}"
        )

    if not silver_objects[
        "accepted"
    ]:
        raise RuntimeError(
            "No accepted Silver Parquet "
            "object found."
        )

    if not silver_objects[
        "quarantine"
    ]:
        raise RuntimeError(
            "No quarantine Silver JSONL "
            "object found."
        )

    # ------------------------------------------------
    # Trace Silver back to Bronze
    # ------------------------------------------------

    bronze_run_id = (
        determine_bronze_run_id(
            minio_client=minio_client,
            quarantine_objects=(
                silver_objects[
                    "quarantine"
                ]
            ),
        )
    )

    print(
        "\nBronze parent run:"
    )
    print(bronze_run_id)

    bronze_objects = (
        find_bronze_objects(
            minio_client=minio_client,
            bronze_run_id=(
                bronze_run_id
            ),
        )
    )

    print("\nBronze objects:")

    for object_name in (
        bronze_objects
    ):
        print(
            f"  {object_name}"
        )

    # ------------------------------------------------
    # Build experimental ID -> event ID mapping
    # ------------------------------------------------

    test_id_event_map = (
        build_test_id_event_map(
            minio_client=minio_client,
            bronze_objects=(
                bronze_objects
            ),
        )
    )

    print(
        "\nControlled test IDs "
        "found in Bronze: "
        f"{len(test_id_event_map)}"
    )

    # ------------------------------------------------
    # Read actual Silver decisions
    # ------------------------------------------------

    accepted_event_ids = (
        get_accepted_event_ids(
            minio_client=minio_client,
            accepted_objects=(
                silver_objects[
                    "accepted"
                ]
            ),
        )
    )

    quarantined_event_ids = (
        get_quarantined_event_ids(
            minio_client=minio_client,
            quarantine_objects=(
                silver_objects[
                    "quarantine"
                ]
            ),
        )
    )

    print(
        "Accepted event IDs found: "
        f"{len(accepted_event_ids)}"
    )

    print(
        "Quarantined event IDs found: "
        f"{len(quarantined_event_ids)}"
    )

    # ------------------------------------------------
    # Confusion matrix
    # ------------------------------------------------

    tp = 0
    tn = 0
    fp = 0
    fn = 0

    results = []
    missing_records = []

    for (
        test_id,
        expected_result,
    ) in expected_results.items():

        event_id = (
            test_id_event_map.get(
                test_id
            )
        )

        if event_id is None:

            actual_result = (
                "missing_from_bronze"
            )

            missing_records.append(
                test_id
            )

        elif (
            event_id
            in accepted_event_ids
        ):

            actual_result = (
                "accepted"
            )

        elif (
            event_id
            in quarantined_event_ids
        ):

            actual_result = (
                "quarantined"
            )

        else:

            actual_result = (
                "missing_from_silver"
            )

            missing_records.append(
                test_id
            )

        if (
            expected_result
            == "quarantined"
            and actual_result
            == "quarantined"
        ):

            tp += 1

        elif (
            expected_result
            == "accepted"
            and actual_result
            == "accepted"
        ):

            tn += 1

        elif (
            expected_result
            == "accepted"
            and actual_result
            == "quarantined"
        ):

            fp += 1

        elif (
            expected_result
            == "quarantined"
            and actual_result
            == "accepted"
        ):

            fn += 1

        results.append(
            {
                "test_id": test_id,
                "event_id": event_id,
                "expected": (
                    expected_result
                ),
                "actual": (
                    actual_result
                ),
            }
        )

    metrics = calculate_metrics(
        tp=tp,
        tn=tn,
        fp=fp,
        fn=fn,
    )

    # ------------------------------------------------
    # Save report
    # ------------------------------------------------

    report = {
        "experiment": (
            "mixed_quality_classification"
        ),
        "bucket": MINIO_BUCKET,
        "silver_run_id": (
            args.silver_run_id
        ),
        "bronze_run_id": (
            bronze_run_id
        ),
        "ground_truth_records": (
            len(expected_results)
        ),
        "bronze_test_ids_found": (
            len(test_id_event_map)
        ),
        "accepted_event_ids_found": (
            len(accepted_event_ids)
        ),
        "quarantined_event_ids_found": (
            len(quarantined_event_ids)
        ),
        "confusion_matrix": {
            "true_positive": tp,
            "true_negative": tn,
            "false_positive": fp,
            "false_negative": fn,
        },
        "metrics": metrics,
        "missing_records": (
            missing_records
        ),
        "record_results": results,
    }

    output_directory = (
        PROJECT_ROOT
        / "experiments"
        / "paper-evaluation"
        / "quality-stress-test"
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_directory
        / "mixed_quality_evaluation.json"
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

    # ------------------------------------------------
    # Print final results
    # ------------------------------------------------

    print("\nConfusion matrix")
    print("----------------")

    print(
        f"TP: {tp}"
    )

    print(
        f"TN: {tn}"
    )

    print(
        f"FP: {fp}"
    )

    print(
        f"FN: {fn}"
    )

    print(
        "\nClassification metrics"
    )

    print(
        "----------------------"
    )

    for (
        metric_name,
        metric_value,
    ) in metrics.items():

        print(
            f"{metric_name}: "
            f"{metric_value:.4f}"
        )

    print(
        "\nMissing records: "
        f"{len(missing_records)}"
    )

    if missing_records:

        print(
            "Missing test IDs:"
        )

        for test_id in (
            missing_records
        ):

            print(
                f"  {test_id}"
            )

    print(
        "\nEvaluation report:"
    )

    print(output_path)


if __name__ == "__main__":
    main()