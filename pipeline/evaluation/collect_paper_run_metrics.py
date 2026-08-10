import argparse
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pipeline.common.config import (
    LINEAGE_PREFIX,
    MANIFEST_PREFIX,
    MINIO_BUCKET,
    PROJECT_ROOT,
)
from pipeline.common.minio_client import (
    create_minio_client,
    ensure_bucket_exists,
)


FIELD_BRONZE_JOB = "bronze_ingestion"
FIELD_SILVER_JOB = "silver_field_observations"

WEATHER_BRONZE_JOB = "weather_bronze_ingestion"
WEATHER_SILVER_JOB = "silver_weather_observations"

INTEGRATION_JOB = "silver_field_weather_integration"

GOLD_JOB = "gold_integrated_field_anomaly_detection"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect publication-oriented metrics "
            "for one isolated end-to-end pipeline run."
        )
    )

    parser.add_argument(
        "--field-bronze-run-id",
        required=True,
    )

    parser.add_argument(
        "--field-silver-run-id",
        required=True,
    )

    parser.add_argument(
        "--weather-bronze-run-id",
        required=True,
    )

    parser.add_argument(
        "--weather-silver-run-id",
        required=True,
    )

    parser.add_argument(
        "--integration-run-id",
        required=True,
    )

    parser.add_argument(
        "--gold-run-id",
        required=True,
    )

    parser.add_argument(
        "--experiment-name",
        default="paper-evaluation-run-01",
    )

    return parser.parse_args()


def read_json_object(
    minio_client: Any,
    object_name: str,
) -> Dict[str, Any]:

    response = minio_client.get_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
    )

    try:
        return json.loads(
            response.read().decode("utf-8")
        )

    finally:
        response.close()
        response.release_conn()


def object_exists(
    minio_client: Any,
    object_name: str,
) -> bool:

    try:
        minio_client.stat_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
        )

        return True

    except Exception:
        return False


def get_manifest(
    minio_client: Any,
    job_name: str,
    run_id: str,
) -> Dict[str, Any]:

    object_name = (
        f"{MANIFEST_PREFIX}/"
        f"{job_name}/"
        f"run_id={run_id}/"
        f"manifest_completed.json"
    )

    if not object_exists(
        minio_client,
        object_name,
    ):
        raise RuntimeError(
            f"Completed manifest not found: "
            f"{object_name}"
        )

    manifest = read_json_object(
        minio_client,
        object_name,
    )

    manifest["_manifest_object"] = object_name

    return manifest


def get_lineage(
    minio_client: Any,
    job_name: str,
    run_id: str,
) -> Optional[Dict[str, Any]]:

    object_name = (
        f"{LINEAGE_PREFIX}/"
        f"{job_name}/"
        f"run_id={run_id}/"
        f"lineage.json"
    )

    if not object_exists(
        minio_client,
        object_name,
    ):
        return None

    lineage = read_json_object(
        minio_client,
        object_name,
    )

    lineage["_lineage_object"] = object_name

    return lineage


def parse_timestamp(
    value: Optional[str],
) -> Optional[datetime]:

    if not value:
        return None

    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


def calculate_duration(
    manifest: Dict[str, Any],
) -> Optional[float]:

    started_at = parse_timestamp(
        manifest.get("started_at")
    )

    completed_at = parse_timestamp(
        manifest.get("completed_at")
    )

    if (
        started_at is None
        or completed_at is None
    ):
        return None

    return round(
        (
            completed_at
            - started_at
        ).total_seconds(),
        4,
    )


def get_object_size(
    minio_client: Any,
    object_name: str,
) -> int:

    try:
        result = minio_client.stat_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
        )

        return int(result.size)

    except Exception:
        return 0


def measure_objects(
    minio_client: Any,
    object_names: List[str],
) -> Dict[str, Any]:

    objects = []

    total_bytes = 0

    for object_name in object_names:
        size_bytes = get_object_size(
            minio_client,
            object_name,
        )

        total_bytes += size_bytes

        objects.append(
            {
                "object_name": object_name,
                "size_bytes": size_bytes,
            }
        )

    return {
        "object_count": len(object_names),
        "total_bytes": total_bytes,
        "total_kilobytes": round(
            total_bytes / 1024,
            4,
        ),
        "total_megabytes": round(
            total_bytes / (1024 * 1024),
            6,
        ),
        "objects": objects,
    }


def collect_job(
    minio_client: Any,
    job_name: str,
    run_id: str,
) -> Dict[str, Any]:

    manifest = get_manifest(
        minio_client,
        job_name,
        run_id,
    )

    lineage = get_lineage(
        minio_client,
        job_name,
        run_id,
    )

    input_objects = manifest.get(
        "input_objects",
        [],
    )

    output_objects = manifest.get(
        "output_objects",
        [],
    )

    return {
        "job_name": job_name,
        "run_id": run_id,
        "duration_seconds": (
            calculate_duration(
                manifest
            )
        ),
        "metrics": manifest.get(
            "metrics",
            {},
        ),
        "input_storage": measure_objects(
            minio_client,
            input_objects,
        ),
        "output_storage": measure_objects(
            minio_client,
            output_objects,
        ),
        "manifest_object": manifest.get(
            "_manifest_object"
        ),
        "lineage": {
            "available": (
                lineage is not None
            ),
            "parent_run_ids": (
                lineage.get(
                    "parent_run_ids",
                    [],
                )
                if lineage
                else []
            ),
            "parent_run_count": (
                len(
                    lineage.get(
                        "parent_run_ids",
                        [],
                    )
                )
                if lineage
                else 0
            ),
        },
    }


def build_report(
    minio_client: Any,
    args: argparse.Namespace,
) -> Dict[str, Any]:

    jobs = {
        "field_bronze": collect_job(
            minio_client,
            FIELD_BRONZE_JOB,
            args.field_bronze_run_id,
        ),

        "field_silver": collect_job(
            minio_client,
            FIELD_SILVER_JOB,
            args.field_silver_run_id,
        ),

        "weather_bronze": collect_job(
            minio_client,
            WEATHER_BRONZE_JOB,
            args.weather_bronze_run_id,
        ),

        "weather_silver": collect_job(
            minio_client,
            WEATHER_SILVER_JOB,
            args.weather_silver_run_id,
        ),

        "integration": collect_job(
            minio_client,
            INTEGRATION_JOB,
            args.integration_run_id,
        ),

        "integrated_gold": collect_job(
            minio_client,
            GOLD_JOB,
            args.gold_run_id,
        ),
    }

    total_output_bytes = sum(
        job["output_storage"]["total_bytes"]
        for job in jobs.values()
    )

    bounded_jobs = [
        jobs["field_silver"],
        jobs["weather_silver"],
        jobs["integration"],
        jobs["integrated_gold"],
    ]

    bounded_durations = [
        job["duration_seconds"]
        for job in bounded_jobs
        if job["duration_seconds"] is not None
    ]

    integration_metrics = (
        jobs["integration"]["metrics"]
    )

    gold_metrics = (
        jobs["integrated_gold"]["metrics"]
    )

    field_silver_metrics = (
        jobs["field_silver"]["metrics"]
    )

    weather_silver_metrics = (
        jobs["weather_silver"]["metrics"]
    )

    transformation_jobs = [
        jobs["field_silver"],
        jobs["weather_silver"],
        jobs["integration"],
        jobs["integrated_gold"],
    ]

    lineage_count = sum(
        1
        for job in transformation_jobs
        if job["lineage"]["available"]
    )

    lineage_coverage = (
        lineage_count
        / len(transformation_jobs)
    )

    return {
        "experiment_name": (
            args.experiment_name
        ),
        "environment": "evaluation",
        "bucket": MINIO_BUCKET,
        "generated_at": (
            datetime.now()
            .astimezone()
            .isoformat()
        ),

        "run_ids": {
            "field_bronze": (
                args.field_bronze_run_id
            ),
            "field_silver": (
                args.field_silver_run_id
            ),
            "weather_bronze": (
                args.weather_bronze_run_id
            ),
            "weather_silver": (
                args.weather_silver_run_id
            ),
            "integration": (
                args.integration_run_id
            ),
            "integrated_gold": (
                args.gold_run_id
            ),
        },

        "summary": {
            "field_input_records": (
                field_silver_metrics.get(
                    "input_records"
                )
            ),

            "field_accepted_records": (
                field_silver_metrics.get(
                    "accepted_records"
                )
            ),

            "field_quarantined_records": (
                field_silver_metrics.get(
                    "quarantined_records"
                )
            ),

            "weather_input_records": (
                weather_silver_metrics.get(
                    "input_records"
                )
            ),

            "weather_accepted_records": (
                weather_silver_metrics.get(
                    "accepted_records"
                )
            ),

            "weather_quarantined_records": (
                weather_silver_metrics.get(
                    "quarantined_records"
                )
            ),

            "integrated_records": (
                integration_metrics.get(
                    "integrated_output_records"
                )
            ),

            "matched_records": (
                integration_metrics.get(
                    "matched_field_records"
                )
            ),

            "unmatched_records": (
                integration_metrics.get(
                    "unmatched_field_records"
                )
            ),

            "integration_match_rate": (
                integration_metrics.get(
                    "weather_match_rate"
                )
            ),

            "gold_total_records": (
                gold_metrics.get(
                    "total_records"
                )
            ),

            "normal_records": (
                gold_metrics.get(
                    "normal_records"
                )
            ),

            "anomaly_records": (
                gold_metrics.get(
                    "anomaly_records"
                )
            ),

            "anomaly_rate": (
                gold_metrics.get(
                    "anomaly_rate"
                )
            ),

            "weather_matched_anomalies": (
                gold_metrics.get(
                    "weather_matched_anomalies"
                )
            ),

            "bounded_processing_seconds": round(
                sum(bounded_durations),
                4,
            ),

            "run_output_storage_bytes": (
                total_output_bytes
            ),

            "run_output_storage_megabytes": round(
                total_output_bytes
                / (1024 * 1024),
                6,
            ),

            "transformation_jobs": (
                len(transformation_jobs)
            ),

            "jobs_with_lineage": (
                lineage_count
            ),

            "lineage_coverage": round(
                lineage_coverage,
                4,
            ),
        },

        "jobs": jobs,
    }


def save_report(
    report: Dict[str, Any],
    experiment_name: str,
) -> Path:

    output_dir = (
        PROJECT_ROOT
        / "experiments"
        / "paper-evaluation"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir
        / f"{experiment_name}.json"
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            report,
            file,
            indent=2,
            ensure_ascii=False,
        )

    return output_path


def save_csv(
    report: Dict[str, Any],
    experiment_name: str,
) -> Path:

    output_dir = (
        PROJECT_ROOT
        / "experiments"
        / "paper-evaluation"
    )

    output_path = (
        output_dir
        / f"{experiment_name}_jobs.csv"
    )

    lines = [
        (
            "job,run_id,duration_seconds,"
            "output_objects,output_bytes,"
            "lineage,parent_count"
        )
    ]

    for key, job in report["jobs"].items():

        lines.append(
            ",".join(
                [
                    key,
                    str(job["run_id"]),
                    str(
                        job[
                            "duration_seconds"
                        ]
                    ),
                    str(
                        job[
                            "output_storage"
                        ][
                            "object_count"
                        ]
                    ),
                    str(
                        job[
                            "output_storage"
                        ][
                            "total_bytes"
                        ]
                    ),
                    str(
                        job[
                            "lineage"
                        ][
                            "available"
                        ]
                    ),
                    str(
                        job[
                            "lineage"
                        ][
                            "parent_run_count"
                        ]
                    ),
                ]
            )
        )

    output_path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )

    return output_path


def print_summary(
    report: Dict[str, Any],
) -> None:

    summary = report["summary"]

    print("\nPaper evaluation summary")
    print("------------------------")

    print(
        "Field records: "
        f"{summary['field_input_records']}"
    )

    print(
        "Field accepted: "
        f"{summary['field_accepted_records']}"
    )

    print(
        "Field quarantined: "
        f"{summary['field_quarantined_records']}"
    )

    print(
        "Weather records: "
        f"{summary['weather_input_records']}"
    )

    print(
        "Weather accepted: "
        f"{summary['weather_accepted_records']}"
    )

    print(
        "Weather quarantined: "
        f"{summary['weather_quarantined_records']}"
    )

    print(
        "Integrated records: "
        f"{summary['integrated_records']}"
    )

    print(
        "Matched records: "
        f"{summary['matched_records']}"
    )

    print(
        "Integration match rate: "
        f"{summary['integration_match_rate']}"
    )

    print(
        "Gold records: "
        f"{summary['gold_total_records']}"
    )

    print(
        "Anomalies: "
        f"{summary['anomaly_records']}"
    )

    print(
        "Bounded processing time: "
        f"{summary['bounded_processing_seconds']} s"
    )

    print(
        "Run output storage: "
        f"{summary['run_output_storage_megabytes']} MB"
    )

    print(
        "Lineage coverage: "
        f"{summary['lineage_coverage']}"
    )


def main() -> None:

    args = parse_arguments()

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client)

    report = build_report(
        minio_client,
        args,
    )

    json_path = save_report(
        report,
        args.experiment_name,
    )

    csv_path = save_csv(
        report,
        args.experiment_name,
    )

    print_summary(report)

    print("\nJSON report:")
    print(json_path)

    print("\nCSV report:")
    print(csv_path)


if __name__ == "__main__":
    main()