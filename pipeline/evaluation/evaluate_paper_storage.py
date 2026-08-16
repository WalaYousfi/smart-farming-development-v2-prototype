import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List

from pipeline.common.config import (
    MINIO_BUCKET,
    PROJECT_ROOT,
)
from pipeline.common.minio_client import (
    create_minio_client,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure storage usage for one isolated "
            "publication-oriented pipeline execution."
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
        default="paper-storage-run-01",
    )

    return parser.parse_args()


def list_matching_objects(
    minio_client: Any,
    prefix: str,
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Return MinIO objects below prefix belonging
    to the requested run.
    """

    objects = minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    marker = f"run_id={run_id}/"

    results = []

    for obj in objects:
        if marker not in obj.object_name:
            continue

        results.append(
            {
                "object_name": obj.object_name,
                "size_bytes": int(
                    obj.size or 0
                ),
            }
        )

    return results


def measure_category(
    objects: List[Dict[str, Any]],
) -> Dict[str, Any]:
    total_bytes = sum(
        item["size_bytes"]
        for item in objects
    )

    return {
        "object_count": len(objects),
        "total_bytes": total_bytes,
        "total_kilobytes": round(
            total_bytes / 1024,
            4,
        ),
        "total_megabytes": round(
            total_bytes
            / (1024 * 1024),
            6,
        ),
        "objects": objects,
    }


def collect_multiple_runs(
    minio_client: Any,
    prefix: str,
    run_ids: List[str],
) -> Dict[str, Any]:
    """
    Combine objects belonging to several run IDs
    under one architectural category.
    """

    objects = []

    seen = set()

    for run_id in run_ids:
        matches = list_matching_objects(
            minio_client=minio_client,
            prefix=prefix,
            run_id=run_id,
        )

        for item in matches:
            if item["object_name"] in seen:
                continue

            seen.add(
                item["object_name"]
            )

            objects.append(item)

    return measure_category(objects)


def percentage(
    part: int,
    total: int,
) -> float:
    if total == 0:
        return 0.0

    return round(
        part / total * 100,
        4,
    )


def build_report(
    minio_client: Any,
    args: argparse.Namespace,
) -> Dict[str, Any]:

    transformation_run_ids = [
        args.field_silver_run_id,
        args.weather_silver_run_id,
        args.integration_run_id,
        args.gold_run_id,
    ]

    all_run_ids = [
        args.field_bronze_run_id,
        args.field_silver_run_id,
        args.weather_bronze_run_id,
        args.weather_silver_run_id,
        args.integration_run_id,
        args.gold_run_id,
    ]

    categories = {
        "bronze": collect_multiple_runs(
            minio_client,
            "bronze/",
            [
                args.field_bronze_run_id,
                args.weather_bronze_run_id,
            ],
        ),

        "accepted_silver": collect_multiple_runs(
            minio_client,
            "silver/accepted/",
            [
                args.field_silver_run_id,
                args.weather_silver_run_id,
            ],
        ),

        "quarantine": collect_multiple_runs(
            minio_client,
            "silver/quarantine/",
            all_run_ids,
        ),

        "integrated_silver": collect_multiple_runs(
            minio_client,
            "silver/integrated/",
            [
                args.integration_run_id,
            ],
        ),

        "gold_analytical": collect_multiple_runs(
            minio_client,
            "gold/analytical/",
            [
                args.gold_run_id,
            ],
        ),

        "gold_products": collect_multiple_runs(
            minio_client,
            "gold/data-products/",
            [
                args.gold_run_id,
            ],
        ),

        "manifests": collect_multiple_runs(
            minio_client,
            "metadata/manifests/",
            all_run_ids,
        ),

        "lineage": collect_multiple_runs(
            minio_client,
            "metadata/lineage/",
            transformation_run_ids,
        ),

        "quality_reports": collect_multiple_runs(
            minio_client,
            "metadata/quality-reports/",
            [
                args.field_silver_run_id,
                args.weather_silver_run_id,
                args.integration_run_id,
            ],
        ),
    }

    data_categories = [
        "bronze",
        "accepted_silver",
        "quarantine",
        "integrated_silver",
        "gold_analytical",
        "gold_products",
    ]

    metadata_categories = [
        "manifests",
        "lineage",
        "quality_reports",
    ]

    data_bytes = sum(
        categories[name]["total_bytes"]
        for name in data_categories
    )

    metadata_bytes = sum(
        categories[name]["total_bytes"]
        for name in metadata_categories
    )

    total_bytes = (
        data_bytes
        + metadata_bytes
    )

    data_objects = sum(
        categories[name]["object_count"]
        for name in data_categories
    )

    metadata_objects = sum(
        categories[name]["object_count"]
        for name in metadata_categories
    )

    for category in categories.values():
        category["share_of_total_storage_percent"] = (
            percentage(
                category["total_bytes"],
                total_bytes,
            )
        )

    metadata_to_data_ratio = (
        round(
            metadata_bytes / data_bytes,
            6,
        )
        if data_bytes
        else 0.0
    )

    return {
        "experiment": (
            "isolated_run_storage_overhead"
        ),
        "experiment_name": (
            args.experiment_name
        ),
        "generated_at": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        "bucket": MINIO_BUCKET,
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
            "gold": (
                args.gold_run_id
            ),
        },
        "summary": {
            "total_storage_bytes": (
                total_bytes
            ),
            "total_storage_megabytes": round(
                total_bytes
                / (1024 * 1024),
                6,
            ),
            "data_storage_bytes": (
                data_bytes
            ),
            "data_storage_megabytes": round(
                data_bytes
                / (1024 * 1024),
                6,
            ),
            "metadata_storage_bytes": (
                metadata_bytes
            ),
            "metadata_storage_megabytes": round(
                metadata_bytes
                / (1024 * 1024),
                6,
            ),
            "metadata_to_data_storage_ratio": (
                metadata_to_data_ratio
            ),
            "metadata_storage_percentage": (
                percentage(
                    metadata_bytes,
                    total_bytes,
                )
            ),
            "data_object_count": (
                data_objects
            ),
            "metadata_object_count": (
                metadata_objects
            ),
            "total_object_count": (
                data_objects
                + metadata_objects
            ),
        },
        "categories": categories,
    }


def save_json(
    report: Dict[str, Any],
    experiment_name: str,
) -> Path:

    output_directory = (
        PROJECT_ROOT
        / "experiments"
        / "paper-evaluation"
        / "storage"
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_directory
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

    output_directory = (
        PROJECT_ROOT
        / "experiments"
        / "paper-evaluation"
        / "storage"
    )

    output_path = (
        output_directory
        / f"{experiment_name}.csv"
    )

    lines = [
        (
            "category,object_count,"
            "total_bytes,total_kilobytes,"
            "total_megabytes,"
            "share_of_total_storage_percent"
        )
    ]

    for (
        category_name,
        category,
    ) in report["categories"].items():

        lines.append(
            ",".join(
                [
                    category_name,
                    str(
                        category[
                            "object_count"
                        ]
                    ),
                    str(
                        category[
                            "total_bytes"
                        ]
                    ),
                    str(
                        category[
                            "total_kilobytes"
                        ]
                    ),
                    str(
                        category[
                            "total_megabytes"
                        ]
                    ),
                    str(
                        category[
                            "share_of_total_storage_percent"
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

    print("\nISOLATED STORAGE EVALUATION")
    print("===========================")

    print(
        "\nTotal storage: "
        f"{summary['total_storage_megabytes']} MB"
    )

    print(
        "Data storage: "
        f"{summary['data_storage_megabytes']} MB"
    )

    print(
        "Metadata storage: "
        f"{summary['metadata_storage_megabytes']} MB"
    )

    print(
        "Metadata/data ratio: "
        f"{summary['metadata_to_data_storage_ratio']}"
    )

    print(
        "Metadata storage percentage: "
        f"{summary['metadata_storage_percentage']}%"
    )

    print(
        "Data objects: "
        f"{summary['data_object_count']}"
    )

    print(
        "Metadata objects: "
        f"{summary['metadata_object_count']}"
    )

    print("\nCategories")
    print("----------")

    for (
        name,
        category,
    ) in report["categories"].items():

        print(
            f"{name}: "
            f"{category['object_count']} objects | "
            f"{category['total_kilobytes']} KB | "
            f"{category['share_of_total_storage_percent']}%"
        )


def main() -> None:

    args = parse_arguments()

    minio_client = (
        create_minio_client()
    )

    report = build_report(
        minio_client,
        args,
    )

    json_path = save_json(
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