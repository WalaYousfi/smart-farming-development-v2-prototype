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
    ensure_bucket_exists,
)


DEFAULT_PREFIXES = {
    "bronze_data": [
        "bronze/",
    ],
    "silver_accepted": [
        "silver/accepted/",
    ],
    "silver_quarantine": [
        "silver/quarantine/",
    ],
    "silver_integrated": [
        "silver/integrated/",
    ],
    "gold_analytical": [
        "gold/analytical/",
    ],
    "gold_data_products": [
        "gold/data-products/",
    ],
    "metadata_manifests": [
        "metadata/manifests/",
    ],
    "metadata_lineage": [
        "metadata/lineage/",
    ],
    "metadata_quality_reports": [
        "metadata/quality-reports/",
    ],
    "metadata_traceability": [
        "metadata/traceability/",
    ],
    "metadata_schemas": [
        "metadata/schemas/",
    ],
}


def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def parse_arguments() -> argparse.Namespace:
    """
    Read storage-evaluation settings.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Measure MinIO storage usage by "
            "architectural category."
        )
    )

    parser.add_argument(
        "--experiment-name",
        default="storage-overhead-v2",
        help=(
            "Name used for the generated report."
        ),
    )

    return parser.parse_args()


def bytes_to_kilobytes(
    size_bytes: int,
) -> float:
    return round(
        size_bytes / 1024,
        4,
    )


def bytes_to_megabytes(
    size_bytes: int,
) -> float:
    return round(
        size_bytes / (1024 * 1024),
        6,
    )


def list_objects_for_prefix(
    minio_client: Any,
    prefix: str,
) -> List[Dict[str, Any]]:
    """
    Return object names and sizes for one prefix.
    """

    objects = minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    results = []

    for obj in objects:
        size_bytes = int(
            obj.size or 0
        )

        results.append(
            {
                "object_name": obj.object_name,
                "prefix": prefix,
                "size_bytes": size_bytes,
                "size_kilobytes": (
                    bytes_to_kilobytes(
                        size_bytes
                    )
                ),
                "size_megabytes": (
                    bytes_to_megabytes(
                        size_bytes
                    )
                ),
                "last_modified": (
                    obj.last_modified.isoformat()
                    if obj.last_modified
                    else None
                ),
            }
        )

    return results


def collect_category_metrics(
    minio_client: Any,
    category_name: str,
    prefixes: List[str],
) -> Dict[str, Any]:
    """
    Measure one architectural storage category.
    """

    category_objects = []

    for prefix in prefixes:
        category_objects.extend(
            list_objects_for_prefix(
                minio_client=minio_client,
                prefix=prefix,
            )
        )

    total_bytes = sum(
        obj["size_bytes"]
        for obj in category_objects
    )

    return {
        "category": category_name,
        "prefixes": prefixes,
        "object_count": len(
            category_objects
        ),
        "total_bytes": total_bytes,
        "total_kilobytes": (
            bytes_to_kilobytes(
                total_bytes
            )
        ),
        "total_megabytes": (
            bytes_to_megabytes(
                total_bytes
            )
        ),
        "objects": category_objects,
    }


def calculate_ratio(
    numerator: int,
    denominator: int,
) -> float:
    """
    Calculate a safe ratio.
    """

    if denominator == 0:
        return 0.0

    return round(
        numerator / denominator,
        6,
    )


def build_report(
    minio_client: Any,
    experiment_name: str,
) -> Dict[str, Any]:
    """
    Build the complete storage-overhead report.
    """

    categories = {}

    for category_name, prefixes in (
        DEFAULT_PREFIXES.items()
    ):
        categories[category_name] = (
            collect_category_metrics(
                minio_client=minio_client,
                category_name=category_name,
                prefixes=prefixes,
            )
        )

    data_category_names = [
        "bronze_data",
        "silver_accepted",
        "silver_quarantine",
        "silver_integrated",
        "gold_analytical",
        "gold_data_products",
    ]

    metadata_category_names = [
        "metadata_manifests",
        "metadata_lineage",
        "metadata_quality_reports",
        "metadata_traceability",
        "metadata_schemas",
    ]

    total_data_bytes = sum(
        categories[name]["total_bytes"]
        for name in data_category_names
    )

    total_metadata_bytes = sum(
        categories[name]["total_bytes"]
        for name in metadata_category_names
    )

    total_storage_bytes = (
        total_data_bytes
        + total_metadata_bytes
    )

    total_data_objects = sum(
        categories[name]["object_count"]
        for name in data_category_names
    )

    total_metadata_objects = sum(
        categories[name]["object_count"]
        for name in metadata_category_names
    )

    total_objects = (
        total_data_objects
        + total_metadata_objects
    )

    return {
        "experiment_name": experiment_name,
        "experiment_type": (
            "architectural_storage_overhead"
        ),
        "architecture": (
            "proposed dual-dimensional V2"
        ),
        "bucket": MINIO_BUCKET,
        "generated_at": utc_now(),
        "summary": {
            "total_storage_bytes": (
                total_storage_bytes
            ),
            "total_storage_kilobytes": (
                bytes_to_kilobytes(
                    total_storage_bytes
                )
            ),
            "total_storage_megabytes": (
                bytes_to_megabytes(
                    total_storage_bytes
                )
            ),
            "total_object_count": total_objects,
            "data_storage_bytes": (
                total_data_bytes
            ),
            "data_storage_megabytes": (
                bytes_to_megabytes(
                    total_data_bytes
                )
            ),
            "data_object_count": (
                total_data_objects
            ),
            "metadata_storage_bytes": (
                total_metadata_bytes
            ),
            "metadata_storage_megabytes": (
                bytes_to_megabytes(
                    total_metadata_bytes
                )
            ),
            "metadata_object_count": (
                total_metadata_objects
            ),
            "metadata_to_data_storage_ratio": (
                calculate_ratio(
                    total_metadata_bytes,
                    total_data_bytes,
                )
            ),
            "metadata_storage_percentage": (
                round(
                    calculate_ratio(
                        total_metadata_bytes,
                        total_storage_bytes,
                    )
                    * 100,
                    4,
                )
            ),
            "metadata_object_percentage": (
                round(
                    calculate_ratio(
                        total_metadata_objects,
                        total_objects,
                    )
                    * 100,
                    4,
                )
            ),
        },
        "categories": categories,
    }


def save_json_report(
    report: Dict[str, Any],
    experiment_name: str,
) -> Path:
    """
    Save the detailed storage report.
    """

    output_directory = (
        PROJECT_ROOT
        / "experiments"
        / "prototype-v2"
        / "storage"
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    safe_name = (
        experiment_name
        .strip()
        .replace(" ", "_")
    )

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

    output_path = (
        output_directory
        / f"{safe_name}_{timestamp}.json"
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as output_file:
        json.dump(
            report,
            output_file,
            indent=2,
            ensure_ascii=False,
        )

    return output_path


def save_csv_summary(
    report: Dict[str, Any],
    json_output_path: Path,
) -> Path:
    """
    Save one row per category for paper tables.
    """

    csv_output_path = (
        json_output_path.with_suffix(
            ".csv"
        )
    )

    lines = [
        (
            "category,object_count,total_bytes,"
            "total_kilobytes,total_megabytes"
        )
    ]

    for category in (
        report["categories"].values()
    ):
        lines.append(
            ",".join(
                [
                    category["category"],
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
                ]
            )
        )

    csv_output_path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )

    return csv_output_path


def print_summary(
    report: Dict[str, Any],
) -> None:
    """
    Display paper-relevant storage findings.
    """

    summary = report["summary"]

    print("\nStorage-overhead summary")
    print("------------------------")

    print(
        "Total storage: "
        f"{summary['total_storage_megabytes']} MB"
    )

    print(
        "Total objects: "
        f"{summary['total_object_count']}"
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
        "Metadata-to-data storage ratio: "
        f"{summary['metadata_to_data_storage_ratio']}"
    )

    print(
        "Metadata storage percentage: "
        f"{summary['metadata_storage_percentage']}%"
    )

    print(
        "Metadata object percentage: "
        f"{summary['metadata_object_percentage']}%"
    )

    print("\nStorage by category:")

    for category in (
        report["categories"].values()
    ):
        print(
            f"  {category['category']}: "
            f"{category['object_count']} objects, "
            f"{category['total_megabytes']} MB"
        )


def main() -> None:
    arguments = parse_arguments()

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client)

    report = build_report(
        minio_client=minio_client,
        experiment_name=(
            arguments.experiment_name
        ),
    )

    json_output_path = save_json_report(
        report=report,
        experiment_name=(
            arguments.experiment_name
        ),
    )

    csv_output_path = save_csv_summary(
        report=report,
        json_output_path=json_output_path,
    )

    print_summary(report)

    print("\nDetailed JSON report:")
    print(json_output_path)

    print("\nPaper-ready CSV summary:")
    print(csv_output_path)


if __name__ == "__main__":
    main()