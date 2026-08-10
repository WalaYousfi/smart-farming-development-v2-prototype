import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from pipeline.common.config import PROJECT_ROOT


QUALITY_TEST_DIR = (
    PROJECT_ROOT
    / "experiments"
    / "paper-evaluation"
    / "quality-stress-test"
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate controlled invalid agricultural "
            "records for Silver quality evaluation."
        )
    )

    parser.add_argument(
        "--source-file",
        required=True,
        help="Path to the original agricultural CSV.",
    )

    return parser.parse_args()


def create_test_record(
    source_row: pd.Series,
    test_id: str,
    defect_type: str,
    column: str,
    invalid_value: Any,
) -> Dict[str, Any]:
    """
    Create one intentionally corrupted record while
    preserving its expected defect as ground truth.
    """

    record = source_row.to_dict()

    record[column] = invalid_value

    record["_quality_test_id"] = test_id
    record["_injected_defect"] = defect_type
    record["_injected_column"] = column

    return record


def build_test_dataset(
    source_df: pd.DataFrame,
) -> List[Dict[str, Any]]:

    if len(source_df) < 20:
        raise ValueError(
            "Source dataset must contain at least 20 records."
        )

    records = []

    # Test 1:
    # Soil moisture should not be negative.
    for index in range(0, 5):
        records.append(
            create_test_record(
                source_df.iloc[index],
                f"soil-moisture-{index + 1:02d}",
                "invalid_soil_moisture",
                "soil_moisture_%",
                -10.0,
            )
        )

    # Test 2:
    # Humidity above 100% is invalid.
    for index in range(5, 10):
        records.append(
            create_test_record(
                source_df.iloc[index],
                f"humidity-{index - 4:02d}",
                "invalid_humidity",
                "humidity_%",
                150.0,
            )
        )

    # Test 3:
    # Use an impossible pH value.
    for index in range(10, 15):
        records.append(
            create_test_record(
                source_df.iloc[index],
                f"soil-ph-{index - 9:02d}",
                "invalid_soil_ph",
                "soil_pH",
                20.0,
            )
        )

    # Test 4:
    # NDVI normally falls between -1 and 1.
    for index in range(15, 20):
        records.append(
            create_test_record(
                source_df.iloc[index],
                f"ndvi-{index - 14:02d}",
                "invalid_ndvi",
                "NDVI_index",
                2.0,
            )
        )

    return records


def main() -> None:
    args = parse_arguments()

    source_path = Path(args.source_file)

    if not source_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_path}"
        )

    source_df = pd.read_csv(source_path)

    records = build_test_dataset(source_df)

    QUALITY_TEST_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    csv_path = (
        QUALITY_TEST_DIR
        / "controlled_invalid_field_records.csv"
    )

    json_path = (
        QUALITY_TEST_DIR
        / "quality_test_ground_truth.json"
    )

    pd.DataFrame(records).to_csv(
        csv_path,
        index=False,
    )

    ground_truth = {
        "experiment": (
            "controlled_field_quality_stress_test"
        ),
        "generated_at": (
            datetime.now(timezone.utc).isoformat()
        ),
        "source_file": str(source_path),
        "total_injected_records": len(records),
        "expected_quarantined_records": 20,
        "expected_accepted_records": 0,
        "defects": {
            "invalid_soil_moisture": 5,
            "invalid_humidity": 5,
            "invalid_soil_ph": 5,
            "invalid_ndvi": 5,
        },
        "records": [
            {
                "test_id": record[
                    "_quality_test_id"
                ],
                "defect": record[
                    "_injected_defect"
                ],
                "column": record[
                    "_injected_column"
                ],
            }
            for record in records
        ],
    }

    with json_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            ground_truth,
            file,
            indent=2,
        )

    print(
        "\nControlled quality dataset generated."
    )

    print(f"Records: {len(records)}")

    print("\nInjected defects:")
    print("  Invalid soil moisture: 5")
    print("  Invalid humidity: 5")
    print("  Invalid soil pH: 5")
    print("  Invalid NDVI: 5")

    print("\nExpected result:")
    print("  Accepted: 0")
    print("  Quarantined: 20")

    print(f"\nDataset: {csv_path}")
    print(f"Ground truth: {json_path}")


if __name__ == "__main__":
    main()