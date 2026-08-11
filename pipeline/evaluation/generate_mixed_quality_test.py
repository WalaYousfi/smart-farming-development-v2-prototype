import argparse
import json
from pathlib import Path

import pandas as pd

from pipeline.common.config import PROJECT_ROOT


OUTPUT_DIR = (
    PROJECT_ROOT
    / "experiments"
    / "paper-evaluation"
    / "quality-stress-test"
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Create a controlled mixed valid/invalid "
            "dataset for Silver quality evaluation."
        )
    )

    parser.add_argument(
        "--source-file",
        required=True,
    )

    parser.add_argument(
        "--invalid-file",
        required=True,
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    source_path = Path(args.source_file)
    invalid_path = Path(args.invalid_file)

    if not source_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_path}"
        )

    if not invalid_path.exists():
        raise FileNotFoundError(
            f"Invalid test file not found: {invalid_path}"
        )

    source_df = pd.read_csv(source_path)
    invalid_df = pd.read_csv(invalid_path)

    if len(source_df) < 40:
        raise ValueError(
            "Source dataset must contain at least 40 records."
        )

    if len(invalid_df) != 20:
        raise ValueError(
            "Expected exactly 20 controlled invalid records."
        )

    # Use different source rows from those used to create
    # the invalid observations.
    valid_df = (
        source_df
        .iloc[20:40]
        .copy()
    )

    valid_df["_quality_test_id"] = [
        f"valid-{number:02d}"
        for number in range(1, 21)
    ]

    valid_df["_injected_defect"] = "none"
    valid_df["_injected_column"] = "none"

    valid_df["_expected_result"] = "accepted"

    invalid_df = invalid_df.copy()

    invalid_df["_expected_result"] = (
        "quarantined"
    )

    mixed_df = pd.concat(
        [
            valid_df,
            invalid_df,
        ],
        ignore_index=True,
    )

    # Fixed seed makes the experiment reproducible.
    mixed_df = mixed_df.sample(
        frac=1,
        random_state=42,
    ).reset_index(drop=True)

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        OUTPUT_DIR
        / "mixed_quality_test_40.csv"
    )

    ground_truth_path = (
        OUTPUT_DIR
        / "mixed_quality_ground_truth.json"
    )

    mixed_df.to_csv(
        output_path,
        index=False,
    )

    ground_truth = {
        "experiment": (
            "mixed_field_quality_classification"
        ),
        "total_records": 40,
        "valid_records": 20,
        "invalid_records": 20,
        "expected_accepted": 20,
        "expected_quarantined": 20,
        "shuffle_random_state": 42,
        "records": [],
    }

    for _, row in mixed_df.iterrows():

        ground_truth["records"].append(
            {
                "test_id": row[
                    "_quality_test_id"
                ],
                "defect": row[
                    "_injected_defect"
                ],
                "expected_result": row[
                    "_expected_result"
                ],
            }
        )

    with ground_truth_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            ground_truth,
            file,
            indent=2,
        )

    print("\nMixed quality dataset generated.")
    print("--------------------------------")

    print(f"Total records: {len(mixed_df)}")
    print("Known valid records: 20")
    print("Known invalid records: 20")

    print("\nExpected Silver result:")
    print("Accepted: 20")
    print("Quarantined: 20")

    print("\nDataset:")
    print(output_path)

    print("\nGround truth:")
    print(ground_truth_path)


if __name__ == "__main__":
    main()