import argparse
import json
import random
from pathlib import Path

import pandas as pd


DEFECT_TYPES = [
    "invalid_soil_moisture",
    "invalid_humidity",
    "invalid_soil_ph",
    "invalid_ndvi",
    "missing_sensor_id",
    "malformed_timestamp",
    "negative_rainfall",
    "harvest_before_sowing",
]


def inject_defect(df, row_index, defect_type):
    if defect_type == "invalid_soil_moisture":
        df.at[row_index, "soil_moisture_%"] = -0.5

    elif defect_type == "invalid_humidity":
        df.at[row_index, "humidity_%"] = 100.5

    elif defect_type == "invalid_soil_ph":
        df.at[row_index, "soil_pH"] = 14.2

    elif defect_type == "invalid_ndvi":
        df.at[row_index, "NDVI_index"] = 1.05

    elif defect_type == "missing_sensor_id":
        df.at[row_index, "sensor_id"] = None

    elif defect_type == "malformed_timestamp":
        df.at[row_index, "timestamp"] = "invalid_timestamp"

    elif defect_type == "negative_rainfall":
        df.at[row_index, "rainfall_mm"] = -2.0

    elif defect_type == "harvest_before_sowing":
        df.at[row_index, "sowing_date"] = "2024-08-15"
        df.at[row_index, "harvest_date"] = "2024-06-15"

    else:
        raise ValueError("Unknown defect type: {}".format(defect_type))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        default="data/source/Smart_Farming_Crop_Yield_2024.csv",
    )

    parser.add_argument(
        "--output-dir",
        default="evaluation/datasets/quality_robustness",
    )

    parser.add_argument(
        "--corruption-rate",
        type=float,
        default=0.20,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    args = parser.parse_args()

    random_generator = random.Random(args.seed)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path)

    total_records = len(df)

    number_to_corrupt = int(
        round(total_records * args.corruption_rate)
    )

    # Evaluation-only identifiers.
    # These allow us to reconstruct the exact ground truth later.
    df["_quality_test_id"] = [
        "quality_eval_{:04d}".format(i)
        for i in range(total_records)
    ]

    df["_quality_expected_invalid"] = False
    df["_quality_defect_type"] = "none"

    candidate_indices = list(df.index)

    corrupted_indices = random_generator.sample(
        candidate_indices,
        number_to_corrupt,
    )

    # Balance defect categories approximately equally.
    defect_assignments = []

    for i in range(number_to_corrupt):
        defect_assignments.append(
            DEFECT_TYPES[i % len(DEFECT_TYPES)]
        )

    random_generator.shuffle(defect_assignments)

    ground_truth_rows = []

    for row_index, defect_type in zip(
        corrupted_indices,
        defect_assignments,
    ):
        test_id = df.at[row_index, "_quality_test_id"]

        inject_defect(
            df,
            row_index,
            defect_type,
        )

        df.at[
            row_index,
            "_quality_expected_invalid"
        ] = True

        df.at[
            row_index,
            "_quality_defect_type"
        ] = defect_type

        ground_truth_rows.append(
            {
                "quality_test_id": test_id,
                "row_index": int(row_index),
                "expected_invalid": True,
                "defect_type": defect_type,
            }
        )

    # Add valid observations to ground truth too.
    corrupted_index_set = set(corrupted_indices)

    for row_index in df.index:
        if row_index not in corrupted_index_set:
            ground_truth_rows.append(
                {
                    "quality_test_id": df.at[
                        row_index,
                        "_quality_test_id"
                    ],
                    "row_index": int(row_index),
                    "expected_invalid": False,
                    "defect_type": "none",
                }
            )

    corruption_percent = int(
        args.corruption_rate * 100
    )

    dataset_name = (
        "quality_pilot_{}pct_seed_{}.csv".format(
            corruption_percent,
            args.seed,
        )
    )

    truth_name = (
        "quality_pilot_{}pct_seed_{}_ground_truth.csv".format(
            corruption_percent,
            args.seed,
        )
    )

    summary_name = (
        "quality_pilot_{}pct_seed_{}_summary.json".format(
            corruption_percent,
            args.seed,
        )
    )

    dataset_path = output_dir / dataset_name
    truth_path = output_dir / truth_name
    summary_path = output_dir / summary_name

    df.to_csv(
        dataset_path,
        index=False,
    )

    ground_truth_df = pd.DataFrame(
        ground_truth_rows
    ).sort_values("row_index")

    ground_truth_df.to_csv(
        truth_path,
        index=False,
    )

    defect_counts = (
        ground_truth_df[
            ground_truth_df["expected_invalid"] == True
        ]["defect_type"]
        .value_counts()
        .to_dict()
    )

    summary = {
        "experiment": "quality_robustness_pilot",
        "source_dataset": str(input_path),
        "seed": args.seed,
        "total_records": total_records,
        "corruption_rate": args.corruption_rate,
        "corrupted_records": number_to_corrupt,
        "valid_records": total_records - number_to_corrupt,
        "defect_counts": defect_counts,
        "dataset_path": str(dataset_path),
        "ground_truth_path": str(truth_path),
    }

    with open(
        summary_path,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            summary,
            file,
            indent=2,
        )

    print()
    print("Quality robustness dataset generated")
    print("------------------------------------")
    print("Total records:", total_records)
    print("Valid records:", total_records - number_to_corrupt)
    print("Corrupted records:", number_to_corrupt)
    print()
    print("Defect distribution:")

    for defect_type, count in sorted(
        defect_counts.items()
    ):
        print(
            "  {}: {}".format(
                defect_type,
                count,
            )
        )

    print()
    print("Dataset:")
    print(dataset_path)

    print()
    print("Ground truth:")
    print(truth_path)

    print()
    print("Summary:")
    print(summary_path)


if __name__ == "__main__":
    main()