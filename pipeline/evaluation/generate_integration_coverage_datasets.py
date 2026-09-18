import argparse
import json
import os
import random


COVERAGE_LEVELS = [1.00, 0.90, 0.70, 0.50]


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Generate Weather coverage datasets for integration evaluation."
    )

    parser.add_argument(
        "--input-file",
        required=True,
        help="Path to the original Weather JSON file.",
    )

    parser.add_argument(
        "--output-dir",
        default="evaluation/datasets/integration_coverage",
        help="Directory for generated evaluation datasets.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed.",
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    random.seed(args.seed)

    with open(args.input_file, "r") as file:
        weather_records = json.load(file)

    os.makedirs(args.output_dir, exist_ok=True)

    total_records = len(weather_records)

    print("Original Weather records:", total_records)

    for coverage in COVERAGE_LEVELS:

        keep_count = int(total_records * coverage)

        # Start from the same shuffled order so that lower
        # coverage datasets are subsets of higher coverage datasets.
        indices = list(range(total_records))

        random_generator = random.Random(args.seed)
        random_generator.shuffle(indices)

        selected_indices = set(indices[:keep_count])

        selected_records = [
            record
            for index, record in enumerate(weather_records)
            if index in selected_indices
        ]

        percentage = int(coverage * 100)

        output_file = os.path.join(
            args.output_dir,
            "weather_coverage_{}pct.json".format(percentage),
        )

        with open(output_file, "w") as file:
            json.dump(selected_records, file, indent=2)

        print(
            "{}% coverage: {} records -> {}".format(
                percentage,
                len(selected_records),
                output_file,
            )
        )


if __name__ == "__main__":
    main()