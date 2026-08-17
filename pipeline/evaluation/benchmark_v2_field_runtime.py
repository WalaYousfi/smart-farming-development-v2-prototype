import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

from pipeline.common.config import (
    APP_ENV,
    MINIO_BUCKET,
    PROJECT_ROOT,
)


DEFAULT_BRONZE_RUN_ID = (
    "20260809T122640Z_b09798ef"
)


def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark the V2 Field Bronze -> Silver -> "
            "Field Gold path using repeated immutable runs."
        )
    )

    parser.add_argument(
        "--bronze-run-id",
        default=DEFAULT_BRONZE_RUN_ID,
        help=(
            "Completed 500-record Field Bronze run "
            "used as the fixed benchmark input."
        ),
    )

    parser.add_argument(
        "--repetitions",
        type=int,
        default=5,
    )

    parser.add_argument(
        "--contamination",
        type=float,
        default=0.05,
    )

    parser.add_argument(
        "--experiment-name",
        default="proposed-v2-field-runtime-5-runs",
    )

    return parser.parse_args()


def validate_arguments(
    args: argparse.Namespace,
) -> None:

    if args.repetitions < 2:
        raise ValueError(
            "At least two repetitions are required."
        )

    if not 0 < args.contamination <= 0.5:
        raise ValueError(
            "Contamination must be greater than 0 "
            "and less than or equal to 0.5."
        )

    if APP_ENV != "evaluation":
        raise RuntimeError(
            "This benchmark must run in the "
            "evaluation environment.\n"
            f"Current APP_ENV: {APP_ENV}"
        )


def run_process(
    command: List[str],
    step_name: str,
) -> Dict[str, Any]:
    """
    Execute one V2 processing stage and measure
    external wall-clock execution time.
    """

    print("\nRunning:")
    print(" ".join(command))

    started_at = utc_now()

    start_time = time.perf_counter()

    process = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )

    duration_seconds = round(
        time.perf_counter()
        - start_time,
        4,
    )

    result = {
        "step": step_name,
        "started_at": started_at,
        "completed_at": utc_now(),
        "duration_seconds": (
            duration_seconds
        ),
        "return_code": (
            process.returncode
        ),
        "stdout": process.stdout,
        "stderr": process.stderr,
    }

    if process.returncode != 0:
        raise RuntimeError(
            f"{step_name} failed.\n\n"
            f"STDOUT:\n"
            f"{process.stdout}\n\n"
            f"STDERR:\n"
            f"{process.stderr}"
        )

    return result


def extract_value_after_label(
    output: str,
    label: str,
) -> str:
    """
    Extract a printed value such as:

    Run ID: ...
    Gold run ID: ...
    """

    for line in output.splitlines():

        cleaned = line.strip()

        if cleaned.startswith(label):

            value = cleaned.split(
                ":",
                1,
            )[1].strip()

            if value:
                return value

    raise RuntimeError(
        f"Could not extract value "
        f"for label: {label}"
    )


def validate_silver_output(
    output: str,
) -> str:
    """
    Confirm the fixed V2 Silver workload and return
    the generated Silver run ID.
    """

    required_fragments = [
        "Input records: 500",
        "Accepted records: 500",
        "Quarantined records: 0",
        "Overall quality score: 1.0",
    ]

    missing = [
        fragment
        for fragment in required_fragments
        if fragment not in output
    ]

    if missing:
        raise RuntimeError(
            "V2 Silver workload validation failed. "
            f"Missing: {missing}"
        )

    return extract_value_after_label(
        output=output,
        label="Run ID",
    )


def validate_gold_output(
    output: str,
) -> str:
    """
    Confirm the fixed V2 Gold workload and return
    the generated Gold run ID.
    """

    required_fragments = [
        "Total records: 500",
        "Normal records: 475",
        "Anomaly records: 25",
        "Anomaly rate: 0.05",
    ]

    missing = [
        fragment
        for fragment in required_fragments
        if fragment not in output
    ]

    if missing:
        raise RuntimeError(
            "V2 Gold workload validation failed. "
            f"Missing: {missing}"
        )

    return extract_value_after_label(
        output=output,
        label="Gold run ID",
    )


def calculate_statistics(
    values: List[float],
) -> Dict[str, Optional[float]]:

    if not values:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "minimum": None,
            "maximum": None,
            "standard_deviation": None,
            "coefficient_of_variation": None,
        }

    mean_value = statistics.mean(
        values
    )

    standard_deviation = (
        statistics.stdev(values)
        if len(values) >= 2
        else 0.0
    )

    coefficient_of_variation = (
        standard_deviation / mean_value
        if mean_value
        else 0.0
    )

    return {
        "count": len(values),
        "mean": round(
            mean_value,
            4,
        ),
        "median": round(
            statistics.median(values),
            4,
        ),
        "minimum": round(
            min(values),
            4,
        ),
        "maximum": round(
            max(values),
            4,
        ),
        "standard_deviation": round(
            standard_deviation,
            4,
        ),
        "coefficient_of_variation": round(
            coefficient_of_variation,
            4,
        ),
    }


def run_repetition(
    repetition_number: int,
    args: argparse.Namespace,
) -> Dict[str, Any]:

    print("\n" + "=" * 60)

    print(
        f"V2 Field repetition "
        f"{repetition_number}/{args.repetitions}"
    )

    print("=" * 60)

    # -----------------------------------------
    # Silver
    # -----------------------------------------

    silver_command = [
        sys.executable,
        "-m",
        "pipeline.silver.silver_processor",
        "--bronze-run-id",
        args.bronze_run_id,
        "--force",
    ]

    silver_result = run_process(
        command=silver_command,
        step_name="v2_field_silver",
    )

    silver_run_id = (
        validate_silver_output(
            silver_result["stdout"]
        )
    )

    print(
        "Silver validation: PASSED"
    )

    print(
        f"Silver run ID: "
        f"{silver_run_id}"
    )

    print(
        "Silver duration: "
        f"{silver_result['duration_seconds']} s"
    )

    # -----------------------------------------
    # Gold
    # -----------------------------------------

    gold_command = [
        sys.executable,
        "-m",
        "pipeline.gold.anomaly_detection",
        "--silver-run-id",
        silver_run_id,
        "--contamination",
        str(
            args.contamination
        ),
    ]

    gold_result = run_process(
        command=gold_command,
        step_name="v2_field_gold",
    )

    gold_run_id = (
        validate_gold_output(
            gold_result["stdout"]
        )
    )

    print(
        "Gold validation: PASSED"
    )

    print(
        f"Gold run ID: "
        f"{gold_run_id}"
    )

    print(
        "Gold duration: "
        f"{gold_result['duration_seconds']} s"
    )

    combined_duration = round(
        silver_result[
            "duration_seconds"
        ]
        + gold_result[
            "duration_seconds"
        ],
        4,
    )

    print(
        "Combined duration: "
        f"{combined_duration} s"
    )

    return {
        "repetition": repetition_number,

        "input": {
            "bronze_run_id": (
                args.bronze_run_id
            ),
            "field_records": 500,
        },

        "generated_runs": {
            "silver_run_id": (
                silver_run_id
            ),
            "gold_run_id": (
                gold_run_id
            ),
        },

        "validation": {
            "input_records": 500,
            "accepted_records": 500,
            "quarantined_records": 0,
            "gold_records": 500,
            "normal_records": 475,
            "anomaly_records": 25,
            "passed": True,
        },

        "durations": {
            "silver_seconds": (
                silver_result[
                    "duration_seconds"
                ]
            ),
            "gold_seconds": (
                gold_result[
                    "duration_seconds"
                ]
            ),
            "combined_seconds": (
                combined_duration
            ),
        },

        "steps": {
            "silver": silver_result,
            "gold": gold_result,
        },
    }


def build_summary(
    repetitions: List[
        Dict[str, Any]
    ],
) -> Dict[str, Any]:

    silver_values = [
        repetition[
            "durations"
        ][
            "silver_seconds"
        ]
        for repetition in repetitions
    ]

    gold_values = [
        repetition[
            "durations"
        ][
            "gold_seconds"
        ]
        for repetition in repetitions
    ]

    combined_values = [
        repetition[
            "durations"
        ][
            "combined_seconds"
        ]
        for repetition in repetitions
    ]

    return {
        "completed_repetitions": (
            len(repetitions)
        ),

        "silver_duration_seconds": (
            calculate_statistics(
                silver_values
            )
        ),

        "gold_duration_seconds": (
            calculate_statistics(
                gold_values
            )
        ),

        "combined_duration_seconds": (
            calculate_statistics(
                combined_values
            )
        ),
    }


def save_report(
    report: Dict[str, Any],
    experiment_name: str,
) -> Path:

    output_directory = (
        PROJECT_ROOT
        / "experiments"
        / "paper-evaluation"
        / "baseline-comparison"
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


def print_statistics(
    title: str,
    statistics_result: Dict[
        str,
        Optional[float]
    ],
) -> None:

    print(f"\n{title}")

    print(
        "  Mean: "
        f"{statistics_result['mean']} s"
    )

    print(
        "  Median: "
        f"{statistics_result['median']} s"
    )

    print(
        "  Minimum: "
        f"{statistics_result['minimum']} s"
    )

    print(
        "  Maximum: "
        f"{statistics_result['maximum']} s"
    )

    print(
        "  SD: "
        f"{statistics_result['standard_deviation']} s"
    )

    print(
        "  CV: "
        f"{statistics_result['coefficient_of_variation']}"
    )


def main() -> None:

    args = parse_arguments()

    validate_arguments(args)

    print(
        "V2 FIELD-ONLY RUNTIME BENCHMARK"
    )

    print(
        "==============================="
    )

    print(
        f"Environment: {APP_ENV}"
    )

    print(
        f"MinIO bucket: {MINIO_BUCKET}"
    )

    print(
        "Fixed Bronze run: "
        f"{args.bronze_run_id}"
    )

    experiment_started_at = (
        utc_now()
    )

    repetitions = []

    for repetition_number in range(
        1,
        args.repetitions + 1,
    ):

        result = run_repetition(
            repetition_number=(
                repetition_number
            ),
            args=args,
        )

        repetitions.append(result)

    summary = build_summary(
        repetitions
    )

    report = {
        "experiment": (
            "proposed_v2_field_runtime_benchmark"
        ),

        "experiment_name": (
            args.experiment_name
        ),

        "started_at": (
            experiment_started_at
        ),

        "completed_at": utc_now(),

        "environment": APP_ENV,

        "bucket": MINIO_BUCKET,

        "configuration": {
            "repetitions": (
                args.repetitions
            ),
            "bronze_run_id": (
                args.bronze_run_id
            ),
            "field_records": 500,
            "expected_silver_records": 500,
            "expected_gold_records": 500,
            "expected_anomalies": 25,
            "isolation_forest_estimators": 200,
            "isolation_forest_contamination": (
                args.contamination
            ),
            "isolation_forest_random_state": 42,
            "forced_silver_reprocessing": True,
        },

        "summary": summary,

        "repetitions": repetitions,
    }

    output_path = save_report(
        report=report,
        experiment_name=(
            args.experiment_name
        ),
    )

    print(
        "\nV2 FIELD RUNTIME SUMMARY"
    )

    print(
        "========================"
    )

    print(
        "\nCompleted repetitions: "
        f"{summary['completed_repetitions']}"
    )

    print_statistics(
        "Silver",
        summary[
            "silver_duration_seconds"
        ],
    )

    print_statistics(
        "Gold",
        summary[
            "gold_duration_seconds"
        ],
    )

    print_statistics(
        "Combined",
        summary[
            "combined_duration_seconds"
        ],
    )

    print(
        "\nReport:"
    )

    print(output_path)


if __name__ == "__main__":
    main()