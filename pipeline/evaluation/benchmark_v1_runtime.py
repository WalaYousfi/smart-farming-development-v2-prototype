import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
from typing import Dict, List, Optional

from pipeline.common.config import PROJECT_ROOT


DEFAULT_V1_ROOT = Path(
    r"C:\docker-learning"
)


def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark the frozen V1 Silver and Gold "
            "processing scripts without modifying V1."
        )
    )

    parser.add_argument(
        "--v1-root",
        default=str(DEFAULT_V1_ROOT),
        help="Path to the frozen V1 repository.",
    )

    parser.add_argument(
        "--repetitions",
        type=int,
        default=5,
    )

    parser.add_argument(
        "--experiment-name",
        default="baseline-v1-runtime-5-runs",
    )

    return parser.parse_args()


def validate_arguments(
    args: argparse.Namespace,
) -> Path:

    if args.repetitions < 2:
        raise ValueError(
            "At least two repetitions are required."
        )

    v1_root = Path(
        args.v1_root
    ).resolve()

    silver_script = (
        v1_root
        / "pipeline"
        / "silver"
        / "silver_processor.py"
    )

    gold_script = (
        v1_root
        / "pipeline"
        / "gold"
        / "anomaly_detection.py"
    )

    if not silver_script.exists():
        raise FileNotFoundError(
            f"V1 Silver script not found: "
            f"{silver_script}"
        )

    if not gold_script.exists():
        raise FileNotFoundError(
            f"V1 Gold script not found: "
            f"{gold_script}"
        )

    return v1_root


def run_process(
    command: List[str],
    cwd: Path,
    step_name: str,
) -> Dict:

    start_timestamp = utc_now()

    start = time.perf_counter()

    process = subprocess.run(
        command,
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )

    duration = round(
        time.perf_counter() - start,
        4,
    )

    result = {
        "step": step_name,
        "started_at": start_timestamp,
        "completed_at": utc_now(),
        "duration_seconds": duration,
        "return_code": (
            process.returncode
        ),
        "stdout": process.stdout,
        "stderr": process.stderr,
    }

    if process.returncode != 0:
        raise RuntimeError(
            f"{step_name} failed.\n\n"
            f"STDOUT:\n{process.stdout}\n\n"
            f"STDERR:\n{process.stderr}"
        )

    return result


def validate_silver_output(
    output: str,
) -> None:

    required_fragments = [
        "Bronze rows loaded: 500",
        "Silver rows after deduplication: 500",
        "valid    500",
        "Silver file uploaded successfully",
    ]

    missing = [
        fragment
        for fragment in required_fragments
        if fragment not in output
    ]

    if missing:
        raise RuntimeError(
            "V1 Silver workload validation failed. "
            f"Missing output fragments: {missing}"
        )


def validate_gold_output(
    output: str,
) -> None:

    required_fragments = [
        "normal     475",
        "anomaly     25",
        "Gold uploaded",
    ]

    missing = [
        fragment
        for fragment in required_fragments
        if fragment not in output
    ]

    if missing:
        raise RuntimeError(
            "V1 Gold workload validation failed. "
            f"Missing output fragments: {missing}"
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
    repetition: int,
    v1_root: Path,
) -> Dict:

    print(
        "\n"
        + "=" * 60
    )

    print(
        f"V1 repetition {repetition}"
    )

    print(
        "=" * 60
    )

    silver_result = run_process(
        command=[
            sys.executable,
            "pipeline/silver/silver_processor.py",
        ],
        cwd=v1_root,
        step_name="v1_silver",
    )

    validate_silver_output(
        silver_result["stdout"]
    )

    print(
        "Silver validation: PASSED"
    )

    print(
        "Silver duration: "
        f"{silver_result['duration_seconds']} s"
    )

    gold_result = run_process(
        command=[
            sys.executable,
            "pipeline/gold/anomaly_detection.py",
        ],
        cwd=v1_root,
        step_name="v1_gold",
    )

    validate_gold_output(
        gold_result["stdout"]
    )

    print(
        "Gold validation: PASSED"
    )

    print(
        "Gold duration: "
        f"{gold_result['duration_seconds']} s"
    )

    combined = round(
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
        f"{combined} s"
    )

    return {
        "repetition": repetition,
        "validation": {
            "bronze_records": 500,
            "silver_records": 500,
            "valid_records": 500,
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
            "combined_seconds": combined,
        },
        "steps": {
            "silver": silver_result,
            "gold": gold_result,
        },
    }


def build_summary(
    repetitions: List[Dict],
) -> Dict:

    silver = [
        result["durations"][
            "silver_seconds"
        ]
        for result in repetitions
    ]

    gold = [
        result["durations"][
            "gold_seconds"
        ]
        for result in repetitions
    ]

    combined = [
        result["durations"][
            "combined_seconds"
        ]
        for result in repetitions
    ]

    return {
        "completed_repetitions": len(
            repetitions
        ),
        "silver_duration_seconds": (
            calculate_statistics(
                silver
            )
        ),
        "gold_duration_seconds": (
            calculate_statistics(
                gold
            )
        ),
        "combined_duration_seconds": (
            calculate_statistics(
                combined
            )
        ),
    }


def save_report(
    report: Dict,
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
    stats: Dict,
) -> None:

    print(f"\n{title}")

    print(
        f"  Mean: "
        f"{stats['mean']} s"
    )

    print(
        f"  Median: "
        f"{stats['median']} s"
    )

    print(
        f"  Minimum: "
        f"{stats['minimum']} s"
    )

    print(
        f"  Maximum: "
        f"{stats['maximum']} s"
    )

    print(
        f"  SD: "
        f"{stats['standard_deviation']} s"
    )

    print(
        f"  CV: "
        f"{stats['coefficient_of_variation']}"
    )


def main() -> None:

    args = parse_arguments()

    v1_root = validate_arguments(
        args
    )

    print(
        "Frozen V1 repository:"
    )

    print(v1_root)

    repetitions = []

    started_at = utc_now()

    for repetition in range(
        1,
        args.repetitions + 1,
    ):

        repetitions.append(
            run_repetition(
                repetition=repetition,
                v1_root=v1_root,
            )
        )

    summary = build_summary(
        repetitions
    )

    report = {
        "experiment": (
            "baseline_v1_runtime_benchmark"
        ),
        "experiment_name": (
            args.experiment_name
        ),
        "started_at": started_at,
        "completed_at": utc_now(),
        "baseline_repository": (
            str(v1_root)
        ),
        "configuration": {
            "repetitions": (
                args.repetitions
            ),
            "field_records": 500,
            "expected_silver_records": 500,
            "expected_gold_records": 500,
            "expected_anomalies": 25,
            "isolation_forest_estimators": 200,
            "isolation_forest_contamination": 0.05,
            "isolation_forest_random_state": 42,
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
        "\nV1 RUNTIME BENCHMARK"
    )

    print(
        "===================="
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