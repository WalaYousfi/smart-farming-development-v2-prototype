import json
from pathlib import Path

from pipeline.common.config import PROJECT_ROOT


OUTPUT_DIR = (
    PROJECT_ROOT
    / "experiments"
    / "paper-evaluation"
    / "baseline-comparison"
)


BASELINE_CAPABILITIES = {
    "source_count": 1,
    "source_formats": ["CSV"],
    "separate_source_ingestion_paths": False,
    "source_aware_bronze_envelope": False,
    "bronze_raw_preservation": True,
    "canonical_silver_models": False,
    "quarantine_zone": False,
    "quality_reports": False,
    "heterogeneous_integration": False,
    "multi_parent_lineage": False,
    "run_manifests": False,
    "explicit_lineage_records": False,
    "historical_run_selection": False,
    "isolated_experiment_environment": False,
    "gold_anomaly_detection": True,
    "purpose_specific_gold_products": False,
    "automated_evaluation_framework": False,
}


V2_CAPABILITIES = {
    "source_count": 2,
    "source_formats": ["CSV", "JSON"],
    "separate_source_ingestion_paths": True,
    "source_aware_bronze_envelope": True,
    "bronze_raw_preservation": True,
    "canonical_silver_models": True,
    "quarantine_zone": True,
    "quality_reports": True,
    "heterogeneous_integration": True,
    "multi_parent_lineage": True,
    "run_manifests": True,
    "explicit_lineage_records": True,
    "historical_run_selection": True,
    "isolated_experiment_environment": True,
    "gold_anomaly_detection": True,
    "purpose_specific_gold_products": True,
    "automated_evaluation_framework": True,
}


def main() -> None:
    comparison = {}

    for capability in BASELINE_CAPABILITIES:
        baseline_value = BASELINE_CAPABILITIES[
            capability
        ]

        v2_value = V2_CAPABILITIES[
            capability
        ]

        comparison[capability] = {
            "baseline_v1": baseline_value,
            "proposed_v2": v2_value,
            "changed": (
                baseline_value != v2_value
            ),
        }

    added_capabilities = [
        capability
        for capability, values in comparison.items()
        if (
            values["baseline_v1"] is False
            and values["proposed_v2"] is True
        )
    ]

    unchanged_capabilities = [
        capability
        for capability, values in comparison.items()
        if (
            values["baseline_v1"]
            == values["proposed_v2"]
        )
    ]

    report = {
        "experiment": (
            "baseline_v1_vs_proposed_v2"
        ),
        "baseline_repository": (
            "docker-learning"
        ),
        "proposed_repository": (
            "smart-farming-development"
        ),
        "summary": {
            "capabilities_compared": len(
                comparison
            ),
            "capabilities_added": len(
                added_capabilities
            ),
            "added_capabilities": (
                added_capabilities
            ),
            "unchanged_capabilities": (
                unchanged_capabilities
            ),
        },
        "comparison": comparison,
    }

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        OUTPUT_DIR
        / "baseline_v1_vs_v2.json"
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

    print("\nBASELINE V1 VS PROPOSED V2")
    print("==========================")

    print(
        "Capabilities compared: "
        f"{len(comparison)}"
    )

    print(
        "Capabilities added in V2: "
        f"{len(added_capabilities)}"
    )

    print("\nAdded capabilities:")

    for capability in added_capabilities:
        print(
            f"  - {capability}"
        )

    print("\nUnchanged capabilities:")

    for capability in unchanged_capabilities:
        print(
            f"  - {capability}"
        )

    print("\nReport:")
    print(output_path)


if __name__ == "__main__":
    main()