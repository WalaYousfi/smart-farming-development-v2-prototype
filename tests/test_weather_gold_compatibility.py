import pandas as pd

from pipeline.evaluation.stream_augmented_workload import generate

from pipeline.silver.silver_processor import (
    process_records as process_field_records,
)

from pipeline.silver.weather_silver_processor import (
    process_records as process_weather_records,
)

from pipeline.integration.field_weather_integration import (
    prepare_field_data,
    prepare_weather_data,
    integrate_datasets,
)

from pipeline.gold.integrated_anomaly_detection import (
    FEATURE_COLUMNS,
    prepare_features,
    score_observations,
)

from pipeline.evaluation.evaluate_gold_holdout import (
    evaluate_holdout,
)


COUNT = 1000
SEED = 42


def bronze_envelope(payload, index, source):
    return {
        "metadata": {
            "event_id": "{}-evaluation-{}".format(source, index),
            "source_system": source,
            "source_schema_version": "1.0.0",
            "ingestion_timestamp": "2026-10-27T12:00:00+00:00",
        },
        "payload": payload,
    }


def test_weather_silver_and_gold_compatibility():
    # Generate matching Field and Weather observations.
    field_source = list(generate("field", COUNT, SEED))
    weather_source = list(generate("weather", COUNT, SEED))

    field_bronze = [
        bronze_envelope(record, index, "field")
        for index, record in enumerate(field_source)
    ]

    weather_bronze = [
        bronze_envelope(record, index, "weather")
        for index, record in enumerate(weather_source)
    ]

    # Run actual Field Silver processing.
    field_accepted, field_quarantined, field_counters = (
        process_field_records(
            bronze_records=field_bronze,
            silver_run_id="test-field-silver",
        )
    )

    # Run actual Weather Silver processing.
    weather_accepted, weather_quarantined, weather_counters = (
        process_weather_records(
            bronze_records=weather_bronze,
            silver_run_id="test-weather-silver",
        )
    )

    print("\nFIELD SILVER")
    print("Accepted:", field_counters["accepted_records"])
    print("Quarantined:", field_counters["quarantined_records"])

    print("\nWEATHER SILVER")
    print("Accepted:", weather_counters["accepted_records"])
    print("Quarantined:", weather_counters["quarantined_records"])

    if field_quarantined:
        print(
            "First Field quarantine:",
            field_quarantined[0].get("failure_reasons"),
        )

    if weather_quarantined:
        print(
            "First Weather quarantine:",
            weather_quarantined[0].get("failure_reasons"),
        )

    assert len(field_accepted) == COUNT
    assert len(weather_accepted) == COUNT
    assert len(field_quarantined) == 0
    assert len(weather_quarantined) == 0

    # Integrate accepted canonical observations.
    field_df = prepare_field_data(pd.DataFrame(field_accepted))
    weather_df = prepare_weather_data(pd.DataFrame(weather_accepted))

    integrated = integrate_datasets(
        field_dataframe=field_df,
        weather_dataframe=weather_df,
        integration_run_id="test-integration",
    )

    assert len(integrated) == COUNT
    assert (integrated["weather_match_status"] == "matched").all()

    # Confirm evaluation metadata survived integration.
    assert integrated["augmentation_family_id"].notna().all()
    assert integrated["ground_truth_anomaly"].notna().all()

    # Verify that labels and provenance are not model features.
    assert "ground_truth_anomaly" not in FEATURE_COLUMNS
    assert "augmentation_family_id" not in FEATURE_COLUMNS

    # Run actual Gold feature preparation and scoring.
    features = prepare_features(integrated)

    scored, model, split_metadata = score_observations(
        dataframe=integrated,
        feature_dataframe=features,
        contamination=0.05,
        gold_run_id="test-gold",
        require_family_groups=True,
    )

    print("\nGOLD")
    print("Scored records:", len(scored))
    print("Training records:", split_metadata["train_records"])
    print("Testing records:", split_metadata["test_records"])
    print("Split method:", split_metadata["split_method"])

    assert len(scored) == COUNT
    assert split_metadata["split_method"] == "GroupShuffleSplit"

    assert set(scored["dataset_split"]) == {"train", "test"}
    assert set(scored["anomaly_prediction"]).issubset({-1, 1})

    # Check that augmentation families cannot cross partitions.
    train_families = set(
        scored.loc[
            scored["dataset_split"] == "train",
            "augmentation_family_id",
        ]
    )

    test_families = set(
        scored.loc[
            scored["dataset_split"] == "test",
            "augmentation_family_id",
        ]
    )

    assert train_families.isdisjoint(test_families)

    # Verify ground-truth labels survived Gold.
    assert scored["ground_truth_anomaly"].notna().all()

    # Execute independent holdout evaluation.
    metrics = evaluate_holdout(scored)

    print("\nHOLDOUT EVALUATION")
    print("Accuracy:", metrics["accuracy"])
    print("Precision:", metrics["precision"])
    print("Recall:", metrics["recall"])
    print("F1:", metrics["f1"])
    print("TP:", metrics["tp"])
    print("TN:", metrics["tn"])
    print("FP:", metrics["fp"])
    print("FN:", metrics["fn"])

    assert metrics["evaluation_scope"] == "test_only"

    assert (
        metrics["tp"]
        + metrics["tn"]
        + metrics["fp"]
        + metrics["fn"]
        == split_metadata["test_records"]
    )

    print("\nWeather Silver and Gold compatibility PASSED")