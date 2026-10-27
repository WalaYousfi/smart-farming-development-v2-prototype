import pandas as pd

from pipeline.evaluation.stream_augmented_workload import generate
from pipeline.silver.silver_processor import process_records
from pipeline.silver.weather_canonical_mapper import (
    map_weather_bronze_to_canonical,
)
from pipeline.integration.field_weather_integration import (
    prepare_field_data,
    prepare_weather_data,
    integrate_datasets,
)


COUNT = 1000
SEED = 42


def bronze_envelope(payload, index, source):
    return {
        "metadata": {
            "event_id": "{}-test-{}".format(source, index),
            "source_system": source,
            "source_schema_version": "1.0.0",
            "ingestion_timestamp": "2026-10-27T12:00:00+00:00",
        },
        "payload": payload,
    }


def test_silver_and_integration_compatibility():
    # Generate the same deterministic Field/Weather workload.
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

    # Execute the real Field Silver validation and mapping.
    accepted, quarantined, counters = process_records(
        bronze_records=field_bronze,
        silver_run_id="test-field-silver",
    )

    print("\nFIELD SILVER RESULTS")
    print("Input:", counters["input_records"])
    print("Accepted:", counters["accepted_records"])
    print("Quarantined:", counters["quarantined_records"])

    if quarantined:
        print(
            "First quarantine reason:",
            quarantined[0].get("failure_reasons"),
        )

    assert len(accepted) == COUNT, (
        "Field Silver rejected {} records".format(
            len(quarantined)
        )
    )

    # Verify augmentation metadata survived Silver.
    assert all(
        record.get("augmentation_family_id")
        for record in accepted
    )

    assert all(
        record.get("ground_truth_anomaly") in (0, 1)
        for record in accepted
    )

    # Convert Weather using the existing canonical mapper.
    # This checks mapping, not the full Weather Silver validator.
    weather_accepted = [
        map_weather_bronze_to_canonical(
            bronze_record=record,
            silver_run_id="test-weather-silver",
        )
        for record in weather_bronze
    ]

    field_df = prepare_field_data(
        pd.DataFrame(accepted)
    )

    weather_df = prepare_weather_data(
        pd.DataFrame(weather_accepted)
    )

    # Execute the real integration logic.
    integrated = integrate_datasets(
        field_dataframe=field_df,
        weather_dataframe=weather_df,
        integration_run_id="test-integration",
    )

    matched = int(
        (
            integrated["weather_match_status"]
            == "matched"
        ).sum()
    )

    unmatched = int(
        (
            integrated["weather_match_status"]
            == "not_matched"
        ).sum()
    )

    print("\nINTEGRATION RESULTS")
    print("Field records:", len(field_df))
    print("Weather records:", len(weather_df))
    print("Integrated records:", len(integrated))
    print("Matched:", matched)
    print("Unmatched:", unmatched)

    assert len(field_df) == COUNT
    assert len(weather_df) == COUNT
    assert len(integrated) == COUNT
    assert matched == COUNT
    assert unmatched == 0

    assert integrated["observation_id"].is_unique

    assert integrated["augmentation_family_id"].notna().all()
    assert integrated["ground_truth_anomaly"].notna().all()

    print("\nSilver and integration compatibility PASSED")