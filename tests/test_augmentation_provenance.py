# role: Verify provenance preservation

from pipeline.silver.canonical_mapper import (
    map_bronze_to_canonical,
)


def test_augmentation_provenance_survives_mapping():
    bronze_record = {
        "metadata": {
            "event_id": "test-event-001",
            "source_system": "test",
            "source_schema_version": "source-1.0.0",
            "ingestion_timestamp": "2026-10-26T12:00:00+00:00",
        },
        "payload": {
            "farm_id": "F001",
            "sensor_id": "S001",
            "timestamp": "2026-10-26T12:00:00+00:00",
            "soil_moisture_%": 40.0,
            "soil_pH": 6.5,
            "temperature_C": 25.0,
            "rainfall_mm": 10.0,
            "humidity_%": 60.0,
            "sunlight_hours": 8.0,
            "pesticide_usage_ml": 15.0,
            "NDVI_index": 0.7,
            "augmentation_family_id": "family-001",
            "ground_truth_anomaly": 1,
        },
    }

    result = map_bronze_to_canonical(
        bronze_record=bronze_record,
        silver_run_id="test-silver-run",
    )

    assert result["augmentation_family_id"] == "family-001"
    assert result["ground_truth_anomaly"] == 1