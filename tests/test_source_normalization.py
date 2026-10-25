# Role: Verify heterogeneous column names, date formats, identifier mappings, and conflicting values.


import pytest

from pipeline.silver.source_normalization import (
    normalize_payload,
)


def test_field_column_names():
    record = {
        "farmId": "F001",
        "sensorId": "S001",
        "date_time": "25/10/2026",
    }

    result = normalize_payload(record, "field")

    assert result["farm_id"] == "F001"
    assert result["sensor_id"] == "S001"
    assert result["timestamp"].startswith("2026-10-25")


def test_weather_date_format():
    record = {
        "farm_code": "F001",
        "timestamp": "2026-10-25",
    }

    result = normalize_payload(record, "weather")

    assert result["farm_id"] == "F001"
    assert result["observed_at"].startswith("2026-10-25")


def test_explicit_farm_mapping():
    record = {
        "farm_id": "EXT-001",
        "timestamp": "2026-10-25",
    }

    result = normalize_payload(
        record,
        "field",
        farm_mapping={"EXT-001": "F001"},
    )

    assert result["farm_id"] == "F001"


def test_unmapped_identifier():
    record = {
        "farm_id": "UNKNOWN",
        "timestamp": "2026-10-25",
    }

    with pytest.raises(ValueError):
        normalize_payload(
            record,
            "field",
            farm_mapping={"EXT-001": "F001"},
        )


def test_conflicting_column_values():
    record = {
        "farm_id": "F001",
        "farmId": "F002",
        "timestamp": "2026-10-25",
    }

    with pytest.raises(ValueError):
        normalize_payload(record, "field")