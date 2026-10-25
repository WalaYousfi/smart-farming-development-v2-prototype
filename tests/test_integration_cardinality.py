# Verify normal matching, missing Weather coverage, and duplicate-key protection.

import pandas as pd
import pytest

from pipeline.integration.field_weather_integration import (
    prepare_field_data,
    prepare_weather_data,
    integrate_datasets,
)


def make_field():
    return pd.DataFrame({
        "observation_id": ["F1", "F2", "F3"],
        "farm_id": ["A", "B", "C"],
        "observed_at": [
            "2026-01-01",
            "2026-01-01",
            "2026-01-01",
        ],
    })


def make_weather():
    return pd.DataFrame({
        "weather_observation_id": ["W1", "W2"],
        "farm_id": ["A", "B"],
        "observed_at": [
            "2026-01-01",
            "2026-01-01",
        ],
    })


def test_unmatched_field_is_preserved():
    field = prepare_field_data(make_field())
    weather = prepare_weather_data(make_weather())

    result = integrate_datasets(
        field,
        weather,
        "test-run",
    )

    assert len(result) == 3
    assert (
        result["weather_match_status"] == "matched"
    ).sum() == 2

    assert (
        result["weather_match_status"] == "not_matched"
    ).sum() == 1


def test_duplicate_weather_keys_are_rejected():
    field = prepare_field_data(make_field())

    weather_data = make_weather()
    weather_data = pd.concat(
        [weather_data, weather_data.iloc[[0]]],
        ignore_index=True,
    )

    weather = prepare_weather_data(weather_data)

    with pytest.raises(ValueError):
        integrate_datasets(
            field,
            weather,
            "test-run",
        )


def test_duplicate_field_ids_are_rejected():
    field_data = make_field()
    field_data.loc[1, "observation_id"] = "F1"

    field = prepare_field_data(field_data)
    weather = prepare_weather_data(make_weather())

    with pytest.raises(ValueError):
        integrate_datasets(
            field,
            weather,
            "test-run",
        )