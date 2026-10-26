# This test verifies that the two generators produce matching keys, 
# that the labels are reproducible, 
# and that related records share their augmentation family.


from pipeline.evaluation.stream_augmented_workload import (
    generate,
    should_inject_anomaly,
)


def test_reproducible_field_generation():
    first = list(generate("field", 30, 42))
    second = list(generate("field", 30, 42))

    assert first == second


def test_field_weather_alignment():
    fields = list(generate("field", 30, 42))
    weathers = list(generate("weather", 30, 42))

    field_keys = {
        (record["farm_id"], record["timestamp"][:10])
        for record in fields
    }

    weather_keys = {
        (record["farm_id"], record["observed_at"][:10])
        for record in weathers
    }

    assert field_keys == weather_keys
    assert len(field_keys) == 30
    assert len(weather_keys) == 30


def test_ground_truth_labels():
    records = list(generate("field", 100, 42))

    for index, record in enumerate(records):
        expected = int(
            should_inject_anomaly(index, 42, 0.05)
        )

        assert record["ground_truth_anomaly"] == expected


def test_augmentation_family_present():
    records = list(generate("field", 30, 42))

    assert all(
        record["augmentation_family_id"]
        for record in records
    )


def test_weather_schema_not_extended():
    records = list(generate("weather", 30, 42))

    assert all(
        "ground_truth_anomaly" not in record
        for record in records
    )

    assert all(
        "augmentation_family_id" not in record
        for record in records
    )