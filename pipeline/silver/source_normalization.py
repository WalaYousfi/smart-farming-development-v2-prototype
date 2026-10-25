# Role: Normalize source column names, timestamps, and farm identifiers before canonical mapping.



from datetime import datetime, timezone

FIELD_ALIASES = {
    "farm_id": ["farmId", "farm_code"],
    "sensor_id": ["sensorId", "sensor_code"],
    "timestamp": ["date_time", "measurement_time"],
    "soil_moisture_%": ["soil_moisture", "soilMoisture"],
    "soil_pH": ["soil_ph", "soilPH"],
    "temperature_C": ["temperature_c", "temperatureC"],
}

WEATHER_ALIASES = {
    "farm_id": ["farmId", "farm_code"],
    "observed_at": ["timestamp", "date_time", "measurement_time"],
    "weather_station_id": ["station_id", "stationId"],
}

DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%d/%m/%Y",
    "%d/%m/%Y %H:%M:%S",
    "%m/%d/%Y",
    "%m/%d/%Y %H:%M:%S",
]


def normalize_date(value):
    if value is None or not str(value).strip():
        raise ValueError("Missing timestamp")

    value = str(value).strip()

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except ValueError:
        parsed = None

        for date_format in DATE_FORMATS:
            try:
                parsed = datetime.strptime(value, date_format)
                break
            except ValueError:
                continue

        if parsed is None:
            raise ValueError(
                "Unsupported timestamp format: {}".format(value)
            )

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc).isoformat()


def normalize_payload(payload, source, farm_mapping=None):
    if source not in ("field", "weather"):
        raise ValueError("Unknown source: {}".format(source))

    aliases = (
        FIELD_ALIASES if source == "field"
        else WEATHER_ALIASES
    )

    result = dict(payload)

    for canonical_name, alternatives in aliases.items():
        present = [
            name for name in [canonical_name] + alternatives
            if name in result
            and result[name] is not None
            and str(result[name]).strip()
        ]

        if not present:
            continue

        values = {
            str(result[name]).strip()
            for name in present
        }

        if len(values) > 1:
            raise ValueError(
                "Conflicting values for {}".format(canonical_name)
            )

        result[canonical_name] = result[present[0]]

    farm_id = result.get("farm_id")

    if farm_mapping is not None and farm_id is not None:
        original_id = str(farm_id).strip()

        if original_id not in farm_mapping:
            raise ValueError(
                "Unmapped farm identifier: {}".format(original_id)
            )

        result["farm_id"] = farm_mapping[original_id]

    timestamp_field = (
        "timestamp" if source == "field"
        else "observed_at"
    )

    if timestamp_field in result:
        result[timestamp_field] = normalize_date(
            result[timestamp_field]
        )

    return result