# Paper Evaluation Run 01

## Environment

Environment: evaluation

MinIO bucket:
smart-farming-paper-eval

Field Kafka topic:
eval-field-readings

Weather Kafka topic:
eval-weather-readings

## Input

Field records: 500

Weather records: 500

## Run identifiers

FIELD_BRONZE_RUN_ID = 20260809T122640Z_b09798ef
WEATHER_BRONZE_RUN_ID = 20260809T123248Z_81dff0c0
Field Silver run ID: 20260809T123145Z_a587e198
Weather Silver run ID: 20260809T123659Z_fe5574f3
Integration run ID: 20260809T124056Z_2102200d
Integrated Gold run ID: 20260809T124237Z_b7b4cee8


## Expected validation results

Field Silver:
- Input: 500
- Accepted: 500
- Quarantined: 0

Weather Silver:
- Input: 500
- Accepted: 500
- Quarantined: 0

Integration:
- Field records: 500
- Weather records: 500
- Matched: 500
- Unmatched: 0
- Match rate: 1.0

Integrated Gold:
- Total records: 500
- Normal: 475
- Anomalies: 25
- Weather-matched records: 500

## Model configuration

Algorithm: Isolation Forest

Number of estimators: 200

Contamination: 0.05

Random state: 42