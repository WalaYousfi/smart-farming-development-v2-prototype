## Table — Storage Usage by Architectural Category

| Architectural category | Objects | Storage (KB) | Share of total storage |
|---|---:|---:|---:|
| Bronze | TBD | TBD | TBD |
| Accepted Silver | TBD | TBD | TBD |
| Quarantine | TBD | TBD | TBD |
| Integrated Silver | TBD | TBD | TBD |
| Gold analytical | TBD | TBD | TBD |
| Gold products | TBD | TBD | TBD |
| Manifests | TBD | TBD | TBD |
| Lineage | TBD | TBD | TBD |
| Quality reports | TBD | TBD | TBD |

**Source:** Automatically generated from MinIO object metadata.

**Interpretation:** The table evaluates whether the storage cost of
governance metadata remains small relative to stored agricultural
data.


## Table — End-to-End Controlled Evaluation

| Metric | Result |
|---|---:|
| Field source records | 500 |
| Weather source records | 500 |
| Field accepted records | 500 |
| Weather accepted records | 500 |
| Field quarantined records | 0 |
| Weather quarantined records | 0 |
| Integrated records | 500 |
| Integration match rate | 1.0 |
| Gold records scored | 500 |
| Anomalies flagged | 25 |
| Transformation lineage coverage | 1.0 |
| Bounded processing time | 5.4104 seconds |
| Run output storage | 1.521684 MB |

**Configuration:** 500 Field observations, 500 synthetic Weather
observations, Isolation Forest contamination = 0.05, random state = 42.

## Table — Repeated Processing-Time Evaluation

| Stage | Mean (s) | Median (s) | Min (s) | Max (s) | SD (s) | CV |
|---|---:|---:|---:|---:|---:|---:|
| Field–Weather integration | 3.957 | 2.8296 | 2.5489 | 8.7265 | 2.6702 | 0.6748|
| Integrated Gold processing |  7.6086 | 5.4612 | 5.0713 | 16.6278 | 5.0457 | 0.6632 |
| Combined | 11.5656 | 8.1461 | 8.0173 |25.3543 | 7.7084 | 0.6665 |


**Experimental conditions:** Five repetitions using the same
500-record Field Silver dataset and 500-record Weather Silver
dataset in the isolated evaluation environment.