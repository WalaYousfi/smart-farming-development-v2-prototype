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