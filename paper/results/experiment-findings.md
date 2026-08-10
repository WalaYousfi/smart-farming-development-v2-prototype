## Processing-Time Experiment

### Objective

Evaluate the execution-time stability of the heterogeneous
integration stage and integrated Gold anomaly-detection stage.

### Experimental configuration

- Environment: isolated paper-evaluation environment.
- Field Silver observations: 500.
- Weather Silver observations: 500.
- Integration coverage: 100%.
- Repetitions: 5.
- Isolation Forest estimators: 200.
- Contamination: 0.05.
- Random state: 42.
- Processing engine: pandas and scikit-learn.
- Storage: MinIO.
- Execution environment: local Docker-based prototype.

### Workload validation

Each repetition was required to produce:

- 500 Field observations.
- 500 Weather observations.
- 500 integrated observations.
- 500 matched observations.
- 500 Gold-scored observations.
- 475 normal observations.
- 25 anomaly-labelled observations.

Runs failing these checks were excluded as invalid experimental
executions rather than treated as timing measurements.

### Results

| Stage | Mean (s) | Median (s) | Min (s) | Max (s) | SD (s) | CV |
|---|---:|---:|---:|---:|---:|---:|
| Field–Weather integration | 3.957 | 2.8296 | 2.5489 | 8.7265 | 2.6702 | 0.6748|
| Integrated Gold processing |  7.6086 | 5.4612 | 5.0713 | 16.6278 | 5.0457 | 0.6632 |
| Combined | 11.5656 | 8.1461 | 8.0173 |25.3543 | 7.7084 | 0.6665 |

### Interpretation

TBD after experimental execution.

### Limitations

The measurements were performed using a 500-record-per-source
controlled workload on a single local machine. They therefore
characterize prototype execution stability rather than
large-scale distributed performance.



## Storage-Overhead Experiment

### Objective

Measure the storage introduced by data zones and cross-cutting
metadata in the proposed architecture.

### Measurement scope

The preliminary storage evaluator groups MinIO objects into:

- Bronze data.
- Accepted Silver data.
- Quarantine data.
- Integrated Silver data.
- Gold analytical outputs.
- Gold data products.
- Manifests.
- Lineage records.
- Quality reports.
- Traceability reports.

### Preliminary results

| Category | Object count | Size (bytes) | Size (MB) |
|---|---:|---:|---:|
| Bronze data | TBD | TBD | TBD |
| Accepted Silver | TBD | TBD | TBD |
| Silver quarantine | TBD | TBD | TBD |
| Integrated Silver | TBD | TBD | TBD |
| Gold analytical | TBD | TBD | TBD |
| Gold data products | TBD | TBD | TBD |
| Manifests | TBD | TBD | TBD |
| Lineage | TBD | TBD | TBD |
| Quality reports | TBD | TBD | TBD |
| Traceability | TBD | TBD | TBD |

### Aggregate measurements

- Total data storage: TBD.
- Total metadata storage: TBD.
- Metadata-to-data storage ratio: TBD.
- Metadata storage percentage: TBD.
- Data object count: TBD.
- Metadata object count: TBD.

### Interpretation

To be completed after measuring a clean experimental run.

### Limitation

The first evaluator execution may include accumulated development
runs. Final publication measurements must use an isolated bucket or
run-specific object selection.



## Experimental Environment Isolation

The final evaluation uses a clean, isolated data-lake environment.

Development and experimental resources are separated to prevent
historical objects or Kafka offsets from affecting measurements.

### Development environment

- MinIO bucket: `smart-farming-v2`
- Field topic: `raw-field-readings`
- Weather topic: `raw-weather-readings`

### Evaluation environment

- MinIO bucket: `smart-farming-paper-eval`
- Field topic: `eval-field-readings`
- Weather topic: `eval-weather-readings`

Initial evaluation-bucket object count: 0.

This isolation is intended to improve the reproducibility and
internal validity of storage, record-count, and performance
measurements.

## Isolated End-to-End Evaluation

### Objective

Evaluate one complete execution of the proposed architecture using
an isolated MinIO bucket and dedicated Kafka topics.

### Input

- Field observations: 500
- Weather observations: 500
- Source systems: 2
- Source formats: CSV and JSON

### Data-quality results

| Source | Input | Accepted | Quarantined | Acceptance rate |
|---|---:|---:|---:|---:|
| Field | 500| 500 | 0 | 1.0 |
| Weather | 500 | 500 | 0 | 1.0 |

### Integration results

- Field records: 500
- Weather records: 500
- Integrated records: 500
- Matched records: 500
- Unmatched records: 0
- Match rate: 1.0

### Gold analytical results

- Records scored: 500*
- Normal observations: 475
- Anomalous observations: 25
- Weather-matched observations: 500
- Weather-matched anomalies: 25

### Traceability

- Transformation jobs evaluated: 4
- Jobs with lineage: 4
- Lineage coverage: 1.0
- Integration parent runs: 2
- Gold parent runs: 1

### Performance

- Total bounded processing time: 5.4104 seconds
- Run-specific output storage: 1.521684 MB

### Interpretation

The controlled experiment evaluates whether two heterogeneous source
representations can pass independently through Bronze and canonical
Silver processing before being combined into one integrated Silver
dataset and consumed by an AI-enabled Gold stage.

### Important limitation

The synthetic weather dataset was deliberately generated with matching
farm and temporal keys. Therefore, complete match coverage demonstrates
the architecture's ability to integrate compatible heterogeneous
datasets; it does not represent naturally occurring real-world weather
matching rates.

## Controlled Data-Quality Stress Test

### Objective

Evaluate whether the Silver quality-control stage detects and
quarantines known invalid agricultural observations while Bronze
preserves the source records unchanged according to the ELT design.

### Experimental design

Twenty Field observations were deliberately corrupted after selection
from the clean source dataset.

Four defect categories were introduced:

| Defect category | Injected records |
|---|---:|
| Invalid soil moisture | 5 |
| Invalid humidity | 5 |
| Invalid soil pH | 5 |
| Invalid NDVI | 5 |
| **Total** | **20** |

The ground-truth defect labels were recorded before ingestion.

### Expected result

- Input records: 20
- Expected accepted records: 0
- Expected quarantined records: 20
- Expected invalid-record detection rate: 100%

### Actual result

- Input records: TBD
- Accepted records: TBD
- Quarantined records: TBD
- Detection rate: TBD

### Architectural significance

The experiment evaluates the separation of responsibilities between
Bronze and Silver. Bronze is expected to preserve source observations,
including invalid values, while Silver applies explicit quality rules
and separates accepted from quarantined records.

### Limitation

The injected defects are controlled synthetic quality violations and
therefore evaluate rule enforcement rather than the frequency or
distribution of naturally occurring agricultural data-quality errors.