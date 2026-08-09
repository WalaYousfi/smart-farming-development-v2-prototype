## Processing-Time Experiment

### Objective

Measure the execution-time stability of heterogeneous Silver
integration and integrated Gold anomaly detection.

### Configuration

- Field observations: 500
- Weather observations: 500
- Integration coverage: 100%
- Repetitions: 3 for preliminary testing
- Isolation Forest contamination: 0.05
- Isolation Forest estimators: 200
- Random seed: 42
- Execution environment: local Docker-based prototype

### Results

| Stage | Mean (s) | Median (s) | Minimum (s) | Maximum (s) | Standard deviation (s) |
|---|---:|---:|---:|---:|---:|
| Field–Weather integration | 2.8349 | 2.7388 | 2.6726 | 3.0933 | 0.2262 |
| Integrated Gold processing | 5.4846 | 5.4174 | 5.2804 | 5.7560 | 0.2448 |
| Combined | 8.3195 | 8.1562 | 7.9530 | 8.8493 | 0.4699 |

The preliminary three-run experiment shows that the Field–Weather
integration stage required an average of 2.8349 seconds, while the
Integrated Gold anomaly-detection stage required an average of
5.4846 seconds. The complete integration and Gold-processing
workflow required an average of 8.3195 seconds.

The standard deviations were 0.2262 seconds for integration,
0.2448 seconds for Gold processing, and 0.4699 seconds for the
combined workflow. These relatively small variations indicate
reasonably stable execution times across the three preliminary runs
under the same local experimental conditions.

The Integrated Gold stage was the most time-consuming part of the
measured workflow. Its mean execution time was approximately 1.93
times the integration-stage mean, which is consistent with the
additional work required for feature preparation, Isolation Forest
training, anomaly scoring, and the generation of analytical and
consumer-oriented Gold outputs.

### Experiment artifact

The complete machine-generated experiment report is stored at:

`experiments/prototype-v2/repeated-runs/timing-test-3-runs_20260806T225321Z.json`
### Preliminary interpretation

To be completed after the timing values are generated.

### Limitations

- Preliminary results use only three repetitions.
- The dataset contains 500 records per source.
- Tests run locally on one machine.
- Results do not represent distributed execution.


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