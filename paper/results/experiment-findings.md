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

The repeated experiment showed substantial execution-time variability
under the local Docker-based environment. Field–Weather integration
required a mean of 3.9570 s, while integrated Gold processing required
a mean of 7.6086 s. The corresponding coefficients of variation were
0.6748 and 0.6632, indicating that execution times were not highly
stable across the five repetitions.

The median durations were considerably lower than the respective
means, particularly for integrated Gold processing, because one or
more slower executions increased the average. Consequently, median
values provide a useful complement to mean execution time for this
small experimental sample.

Integrated Gold processing was consistently the more computationally
expensive of the two evaluated stages, which is consistent with its
additional responsibilities including feature preparation, Isolation
Forest training and scoring, Gold-product generation, object-storage
writes, manifest generation, and lineage recording.

These measurements characterize prototype behavior under the tested
local environment and are not intended as claims of production-scale
or distributed-system performance.

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

- Records scored: 500
- Normal observations: 475
- Anomalous observations: 25
- Weather-matched observations: 500
- Weather-matched anomalies: 25

#### interpretation: Gold anomalies

The anomaly count should be interpreted in relation to the configured
Isolation Forest contamination parameter of 0.05. Therefore, the
25 anomaly-labelled observations are model outputs under the defined
experimental configuration and should not be interpreted as an
estimated 5% prevalence of true agricultural anomalies.


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

- Input records: 20
- Accepted records: 0
- Quarantined records: 20
- Correctly detected invalid records: 20
- Invalid-record detection rate: 100%
- Defect categories detected: 4/4

All injected violations were rejected during canonical schema
validation. The quarantine records retained the corresponding
validation reason and source provenance.

### Architectural significance

The experiment evaluates the separation of responsibilities between
Bronze and Silver. Bronze is expected to preserve source observations,
including invalid values, while Silver applies explicit quality rules
and separates accepted from quarantined records.

### Limitation

The injected defects are controlled synthetic quality violations and
therefore evaluate rule enforcement rather than the frequency or
distribution of naturally occurring agricultural data-quality errors.

## Mixed Valid-Invalid Quality Classification Experiment

### Objective

Evaluate whether the Silver quality-control stage can correctly
distinguish known-valid observations from deliberately corrupted
observations rather than simply rejecting all records in a stress-test
dataset.

### Experimental design

A balanced controlled dataset containing 40 Field observations was
constructed:

- 20 known-valid observations.
- 20 deliberately invalid observations.
- Four invalid-data categories were represented:
  - Invalid soil moisture.
  - Invalid humidity.
  - Invalid soil pH.
  - Invalid NDVI.

The dataset was shuffled using a fixed random state of 42 before
ingestion. Ground-truth labels were stored independently before
processing.

### Silver processing result

- Input records: 40
- Accepted records: 20
- Quarantined records: 20
- Acceptance rate: 0.50
- Uniqueness score: 1.00
- Composite Silver quality score: 0.75

The current prototype defines the composite Silver quality score as the
mean of the acceptance rate and uniqueness score:

`Composite quality score = (acceptance rate + uniqueness score) / 2`

This metric is used as an architectural monitoring indicator and is
evaluated separately from ground-truth classification metrics such as
accuracy, precision, recall, and F1 score.


### Record-level classification results

| Metric | Result |
|---|---:|
| True positives | 20 |
| True negatives | 20 |
| False positives | 0 |
| False negatives | 0 |
| Accuracy | 1.0000 |
| Precision | 1.0000 |
| Recall | 1.0000 |
| Specificity | 1.0000 |
| F1 score | 1.0000 |
| False-positive rate | 0.0000 |
| Missing records | 0 |

### Traceability validation

The evaluator reconstructed each quality-test observation through the
Bronze-to-Silver provenance chain.

- MIXED_QUALITY_BRONZE_RUN_ID =  `20260811T194526Z_6e6b7acf` 
- MIXED_QUALITY_SILVER_RUN_ID =  `20260811T212148Z_5754ed4c `
- Controlled test IDs recovered from Bronze: 40
- Accepted Silver event IDs recovered: 20
- Quarantined event IDs recovered: 20
- Missing records: 0

The experimental identifier was retained only in the Bronze payload,
while accepted canonical Silver records used `source_event_id` to
preserve provenance without polluting the canonical schema with
experiment-specific attributes.

### Interpretation

Under the controlled rule-based experiment, the Silver stage correctly
classified all 40 observations. All known-valid observations were
accepted and all deliberately invalid observations were quarantined.

The result demonstrates correct enforcement of the four evaluated
quality-rule categories and confirms that quality decisions remain
traceable to the original Bronze events.

### Limitation

The result should not be interpreted as evidence of perfect quality
classification for arbitrary real-world agricultural data. The
experiment evaluates a defined set of synthetic violations against
explicit validation rules.



## End-to-End Traceability Evaluation

### Objective

Evaluate whether the provenance of an integrated Gold analytical product can be reconstructed automatically through both heterogeneous Silver branches to their original Bronze ingestion runs.

### Experimental procedure

The evaluator started from the completed integrated Gold run and recursively followed the `parent_run_ids` stored in transformation lineage records. Bronze ingestion runs were treated as lineage roots rather than transformation jobs. Source-system information was recovered from the Bronze object paths referenced by the corresponding Silver lineage records.

### Results

| Metric                                   |   Result |
| ---------------------------------------- | -------: |
| Lineage graph nodes discovered           |    6 / 6 |
| Transformation edges discovered          |    5 / 5 |
| Transformation lineage records recovered |    4 / 4 |
| Bronze source roots recovered            |    2 / 2 |
| Source systems recovered                 |        2 |
| Maximum lineage depth                    |        3 |
| Node completeness                        |   1.0000 |
| Edge completeness                        |   1.0000 |
| Transformation-lineage completeness      |   1.0000 |
| Bronze-root completeness                 |   1.0000 |
| Multi-parent integration preserved       |     True |
| End-to-end source ancestry preserved     |     True |
| Overall traceability status              | Complete |

The recovered lineage graph contained two independent Bronze roots, one for the crop-field source and one for the weather source. These roots were linked to their corresponding canonical Silver processing runs. Both Silver runs were then recovered as parents of the heterogeneous integration run, which in turn was identified as the direct parent of the integrated Gold anomaly-detection run.

The reconstructed ancestry therefore followed the complete path:

`Field Bronze → Field Silver → Integrated Silver → Gold`

and:

`Weather Bronze → Weather Silver → Integrated Silver → Gold`

The integration stage preserved both Silver parents explicitly, demonstrating that heterogeneous multi-parent provenance was maintained rather than collapsed into a single upstream reference.

### Interpretation

The experiment demonstrates complete run-level traceability for the controlled end-to-end prototype execution. Starting only from the Gold run identifier, the evaluator reconstructed all expected transformation dependencies and both source-specific Bronze roots.

This result supports the use of run-level lineage as a cross-cutting architectural mechanism linking data maturity and functional processing stages. It also demonstrates that provenance can be reconstructed without embedding source-specific experimental attributes into downstream canonical datasets.

### Limitation

The evaluation verifies lineage completeness within the implemented prototype and controlled two-source workflow. It does not evaluate large-scale lineage-graph traversal, lineage across external systems, schema-evolution provenance, or distributed metadata-catalog performance.
