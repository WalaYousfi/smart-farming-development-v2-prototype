# Smart Farming — Dual-Dimensional Data Lake Prototype (V2)

A research prototype for **ingesting, cleaning, integrating, governing, and analyzing heterogeneous agricultural observations**. It combines **functional pipeline stages** (ingestion, validation/canonicalization, integration, analytics) with **data-maturity zones** (Bronze, Silver, Gold). Run-specific manifests, quality reports, and lineage records provide traceability across the stages.

The current end-to-end implementation uses **Field CSV** and **Weather JSON** source data, simulates both as Kafka streams, stores data in **MinIO**, and trains an **Isolation Forest** anomaly detector on integrated observations. The prototype is a research demonstrator, not a production farm monitoring system.

## Architecture

```text
Field CSV ──> reproducible Field generator ──> Kafka Field topic ──> Field Bronze (JSONL)
                                                                         │
                                                                         ▼
                                                              Field Silver (Parquet)
                                                              normalize / validate
                                                                         │
                                                                         ├──────────┐
                                                                         │          │
Weather JSON -> reproducible Weather generator -> Kafka Weather topic -> Weather Bronze (JSONL)
                                                                         │          │
                                                                         ▼          │
                                                              Weather Silver (Parquet)
                                                              normalize / validate  │
                                                                         │          │
                                                                         └────┬─────┘
                                                                              ▼
                                                            Integrated Silver (Parquet)
                                                            join by farm ID + date
                                                                              │
                                                                              ▼
                                                            Gold: Isolation Forest
                                                            train / test / score
                                                                              │
                                          ┌───────────────────────────────────┴───────────┐
                                          ▼                                               ▼
                                  Scored observations                            Alerts and summary
                                          │
                                          ▼
                                 Holdout evaluation
                                 independent labels, metrics

Cross-cutting governance: run IDs, manifests, lineage, quality reports, quarantine,
source-event provenance, and reproducible configuration.
```

**Functional dimension:** ingestion → source normalization and validation → canonicalization → Field–Weather integration → analytics and evaluation. **Maturity dimension:** Bronze (raw envelopes), Silver (validated canonical data and integrated observations), Gold (model, scores, alerts, metrics). These are distinct architectural concerns, not two separate physical storage systems.

## Data and augmentation

| Source               | Repository file                                 | Original format | Role                                                                                                   |
| -------------------- | ----------------------------------------------- | --------------- | ------------------------------------------------------------------------------------------------------ |
| Field observations   | `data/source/Smart_Farming_Crop_Yield_2024.csv` | CSV             | Crop, soil, vegetation, weather-related field measurements, farm and date identifiers                  |
| Weather observations | `data/source/weather/weather_observations.json` | JSON            | Station measurements, including temperature, humidity, rainfall, sunlight, wind, pressure and location |

The supplied workload generator starts from **500 matched Field–Weather source pairs** and can create larger, deterministic evaluation workloads. It uses consistent synthetic farm IDs and date shifts for both streams, avoiding duplicate integration keys and maintaining Field–Weather correspondence. Records derived from the same source pair share an `augmentation_family_id`.

For the demonstrated **100,000-Field + 100,000-Weather** workload, the generator (`pipeline/evaluation/stream_augmented_workload.py`) uses seed `42`, an anomaly injection rate of `0.05`, and batch size `1000`. The verified workload contained **500 augmentation families**, **5,000 independently labeled Field anomalies**, and **100,000 matching Field–Weather keys**. The injected anomalies are controlled, schema-valid numerical patterns (for example low soil moisture with high temperature); they are **not necessarily invalid data** and need not be quarantined by Silver.

The Weather source contains daily dates such as `2024-01-29`. The canonical mapper represents a date-only observation as midnight UTC (`2024-01-29T00:00:00+00:00`) **by convention**. This is a normalization convention, **not a claim that the station measured at midnight**. Integration matches by farm ID and calendar date, not exact observation timestamp.

## Technologies and versions

| Component                     | Used for                                    | Version information                                                                                                       |
| ----------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Python                        | Pipeline and evaluation code                | **Python 3.7-compatible code**; the successful runs used a Python 3.7 environment (exact patch release not recorded)      |
| `kafka-python`                | Python producer and consumers               | Declared in `requirements.txt`; **not pinned**                                                                            |
| `pandas`, `pyarrow`           | Tabular transformations and Parquet         | Declared; **not pinned**                                                                                                  |
| `scikit-learn`, `numpy`       | GroupShuffleSplit and Isolation Forest      | scikit-learn declared; exact installed versions **not recorded**                                                          |
| `minio`                       | Object storage client                       | Declared; **not pinned**                                                                                                  |
| `jsonschema`, `python-dotenv` | Validation and environment configuration    | Declared; **not pinned**                                                                                                  |
| Apache Kafka                  | Event streaming                             | Docker image `apache/kafka:latest` in the current Compose file; **not pinned**                                            |
| MinIO                         | S3-compatible data lake                     | Docker image `minio/minio` in the current Compose file; **not pinned**                                                    |
| Node.js, KafkaJS, csv-parser  | Optional original CSV/JSON producer scripts | `package.json` declares KafkaJS `^2.2.4` and csv-parser `^3.2.1`; `package-lock.json` resolves installed package versions |
| Docker Compose                | Local Kafka and MinIO services              | Local Docker/Compose version **not recorded**                                                                             |
| Isolation Forest              | Gold anomaly detection                      | **Application model version `2.2.0`**, not the scikit-learn library version; `200` trees, seed `42`, contamination `0.05` |

**Reproducibility limitation:** `requirements.txt` and Docker images are not fully version-pinned. To reproduce the _exact software environment_ of an existing run, capture `python --version`, `python -m pip freeze`, `docker compose version`, and the image digests, then create a lockfile or pinned environment. A fresh install with unpinned dependencies may not work with Python 3.7; select Python-3.7-compatible package releases or upgrade and test the code in a newer environment. Do not interpret model version `2.2.0` as a library version.

## Repository layout

```text
smart-farming-development/
├── data/source/                   # Original Field CSV and Weather JSON
├── schemas/source/                # Source schemas
├── schemas/canonical/             # Canonical Silver schemas
├── pipeline/
│   ├── common/                    # Config, MinIO, manifests, lineage, run IDs
│   ├── ingestion/                 # Field and Weather Kafka → Bronze consumers
│   ├── silver/                    # Field and Weather normalization, validation, mapping
│   ├── integration/               # Canonical Field–Weather join
│   ├── gold/                      # Isolation Forest and Gold run selection
│   └── evaluation/                # Workload generation, holdout and other experiments
├── scripts/                       # Supporting data generation
├── tests/                         # Automated checks
├── docker-compose.yml             # Kafka and MinIO
├── requirements.txt               # Python dependencies (currently unpinned)
├── package.json / package-lock.json
├── .env.example                   # Configuration template
├── .env.evaluation                # Evaluation environment settings
└── README.md
```

## Start-to-finish walkthrough (Windows PowerShell)

Run commands from the **repository root** (`smart-farming-development`). This walkthrough reproduces the evaluated **100,000 + 100,000 record** workflow. It assumes Docker Desktop, Python, and Git are installed and Kafka/MinIO ports `9092`, `9000`, and `9001` are available.

### 1. Obtain the project and install dependencies

```powershell
# If hosted in Git, clone your actual repository URL:
# git clone <YOUR_REPOSITORY_URL>
# cd smart-farming-development

# Or extract the provided project ZIP and open PowerShell in its root.

python --version
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

**Python 3.7 note:** the newest versions of some unpinned packages do not support Python 3.7. Use compatible package versions; a fully pinned `requirements-lock.txt` is not yet provided. The optional Node.js producers are **not required** for the Python-based 100,000-record workflow. For those scripts, install Node.js and run `npm ci` separately.

### 2. Configure local services and credentials

```powershell
docker compose up -d
docker compose ps
```

MinIO API: `http://localhost:9000`; MinIO Console: `http://localhost:9001`; Kafka bootstrap server: `localhost:9092`. The current `docker-compose.yml` supplies demonstration MinIO credentials. **Change these before sharing or deploying the project.**

The application selects `.env.evaluation` when `APP_ENV=evaluation`; otherwise it loads `.env`. Ensure the relevant file exists and its Kafka/MinIO settings match Docker Compose. For a new checkout, create your own local configuration from `.env.example`, and set `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` to the credentials used by MinIO. The included `.env.evaluation` defines the dedicated evaluation topics and bucket; review its credentials before running.

```powershell
$env:APP_ENV = "evaluation"
```

Evaluation configuration used in the demonstrated run:

```dotenv
KAFKA_SERVER=localhost:9092
KAFKA_TOPIC=eval-100000-field-readings
WEATHER_KAFKA_TOPIC=eval-100000-weather-readings
MINIO_ENDPOINT=localhost:9000
MINIO_BUCKET=smart-farming-100000-records-evaluation
MINIO_SECURE=false
BRONZE_BATCH_SIZE=1000
KAFKA_CONSUMER_GROUP=bronze-field-100k-new-run
WEATHER_CONSUMER_GROUP=bronze-weather-100k-new-run
```

The remaining source metadata variables are documented in `.env.example` and `.env.evaluation`. **Never commit real credentials.** The consumers use Kafka `auto_offset_reset="earliest"`, but this only applies to groups with no committed offsets. For a fresh experiment, use new group IDs and avoid reusing old Kafka topics containing previous workloads; a new MinIO bucket alone does **not** reset Kafka offsets or delete topic messages.

### 3. Check the deterministic workload before publishing

```powershell
python -m pipeline.evaluation.stream_augmented_workload `
    --source field `
    --count 100000 `
    --seed 42 `
    --anomaly-rate 0.05 `
    --batch-size 1000 `
    --check-only
```

Expected checks: 100,000 Field records, 100,000 Weather records, 500 augmentation families, 5,000 anomaly labels, 100,000 matching keys. `--check-only` does not publish to Kafka.

### 4. Publish Field and Weather workloads to Kafka

```powershell
python -m pipeline.evaluation.stream_augmented_workload `
    --source field `
    --count 100000 `
    --seed 42 `
    --anomaly-rate 0.05 `
    --batch-size 1000

python -m pipeline.evaluation.stream_augmented_workload `
    --source weather `
    --count 100000 `
    --seed 42 `
    --anomaly-rate 0.05 `
    --batch-size 1000
```

Expected: 100,000 published messages per topic. The same count, seed, and anomaly rate preserve paired source generation. Do **not** republish into the same topics and then blindly consume from offset zero: that can duplicate the experiment.

### 5. Ingest both streams into Bronze

```powershell
# Use new group IDs for this run.
$env:KAFKA_CONSUMER_GROUP = "bronze-field-100k-20261101"
python -m pipeline.ingestion.bronze_consumer --max-records 100000

$env:WEATHER_CONSUMER_GROUP = "bronze-weather-100k-20261101"
python -m pipeline.ingestion.weather_bronze_consumer --max-records 100000
```

Expected: 100,000 Bronze envelopes for each source, stored as run-partitioned JSONL in MinIO. Record the **Field Bronze** and **Weather Bronze run IDs** printed by the consumers. For later independent experiments, use **new** group IDs (the dates above are examples, not mandatory identifiers).

### 6. Normalize, validate and canonicalize both Silver datasets

```powershell
python -m pipeline.silver.silver_processor
python -m pipeline.silver.weather_silver_processor
```

Both processors default to the latest usable Bronze run; for reproducibility you can specify `--bronze-run-id <FIELD_BRONZE_RUN_ID>` or `--bronze-run-id <WEATHER_BRONZE_RUN_ID>` respectively. Field and Weather Silver perform source normalization, schema and semantic validation, canonical mapping, cleaning and quality accounting. Invalid records are quarantined when applicable; manifests, quality reports and lineage are recorded.

**Do not assume zero quarantines for arbitrary input data.** The demonstrated schema-valid injected anomalies passed Silver validation, which is consistent with their experimental design.

Record the resulting **Field Silver** and **Weather Silver run IDs**.

### 7. Integrate Field and Weather Silver

```powershell
python -m pipeline.integration.field_weather_integration `
    --field-silver-run-id <FIELD_SILVER_RUN_ID> `
    --weather-silver-run-id <WEATHER_SILVER_RUN_ID>
```

Replace the angle-bracket placeholders with actual run IDs (without brackets). The integration uses farm ID and observation date, checks matching/cardinality constraints, and writes integrated Silver Parquet plus quality, manifest and lineage artifacts. Record the **integration run ID**.

### 8. Train and score the Gold anomaly model

```powershell
python -m pipeline.gold.integrated_anomaly_detection `
    --integration-run-id <INTEGRATION_RUN_ID> `
    --require-family-groups `
    --contamination 0.05
```

The model uses **14 numerical features** (eight Field and six Weather features), `IsolationForest(n_estimators=200, random_state=42, contamination=0.05)`, and `GroupShuffleSplit` with seed `42` and target 70/30 split. Augmented siblings remain in the same partition. The model **fits on training observations only**, then scores both partitions. Family IDs and labels are **not** model inputs. Gold saves the model (`model.pkl`), split metadata, scored Parquet, alerts, summary, manifest, and lineage. Record the **Gold run ID**.

### 9. Evaluate on the held-out test partition

```powershell
python -m pipeline.evaluation.evaluate_gold_holdout `
    --gold-run-id <GOLD_RUN_ID>
```

The evaluation checks for family overlap and compares predictions (`-1` = anomaly) against independently generated labels (`1` = anomaly) **on test records only**. It writes a JSON report containing TP, TN, FP, FN, accuracy, precision, recall, specificity and F1 to:

```text
gold/evaluation/holdout_metrics/run_id=<GOLD_RUN_ID>/metrics.json
```

**Example: the completed 2026-11-01 evaluation** (not guaranteed for different inputs or environments):

| Stage / metric                      |                                  Observed result |
| ----------------------------------- | -----------------------------------------------: |
| Field Bronze / Weather Bronze       | 100,000 / 100,000 records published and ingested |
| Field Silver / Weather Silver       |        100,000 / 100,000 accepted; 0 quarantined |
| Integrated Silver                   |       100,000 matched Field records; 0 unmatched |
| Gold normal / anomalous predictions |                                   94,495 / 5,505 |
| Train / test                        |                                  70,190 / 29,810 |
| Test-set TP / TN / FP / FN          |                       994 / 27,221 / 1,139 / 456 |
| Test-set accuracy                   |                                           0.9465 |
| Test-set precision                  |                                           0.4660 |
| Test-set recall                     |                                           0.6855 |
| Test-set specificity                |                                           0.9598 |
| Test-set F1                         |                                           0.5548 |

Recorded run IDs for this specific example:

```text
Field Silver:       20261101T014004Z_8c22d396
Weather Silver:     20261101T014137Z_29fad255
Integrated Silver:  20261101T014305Z_3ab8b865
Gold:               20261101T014359Z_08f0f91b
```

These results show detection of some injected anomalies, **not** 94.65% anomaly-detection recall. Because 95% of observations are labeled normal, accuracy alone is misleading; precision, recall, F1 and the confusion matrix must also be reported. The 100,000 observations derive from 500 source families, not 100,000 independent farms.

### 10. Run automated tests (recommended)

```powershell
python -m pip install pytest
python -m pytest -q
```

The repository includes tests for source normalization, schemas, augmentation provenance, Field–Weather matching, run selection, Gold splitting, holdout evaluation and lineage. Passing tests are not a substitute for the end-to-end MinIO/Kafka run.

## Storage organization and provenance

Representative MinIO keys (the run IDs vary):

```text
bronze/source=smart_farming_crop_yield_csv/.../run_id=.../partition=0/events_*.jsonl
bronze/source=farm_weather_station/.../run_id=.../partition=0/events_*.jsonl
silver/accepted/field_observations/run_id=.../field_observations.parquet
silver/accepted/weather_observations/run_id=.../weather_observations.parquet
silver/quarantine/.../run_id=.../...
silver/integrated/field_weather_observations/run_id=.../integrated_observations.parquet
gold/models/integrated_isolation_forest/run_id=.../model.pkl
gold/evaluation/train_test_split/run_id=.../split_metadata.json
gold/analytical/integrated_field_weather_scores/run_id=.../scored_integrated_observations.parquet
gold/data-products/integrated_field_weather_alerts/run_id=.../alerts.parquet
gold/data-products/integrated_anomaly_summary/run_id=.../summary.json
gold/evaluation/holdout_metrics/run_id=.../metrics.json
metadata/manifests/<job>/run_id=.../manifest_started.json
metadata/manifests/<job>/run_id=.../manifest_completed.json
metadata/lineage/<job>/run_id=.../lineage.json
metadata/quality-reports/.../run_id=.../quality_report.json
```

The exact output paths and availability of quarantine or alert objects depend on the stage and its results. The lineage structure links Gold to integrated Silver, which has **two parent Silver runs**, and onward to the respective Bronze ingestion runs. Manifests record stage status, inputs, outputs, run IDs and execution metrics. Source event IDs and schema versions support record-level provenance.

## Additional evaluation scripts

The `pipeline/evaluation/` directory also contains targeted research experiments, including quality robustness/classification, integration coverage, runtime comparisons (V1 versus V2), storage overhead, lineage traceability, and replay. These are **separate experiments**; they are not required for the nine-stage end-to-end workflow above. Check each script's `--help` and input requirements before using it. The Field-only baseline and historical evaluation results should be retained or archived if needed for thesis comparisons.

## Known limitations and research scope

- The primary large-scale evaluation uses **synthetic augmentation of 500 matched source pairs**, not 100,000 independently collected farm observations.
- Field–Weather integration currently relies on **exact farm ID and calendar-date matching**; fuzzy identifier reconciliation and missing Weather coverage are not established by the 100% matched workload.
- The anomaly model is **unsupervised Isolation Forest**. The ground-truth labels are used for evaluation, not supervised training. The reported metrics reflect the chosen synthetic injection patterns and data distribution.
- Gold currently requires fully matched Field–Weather records with valid numerical features; missing Weather observations require a separate integration-coverage experiment rather than being silently imputed.
- Silver's overall quality score reflects its implemented quality criteria; **a score of 1.0 does not imply that every observation is physically realistic or free from statistical anomalies**.
- Current Python dependencies and container images are not pinned to immutable versions. Pin them before making strong bit-for-bit reproducibility claims.
- This repository is a research prototype. Local Docker credentials and development configuration must be hardened before deployment.

## License and citation

No explicit project license or formal citation metadata was identified in the reviewed repository. Add a `LICENSE` and thesis/paper citation information if the project will be publicly distributed.
