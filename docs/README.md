# 🔍 IEEE-CIS Fraud Detection Pipeline

> Production-ready ETL pipeline for fraud detection using DuckDB, Apache Airflow, Great Expectations, dbt and Google Cloud Platform

[![Python](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Airflow](https://img.shields.io/badge/airflow-3.1.7-017CEE.svg)](https://airflow.apache.org/)
[![DuckDB](https://img.shields.io/badge/duckdb-1.4.4-yellow.svg)](https://duckdb.org/)
[![dbt](https://img.shields.io/badge/dbt-1.11.7-FF694B.svg)](https://www.getdbt.com/)
[![Docker](https://img.shields.io/badge/docker-required-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-19%20passed-brightgreen.svg)]()
[![GE](https://img.shields.io/badge/great%20expectations-16%2F16-brightgreen.svg)]()
[![dbt tests](https://img.shields.io/badge/dbt%20tests-17%2F17-brightgreen.svg)]()

## 📋 Table of Contents

- [About](#about)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Status](#project-status)
- [Getting Started](#getting-started)
- [Pipeline DAGs](#pipeline-dags)
- [Data Quality](#data-quality)
- [Testing](#testing)
- [dbt Models](#dbt-models)
- [Performance](#performance)
- [Contact](#contact)

## 🎯 About

End-to-end data engineering pipeline processing the [IEEE-CIS Fraud Detection dataset](https://www.kaggle.com/c/ieee-fraud-detection) (~590k transactions). Built to demonstrate production-grade data engineering practices.

**Key numbers:**

- 590,540 transactions processed
- 20,663 fraud cases (3.50% fraud rate)
- 37 MB Parquet (from 1.5 GB CSV — 40x compression)
- 19 pytest tests + 16 GE validations + 17 dbt tests — all green

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     FULL PIPELINE OVERVIEW                          │
│                                                                     │
│  train_transaction.csv ─┐                                          │
│                          ├─► DuckDB ─► Transform ─► Parquet        │
│  train_identity.csv    ─┘   (raw)     (features)   (37 MB)        │
│                                            │                        │
│                                            ▼                        │
│                                   Great Expectations                │
│                                   (16 validations)                  │
│                                            │                        │
│                                            ▼                        │
│                                 GCS Bucket ─► BigQuery             │
│                                 (data lake)   (warehouse)           │
│                                                    │                │
│                                                    ▼                │
│                                              dbt models             │
│                                         (stg + marts layer)         │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow (Airflow DAGs)

```
extract_raw_data
      │
      ▼
transform_data
      │
      ▼
validate_data          ← Great Expectations (16 checks)
      │
      ▼
load_to_gcp            ← GCS + BigQuery
      │
      ▼
dbt run                ← stg_fraud → fraud_hourly, fraud_summary
```

## 🛠️ Tech Stack

| Component            | Technology           | Version   | Purpose                       |
| -------------------- | -------------------- | --------- | ----------------------------- |
| **Orchestration**    | Apache Airflow       | 3.1.7     | DAG scheduling and monitoring |
| **Data Processing**  | DuckDB               | 1.4.4 LTS | OLAP queries, staging         |
| **DataFrames**       | Pandas               | 3.0.1     | Data manipulation             |
| **Storage**          | Parquet + Snappy     | —         | Columnar, compressed storage  |
| **Data Lake**        | Google Cloud Storage | —         | Parquet backup and staging    |
| **Data Warehouse**   | BigQuery             | —         | Analytics and reporting       |
| **Transformations**  | dbt                  | 1.11.7    | SQL models in BigQuery        |
| **Data Quality**     | Great Expectations   | 1.15.1    | Data validation               |
| **Testing**          | pytest + pytest-cov  | 9.0.2     | Unit and integration tests    |
| **Config**           | Pydantic Settings    | 2.12.5    | Type-safe configuration       |
| **Logging**          | Loguru               | 0.7.3     | Structured logging            |
| **Containerization** | Docker Compose       | —         | Local dev environment         |

## 📊 Project Status

### Phase 1: Foundation ✅ Complete

ETL pipeline with Airflow orchestration and DuckDB.

- [x] CSV ingestion → DuckDB (590k rows)
- [x] Data cleaning (NULL handling: -999 / 'unknown')
- [x] Feature engineering (temporal, flags, card aggregates)
- [x] Parquet export with Snappy compression (37 MB)
- [x] Airflow 3.x DAGs (TaskFlow API, `airflow.sdk`)
- [x] Docker Compose setup
- [x] Structured logging with Loguru
- [x] Pydantic Settings configuration

**DAGs**: `extract_raw_data` → `transform_data`

### Phase 2: Cloud & Quality ✅ Complete

GCP integration, data validation, dbt transformations, testing.

- [x] Google Cloud Storage upload
- [x] BigQuery load (WRITE_TRUNCATE, autodetect schema)
- [x] GCP auth via Application Default Credentials (ADC)
- [x] Great Expectations 1.15.1 — 16/16 validations passed
- [x] dbt 1.11.7 — 3 models, 17/17 tests passed
- [x] pytest — 19/19 unit tests passed
- [x] DuckDB in-memory fixtures for fast testing

**DAGs**: `validate_data` → `load_to_gcp`

**dbt models**: `stg_fraud` (view) → `fraud_hourly`, `fraud_summary` (tables)

### Phase 3: CI/CD & ML ⏳ In Progress

- [ ] CI/CD via GitHub Actions (pytest on every push)
- [ ] MLflow experiment tracking
- [ ] Baseline fraud detection model

## 🚀 Getting Started

### Prerequisites

```bash
- Docker Desktop 4.x+
- Python 3.11+
- Git
- Google Cloud SDK (for GCP integration)
```

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/Revo69/ieee-cis-fraud-detection.git
cd ieee-cis-fraud-detection

# 2. Copy environment variables
cp .env.example .env
# Edit .env — set your GCP project, bucket, dataset

# 3. Download dataset from Kaggle
# Place files in data/raw/:
#   train_transaction.csv
#   train_identity.csv

# 4. Authenticate with GCP
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# 5. Start Airflow
docker-compose up -d

# 6. Open Airflow UI
# http://localhost:8080
# Login: admin / admin

# 7. Run DAGs in order:
#   extract_raw_data → transform_data → validate_data → load_to_gcp
```

### Environment Variables (.env)

```env
# Airflow
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# GCP
FRAUD_GCP_PROJECT=your-project-id
FRAUD_GCS_BUCKET=your-bucket-name
FRAUD_BIGQUERY_DATASET=fraud_detection
GOOGLE_CLOUD_PROJECT=your-project-id
```

## 🔄 Pipeline DAGs

| DAG                | Description                                   | Phase |
| ------------------ | --------------------------------------------- | ----- |
| `hello_world`      | Environment check, library versions           | Test  |
| `extract_raw_data` | CSV → DuckDB (590k rows)                      | 1     |
| `transform_data`   | Merge + Clean + Feature Engineering → Parquet | 1     |
| `validate_data`    | Great Expectations (16 checks)                | 2     |
| `load_to_gcp`      | Parquet → GCS → BigQuery                      | 2     |

## ✅ Data Quality

### Great Expectations (16 validations)

```
Table level:
  ✓ Row count between 500k–700k
  ✓ Required columns exist

Column level:
  ✓ isFraud in {0, 1}, not null
  ✓ isFraud mean between 1%–10% (fraud rate)
  ✓ TransactionAmt not null
  ✓ log_TransactionAmt >= 0, not null
  ✓ ProductCD in {W, H, C, S, R, unknown}
  ✓ P_emaildomain not null
  ✓ tx_hour between 0–23
  ✓ tx_day_of_week between 0–6
  ✓ is_night_tx in {0, 1}
  ✓ is_large_tx in {0, 1}
  ✓ has_identity in {0, 1}
  ✓ card1_tx_count >= 0

Result: 16/16 PASSED ✅
```

### dbt Tests (17 tests)

```
Source tests (staging_fraud):
  ✓ TransactionID unique + not null
  ✓ isFraud not null, accepted values [0, 1]
  ✓ TransactionAmt not null
  ✓ has_identity accepted values [0, 1]
  ✓ tx_hour not null

Model tests (stg_fraud):
  ✓ transaction_id unique + not null
  ✓ is_fraud not null, accepted values [0, 1]
  ✓ transaction_amt not null
  ✓ tx_hour not null

Model tests (fraud_hourly, fraud_summary):
  ✓ tx_hour unique + not null
  ✓ card1 not null
  ✓ fraud_rate_pct not null

Result: 17/17 PASSED ✅
```

## 🧪 Testing

```bash
# Run all pytest tests
docker-compose exec --user airflow airflow-apiserver \
    python -m pytest tests/ -v

# Run with coverage
docker-compose exec --user airflow airflow-apiserver \
    python -m pytest tests/ --cov=src --cov-report=term-missing
```

### pytest Results

```
tests/unit/test_transformer.py::TestMergeTables::test_merge_preserves_all_transactions   PASSED
tests/unit/test_transformer.py::TestMergeTables::test_merge_adds_has_identity_flag        PASSED
tests/unit/test_transformer.py::TestMergeTables::test_merge_joins_device_info             PASSED
tests/unit/test_transformer.py::TestMergeTables::test_merge_null_device_for_missing_identity PASSED
tests/unit/test_transformer.py::TestCleanData::test_clean_fills_missing_email             PASSED
tests/unit/test_transformer.py::TestCleanData::test_clean_fills_missing_addr              PASSED
tests/unit/test_transformer.py::TestCleanData::test_clean_preserves_existing_values       PASSED
tests/unit/test_transformer.py::TestCleanData::test_clean_row_count_unchanged             PASSED
tests/unit/test_transformer.py::TestEngineerFeatures::test_features_tx_hour_range         PASSED
tests/unit/test_transformer.py::TestEngineerFeatures::test_features_is_night_tx           PASSED
tests/unit/test_transformer.py::TestEngineerFeatures::test_features_log_amount_positive   PASSED
tests/unit/test_transformer.py::TestEngineerFeatures::test_features_card_aggregates_exist PASSED
tests/unit/test_transformer.py::TestEngineerFeatures::test_features_is_large_tx           PASSED
tests/unit/test_transformer.py::TestEngineerFeatures::test_features_row_count_preserved   PASSED
tests/unit/test_transformer.py::TestSettings::test_settings_loads                         PASSED
tests/unit/test_transformer.py::TestSettings::test_default_paths_are_absolute             PASSED
tests/unit/test_transformer.py::TestSettings::test_table_names_not_empty                  PASSED
tests/unit/test_transformer.py::TestSettings::test_gcp_project_set                        PASSED
tests/unit/test_transformer.py::TestSettings::test_gcs_parquet_path_format                PASSED

19 passed in 2.14s ✅
```

## 📦 dbt Models

```
fraud_dbt/
└── models/
    ├── staging/
    │   └── stg_fraud.sql          # view — cleaned source data
    └── marts/
        ├── fraud_hourly.sql       # table — fraud stats by hour
        └── fraud_summary.sql      # table — fraud stats by card
```

```bash
# Run dbt models
cd fraud_dbt
dbt run    # PASS=3
dbt test   # PASS=17
dbt docs generate && dbt docs serve  # documentation
```

## ⚡ Performance

| Operation            | Before | After               | Gain            |
| -------------------- | ------ | ------------------- | --------------- |
| CSV storage          | 1.5 GB | 37 MB (Parquet)     | **40x smaller** |
| Extract (590k rows)  | —      | ~30s via DuckDB     | —               |
| Transform + Features | —      | ~60s via DuckDB SQL | —               |
| BigQuery load        | —      | ~90s via GCS        | —               |
| dbt run (3 models)   | —      | ~15s                | —               |
| pytest (19 tests)    | —      | 2.14s               | —               |

## 🔧 Project Structure

```
ieee-cis-fraud-detection/
├── dags/                          # Airflow DAGs
│   ├── hello_world_dag.py
│   ├── extract_raw_data_dag.py
│   ├── transform_data_dag.py
│   ├── validate_data_dag.py
│   └── load_to_gcp_dag.py
├── src/                           # Pipeline source code
│   ├── config/
│   │   └── settings.py            # Pydantic Settings
│   ├── extract/
│   │   └── csv_loader.py          # CSV → DuckDB
│   ├── transform/
│   │   └── transformer.py         # Merge + Clean + Features
│   ├── validate/
│   │   └── ge_validator.py        # Great Expectations
│   ├── load/
│   │   └── gcp_loader.py          # GCS + BigQuery
│   └── utils/
│       └── logger.py              # Loguru setup
├── tests/
│   ├── conftest.py                # pytest fixtures
│   └── unit/
│       └── test_transformer.py    # 19 unit tests
├── fraud_dbt/                     # dbt project
│   ├── models/
│   │   ├── staging/stg_fraud.sql
│   │   └── marts/
│   │       ├── fraud_hourly.sql
│   │       └── fraud_summary.sql
│   └── dbt_project.yml
├── data/
│   ├── raw/                       # Source CSV files (gitignored)
│   ├── processed/parquet/         # Output Parquet (gitignored)
│   └── duckdb/                    # DuckDB file (gitignored)
├── docker-compose.yml
├── requirements.txt
├── pytest.ini
└── .env.example
```

## 📬 Contact

**Serghei Matenco** — Data Engineer

- 📧 Email: sergey.revo@outlook.com
- 💼 LinkedIn: [serghei-matenco](https://linkedin.com/in/serghei-matenco)
- 🐙 GitHub: [@Revo69](https://github.com/Revo69)

---

⭐ **Star this repo** if you find it helpful!
