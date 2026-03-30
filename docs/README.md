# 🔍 IEEE-CIS Fraud Detection Pipeline

> Production-ready ETL pipeline for fraud detection using DuckDB, Apache Airflow, Great Expectations, dbt, MLflow and Google Cloud Platform

[![Python](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Airflow](https://img.shields.io/badge/airflow-3.1.7-017CEE.svg)](https://airflow.apache.org/)
[![DuckDB](https://img.shields.io/badge/duckdb-1.4.4-yellow.svg)](https://duckdb.org/)
[![dbt](https://img.shields.io/badge/dbt-1.11.7-FF694B.svg)](https://www.getdbt.com/)
[![MLflow](https://img.shields.io/badge/mlflow-3.10.1-0194E2.svg)](https://mlflow.org/)
[![CI](https://github.com/Revo69/ieee-cis-fraud-detection/actions/workflows/ci.yml/badge.svg)](https://github.com/Revo69/ieee-cis-fraud-detection/actions/workflows/ci.yml)
[![Docker](https://img.shields.io/badge/docker-required-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/pytest-19%20passed-brightgreen.svg)]()
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
- [MLflow Experiments](#mlflow-experiments)
- [Performance](#performance)
- [Contact](#contact)

## 🎯 About

End-to-end data engineering pipeline processing the [IEEE-CIS Fraud Detection dataset](https://www.kaggle.com/c/ieee-fraud-detection) (~590k transactions). Built to demonstrate production-grade data engineering practices across the full stack.

**Key numbers:**

- 590,540 transactions processed
- 20,663 fraud cases (3.50% fraud rate)
- 28 MB Parquet (from 667 MB CSV — 24x compression)
- 19 pytest + 16 GE validations + 17 dbt tests — all green ✅
- CI/CD: automated tests on every push via GitHub Actions ✅
- 3 MLflow experiments tracked (best model: recall=0.65, roc_auc=0.74) ✅

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
│                                                                     │
│  Parquet ──────────────────────────────────► MLflow                │
│                                         (experiment tracking)       │
└─────────────────────────────────────────────────────────────────────┘
```

### Airflow DAG Flow

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
| **ML Tracking**      | MLflow               | 3.10.1    | Experiment tracking           |
| **ML Models**        | scikit-learn         | 1.8.0     | Baseline fraud model          |
| **Testing**          | pytest + pytest-cov  | 9.0.2     | Unit and integration tests    |
| **CI/CD**            | GitHub Actions       | —         | Automated testing on push     |
| **Config**           | Pydantic Settings    | 2.12.5    | Type-safe configuration       |
| **Logging**          | Loguru               | 0.7.3     | Structured logging            |
| **Containerization** | Docker Compose       | —         | Local dev environment         |

## 📊 Project Status

### Phase 1: Foundation ✅ Complete

- [x] CSV ingestion → DuckDB (590k rows, ~30s)
- [x] Data cleaning (NULL → -999 / 'unknown')
- [x] Feature engineering: temporal, flags, card aggregates
- [x] Parquet export with Snappy compression (37 MB)
- [x] Airflow 3.x DAGs with TaskFlow API (`airflow.sdk`)
- [x] Docker Compose setup (4 containers)
- [x] Structured logging with Loguru
- [x] Pydantic Settings configuration

### Phase 2: Cloud & Quality ✅ Complete

- [x] Google Cloud Storage upload
- [x] BigQuery load (WRITE_TRUNCATE, autodetect schema)
- [x] GCP auth via Application Default Credentials (no JSON keys)
- [x] Great Expectations 1.15.1 — 16/16 validations passed
- [x] dbt 1.11.7 — 3 models, 17/17 tests passed
- [x] pytest — 19/19 unit tests passed
- [x] DuckDB in-memory fixtures for fast isolated testing

### Phase 3: CI/CD & ML ✅ Complete

- [x] GitHub Actions CI — pytest on every push (Python 3.11 + 3.12)
- [x] MLflow experiment tracking (SQLite backend)
- [x] 3 baseline experiments: LogisticRegression variants
- [x] Best model: balanced class weights (recall=0.65, roc_auc=0.74)
- [x] Feature importance logging as MLflow artifacts

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

# 6. Open Airflow UI → http://localhost:8080
# Login: admin / admin

# 7. Run DAGs in order:
#   extract_raw_data → transform_data → validate_data → load_to_gcp

# 8. Run dbt models
cd fraud_dbt
dbt run
dbt test

# 9. Run MLflow experiments
cd ..
python src/ml/train.py
mlflow ui --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./mlartifacts
# Open: http://localhost:5000
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

### Great Expectations (16/16 passed)

```
Table:    row count 500k–700k, required columns exist
isFraud:  values in {0,1}, not null, mean between 1–10%
Amount:   not null, log_amount >= 0
Features: tx_hour 0–23, day_of_week 0–6
Flags:    is_night_tx, is_large_tx, has_identity in {0,1}
Cards:    card1_tx_count >= 0, P_emaildomain not null
```

### dbt Tests (17/17 passed)

```
Source:  TransactionID unique+not_null, isFraud values, amounts not null
Models:  stg_fraud, fraud_hourly, fraud_summary — all constraints pass
```

## 🧪 Testing

```bash
# Run inside Docker container
docker-compose exec --user airflow airflow-apiserver \
    python -m pytest tests/ -v

# Run locally
PYTHONPATH=. python -m pytest tests/ -v --cov=src

# CI/CD: runs automatically on every push to main/develop
```

### pytest Results (19/19 passed in 2.14s)

```
TestMergeTables      4/4  — LEFT JOIN logic, has_identity flag
TestCleanData        4/4  — NULL filling, value preservation
TestEngineerFeatures 6/6  — tx_hour, is_night_tx, log_amount, card aggregates
TestSettings         5/5  — paths, table names, GCP config
```

## 📦 dbt Models

```
fraud_dbt/models/
├── staging/
│   └── stg_fraud.sql          # view — cleaned source data
└── marts/
    ├── fraud_hourly.sql        # table — fraud rate by hour of day
    └── fraud_summary.sql       # table — fraud stats by card
```

```bash
cd fraud_dbt
dbt run     # PASS=3  (3 models built)
dbt test    # PASS=17 (17 data tests)
dbt docs generate && dbt docs serve
```

## 🤖 MLflow Experiments

Three LogisticRegression experiments on 590k transactions (22 features):

| Run           | class_weight | accuracy   | precision | recall     | f1     | roc_auc    |
| ------------- | ------------ | ---------- | --------- | ---------- | ------ | ---------- |
| baseline_C1.0 | None         | **0.9653** | 0.8235    | 0.0102     | 0.0201 | 0.7360     |
| balanced_C1.0 | balanced     | 0.6985     | 0.0731    | **0.6523** | 0.1315 | **0.7409** |
| balanced_C0.1 | balanced     | 0.6985     | 0.0732    | **0.6526** | 0.1316 | **0.7409** |

**Key insight:** For fraud detection, `recall` matters more than `accuracy`.
Baseline looks great (96.5% accuracy) but catches only 1% of fraud.
Balanced model catches 65% of fraud — the right choice for this problem.

**Top features (by coefficient magnitude):** `C2`, `C11`, `addr1`, `has_identity`, `C1`

```bash
# Run experiments
python src/ml/train.py

# View results in MLflow UI
mlflow ui --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./mlartifacts
# Open: http://localhost:5000
```

## ⚡ Performance

| Operation                 | Result                   |
| ------------------------- | ------------------------ |
| CSV → Parquet compression | 1.5 GB → 37 MB (**40x**) |
| Extract (590k rows)       | ~30s via DuckDB          |
| Transform + Features      | ~60s via DuckDB SQL      |
| BigQuery load             | ~90s via GCS             |
| dbt run (3 models)        | ~15s                     |
| pytest (19 tests)         | **2.14s**                |
| MLflow (3 experiments)    | ~60s total               |

## 🔧 Project Structure

```
ieee-cis-fraud-detection/
├── .github/workflows/
│   └── ci.yml                     # GitHub Actions CI
├── dags/                          # Airflow DAGs
│   ├── hello_world_dag.py
│   ├── extract_raw_data_dag.py
│   ├── transform_data_dag.py
│   ├── validate_data_dag.py
│   └── load_to_gcp_dag.py
├── src/                           # Pipeline source code
│   ├── config/settings.py         # Pydantic Settings
│   ├── extract/csv_loader.py      # CSV → DuckDB
│   ├── transform/transformer.py   # Merge + Clean + Features
│   ├── validate/ge_validator.py   # Great Expectations
│   ├── load/gcp_loader.py         # GCS + BigQuery
│   ├── ml/train.py                # MLflow experiments
│   └── utils/logger.py            # Loguru setup
├── tests/
│   ├── conftest.py                # pytest fixtures (DuckDB in-memory)
│   └── unit/test_transformer.py   # 19 unit tests
├── fraud_dbt/                     # dbt project
│   ├── models/staging/stg_fraud.sql
│   └── models/marts/
│       ├── fraud_hourly.sql
│       └── fraud_summary.sql
├── data/
│   ├── raw/                       # Source CSV (gitignored)
│   ├── processed/parquet/         # Output Parquet (gitignored)
│   └── duckdb/                    # DuckDB file (gitignored)
├── docker-compose.yml
├── requirements.txt               # Production deps
├── requirements-dev.txt           # Dev deps (pytest, ruff, jupyter)
├── requirements-ci.txt            # CI deps (lightweight)
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
