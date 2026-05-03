# Stack Overflow Analytics Warehouse

End-to-end batch ELT pipeline that ingests Stack Overflow public data from BigQuery, stages it in Google Cloud Storage as Parquet, loads it into a BigQuery warehouse, and transforms it through a medallion architecture (raw → trusted → marts) using dbt. Built as the capstone project for the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).

## Problem

Stack Overflow's public dataset holds ~15 years of question, answer, user and tag activity. The raw tables are large, denormalized, and not suited for direct analytics. This pipeline transforms them into a partitioned, clustered, analytics-ready warehouse with a dimensional model layered for BI consumption — enabling analysis of technology adoption trends, community engagement, and answer-quality dynamics over time.

## Architecture

Warehouse-centric batch ELT on GCP. GCS acts as a Parquet staging zone; BigQuery is the system of record. The `raw` dataset uses native managed tables, not external tables over GCS.

```
┌──────────────────────────────────────┐
│  bigquery-public-data.stackoverflow  │
└─────────────────┬────────────────────┘
                  │  Airflow — EXPORT DATA / BigQueryToGCSOperator
                  ▼
┌──────────────────────────────────────┐
│  GCS — Parquet staging               │
│  posts_*/year=YYYY/                  │
│  users|tags/snapshot_date=YYYY-MM-DD │
└─────────────────┬────────────────────┘
                  │  Airflow — BigQueryInsertJob / GCSToBigQueryOperator
                  ▼
┌──────────────────────────────────────┐
│  BigQuery — stackoverflow_raw        │  ← native managed tables
└─────────────────┬────────────────────┘
                  │  dbt run --select staging
                  ▼
┌──────────────────────────────────────┐
│  BigQuery — stackoverflow_trusted    │  ← partitioned + clustered tables
│  + intermediate views                │
└─────────────────┬────────────────────┘
                  │  dbt run --select marts
                  ▼
┌──────────────────────────────────────┐
│  BigQuery — stackoverflow_marts      │  ← dimensional model
└─────────────────┬────────────────────┘
                  │
                  ▼
           Looker Studio  (in progress)
```

## Tech stack

| Layer           | Tool                                |
| --------------- | ----------------------------------- |
| Cloud           | GCP                                 |
| Infrastructure  | Terraform                           |
| Orchestration   | Apache Airflow 3.2 (Docker Compose) |
| Staging storage | Google Cloud Storage (Parquet)      |
| Data warehouse  | BigQuery                            |
| Transformation  | dbt (`dbt-bigquery`, `dbt-utils`)   |
| Dashboard       | Looker Studio _(in progress)_       |

## Dataset

[Stack Overflow — BigQuery Public Data](https://console.cloud.google.com/marketplace/product/stack-exchange/stack-overflow)

Tables ingested: `posts_questions`, `posts_answers`, `users`, `tags`.

`posts_questions` and `posts_answers` are exported per year (2008–2022) via `EXPORT DATA` to keep file sizes manageable and enable parallelized Airflow tasks. `users` and `tags` are exported as full snapshots keyed by date.

## Repository structure

```
.
├── airflow/
│   └── dags/
│       └── stackoverflow_pipeline.py    # extract → load → transform DAG
├── dbt/
│   ├── profiles.example.yml             # profile template (env-var driven)
│   └── stackoverflow/
│       ├── dbt_project.yml              # schema routing via +schema per layer
│       ├── packages.yml                 # dbt-labs/codegen, dbt-labs/dbt_utils
│       └── models/
│           ├── staging/
│           │   ├── _stackoverflow__sources.yml
│           │   ├── _staging__models.yml
│           │   ├── stg_stackoverflow__posts_questions.sql
│           │   ├── stg_stackoverflow__posts_answers.sql
│           │   ├── stg_stackoverflow__users.sql
│           │   └── stg_stackoverflow__tags.sql
│           ├── intermediate/
│           │   ├── _stackoverflow__models.yml
│           │   ├── int_answer_acceptance.sql
│           │   └── int_bridge_question_tags.sql
│           └── marts/
│               ├── _stackoverflow__models.yml
│               ├── dim_date.sql
│               ├── dim_tags.sql
│               ├── dim_users.sql
│               ├── fact_questions.sql
│               ├── fact_answers.sql
│               └── mart_technology_trends.sql
├── terraform/
│   ├── main.tf                          # GCS bucket + 3 BQ datasets
│   ├── variables.tf
│   ├── outputs.tf
│   └── providers.tf
├── credentials/                         # service-account JSON (gitignored)
├── docker-compose.yaml                  # Airflow stack
├── Dockerfile                           # apache/airflow:3.2.1 + google provider + dbt-bigquery
└── .env.example
```

## BigQuery layout & optimization

Dataset routing is handled by `+schema` in `dbt_project.yml` against a base profile dataset of `stackoverflow`:

| Layer        | `+schema` | Target dataset          | Materialization |
| ------------ | --------- | ----------------------- | --------------- |
| Staging      | `trusted` | `stackoverflow_trusted` | table           |
| Intermediate | `trusted` | `stackoverflow_trusted` | view            |
| Marts        | `marts`   | `stackoverflow_marts`   | table           |

### Trusted layer — partition & cluster strategy

| Model                                | Partition (monthly) | Cluster                            |
| ------------------------------------ | ------------------- | ---------------------------------- |
| `stg_stackoverflow__posts_questions` | `creation_date`     | `owner_user_id`, `id`              |
| `stg_stackoverflow__posts_answers`   | `creation_date`     | `parent_id`, `owner_user_id`, `id` |
| `stg_stackoverflow__users`           | `creation_date`     | `id`                               |
| `stg_stackoverflow__tags`            | —                   | —                                  |

Monthly partitioning on `creation_date` minimizes scanned bytes for time-filtered queries. Clustering on `owner_user_id`/`id` accelerates user-level joins across questions, answers and users; `parent_id` clustering on answers accelerates the most frequent join in the marts layer.

### Marts layer — cluster strategy

| Model                    | Cluster                        |
| ------------------------ | ------------------------------ |
| `dim_date`               | `date_day`                     |
| `dim_tags`               | `tag_id`                       |
| `dim_users`              | `user_id`                      |
| `fact_questions`         | `owner_user_id`, `question_id` |
| `fact_answers`           | `owner_user_id`, `question_id` |
| `mart_technology_trends` | `tag_name`, `year`             |

## Prerequisites

- GCP account with billing enabled and a project created
- Service account with `BigQuery Admin` and `Storage Admin` roles, plus a downloaded JSON key
- [Terraform](https://developer.hashicorp.com/terraform/install) ≥ 1.5
- [Docker & Docker Compose](https://docs.docker.com/get-docker/)

> dbt runs inside the Airflow container — no local dbt installation required.

## Setup

### 1. Clone and configure

```bash
git clone <repo-url>
cd stackoverflow-analytics-warehouse
cp .env.example .env
```

Edit `.env` with your GCP values. Place your service-account JSON at `credentials/terraform-runner-service-account.json` — this directory is gitignored.

### 2. Provision GCP infrastructure

```bash
cd terraform
terraform init
terraform apply \
  -var "project_id=$GCP_PROJECT_ID" \
  -var "region=$GCP_REGION" \
  -var "bucket_name=$GCS_RAW_BUCKET"
cd ..
```

Creates one GCS bucket and three BigQuery datasets: `stackoverflow_raw`, `stackoverflow_trusted`, `stackoverflow_marts`.

### 3. Configure the dbt profile

```bash
cp dbt/profiles.example.yml dbt/profiles.yml
```

The profile is env-var driven — no manual edits needed if `.env` is populated.

### 4. Start the Airflow stack

```bash
docker compose up -d
```

Airflow UI is at `http://localhost:8080`. Sign in with the credentials set in `_AIRFLOW_WWW_USER_USERNAME` (see Airflow logs for the auto-generated password on first start).

### 5. Run the pipeline

Unpause the `stackoverflow` DAG in the Airflow UI and trigger it manually. Three task groups run in sequence:

1. **`extract_to_gcs`** — exports each source table to GCS as Parquet. `posts_questions` and `posts_answers` are mapped per year (2008–2022); `users` and `tags` are full snapshots.
2. **`load_to_bigquery`** — loads all Parquet files into `stackoverflow_raw` (`WRITE_TRUNCATE`).
3. **`transform`** — four sequential dbt tasks:
   - `dbt_deps` — installs dbt packages
   - `dbt_run_staging` — builds partitioned/clustered tables in `stackoverflow_trusted`
   - `dbt_run_intermediate` — builds bridge and acceptance views in `stackoverflow_trusted`
   - `dbt_run_mart` — builds the dimensional model in `stackoverflow_marts`

## Environment variables

| Variable                         | Description                                                                                                                        |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `GCP_PROJECT_ID`                 | GCP project ID                                                                                                                     |
| `GCP_REGION`                     | GCP region (e.g. `us-central1`) — must match BigQuery dataset location                                                             |
| `GCS_RAW_BUCKET`                 | Parquet staging bucket name                                                                                                        |
| `BQ_RAW_DATASET`                 | Raw dataset name (default: `stackoverflow_raw`)                                                                                    |
| `BQ_TRUSTED_DATASET`             | Trusted dataset name (default: `stackoverflow_trusted`)                                                                            |
| `BQ_MARTS_DATASET`               | Marts dataset name (default: `stackoverflow_marts`)                                                                                |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path inside the container to the service-account JSON                                                                              |
| `AIRFLOW_UID`                    | Host user ID (`id -u`) — prevents root-owned files in mounted volumes                                                              |
| `CREDENTIALS_PATH`               | Host path to the `credentials/` directory                                                                                          |
| `FERNET_KEY`                     | Airflow encryption key — generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `_AIRFLOW_WWW_USER_USERNAME`     | Airflow UI admin username                                                                                                          |
