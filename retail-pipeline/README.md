# Retail Sales Analytics Data Pipeline

End-to-end data pipeline for retail sales analytics built on GCP.

## Stack
- **BigQuery** — Data warehouse (raw → staging → mart layers)
- **GCP Dataform** — SQL-based data transformations
- **Apache Airflow** — Orchestration
- **GitHub Actions** — CI/CD

## Architecture

```
CSV/API Sources → Airflow Ingestion DAG → BigQuery Raw
                                              ↓
                                    Dataform Transformations
                                    (Staging → Marts)
                                              ↓
                                    Power BI / Looker Studio
```

## Quick Start

### 1. Prerequisites
- GCP project with BigQuery and Dataform APIs enabled
- Service account with BigQuery Admin + Dataform Editor roles
- Python 3.10+ and Apache Airflow 2.x

### 2. Setup
```bash
git clone <your-repo>
cd retail-pipeline

# Install Airflow
pip install apache-airflow\[google\]==2.9.0

# Configure secrets
cp .env.example .env
# Fill in GCP_PROJECT_ID, BQ_DATASET_RAW, DATAFORM_REPO, etc.
```

### 3. GitHub Secrets Required
| Secret | Description |
|--------|-------------|
| `GCP_SA_KEY` | Service account JSON key (base64) |
| `GCP_PROJECT_ID` | Your GCP project ID |
| `BQ_DATASET_RAW` | BigQuery raw dataset name |
| `DATAFORM_REPO` | Dataform repository name |
| `DATAFORM_REGION` | GCP region (e.g., `us-central1`) |

## Project Structure
```
retail-pipeline/
├── .github/workflows/        # CI/CD pipelines
├── airflow/dags/             # Airflow orchestration DAGs
├── dataform/                 # Dataform transformation models
│   ├── definitions/
│   │   ├── staging/          # Staging layer models
│   │   └── marts/            # Business mart models
│   ├── includes/             # Shared macros/helpers
│   └── tests/                # Data quality assertions
├── bigquery/schemas/         # Table schema definitions
├── scripts/                  # Utility scripts
└── docs/                     # Architecture diagrams
```

## Data Layers
| Layer | Dataset | Purpose |
|-------|---------|---------|
| Raw | `retail_raw` | Landing zone — exact source data |
| Staging | `retail_staging` | Cleaned, typed, deduplicated |
| Mart | `retail_mart` | Star schema, BI-ready |

## KPIs Tracked
- Total Revenue & Sales Volume
- Product Performance (top SKUs)
- Regional Sales Metrics
- Daily/Weekly/Monthly trends
