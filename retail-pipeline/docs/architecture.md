# Architecture Overview

## Data Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        SOURCE LAYER                                      │
│  CSV Files / API Endpoints → GCS Bucket (data/sales/, data/products/)   │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │ Airflow GCSToBigQueryOperator
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        RAW LAYER  (retail_raw)                           │
│  raw_sales          raw_products          raw_stores                     │
│  Partitioned by _ingested_at — append-only, no transforms                │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │ Dataform (stg_* models)
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                       STAGING LAYER  (retail_staging)                    │
│  stg_sales          stg_products         stg_stores                      │
│  • Type-cast    • Deduplicated    • Null-filled    • Audit columns        │
│  Incremental on sale_date, partitioned + clustered                       │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │ Dataform (mart models)
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                         MART LAYER  (retail_mart)                        │
│                                                                          │
│         dim_date ──────────────────────────────────┐                    │
│         dim_product ───────── fact_sales ──────────┤                    │
│         dim_store ─────────────────────────────────┘                    │
│                                  │                                       │
│                          rpt_sales_kpis                                  │
│              (pre-aggregated daily KPI rollup)                           │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │
                                  ▼
                    Power BI / Looker Studio Dashboards
```

## Star Schema

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  date_key   │
                    │  year       │
                    │  quarter    │
                    │  month      │
                    │  is_weekend │
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴──────────────┐    ┌─────────────────┐
│ dim_product │    │     fact_sales        │    │   dim_store     │
│ product_key │◄───│  sale_key (PK)        │───►│  store_key      │
│ product_id  │    │  date_key (FK)        │    │  store_id       │
│ product_name│    │  store_key (FK)       │    │  store_name     │
│ category    │    │  product_key (FK)     │    │  region         │
│ subcategory │    │  transaction_id       │    │  state          │
│ brand       │    │  quantity             │    │  city           │
│ cost_price  │    │  unit_price           │    │  open_date      │
│ list_price  │    │  total_amount         │    └─────────────────┘
└─────────────┘    │  gross_profit         │
                   │  discount_pct         │
                   └───────────────────────┘
```

## CI/CD Flow

```
Developer
   │
   │  git push (feature branch)
   ▼
GitHub Pull Request
   │
   ├─► Lint Python (black, isort, flake8)
   ├─► Validate Airflow DAGs (import check)
   ├─► Compile Dataform (dry-run, no GCP creds)
   ├─► Validate JSON schemas
   └─► Security scan (bandit)
           │
           │  All checks pass → PR mergeable
           ▼
   Merge to main
           │
           ▼
GitHub Actions CD Workflow
   │
   ├─► Authenticate to GCP (service account)
   ├─► Create/update BigQuery datasets
   ├─► Deploy raw table schemas (bq update)
   ├─► Compile Dataform → push to GCP repo
   ├─► Run Dataform workflow invocation
   └─► Post-deploy quality checks
           │
           ▼
   GitHub Step Summary — deploy report
```

## Orchestration (Airflow)

```
retail_ingestion_dag (daily 02:00 UTC)
│
├── create_raw_dataset_if_not_exists
│
├── load_raw_sales        ──┐
├── load_raw_products       ├── (parallel)
└── load_raw_stores       ──┘
         │
         ▼
    validate_raw_row_counts
         │
         ▼
    compile_dataform_models
         │
         ▼
    run_dataform_workflow
         │
         ▼  (triggers separately)
retail_data_quality_dag
         │
         ├── run_quality_checks (BranchPythonOperator)
         │       ├── quality_passed
         │       └── notify_failure (email alert)
```

## GitHub Secrets Required

| Secret | Used by | Description |
|--------|---------|-------------|
| `GCP_SA_KEY` | CD, Scheduled | Service account JSON (base64 encoded) |
| `GCP_PROJECT_ID_PROD` | CD, Scheduled | Production GCP project ID |
| `GCP_PROJECT_ID_STAGING` | CD | Staging GCP project ID |
| `DATAFORM_REPO` | CD, Scheduled | Dataform repository name |
| `DATAFORM_REGION` | CD, Scheduled | GCP region (e.g. `us-central1`) |
