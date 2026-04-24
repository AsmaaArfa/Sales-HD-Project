"""
Retail Sales Ingestion DAG
--------------------------
Orchestrates daily ingestion of sales, products, and store data
from CSV sources into BigQuery raw layer.

Schedule: Daily at 02:00 UTC
Owner: Data Engineering
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.utils.dates import days_ago

# ── Config ────────────────────────────────────────────────────────────────────
GCP_PROJECT_ID   = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET       = os.environ["GCS_BUCKET"]
BQ_DATASET_RAW   = os.environ.get("BQ_DATASET_RAW", "retail_raw")
DATAFORM_REPO    = os.environ["DATAFORM_REPO"]
DATAFORM_REGION  = os.environ.get("DATAFORM_REGION", "us-central1")
GCP_CONN_ID      = "google_cloud_default"

SCHEMA_DIR = "/opt/airflow/dags/../bigquery/schemas"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="retail_sales_ingestion",
    description="Daily ingestion: CSV → BigQuery raw, then trigger Dataform",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",   # 02:00 UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "ingestion", "bigquery"],
) as dag:

    # ── 1. Ensure raw dataset exists ──────────────────────────────────────────
    create_raw_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_raw_dataset_if_not_exists",
        dataset_id=BQ_DATASET_RAW,
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
        exists_ok=True,
    )

    # ── 2. Load sales transactions ────────────────────────────────────────────
    load_sales = GCSToBigQueryOperator(
        task_id="load_raw_sales",
        bucket=GCS_BUCKET,
        source_objects=["data/sales/{{ ds_nodash }}/sales_*.csv"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.raw_sales",
        schema_fields=None,
        schema_object=f"schemas/raw_sales.json",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        allow_quoted_newlines=True,
        max_bad_records=10,
        gcp_conn_id=GCP_CONN_ID,
    )

    # ── 3. Load products (full refresh) ──────────────────────────────────────
    load_products = GCSToBigQueryOperator(
        task_id="load_raw_products",
        bucket=GCS_BUCKET,
        source_objects=["data/products/products_latest.csv"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.raw_products",
        schema_object=f"schemas/raw_products.json",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id=GCP_CONN_ID,
    )

    # ── 4. Load stores (full refresh) ────────────────────────────────────────
    load_stores = GCSToBigQueryOperator(
        task_id="load_raw_stores",
        bucket=GCS_BUCKET,
        source_objects=["data/stores/stores_latest.csv"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.raw_stores",
        schema_object=f"schemas/raw_stores.json",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id=GCP_CONN_ID,
    )

    # ── 5. Row-count validation after ingestion ───────────────────────────────
    validate_ingestion = BigQueryInsertJobOperator(
        task_id="validate_raw_row_counts",
        configuration={
            "query": {
                "query": f"""
                    SELECT
                        'raw_sales'    AS table_name,
                        COUNT(*)       AS row_count,
                        DATE(_ingested_at) AS ingested_date
                    FROM `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.raw_sales`
                    WHERE DATE(_ingested_at) = '{{{{ ds }}}}'

                    UNION ALL

                    SELECT 'raw_products', COUNT(*), CURRENT_DATE()
                    FROM `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.raw_products`

                    UNION ALL

                    SELECT 'raw_stores', COUNT(*), CURRENT_DATE()
                    FROM `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.raw_stores`
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # ── 6. Compile Dataform models ────────────────────────────────────────────
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform_models",
        project_id=GCP_PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPO,
        compilation_result={
            "git_commitish": "main",
            "code_compilation_config": {
                "default_database": GCP_PROJECT_ID,
                "default_schema": "retail_staging",
                "vars": {"execution_date": "{{ ds }}"},
            },
        },
    )

    # ── 7. Run Dataform workflow ──────────────────────────────────────────────
    run_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform_workflow",
        project_id=GCP_PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPO,
        asynchronous=False,
        workflow_invocation={
            "compilation_result": (
                "{{ task_instance.xcom_pull("
                "    'compile_dataform_models', "
                "    key='compilation_result_name') }}"
            )
        },
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────
    create_raw_dataset >> [load_sales, load_products, load_stores]
    [load_sales, load_products, load_stores] >> validate_ingestion
    validate_ingestion >> compile_dataform >> run_dataform
