"""
Retail Data Quality DAG
-----------------------
Runs post-transformation data quality assertions against mart tables.
Triggered after the ingestion DAG completes successfully.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCP_CONN_ID    = "google_cloud_default"
ALERT_EMAIL    = os.environ.get("ALERT_EMAIL", "data-team@company.com")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# ── Quality check SQL templates ───────────────────────────────────────────────
CHECKS = {
    "no_null_transaction_ids": f"""
        SELECT COUNT(*) AS failures
        FROM `{GCP_PROJECT_ID}.retail_staging.stg_sales`
        WHERE transaction_id IS NULL
    """,
    "no_negative_revenue": f"""
        SELECT COUNT(*) AS failures
        FROM `{GCP_PROJECT_ID}.retail_staging.stg_sales`
        WHERE total_amount < 0
    """,
    "mart_fact_sales_not_empty": f"""
        SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failures
        FROM `{GCP_PROJECT_ID}.retail_mart.fact_sales`
        WHERE sale_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """,
    "referential_integrity_product": f"""
        SELECT COUNT(*) AS failures
        FROM `{GCP_PROJECT_ID}.retail_mart.fact_sales` f
        LEFT JOIN `{GCP_PROJECT_ID}.retail_mart.dim_product` p
               ON f.product_key = p.product_key
        WHERE p.product_key IS NULL
    """,
}


def run_quality_checks(**context):
    """Execute each check query and raise on failure."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    failures = {}

    for check_name, sql in CHECKS.items():
        result = list(client.query(sql.strip()).result())
        failure_count = result[0]["failures"] if result else 0
        if failure_count > 0:
            failures[check_name] = failure_count

    if failures:
        context["ti"].xcom_push(key="failed_checks", value=failures)
        return "notify_failure"
    return "quality_passed"


with DAG(
    dag_id="retail_data_quality",
    description="Post-load data quality assertions for retail mart",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,   # Triggered by ingestion DAG
    start_date=days_ago(1),
    catchup=False,
    tags=["retail", "quality", "testing"],
) as dag:

    run_checks = BranchPythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
        provide_context=True,
    )

    quality_passed = PythonOperator(
        task_id="quality_passed",
        python_callable=lambda: print("✅ All data quality checks passed."),
    )

    notify_failure = EmailOperator(
        task_id="notify_failure",
        to=ALERT_EMAIL,
        subject="[ALERT] Retail Pipeline — Data Quality Failures",
        html_content="""
        <h3>Data Quality Check Failures</h3>
        <p>One or more data quality checks failed for today's run.</p>
        <p>Failed checks: {{ ti.xcom_pull(key='failed_checks') }}</p>
        <p>Please investigate the retail_staging and retail_mart datasets.</p>
        """,
    )

    run_checks >> [quality_passed, notify_failure]
