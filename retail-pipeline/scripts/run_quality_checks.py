#!/usr/bin/env python3
"""
run_quality_checks.py
---------------------
Runs post-pipeline data quality checks against BigQuery.
Exits with code 1 if any check fails, for use in CI/CD.
"""

import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta

from google.cloud import bigquery

PROJECT = os.environ["GCP_PROJECT_ID"]
YESTERDAY = (date.today() - timedelta(days=1)).isoformat()


print(f"PROJECT: {PROJECT}")

if not PROJECT:
    raise ValueError("❌ GCP_PROJECT_ID is not set in environment variables")

@dataclass
class QualityCheck:
    name: str
    sql: str
    description: str
    severity: str = "ERROR"   # ERROR | WARNING
    threshold: int = 0        # Fail if failures > threshold


CHECKS = [
    QualityCheck(
        name="stg_sales_no_nulls",
        description="No null transaction IDs in staging",
        sql=f"""
            SELECT COUNT(*) AS failures
            FROM `{PROJECT}.retail_staging.stg_sales`
            WHERE transaction_id IS NULL
        """,
    ),
    QualityCheck(
        name="stg_sales_no_negative_revenue",
        description="All sale amounts are non-negative",
        sql=f"""
            SELECT COUNT(*) AS failures
            FROM `{PROJECT}.retail_staging.stg_sales`
            WHERE total_amount < 0
        """,
    ),
    QualityCheck(
        name="fact_sales_loaded_yesterday",
        description="Fact table has rows for yesterday",
        sql=f"""
            SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failures
            FROM `{PROJECT}.retail_mart.fact_sales`
            WHERE sale_date = '{YESTERDAY}'
        """,
    ),
    QualityCheck(
        name="dim_product_no_orphans",
        description="All products in fact table exist in dim_product",
        sql=f"""
            SELECT COUNT(*) AS failures
            FROM `{PROJECT}.retail_mart.fact_sales` f
            LEFT JOIN `{PROJECT}.retail_mart.dim_product` p
                   ON f.product_key = p.product_key
            WHERE p.product_key IS NULL
              AND f.sale_date = '{YESTERDAY}'
        """,
    ),
    QualityCheck(
        name="dim_store_no_orphans",
        description="All stores in fact table exist in dim_store",
        sql=f"""
            SELECT COUNT(*) AS failures
            FROM `{PROJECT}.retail_mart.fact_sales` f
            LEFT JOIN `{PROJECT}.retail_mart.dim_store` s
                   ON f.store_key = s.store_key
            WHERE s.store_key IS NULL
              AND f.sale_date = '{YESTERDAY}'
        """,
    ),
    QualityCheck(
        name="rpt_kpis_revenue_positive",
        description="KPI report has positive total revenue for yesterday",
        sql=f"""
            SELECT CASE WHEN SUM(total_revenue) <= 0 THEN 1 ELSE 0 END AS failures
            FROM `{PROJECT}.retail_mart.rpt_sales_kpis`
            WHERE sale_date = '{YESTERDAY}'
        """,
        severity="WARNING",
    ),
]


def run_checks(client: bigquery.Client) -> tuple[list, list]:
    errors = []
    warnings = []

    for check in CHECKS:
        print(f"  Running: {check.name} ... ", end="", flush=True)
        rows = list(client.query(check.sql.strip()).result())
        failure_count = rows[0]["failures"] if rows else 0

        if failure_count > check.threshold:
            msg = f"{check.name}: {failure_count} failures — {check.description}"
            if check.severity == "ERROR":
                print(f"❌ FAIL ({failure_count} failures)")
                errors.append(msg)
            else:
                print(f"⚠️  WARN ({failure_count} failures)")
                warnings.append(msg)
        else:
            print("✅ PASS")

    return errors, warnings


def main():
    print(f"🔍 Running data quality checks (execution date: {YESTERDAY})")
    print(f"   Project: {PROJECT}\n")

    client = bigquery.Client(project=PROJECT)
    errors, warnings = run_checks(client)

    print()
    if warnings:
        print(f"⚠️  {len(warnings)} warning(s):")
        for w in warnings:
            print(f"   - {w}")

    if errors:
        print(f"\n❌ {len(errors)} error(s):")
        for e in errors:
            print(f"   - {e}")
        print("\nPipeline quality gate FAILED.")
        sys.exit(1)

    print(f"✅ All {len(CHECKS)} quality checks passed.")


if __name__ == "__main__":
    main()
