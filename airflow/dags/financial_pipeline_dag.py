"""
DAG principal du data lake financier.

Orchestration complète : Raw → Staging → Curated
Scheduling : quotidien à 6h UTC (après ouverture des marchés européens)

Flux :
  [ingest_file] ──┐
                  ├──▶ [transform_staging] ──▶ [transform_curated] ──▶ [notify_done]
  [ingest_api]  ──┘
"""
import sys
import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# Montage des modules du projet dans le PYTHONPATH Airflow
sys.path.insert(0, "/opt/airflow")

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":            "data-engineer",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}


# ── Callables pour les PythonOperators ────────────────────────────────────

def task_ingest_file(**context) -> dict:
    """Ingère le dataset fichier complet (tickers S&P500 + indices)."""
    from ingestion.ingest_file import ingest_file_source
    results = ingest_file_source()
    # Push le résumé en XCom pour les tâches aval
    context["ti"].xcom_push(key="ingest_file_results", value=results)
    if results["errors"]:
        log.warning("Ingestion fichier : %d erreurs", len(results["errors"]))
    if not results["success"]:
        raise RuntimeError("Ingestion fichier sans aucun succes")
    return results


def task_ingest_api(**context) -> dict:
    """Ingère les dernières données depuis l'API Yahoo Finance."""
    from ingestion.ingest_api import ingest_api_source
    results = ingest_api_source(days_back=2)
    context["ti"].xcom_push(key="ingest_api_results", value=results)
    if results["errors"]:
        log.warning("Ingestion API : %d erreurs", len(results["errors"]))
    if not results["success"]:
        raise RuntimeError("Ingestion API sans aucun succes")
    return results


def task_transform_staging(**context) -> dict:
    """
    Transforme les données Raw vers la zone Staging.
    Récupère la liste des tickers depuis les XComs des tâches d'ingestion.
    """
    from transformation.staging.transform_staging import run_staging

    # Lecture des tickers ingérés avec succès (XCom)
    ti = context["ti"]
    file_results = ti.xcom_pull(task_ids="ingest_file", key="ingest_file_results") or {}
    api_results  = ti.xcom_pull(task_ids="ingest_api",  key="ingest_api_results")  or {}

    tickers = list({
        r["ticker"]
        for r in file_results.get("success", []) + api_results.get("success", [])
    })

    if not tickers:
        log.warning("Aucun ticker disponible pour le staging")
        return {"processed": 0}

    results = run_staging(tickers)
    context["ti"].xcom_push(key="staging_results", value=results)
    return results


def task_transform_curated(**context) -> dict:
    """Transforme les données Staging vers la zone Curated avec détection d'anomalies."""
    from transformation.curated.transform_curated import run_curated

    ti = context["ti"]
    staging_results = ti.xcom_pull(task_ids="transform_staging", key="staging_results") or {}
    tickers = [r["ticker"] for r in staging_results.get("success", [])]

    if not tickers:
        log.warning("Aucun ticker disponible pour le curated")
        return {"processed": 0}

    results = run_curated(tickers)
    context["ti"].xcom_push(key="curated_results", value=results)
    return results


def task_log_pipeline_summary(**context) -> None:
    """Log un résumé complet du pipeline dans les logs Airflow."""
    ti = context["ti"]
    file_res   = ti.xcom_pull(task_ids="ingest_file",        key="ingest_file_results") or {}
    api_res    = ti.xcom_pull(task_ids="ingest_api",         key="ingest_api_results")  or {}
    stag_res   = ti.xcom_pull(task_ids="transform_staging",  key="staging_results")     or {}
    curat_res  = ti.xcom_pull(task_ids="transform_curated",  key="curated_results")     or {}

    summary = {
        "logical_date":        str(context["logical_date"]),
        "ingestion_file":      {"success": len(file_res.get("success", [])), "errors": len(file_res.get("errors", []))},
        "ingestion_api":       {"success": len(api_res.get("success",  [])), "errors": len(api_res.get("errors",  []))},
        "staging_processed":   stag_res.get("processed", 0),
        "curated_processed":   curat_res.get("processed", 0),
        "anomalies_detected":  curat_res.get("anomalies_detected", 0),
    }
    log.info("=== PIPELINE SUMMARY ===\n%s", json.dumps(summary, indent=2))


# ── Définition du DAG ─────────────────────────────────────────────────────

with DAG(
    dag_id="financial_data_lake_pipeline",
    description="Pipeline complet Raw→Staging→Curated pour le data lake financier",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * 1-5",  # lun-ven à 6h UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["finance", "data-lake", "yfinance"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_file = PythonOperator(
        task_id="ingest_file",
        python_callable=task_ingest_file,
    )

    ingest_api = PythonOperator(
        task_id="ingest_api",
        python_callable=task_ingest_api,
    )

    transform_staging = PythonOperator(
        task_id="transform_staging",
        python_callable=task_transform_staging,
    )

    transform_curated = PythonOperator(
        task_id="transform_curated",
        python_callable=task_transform_curated,
    )

    log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=task_log_pipeline_summary,
    )

    end = EmptyOperator(task_id="end")

    # Dépendances : ingestion parallèle → staging → curated → résumé
    start >> [ingest_file, ingest_api] >> transform_staging >> transform_curated >> log_summary >> end
