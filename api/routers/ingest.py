"""
Endpoint POST /ingest : ingestion synchrone avec benchmark.

Accepte un batch de tickers en JSON, exécute le pipeline complet
Raw → Staging → Curated de façon séquentielle et mesure le temps d'exécution.
"""
import logging
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from dependencies import get_pg_conn

log = logging.getLogger(__name__)
router = APIRouter()


class IngestRequest(BaseModel):
    data: dict = Field(
        ...,
        example={
            "tickers": ["AAPL", "MSFT"],
            "period": "6mo",
            "run_staging": True,
            "run_curated": True,
        },
    )


class IngestResponse(BaseModel):
    status:          str
    tickers:         list[str]
    pipeline_steps:  dict
    performance:     dict
    errors:          list[dict]
    timestamp:       str


@router.post("/ingest", response_model=IngestResponse, summary="Ingestion synchrone avec benchmark")
def ingest(body: IngestRequest) -> IngestResponse:
    """
    Lance le pipeline complet d'ingestion de façon **séquentielle** pour un batch de tickers.

    Steps :
    1. Téléchargement yfinance → MinIO + Elasticsearch (zone Raw)
    2. Calcul des indicateurs techniques → PostgreSQL (zone Staging)
    3. Isolation Forest + enrichissement → PostgreSQL (zone Curated)

    Les temps d'exécution sont mesurés pour chaque étape et retournés dans
    `performance` afin de servir de base de comparaison avec `/ingest_fast`.
    """
    tickers     = body.data.get("tickers", [])
    period      = body.data.get("period", "1mo")
    run_staging = body.data.get("run_staging", True)
    run_curated = body.data.get("run_curated", True)

    if not tickers:
        raise HTTPException(status_code=422, detail="Le champ 'tickers' est requis et ne doit pas être vide")
    if len(tickers) > 200:
        raise HTTPException(status_code=422, detail="Maximum 200 tickers par batch")

    tickers = [t.upper() for t in tickers]
    pipeline_steps: dict = {}
    all_errors: list[dict] = []
    t_global_start = time.perf_counter()

    # ── Étape 1 : Ingestion Raw ───────────────────────────────────────────
    t0 = time.perf_counter()
    raw_results = {"success": [], "errors": []}
    try:
        from ingestion.ingest_file import (
            get_minio_client, get_es_client, ensure_es_index,
            fetch_ticker_data, upload_to_minio, index_to_elasticsearch,
        )
        minio_client = get_minio_client()
        es           = get_es_client()
        ensure_es_index(es)

        for ticker in tickers:
            try:
                df          = fetch_ticker_data(ticker, period, "1d")
                object_name = upload_to_minio(minio_client, ticker, df)
                indexed     = index_to_elasticsearch(es, df)
                raw_results["success"].append({"ticker": ticker, "rows": len(df)})
            except Exception as exc:
                err = {"ticker": ticker, "step": "raw", "error": str(exc)}
                raw_results["errors"].append(err)
                all_errors.append(err)
    except Exception as exc:
        all_errors.append({"step": "raw_init", "error": str(exc)})

    raw_duration_ms = int((time.perf_counter() - t0) * 1000)
    pipeline_steps["raw"] = {
        "success": len(raw_results["success"]),
        "errors":  len(raw_results["errors"]),
        "duration_ms": raw_duration_ms,
    }

    # ── Étape 2 : Staging ─────────────────────────────────────────────────
    staging_results: dict = {"success": [], "errors": []}
    if run_staging:
        t0 = time.perf_counter()
        successful_tickers = [r["ticker"] for r in raw_results["success"]]
        if successful_tickers:
            try:
                from transformation.staging.transform_staging import run_staging as do_staging
                staging_results = do_staging(successful_tickers)
                all_errors.extend([{**e, "step": "staging"} for e in staging_results.get("errors", [])])
            except Exception as exc:
                all_errors.append({"step": "staging", "error": str(exc)})
        staging_duration_ms = int((time.perf_counter() - t0) * 1000)
        pipeline_steps["staging"] = {
            "processed":   staging_results.get("processed", 0),
            "errors":      len(staging_results.get("errors", [])),
            "duration_ms": staging_duration_ms,
        }

    # ── Étape 3 : Curated ─────────────────────────────────────────────────
    curated_results: dict = {"success": [], "errors": []}
    if run_curated and run_staging:
        t0 = time.perf_counter()
        staged_tickers = [r["ticker"] for r in staging_results.get("success", [])]
        if staged_tickers:
            try:
                from transformation.curated.transform_curated import run_curated as do_curated
                curated_results = do_curated(staged_tickers)
                all_errors.extend([{**e, "step": "curated"} for e in curated_results.get("errors", [])])
            except Exception as exc:
                all_errors.append({"step": "curated", "error": str(exc)})
        curated_duration_ms = int((time.perf_counter() - t0) * 1000)
        pipeline_steps["curated"] = {
            "processed":          curated_results.get("processed", 0),
            "anomalies_detected": curated_results.get("anomalies_detected", 0),
            "errors":             len(curated_results.get("errors", [])),
            "duration_ms":        curated_duration_ms,
        }

    # ── Log d'ingestion ───────────────────────────────────────────────────
    total_duration_ms = int((time.perf_counter() - t_global_start) * 1000)
    try:
        conn = get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO ingestion_logs (source, records_count, status, duration_ms)
                   VALUES (%s, %s, %s, %s)""",
                ("manual_ingest", len(tickers), "success" if not all_errors else "partial", total_duration_ms),
            )
        conn.commit()
        conn.close()
    except Exception:
        pass

    return IngestResponse(
        status="success" if not all_errors else "partial",
        tickers=tickers,
        pipeline_steps=pipeline_steps,
        performance={
            "total_duration_ms": total_duration_ms,
            "batch_size":        len(tickers),
            "ms_per_ticker":     round(total_duration_ms / len(tickers), 2) if tickers else 0,
        },
        errors=all_errors,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
