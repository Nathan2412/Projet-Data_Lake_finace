"""Endpoint /stats : métriques de remplissage du data lake."""
import logging
from fastapi import APIRouter, HTTPException

from dependencies import get_pg_conn, get_minio, get_es, BUCKET_RAW_FILE, BUCKET_RAW_API, ES_INDEX

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/stats", summary="Métriques de remplissage du data lake")
def get_stats() -> dict:
    """
    Retourne des métriques sur chaque zone du data lake :
    - Zone Raw : nombre d'objets MinIO + documents Elasticsearch
    - Zone Staging : nombre de lignes dans staging_ohlcv
    - Zone Curated : nombre de lignes dans curated_analysis + anomalies détectées
    - Logs d'ingestion : dernières opérations
    """
    stats: dict = {}

    # ── Zone Raw : MinIO ──────────────────────────────────────────────────
    try:
        minio = get_minio()
        raw_file_count = sum(1 for _ in minio.list_objects(BUCKET_RAW_FILE, recursive=True))
        raw_api_count  = sum(1 for _ in minio.list_objects(BUCKET_RAW_API,  recursive=True))
        stats["raw_minio"] = {
            "bucket_file_objects": raw_file_count,
            "bucket_api_objects":  raw_api_count,
            "total_objects":       raw_file_count + raw_api_count,
        }
    except Exception as exc:
        log.error("Stats MinIO failed: %s", exc)
        stats["raw_minio"] = {"error": str(exc)}

    # ── Zone Raw : Elasticsearch ──────────────────────────────────────────
    try:
        es = get_es()
        if es.indices.exists(index=ES_INDEX):
            count_resp = es.count(index=ES_INDEX)
            # Agrégation par ticker
            agg_resp = es.search(
                index=ES_INDEX,
                body={
                    "size": 0,
                    "aggs": {
                        "by_ticker": {
                            "terms": {"field": "ticker", "size": 50}
                        }
                    },
                },
            )
            ticker_counts = {
                b["key"]: b["doc_count"]
                for b in agg_resp["aggregations"]["by_ticker"]["buckets"]
            }
            stats["raw_elasticsearch"] = {
                "total_documents": count_resp["count"],
                "by_ticker":       ticker_counts,
            }
        else:
            stats["raw_elasticsearch"] = {"total_documents": 0, "note": "Index non créé"}
    except Exception as exc:
        log.error("Stats ES failed: %s", exc)
        stats["raw_elasticsearch"] = {"error": str(exc)}

    # ── Zone Staging + Curated : PostgreSQL ───────────────────────────────
    try:
        conn = get_pg_conn()
        with conn.cursor() as cur:
            # Staging
            cur.execute("SELECT COUNT(*) FROM staging_ohlcv")
            staging_total = cur.fetchone()[0]

            cur.execute("""
                SELECT ticker, COUNT(*) as rows,
                       MIN(date) as first_date, MAX(date) as last_date
                FROM staging_ohlcv
                GROUP BY ticker
                ORDER BY ticker
            """)
            staging_by_ticker = [
                {"ticker": r[0], "rows": r[1], "from": str(r[2]), "to": str(r[3])}
                for r in cur.fetchall()
            ]

            # Curated
            cur.execute("SELECT COUNT(*), SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) FROM curated_analysis")
            row = cur.fetchone()
            curated_total, anomalies_total = row[0], row[1] or 0

            cur.execute("""
                SELECT ticker,
                       COUNT(*) as rows,
                       SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies,
                       MAX(date) as last_date
                FROM curated_analysis
                GROUP BY ticker
                ORDER BY ticker
            """)
            curated_by_ticker = [
                {"ticker": r[0], "rows": r[1], "anomalies": r[2], "last_date": str(r[3])}
                for r in cur.fetchall()
            ]

            # Ingestion logs
            cur.execute("""
                SELECT source, status, COUNT(*), MAX(created_at)
                FROM ingestion_logs
                GROUP BY source, status
                ORDER BY MAX(created_at) DESC
                LIMIT 20
            """)
            log_rows = cur.fetchall()

        conn.close()

        stats["staging"] = {
            "total_rows": staging_total,
            "by_ticker":  staging_by_ticker,
        }
        stats["curated"] = {
            "total_rows":        curated_total,
            "anomalies_detected": anomalies_total,
            "anomaly_rate_pct":  round(100 * anomalies_total / curated_total, 2) if curated_total else 0,
            "by_ticker":         curated_by_ticker,
        }
        stats["ingestion_logs"] = [
            {"source": r[0], "status": r[1], "count": r[2], "last_run": str(r[3])}
            for r in log_rows
        ]

    except Exception as exc:
        log.error("Stats PostgreSQL failed: %s", exc)
        stats["staging"]        = {"error": str(exc)}
        stats["curated"]        = {"error": str(exc)}
        stats["ingestion_logs"] = {"error": str(exc)}

    return stats
