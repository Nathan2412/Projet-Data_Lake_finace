"""Endpoint /curated : accès aux données enrichies avec scores d'anomalie."""
import logging
from fastapi import APIRouter, Query, HTTPException

from dependencies import get_pg_conn

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/curated", summary="Données curated avec scores d'anomalie")
def get_curated(
    ticker:       str | None = Query(None,  description="Ticker (ex: AAPL, ^GSPC)"),
    from_date:    str | None = Query(None,  description="Date début (YYYY-MM-DD)"),
    to_date:      str | None = Query(None,  description="Date fin (YYYY-MM-DD)"),
    anomalies_only: bool     = Query(False, description="Retourner uniquement les anomalies"),
    signal:       str | None = Query(None,  description="Filtrer par signal : buy, sell, hold"),
    limit:        int        = Query(100,   ge=1, le=5000),
    offset:       int        = Query(0,     ge=0),
) -> dict:
    """
    Retourne les données de la zone Curated :
    données enrichies avec score d'anomalie Isolation Forest,
    type d'anomalie, tendance de prix et signal de trading.
    """
    conn = get_pg_conn()

    where_clauses = []
    params: list = []

    if ticker:
        where_clauses.append("ticker = %s")
        params.append(ticker.upper())
    if from_date:
        where_clauses.append("date >= %s")
        params.append(from_date)
    if to_date:
        where_clauses.append("date <= %s")
        params.append(to_date)
    if anomalies_only:
        where_clauses.append("is_anomaly = TRUE")
    if signal:
        where_clauses.append("signal = %s")
        params.append(signal.lower())

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM curated_analysis {where_sql}", params)
            total = cur.fetchone()[0]

            cur.execute(
                f"""
                SELECT ticker, date, close, volume, daily_return, volatility_20,
                       rsi_14, anomaly_score, is_anomaly, anomaly_type,
                       price_trend, signal, processed_at
                FROM curated_analysis
                {where_sql}
                ORDER BY date DESC
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]

            for row in rows:
                if row.get("date"):
                    row["date"] = str(row["date"])
                if row.get("processed_at"):
                    row["processed_at"] = str(row["processed_at"])

    except Exception as exc:
        log.error("Curated query error: %s", exc)
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {exc}")
    finally:
        conn.close()

    return {
        "total":    total,
        "returned": len(rows),
        "offset":   offset,
        "limit":    limit,
        "data":     rows,
    }


@router.get("/curated/anomalies/summary", summary="Résumé des anomalies par ticker")
def get_anomalies_summary() -> dict:
    """Retourne un résumé des anomalies détectées par ticker et par type."""
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH totals AS (
                    SELECT ticker,
                           COUNT(*) FILTER (WHERE is_anomaly) AS anomaly_count,
                           COUNT(*) AS total_rows,
                           MAX(date) FILTER (WHERE is_anomaly) AS last_anomaly_date
                    FROM curated_analysis
                    GROUP BY ticker
                ), breakdown AS (
                    SELECT ticker, anomaly_type, COUNT(*) AS count
                    FROM curated_analysis
                    WHERE is_anomaly AND anomaly_type IS NOT NULL
                    GROUP BY ticker, anomaly_type
                ), breakdown_json AS (
                    SELECT ticker, json_object_agg(anomaly_type, count) AS anomaly_breakdown
                    FROM breakdown
                    GROUP BY ticker
                )
                SELECT t.ticker, t.anomaly_count, t.total_rows,
                       ROUND(100.0 * t.anomaly_count / NULLIF(t.total_rows, 0), 2) AS anomaly_rate_pct,
                       t.last_anomaly_date,
                       COALESCE(b.anomaly_breakdown, '{}'::json) AS anomaly_breakdown
                FROM totals t
                LEFT JOIN breakdown_json b USING (ticker)
                ORDER BY t.anomaly_count DESC
            """)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            for row in rows:
                if row.get("last_anomaly_date"):
                    row["last_anomaly_date"] = str(row["last_anomaly_date"])
    except Exception as exc:
        conn.close()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()

    return {"summary": rows}


@router.get("/curated/signals", summary="Tickers avec signaux actifs (buy/sell)")
def get_active_signals() -> dict:
    """Retourne les tickers ayant un signal buy ou sell sur la dernière date disponible."""
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ticker, date, close, rsi_14, signal, price_trend, is_anomaly
                FROM (
                    SELECT DISTINCT ON (ticker)
                           ticker, date, close, rsi_14, signal, price_trend, is_anomaly
                    FROM curated_analysis
                    ORDER BY ticker, date DESC
                ) latest
                WHERE signal IN ('buy', 'sell')
                ORDER BY ticker
            """)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            for row in rows:
                if row.get("date"):
                    row["date"] = str(row["date"])
    except Exception as exc:
        conn.close()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()

    return {"signals": rows, "count": len(rows)}
