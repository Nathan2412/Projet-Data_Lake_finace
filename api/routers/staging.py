"""Endpoint /staging : accès aux données transformées avec indicateurs techniques."""
import logging
from fastapi import APIRouter, Query, HTTPException

from dependencies import get_pg_conn

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/staging", summary="Données staging avec indicateurs techniques")
def get_staging(
    ticker:    str | None = Query(None,  description="Ticker (ex: AAPL, ^GSPC)"),
    from_date: str | None = Query(None,  description="Date début (YYYY-MM-DD)"),
    to_date:   str | None = Query(None,  description="Date fin (YYYY-MM-DD)"),
    limit:     int        = Query(100,   ge=1, le=5000),
    offset:    int        = Query(0,     ge=0),
) -> dict:
    """
    Retourne les données de la zone Staging :
    OHLCV nettoyé + indicateurs techniques (SMA, EMA, RSI, MACD, Bollinger, volatilité).
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

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    try:
        with conn.cursor() as cur:
            # Count total
            cur.execute(f"SELECT COUNT(*) FROM staging_ohlcv {where_sql}", params)
            total = cur.fetchone()[0]

            # Data
            cur.execute(
                f"""
                SELECT ticker, date, open, high, low, close, adj_close, volume,
                       sma_20, sma_50, ema_12, ema_26, rsi_14,
                       macd, macd_signal, bollinger_upper, bollinger_lower,
                       daily_return, volatility_20, ingested_at
                FROM staging_ohlcv
                {where_sql}
                ORDER BY date DESC
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]

            # Conversion des dates pour JSON
            for row in rows:
                if row.get("date"):
                    row["date"] = str(row["date"])
                if row.get("ingested_at"):
                    row["ingested_at"] = str(row["ingested_at"])

    except Exception as exc:
        log.error("Staging query error: %s", exc)
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


@router.get("/staging/tickers", summary="Liste des tickers disponibles en staging")
def get_staging_tickers() -> dict:
    """Retourne la liste des tickers disponibles dans la zone Staging."""
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ticker, COUNT(*) as rows, MIN(date) as first_date, MAX(date) as last_date
                FROM staging_ohlcv
                GROUP BY ticker
                ORDER BY ticker
            """)
            tickers = [
                {"ticker": r[0], "rows": r[1], "from": str(r[2]), "to": str(r[3])}
                for r in cur.fetchall()
            ]
    except Exception as exc:
        conn.close()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()

    return {"tickers": tickers, "count": len(tickers)}
