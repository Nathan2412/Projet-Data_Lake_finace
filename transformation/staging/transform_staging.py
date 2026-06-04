"""
Transformation Raw → Staging.

Pour chaque ticker :
1. Lit les données brutes depuis Elasticsearch (zone Raw)
2. Nettoie et normalise (gestion NaN, types, doublons)
3. Calcule les indicateurs techniques : SMA, EMA, RSI, MACD, Bollinger, volatilité
4. Écrit dans la table staging_ohlcv (PostgreSQL)
"""
import logging
import sys
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from elasticsearch import Elasticsearch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import POSTGRES, ES_URL, ES_INDEX_RAW

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def get_pg_conn():
    return psycopg2.connect(**POSTGRES)


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_URL)


# ── Indicateurs techniques ─────────────────────────────────────────────────

def calc_sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=1).mean()


def calc_ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calc_macd(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    ema12 = calc_ema(series, 12)
    ema26 = calc_ema(series, 26)
    macd  = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal


def calc_bollinger(series: pd.Series, window: int = 20) -> tuple[pd.Series, pd.Series]:
    sma = calc_sma(series, window)
    std = series.rolling(window=window, min_periods=1).std()
    return sma + 2 * std, sma - 2 * std  # upper, lower


def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calcule tous les indicateurs techniques sur la colonne 'close'."""
    close = df["close"].astype(float)

    df["sma_20"]          = calc_sma(close, 20)
    df["sma_50"]          = calc_sma(close, 50)
    df["ema_12"]          = calc_ema(close, 12)
    df["ema_26"]          = calc_ema(close, 26)
    df["rsi_14"]          = calc_rsi(close, 14)
    df["macd"], df["macd_signal"] = calc_macd(close)
    df["bollinger_upper"], df["bollinger_lower"] = calc_bollinger(close, 20)
    df["daily_return"]    = close.pct_change()
    df["volatility_20"]   = df["daily_return"].rolling(window=20, min_periods=1).std()

    return df


# ── Récupération des données Raw depuis ES ─────────────────────────────────

def fetch_raw_from_es(es: Elasticsearch, ticker: str) -> pd.DataFrame:
    """Récupère toutes les données brutes d'un ticker depuis Elasticsearch."""
    query = {
        "query": {"term": {"ticker": ticker}},
        "sort":  [{"date": {"order": "asc"}}],
        "size":  10000,
    }

    resp = es.search(index=ES_INDEX_RAW, body=query)
    hits = resp["hits"]["hits"]

    if not hits:
        raise ValueError(f"Aucune donnée trouvée dans ES pour {ticker}")

    records = [h["_source"] for h in hits]
    df = pd.DataFrame(records)

    required = ["ticker", "date", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes pour {ticker} : {missing}")

    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
    df["adj_close"] = pd.to_numeric(df.get("adj_close", df["close"]), errors="coerce")

    # Suppression des doublons (garder la dernière source)
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    df = df.dropna(subset=["close"])  # close obligatoire

    log.info("ES → %d lignes brutes récupérées pour %s", len(df), ticker)
    return df.reset_index(drop=True)


# ── Écriture dans PostgreSQL ───────────────────────────────────────────────

def upsert_to_staging(conn, df: pd.DataFrame) -> int:
    """Upsert les données dans staging_ohlcv."""
    sql = """
        INSERT INTO staging_ohlcv (
            ticker, date, open, high, low, close, adj_close, volume,
            sma_20, sma_50, ema_12, ema_26, rsi_14, macd, macd_signal,
            bollinger_upper, bollinger_lower, daily_return, volatility_20
        ) VALUES (
            %(ticker)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(adj_close)s, %(volume)s,
            %(sma_20)s, %(sma_50)s, %(ema_12)s, %(ema_26)s, %(rsi_14)s, %(macd)s, %(macd_signal)s,
            %(bollinger_upper)s, %(bollinger_lower)s, %(daily_return)s, %(volatility_20)s
        )
        ON CONFLICT (ticker, date) DO UPDATE SET
            open            = EXCLUDED.open,
            high            = EXCLUDED.high,
            low             = EXCLUDED.low,
            close           = EXCLUDED.close,
            adj_close       = EXCLUDED.adj_close,
            volume          = EXCLUDED.volume,
            sma_20          = EXCLUDED.sma_20,
            sma_50          = EXCLUDED.sma_50,
            ema_12          = EXCLUDED.ema_12,
            ema_26          = EXCLUDED.ema_26,
            rsi_14          = EXCLUDED.rsi_14,
            macd            = EXCLUDED.macd,
            macd_signal     = EXCLUDED.macd_signal,
            bollinger_upper = EXCLUDED.bollinger_upper,
            bollinger_lower = EXCLUDED.bollinger_lower,
            daily_return    = EXCLUDED.daily_return,
            volatility_20   = EXCLUDED.volatility_20,
            ingested_at     = NOW()
    """

    def safe(v):
        """Convertit NaN/Inf en None pour PostgreSQL."""
        if v is None:
            return None
        try:
            f = float(v)
            return None if (np.isnan(f) or np.isinf(f)) else f
        except (TypeError, ValueError):
            return None

    records = [
        {
            "ticker":          row["ticker"],
            "date":            row["date"].date() if hasattr(row["date"], "date") else row["date"],
            "open":            safe(row.get("open")),
            "high":            safe(row.get("high")),
            "low":             safe(row.get("low")),
            "close":           safe(row.get("close")),
            "adj_close":       safe(row.get("adj_close")),
            "volume":          int(row["volume"]) if row.get("volume") else None,
            "sma_20":          safe(row.get("sma_20")),
            "sma_50":          safe(row.get("sma_50")),
            "ema_12":          safe(row.get("ema_12")),
            "ema_26":          safe(row.get("ema_26")),
            "rsi_14":          safe(row.get("rsi_14")),
            "macd":            safe(row.get("macd")),
            "macd_signal":     safe(row.get("macd_signal")),
            "bollinger_upper": safe(row.get("bollinger_upper")),
            "bollinger_lower": safe(row.get("bollinger_lower")),
            "daily_return":    safe(row.get("daily_return")),
            "volatility_20":   safe(row.get("volatility_20")),
        }
        for _, row in df.iterrows()
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records, page_size=500)
    conn.commit()
    return len(records)


# ── Point d'entrée ─────────────────────────────────────────────────────────

def run_staging(tickers: list[str]) -> dict:
    """Transforme et charge les données de la zone Raw vers Staging."""
    es = get_es_client()
    conn = get_pg_conn()
    results = {"success": [], "errors": [], "processed": 0}

    try:
        for ticker in tickers:
            try:
                log.info("=== Staging : %s ===", ticker)
                df = fetch_raw_from_es(es, ticker)
                df = add_technical_indicators(df)
                count = upsert_to_staging(conn, df)
                results["success"].append({"ticker": ticker, "rows": count})
                results["processed"] += count
                log.info("Staging OK : %s → %d lignes", ticker, count)
            except Exception as exc:
                log.error("Staging ERREUR %s : %s", ticker, exc)
                results["errors"].append({"ticker": ticker, "error": str(exc)})
                conn.rollback()
    finally:
        conn.close()

    log.info("Staging terminé : %d tickers OK, %d erreurs",
             len(results["success"]), len(results["errors"]))
    return results


if __name__ == "__main__":
    import json
    from config.settings import ALL_TICKERS
    print(json.dumps(run_staging(ALL_TICKERS), indent=2))
