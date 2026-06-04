"""
Ingestion depuis un dataset fichier yfinance (source 1 : dataset fichier).
- Télécharge l'historique complet de tous les tickers configurés
- Stocke les CSV bruts dans MinIO (bucket raw-financial-data)
- Indexe chaque ligne dans Elasticsearch pour la recherche
"""
import io
import json
import logging
import sys
import os
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
from minio import Minio
from minio.error import S3Error
from elasticsearch import Elasticsearch, helpers

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    MINIO, MINIO_BUCKET_RAW_FILE,
    ES_URL, ES_INDEX_RAW,
    ALL_TICKERS, DEFAULT_PERIOD, DEFAULT_INTERVAL,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def get_minio_client() -> Minio:
    return Minio(
        MINIO["endpoint"],
        access_key=MINIO["access_key"],
        secret_key=MINIO["secret_key"],
        secure=MINIO["secure"],
    )


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_URL)


def ensure_es_index(es: Elasticsearch) -> None:
    """Crée l'index Elasticsearch s'il n'existe pas encore."""
    if not es.indices.exists(index=ES_INDEX_RAW):
        es.indices.create(
            index=ES_INDEX_RAW,
            body={
                "mappings": {
                    "properties": {
                        "ticker":    {"type": "keyword"},
                        "date":      {"type": "date", "format": "yyyy-MM-dd"},
                        "open":      {"type": "float"},
                        "high":      {"type": "float"},
                        "low":       {"type": "float"},
                        "close":     {"type": "float"},
                        "adj_close": {"type": "float"},
                        "volume":    {"type": "long"},
                        "source":    {"type": "keyword"},
                        "ingested_at": {"type": "date"},
                    }
                }
            },
        )
        log.info("Index ES '%s' créé.", ES_INDEX_RAW)


def fetch_ticker_data(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """Télécharge les données OHLCV via yfinance."""
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=False)
        if df.empty:
            raise ValueError(f"Pas de données retournées pour {ticker}")
        df.reset_index(inplace=True)
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        df.rename(columns={
            "Date": "date", "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Adj Close": "adj_close", "Volume": "volume",
        }, inplace=True)
        df["ticker"] = ticker
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        return df
    except Exception as exc:
        log.error("Erreur lors du téléchargement de %s : %s", ticker, exc)
        raise


def upload_to_minio(minio_client: Minio, ticker: str, df: pd.DataFrame) -> str:
    """Sérialise le DataFrame en CSV et l'upload dans MinIO."""
    csv_buffer = io.BytesIO(df.to_csv(index=False).encode("utf-8"))
    object_name = f"{ticker}/{datetime.now(timezone.utc).strftime('%Y%m%d')}/{ticker}_ohlcv.csv"

    minio_client.put_object(
        MINIO_BUCKET_RAW_FILE,
        object_name,
        data=csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="text/csv",
    )
    log.info("MinIO : %s/%s uploadé (%d lignes)", MINIO_BUCKET_RAW_FILE, object_name, len(df))
    return object_name


def index_to_elasticsearch(es: Elasticsearch, df: pd.DataFrame, source: str = "yfinance_file") -> int:
    """Indexe le DataFrame en bulk dans Elasticsearch."""
    now_iso = datetime.now(timezone.utc).isoformat()

    actions = [
        {
            "_index": ES_INDEX_RAW,
            "_id":    f"{row['ticker']}_{row['date']}",
            "_source": {
                "ticker":       row["ticker"],
                "date":         row["date"],
                "open":         float(row["open"])      if pd.notna(row.get("open"))      else None,
                "high":         float(row["high"])      if pd.notna(row.get("high"))      else None,
                "low":          float(row["low"])       if pd.notna(row.get("low"))       else None,
                "close":        float(row["close"])     if pd.notna(row.get("close"))     else None,
                "adj_close":    float(row["adj_close"]) if pd.notna(row.get("adj_close")) else None,
                "volume":       int(row["volume"])      if pd.notna(row.get("volume"))    else None,
                "source":       source,
                "ingested_at":  now_iso,
            },
        }
        for _, row in df.iterrows()
    ]

    success, errors = helpers.bulk(es, actions, raise_on_error=False)
    if errors:
        log.warning("%d erreurs lors de l'indexation ES pour %s", len(errors), df["ticker"].iloc[0])
    log.info("ES : %d documents indexés pour %s", success, df["ticker"].iloc[0])
    return success


def ingest_file_source(
    tickers: list[str] | None = None,
    period: str = DEFAULT_PERIOD,
    interval: str = DEFAULT_INTERVAL,
) -> dict:
    """
    Point d'entrée principal pour l'ingestion fichier.
    Retourne un résumé des opérations effectuées.
    """
    tickers = tickers or ALL_TICKERS
    minio_client = get_minio_client()
    es = get_es_client()
    ensure_es_index(es)

    results = {"success": [], "errors": []}

    for ticker in tickers:
        try:
            log.info("=== Ingestion fichier : %s ===", ticker)
            df = fetch_ticker_data(ticker, period, interval)
            object_name = upload_to_minio(minio_client, ticker, df)
            indexed = index_to_elasticsearch(es, df)
            results["success"].append({
                "ticker": ticker,
                "rows": len(df),
                "minio_object": object_name,
                "es_indexed": indexed,
            })
        except Exception as exc:
            log.error("Échec ingestion %s : %s", ticker, exc)
            results["errors"].append({"ticker": ticker, "error": str(exc)})

    log.info("Ingestion fichier terminée : %d succès, %d erreurs",
             len(results["success"]), len(results["errors"]))
    return results


if __name__ == "__main__":
    summary = ingest_file_source()
    print(json.dumps(summary, indent=2))
