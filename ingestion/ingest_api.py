"""
Ingestion depuis l'API Yahoo Finance via yfinance (source 2 : API polling).
- Récupère les dernières données du jour pour tous les tickers
- Stocke le payload JSON brut dans MinIO (bucket raw-api-data)
- Indexe dans Elasticsearch avec métadonnées d'ingestion
- Conçu pour être appelé par l'Airflow scheduler quotidiennement
"""
from __future__ import annotations

import io
import json
import logging
import sys
import os
from datetime import datetime, timezone, timedelta

import pandas as pd
import yfinance as yf
from minio import Minio
from elasticsearch import Elasticsearch, helpers

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    MINIO, MINIO_BUCKET_RAW_API,
    ES_URL, ES_INDEX_RAW,
    ALL_TICKERS,
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


def fetch_latest_data(ticker: str, days_back: int = 5) -> dict:
    """
    Récupère les dernières données via l'API yfinance.
    Retourne un payload JSON enrichi de métadonnées.
    """
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)

    stock = yf.Ticker(ticker)
    df = stock.history(start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))

    if df.empty:
        raise ValueError(f"Aucune donnée récente pour {ticker}")

    df.reset_index(inplace=True)
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

    # Récupération des métadonnées du ticker
    info = {}
    try:
        raw_info = stock.info
        info = {
            "shortName":      raw_info.get("shortName"),
            "sector":         raw_info.get("sector"),
            "industry":       raw_info.get("industry"),
            "marketCap":      raw_info.get("marketCap"),
            "currency":       raw_info.get("currency", "USD"),
            "exchange":       raw_info.get("exchange"),
        }
    except Exception:
        pass  # métadonnées optionnelles

    payload = {
        "ticker":       ticker,
        "fetch_date":   end_date.isoformat(),
        "source":       "yfinance_api",
        "metadata":     info,
        "records":      df.rename(columns={
            "Date": "date", "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume", "Dividends": "dividends",
            "Stock Splits": "stock_splits",
        }).to_dict(orient="records"),
    }
    return payload


def upload_payload_to_minio(minio_client: Minio, ticker: str, payload: dict) -> str:
    """Upload le payload JSON brut dans MinIO."""
    json_bytes = json.dumps(payload, default=str).encode("utf-8")
    buffer = io.BytesIO(json_bytes)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    object_name = f"{ticker}/{datetime.now(timezone.utc).strftime('%Y%m%d')}/{ticker}_{timestamp}_api.json"

    minio_client.put_object(
        MINIO_BUCKET_RAW_API,
        object_name,
        data=buffer,
        length=len(json_bytes),
        content_type="application/json",
    )
    log.info("MinIO API : %s/%s uploadé", MINIO_BUCKET_RAW_API, object_name)
    return object_name


def index_api_data_to_es(es: Elasticsearch, payload: dict) -> int:
    """Indexe les records du payload API dans Elasticsearch."""
    ticker = payload["ticker"]
    now_iso = datetime.now(timezone.utc).isoformat()

    actions = [
        {
            "_index": ES_INDEX_RAW,
            "_id":    f"{ticker}_{record['date']}_api",
            "_source": {
                "ticker":      ticker,
                "date":        record["date"],
                "open":        record.get("open"),
                "high":        record.get("high"),
                "low":         record.get("low"),
                "close":       record.get("close"),
                "volume":      record.get("volume"),
                "source":      "yfinance_api",
                "metadata":    payload.get("metadata", {}),
                "ingested_at": now_iso,
            },
        }
        for record in payload.get("records", [])
    ]

    if not actions:
        return 0

    success, _ = helpers.bulk(es, actions, raise_on_error=False)
    log.info("ES API : %d documents indexés pour %s", success, ticker)
    return success


def ingest_api_source(
    tickers: list[str] | None = None,
    days_back: int = 5,
) -> dict:
    """
    Point d'entrée principal pour l'ingestion API.
    Utilisé par le DAG Airflow en scheduling quotidien.
    """
    tickers = tickers or ALL_TICKERS
    minio_client = get_minio_client()
    es = get_es_client()

    results = {"success": [], "errors": []}

    for ticker in tickers:
        try:
            log.info("=== Ingestion API : %s ===", ticker)
            payload = fetch_latest_data(ticker, days_back)
            object_name = upload_payload_to_minio(minio_client, ticker, payload)
            indexed = index_api_data_to_es(es, payload)
            results["success"].append({
                "ticker":      ticker,
                "records":     len(payload.get("records", [])),
                "minio_object": object_name,
                "es_indexed":  indexed,
            })
        except Exception as exc:
            log.error("Échec ingestion API %s : %s", ticker, exc)
            results["errors"].append({"ticker": ticker, "error": str(exc)})

    log.info("Ingestion API terminée : %d succès, %d erreurs",
             len(results["success"]), len(results["errors"]))
    return results


if __name__ == "__main__":
    summary = ingest_api_source()
    print(json.dumps(summary, indent=2, default=str))
