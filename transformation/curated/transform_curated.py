"""
Transformation Staging → Curated.

Pour chaque ticker :
1. Lit les données depuis staging_ohlcv (PostgreSQL)
2. Détecte les anomalies via Isolation Forest (sklearn) sur 4 features :
   daily_return, volatility_20, volume_zscore, rsi_14
3. Classifie le type d'anomalie : flash_crash, volume_spike, price_spike, high_volatility
4. Calcule la tendance de prix et le signal de trading simplifié
5. Écrit dans curated_analysis (PostgreSQL)
"""
import logging
import sys
import os

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import POSTGRES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Contamination Isolation Forest : ~5% des points considérés comme anomalies
CONTAMINATION = 0.05
MIN_ROWS_FOR_MODEL = 30  # minimum de lignes pour entraîner le modèle


def get_pg_conn():
    return psycopg2.connect(**POSTGRES)


# ── Chargement depuis Staging ──────────────────────────────────────────────

def load_from_staging(conn, ticker: str) -> pd.DataFrame:
    """Charge les données de staging_ohlcv pour un ticker."""
    sql = """
        SELECT ticker, date, close, volume, daily_return, volatility_20,
               rsi_14, macd, macd_signal, sma_20, sma_50
        FROM staging_ohlcv
        WHERE ticker = %s
        ORDER BY date ASC
    """
    df = pd.read_sql(sql, conn, params=(ticker,))
    if df.empty:
        raise ValueError(f"Aucune donnée staging pour {ticker}")
    log.info("Staging → %d lignes chargées pour %s", len(df), ticker)
    return df


# ── Détection d'anomalies ──────────────────────────────────────────────────

def compute_volume_zscore(volume: pd.Series) -> pd.Series:
    """Z-score glissant du volume (fenêtre 20 jours)."""
    rolling_mean = volume.rolling(window=20, min_periods=5).mean()
    rolling_std  = volume.rolling(window=20, min_periods=5).std()
    z = (volume - rolling_mean) / rolling_std.replace(0, np.nan)
    return z.fillna(0)


def run_isolation_forest(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique l'Isolation Forest sur les features numériques.
    Retourne le DataFrame enrichi de anomaly_score et is_anomaly.
    """
    df = df.copy()
    df["volume_zscore"] = compute_volume_zscore(df["volume"].astype(float))

    features = ["daily_return", "volatility_20", "volume_zscore", "rsi_14"]
    feature_df = df[features].copy()

    # Remplacement des NaN par la médiane de chaque colonne
    for col in features:
        feature_df[col] = feature_df[col].fillna(feature_df[col].median())

    if len(feature_df) < MIN_ROWS_FOR_MODEL:
        log.warning("Pas assez de données pour Isolation Forest (%d lignes), skipping", len(feature_df))
        df["anomaly_score"] = 0.0
        df["is_anomaly"] = False
        return df

    scaler = StandardScaler()
    X = scaler.fit_transform(feature_df)

    model = IsolationForest(
        n_estimators=100,
        contamination=CONTAMINATION,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X)

    # score_samples retourne un score négatif : plus négatif = plus anormal
    df["anomaly_score"] = model.score_samples(X)
    # predict : -1 = anomalie, 1 = normal
    df["is_anomaly"] = model.predict(X) == -1

    anomaly_count = df["is_anomaly"].sum()
    log.info("Isolation Forest : %d anomalies détectées sur %d points (%.1f%%)",
             anomaly_count, len(df), 100 * anomaly_count / len(df))
    return df


def classify_anomaly_type(row: pd.Series) -> str | None:
    """Classifie le type d'anomalie pour les lignes flaggées."""
    if not row.get("is_anomaly"):
        return None

    daily_return   = row.get("daily_return", 0) or 0
    volume_zscore  = row.get("volume_zscore", 0) or 0
    volatility     = row.get("volatility_20", 0) or 0

    if daily_return < -0.05:
        return "flash_crash"
    if daily_return > 0.05:
        return "price_spike"
    if abs(volume_zscore) > 3:
        return "volume_spike"
    if volatility > 0.04:
        return "high_volatility"
    return "unknown_anomaly"


def compute_trend(row: pd.Series) -> str:
    """Détermine la tendance de prix basée sur SMA 20/50."""
    sma20 = row.get("sma_20")
    sma50 = row.get("sma_50")
    close = row.get("close")

    if sma20 is None or sma50 is None or close is None:
        return "neutral"
    if close > sma20 > sma50:
        return "bullish"
    if close < sma20 < sma50:
        return "bearish"
    return "neutral"


def compute_signal(row: pd.Series) -> str:
    """Signal de trading simplifié basé sur RSI + MACD."""
    rsi         = row.get("rsi_14")
    macd        = row.get("macd")
    macd_signal = row.get("macd_signal")

    if rsi is None or macd is None or macd_signal is None:
        return "hold"

    # Survente RSI + croisement MACD haussier → buy
    if rsi < 30 and macd > macd_signal:
        return "buy"
    # Surachat RSI + croisement MACD baissier → sell
    if rsi > 70 and macd < macd_signal:
        return "sell"
    return "hold"


# ── Écriture dans curated ──────────────────────────────────────────────────

def upsert_to_curated(conn, df: pd.DataFrame) -> int:
    """Upsert les données enrichies dans curated_analysis."""
    sql = """
        INSERT INTO curated_analysis (
            ticker, date, close, volume, daily_return, volatility_20,
            rsi_14, anomaly_score, is_anomaly, anomaly_type, price_trend, signal
        ) VALUES (
            %(ticker)s, %(date)s, %(close)s, %(volume)s, %(daily_return)s, %(volatility_20)s,
            %(rsi_14)s, %(anomaly_score)s, %(is_anomaly)s, %(anomaly_type)s, %(price_trend)s, %(signal)s
        )
        ON CONFLICT (ticker, date) DO UPDATE SET
            close         = EXCLUDED.close,
            volume        = EXCLUDED.volume,
            daily_return  = EXCLUDED.daily_return,
            volatility_20 = EXCLUDED.volatility_20,
            rsi_14        = EXCLUDED.rsi_14,
            anomaly_score = EXCLUDED.anomaly_score,
            is_anomaly    = EXCLUDED.is_anomaly,
            anomaly_type  = EXCLUDED.anomaly_type,
            price_trend   = EXCLUDED.price_trend,
            signal        = EXCLUDED.signal,
            processed_at  = NOW()
    """

    def safe(v):
        if v is None:
            return None
        try:
            f = float(v)
            return None if (np.isnan(f) or np.isinf(f)) else f
        except (TypeError, ValueError):
            return None

    records = [
        {
            "ticker":        row["ticker"],
            "date":          row["date"].date() if hasattr(row["date"], "date") else row["date"],
            "close":         safe(row.get("close")),
            "volume":        int(row["volume"]) if row.get("volume") else None,
            "daily_return":  safe(row.get("daily_return")),
            "volatility_20": safe(row.get("volatility_20")),
            "rsi_14":        safe(row.get("rsi_14")),
            "anomaly_score": safe(row.get("anomaly_score")),
            "is_anomaly":    bool(row.get("is_anomaly", False)),
            "anomaly_type":  classify_anomaly_type(row),
            "price_trend":   compute_trend(row),
            "signal":        compute_signal(row),
        }
        for _, row in df.iterrows()
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records, page_size=500)
    conn.commit()
    return len(records)


# ── Point d'entrée ─────────────────────────────────────────────────────────

def run_curated(tickers: list[str]) -> dict:
    """Transforme les données Staging → Curated avec détection d'anomalies."""
    conn = get_pg_conn()
    results = {
        "success":           [],
        "errors":            [],
        "processed":         0,
        "anomalies_detected": 0,
    }

    try:
        for ticker in tickers:
            try:
                log.info("=== Curated : %s ===", ticker)
                df = load_from_staging(conn, ticker)
                df = run_isolation_forest(df)
                count = upsert_to_curated(conn, df)
                anomalies = int(df["is_anomaly"].sum())
                results["success"].append({"ticker": ticker, "rows": count, "anomalies": anomalies})
                results["processed"] += count
                results["anomalies_detected"] += anomalies
                log.info("Curated OK : %s → %d lignes, %d anomalies", ticker, count, anomalies)
            except Exception as exc:
                log.error("Curated ERREUR %s : %s", ticker, exc)
                results["errors"].append({"ticker": ticker, "error": str(exc)})
                conn.rollback()
    finally:
        conn.close()

    log.info("Curated terminé : %d tickers OK, %d anomalies totales",
             len(results["success"]), results["anomalies_detected"])
    return results


if __name__ == "__main__":
    import json
    from config.settings import ALL_TICKERS
    print(json.dumps(run_curated(ALL_TICKERS), indent=2))
