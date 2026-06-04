-- Base de données pour le data lake financier
-- Créée automatiquement au démarrage de PostgreSQL

-- Schéma staging : données nettoyées avec indicateurs techniques
CREATE TABLE IF NOT EXISTS staging_ohlcv (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(20)     NOT NULL,
    date            DATE            NOT NULL,
    open            NUMERIC(18, 6),
    high            NUMERIC(18, 6),
    low             NUMERIC(18, 6),
    close           NUMERIC(18, 6),
    adj_close       NUMERIC(18, 6),
    volume          BIGINT,
    sma_20          NUMERIC(18, 6),
    sma_50          NUMERIC(18, 6),
    ema_12          NUMERIC(18, 6),
    ema_26          NUMERIC(18, 6),
    rsi_14          NUMERIC(8, 4),
    macd            NUMERIC(18, 6),
    macd_signal     NUMERIC(18, 6),
    bollinger_upper NUMERIC(18, 6),
    bollinger_lower NUMERIC(18, 6),
    daily_return    NUMERIC(10, 6),
    volatility_20   NUMERIC(10, 6),
    ingested_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE(ticker, date)
);

-- Schéma curated : données enrichies avec scores d'anomalie
CREATE TABLE IF NOT EXISTS curated_analysis (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(20)     NOT NULL,
    date            DATE            NOT NULL,
    close           NUMERIC(18, 6),
    volume          BIGINT,
    daily_return    NUMERIC(10, 6),
    volatility_20   NUMERIC(10, 6),
    rsi_14          NUMERIC(8, 4),
    anomaly_score   NUMERIC(10, 6),   -- score Isolation Forest (négatif = anomalie)
    is_anomaly      BOOLEAN,
    anomaly_type    VARCHAR(50),       -- 'price_spike', 'volume_spike', 'flash_crash', etc.
    price_trend     VARCHAR(20),       -- 'bullish', 'bearish', 'neutral'
    signal          VARCHAR(20),       -- 'buy', 'sell', 'hold'
    processed_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE(ticker, date)
);

-- Logs d'ingestion pour /stats
CREATE TABLE IF NOT EXISTS ingestion_logs (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50)     NOT NULL,   -- 'yfinance_file', 'yfinance_api', 'manual'
    ticker          VARCHAR(20),
    records_count   INT,
    status          VARCHAR(20)     NOT NULL,   -- 'success', 'error', 'partial'
    error_message   TEXT,
    duration_ms     INT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Index pour les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_staging_ticker_date ON staging_ohlcv(ticker, date DESC);
CREATE INDEX IF NOT EXISTS idx_curated_ticker_date ON curated_analysis(ticker, date DESC);
CREATE INDEX IF NOT EXISTS idx_curated_anomaly ON curated_analysis(is_anomaly, date DESC);
CREATE INDEX IF NOT EXISTS idx_logs_created ON ingestion_logs(created_at DESC);
