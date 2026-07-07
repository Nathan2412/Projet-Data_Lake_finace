import os
import tempfile
import unittest

import numpy as np
import pandas as pd

from ingestion.ingest_file import load_file_dataset
from transformation.staging.transform_staging import add_technical_indicators, calc_rsi
from transformation.curated.transform_curated import classify_anomaly_type
from routers.ingest_fast import compute_indicators_vectorized


class FinancialIndicatorTests(unittest.TestCase):
    def make_frame(self) -> pd.DataFrame:
        close = np.linspace(100.0, 160.0, 80) + np.sin(np.arange(80))
        return pd.DataFrame(
            {
                "ticker": "TEST",
                "date": pd.date_range("2024-01-01", periods=80),
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "adj_close": close,
                "volume": np.arange(80) + 1_000,
            }
        )

    def test_fast_indicators_match_standard_pipeline(self):
        standard = add_technical_indicators(self.make_frame().copy())
        fast = compute_indicators_vectorized(self.make_frame().copy())
        for column in (
            "sma_20",
            "sma_50",
            "ema_12",
            "ema_26",
            "rsi_14",
            "macd",
            "macd_signal",
            "bollinger_upper",
            "bollinger_lower",
            "daily_return",
            "volatility_20",
        ):
            np.testing.assert_allclose(
                standard[column].to_numpy(),
                fast[column].to_numpy(),
                rtol=1e-10,
                atol=1e-10,
                equal_nan=True,
                err_msg=column,
            )

    def test_daily_return_uses_previous_close(self):
        frame = self.make_frame().iloc[:3].copy()
        frame["close"] = [100.0, 110.0, 121.0]
        result = compute_indicators_vectorized(frame)
        self.assertAlmostEqual(result.loc[1, "daily_return"], 0.10)
        self.assertAlmostEqual(result.loc[2, "daily_return"], 0.10)

    def test_rsi_handles_monotonic_and_flat_series(self):
        rising = calc_rsi(pd.Series(np.arange(1.0, 40.0)))
        flat = calc_rsi(pd.Series(np.ones(40)))
        self.assertEqual(rising.iloc[-1], 100.0)
        self.assertEqual(flat.iloc[-1], 50.0)


class FileDatasetTests(unittest.TestCase):
    def test_load_file_dataset_normalizes_and_deduplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "finance.csv")
            pd.DataFrame(
                [
                    {"Ticker": "aapl", "Date": "2024-01-02", "Open": 1, "High": 2, "Low": 1, "Close": 2, "Volume": 10},
                    {"Ticker": "AAPL", "Date": "2024-01-02", "Open": 2, "High": 3, "Low": 2, "Close": 3, "Volume": 20},
                ]
            ).to_csv(path, index=False)
            result = load_file_dataset(path)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.loc[0, "ticker"], "AAPL")
        self.assertEqual(result.loc[0, "adj_close"], 3)

    def test_load_file_dataset_rejects_missing_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "invalid.csv")
            pd.DataFrame([{"ticker": "AAPL", "date": "2024-01-02"}]).to_csv(path, index=False)
            with self.assertRaisesRegex(ValueError, "Colonnes manquantes"):
                load_file_dataset(path)


class CuratedLogicTests(unittest.TestCase):
    def test_classify_flash_crash(self):
        row = pd.Series({"is_anomaly": True, "daily_return": -0.08, "volume_zscore": 1, "volatility_20": 0.02})
        self.assertEqual(classify_anomaly_type(row), "flash_crash")


if __name__ == "__main__":
    unittest.main()
