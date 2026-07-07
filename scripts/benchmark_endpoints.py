"""Benchmark reproductible des endpoints /ingest et /ingest_fast."""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen


TICKERS_100 = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "JPM", "JNJ",
    "V", "PG", "XOM", "UNH", "MA", "HD", "CVX", "MRK", "ABBV", "KO",
    "PEP", "COST", "AVGO", "LLY", "WMT", "BAC", "MCD", "CSCO", "TMO", "ACN",
    "CRM", "ABT", "DHR", "LIN", "CMCSA", "NKE", "TXN", "PM", "NEE", "ORCL",
    "AMD", "UPS", "RTX", "HON", "QCOM", "LOW", "AMGN", "IBM", "INTC", "CAT",
    "SPGI", "GS", "BLK", "DE", "SBUX", "INTU", "ISRG", "MDT", "GILD", "BKNG",
    "ADP", "TJX", "NOC", "ADI", "VRTX", "LMT", "SYK", "REGN", "PGR", "CB",
    "SCHW", "AMT", "C", "MO", "ZTS", "SO", "DUK", "PLD", "CI", "BDX",
    "CME", "USB", "CL", "TGT", "EL", "MMM", "FIS", "ITW", "CSX", "NSC",
    "GM", "F", "MU", "PANW", "SNPS", "CDNS", "KLAC", "MCK", "MAR", "AON",
]


def post_json(url: str, payload: dict, timeout: int) -> tuple[dict, float]:
    body = json.dumps(payload).encode("utf-8")
    request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    started = time.perf_counter()
    with urlopen(request, timeout=timeout) as response:
        result = json.loads(response.read().decode("utf-8"))
    return result, time.perf_counter() - started


def benchmark(base_url: str, period: str, timeout: int) -> dict:
    report = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "base_url": base_url,
        "period": period,
        "cache_enabled": False,
        "results": [],
    }
    for size in (1, 100):
        tickers = TICKERS_100[:size]
        payload = {
            "data": {
                "tickers": tickers,
                "period": period,
                "run_staging": True,
                "run_curated": True,
                "use_cache": False,
            }
        }
        standard, standard_wall = post_json(f"{base_url}/ingest", payload, timeout)
        fast, fast_wall = post_json(f"{base_url}/ingest_fast", payload, timeout)
        gain = 100.0 * (standard_wall - fast_wall) / standard_wall if standard_wall else 0.0
        report["results"].append(
            {
                "batch_size": size,
                "standard_wall_ms": round(standard_wall * 1000, 2),
                "fast_wall_ms": round(fast_wall * 1000, 2),
                "gain_pct": round(gain, 2),
                "standard_status": standard.get("status"),
                "fast_status": fast.get("status"),
                "standard_errors": standard.get("errors", []),
                "fast_errors": fast.get("errors", []),
            }
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--period", default="5d")
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--output", default="benchmarks/benchmark_results.json")
    args = parser.parse_args()

    report = benchmark(args.base_url.rstrip("/"), args.period, args.timeout)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
