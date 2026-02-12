"""
BTC Signal Backtesting — Entry point and orchestrator.
Loads data from btc_signals.db, derives directions, splits IS/OOS,
runs all analysis modules, and saves report.txt + results.json.
"""
import os
import json
import sqlite3
import logging
import time

import numpy as np
import pandas as pd

from backtesting import channel_stats, confluence, optimal_params
from backtesting import time_patterns, risk_metrics, sequences
from backtesting import mfe_mae, market_regimes, correlations
from backtesting import latency_decay, monte_carlo, report_builder
from backtesting import deep_analysis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backtesting")

# ---- Configuration ----
DB_PATH = os.environ.get(
    "BTC_DB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "btc_signals.db"),
)
FEE_RATE = 0.001        # 0.1% per side → 0.2% round-trip
IS_RATIO = 0.70
MIN_SIGNALS = 50
MIN_SIGNALS_WF = 100
OUTPUT_DIR = os.path.dirname(__file__)

DIRECTION_THRESHOLDS = {
    "AltSwing":        (60.0, 40.0),
    "Scalp17":         (70.0, 50.0),
    "SellsPowerIndex": (60.0, 40.0),
    "AltSPI":          (60.0, 40.0),
}

HORIZONS = {"5m": 1, "15m": 2, "1h": 4, "4h": 8, "24h": 16}


def load_data():
    """Load signals, prices, context from SQLite into DataFrames."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    df_signals = pd.read_sql_query(
        "SELECT id, channel_name, timestamp, indicator_value, signal_color, "
        "signal_direction, timeframe, btc_price_binance, extra_data "
        "FROM signals ORDER BY timestamp", conn,
    )
    df_prices = pd.read_sql_query(
        "SELECT timestamp, price, volume FROM btc_price ORDER BY timestamp", conn,
    )
    df_context = pd.read_sql_query(
        "SELECT signal_id, channel_name, signal_timestamp, price_at_signal, "
        "change_5m_pct, change_15m_pct, change_1h_pct, change_4h_pct, "
        "change_24h_pct, filled_mask FROM signal_price_context", conn,
    )
    conn.close()

    for df, col in [(df_signals, "timestamp"), (df_prices, "timestamp"),
                    (df_context, "signal_timestamp")]:
        df[col] = pd.to_datetime(df[col], utc=True)

    df_signals["extra_data"] = df_signals["extra_data"].apply(
        lambda x: json.loads(x) if x else {}
    )
    logger.info(
        f"Loaded: {len(df_signals)} signals, {len(df_prices)} prices, "
        f"{len(df_context)} contexts"
    )
    return df_signals, df_prices, df_context


def derive_directions(df_signals):
    """Add 'derived_direction' column using thresholds for value-only channels."""
    dirs = df_signals["signal_direction"].copy()

    for ch_name, (bull_th, bear_th) in DIRECTION_THRESHOLDS.items():
        mask = df_signals["channel_name"] == ch_name
        vals = df_signals.loc[mask, "indicator_value"]
        dirs.loc[mask & (vals >= bull_th)] = "bullish"
        dirs.loc[mask & (vals <= bear_th)] = "bearish"
        dirs.loc[mask & (vals > bear_th) & (vals < bull_th)] = "neutral"
        dirs.loc[mask & vals.isna()] = None

    df_signals["derived_direction"] = dirs
    return df_signals


def split_is_oos(df, ts_col="timestamp"):
    """Split DataFrame into In-Sample (70%) and Out-of-Sample (30%) by time."""
    sorted_ts = df[ts_col].sort_values()
    split_ts = sorted_ts.iloc[int(len(sorted_ts) * IS_RATIO)]
    return df[df[ts_col] < split_ts].copy(), df[df[ts_col] >= split_ts].copy()


def main():
    t0 = time.time()
    logger.info("Loading data...")
    df_signals, df_prices, df_context = load_data()
    df_signals = derive_directions(df_signals)

    df_sig_is, df_sig_oos = split_is_oos(df_signals)
    df_ctx_is, df_ctx_oos = split_is_oos(df_context, "signal_timestamp")
    split_ts = df_sig_oos["timestamp"].min()
    logger.info(f"IS/OOS split at {split_ts}: IS={len(df_sig_is)}, OOS={len(df_sig_oos)}")

    results = {"metadata": {
        "data_start": str(df_signals["timestamp"].min()),
        "data_end": str(df_signals["timestamp"].max()),
        "total_signals": len(df_signals),
        "total_prices": len(df_prices),
        "fee_rate": FEE_RATE,
        "is_oos_ratio": IS_RATIO,
        "split_timestamp": str(split_ts),
    }}

    modules = [
        ("channel_stats", channel_stats),
        ("confluence", confluence),
        ("optimal_params", optimal_params),
        ("time_patterns", time_patterns),
        ("risk_metrics", risk_metrics),
        ("sequences", sequences),
        ("mfe_mae", mfe_mae),
        ("market_regimes", market_regimes),
        ("correlations", correlations),
        ("latency_decay", latency_decay),
        ("monte_carlo", monte_carlo),
    ]

    for name, mod in modules:
        logger.info(f"Running {name}...")
        t1 = time.time()
        try:
            if name == "optimal_params":
                results[name] = mod.run(
                    df_signals, df_prices, df_context,
                    df_sig_is, df_sig_oos, df_ctx_is, df_ctx_oos,
                    fee_rate=FEE_RATE,
                )
            else:
                results[name] = mod.run(
                    df_signals, df_prices, df_context, fee_rate=FEE_RATE,
                )
        except Exception as e:
            logger.error(f"{name} failed: {e}", exc_info=True)
            results[name] = {"error": str(e)}
        logger.info(f"  {name} done in {time.time() - t1:.1f}s")

    report_builder.run(results, OUTPUT_DIR)

    logger.info("Running deep_analysis...")
    t1 = time.time()
    try:
        deep_analysis.run(df_signals, df_prices, df_context,
                          df_sig_is, df_sig_oos, df_ctx_is, df_ctx_oos,
                          fee_rate=FEE_RATE)
    except Exception as e:
        logger.error(f"deep_analysis failed: {e}", exc_info=True)
    logger.info(f"  deep_analysis done in {time.time() - t1:.1f}s")

    logger.info(f"Total time: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
