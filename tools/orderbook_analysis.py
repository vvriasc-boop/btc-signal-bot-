"""
Orderbook channel analysis — entry point and orchestrator.

Usage:
    python3 -m tools.orderbook_analysis              # analyze only
    python3 -m tools.orderbook_analysis --download    # download first

Output:
    backtesting/orderbook_report.txt
    backtesting/orderbook_results.json
"""
import os
import sys
import json
import logging
import sqlite3
import time as _time

import numpy as np
import pandas as pd

from tools.orderbook_config import ALL_TITLES, FEE_RATE
from tools.orderbook_db import (
    get_db, build_price_index, cleanup_orderbook_data,
    parse_and_insert, fill_price_context,
)
from tools import orderbook_h1_imbalance
from tools import orderbook_h2_levels
from tools.orderbook_report import build_report

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("orderbook.analysis")

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "btc_signals.db")


def _load_data(conn):
    """Load orderbook signals + prices into DataFrames."""
    ph = ",".join("?" * len(ALL_TITLES))

    df_signals = pd.read_sql_query(f"""
        SELECT id, channel_name, timestamp, indicator_value,
               signal_direction, btc_price_from_channel, btc_price_binance,
               extra_data
        FROM signals
        WHERE channel_name IN ({ph})
        ORDER BY timestamp
    """, conn, params=ALL_TITLES)

    df_prices = pd.read_sql_query(
        "SELECT timestamp, price FROM btc_price ORDER BY timestamp", conn)

    # Parse timestamps
    df_signals["timestamp"] = pd.to_datetime(df_signals["timestamp"], utc=True)
    df_prices["timestamp"] = pd.to_datetime(df_prices["timestamp"], utc=True)

    # Parse extra_data JSON
    df_signals["extra_data"] = df_signals["extra_data"].apply(
        lambda x: json.loads(x) if x else {})

    logger.info(f"Loaded {len(df_signals)} orderbook signals, "
                f"{len(df_prices)} price points")
    return df_signals, df_prices


def run_analysis(conn):
    """Run full orderbook analysis pipeline."""
    t0 = _time.time()

    # Step 1: Build price index and parse signals
    logger.info("Building price index...")
    price_index = build_price_index(conn)

    logger.info("Cleaning up old orderbook data...")
    cleanup_orderbook_data(conn, ALL_TITLES)

    logger.info("Parsing and inserting signals...")
    parse_stats = parse_and_insert(conn, price_index)

    # Count parsed
    total_parsed = sum(s.get("parsed", 0) for s in parse_stats.values())
    total_failed = sum(s.get("failed", 0) for s in parse_stats.values())
    logger.info(f"Parsed: {total_parsed} signals ({total_failed} failed)")

    if total_parsed == 0:
        logger.error("No signals parsed — check raw_messages for orderbook channels")
        return

    logger.info("Filling price context...")
    filled = fill_price_context(conn, ALL_TITLES, price_index)
    logger.info(f"Price context filled: {filled} signals")

    # Step 2: Load data
    logger.info("Loading data for analysis...")
    df_signals, df_prices = _load_data(conn)

    if len(df_signals) < 20:
        logger.error(f"Only {len(df_signals)} signals — insufficient for analysis")
        return

    # Step 3: Run Hypothesis 1
    logger.info("Running Hypothesis 1: Bid/Ask Imbalance...")
    t1 = _time.time()
    h1_results = orderbook_h1_imbalance.run(df_signals, df_prices, FEE_RATE)
    logger.info(f"H1 done in {_time.time() - t1:.1f}s")

    # Step 4: Run Hypothesis 2
    logger.info("Running Hypothesis 2: Levels...")
    t1 = _time.time()
    h2_results = orderbook_h2_levels.run(df_signals, df_prices, FEE_RATE)
    logger.info(f"H2 done in {_time.time() - t1:.1f}s")

    # Step 5: Build report
    logger.info("Building report...")
    report_path, json_path = build_report(h1_results, h2_results, parse_stats)

    # Print key findings
    _print_findings(h1_results, h2_results)

    logger.info(f"Total analysis time: {_time.time() - t0:.1f}s")


def _print_findings(h1_results, h2_results):
    """Print key findings to stdout."""
    print()
    print("=" * 60)
    print("KEY FINDINGS — Orderbook Analysis")
    print("=" * 60)

    # H1
    print("\n[H1: Bid/Ask Imbalance]")
    from tools.orderbook_config import PAIRS
    for _, _, pair in PAIRS:
        data = h1_results.get(pair, {})
        if data.get("skipped"):
            continue
        wf = data.get("walk_forward", {})
        if "best_params" not in wf:
            continue
        bp = wf["best_params"]
        oos = wf.get("oos_1h", {})
        marker = ""
        if oos.get("sharpe", 0) > 0 and not wf.get("overfitted"):
            marker = " <<<< POSITIVE OOS"
        print(f"  {pair}: thr={bp['threshold']}, {bp['mode']}, "
              f"OOS_Sh={oos.get('sharpe', 0):.3f}{marker}")

    agg = h1_results.get("Aggregate", {})
    if not agg.get("skipped"):
        wf = agg.get("walk_forward", {})
        if "best_params" in wf:
            bp = wf["best_params"]
            oos = wf.get("oos_1h", {})
            print(f"  Aggregate: thr={bp['threshold']}, {bp['mode']}, "
                  f"OOS_Sh={oos.get('sharpe', 0):.3f}")

    # H2
    print("\n[H2: Limit Orders as S/R Levels]")
    from tools.orderbook_config import H2_CHANNEL_SETS
    for set_name in H2_CHANNEL_SETS:
        data = h2_results.get(set_name, {})
        if data.get("skipped"):
            print(f"  {set_name}: SKIPPED ({data.get('reason', '')})")
            continue
        wf = data.get("walk_forward", [])
        if not wf:
            print(f"  {set_name}: no WF combos")
            continue
        w = wf[0]
        p = w["params"]
        oos = w.get("oos_1h", {})
        marker = ""
        if oos.get("sharpe", 0) > 0 and not w.get("overfitted"):
            marker = " <<<< POSITIVE OOS"
        print(f"  {set_name}: zw=+-{p['zw']}%, ms={p['ms']}, "
              f"{p['pattern']}, OOS_Sh={oos.get('sharpe', 0):.3f}{marker}")

    print()


async def run_download():
    """Download orderbook channels."""
    from tools.orderbook_download import main_download
    await main_download()


def main():
    """Entry point."""
    download = "--download" in sys.argv

    if download:
        import asyncio
        logger.info("Starting download...")
        asyncio.run(run_download())
        logger.info("Download complete")

    conn = get_db(DB_PATH)
    try:
        # Check if we have raw messages
        count = conn.execute(
            f"SELECT COUNT(*) FROM raw_messages WHERE channel_name IN "
            f"({','.join('?' * len(ALL_TITLES))})",
            ALL_TITLES,
        ).fetchone()[0]

        if count == 0:
            logger.error(
                "No raw messages found for orderbook channels.\n"
                "Run with --download first:\n"
                "  python3 -m tools.orderbook_analysis --download"
            )
            return

        logger.info(f"Found {count} raw messages for orderbook channels")
        run_analysis(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
