"""
Import TotalAlert + BTCLow CSV signals into btc_signals.db, fill price context,
run quantitative backtesting (6 modules + streak analysis), generate report.

Usage:
    python3 -m backtesting.import_csv_signals
"""
import os
import re
import csv
import json
import sqlite3
import logging
import shutil
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from backtesting import (
    channel_stats, mfe_mae, time_patterns,
    market_regimes, monte_carlo, deep_analysis,
)
from backtesting.analyze import split_is_oos

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("import_csv")

# ---- Config ----
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "btc_signals.db")
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
UPLOAD_DIR = "/mnt/user-data/uploads"
OUTPUT_DIR = os.path.dirname(__file__)

TOTAL_ALERT_FILE = "Total alert.csv"
BTC_LOW_FILE = "BTC low.csv"

CH_TOTAL = {"id": -100, "name": "TotalAlert"}
CH_LOW = {"id": -200, "name": "BTCLow"}

FEE_RATE = 0.001       # 0.1% per side
FEE_PCT = FEE_RATE * 2 * 100  # 0.2% round-trip
IS_RATIO = 0.70

# ---- Regex patterns ----
RE_BTC_PRICE = re.compile(r"BTC:\s*\$([\d,]+\.\d+)")
RE_VALUE_M = re.compile(r"=\s*([\d.]+)m\$")
RE_LOW_WITH_ACTUAL = re.compile(
    r"(ask|bid)\s+[\d.]+\s*[<>]\s*([\d.]+)m\$\s*\(([\d.]+)m\$\)"
)
RE_LOW_SIMPLE = re.compile(
    r"(ask|bid)\s+[\d.]+\s*[<>]\s*([\d.]+)m\$"
)
RE_OPPOSITE_VALUE = re.compile(r"(?:bid|ask)\s+[\d.]+\s*=\s*([\d.]+)m\$")


# ---- Step 1: Copy files ----

def copy_csv_files():
    """Copy CSV files from uploads to data/ if not present."""
    os.makedirs(DATA_DIR, exist_ok=True)
    for fname in [TOTAL_ALERT_FILE, BTC_LOW_FILE]:
        dst = os.path.join(DATA_DIR, fname)
        if os.path.exists(dst):
            logger.info(f"Already exists: {dst}")
            continue
        src = os.path.join(UPLOAD_DIR, fname)
        if os.path.exists(src):
            shutil.copy2(src, dst)
            logger.info(f"Copied: {src} -> {dst}")
        else:
            logger.warning(f"Source not found: {src}")


# ---- Step 2: Parse Total alert.csv ----

def parse_total_alert(path: str) -> list:
    """Parse Total alert CSV -> list of signal dicts."""
    signals = []
    with open(path, encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for idx, row in enumerate(reader, start=1):
            if len(row) < 2:
                continue
            date_str, text = row[0].strip(), row[1].strip()
            if not text:
                continue

            # Direction from emoji (accept both single 🟩 and double 🟩🟩)
            if "🟩" in text:
                color, direction = "green", "bullish"
            elif "🟥" in text:
                color, direction = "red", "bearish"
            else:
                continue

            # BTC price (optional — will use Binance price if absent)
            m_btc = RE_BTC_PRICE.search(text)
            btc_price = float(m_btc.group(1).replace(",", "")) if m_btc else None

            # Extract bid and ask values from the value lines
            lines = text.split("\n")
            bid_val, ask_val = None, None
            for line in lines[1:]:  # skip first line (emoji + comparison)
                m_val = RE_VALUE_M.search(line)
                if not m_val:
                    continue
                val = float(m_val.group(1))
                if line.strip().startswith("bid"):
                    bid_val = val
                elif line.strip().startswith("ask"):
                    ask_val = val

            if bid_val is None or ask_val is None:
                continue

            ratio = round(bid_val / ask_val, 4) if ask_val > 0 else None

            ts = _parse_timestamp(date_str)
            if ts is None:
                continue

            signals.append({
                "channel_id": CH_TOTAL["id"],
                "channel_name": CH_TOTAL["name"],
                "message_id": idx,
                "message_text": text[:2000],
                "timestamp": ts,
                "indicator_value": ratio,
                "signal_color": color,
                "signal_direction": direction,
                "timeframe": None,
                "btc_price_from_channel": btc_price,
                "extra_data": json.dumps({
                    "bid": bid_val, "ask": ask_val, "type": "imbalance",
                }, ensure_ascii=False),
            })

    logger.info(f"Parsed TotalAlert: {len(signals)} signals from {path}")
    return signals


# ---- Step 3: Parse BTC low.csv ----

def parse_btc_low(path: str) -> list:
    """Parse BTC low CSV -> list of signal dicts."""
    signals = []
    with open(path, encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for idx, row in enumerate(reader, start=1):
            if len(row) < 2:
                continue
            date_str, text = row[0].strip(), row[1].strip()
            if not text:
                continue

            # Direction from single emoji
            if text.startswith("🟩"):
                color, direction = "green", "bullish"
            elif text.startswith("🟥"):
                color, direction = "red", "bearish"
            else:
                continue

            # BTC price (optional — will use Binance price if absent)
            m_btc = RE_BTC_PRICE.search(text)
            btc_price = float(m_btc.group(1).replace(",", "")) if m_btc else None

            # First line: side, threshold, optional actual value
            lines = text.split("\n")
            m_full = RE_LOW_WITH_ACTUAL.search(lines[0])
            m_simple = RE_LOW_SIMPLE.search(lines[0]) if not m_full else None

            if m_full:
                side = m_full.group(1)
                threshold = float(m_full.group(2))
                actual_val = float(m_full.group(3))
                sig_type = "low_liquidity"
            elif m_simple:
                side = m_simple.group(1)
                threshold = float(m_simple.group(2))
                actual_val = threshold  # exact value unknown, use threshold
                sig_type = "high_liquidity"
            else:
                continue

            # Opposite side value (second line)
            opposite_val = None
            if len(lines) > 1:
                m_opp = RE_OPPOSITE_VALUE.search(lines[1])
                if m_opp:
                    opposite_val = float(m_opp.group(1))

            ts = _parse_timestamp(date_str)
            if ts is None:
                continue

            signals.append({
                "channel_id": CH_LOW["id"],
                "channel_name": CH_LOW["name"],
                "message_id": idx,
                "message_text": text[:2000],
                "timestamp": ts,
                "indicator_value": actual_val,
                "signal_color": color,
                "signal_direction": direction,
                "timeframe": None,
                "btc_price_from_channel": btc_price,
                "extra_data": json.dumps({
                    "type": sig_type,
                    "side": side,
                    "threshold": threshold,
                    "opposite_value": opposite_val,
                }, ensure_ascii=False),
            })

    logger.info(f"Parsed BTCLow: {len(signals)} signals from {path}")
    return signals


def _parse_timestamp(date_str: str) -> str | None:
    """Parse 'YYYY-MM-DD HH:MM:SS+00:00' -> ISO format string."""
    try:
        dt = datetime.fromisoformat(date_str)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except (ValueError, TypeError):
        return None


# ---- Step 4: Insert into DB ----

def _build_price_index(conn) -> dict:
    """Build {minute_key: price} dict for O(1) lookup."""
    rows = conn.execute("SELECT timestamp, price FROM btc_price ORDER BY timestamp").fetchall()
    idx = {}
    for ts, price in rows:
        idx[ts[:16]] = price
    logger.info(f"Price index: {len(idx)} points")
    return idx


def _get_price(price_index: dict, target: datetime, tolerance: int = 2):
    """O(1) price lookup with tolerance."""
    for offset in range(tolerance + 1):
        deltas = ([timedelta(0)] if offset == 0
                  else [timedelta(minutes=offset), timedelta(minutes=-offset)])
        for delta in deltas:
            key = (target + delta).strftime("%Y-%m-%dT%H:%M")
            if key in price_index:
                return price_index[key]
    return None


def insert_signals(signals: list) -> int:
    """Delete old data for these channels, insert new signals. Returns count."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    channel_names = list({s["channel_name"] for s in signals})
    placeholders = ",".join("?" * len(channel_names))

    # Delete old signal_price_context entries
    conn.execute(f"""
        DELETE FROM signal_price_context
        WHERE signal_id IN (
            SELECT id FROM signals WHERE channel_name IN ({placeholders})
        )
    """, channel_names)

    # Delete old signals
    conn.execute(f"DELETE FROM signals WHERE channel_name IN ({placeholders})", channel_names)

    # Register channels
    for s in signals:
        conn.execute(
            "INSERT OR REPLACE INTO channels (channel_id, name, parser_type) "
            "VALUES (?, ?, ?)",
            (s["channel_id"], s["channel_name"], "csv_import"),
        )

    # Build price index for btc_price_binance lookup
    price_index = _build_price_index(conn)

    # Insert signals
    inserted = 0
    for s in signals:
        ts_dt = datetime.fromisoformat(s["timestamp"]).replace(tzinfo=timezone.utc)
        btc_binance = _get_price(price_index, ts_dt)
        conn.execute("""
            INSERT OR IGNORE INTO signals
            (channel_id, channel_name, message_id, message_text, timestamp,
             indicator_value, signal_color, signal_direction, timeframe,
             btc_price_from_channel, btc_price_binance, extra_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (s["channel_id"], s["channel_name"], s["message_id"],
              s["message_text"], s["timestamp"],
              s["indicator_value"], s["signal_color"], s["signal_direction"],
              s["timeframe"], s["btc_price_from_channel"], btc_binance,
              s["extra_data"]))
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            inserted += 1

    conn.commit()
    logger.info(f"Inserted {inserted} signals ({len(signals)} parsed)")

    # ---- Step 5: Fill signal_price_context ----
    _fill_price_context(conn, channel_names, price_index)

    conn.close()
    return inserted


def _pct_change(base, target):
    """Percentage change from base to target."""
    if base and target and base > 0:
        return round(((target - base) / base) * 100, 4)
    return None


def _fill_price_context(conn, channel_names, price_index):
    """Fill signal_price_context for newly inserted signals."""
    placeholders = ",".join("?" * len(channel_names))
    rows = conn.execute(f"""
        SELECT s.id, s.timestamp, s.btc_price_binance, s.btc_price_from_channel, s.channel_name
        FROM signals s
        LEFT JOIN signal_price_context ctx ON ctx.signal_id = s.id
        WHERE s.channel_name IN ({placeholders}) AND ctx.id IS NULL
        ORDER BY s.timestamp
    """, channel_names).fetchall()

    filled = 0
    for row in rows:
        sig_id, ts_str, bp_bin, bp_ch, ch_name = row
        st = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)

        price_at = bp_bin or bp_ch
        if not price_at:
            price_at = _get_price(price_index, st)
        if not price_at:
            continue

        p5b = _get_price(price_index, st - timedelta(minutes=5))
        p15b = _get_price(price_index, st - timedelta(minutes=15))
        p1hb = _get_price(price_index, st - timedelta(hours=1))
        p5 = _get_price(price_index, st + timedelta(minutes=5))
        p15 = _get_price(price_index, st + timedelta(minutes=15))
        p1h = _get_price(price_index, st + timedelta(hours=1))
        p4h = _get_price(price_index, st + timedelta(hours=4))
        p24h = _get_price(price_index, st + timedelta(hours=24))

        mask = 0
        if p5:   mask |= 1
        if p15:  mask |= 2
        if p1h:  mask |= 4
        if p4h:  mask |= 8
        if p24h: mask |= 16

        conn.execute("""
            INSERT OR IGNORE INTO signal_price_context (
                signal_id, channel_name, signal_timestamp, price_at_signal,
                price_5m_before, price_15m_before, price_1h_before,
                price_5m_after, price_15m_after, price_1h_after,
                price_4h_after, price_24h_after,
                change_5m_pct, change_15m_pct, change_1h_pct,
                change_4h_pct, change_24h_pct, filled_mask
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sig_id, ch_name, ts_str, price_at,
              p5b, p15b, p1hb, p5, p15, p1h, p4h, p24h,
              _pct_change(price_at, p5), _pct_change(price_at, p15),
              _pct_change(price_at, p1h), _pct_change(price_at, p4h),
              _pct_change(price_at, p24h), mask))
        filled += 1

    conn.commit()
    logger.info(f"Filled price context: {filled} signals")


# ---- Step 6: Run backtesting ----

def run_backtest() -> dict:
    """Load new channels from DB, run 6 modules + streak analysis."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    new_channels = (CH_TOTAL["name"], CH_LOW["name"])

    df_signals = pd.read_sql_query(
        "SELECT id, channel_name, timestamp, indicator_value, signal_color, "
        "signal_direction, timeframe, btc_price_binance, extra_data "
        "FROM signals WHERE channel_name IN (?, ?) ORDER BY timestamp",
        conn, params=new_channels,
    )
    df_prices = pd.read_sql_query(
        "SELECT timestamp, price, volume FROM btc_price ORDER BY timestamp", conn,
    )
    df_context = pd.read_sql_query(
        "SELECT signal_id, channel_name, signal_timestamp, price_at_signal, "
        "change_5m_pct, change_15m_pct, change_1h_pct, change_4h_pct, "
        "change_24h_pct, filled_mask FROM signal_price_context "
        "WHERE channel_name IN (?, ?)",
        conn, params=new_channels,
    )
    conn.close()

    if df_signals.empty:
        logger.error("No signals found for new channels!")
        return {"error": "no_signals"}

    # Convert timestamps
    for df, col in [(df_signals, "timestamp"), (df_prices, "timestamp"),
                    (df_context, "signal_timestamp")]:
        df[col] = pd.to_datetime(df[col], utc=True)

    df_signals["extra_data"] = df_signals["extra_data"].apply(
        lambda x: json.loads(x) if x else {}
    )

    # These channels have explicit directions — no threshold derivation needed
    df_signals["derived_direction"] = df_signals["signal_direction"]

    # IS/OOS split
    df_sig_is, df_sig_oos = split_is_oos(df_signals)
    df_ctx_is, df_ctx_oos = split_is_oos(df_context, "signal_timestamp")
    split_ts = df_sig_oos["timestamp"].min()

    logger.info(
        f"Data: {len(df_signals)} signals, split at {split_ts}: "
        f"IS={len(df_sig_is)}, OOS={len(df_sig_oos)}"
    )

    results = {"metadata": {
        "data_start": str(df_signals["timestamp"].min()),
        "data_end": str(df_signals["timestamp"].max()),
        "total_signals": len(df_signals),
        "total_prices": len(df_prices),
        "fee_rate": FEE_RATE,
        "is_oos_ratio": IS_RATIO,
        "split_timestamp": str(split_ts),
        "channels": list(new_channels),
    }}

    # Count per channel
    for ch in new_channels:
        n = len(df_signals[df_signals["channel_name"] == ch])
        n_ctx = len(df_context[df_context["channel_name"] == ch])
        ctx_full = len(df_context[(df_context["channel_name"] == ch)
                                   & (df_context["filled_mask"].astype(int) == 31)])
        results["metadata"][f"{ch}_signals"] = n
        results["metadata"][f"{ch}_contexts"] = n_ctx
        results["metadata"][f"{ch}_full_ctx"] = ctx_full

    # Run modules
    modules = [
        ("channel_stats", channel_stats),
        ("mfe_mae", mfe_mae),
        ("time_patterns", time_patterns),
        ("market_regimes", market_regimes),
        ("monte_carlo", monte_carlo),
    ]
    for name, mod in modules:
        logger.info(f"Running {name}...")
        t1 = time.time()
        try:
            results[name] = mod.run(df_signals, df_prices, df_context, fee_rate=FEE_RATE)
        except Exception as e:
            logger.error(f"{name} failed: {e}", exc_info=True)
            results[name] = {"error": str(e)}
        logger.info(f"  {name} done in {time.time() - t1:.1f}s")

    # Streak analysis (from deep_analysis)
    logger.info("Running streak analysis...")
    t1 = time.time()
    try:
        fee_pct = FEE_PCT
        mi = deep_analysis._prepare_merged(df_sig_is, df_ctx_is, fee_pct)
        mo = deep_analysis._prepare_merged(df_sig_oos, df_ctx_oos, fee_pct)
        results["streak_strategy"] = deep_analysis._analysis_streak(mi, mo)
    except Exception as e:
        logger.error(f"streak analysis failed: {e}", exc_info=True)
        results["streak_strategy"] = {"error": str(e)}
    logger.info(f"  streak done in {time.time() - t1:.1f}s")

    return results


# ---- Step 7: Build report ----

def build_report(results: dict) -> str:
    """Build human-readable report text."""
    meta = results.get("metadata", {})
    lines = [
        "=" * 70,
        "CSV SIGNALS BACKTESTING REPORT (TotalAlert + BTCLow)",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"Data: {meta.get('data_start', '?')[:10]} to {meta.get('data_end', '?')[:10]}",
        f"Signals: {meta.get('total_signals', '?'):,}  |  "
        f"Prices: {meta.get('total_prices', '?'):,}",
        f"Fee: {FEE_RATE*100:.1f}% per side ({FEE_PCT:.1f}% round-trip)",
        f"IS/OOS: {IS_RATIO*100:.0f}%/{(1-IS_RATIO)*100:.0f}%  |  "
        f"Split: {str(meta.get('split_timestamp', ''))[:10]}",
        "=" * 70, "",
    ]

    for ch in ["TotalAlert", "BTCLow"]:
        n = meta.get(f"{ch}_signals", 0)
        nc = meta.get(f"{ch}_contexts", 0)
        nf = meta.get(f"{ch}_full_ctx", 0)
        lines.append(f"  {ch}: {n} signals, {nc} with context, {nf} fully filled")
    lines.append("")

    # Channel stats
    cs = results.get("channel_stats", {})
    if cs and "error" not in cs:
        lines += _section_channel_stats(cs)

    # MFE/MAE
    mm = results.get("mfe_mae", {})
    if mm and "error" not in mm:
        lines += _section_mfe_mae(mm)

    # Time patterns
    tp = results.get("time_patterns", {})
    if tp and "error" not in tp:
        lines += _section_time_patterns(tp)

    # Market regimes
    mr = results.get("market_regimes", {})
    if mr and "error" not in mr:
        lines += _section_market_regimes(mr)

    # Monte Carlo
    mc = results.get("monte_carlo", {})
    if mc and "error" not in mc:
        lines += _section_monte_carlo(mc)

    # Streak
    ss = results.get("streak_strategy", {})
    if ss and "error" not in ss:
        lines += _section_streak(ss)

    # Comparison with existing channels
    lines += _section_comparison(results)

    lines += ["", "=" * 70, "END OF REPORT", "=" * 70]
    return "\n".join(lines)


def _section_channel_stats(data):
    lines = [
        "", "=" * 60,
        "  1. CHANNEL PERFORMANCE (net of fees)",
        "=" * 60, "",
    ]
    fmt = "{:<14s} {:>6s} {:>7s} {:>8s} {:>6s} {:>7s} {:>7s}"
    for horizon_label in ["5m", "15m", "1h", "4h", "24h"]:
        lines.append(f"  [{horizon_label.upper()} HORIZON]")
        lines.append(fmt.format("Channel", "Trades", "WinR%", "AvgRet%",
                                "PF", "Sharpe", "Sortino"))
        lines.append("-" * 62)
        for ch in sorted(data.keys()):
            h = data[ch].get("horizons", {}).get(horizon_label, {})
            if not h or h.get("trades", 0) == 0:
                continue
            lines.append(fmt.format(
                ch[:14], str(h.get("trades", 0)),
                f"{h.get('win_rate_net_pct', 0):.1f}",
                f"{h.get('avg_return_net_pct', 0):+.4f}",
                f"{h.get('profit_factor_net', 0):.2f}",
                f"{h.get('sharpe_net', 0):.3f}",
                f"{h.get('sortino_net', 0):.3f}",
            ))
        lines.append("")
    return lines


def _section_mfe_mae(data):
    lines = [
        "", "=" * 60,
        "  2. MFE / MAE ANALYSIS",
        "=" * 60, "",
    ]
    per_ch = data.get("per_channel", {})
    for ch in sorted(per_ch):
        for horizon in ["1h", "24h"]:
            h = per_ch[ch].get(horizon, {})
            if not h:
                continue
            lines.append(f"  {ch} ({horizon}):")
            lines.append(f"    MFE avg/med: {h.get('avg_mfe_pct', 0):+.3f}% / "
                         f"{h.get('median_mfe_pct', 0):+.3f}%")
            lines.append(f"    MAE avg/med: {h.get('avg_mae_pct', 0):.3f}% / "
                         f"{h.get('median_mae_pct', 0):.3f}%")
            lines.append(f"    MFE/MAE ratio: {h.get('mfe_mae_ratio', 0):.2f}")
            lines.append(f"    Suggested TP: {h.get('suggested_tp_pct', 0):.2f}%  "
                         f"SL: {h.get('suggested_sl_pct', 0):.2f}%")
    return lines


def _section_time_patterns(data):
    lines = [
        "", "=" * 60,
        "  3. TIME PATTERNS",
        "=" * 60, "",
    ]
    lines.append(f"  Best hour (UTC):  {data.get('best_hour', '?')}")
    lines.append(f"  Worst hour (UTC): {data.get('worst_hour', '?')}")
    lines.append(f"  Best session:     {data.get('best_session', '?')}")
    lines.append(f"  Worst session:    {data.get('worst_session', '?')}")
    by_session = data.get("by_session", {})
    for s, info in sorted(by_session.items()):
        lines.append(f"    {s:8s}: {info.get('signal_count', 0):5d} sig, "
                     f"ret={info.get('avg_return_1h_net', 0):+.4f}%, "
                     f"WR={info.get('win_rate_1h_pct', 0):.1f}%")
    return lines


def _section_market_regimes(data):
    lines = [
        "", "=" * 60,
        "  4. MARKET REGIMES",
        "=" * 60, "",
    ]
    for label in ["volatility_regimes", "trend_regimes"]:
        regimes = data.get(label, {})
        lines.append(f"  [{label.replace('_', ' ').upper()}]")
        for r, info in sorted(regimes.items()):
            lines.append(f"    {r:12s}: {info.get('signal_count', 0):5d} sig, "
                         f"ret={info.get('avg_return_1h_net', 0):+.4f}%, "
                         f"WR={info.get('win_rate_1h_pct', 0):.1f}%")
        lines.append("")
    return lines


def _section_monte_carlo(data):
    lines = [
        "", "=" * 60,
        "  5. MONTE CARLO SIGNIFICANCE",
        "=" * 60, "",
    ]
    lines.append(f"  Verdict: {data.get('overall_verdict', '?')}")
    lines.append("")
    dr = data.get("direction_shuffle", {})
    fmt = "  {:<14s} {:>7s} {:>7s} {:>7s} {:>3s}"
    lines.append(fmt.format("Channel", "Sharpe", "p-val", "z-score", "Sig"))
    lines.append("  " + "-" * 45)
    for ch in sorted(dr):
        info = dr[ch]
        sig = ("**" if info.get("significant_1pct")
               else ("*" if info.get("significant_5pct") else ""))
        lines.append(fmt.format(
            ch[:14],
            f"{info.get('actual_sharpe', 0):.3f}",
            f"{info.get('p_value', 1):.3f}",
            f"{info.get('z_score', 0):.2f}",
            sig,
        ))
    return lines


def _section_streak(data):
    lines = [
        "", "=" * 60,
        "  6. STREAK STRATEGY (enter after N wins, stop after M losses)",
        "=" * 60, "",
    ]
    fmt = "{:<14s} {:>2s}/{:>2s} {:>6s} {:>7s} {:>6s} {:>7s} {:>4s}"
    lines.append(fmt.format("Channel", "N", "M", "IS_WR", "IS_Sh",
                            "OOS_WR", "OOS_Sh", "Fit"))
    lines.append("-" * 55)
    for ch in sorted(data):
        d = data[ch]
        if d.get("skipped"):
            lines.append(f"  {ch}: SKIPPED ({d.get('reason', '')})")
            continue
        p = d["best_is_params"]
        i = d["is_stats"]
        o = d.get("oos_stats", {})
        lines.append(fmt.format(
            ch[:14], str(p["n_wins"]), str(p["m_losses"]),
            f"{i['win_rate']:.1f}", f"{i['sharpe']:.3f}",
            f"{o.get('win_rate', 0):.1f}", f"{o.get('sharpe', 0):.3f}",
            "OVER" if d.get("overfitted") else "OK",
        ))
    return lines


def _section_comparison(results):
    """Compare new channels with existing 9 channels from results.json."""
    lines = [
        "", "=" * 60,
        "  7. COMPARISON WITH EXISTING 9 CHANNELS",
        "=" * 60, "",
    ]
    existing_path = os.path.join(OUTPUT_DIR, "results.json")
    if not os.path.exists(existing_path):
        lines.append("  (results.json not found — run full backtest first)")
        return lines

    with open(existing_path, encoding="utf-8") as f:
        existing = json.load(f)

    old_cs = existing.get("channel_stats", {})
    new_cs = results.get("channel_stats", {})

    # Collect all 1h stats for comparison
    all_channels = []
    for ch, stats in old_cs.items():
        h = stats.get("horizons", {}).get("1h", {})
        if h and h.get("trades", 0) >= 20:
            all_channels.append((ch, h, "existing"))

    for ch, stats in new_cs.items():
        h = stats.get("horizons", {}).get("1h", {})
        if h and h.get("trades", 0) >= 20:
            all_channels.append((ch, h, "NEW"))

    if not all_channels:
        lines.append("  Insufficient data for comparison")
        return lines

    all_channels.sort(key=lambda x: x[1].get("sharpe_net", 0), reverse=True)

    fmt = "{:<20s} {:>6s} {:>7s} {:>8s} {:>7s} {:>7s}  {:>3s}"
    lines.append(fmt.format("Channel", "Trades", "WinR%", "AvgRet%",
                            "Sharpe", "Sortino", ""))
    lines.append("-" * 65)
    for ch, h, tag in all_channels:
        marker = "<<<" if tag == "NEW" else ""
        lines.append(fmt.format(
            ch[:20], str(h.get("trades", 0)),
            f"{h.get('win_rate_net_pct', 0):.1f}",
            f"{h.get('avg_return_net_pct', 0):+.4f}",
            f"{h.get('sharpe_net', 0):.3f}",
            f"{h.get('sortino_net', 0):.3f}",
            marker,
        ))

    return lines


def print_key_findings(results):
    """Print key findings to stdout."""
    print("\n" + "=" * 50)
    print("KEY FINDINGS")
    print("=" * 50)

    cs = results.get("channel_stats", {})
    for ch in ["TotalAlert", "BTCLow"]:
        stats = cs.get(ch, {})
        h1 = stats.get("horizons", {}).get("1h", {})
        if not h1:
            print(f"\n  {ch}: no 1h data")
            continue
        n = h1.get("trades", 0)
        if n < 20:
            print(f"\n  {ch}: insufficient data (N={n} < 20)")
            continue
        print(f"\n  {ch} (1h horizon, {n} trades):")
        print(f"    Win Rate:     {h1.get('win_rate_net_pct', 0):.1f}%")
        print(f"    Avg Return:   {h1.get('avg_return_net_pct', 0):+.4f}%")
        print(f"    Sharpe:       {h1.get('sharpe_net', 0):.4f}")
        print(f"    Sortino:      {h1.get('sortino_net', 0):.4f}")
        print(f"    Profit Fac:   {h1.get('profit_factor_net', 0):.3f}")

    mc = results.get("monte_carlo", {})
    dr = mc.get("direction_shuffle", {})
    for ch in ["TotalAlert", "BTCLow"]:
        info = dr.get(ch, {})
        if info:
            sig = "YES" if info.get("significant_5pct") else "NO"
            print(f"\n  {ch} MC significance: {sig} "
                  f"(p={info.get('p_value', 1):.3f}, z={info.get('z_score', 0):.2f})")

    ss = results.get("streak_strategy", {})
    for ch in ["TotalAlert", "BTCLow"]:
        d = ss.get(ch, {})
        if d and not d.get("skipped") and not d.get("error"):
            p = d["best_is_params"]
            oos = d.get("oos_stats", {})
            print(f"\n  {ch} streak: N={p['n_wins']}/M={p['m_losses']}, "
                  f"OOS Sharpe={oos.get('sharpe', 0):.3f}, "
                  f"{'OVERFIT' if d.get('overfitted') else 'OK'}")

    print()


# ---- Main ----

def main():
    t0 = time.time()

    # Step 1: Copy files
    copy_csv_files()

    # Steps 2-3: Parse CSV files
    total_path = os.path.join(DATA_DIR, TOTAL_ALERT_FILE)
    low_path = os.path.join(DATA_DIR, BTC_LOW_FILE)

    if not os.path.exists(total_path):
        logger.error(f"File not found: {total_path}")
        return
    if not os.path.exists(low_path):
        logger.error(f"File not found: {low_path}")
        return

    signals_total = parse_total_alert(total_path)
    signals_low = parse_btc_low(low_path)
    all_signals = signals_total + signals_low

    if not all_signals:
        logger.error("No signals parsed!")
        return

    # Step 4-5: Insert + fill context
    inserted = insert_signals(all_signals)
    logger.info(f"Total inserted: {inserted}")

    # Step 6: Run backtesting
    results = run_backtest()
    if "error" in results:
        logger.error(f"Backtest failed: {results['error']}")
        return

    # Step 7: Generate report
    report_text = build_report(results)
    report_path = os.path.join(OUTPUT_DIR, "csv_signals_report.txt")
    json_path = os.path.join(OUTPUT_DIR, "csv_signals_results.json")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str, ensure_ascii=False)

    print(f"\nReport saved: {report_path}")
    print(f"JSON saved:   {json_path}")

    print_key_findings(results)

    logger.info(f"Total time: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
