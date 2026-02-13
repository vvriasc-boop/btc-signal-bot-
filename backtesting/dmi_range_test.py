"""
DMI Range Compression Test.

Hypothesis: when DMI_SMF sends multiple signals within 4 hours
and BTC price stays in a narrow range (0.3% or 0.5%),
the next move is stronger and more predictable.

Also tests the same hypothesis for DyorAlerts and Scalp17.

Usage:
    python3 -m backtesting.dmi_range_test
Output:
    backtesting/dmi_range_report.txt
"""
import os
import json
import logging
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

logger = logging.getLogger("backtesting.dmi_range")

OUTPUT_DIR = os.path.dirname(__file__)
ANN_FACTOR = 8760
LOOKBACK_SEC = 4 * 3600  # 4 hours in seconds
MIN_SIGNALS = 20
FEE_RATE_DEFAULT = 0.001

FILTERS = [
    (2, 0.3), (2, 0.5),
    (3, 0.3), (3, 0.5),
    (4, 0.3), (4, 0.5),
]

CHANNELS = ["DMI_SMF", "DyorAlerts", "Scalp17"]

HORIZONS = {
    "5m":  ("change_5m_pct",  1),
    "15m": ("change_15m_pct", 2),
    "1h":  ("change_1h_pct",  4),
    "4h":  ("change_4h_pct",  8),
}


# ---- Helpers ----

def _sharpe(r):
    if len(r) < 2 or np.std(r) == 0:
        return 0.0
    return round(float(np.mean(r) / np.std(r) * np.sqrt(ANN_FACTOR)), 4)


def _pf(r):
    g = float(r[r > 0].sum())
    l = abs(float(r[r < 0].sum()))
    if l == 0:
        return 99.0 if g > 0 else 0.0
    return round(g / l, 3)


def _quick_stats(rets):
    if len(rets) == 0:
        return {"trades": 0, "win_rate": 0, "avg_return": 0,
                "profit_factor": 0, "sharpe": 0, "total_return": 0}
    return {
        "trades": int(len(rets)),
        "win_rate": round(float((rets > 0).mean() * 100), 1),
        "avg_return": round(float(np.mean(rets)), 4),
        "profit_factor": _pf(rets),
        "sharpe": _sharpe(rets),
        "total_return": round(float(np.sum(rets)), 2),
    }


def _horizon_stats(grp, fee_pct, sign):
    """Compute stats per horizon for a filtered group."""
    results = {}
    for hz_name, (col, mask_bit) in HORIZONS.items():
        valid = (grp["filled_mask"].values.astype(int) & mask_bit) > 0
        valid &= grp[col].notna().values
        if valid.sum() < 2:
            results[hz_name] = {"trades": 0, "insufficient": True}
            continue
        raw = grp.loc[valid, col].values
        s = sign[valid]
        gross = raw * s
        net = gross - fee_pct
        results[hz_name] = _quick_stats(net)
    return results


# ---- Core: compute lookback counts & price range ----

def _compute_lookback(ch_signals, price_ts, price_vals):
    """For each signal, compute count_4h and range_pct in lookback window.

    Args:
        ch_signals: DataFrame with 'timestamp' (datetime64[ns, UTC]) sorted
        price_ts: numpy array of int64 (epoch seconds) for btc_price, sorted
        price_vals: numpy array of float64 for btc_price

    Returns:
        count_4h: int array, number of signals (including self) in [t-4h, t]
        range_pct: float array, (max-min)/avg * 100 for price in [t-4h, t]
    """
    sig_ts = ch_signals["timestamp"].values.astype("int64") // 10**9  # epoch sec
    n = len(sig_ts)
    count_4h = np.ones(n, dtype=int)  # at least self
    range_pct = np.zeros(n, dtype=float)

    for i in range(n):
        t = sig_ts[i]
        t_start = t - LOOKBACK_SEC

        # Count signals in [t_start, t] (including self)
        left = np.searchsorted(sig_ts, t_start, side="left")
        count_4h[i] = i - left + 1  # signals from left..i inclusive

        # Price range in [t_start, t]
        p_left = np.searchsorted(price_ts, t_start, side="left")
        p_right = np.searchsorted(price_ts, t, side="right")
        if p_right > p_left:
            window = price_vals[p_left:p_right]
            mn, mx = window.min(), window.max()
            avg = window.mean()
            if avg > 0:
                range_pct[i] = (mx - mn) / avg * 100
        else:
            range_pct[i] = np.nan

    return count_4h, range_pct


# ---- Analysis for one channel ----

def _analyze_channel(ch_name, merged, df_prices, fee_pct, is_cutoff=None):
    """Run range compression analysis for one channel.

    Args:
        ch_name: channel name
        merged: merged signals+context for this channel, sorted by timestamp
        df_prices: btc_price DataFrame
        fee_pct: round-trip fee in % (e.g. 0.2)
        is_cutoff: timestamp for IS/OOS split (None = no walk-forward)

    Returns:
        dict with results
    """
    if len(merged) < MIN_SIGNALS:
        return {"skipped": True, "reason": f"signals < {MIN_SIGNALS}"}

    # Prepare price arrays for lookback
    price_ts = df_prices["timestamp"].values.astype("int64") // 10**9
    price_vals = df_prices["price"].values.astype(float)

    # Compute lookback features
    count_4h, range_pct_arr = _compute_lookback(merged, price_ts, price_vals)
    merged = merged.copy()
    merged["count_4h"] = count_4h
    merged["range_pct"] = range_pct_arr

    # Direction sign
    sign = merged["derived_direction"].map({"bullish": 1.0, "bearish": -1.0}).values

    # Baseline (all signals, no filter)
    baseline = _horizon_stats(merged, fee_pct, sign)

    # Baseline by direction
    bull_mask = merged["derived_direction"].values == "bullish"
    bear_mask = merged["derived_direction"].values == "bearish"
    baseline_bull = _horizon_stats(merged[bull_mask], fee_pct, sign[bull_mask]) if bull_mask.sum() >= 5 else {}
    baseline_bear = _horizon_stats(merged[bear_mask], fee_pct, sign[bear_mask]) if bear_mask.sum() >= 5 else {}

    # Test each filter
    filter_results = {}
    for min_count, max_range in FILTERS:
        key = f"count>={min_count}_range<={max_range}"
        mask = (merged["count_4h"].values >= min_count) & (merged["range_pct"].values <= max_range)
        n_pass = int(mask.sum())

        if n_pass < MIN_SIGNALS:
            filter_results[key] = {
                "n_signals": n_pass,
                "insufficient": True,
                "message": f"N={n_pass} < {MIN_SIGNALS}",
            }
            continue

        grp = merged[mask]
        grp_sign = sign[mask]

        # All directions
        hz_all = _horizon_stats(grp, fee_pct, grp_sign)

        # By direction
        bull_m = grp["derived_direction"].values == "bullish"
        bear_m = grp["derived_direction"].values == "bearish"
        hz_bull = _horizon_stats(grp[bull_m], fee_pct, grp_sign[bull_m]) if bull_m.sum() >= 5 else {}
        hz_bear = _horizon_stats(grp[bear_m], fee_pct, grp_sign[bear_m]) if bear_m.sum() >= 5 else {}

        # First vs last signal in cluster
        first_last = _first_vs_last(grp, merged, fee_pct)

        filter_results[key] = {
            "n_signals": n_pass,
            "horizons_all": hz_all,
            "horizons_bullish": hz_bull,
            "horizons_bearish": hz_bear,
            "first_vs_last": first_last,
        }

    # Walk-forward for best filter
    wf_result = {}
    if is_cutoff is not None:
        wf_result = _walk_forward(merged, sign, fee_pct, is_cutoff)

    return {
        "total_signals": len(merged),
        "baseline": baseline,
        "baseline_bullish": baseline_bull,
        "baseline_bearish": baseline_bear,
        "filters": filter_results,
        "walk_forward": wf_result,
    }


def _first_vs_last(grp, all_signals, fee_pct):
    """Check if first or last signal in a cluster performs better."""
    sig_ts = all_signals["timestamp"].values.astype("int64") // 10**9
    grp_ts = grp["timestamp"].values.astype("int64") // 10**9

    is_last = np.zeros(len(grp), dtype=bool)
    is_first_in_cluster = np.zeros(len(grp), dtype=bool)

    for i in range(len(grp)):
        t = grp_ts[i]
        # Find next signal in channel after this one
        idx_in_all = np.searchsorted(sig_ts, t, side="right")
        if idx_in_all >= len(sig_ts):
            is_last[i] = True
        else:
            next_t = sig_ts[idx_in_all]
            # If next signal is > 4h away, this is last in cluster
            if next_t - t > LOOKBACK_SEC:
                is_last[i] = True

        # Check if this is the first signal that meets the count threshold
        t_start = t - LOOKBACK_SEC
        left = np.searchsorted(sig_ts, t_start, side="left")
        idx_self = np.searchsorted(sig_ts, t, side="left")
        if idx_self == left:
            is_first_in_cluster[i] = True

    sign = grp["derived_direction"].map({"bullish": 1.0, "bearish": -1.0}).values
    result = {}

    if is_last.sum() >= 5:
        result["last_in_cluster"] = _horizon_stats(
            grp[is_last], fee_pct, sign[is_last])
    if is_first_in_cluster.sum() >= 5:
        result["first_in_cluster"] = _horizon_stats(
            grp[is_first_in_cluster], fee_pct, sign[is_first_in_cluster])
    if (~is_last & ~is_first_in_cluster).sum() >= 5:
        mid = ~is_last & ~is_first_in_cluster
        result["middle_of_cluster"] = _horizon_stats(
            grp[mid], fee_pct, sign[mid])

    return result


def _walk_forward(merged, sign, fee_pct, is_cutoff):
    """Walk-forward: find best filter on IS, validate on OOS."""
    is_mask = merged["timestamp"] < is_cutoff
    oos_mask = ~is_mask

    is_data = merged[is_mask]
    oos_data = merged[oos_mask]
    is_sign = sign[is_mask.values]
    oos_sign = sign[oos_mask.values]

    if len(is_data) < MIN_SIGNALS or len(oos_data) < 10:
        return {"skipped": True, "reason": "insufficient IS or OOS data"}

    # Find best filter on IS by 1h Sharpe
    best_key = None
    best_sharpe = -np.inf
    is_results = {}

    for min_count, max_range in FILTERS:
        key = f"count>={min_count}_range<={max_range}"
        mask_is = (is_data["count_4h"].values >= min_count) & (is_data["range_pct"].values <= max_range)

        if mask_is.sum() < MIN_SIGNALS:
            continue

        grp = is_data[mask_is]
        gs = is_sign[mask_is]

        # Use 1h horizon for comparison
        col, bit = HORIZONS["1h"]
        valid = (grp["filled_mask"].values.astype(int) & bit) > 0
        valid &= grp[col].notna().values
        if valid.sum() < 10:
            continue

        rets = grp.loc[valid, col].values * gs[valid] - fee_pct
        s = _quick_stats(rets)
        is_results[key] = s

        if s["sharpe"] > best_sharpe:
            best_sharpe = s["sharpe"]
            best_key = key

    if best_key is None:
        return {"skipped": True, "reason": "no valid IS filter"}

    # Parse best params
    parts = best_key.replace("count>=", "").replace("_range<=", " ").split()
    best_count = int(parts[0])
    best_range = float(parts[1])

    # Apply to OOS
    mask_oos = ((oos_data["count_4h"].values >= best_count)
                & (oos_data["range_pct"].values <= best_range))

    oos_filtered = oos_data[mask_oos]
    oos_fs = oos_sign[mask_oos]

    if len(oos_filtered) < 5:
        return {
            "best_filter": best_key,
            "is_stats": is_results[best_key],
            "oos_stats": {"trades": int(mask_oos.sum()), "insufficient": True},
            "overfitted": None,
        }

    oos_hz = _horizon_stats(oos_filtered, fee_pct, oos_fs)

    # Overfitting check on 1h
    oos_1h_sharpe = oos_hz.get("1h", {}).get("sharpe", 0)
    overfitted = best_sharpe > 0 and oos_1h_sharpe < best_sharpe * 0.5

    return {
        "best_filter": best_key,
        "is_stats": is_results[best_key],
        "oos_stats": oos_hz,
        "overfitted": overfitted,
    }


# ---- Report builder ----

def _build_report(results):
    L = [
        "=" * 70,
        "DMI RANGE COMPRESSION TEST",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "Fee: 0.1% per side (0.2% round-trip)",
        "",
        "Hypothesis: multiple signals in 4h + narrow price range => stronger move",
        "=" * 70,
    ]

    for ch_name in CHANNELS:
        ch = results.get(ch_name, {})
        L += ["", "=" * 60, f"  {ch_name}", "=" * 60, ""]

        if ch.get("skipped"):
            L.append(f"  SKIPPED: {ch.get('reason', '')}")
            continue

        L.append(f"  Total directional signals: {ch['total_signals']}")

        # Baseline
        L.append("")
        L.append("  [BASELINE - no filter]")
        _append_hz_table(L, ch["baseline"])

        if ch.get("baseline_bullish"):
            L.append("  [BASELINE - bullish only]")
            _append_hz_table(L, ch["baseline_bullish"])
        if ch.get("baseline_bearish"):
            L.append("  [BASELINE - bearish only]")
            _append_hz_table(L, ch["baseline_bearish"])

        # Filters
        L.append("  [FILTERED RESULTS]")
        for fkey in sorted(ch.get("filters", {})):
            fd = ch["filters"][fkey]
            L.append(f"  --- {fkey} ---")
            if fd.get("insufficient"):
                L.append(f"    N={fd['n_signals']} — insufficient data")
                L.append("")
                continue
            L.append(f"    N={fd['n_signals']} signals passed filter")

            L.append("    All directions:")
            _append_hz_table(L, fd["horizons_all"], indent=6)

            if fd.get("horizons_bullish"):
                L.append("    Bullish only:")
                _append_hz_table(L, fd["horizons_bullish"], indent=6)
            if fd.get("horizons_bearish"):
                L.append("    Bearish only:")
                _append_hz_table(L, fd["horizons_bearish"], indent=6)

            fl = fd.get("first_vs_last", {})
            if fl:
                L.append("    Position in cluster:")
                for pos_key in ["first_in_cluster", "last_in_cluster", "middle_of_cluster"]:
                    if pos_key in fl:
                        L.append(f"      {pos_key}:")
                        _append_hz_table(L, fl[pos_key], indent=8)

        # Walk-forward
        wf = ch.get("walk_forward", {})
        L += ["", "  [WALK-FORWARD VALIDATION (70/30)]"]
        if wf.get("skipped"):
            L.append(f"    SKIPPED: {wf.get('reason', '')}")
        elif wf.get("best_filter"):
            L.append(f"    Best IS filter: {wf['best_filter']}")
            is_s = wf.get("is_stats", {})
            L.append(f"    IS:  {is_s.get('trades',0)} tr, "
                     f"WR={is_s.get('win_rate',0):.1f}%, "
                     f"avg={is_s.get('avg_return',0):+.4f}%, "
                     f"Sh={is_s.get('sharpe',0):.3f}")
            oos_s = wf.get("oos_stats", {})
            if oos_s.get("insufficient"):
                L.append(f"    OOS: {oos_s.get('trades', 0)} tr — insufficient data")
            elif "1h" in oos_s:
                o1h = oos_s["1h"]
                L.append(f"    OOS 1h: {o1h.get('trades',0)} tr, "
                         f"WR={o1h.get('win_rate',0):.1f}%, "
                         f"avg={o1h.get('avg_return',0):+.4f}%, "
                         f"Sh={o1h.get('sharpe',0):.3f}")
            ovf = wf.get("overfitted")
            if ovf is not None:
                L.append(f"    Overfitted: {'YES' if ovf else 'NO'}")
        L.append("")

    # Summary
    L += ["", "=" * 60, "  SUMMARY", "=" * 60, ""]
    for ch_name in CHANNELS:
        ch = results.get(ch_name, {})
        if ch.get("skipped"):
            L.append(f"  {ch_name}: SKIPPED")
            continue
        _append_channel_summary(L, ch_name, ch)

    L += ["", "=" * 70, "END OF DMI RANGE COMPRESSION TEST", "=" * 70]
    return L


def _append_hz_table(L, hz_dict, indent=4):
    """Append a compact horizon table."""
    pad = " " * indent
    fmt = f"{pad}{{:<4s}} {{:>4d}} tr  WR={{:>5.1f}}%  avg={{:>+8.4f}}%  PF={{:>6.3f}}  Sh={{:>7.3f}}"
    for hz in ["5m", "15m", "1h", "4h"]:
        d = hz_dict.get(hz, {})
        if d.get("insufficient") or d.get("trades", 0) == 0:
            L.append(f"{pad}{hz:<4s}   -- insufficient data --")
            continue
        L.append(fmt.format(hz, d["trades"], d["win_rate"],
                            d["avg_return"], d["profit_factor"], d["sharpe"]))
    L.append("")


def _append_channel_summary(L, ch_name, ch):
    """Print best filter improvement vs baseline for a channel."""
    base_1h = ch.get("baseline", {}).get("1h", {})
    base_wr = base_1h.get("win_rate", 0)
    base_avg = base_1h.get("avg_return", 0)
    base_sh = base_1h.get("sharpe", 0)

    best_improvement = None
    best_key = None

    for fkey, fd in ch.get("filters", {}).items():
        if fd.get("insufficient"):
            continue
        f_1h = fd.get("horizons_all", {}).get("1h", {})
        if f_1h.get("trades", 0) < MIN_SIGNALS:
            continue
        f_sh = f_1h.get("sharpe", 0)
        if best_improvement is None or f_sh > best_improvement:
            best_improvement = f_sh
            best_key = fkey

    if best_key is None:
        L.append(f"  {ch_name}: no filter with enough data to compare")
        return

    fd = ch["filters"][best_key]
    f_1h = fd["horizons_all"]["1h"]
    L.append(f"  {ch_name}:")
    L.append(f"    Baseline 1h: {base_1h.get('trades',0)} tr, "
             f"WR={base_wr:.1f}%, avg={base_avg:+.4f}%, Sh={base_sh:.3f}")
    L.append(f"    Best filter ({best_key}): {f_1h['trades']} tr, "
             f"WR={f_1h['win_rate']:.1f}%, avg={f_1h['avg_return']:+.4f}%, "
             f"Sh={f_1h['sharpe']:.3f}")

    if base_sh != 0:
        diff = f_1h["sharpe"] - base_sh
        L.append(f"    Sharpe delta: {diff:+.3f} ({'BETTER' if diff > 0 else 'WORSE'})")
    else:
        L.append(f"    Sharpe: {f_1h['sharpe']:.3f} (baseline=0)")


# ---- Entry point ----

def run(df_signals=None, df_prices=None, df_context=None, fee_rate=FEE_RATE_DEFAULT):
    """Run range compression test. Can be called standalone or from analyze.py."""
    from backtesting.analyze import (load_data, derive_directions,
                                     split_is_oos, IS_RATIO)

    if df_signals is None:
        df_signals, df_prices, df_context = load_data()
        df_signals = derive_directions(df_signals)

    fee_pct = fee_rate * 2 * 100  # 0.2%

    # IS/OOS split
    sorted_ts = df_signals["timestamp"].sort_values()
    is_cutoff = sorted_ts.iloc[int(len(sorted_ts) * IS_RATIO)]

    # Merge signals + context
    merged = df_signals.merge(
        df_context, left_on="id", right_on="signal_id",
        how="inner", suffixes=("", "_ctx"),
    )
    mask = (
        merged["derived_direction"].isin(["bullish", "bearish"])
        & ((merged["filled_mask"].astype(int) & 4) > 0)  # at least 1h filled
    )
    merged = merged[mask].sort_values("timestamp").reset_index(drop=True)

    # Sort prices once
    df_prices_sorted = df_prices.sort_values("timestamp").reset_index(drop=True)

    results = {}
    for ch_name in CHANNELS:
        logger.info(f"Analyzing {ch_name}...")
        t1 = time.time()
        ch_merged = merged[merged["channel_name"] == ch_name].copy()
        ch_merged = ch_merged.sort_values("timestamp").reset_index(drop=True)
        results[ch_name] = _analyze_channel(
            ch_name, ch_merged, df_prices_sorted, fee_pct, is_cutoff)
        logger.info(f"  {ch_name} done in {time.time() - t1:.1f}s")

    # Write report
    report_lines = _build_report(results)
    report_path = os.path.join(OUTPUT_DIR, "dmi_range_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    json_path = os.path.join(OUTPUT_DIR, "dmi_range_results.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str, ensure_ascii=False)

    # Print key findings
    print()
    print("=" * 60)
    print("KEY FINDINGS — DMI Range Compression Test")
    print("=" * 60)
    for ch_name in CHANNELS:
        ch = results.get(ch_name, {})
        if ch.get("skipped"):
            print(f"\n{ch_name}: SKIPPED ({ch.get('reason', '')})")
            continue
        print(f"\n{ch_name} ({ch['total_signals']} signals):")

        base = ch.get("baseline", {}).get("1h", {})
        if base.get("trades", 0) > 0:
            print(f"  Baseline 1h: WR={base['win_rate']:.1f}%, "
                  f"avg={base['avg_return']:+.4f}%, Sh={base['sharpe']:.3f}")

        for fkey in sorted(ch.get("filters", {})):
            fd = ch["filters"][fkey]
            if fd.get("insufficient"):
                print(f"  {fkey}: N={fd['n_signals']} (insufficient)")
                continue
            f_1h = fd.get("horizons_all", {}).get("1h", {})
            if f_1h.get("trades", 0) > 0:
                marker = ""
                if (base.get("sharpe", 0) > 0
                        and f_1h.get("sharpe", 0) > base["sharpe"] * 1.2):
                    marker = " <<<< IMPROVEMENT"
                elif f_1h.get("sharpe", 0) > 0 and base.get("sharpe", 0) <= 0:
                    marker = " <<<< EDGE FOUND"
                print(f"  {fkey}: N={fd['n_signals']}, "
                      f"WR={f_1h['win_rate']:.1f}%, "
                      f"avg={f_1h['avg_return']:+.4f}%, "
                      f"Sh={f_1h['sharpe']:.3f}{marker}")

        wf = ch.get("walk_forward", {})
        if wf.get("best_filter") and not wf.get("skipped"):
            ovf_str = ""
            if wf.get("overfitted") is True:
                ovf_str = " [OVERFITTED]"
            elif wf.get("overfitted") is False:
                ovf_str = " [OK]"
            print(f"  Walk-forward best: {wf['best_filter']}{ovf_str}")

    print()
    print(f"Report: {report_path}")
    print(f"JSON:   {json_path}")

    return results


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    t0 = time.time()
    run()
    logger.info(f"Total: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
