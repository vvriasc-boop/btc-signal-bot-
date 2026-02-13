"""
Hypothesis 1: Bid/Ask Imbalance in 5-minute windows predicts price movement.

For each of 7 pairs, count bid vs ask signals per 5-min window.
Test DIRECT (more bids → up) and INVERSE (more bids → down = spoofing).
Walk-forward 70/30.
"""
import logging
import numpy as np
import pandas as pd

from tools.orderbook_config import (
    PAIRS, H1_WINDOW_SEC, H1_THRESHOLDS, H1_HORIZONS,
    FEE_PCT, IS_RATIO, MIN_TRADES, ANN_FACTOR,
)

logger = logging.getLogger("orderbook.h1")


# ---- Metrics ----

def _sharpe(r):
    if len(r) < 2 or np.std(r) == 0:
        return 0.0
    return round(float(np.mean(r) / np.std(r) * np.sqrt(ANN_FACTOR)), 4)


def _pf(r):
    g, l = float(r[r > 0].sum()), abs(float(r[r < 0].sum()))
    return 99.0 if l == 0 and g > 0 else (0.0 if l == 0 else round(g / l, 3))


def _stats(rets):
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


# ---- Core: compute imbalance in 5-min windows ----

def _compute_imbalance(bid_ts, ask_ts, window_sec=H1_WINDOW_SEC):
    """Compute bid-ask imbalance per 5-min window.

    Args:
        bid_ts: epoch seconds (int64) for bid signals, sorted
        ask_ts: epoch seconds (int64) for ask signals, sorted

    Returns:
        bin_starts: int64 array of window start times
        imbalances: int array (bid_count - ask_count)
    """
    if len(bid_ts) == 0 and len(ask_ts) == 0:
        return np.array([], dtype=np.int64), np.array([], dtype=int)

    all_ts = np.concatenate([bid_ts, ask_ts])
    t_min = all_ts.min() // window_sec * window_sec
    t_max = all_ts.max() // window_sec * window_sec + window_sec

    bins = np.arange(t_min, t_max, window_sec, dtype=np.int64)
    if len(bins) == 0:
        return np.array([], dtype=np.int64), np.array([], dtype=int)

    bid_counts = np.zeros(len(bins), dtype=int)
    ask_counts = np.zeros(len(bins), dtype=int)

    if len(bid_ts) > 0:
        bid_bins = (bid_ts - t_min) // window_sec
        bid_bins = np.clip(bid_bins, 0, len(bins) - 1)
        np.add.at(bid_counts, bid_bins.astype(int), 1)

    if len(ask_ts) > 0:
        ask_bins = (ask_ts - t_min) // window_sec
        ask_bins = np.clip(ask_bins, 0, len(bins) - 1)
        np.add.at(ask_counts, ask_bins.astype(int), 1)

    imbalances = bid_counts - ask_counts
    return bins, imbalances


def _compute_returns(bin_starts, price_ts, price_vals, fee_pct):
    """Compute forward returns at each horizon for each bin.

    Returns: {horizon_name: float64 array of net returns}
    """
    result = {}
    for hz_name, offset_min in H1_HORIZONS.items():
        entry_idx = np.searchsorted(price_ts, bin_starts, side="left")
        entry_idx = np.clip(entry_idx, 0, len(price_vals) - 1)
        entry_prices = price_vals[entry_idx]

        target_ts = bin_starts + offset_min * 60
        target_idx = np.searchsorted(price_ts, target_ts, side="left")
        target_idx = np.clip(target_idx, 0, len(price_vals) - 1)
        target_prices = price_vals[target_idx]

        # Valid only if target is close to expected time
        valid = np.abs(price_ts[target_idx] - target_ts) < 120
        raw_pct = np.where(
            (entry_prices > 0) & valid,
            (target_prices - entry_prices) / entry_prices * 100,
            np.nan,
        )
        result[hz_name] = raw_pct
    return result


# ---- Test one threshold+mode combo ----

def _test_combo(imbalances, returns_dict, threshold, mode, fee_pct):
    """Test one threshold+mode combination.

    mode='direct': imbalance>threshold → bullish, imbalance<-threshold → bearish
    mode='inverse': opposite (spoofing hypothesis)
    """
    result = {}
    for hz_name, raw_pct in returns_dict.items():
        bullish_mask = imbalances >= threshold
        bearish_mask = imbalances <= -threshold
        active = bullish_mask | bearish_mask

        if active.sum() < 2:
            result[hz_name] = _stats(np.array([]))
            continue

        if mode == "direct":
            signs = np.where(bullish_mask, 1.0, np.where(bearish_mask, -1.0, 0.0))
        else:  # inverse
            signs = np.where(bullish_mask, -1.0, np.where(bearish_mask, 1.0, 0.0))

        directed = raw_pct[active] * signs[active]
        net = directed - fee_pct
        net = net[~np.isnan(net)]
        result[hz_name] = _stats(net)

    return result


# ---- Walk-forward for one pair ----

def _walk_forward(imbalances, bin_starts, returns_dict, fee_pct, split_sec):
    """Grid search on IS, validate on OOS.
    Returns best combo stats.
    """
    is_mask = bin_starts <= split_sec
    oos_mask = ~is_mask

    best_key = None
    best_sharpe = -np.inf
    all_combos = []

    for threshold in H1_THRESHOLDS:
        for mode in ["direct", "inverse"]:
            # IS
            is_imb = imbalances[is_mask]
            is_returns = {h: r[is_mask] for h, r in returns_dict.items()}
            is_stats = _test_combo(is_imb, is_returns, threshold, mode, fee_pct)
            is_1h = is_stats.get("1h", {})

            combo = {
                "threshold": threshold, "mode": mode,
                "is": is_stats, "oos": {},
            }

            if is_1h.get("trades", 0) >= MIN_TRADES:
                sh = is_1h["sharpe"]
                if sh > best_sharpe:
                    best_sharpe = sh
                    best_key = (threshold, mode)

                # OOS
                oos_imb = imbalances[oos_mask]
                oos_returns = {h: r[oos_mask] for h, r in returns_dict.items()}
                oos_stats = _test_combo(oos_imb, oos_returns, threshold, mode, fee_pct)
                combo["oos"] = oos_stats

            all_combos.append(combo)

    if best_key is None:
        return {"skipped": True, "reason": "no IS combo with enough trades"}

    # Overfitting check
    best_combo = next(c for c in all_combos
                      if c["threshold"] == best_key[0] and c["mode"] == best_key[1])
    oos_1h = best_combo.get("oos", {}).get("1h", {})
    is_1h = best_combo["is"].get("1h", {})
    ovf = (is_1h.get("sharpe", 0) > 0
           and oos_1h.get("sharpe", 0) < is_1h.get("sharpe", 0) * 0.5)

    return {
        "best_params": {"threshold": best_key[0], "mode": best_key[1]},
        "is_1h": is_1h,
        "oos_1h": oos_1h,
        "overfitted": ovf,
        "all_combos": all_combos,
    }


# ---- Analyze one pair ----

def _analyze_pair(pair_name, bid_df, ask_df, price_ts, price_vals, fee_pct, split_sec):
    """Full analysis for one bid/ask pair."""
    bid_ts = bid_df["timestamp"].values.astype("int64") // 10**9
    ask_ts = ask_df["timestamp"].values.astype("int64") // 10**9

    if len(bid_ts) == 0 and len(ask_ts) == 0:
        return {"skipped": True, "reason": "no signals"}

    bins, imbalances = _compute_imbalance(bid_ts, ask_ts)
    if len(bins) == 0:
        return {"skipped": True, "reason": "no windows"}

    returns_dict = _compute_returns(bins, price_ts, price_vals, fee_pct)

    # Full data stats per threshold+mode
    full_results = {}
    for threshold in H1_THRESHOLDS:
        for mode in ["direct", "inverse"]:
            key = f"thr{threshold}_{mode}"
            full_results[key] = _test_combo(imbalances, returns_dict,
                                            threshold, mode, fee_pct)

    # Walk-forward
    wf = _walk_forward(imbalances, bins, returns_dict, fee_pct, split_sec)

    return {
        "bid_signals": int(len(bid_ts)),
        "ask_signals": int(len(ask_ts)),
        "total_windows": int(len(bins)),
        "active_windows": int((imbalances != 0).sum()),
        "results": full_results,
        "walk_forward": wf,
    }


# ---- Entry point ----

def run(df_signals, df_prices, fee_rate=0.001):
    """Run Hypothesis 1 analysis.

    Args:
        df_signals: DataFrame with channel_name, timestamp, signal_direction
        df_prices: DataFrame with timestamp, price
        fee_rate: float

    Returns:
        dict with per-pair + aggregate results
    """
    fee_pct = fee_rate * 2 * 100

    # Price arrays
    dfp = df_prices.sort_values("timestamp").reset_index(drop=True)
    price_ts = dfp["timestamp"].values.astype("int64") // 10**9
    price_vals = dfp["price"].values.astype(float)

    # IS/OOS split
    sorted_ts = df_signals["timestamp"].sort_values()
    if len(sorted_ts) < 10:
        return {"skipped": True, "reason": "too few signals"}
    split_ts = sorted_ts.iloc[int(len(sorted_ts) * IS_RATIO)]
    split_sec = int(split_ts.value // 10**9)

    results = {"metadata": {
        "total_signals": len(df_signals),
        "split_timestamp": str(split_ts),
    }}

    # Per-pair analysis
    all_bid_dfs = []
    all_ask_dfs = []

    for bid_title, ask_title, pair_label in PAIRS:
        bid_df = df_signals[df_signals["channel_name"] == bid_title].copy()
        ask_df = df_signals[df_signals["channel_name"] == ask_title].copy()

        logger.info(f"H1 pair {pair_label}: {len(bid_df)} bids, {len(ask_df)} asks")

        results[pair_label] = _analyze_pair(
            pair_label, bid_df, ask_df, price_ts, price_vals, fee_pct, split_sec)

        all_bid_dfs.append(bid_df)
        all_ask_dfs.append(ask_df)

    # Aggregate across all pairs
    if all_bid_dfs and all_ask_dfs:
        all_bid = pd.concat(all_bid_dfs).sort_values("timestamp")
        all_ask = pd.concat(all_ask_dfs).sort_values("timestamp")
        logger.info(f"H1 aggregate: {len(all_bid)} bids, {len(all_ask)} asks")
        results["Aggregate"] = _analyze_pair(
            "Aggregate", all_bid, all_ask, price_ts, price_vals, fee_pct, split_sec)

    return results
