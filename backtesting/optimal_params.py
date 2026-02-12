"""Grid search for optimal TP/SL with walk-forward validation."""
import numpy as np
import pandas as pd

TP_RANGE = np.arange(0.2, 3.1, 0.2)
SL_RANGE = np.arange(0.2, 3.1, 0.2)
THRESHOLD_RANGE = np.arange(30, 81, 5)
MAX_HOLD = 1440  # 24h timeout
OVERFITTING_RATIO = 0.50
MIN_SIGNALS_WF = 100

DIRECTION_THRESHOLDS_CHANNELS = {"AltSwing", "Scalp17", "SellsPowerIndex", "AltSPI"}


def run(df_signals, df_prices, df_context,
        df_sig_is, df_sig_oos, df_ctx_is, df_ctx_oos, fee_rate=0.001):
    fee_pct = fee_rate * 2 * 100
    price_ts, prices = _prepare_prices(df_prices)
    result = {}

    for ch in sorted(df_signals["channel_name"].unique()):
        is_sigs = df_sig_is[df_sig_is["channel_name"] == ch]
        oos_sigs = df_sig_oos[df_sig_oos["channel_name"] == ch]
        if len(is_sigs) < MIN_SIGNALS_WF:
            result[ch] = {"skipped": True, "reason": f"IS signals < {MIN_SIGNALS_WF}"}
            continue

        has_threshold = ch in DIRECTION_THRESHOLDS_CHANNELS
        best = _grid_search_channel(
            is_sigs, price_ts, prices, fee_pct, has_threshold
        )
        if best is None:
            result[ch] = {"skipped": True, "reason": "no valid params"}
            continue

        oos_stats = _evaluate_params(
            oos_sigs, price_ts, prices, fee_pct,
            best["tp"], best["sl"], best.get("threshold"),
        )
        overfitted = False
        if best["sharpe"] > 0 and oos_stats["sharpe"] < best["sharpe"] * OVERFITTING_RATIO:
            overfitted = True

        result[ch] = {
            "best_params_is": {
                "tp_pct": round(float(best["tp"]), 2),
                "sl_pct": round(float(best["sl"]), 2),
                "threshold": round(float(best["threshold"]), 1) if best.get("threshold") else None,
            },
            "is_sharpe": best["sharpe"],
            "is_profit_factor": best["pf"],
            "is_total_return_pct": best["total_ret"],
            "is_trades": best["trades"],
            "oos_sharpe": oos_stats["sharpe"],
            "oos_profit_factor": oos_stats["pf"],
            "oos_total_return_pct": oos_stats["total_ret"],
            "oos_trades": oos_stats["trades"],
            "overfitted": overfitted,
            "sharpe_ratio_oos_is": round(
                oos_stats["sharpe"] / best["sharpe"], 3
            ) if best["sharpe"] > 0 else 0,
        }
    return result


def _prepare_prices(df_prices):
    ts = (df_prices["timestamp"].astype(np.int64) // 10**9).values.astype(np.int64)
    prices = df_prices["price"].values.astype(np.float64)
    order = np.argsort(ts)
    return ts[order], prices[order]


def _build_pct_matrix(sig_ts, price_ts, prices):
    """Build raw % change matrix (n_signals, MAX_HOLD). NaN for overflow."""
    m = len(prices)
    entry_idx = np.searchsorted(price_ts, sig_ts, side="left")
    entry_idx = np.clip(entry_idx, 0, m - 1)
    entry_prices = prices[entry_idx]

    offsets = np.arange(MAX_HOLD)
    idx_2d = entry_idx[:, None] + offsets[None, :]
    overflow = idx_2d >= m
    idx_2d = np.clip(idx_2d, 0, m - 1)

    price_2d = prices[idx_2d]
    ep = entry_prices[:, None]
    with np.errstate(divide="ignore", invalid="ignore"):
        raw_pct = np.where(ep > 0, (price_2d - ep) / ep * 100, np.nan)
    raw_pct[overflow] = np.nan
    return raw_pct, entry_prices


def _grid_search_channel(sigs, price_ts, prices, fee_pct, has_threshold):
    """Grid search over TP/SL (and optionally threshold) on IS data."""
    best = None
    thresholds = THRESHOLD_RANGE if has_threshold else [None]

    all_ts = (sigs["timestamp"].astype(np.int64) // 10**9).values.astype(np.int64)
    all_vals = sigs["indicator_value"].values if has_threshold else None
    all_dirs = sigs["derived_direction"].values

    raw_pct, entry_prices = _build_pct_matrix(all_ts, price_ts, prices)
    valid_entry = entry_prices > 0

    for thresh in thresholds:
        if thresh is not None:
            dirs = np.full(len(sigs), "neutral", dtype=object)
            dirs[all_vals >= thresh] = "bullish"
            dirs[all_vals <= (100 - thresh)] = "bearish"
        else:
            dirs = all_dirs

        mask = ((dirs == "bullish") | (dirs == "bearish")) & valid_entry
        if mask.sum() < 20:
            continue

        signs = np.where(dirs[mask] == "bullish", 1.0, -1.0)
        directed = raw_pct[mask] * signs[:, None]
        best = _search_tpsl(directed, fee_pct, thresh, best)
    return best


def _search_tpsl(directed, fee_pct, thresh, best):
    """Vectorized TP/SL grid search on directed pct matrix."""
    n = directed.shape[0]
    valid_counts = np.sum(~np.isnan(directed), axis=1)
    last_idx = np.clip(valid_counts - 1, 0, MAX_HOLD - 1).astype(int)
    final_rets = directed[np.arange(n), last_idx]
    final_rets = np.where(np.isnan(final_rets), 0.0, final_rets)

    # Precompute SL first-hit indices for all SL values
    sl_firsts = {}
    for sl in SL_RANGE:
        sl_bool = directed <= -sl
        sl_any = sl_bool.any(axis=1)
        sl_firsts[round(float(sl), 2)] = np.where(
            sl_any, np.argmax(sl_bool, axis=1), MAX_HOLD
        )

    for tp in TP_RANGE:
        tp_bool = directed >= tp
        tp_any = tp_bool.any(axis=1)
        tp_first = np.where(tp_any, np.argmax(tp_bool, axis=1), MAX_HOLD)

        for sl in SL_RANGE:
            sl_first = sl_firsts[round(float(sl), 2)]
            tp_wins = (tp_first <= sl_first) & (tp_first < MAX_HOLD)
            sl_hits = (sl_first < tp_first) & (sl_first < MAX_HOLD)

            rets = np.where(
                tp_wins, tp - fee_pct,
                np.where(sl_hits, -sl - fee_pct, final_rets - fee_pct),
            )
            if len(rets) < 10:
                continue
            sharpe = _sharpe(rets)
            if best is None or sharpe > best["sharpe"]:
                best = {
                    "tp": tp, "sl": sl, "threshold": thresh,
                    "sharpe": sharpe, "pf": _pf(rets),
                    "total_ret": round(float(np.sum(rets)), 2),
                    "trades": len(rets),
                }
    return best


def _evaluate_params(sigs, price_ts, prices, fee_pct, tp, sl, threshold):
    """Evaluate specific params on OOS data."""
    df_dir = _apply_threshold(sigs, threshold) if threshold is not None else sigs
    directional = df_dir[df_dir["derived_direction"].isin(["bullish", "bearish"])]
    if len(directional) < 5:
        return {"sharpe": 0, "pf": 0, "total_ret": 0, "trades": 0}

    sig_ts = (directional["timestamp"].astype(np.int64) // 10**9).values.astype(np.int64)
    sig_dirs = directional["derived_direction"].map(
        {"bullish": 1.0, "bearish": -1.0}
    ).values

    raw_pct, entry_prices = _build_pct_matrix(sig_ts, price_ts, prices)
    valid = entry_prices > 0
    if valid.sum() < 5:
        return {"sharpe": 0, "pf": 0, "total_ret": 0, "trades": 0}

    directed = raw_pct[valid] * sig_dirs[valid, None]
    n = len(directed)
    valid_counts = np.sum(~np.isnan(directed), axis=1)
    last_idx = np.clip(valid_counts - 1, 0, MAX_HOLD - 1).astype(int)
    final_rets = directed[np.arange(n), last_idx]
    final_rets = np.where(np.isnan(final_rets), 0.0, final_rets)

    tp_bool = directed >= tp
    sl_bool = directed <= -sl
    tp_first = np.where(tp_bool.any(axis=1), np.argmax(tp_bool, axis=1), MAX_HOLD)
    sl_first = np.where(sl_bool.any(axis=1), np.argmax(sl_bool, axis=1), MAX_HOLD)
    tp_wins = (tp_first <= sl_first) & (tp_first < MAX_HOLD)
    sl_hits = (sl_first < tp_first) & (sl_first < MAX_HOLD)

    rets = np.where(
        tp_wins, tp - fee_pct,
        np.where(sl_hits, -sl - fee_pct, final_rets - fee_pct),
    )
    return {
        "sharpe": _sharpe(rets),
        "pf": _pf(rets),
        "total_ret": round(float(np.sum(rets)), 2),
        "trades": len(rets),
    }


def _apply_threshold(sigs, threshold):
    """Apply threshold to derive direction for value-only channels."""
    df = sigs.copy()
    vals = df["indicator_value"]
    df.loc[vals >= threshold, "derived_direction"] = "bullish"
    df.loc[vals <= (100 - threshold), "derived_direction"] = "bearish"
    df.loc[(vals > (100 - threshold)) & (vals < threshold), "derived_direction"] = "neutral"
    return df


def _sharpe(rets):
    if len(rets) < 2 or np.std(rets) == 0:
        return 0.0
    return round(float(np.mean(rets) / np.std(rets) * np.sqrt(8760)), 4)


def _pf(rets):
    gains = rets[rets > 0].sum()
    losses = abs(rets[rets < 0].sum())
    if losses == 0:
        return 99.0 if gains > 0 else 0.0
    return round(float(gains / losses), 3)
