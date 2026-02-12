"""Per-channel performance statistics: win rate, returns, Sharpe, Sortino, PF."""
import numpy as np
import pandas as pd

HORIZONS = {
    "5m":  ("change_5m_pct", 1, 105120),
    "15m": ("change_15m_pct", 2, 35040),
    "1h":  ("change_1h_pct", 4, 8760),
    "4h":  ("change_4h_pct", 8, 2190),
    "24h": ("change_24h_pct", 16, 365),
}
ROUND_TRIP_MULT = 2  # fee applied on entry + exit


def run(df_signals, df_prices, df_context, fee_rate=0.001):
    fee_pct = fee_rate * ROUND_TRIP_MULT * 100  # 0.2%
    merged = df_signals.merge(
        df_context, left_on="id", right_on="signal_id", how="inner",
        suffixes=("", "_ctx"),
    )
    result = {}
    for ch_name, grp in merged.groupby("channel_name"):
        result[ch_name] = _channel_stats(grp, fee_pct)
    return result


def _channel_stats(grp, fee_pct):
    """Compute stats for one channel across all horizons."""
    directional = grp[grp["derived_direction"].isin(["bullish", "bearish"])].copy()
    stats = {
        "total_signals": len(grp),
        "bullish": int((grp["derived_direction"] == "bullish").sum()),
        "bearish": int((grp["derived_direction"] == "bearish").sum()),
        "neutral": int((grp["derived_direction"] == "neutral").sum()),
        "no_direction": int(grp["derived_direction"].isna().sum()),
        "horizons": {},
    }
    if len(directional) == 0:
        return stats

    dir_sign = directional["derived_direction"].map(
        {"bullish": 1.0, "bearish": -1.0}
    )

    for hz_name, (col, mask_bit, ann_factor) in HORIZONS.items():
        valid = directional["filled_mask"].values.astype(int) & mask_bit > 0
        valid &= directional[col].notna().values
        if valid.sum() < 2:
            continue
        raw_ret = directional.loc[valid, col].values
        signs = dir_sign.loc[valid].values
        gross = raw_ret * signs
        net = gross - fee_pct
        stats["horizons"][hz_name] = _horizon_stats(gross, net, ann_factor)
    return stats


def _horizon_stats(gross, net, ann_factor):
    """Compute metrics for one horizon."""
    return {
        "trades": int(len(gross)),
        "win_rate_gross_pct": _win_rate(gross),
        "win_rate_net_pct": _win_rate(net),
        "avg_return_gross_pct": round(float(np.mean(gross)), 4),
        "avg_return_net_pct": round(float(np.mean(net)), 4),
        "median_return_net_pct": round(float(np.median(net)), 4),
        "total_return_net_pct": round(float(np.sum(net)), 2),
        "profit_factor_gross": _profit_factor(gross),
        "profit_factor_net": _profit_factor(net),
        "sharpe_gross": _sharpe(gross, ann_factor),
        "sharpe_net": _sharpe(net, ann_factor),
        "sortino_net": _sortino(net, ann_factor),
    }


def _win_rate(returns):
    if len(returns) == 0:
        return 0.0
    return round(float((returns > 0).sum() / len(returns) * 100), 2)


def _profit_factor(returns):
    gains = returns[returns > 0].sum()
    losses = abs(returns[returns < 0].sum())
    if losses == 0:
        return float("inf") if gains > 0 else 0.0
    return round(float(gains / losses), 3)


def _sharpe(returns, ann_factor):
    if len(returns) < 2 or np.std(returns) == 0:
        return 0.0
    return round(float(np.mean(returns) / np.std(returns) * np.sqrt(ann_factor)), 4)


def _sortino(returns, ann_factor):
    if len(returns) < 2:
        return 0.0
    downside = returns[returns < 0]
    if len(downside) == 0:
        return float("inf") if np.mean(returns) > 0 else 0.0
    dd = np.sqrt(np.mean(downside ** 2))
    if dd == 0:
        return 0.0
    return round(float(np.mean(returns) / dd * np.sqrt(ann_factor)), 4)
