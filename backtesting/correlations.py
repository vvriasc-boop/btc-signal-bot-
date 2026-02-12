"""Inter-channel correlation: temporal and return-based."""
import numpy as np
import pandas as pd
from itertools import combinations


def run(df_signals, df_prices, df_context, fee_rate=0.001):
    fee_pct = fee_rate * 2 * 100
    channels = sorted(df_signals["channel_name"].unique())
    if len(channels) < 2:
        return {}

    temporal = _temporal_correlation(df_signals, channels)
    ret_corr = _return_correlation(df_signals, df_context, channels, fee_pct)
    pairs = list(combinations(channels, 2))

    temp_vals = [temporal.get(f"{a}_{b}", 0) for a, b in pairs]
    most_corr_idx = int(np.argmax(np.abs(temp_vals))) if temp_vals else 0
    least_corr_idx = int(np.argmin(np.abs(temp_vals))) if temp_vals else 0
    avg_abs = float(np.mean(np.abs(temp_vals))) if temp_vals else 0

    return {
        "temporal_correlation": temporal,
        "return_correlation": ret_corr,
        "most_correlated_pair": {
            "pair": f"{pairs[most_corr_idx][0]}_{pairs[most_corr_idx][1]}",
            "correlation": round(temp_vals[most_corr_idx], 4),
        } if pairs else {},
        "least_correlated_pair": {
            "pair": f"{pairs[least_corr_idx][0]}_{pairs[least_corr_idx][1]}",
            "correlation": round(temp_vals[least_corr_idx], 4),
        } if pairs else {},
        "diversification_score": round(1 - avg_abs, 4),
    }


def _temporal_correlation(df_signals, channels):
    """Bin signals into 1h windows, build binary presence matrix, correlate."""
    df = df_signals.copy()
    df["hour_bin"] = df["timestamp"].dt.floor("h")
    pivot = pd.crosstab(df["hour_bin"], df["channel_name"]).clip(upper=1)
    for ch in channels:
        if ch not in pivot.columns:
            pivot[ch] = 0
    pivot = pivot[channels]

    result = {}
    for a, b in combinations(channels, 2):
        if a in pivot.columns and b in pivot.columns:
            corr = pivot[a].corr(pivot[b])
            result[f"{a}_{b}"] = round(float(corr), 4) if not np.isnan(corr) else 0.0
    return result


def _return_correlation(df_signals, df_context, channels, fee_pct):
    """Correlate 1h returns between channels in overlapping time bins."""
    merged = df_signals.merge(
        df_context, left_on="id", right_on="signal_id", how="inner",
        suffixes=("", "_ctx"),
    )
    mask = (
        merged["derived_direction"].isin(["bullish", "bearish"])
        & merged["change_1h_pct"].notna()
        & ((merged["filled_mask"].astype(int) & 4) > 0)
    )
    df = merged[mask].copy()
    sign = df["derived_direction"].map({"bullish": 1.0, "bearish": -1.0})
    df["net_return"] = df["change_1h_pct"] * sign - fee_pct
    df["hour_bin"] = df["timestamp"].dt.floor("h")

    avg_ret = df.groupby(["hour_bin", "channel_name"])["net_return"].mean().unstack()

    result = {}
    for a, b in combinations(channels, 2):
        if a in avg_ret.columns and b in avg_ret.columns:
            both = avg_ret[[a, b]].dropna()
            if len(both) < 10:
                continue
            corr = both[a].corr(both[b])
            result[f"{a}_{b}"] = round(float(corr), 4) if not np.isnan(corr) else 0.0
    return result
