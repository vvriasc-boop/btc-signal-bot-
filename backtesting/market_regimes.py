"""Market regime detection: volatility and trend classification."""
import numpy as np
import pandas as pd


def run(df_signals, df_prices, df_context, fee_rate=0.001):
    fee_pct = fee_rate * 2 * 100
    regimes = _build_regime_series(df_prices)
    merged = _merge_with_regimes(df_signals, df_context, regimes, fee_pct)
    if len(merged) == 0:
        return {}

    vol_stats = _stats_by_group(merged, "vol_regime")
    trend_stats = _stats_by_group(merged, "trend_regime")
    per_ch = _per_channel_by_regime(merged)

    return {
        "volatility_regimes": vol_stats,
        "trend_regimes": trend_stats,
        "per_channel_by_vol_regime": per_ch,
    }


def _build_regime_series(df_prices):
    """Classify each minute into volatility and trend regimes."""
    df = df_prices[["timestamp", "price"]].copy().sort_values("timestamp")
    df = df.set_index("timestamp")

    returns_1m = df["price"].pct_change()
    vol_24h = returns_1m.rolling(1440, min_periods=720).std() * 100
    vol_terciles = pd.qcut(vol_24h.dropna(), 3, labels=["low", "medium", "high"])
    df["vol_regime"] = vol_terciles

    trend_4h = df["price"].pct_change(240) * 100
    df["trend_regime"] = pd.cut(
        trend_4h,
        bins=[-np.inf, -1.0, 0.0, 1.0, np.inf],
        labels=["strong_down", "mild_down", "mild_up", "strong_up"],
    )
    return df[["vol_regime", "trend_regime"]].reset_index()


def _merge_with_regimes(df_signals, df_context, regimes, fee_pct):
    """Merge signals with context and regime at signal time."""
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

    regimes_sorted = regimes.sort_values("timestamp")
    df = df.sort_values("timestamp")
    df = pd.merge_asof(
        df, regimes_sorted, on="timestamp", direction="backward",
    )
    return df


def _stats_by_group(df, col):
    """Compute stats grouped by regime column."""
    result = {}
    for regime, grp in df.groupby(col, observed=True):
        rets = grp["net_return"].dropna().values
        if len(rets) == 0:
            continue
        result[str(regime)] = {
            "signal_count": len(rets),
            "avg_return_1h_net": round(float(np.mean(rets)), 4),
            "win_rate_1h_pct": round(float((rets > 0).mean() * 100), 1),
            "total_return_pct": round(float(np.sum(rets)), 2),
        }
    return result


def _per_channel_by_regime(df):
    """Per-channel stats broken down by volatility regime."""
    result = {}
    for ch, ch_grp in df.groupby("channel_name"):
        ch_result = {}
        for regime, grp in ch_grp.groupby("vol_regime", observed=True):
            rets = grp["net_return"].dropna().values
            if len(rets) < 5:
                continue
            ch_result[str(regime)] = {
                "count": len(rets),
                "avg_return": round(float(np.mean(rets)), 4),
                "win_rate_pct": round(float((rets > 0).mean() * 100), 1),
            }
        if ch_result:
            result[ch] = ch_result
    return result
