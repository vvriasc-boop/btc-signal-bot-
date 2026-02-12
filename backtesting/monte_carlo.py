"""Monte Carlo significance testing: shuffle directions and timestamps."""
import numpy as np
import pandas as pd

N_SHUFFLES = 1000
MIN_SIGNALS = 50


def run(df_signals, df_prices, df_context, fee_rate=0.001):
    fee_pct = fee_rate * 2 * 100
    merged = _prepare(df_signals, df_context, fee_pct)
    if len(merged) == 0:
        return {}

    dir_results = _direction_shuffle(merged)
    ts_results = _timestamp_shuffle(merged, df_prices, fee_pct)

    sig_count = sum(1 for v in dir_results.values() if v.get("significant_5pct"))
    total = len(dir_results)
    if sig_count > total * 0.7:
        verdict = "statistically_significant"
    elif sig_count > total * 0.3:
        verdict = "mixed"
    else:
        verdict = "not_significant"

    return {
        "direction_shuffle": dir_results,
        "timestamp_shuffle": ts_results,
        "overall_verdict": verdict,
    }


def _prepare(df_signals, df_context, fee_pct):
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
    df["raw_return"] = df["change_1h_pct"].values
    df["dir_sign"] = sign.values
    df["net_return"] = df["raw_return"] * df["dir_sign"] - fee_pct
    return df


def _direction_shuffle(df):
    """Shuffle direction labels, keep raw returns and timestamps."""
    rng = np.random.default_rng(42)
    result = {}

    for ch, grp in df.groupby("channel_name"):
        if len(grp) < MIN_SIGNALS:
            continue
        raw = grp["raw_return"].values
        signs = grp["dir_sign"].values
        actual_rets = raw * signs
        actual_sharpe = _sharpe(actual_rets)

        shuffled_sharpes = np.zeros(N_SHUFFLES)
        for i in range(N_SHUFFLES):
            perm_signs = rng.permutation(signs)
            shuffled_rets = raw * perm_signs
            shuffled_sharpes[i] = _sharpe(shuffled_rets)

        p_value = float((shuffled_sharpes >= actual_sharpe).mean())
        z = _z_score(actual_sharpe, shuffled_sharpes)

        result[ch] = {
            "actual_sharpe": round(actual_sharpe, 4),
            "random_sharpe_mean": round(float(shuffled_sharpes.mean()), 4),
            "random_sharpe_std": round(float(shuffled_sharpes.std()), 4),
            "p_value": round(p_value, 4),
            "z_score": round(z, 3),
            "significant_5pct": bool(p_value < 0.05),
            "significant_1pct": bool(p_value < 0.01),
            "trades": len(grp),
        }
    return result


def _timestamp_shuffle(df, df_prices, fee_pct):
    """Shuffle entry timestamps: assign random prices to check timing value."""
    rng = np.random.default_rng(123)
    result = {}

    price_vals = df_prices["price"].values
    n_prices = len(price_vals)
    if n_prices < 100:
        return {}

    for ch, grp in df.groupby("channel_name"):
        if len(grp) < MIN_SIGNALS:
            continue
        n_sigs = len(grp)
        signs = grp["dir_sign"].values
        actual_mean = float(grp["net_return"].mean())

        shuffled_means = np.zeros(N_SHUFFLES)
        for i in range(N_SHUFFLES):
            rand_idx = rng.integers(0, n_prices - 60, size=n_sigs)
            entry_p = price_vals[rand_idx]
            exit_p = price_vals[rand_idx + 60]
            valid = entry_p > 0
            raw_pct = np.where(valid, (exit_p - entry_p) / entry_p * 100, 0)
            net = raw_pct * signs - fee_pct
            shuffled_means[i] = net.mean()

        p_value = float((shuffled_means >= actual_mean).mean())
        result[ch] = {
            "actual_avg_return": round(actual_mean, 4),
            "random_avg_return_mean": round(float(shuffled_means.mean()), 4),
            "p_value": round(p_value, 4),
            "significant_5pct": bool(p_value < 0.05),
            "trades": len(grp),
        }
    return result


def _sharpe(rets):
    if len(rets) < 2 or np.std(rets) == 0:
        return 0.0
    return float(np.mean(rets) / np.std(rets))


def _z_score(actual, distribution):
    std = float(distribution.std())
    if std == 0:
        return 0.0
    return float((actual - distribution.mean()) / std)
