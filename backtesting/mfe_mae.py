"""Maximum Favorable / Adverse Excursion — numpy vectorized."""
import numpy as np
import pandas as pd

HOLD_PERIODS = {"1h": 60, "24h": 1440}
CHUNK_SIZES = {60: 5000, 1440: 2000}


def run(df_signals, df_prices, df_context, fee_rate=0.001):
    price_ts, prices = _prepare_price_arrays(df_prices)
    directional = df_signals[
        df_signals["derived_direction"].isin(["bullish", "bearish"])
    ].copy()
    if len(directional) == 0:
        return {}

    sig_ts = _to_unix(directional["timestamp"])
    sig_dirs = directional["derived_direction"].map(
        {"bullish": 1, "bearish": -1}
    ).values.astype(np.int8)

    result = {"per_channel": {}, "overall": {}}
    for label, hold in HOLD_PERIODS.items():
        mfe, mae = _compute_mfe_mae(sig_ts, sig_dirs, price_ts, prices, hold)
        directional[f"mfe_{label}"] = mfe
        directional[f"mae_{label}"] = mae

    for ch_name, grp in directional.groupby("channel_name"):
        ch_result = {}
        for label in HOLD_PERIODS:
            mfe_col, mae_col = grp[f"mfe_{label}"].values, grp[f"mae_{label}"].values
            valid = np.isfinite(mfe_col) & np.isfinite(mae_col)
            if valid.sum() < 5:
                continue
            ch_result[label] = _summarize_mfe_mae(mfe_col[valid], mae_col[valid])
        result["per_channel"][ch_name] = ch_result

    for label in HOLD_PERIODS:
        mfe_all = directional[f"mfe_{label}"].dropna().values
        mae_all = directional[f"mae_{label}"].dropna().values
        if len(mfe_all) > 0:
            result["overall"][label] = _summarize_mfe_mae(mfe_all, mae_all)
    return result


def _prepare_price_arrays(df_prices):
    """Convert price DataFrame to sorted numpy arrays (unix_ts, price)."""
    ts = _to_unix(df_prices["timestamp"])
    prices = df_prices["price"].values.astype(np.float64)
    order = np.argsort(ts)
    return ts[order], prices[order]


def _to_unix(series):
    """Convert pd.Timestamp series to int64 Unix seconds."""
    return (series.astype(np.int64) // 10**9).values.astype(np.int64)


def _compute_mfe_mae(sig_ts, sig_dirs, price_ts, prices, hold):
    """Vectorized MFE/MAE for all signals."""
    n = len(sig_ts)
    m = len(prices)
    chunk = CHUNK_SIZES.get(hold, 5000)
    all_mfe = np.full(n, np.nan)
    all_mae = np.full(n, np.nan)

    entry_idx = np.searchsorted(price_ts, sig_ts, side="left")
    entry_idx = np.clip(entry_idx, 0, m - 1)

    for start in range(0, n, chunk):
        end = min(start + chunk, n)
        _chunk_mfe_mae(
            entry_idx[start:end], sig_dirs[start:end],
            prices, m, hold, all_mfe, all_mae, start,
        )
    return all_mfe, all_mae


def _chunk_mfe_mae(chunk_idx, chunk_dirs, prices, m, hold,
                    out_mfe, out_mae, offset):
    """Process one chunk of signals for MFE/MAE."""
    offsets = np.arange(hold, dtype=np.int64)
    indices = chunk_idx[:, None] + offsets[None, :]
    indices = np.clip(indices, 0, m - 1)

    windows = prices[indices]
    entries = prices[chunk_idx]

    mask_valid = entries > 0
    pct = np.where(
        mask_valid[:, None],
        (windows - entries[:, None]) / entries[:, None] * 100.0,
        np.nan,
    )
    directed = pct * chunk_dirs[:, None]

    sz = len(chunk_idx)
    out_mfe[offset:offset + sz] = np.nanmax(directed, axis=1)
    out_mae[offset:offset + sz] = np.nanmin(directed, axis=1)


def _summarize_mfe_mae(mfe, mae):
    """Summary statistics for MFE/MAE arrays."""
    avg_mae = float(np.mean(np.abs(mae)))
    return {
        "avg_mfe_pct": round(float(np.mean(mfe)), 4),
        "avg_mae_pct": round(float(np.mean(mae)), 4),
        "median_mfe_pct": round(float(np.median(mfe)), 4),
        "median_mae_pct": round(float(np.median(mae)), 4),
        "mfe_mae_ratio": round(float(np.mean(mfe)) / avg_mae, 3) if avg_mae > 0 else 0,
        "pct_mfe_gt_0_5": round(float((mfe > 0.5).mean() * 100), 1),
        "pct_mae_lt_neg_0_5": round(float((mae > -0.5).mean() * 100), 1),
        "suggested_tp_pct": round(float(np.percentile(mfe, 75)), 3),
        "suggested_sl_pct": round(float(np.abs(np.percentile(mae, 25))), 3),
    }
