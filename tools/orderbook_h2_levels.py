"""
Hypothesis 2: Limit orders as Support/Resistance levels.

Each orderbook signal defines a price level.
Bid signals → support levels (long on touch).
Ask signals → resistance levels (short on touch).
Also tests breakout-return pattern.
Walk-forward 70/30.
"""
import logging
import numpy as np
import pandas as pd

from tools.orderbook_config import (
    H2_ZONE_WIDTHS, H2_LIFETIME_H, H2_MIN_STRENGTHS,
    H2_ENTRY_HORIZONS, H2_ENTRY_SEP_SEC, H2_BREAKOUT_PCT,
    H2_CHANNEL_SETS, FEE_PCT, IS_RATIO, MIN_TRADES, ANN_FACTOR,
)

logger = logging.getLogger("orderbook.h2")


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


# ---- Core: build levels from signals ----

def _build_levels(sig_ts, sig_prices, sig_sides, sig_quantities,
                  zone_width_pct):
    """Cluster nearby signals into unified levels.

    Returns list of dicts:
        {price, type, strength, quantity_sum, t_first, t_last}
    """
    if len(sig_ts) == 0:
        return []

    order = np.argsort(sig_prices)
    sp = sig_prices[order]
    ss = sig_sides[order]
    st = sig_ts[order]
    sq = sig_quantities[order]

    levels = []
    i = 0
    while i < len(sp):
        j = i + 1
        while j < len(sp) and (sp[j] - sp[i]) / sp[i] * 100 <= zone_width_pct:
            j += 1

        grp_sides = ss[i:j]
        n_bid = (grp_sides == "bid").sum()
        n_ask = (grp_sides == "ask").sum()
        level_type = "support" if n_bid >= n_ask else "resistance"

        grp_qty = sq[i:j]
        qty_sum = float(np.nansum(grp_qty))

        levels.append({
            "price": float(np.mean(sp[i:j])),
            "type": level_type,
            "strength": int(j - i),
            "quantity_sum": qty_sum,
            "t_first": int(st[i:j].min()),
            "t_last": int(st[i:j].max()),
        })
        i = j

    return levels


# ---- Core: detect zone touches ----

def _detect_touches(levels, price_ts, price_vals, zone_width_pct,
                    lifetime_sec, min_strength):
    """Find zone-touch entries with minimum separation."""
    entries = []
    for lv in levels:
        if lv["strength"] < min_strength:
            continue

        zp = lv["price"]
        lo = zp * (1 - zone_width_pct / 100)
        hi = zp * (1 + zone_width_pct / 100)
        t0 = lv["t_last"]  # level active after last signal in cluster
        t1 = t0 + lifetime_sec

        i0 = np.searchsorted(price_ts, t0, side="left")
        i1 = np.searchsorted(price_ts, t1, side="right")
        if i1 <= i0:
            continue

        wp = price_vals[i0:i1]
        wt = price_ts[i0:i1]
        in_zone = (wp >= lo) & (wp <= hi)
        if not in_zone.any():
            continue

        last_t = -H2_ENTRY_SEP_SEC
        touch_num = 0
        for idx in np.where(in_zone)[0]:
            t = wt[idx]
            if t - last_t >= H2_ENTRY_SEP_SEC:
                touch_num += 1
                entries.append({
                    "ts": int(t),
                    "price": float(wp[idx]),
                    "z_type": lv["type"],
                    "z_strength": lv["strength"],
                    "z_qty": lv["quantity_sum"],
                    "z_price": zp,
                    "touch": touch_num,
                })
                last_t = t

    return entries


# ---- Core: detect breakout-return pattern ----

def _detect_breakout_returns(levels, price_ts, price_vals, zone_width_pct,
                             lifetime_sec, min_strength, breakout_pct):
    """Price breaks through level, then returns to zone → entry."""
    entries = []
    for lv in levels:
        if lv["strength"] < min_strength:
            continue

        zp = lv["price"]
        lo = zp * (1 - zone_width_pct / 100)
        hi = zp * (1 + zone_width_pct / 100)
        break_lo = zp * (1 - breakout_pct / 100)
        break_hi = zp * (1 + breakout_pct / 100)
        t0 = lv["t_last"]
        t1 = t0 + lifetime_sec

        i0 = np.searchsorted(price_ts, t0, side="left")
        i1 = np.searchsorted(price_ts, t1, side="right")
        if i1 - i0 < 10:
            continue

        wp = price_vals[i0:i1]
        wt = price_ts[i0:i1]

        # Find breakout points (price goes past the level)
        if lv["type"] == "support":
            # Support: breakout = price goes BELOW break_lo, return = back above lo
            broke = wp < break_lo
        else:
            # Resistance: breakout = price goes ABOVE break_hi, return = back below hi
            broke = wp > break_hi

        if not broke.any():
            continue

        last_t = -H2_ENTRY_SEP_SEC
        break_idx = np.where(broke)[0]
        for bi in break_idx:
            # Look for return to zone after breakout
            after = slice(bi + 1, min(bi + 60, len(wp)))  # up to 1h after break
            after_p = wp[after]
            after_t = wt[after]

            if len(after_p) == 0:
                continue

            if lv["type"] == "support":
                returned = (after_p >= lo) & (after_p <= hi)
            else:
                returned = (after_p >= lo) & (after_p <= hi)

            if not returned.any():
                continue

            ret_idx = np.argmax(returned)
            t = after_t[ret_idx]
            if t - last_t < H2_ENTRY_SEP_SEC:
                continue

            entries.append({
                "ts": int(t),
                "price": float(after_p[ret_idx]),
                "z_type": lv["type"],
                "z_strength": lv["strength"],
                "z_qty": lv["quantity_sum"],
                "z_price": zp,
                "touch": 0,  # breakout-return
                "pattern": "breakout_return",
            })
            last_t = t

    return entries


# ---- Core: compute returns ----

def _compute_returns(entries, price_ts, price_vals, fee_pct):
    """Net returns at all horizons. Support → long, resistance → short."""
    if not entries:
        return {h: np.array([]) for h in H2_ENTRY_HORIZONS}

    ets = np.array([e["ts"] for e in entries])
    eps = np.array([e["price"] for e in entries])
    types = np.array([e["z_type"] for e in entries])

    result = {}
    for hz, offset_min in H2_ENTRY_HORIZONS.items():
        tgt = ets + offset_min * 60
        ti = np.searchsorted(price_ts, tgt, side="left")
        ti = np.clip(ti, 0, len(price_vals) - 1)
        valid = np.abs(price_ts[ti] - tgt) < 120
        fwd = price_vals[ti]
        raw = np.where(
            types == "support",
            (fwd - eps) / eps * 100,
            (eps - fwd) / eps * 100,
        )
        result[hz] = np.where(valid, raw - fee_pct, np.nan)
    return result


# ---- Grid search for one channel set ----

def _grid_search(sig_ts, sig_prices, sig_sides, sig_quantities,
                 price_ts, price_vals, fee_pct, split_sec):
    """Grid over zone_width × min_strength. Returns sorted list."""
    results = []

    for zw in H2_ZONE_WIDTHS:
        levels = _build_levels(sig_ts, sig_prices, sig_sides, sig_quantities, zw)
        if not levels:
            continue

        for ms in H2_MIN_STRENGTHS:
            lifetime_sec = H2_LIFETIME_H * 3600

            # Touch entries
            entries = _detect_touches(
                levels, price_ts, price_vals, zw, lifetime_sec, ms)

            # Breakout-return entries
            br_entries = _detect_breakout_returns(
                levels, price_ts, price_vals, zw, lifetime_sec, ms,
                H2_BREAKOUT_PCT)

            for pattern, elist in [("touch", entries),
                                   ("breakout_return", br_entries),
                                   ("combined", entries + br_entries)]:
                if not elist:
                    continue

                ret = _compute_returns(elist, price_ts, price_vals, fee_pct)
                entry_ts = np.array([e["ts"] for e in elist])
                strengths = np.array([e["z_strength"] for e in elist])
                types_arr = np.array([e["z_type"] for e in elist])

                # All-data stats
                hz_all = {}
                for hz in H2_ENTRY_HORIZONS:
                    r = ret[hz][~np.isnan(ret[hz])]
                    hz_all[hz] = _stats(r)

                # IS / OOS on 1h
                is_m = entry_ts <= split_sec
                oos_m = entry_ts > split_sec
                is_r = ret["1h"][is_m]
                is_r = is_r[~np.isnan(is_r)]
                oos_r = ret["1h"][oos_m]
                oos_r = oos_r[~np.isnan(oos_r)]

                # By type (1h)
                sup_m = types_arr == "support"
                res_m = types_arr == "resistance"
                sup_r = ret["1h"][sup_m]
                sup_r = sup_r[~np.isnan(sup_r)]
                res_r = ret["1h"][res_m]
                res_r = res_r[~np.isnan(res_r)]

                n_levels = len([lv for lv in levels if lv["strength"] >= ms])

                results.append({
                    "zw": zw, "ms": ms, "pattern": pattern,
                    "n_levels": n_levels,
                    "n_entries": len(elist),
                    "hz": hz_all,
                    "is_1h": _stats(is_r) if len(is_r) >= 5 else {},
                    "oos_1h": _stats(oos_r) if len(oos_r) >= 5 else {},
                    "sup_1h": _stats(sup_r) if len(sup_r) >= 5 else {},
                    "res_1h": _stats(res_r) if len(res_r) >= 5 else {},
                })

    results.sort(
        key=lambda x: x.get("hz", {}).get("1h", {}).get("sharpe", -9999),
        reverse=True)
    return results


# ---- Walk-forward top-3 ----

def _walk_forward(grid_results):
    """Extract top-3 IS combos and compare with OOS."""
    with_is = [r for r in grid_results
               if r.get("is_1h", {}).get("trades", 0) >= MIN_TRADES]
    with_is.sort(
        key=lambda x: x["is_1h"].get("sharpe", -9999), reverse=True)

    top3 = []
    for r in with_is[:3]:
        is_s = r["is_1h"]
        oos_s = r.get("oos_1h", {})
        oos_sh = oos_s.get("sharpe", 0)
        is_sh = is_s.get("sharpe", 0)
        ovf = is_sh > 0 and oos_sh < is_sh * 0.5
        top3.append({
            "params": {"zw": r["zw"], "ms": r["ms"],
                       "pattern": r["pattern"]},
            "is_1h": is_s,
            "oos_1h": oos_s,
            "overfitted": ovf if oos_s else None,
        })
    return top3


# ---- Strength breakdown ----

def _strength_breakdown(entries, ret, fee_pct):
    """Breakdown by strength, type, quantity."""
    if not entries:
        return {}

    strengths = np.array([e["z_strength"] for e in entries])
    types = np.array([e["z_type"] for e in entries])
    quantities = np.array([e["z_qty"] for e in entries])

    detail = {}

    # By strength
    for label, smask in [("str==1", strengths == 1),
                         ("str>=2", strengths >= 2),
                         ("str>=3", strengths >= 3)]:
        hz = {}
        for h in H2_ENTRY_HORIZONS:
            r = ret[h][smask]
            r = r[~np.isnan(r)]
            hz[h] = _stats(r)
        detail[label] = hz

    # By type
    for label, tmask in [("support_long", types == "support"),
                         ("resist_short", types == "resistance")]:
        hz = {}
        for h in H2_ENTRY_HORIZONS:
            r = ret[h][tmask]
            r = r[~np.isnan(r)]
            hz[h] = _stats(r)
        detail[label] = hz

    # By quantity (median split)
    if len(quantities) > 10:
        q_med = np.nanmedian(quantities[quantities > 0]) if (quantities > 0).any() else 0
        if q_med > 0:
            for label, qmask in [("high_qty", quantities >= q_med),
                                 ("low_qty", quantities < q_med)]:
                hz = {}
                for h in H2_ENTRY_HORIZONS:
                    r = ret[h][qmask]
                    r = r[~np.isnan(r)]
                    hz[h] = _stats(r)
                detail[label] = hz

    return detail


# ---- Analyze one channel set ----

def _analyze_channel_set(label, sig_df, price_ts, price_vals, fee_pct,
                         split_sec):
    """Full analysis for one channel set."""
    if len(sig_df) < MIN_TRADES:
        return {"skipped": True, "reason": f"signals < {MIN_TRADES}"}

    sig_ts = sig_df["timestamp"].values.astype("int64") // 10**9
    sig_prices = sig_df["btc_price"].values.astype(float)
    sig_sides = sig_df["side"].values.astype(str)
    sig_quantities = sig_df["quantity"].values.astype(float)

    # Filter out signals without valid price
    valid = sig_prices > 1000
    if valid.sum() < MIN_TRADES:
        return {"skipped": True, "reason": "too few signals with valid price"}
    sig_ts = sig_ts[valid]
    sig_prices = sig_prices[valid]
    sig_sides = sig_sides[valid]
    sig_quantities = sig_quantities[valid]

    grid = _grid_search(sig_ts, sig_prices, sig_sides, sig_quantities,
                        price_ts, price_vals, fee_pct, split_sec)

    if not grid:
        return {"skipped": True, "reason": "no entries for any combo",
                "total_signals": int(valid.sum())}

    wf = _walk_forward(grid)

    # Detailed breakdown for best combo
    best = grid[0]
    zw, ms, pat = best["zw"], best["ms"], best["pattern"]
    levels = _build_levels(sig_ts, sig_prices, sig_sides, sig_quantities, zw)
    lifetime_sec = H2_LIFETIME_H * 3600

    if pat == "touch":
        best_entries = _detect_touches(
            levels, price_ts, price_vals, zw, lifetime_sec, ms)
    elif pat == "breakout_return":
        best_entries = _detect_breakout_returns(
            levels, price_ts, price_vals, zw, lifetime_sec, ms,
            H2_BREAKOUT_PCT)
    else:
        best_entries = (_detect_touches(
            levels, price_ts, price_vals, zw, lifetime_sec, ms)
            + _detect_breakout_returns(
                levels, price_ts, price_vals, zw, lifetime_sec, ms,
                H2_BREAKOUT_PCT))

    best_ret = _compute_returns(best_entries, price_ts, price_vals, fee_pct)
    detail = _strength_breakdown(best_entries, best_ret, fee_pct)

    return {
        "total_signals": int(valid.sum()),
        "total_levels": len(levels),
        "grid_top15": grid[:15],
        "walk_forward": wf,
        "best_params": {"zw": zw, "ms": ms, "pattern": pat},
        "detail": detail,
    }


# ---- Entry point ----

def run(df_signals, df_prices, fee_rate=0.001):
    """Run Hypothesis 2 analysis.

    Args:
        df_signals: DataFrame with channel_name, timestamp, extra_data (JSON)
        df_prices: DataFrame with timestamp, price
        fee_rate: float

    Returns:
        dict with per-channel-set results
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

    # Extract side, quantity, btc_price from signals
    def _extract_field(row, field):
        extra = row.get("extra_data")
        if isinstance(extra, dict):
            return extra.get(field)
        return None

    sig = df_signals.copy()
    sig["side"] = sig.apply(lambda r: _extract_field(r, "side"), axis=1)
    sig["quantity"] = sig.apply(lambda r: _extract_field(r, "quantity"), axis=1)
    sig["quantity"] = pd.to_numeric(sig["quantity"], errors="coerce").fillna(0)

    # btc_price: prefer from channel, fallback to Binance
    sig["btc_price"] = sig.apply(
        lambda r: _extract_field(r, "btc_price") or r.get("btc_price_binance"),
        axis=1)
    sig["btc_price"] = pd.to_numeric(sig["btc_price"], errors="coerce")

    results = {"metadata": {
        "total_signals": len(df_signals),
        "split_timestamp": str(split_ts),
    }}

    for set_name, channel_list in H2_CHANNEL_SETS.items():
        set_df = sig[sig["channel_name"].isin(channel_list)].copy()
        set_df = set_df.sort_values("timestamp").reset_index(drop=True)

        logger.info(f"H2 {set_name}: {len(set_df)} signals")

        results[set_name] = _analyze_channel_set(
            set_name, set_df, price_ts, price_vals, fee_pct, split_sec)

    return results
