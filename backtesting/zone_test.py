"""
Zone Test: failed signals as support/resistance zones.

Hypothesis: failed bearish signals form support (long on retest),
failed bullish signals form resistance (short on retest).

Usage:
    python3 -m backtesting.zone_test
Output:
    backtesting/zone_report.txt
    backtesting/zone_results.json
"""
import os
import json
import logging
import time as _time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

logger = logging.getLogger("backtesting.zone")

OUTPUT_DIR = os.path.dirname(__file__)
ANN_FACTOR = 8760
MIN_TRADES = 20
CLUSTER_PCT = 0.3
ENTRY_SEP_SEC = 3600

FAIL_THRESHOLDS = [0.3, 0.5, 0.8, 1.0]
ZONE_WIDTHS = [0.1, 0.15, 0.2, 0.3]
MIN_ZONE_STR = [1, 2, 3]
LIFETIMES_H = [4, 8, 12, 24]
HORIZONS = {"15m": 15, "1h": 60, "4h": 240}
CHANNELS = ["DyorAlerts", "Scalp17"]


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


def _is_round(price, tolerance_pct=0.3):
    nearest_k = round(price / 1000) * 1000
    if nearest_k == 0:
        return False
    return abs(price - nearest_k) / price * 100 <= tolerance_pct


# ---- Core: failure detection ----

def _detect_failures(sig_ts, sig_prices, sig_dirs, price_ts, price_vals, thresh):
    """Vectorized failure detection + confirmation timestamps."""
    n = len(sig_ts)
    n_p = len(price_ts)

    sig_idx = np.searchsorted(price_ts, sig_ts, side="left")
    sig_idx = np.clip(sig_idx, 0, n_p - 1)

    # Forward 4h max/min via reversed rolling
    rev = price_vals[::-1]
    fwd_max = pd.Series(rev).rolling(240, min_periods=1).max().values[::-1]
    fwd_min = pd.Series(rev).rolling(240, min_periods=1).min().values[::-1]

    max_ah = fwd_max[sig_idx]
    min_ah = fwd_min[sig_idx]

    bearish = sig_dirs == "bearish"
    bullish = sig_dirs == "bullish"

    failed = np.zeros(n, dtype=bool)
    with np.errstate(divide="ignore", invalid="ignore"):
        up = np.where(sig_prices > 0, (max_ah - sig_prices) / sig_prices * 100, 0)
        dn = np.where(sig_prices > 0, (sig_prices - min_ah) / sig_prices * 100, 0)
    failed[bearish] = up[bearish] >= thresh
    failed[bullish] = dn[bullish] >= thresh

    # Confirmation timestamps
    t_confirm = np.zeros(n, dtype=np.int64)
    for i in np.where(failed)[0]:
        i0 = sig_idx[i]
        i1 = min(i0 + 240, n_p)
        if i1 <= i0:
            continue
        w = price_vals[i0:i1]
        wt = price_ts[i0:i1]
        if sig_dirs[i] == "bearish":
            hits = w >= sig_prices[i] * (1 + thresh / 100)
        else:
            hits = w <= sig_prices[i] * (1 - thresh / 100)
        if hits.any():
            t_confirm[i] = wt[np.argmax(hits)]

    return failed, t_confirm


# ---- Core: clustering ----

def _cluster_zones(prices, types, t_confirms):
    """Merge zones within ±CLUSTER_PCT."""
    if len(prices) == 0:
        return []
    order = np.argsort(prices)
    sp, st, sc = prices[order], types[order], t_confirms[order]

    clusters = []
    i = 0
    while i < len(sp):
        j = i + 1
        while j < len(sp) and (sp[j] - sp[i]) / sp[i] * 100 <= CLUSTER_PCT:
            j += 1
        grp_t = st[i:j]
        n_sup = (grp_t == "support").sum()
        clusters.append({
            "price": float(np.mean(sp[i:j])),
            "type": "support" if n_sup >= len(grp_t) - n_sup else "resistance",
            "strength": int(j - i),
            "t_confirm": int(sc[i:j].max()),
        })
        i = j
    return clusters


# ---- Core: entry detection ----

def _find_entries(zones, price_ts, price_vals, width_pct, lifetime_sec):
    """Find zone-touch entries with minimum separation."""
    entries = []
    for z in zones:
        zp = z["price"]
        lo = zp * (1 - width_pct / 100)
        hi = zp * (1 + width_pct / 100)
        t0 = z["t_confirm"]
        t1 = t0 + lifetime_sec

        i0 = np.searchsorted(price_ts, t0, side="left")
        i1 = np.searchsorted(price_ts, t1, side="right")
        if i1 <= i0:
            continue

        wp = price_vals[i0:i1]
        wt = price_ts[i0:i1]
        in_z = (wp >= lo) & (wp <= hi)
        if not in_z.any():
            continue

        last_t = -ENTRY_SEP_SEC
        touch = 0
        for idx in np.where(in_z)[0]:
            t = wt[idx]
            if t - last_t >= ENTRY_SEP_SEC:
                touch += 1
                entries.append({
                    "ts": int(t), "price": float(wp[idx]),
                    "z_type": z["type"], "z_str": z["strength"],
                    "z_price": zp, "touch": touch,
                })
                last_t = t
    return entries


# ---- Core: returns ----

def _compute_returns(entries, price_ts, price_vals, fee_pct):
    """Net returns at all horizons."""
    if not entries:
        return {h: np.array([]) for h in HORIZONS}

    ets = np.array([e["ts"] for e in entries])
    eps = np.array([e["price"] for e in entries])
    types = np.array([e["z_type"] for e in entries])

    result = {}
    for hz, offset_min in HORIZONS.items():
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


# ---- Grid search ----

def _run_grid(sig_ts, sig_prices, sig_dirs, price_ts, price_vals,
              fee_pct, split_sec):
    """Full parameter grid. Returns sorted list of results."""
    results = []

    # Pre-compute failures & zones per threshold
    zone_cache = {}
    fail_counts = {}
    for ft in FAIL_THRESHOLDS:
        failed, t_conf = _detect_failures(
            sig_ts, sig_prices, sig_dirs, price_ts, price_vals, ft)
        fail_counts[ft] = int(failed.sum())
        if failed.sum() == 0:
            zone_cache[ft] = []
            continue
        fp = sig_prices[failed]
        ftypes = np.where(sig_dirs[failed] == "bearish", "support", "resistance")
        fc = t_conf[failed]
        zone_cache[ft] = _cluster_zones(fp, ftypes, fc)

    for ft in FAIL_THRESHOLDS:
        zones = zone_cache[ft]
        if not zones:
            continue
        for zw in ZONE_WIDTHS:
            for lt_h in LIFETIMES_H:
                entries = _find_entries(
                    zones, price_ts, price_vals, zw, lt_h * 3600)
                if not entries:
                    continue
                ret = _compute_returns(entries, price_ts, price_vals, fee_pct)

                # Build arrays for filtering
                strengths = np.array([e["z_str"] for e in entries])
                entry_ts = np.array([e["ts"] for e in entries])
                types = np.array([e["z_type"] for e in entries])
                touches = np.array([e["touch"] for e in entries])
                z_prices = np.array([e["z_price"] for e in entries])

                for ms in MIN_ZONE_STR:
                    mask = strengths >= ms
                    if mask.sum() < 5:
                        continue

                    # All-data metrics per horizon
                    hz_all = {}
                    for hz in HORIZONS:
                        r = ret[hz][mask]
                        r = r[~np.isnan(r)]
                        hz_all[hz] = _stats(r)

                    # IS / OOS on 1h
                    is_m = mask & (entry_ts <= split_sec)
                    oos_m = mask & (entry_ts > split_sec)
                    is_r = ret["1h"][is_m]; is_r = is_r[~np.isnan(is_r)]
                    oos_r = ret["1h"][oos_m]; oos_r = oos_r[~np.isnan(oos_r)]

                    # By type (1h)
                    sup_m = mask & (types == "support")
                    res_m = mask & (types == "resistance")
                    sup_r = ret["1h"][sup_m]; sup_r = sup_r[~np.isnan(sup_r)]
                    res_r = ret["1h"][res_m]; res_r = res_r[~np.isnan(res_r)]

                    results.append({
                        "ft": ft, "zw": zw, "lt": lt_h, "ms": ms,
                        "n_failed": fail_counts[ft],
                        "n_zones": len([z for z in zones if z["strength"] >= ms]),
                        "n_entries": int(mask.sum()),
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


# ---- Walk-forward for top 3 ----

def _walk_forward(grid_results, split_sec):
    """Extract top-3 IS combos and compare with OOS."""
    # Filter to combos with IS data
    with_is = [r for r in grid_results if r.get("is_1h", {}).get("trades", 0) >= MIN_TRADES]
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
            "params": {"ft": r["ft"], "zw": r["zw"],
                       "lt": r["lt"], "ms": r["ms"]},
            "is_1h": is_s,
            "oos_1h": oos_s,
            "overfitted": ovf if oos_s else None,
        })
    return top3


# ---- Detailed breakdown for best combo ----

def _detailed(entries, ret, fee_pct, split_sec):
    """Strength, round-number, touch analysis."""
    if not entries:
        return {}

    strengths = np.array([e["z_str"] for e in entries])
    types = np.array([e["z_type"] for e in entries])
    touches = np.array([e["touch"] for e in entries])
    z_prices = np.array([e["z_price"] for e in entries])
    is_round = np.array([_is_round(p) for p in z_prices])

    detail = {}

    # By strength
    for label, smask in [("str==1", strengths == 1),
                         ("str>=2", strengths >= 2),
                         ("str>=3", strengths >= 3)]:
        hz = {}
        for h in HORIZONS:
            r = ret[h][smask]; r = r[~np.isnan(r)]
            hz[h] = _stats(r)
        detail[label] = hz

    # By type
    for label, tmask in [("support_long", types == "support"),
                         ("resist_short", types == "resistance")]:
        hz = {}
        for h in HORIZONS:
            r = ret[h][tmask]; r = r[~np.isnan(r)]
            hz[h] = _stats(r)
        detail[label] = hz

    # By touch
    for label, tmask in [("first_touch", touches == 1),
                         ("later_touch", touches >= 2)]:
        hz = {}
        for h in HORIZONS:
            r = ret[h][tmask]; r = r[~np.isnan(r)]
            hz[h] = _stats(r)
        detail[label] = hz

    # Round numbers
    for label, rmask in [("round_number", is_round),
                         ("non_round", ~is_round)]:
        hz = {}
        for h in HORIZONS:
            r = ret[h][rmask]; r = r[~np.isnan(r)]
            hz[h] = _stats(r)
        detail[label] = hz

    return detail


# ---- Run one channel set ----

def _analyze_channel(label, sig_ts, sig_prices, sig_dirs,
                     price_ts, price_vals, fee_pct, split_sec):
    """Full analysis for one channel (or combined)."""
    if len(sig_ts) < MIN_TRADES:
        return {"skipped": True, "reason": f"signals < {MIN_TRADES}"}

    grid = _run_grid(sig_ts, sig_prices, sig_dirs,
                     price_ts, price_vals, fee_pct, split_sec)

    if not grid:
        return {"skipped": True, "reason": "no entries for any combo",
                "total_signals": len(sig_ts)}

    wf = _walk_forward(grid, split_sec)

    # Detailed for best combo (by 1h Sharpe)
    best = grid[0]
    ft, zw, lt_h, ms = best["ft"], best["zw"], best["lt"], best["ms"]

    # Recompute entries for best combo
    failed, t_conf = _detect_failures(
        sig_ts, sig_prices, sig_dirs, price_ts, price_vals, ft)
    fp = sig_prices[failed]
    ftypes = np.where(sig_dirs[failed] == "bearish", "support", "resistance")
    fc = t_conf[failed]
    zones = _cluster_zones(fp, ftypes, fc)
    entries = _find_entries(zones, price_ts, price_vals, zw, lt_h * 3600)
    ret = _compute_returns(entries, price_ts, price_vals, fee_pct)

    # Filter by min strength
    strengths = np.array([e["z_str"] for e in entries])
    mask = strengths >= ms
    filt_entries = [e for e, m in zip(entries, mask) if m]
    filt_ret = {h: ret[h][mask] for h in HORIZONS}

    detail = _detailed(filt_entries, filt_ret, fee_pct, split_sec)

    return {
        "total_signals": len(sig_ts),
        "grid_top20": grid[:20],
        "walk_forward": wf,
        "best_params": {"ft": ft, "zw": zw, "lt": lt_h, "ms": ms},
        "detail": detail,
    }


# ---- Report ----

def _fmt_s(s, pad=4):
    """Format one stats dict as a compact line."""
    sp = " " * pad
    if s.get("trades", 0) == 0:
        return f"{sp}-- no trades --"
    return (f"{sp}{s['trades']:>4d} tr  WR={s['win_rate']:>5.1f}%  "
            f"avg={s['avg_return']:>+8.4f}%  PF={s['profit_factor']:>6.3f}  "
            f"Sh={s['sharpe']:>8.3f}")


def _build_report(results):
    L = [
        "=" * 70,
        "ZONE TEST: FAILED SIGNALS AS SUPPORT/RESISTANCE",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "Fee: 0.1% per side (0.2% round-trip)",
        "=" * 70,
    ]

    for label in list(CHANNELS) + ["DyorAlerts+Scalp17"]:
        ch = results.get(label, {})
        L += ["", "=" * 60, f"  {label}", "=" * 60, ""]

        if ch.get("skipped"):
            L.append(f"  SKIPPED: {ch.get('reason', '')}")
            continue

        L.append(f"  Total directional signals: {ch['total_signals']}")

        # Top grid results
        L += ["", "  [TOP PARAMETER COMBOS by 1h Sharpe]"]
        hdr = ("  {:>5s} {:>5s} {:>4s} {:>3s}  {:>5s} {:>5s}  "
               "{:>6s} {:>8s} {:>6s} {:>8s}")
        L.append(hdr.format(
            "Fail%", "Width", "Life", "Str", "N_zn", "N_ent",
            "WR_1h", "Avg_1h", "PF_1h", "Sh_1h"))
        L.append("  " + "-" * 72)

        for r in ch.get("grid_top20", [])[:15]:
            h1 = r.get("hz", {}).get("1h", {})
            if h1.get("trades", 0) < 5:
                continue
            row = ("  {:>5.1f} {:>5.2f} {:>4d}h {:>3d}  {:>5d} {:>5d}  "
                   "{:>5.1f}% {:>+8.4f} {:>6.3f} {:>8.3f}")
            L.append(row.format(
                r["ft"], r["zw"], r["lt"], r["ms"],
                r["n_zones"], r["n_entries"],
                h1["win_rate"], h1["avg_return"],
                h1["profit_factor"], h1["sharpe"]))

        # Walk-forward
        wf = ch.get("walk_forward", [])
        L += ["", "  [WALK-FORWARD TOP-3 (70/30)]"]
        if not wf:
            L.append("    No combos with enough IS trades")
        for i, w in enumerate(wf):
            p = w["params"]
            L.append(f"  #{i+1}: fail={p['ft']}%, width=±{p['zw']}%, "
                     f"life={p['lt']}h, min_str={p['ms']}")
            is_s = w.get("is_1h", {})
            oos_s = w.get("oos_1h", {})
            L.append(f"    IS  1h: {is_s.get('trades',0):>4d} tr, "
                     f"WR={is_s.get('win_rate',0):.1f}%, "
                     f"avg={is_s.get('avg_return',0):+.4f}%, "
                     f"Sh={is_s.get('sharpe',0):.3f}")
            if oos_s.get("trades", 0) > 0:
                L.append(f"    OOS 1h: {oos_s['trades']:>4d} tr, "
                         f"WR={oos_s['win_rate']:.1f}%, "
                         f"avg={oos_s['avg_return']:+.4f}%, "
                         f"Sh={oos_s['sharpe']:.3f}")
                ovf = w.get("overfitted")
                if ovf is not None:
                    L.append(f"    Overfitted: {'YES' if ovf else 'NO'}")
            else:
                L.append("    OOS 1h: insufficient data")

        # Detailed breakdown
        detail = ch.get("detail", {})
        if detail:
            bp = ch.get("best_params", {})
            L += ["", f"  [DETAILED BREAKDOWN — best combo: "
                  f"fail={bp.get('ft')}%, ±{bp.get('zw')}%, "
                  f"{bp.get('lt')}h, str>={bp.get('ms')}]"]

            for section, title in [
                ("support_long", "SUPPORT → LONG"),
                ("resist_short", "RESISTANCE → SHORT"),
                ("str==1", "Zone strength == 1"),
                ("str>=2", "Zone strength >= 2"),
                ("str>=3", "Zone strength >= 3"),
                ("first_touch", "First touch of zone"),
                ("later_touch", "Later touches (2nd+)"),
                ("round_number", "Near round number (±0.3%)"),
                ("non_round", "Not near round number"),
            ]:
                sec = detail.get(section, {})
                if not sec:
                    continue
                L.append(f"    [{title}]")
                for hz in ["15m", "1h", "4h"]:
                    s = sec.get(hz, {})
                    if s.get("trades", 0) > 0:
                        L.append(f"      {hz:<4s}" + _fmt_s(s, pad=0))
                L.append("")

    # Summary
    L += ["", "=" * 60, "  SUMMARY", "=" * 60, ""]
    for label in list(CHANNELS) + ["DyorAlerts+Scalp17"]:
        ch = results.get(label, {})
        if ch.get("skipped"):
            L.append(f"  {label}: SKIPPED")
            continue
        top = ch.get("grid_top20", [{}])[0] if ch.get("grid_top20") else {}
        h1 = top.get("hz", {}).get("1h", {})
        if h1.get("trades", 0) >= MIN_TRADES:
            verdict = "POSSIBLE SIGNAL" if h1["sharpe"] > 0 else "NO EDGE"
            L.append(f"  {label}: best 1h Sh={h1['sharpe']:.3f}, "
                     f"WR={h1['win_rate']:.1f}%, "
                     f"avg={h1['avg_return']:+.4f}% → {verdict}")
        elif h1.get("trades", 0) > 0:
            L.append(f"  {label}: best combo has {h1['trades']} trades "
                     f"(< {MIN_TRADES})")
        else:
            L.append(f"  {label}: no valid combos found")

    L += ["", "=" * 70, "END OF ZONE TEST", "=" * 70]
    return L


# ---- Entry point ----

def run(df_signals=None, df_prices=None, df_context=None, fee_rate=0.001):
    """Run zone test."""
    from backtesting.analyze import load_data, derive_directions, IS_RATIO

    if df_signals is None:
        df_signals, df_prices, df_context = load_data()
        df_signals = derive_directions(df_signals)

    fee_pct = fee_rate * 2 * 100  # 0.2%

    # Merge signals + context
    merged = df_signals.merge(
        df_context, left_on="id", right_on="signal_id",
        how="inner", suffixes=("", "_ctx"))
    mask = (
        merged["derived_direction"].isin(["bullish", "bearish"])
        & merged["price_at_signal"].notna()
        & (merged["price_at_signal"] > 0))
    merged = merged[mask].sort_values("timestamp").reset_index(drop=True)

    # Price arrays
    dfp = df_prices.sort_values("timestamp").reset_index(drop=True)
    price_ts = dfp["timestamp"].values.astype("int64") // 10**9
    price_vals = dfp["price"].values.astype(float)

    # IS/OOS split
    sorted_ts = merged["timestamp"].sort_values()
    split_ts = sorted_ts.iloc[int(len(sorted_ts) * IS_RATIO)]
    split_sec = int(split_ts.value // 10**9)

    results = {}

    # Per channel + combined
    channel_sets = [(ch,) for ch in CHANNELS] + [tuple(CHANNELS)]
    for ch_set in channel_sets:
        label = "+".join(ch_set)
        ch_data = merged[merged["channel_name"].isin(ch_set)].copy()
        ch_data = ch_data.sort_values("timestamp").reset_index(drop=True)

        logger.info(f"Analyzing {label} ({len(ch_data)} signals)...")
        t1 = _time.time()

        sig_ts = ch_data["timestamp"].values.astype("int64") // 10**9
        sig_prices = ch_data["price_at_signal"].values.astype(float)
        sig_dirs = ch_data["derived_direction"].values.astype(str)

        results[label] = _analyze_channel(
            label, sig_ts, sig_prices, sig_dirs,
            price_ts, price_vals, fee_pct, split_sec)

        logger.info(f"  {label} done in {_time.time() - t1:.1f}s")

    # Write report
    report_lines = _build_report(results)
    report_path = os.path.join(OUTPUT_DIR, "zone_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    json_path = os.path.join(OUTPUT_DIR, "zone_results.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str, ensure_ascii=False)

    # Print key findings
    print()
    print("=" * 60)
    print("KEY FINDINGS — Zone Test (Failed Signals as S/R)")
    print("=" * 60)

    for label in list(CHANNELS) + ["DyorAlerts+Scalp17"]:
        ch = results.get(label, {})
        if ch.get("skipped"):
            print(f"\n{label}: SKIPPED ({ch.get('reason', '')})")
            continue

        print(f"\n{label} ({ch['total_signals']} signals):")
        grid = ch.get("grid_top20", [])
        if not grid:
            print("  No valid parameter combos")
            continue

        # Show top 5
        for r in grid[:5]:
            h1 = r.get("hz", {}).get("1h", {})
            if h1.get("trades", 0) < 5:
                continue
            marker = ""
            if h1.get("sharpe", 0) > 0:
                marker = " <<<< POSITIVE"
            print(f"  ft={r['ft']}% zw=±{r['zw']}% lt={r['lt']}h "
                  f"str>={r['ms']}: "
                  f"N={r['n_entries']}, WR={h1['win_rate']:.1f}%, "
                  f"avg={h1['avg_return']:+.4f}%, "
                  f"Sh={h1['sharpe']:.3f}{marker}")

        # Walk-forward
        wf = ch.get("walk_forward", [])
        if wf:
            w = wf[0]
            ovf = w.get("overfitted")
            ovf_s = " [OVERFITTED]" if ovf else (" [OK]" if ovf is False else "")
            print(f"  WF best: {w['params']}{ovf_s}")

        # Detailed highlights
        detail = ch.get("detail", {})
        for key in ["support_long", "resist_short",
                     "str>=2", "first_touch", "round_number"]:
            sec = detail.get(key, {})
            h1 = sec.get("1h", {})
            if h1.get("trades", 0) >= 10 and h1.get("sharpe", 0) > 0:
                print(f"  {key} 1h: N={h1['trades']}, "
                      f"WR={h1['win_rate']:.1f}%, Sh={h1['sharpe']:.3f} "
                      f"<<<< EDGE")

    print()
    print(f"Report: {report_path}")
    print(f"JSON:   {json_path}")
    return results


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    t0 = _time.time()
    run()
    logger.info(f"Total: {_time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
