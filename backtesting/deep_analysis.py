"""Deep analysis: streak strategies, contrarian signals, DMI_SMF dive."""
import os, json, logging, time
from datetime import datetime, timezone
import numpy as np
import pandas as pd

logger = logging.getLogger("backtesting.deep")
FEE_RATE = 0.001
ANN_FACTOR = 8760
MC_SHUFFLES = 100
CONTRARIAN_CHANNELS = ["AltSwing", "SellsPowerIndex", "AltSPI", "Scalp17", "DMI_SMF"]
STREAK_N = range(1, 6)
STREAK_M = range(1, 4)
MIN_TRADES = 30
OUTPUT_DIR = os.path.dirname(__file__)


# ---- Shared helpers ----

def _prepare_merged(df_signals, df_context, fee_pct):
    """Merge signals+context, filter directional with 1h data."""
    m = df_signals.merge(df_context, left_on="id", right_on="signal_id",
                         how="inner", suffixes=("", "_ctx"))
    mask = (m["derived_direction"].isin(["bullish", "bearish"])
            & m["change_1h_pct"].notna()
            & ((m["filled_mask"].astype(int) & 4) > 0))
    df = m[mask].sort_values("timestamp").copy()
    sign = df["derived_direction"].map({"bullish": 1.0, "bearish": -1.0})
    df["dir_sign"] = sign.values
    df["net_return"] = df["change_1h_pct"].values * sign.values - fee_pct
    df["outcome"] = (df["net_return"] > 0).astype(int)
    return df.reset_index(drop=True)


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


def _sharpe(r):
    if len(r) < 2 or np.std(r) == 0:
        return 0.0
    return round(float(np.mean(r) / np.std(r) * np.sqrt(ANN_FACTOR)), 4)


def _pf(r):
    g, l = float(r[r > 0].sum()), abs(float(r[r < 0].sum()))
    return 99.0 if l == 0 and g > 0 else (0.0 if l == 0 else round(g / l, 3))


def _mc_pvalue(raw, signs, fee_pct):
    rng = np.random.default_rng(42)
    actual = float(np.mean(raw * signs - fee_pct))
    better = sum(1 for _ in range(MC_SHUFFLES)
                 if float(np.mean(raw * rng.permutation(signs) - fee_pct)) >= actual)
    return round(better / MC_SHUFFLES, 3)


def _mc_pvalue_from_rets(rets):
    if len(rets) < 10:
        return 1.0
    rng = np.random.default_rng(42)
    actual = float(np.mean(rets))
    abs_r = np.abs(rets)
    better = sum(1 for _ in range(MC_SHUFFLES)
                 if float(np.mean(abs_r * rng.choice([-1., 1.], size=len(rets)))) >= actual)
    return round(better / MC_SHUFFLES, 3)


def _sl(k, v, kw=12):
    """Format one stats line."""
    return (f"    {str(k):{kw}s}: {v.get('trades',0):4d} tr, "
            f"WR={v.get('win_rate',0):.1f}%, "
            f"ret={v.get('avg_return',0):+.4f}%, Sh={v.get('sharpe',0):.3f}")


# ---- Analysis 1: Streak strategy ----

def _analysis_streak(merged_is, merged_oos):
    result = {}
    for ch in sorted(merged_is["channel_name"].unique()):
        is_ch = merged_is[merged_is["channel_name"] == ch]
        oos_ch = merged_oos[merged_oos["channel_name"] == ch]
        if len(is_ch) < MIN_TRADES:
            result[ch] = {"skipped": True, "reason": f"IS < {MIN_TRADES}"}
            continue
        result[ch] = _streak_grid(is_ch, oos_ch)
    return result


def _streak_grid(is_df, oos_df):
    oi, ri = is_df["outcome"].values, is_df["net_return"].values
    oo, ro = oos_df["outcome"].values, oos_df["net_return"].values
    best, combos = None, []
    for n in STREAK_N:
        for m in STREAK_M:
            sel = _streak_filter(oi, ri, n, m)
            if len(sel) < 10:
                continue
            s = _quick_stats(sel)
            combos.append({"n_wins": n, "m_losses": m, **s})
            if best is None or s["sharpe"] > best["sharpe"]:
                best = {"n_wins": n, "m_losses": m, **s}
    if best is None:
        return {"skipped": True, "reason": "no valid combo"}
    oos_sel = _streak_filter(oo, ro, best["n_wins"], best["m_losses"])
    oos_s = _quick_stats(oos_sel) if len(oos_sel) >= 5 else {}
    ovf = best["sharpe"] > 0 and oos_s.get("sharpe", 0) < best["sharpe"] * 0.5
    return {
        "best_is_params": {"n_wins": best["n_wins"], "m_losses": best["m_losses"]},
        "is_stats": {k: v for k, v in best.items() if k not in ("n_wins", "m_losses")},
        "oos_stats": oos_s, "overfitted": ovf, "all_combos_is": combos,
    }


def _streak_filter(outcomes, returns, n_enter, m_stop):
    selected, win_streak, loss_streak, active = [], 0, 0, False
    for i in range(len(outcomes)):
        if not active and win_streak >= n_enter:
            active, loss_streak = True, 0
        if active:
            selected.append(returns[i])
            if outcomes[i] == 0:
                loss_streak += 1
                if loss_streak >= m_stop:
                    active, loss_streak, win_streak = False, 0, 0
            else:
                loss_streak = 0
        win_streak = win_streak + 1 if outcomes[i] == 1 else 0
    return np.array(selected) if selected else np.array([])


# ---- Analysis 2: Contrarian ----

def _analysis_contrarian(merged_is, merged_oos, fee_pct):
    result = {}
    for ch in CONTRARIAN_CHANNELS:
        is_ch = merged_is[merged_is["channel_name"] == ch]
        oos_ch = merged_oos[merged_oos["channel_name"] == ch]
        if len(is_ch) < MIN_TRADES:
            result[ch] = {"skipped": True}
            continue
        inv_is = is_ch["change_1h_pct"].values * (-is_ch["dir_sign"].values) - fee_pct
        inv_oos = (oos_ch["change_1h_pct"].values * (-oos_ch["dir_sign"].values) - fee_pct
                   if len(oos_ch) >= 5 else np.array([]))
        mc_p = _mc_pvalue(is_ch["change_1h_pct"].values,
                          -is_ch["dir_sign"].values, fee_pct)
        directed = is_ch["change_1h_pct"].values * (-is_ch["dir_sign"].values)
        mfe = np.where(directed > 0, directed, 0.0)
        mae = np.where(directed < 0, directed, 0.0)
        avg_mae = float(np.mean(mae))
        inv_is_s = _quick_stats(inv_is)
        inv_oos_s = _quick_stats(inv_oos) if len(inv_oos) >= 5 else {}
        ovf = inv_is_s["sharpe"] > 0 and inv_oos_s.get("sharpe", 0) < inv_is_s["sharpe"] * 0.5
        result[ch] = {
            "original_is": _quick_stats(is_ch["net_return"].values),
            "contrarian_is": inv_is_s, "contrarian_oos": inv_oos_s,
            "mc_p_value": mc_p, "mc_significant_5pct": mc_p < 0.05,
            "mfe_mae": {
                "avg_mfe_pct": round(float(np.mean(mfe)), 4),
                "avg_mae_pct": round(avg_mae, 4),
                "mfe_mae_ratio": round(float(np.mean(mfe)) / abs(avg_mae), 3) if avg_mae != 0 else 0,
            },
            "overfitted": ovf,
        }
    return result


# ---- Analysis 3: DMI_SMF deep dive ----

def _analysis_dmi_deep(merged_is, merged_oos, df_prices, fee_pct):
    ch = "DMI_SMF"
    is_ch = merged_is[merged_is["channel_name"] == ch]
    oos_ch = merged_oos[merged_oos["channel_name"] == ch]
    all_ch = pd.concat([is_ch, oos_ch]).sort_values("timestamp").reset_index(drop=True)
    if len(is_ch) < MIN_TRADES:
        return {"skipped": True, "reason": f"IS < {MIN_TRADES}"}
    return {
        "total_signals": len(all_ch),
        "by_direction": _grp_stats(all_ch, "derived_direction"),
        "by_value_quantile": _dmi_by_quantile(all_ch),
        "by_hour": _dmi_by_hour(all_ch),
        "by_day": _dmi_by_day(all_ch),
        "by_regime": _dmi_by_regime(all_ch, df_prices),
        "sweet_spot": _dmi_sweet_spot(is_ch, oos_ch, df_prices, fee_pct),
    }


def _grp_stats(df, col):
    result = {}
    for val, grp in df.groupby(col):
        r = grp["net_return"].values
        if len(r) >= 5:
            result[str(val)] = _quick_stats(r)
    return result


def _dmi_by_quantile(df):
    dv = df[df["indicator_value"].notna()].copy()
    if len(dv) < 20:
        return {}
    try:
        dv["q"] = pd.qcut(dv["indicator_value"], 4,
                           labels=["Q1_low", "Q2", "Q3", "Q4_high"], duplicates="drop")
    except ValueError:
        return {}
    return {str(q): _quick_stats(g["net_return"].values)
            for q, g in dv.groupby("q", observed=True) if len(g) >= 5}


def _dmi_by_hour(df):
    hours = df["timestamp"].dt.hour
    return {int(h): _quick_stats(df.loc[hours == h, "net_return"].values)
            for h in sorted(hours.unique())
            if (hours == h).sum() >= 3}


def _dmi_by_day(df):
    dm = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    dow = df["timestamp"].dt.dayofweek
    return {dm[d]: _quick_stats(df.loc[dow == d, "net_return"].values)
            for d in sorted(dow.unique()) if (dow == d).sum() >= 3}


def _build_regimes(df_prices):
    p = df_prices[["timestamp", "price"]].copy().sort_values("timestamp")
    ps = p.set_index("timestamp")["price"]
    vol = ps.pct_change().rolling(1440, min_periods=720).std() * 100
    vol_q = pd.qcut(vol.dropna(), 3, labels=["low_vol", "med_vol", "high_vol"])
    slope = ps.rolling(240, min_periods=120).mean().pct_change(60) * 100
    trend = pd.cut(slope.dropna(), bins=[-np.inf, -0.05, 0.05, np.inf],
                   labels=["downtrend", "sideways", "uptrend"])
    r = pd.DataFrame({"vol_regime": vol_q, "trend_regime": trend})
    return r.dropna().reset_index().rename(columns={"index": "timestamp"})


def _enrich(df, df_prices):
    reg = _build_regimes(df_prices)
    return pd.merge_asof(df.sort_values("timestamp").copy(),
                         reg.sort_values("timestamp"),
                         on="timestamp", direction="backward")


def _dmi_by_regime(df, df_prices):
    dm = _enrich(df, df_prices)
    result = {"by_vol": {}, "by_trend": {}, "by_combined": {}}
    for col, key in [("vol_regime", "by_vol"), ("trend_regime", "by_trend")]:
        if col not in dm.columns:
            continue
        for v, g in dm.groupby(col, observed=True):
            r = g["net_return"].dropna().values
            if len(r) >= 5:
                result[key][str(v)] = _quick_stats(r)
    if "vol_regime" in dm.columns and "trend_regime" in dm.columns:
        for (v, t), g in dm.groupby(["vol_regime", "trend_regime"], observed=True):
            r = g["net_return"].dropna().values
            if len(r) >= 5:
                result["by_combined"][f"{v}_{t}"] = _quick_stats(r)
    return result


def _dmi_sweet_spot(is_ch, oos_ch, df_prices, fee_pct):
    is_e, oos_e = _enrich(is_ch, df_prices), _enrich(oos_ch, df_prices)
    cands = []
    for d in ["bullish", "bearish"]:
        mask = is_e["derived_direction"] == d
        if mask.sum() >= MIN_TRADES:
            s = _quick_stats(is_e.loc[mask, "net_return"].values)
            if s["avg_return"] > -fee_pct:
                cands.append({"filter": f"direction={d}", "is_stats": s,
                              "trades_is": s["trades"]})
    iv = is_e[is_e["indicator_value"].notna()].copy()
    if len(iv) >= 40:
        try:
            iv["vq"] = pd.qcut(iv["indicator_value"], 4,
                                labels=["Q1", "Q2", "Q3", "Q4"], duplicates="drop")
            for q in ["Q1", "Q2", "Q3", "Q4"]:
                mask = iv["vq"] == q
                if mask.sum() >= MIN_TRADES:
                    s = _quick_stats(iv.loc[mask, "net_return"].values)
                    if s["avg_return"] > -fee_pct:
                        cands.append({"filter": f"value_quantile={q}",
                                      "is_stats": s, "trades_is": s["trades"]})
        except ValueError:
            pass
    if "vol_regime" in is_e.columns:
        for reg in ["low_vol", "med_vol", "high_vol"]:
            mask = is_e["vol_regime"] == reg
            if mask.sum() >= MIN_TRADES:
                s = _quick_stats(is_e.loc[mask, "net_return"].values)
                if s["avg_return"] > -fee_pct:
                    cands.append({"filter": f"vol_regime={reg}",
                                  "is_stats": s, "trades_is": s["trades"]})
    if not cands:
        return {"found": False, "message": "no IS-profitable conditions"}
    cands.sort(key=lambda c: c["is_stats"].get("sharpe", -999), reverse=True)
    best = cands[0]
    oos_rets = _apply_filter(oos_e, best["filter"])
    is_rets = _apply_filter(is_e, best["filter"])
    return {
        "found": True, "best_filter": best["filter"],
        "is_stats": best["is_stats"], "is_trades": best["trades_is"],
        "oos_stats": _quick_stats(oos_rets) if len(oos_rets) >= 5 else {},
        "oos_trades": len(oos_rets),
        "mc_p_value": _mc_pvalue_from_rets(is_rets),
        "mc_significant": _mc_pvalue_from_rets(is_rets) < 0.05,
        "all_candidates": [{"filter": c["filter"], "sharpe": c["is_stats"]["sharpe"],
                            "trades": c["trades_is"]} for c in cands[:5]],
    }


def _apply_filter(df, filt):
    k, v = filt.split("=", 1)
    if k == "direction":
        return df.loc[df["derived_direction"] == v, "net_return"].values
    if k == "value_quantile":
        dv = df[df["indicator_value"].notna()].copy()
        if len(dv) < 4:
            return np.array([])
        try:
            dv["vq"] = pd.qcut(dv["indicator_value"], 4,
                                labels=["Q1", "Q2", "Q3", "Q4"], duplicates="drop")
            return dv.loc[dv["vq"] == v, "net_return"].values
        except ValueError:
            return np.array([])
    if k == "vol_regime" and "vol_regime" in df.columns:
        return df.loc[df["vol_regime"] == v, "net_return"].values
    return np.array([])


# ---- Verdict ----

def _build_verdict(result):
    findings = []
    for ch, d in result.get("streak_strategy", {}).items():
        if not isinstance(d, dict) or d.get("skipped"):
            continue
        o = d.get("oos_stats", {})
        if o.get("avg_return", -1) > 0 and o.get("sharpe", 0) > 0 and not d.get("overfitted"):
            p = d["best_is_params"]
            findings.append(f"streak({ch}: N={p['n_wins']},M={p['m_losses']})")
    for ch, d in result.get("contrarian", {}).items():
        if not isinstance(d, dict) or d.get("skipped"):
            continue
        o = d.get("contrarian_oos", {})
        if o.get("avg_return", -1) > 0 and d.get("mc_significant_5pct") and not d.get("overfitted"):
            findings.append(f"contrarian({ch})")
    dmi = result.get("dmi_smf_dive", {}).get("sweet_spot", {})
    if dmi.get("found") and dmi.get("mc_significant") and dmi.get("oos_stats", {}).get("avg_return", -1) > 0:
        findings.append(f"dmi_sweet_spot({dmi.get('best_filter', '?')})")
    if not findings:
        return ("NO_EDGE_FOUND: All three analyses fail to produce OOS-profitable, "
                "statistically significant strategies net of 0.2% fees.")
    return "POSSIBLE_EDGE: " + "; ".join(findings)


# ---- Report ----

def _build_report(result):
    L = ["=" * 70, "BTC SIGNAL DEEP ANALYSIS REPORT",
         f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
         "Fee: 0.1% per side (0.2% round-trip)", "=" * 70, ""]
    # Streak
    L += ["", "=" * 60, "  1. STREAK STRATEGY (enter after N wins, stop after M losses)", "=" * 60, ""]
    fmt = "{:<18s} {:>2s}/{:>2s} {:>6s} {:>7s} {:>6s} {:>7s} {:>4s}"
    L.append(fmt.format("Channel", "N", "M", "IS_WR", "IS_Sh", "OOS_WR", "OOS_Sh", "Fit"))
    L.append("-" * 60)
    for ch in sorted(result.get("streak_strategy", {})):
        d = result["streak_strategy"][ch]
        if d.get("skipped"):
            L.append(f"  {ch}: SKIPPED ({d.get('reason', '')})")
            continue
        p, i, o = d["best_is_params"], d["is_stats"], d.get("oos_stats", {})
        L.append(fmt.format(ch[:18], str(p["n_wins"]), str(p["m_losses"]),
                            f"{i['win_rate']:.1f}", f"{i['sharpe']:.3f}",
                            f"{o.get('win_rate',0):.1f}", f"{o.get('sharpe',0):.3f}",
                            "OVER" if d.get("overfitted") else "OK"))
    # Contrarian
    L += ["", "=" * 60, "  2. CONTRARIAN SIGNALS (inverted direction)", "=" * 60, ""]
    f2 = "{:<18s} {:>6s} {:>7s} {:>6s} {:>7s} {:>6s} {:>7s} {:>5s} {:>3s}"
    L.append(f2.format("Channel", "OrigWR", "Orig_Sh", "InvWR", "Inv_Sh",
                        "OOSWR", "OOS_Sh", "MCp", "Sig"))
    L.append("-" * 72)
    for ch in sorted(result.get("contrarian", {})):
        d = result["contrarian"][ch]
        if d.get("skipped"):
            L.append(f"  {ch}: SKIPPED"); continue
        og, iv, oo = d["original_is"], d["contrarian_is"], d.get("contrarian_oos", {})
        L.append(f2.format(ch[:18], f"{og['win_rate']:.1f}", f"{og['sharpe']:.3f}",
                           f"{iv['win_rate']:.1f}", f"{iv['sharpe']:.3f}",
                           f"{oo.get('win_rate',0):.1f}", f"{oo.get('sharpe',0):.3f}",
                           f"{d.get('mc_p_value',1):.2f}",
                           "*" if d.get("mc_significant_5pct") else ""))
    # DMI
    L += ["", "=" * 60, "  3. DMI_SMF DEEP DIVE", "=" * 60, ""]
    dmi = result.get("dmi_smf_dive", {})
    if dmi.get("skipped"):
        L.append(f"  SKIPPED: {dmi.get('reason', '')}"); return L
    for sec, title in [("by_direction", "BY DIRECTION"), ("by_value_quantile", "BY VALUE QUANTILE")]:
        g = dmi.get(sec, {})
        if g:
            L.append(f"  [{title}]")
            for k in sorted(g, key=str):
                L.append(_sl(k, g[k]))
            L.append("")
    bh = dmi.get("by_hour", {})
    if bh:
        L.append("  [BY HOUR (UTC)]")
        for h in sorted(bh, key=int):
            L.append(_sl(f"{int(h):02d}:00", bh[h]))
        L.append("")
    bd = dmi.get("by_day", {})
    if bd:
        L.append("  [BY DAY]")
        for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
            if d in bd:
                L.append(_sl(d, bd[d]))
        L.append("")
    reg = dmi.get("by_regime", {})
    for key, title in [("by_vol", "BY VOLATILITY REGIME"), ("by_trend", "BY TREND REGIME"),
                       ("by_combined", "BY VOL x TREND")]:
        g = reg.get(key, {})
        if g:
            L.append(f"  [{title}]")
            for k in sorted(g, key=str):
                L.append(_sl(k, g[k], kw=20))
            L.append("")
    sw = dmi.get("sweet_spot", {})
    L.append("  [SWEET SPOT SEARCH]")
    if sw.get("found"):
        si, so = sw.get("is_stats", {}), sw.get("oos_stats", {})
        L.append(f"  Best filter: {sw['best_filter']}")
        L.append(f"    IS:  {sw.get('is_trades',0)} tr, WR={si.get('win_rate',0):.1f}%, "
                 f"ret={si.get('avg_return',0):+.4f}%, Sh={si.get('sharpe',0):.3f}")
        L.append(f"    OOS: {sw.get('oos_trades',0)} tr, WR={so.get('win_rate',0):.1f}%, "
                 f"ret={so.get('avg_return',0):+.4f}%, Sh={so.get('sharpe',0):.3f}")
        L.append(f"    MC p={sw.get('mc_p_value',1):.3f} "
                 f"{'SIGNIFICANT' if sw.get('mc_significant') else 'not significant'}")
        for c in sw.get("all_candidates", [])[1:]:
            L.append(f"    {c['filter']:30s} Sh={c['sharpe']:.3f} ({c['trades']} tr)")
    else:
        L.append(f"  {sw.get('message', 'no sweet spot found')}")
    # Verdict
    L += ["", "=" * 60, "  VERDICT", "=" * 60, ""]
    L.append(f"  {result.get('verdict', 'N/A')}")
    L += ["", "=" * 70, "END OF DEEP ANALYSIS", "=" * 70]
    return L


def _write_outputs(result):
    jp = os.path.join(OUTPUT_DIR, "deep_results.json")
    rp = os.path.join(OUTPUT_DIR, "deep_report.txt")
    with open(jp, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str, ensure_ascii=False)
    with open(rp, "w", encoding="utf-8") as f:
        f.write("\n".join(_build_report(result)))
    logger.info(f"Deep report: {rp}")
    logger.info(f"Deep JSON:   {jp}")


# ---- Entry points ----

def _run_analyses(df_sig_is, df_sig_oos, df_ctx_is, df_ctx_oos, df_prices, fee_pct):
    mi = _prepare_merged(df_sig_is, df_ctx_is, fee_pct)
    mo = _prepare_merged(df_sig_oos, df_ctx_oos, fee_pct)
    result = {}
    for name, fn, args in [
        ("streak_strategy", _analysis_streak, (mi, mo)),
        ("contrarian", _analysis_contrarian, (mi, mo, fee_pct)),
        ("dmi_smf_dive", _analysis_dmi_deep, (mi, mo, df_prices, fee_pct)),
    ]:
        logger.info(f"Running {name}...")
        t1 = time.time()
        result[name] = fn(*args)
        logger.info(f"  {name} done in {time.time() - t1:.1f}s")
    result["verdict"] = _build_verdict(result)
    _write_outputs(result)
    return result


def run(df_signals, df_prices, df_context,
        df_sig_is, df_sig_oos, df_ctx_is, df_ctx_oos, fee_rate=0.001):
    """Called from analyze.py with pre-loaded and pre-split data."""
    fee_pct = fee_rate * 2 * 100
    return _run_analyses(df_sig_is, df_sig_oos, df_ctx_is, df_ctx_oos, df_prices, fee_pct)


def main():
    from backtesting.analyze import load_data, derive_directions, split_is_oos, FEE_RATE as FR
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    t0 = time.time()
    logger.info("Loading data...")
    df_s, df_p, df_c = load_data()
    df_s = derive_directions(df_s)
    df_si, df_so = split_is_oos(df_s)
    df_ci, df_co = split_is_oos(df_c, "signal_timestamp")
    logger.info(f"IS={len(df_si)}, OOS={len(df_so)}")
    _run_analyses(df_si, df_so, df_ci, df_co, df_p, FR * 2 * 100)
    logger.info(f"Total: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
