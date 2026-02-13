"""
Report builder for orderbook analysis.
Two main sections: H1 (Imbalance) and H2 (Levels).
Output: backtesting/orderbook_report.txt
"""
import os
import json
from datetime import datetime, timezone

from tools.orderbook_config import PAIRS, H2_CHANNEL_SETS

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "backtesting")


def _fmt_s(s, pad=4):
    """Format one stats dict as a compact line."""
    sp = " " * pad
    if s.get("trades", 0) == 0:
        return f"{sp}-- no trades --"
    return (f"{sp}{s['trades']:>4d} tr  WR={s['win_rate']:>5.1f}%  "
            f"avg={s['avg_return']:>+8.4f}%  PF={s['profit_factor']:>6.3f}  "
            f"Sh={s['sharpe']:>8.3f}")


# ---- H1: Imbalance section ----

def _section_h1(h1_results):
    """Build report lines for Hypothesis 1."""
    L = [
        "", "=" * 70,
        "  HYPOTHESIS 1: BID/ASK IMBALANCE IN 5-MINUTE WINDOWS",
        "=" * 70, "",
    ]

    meta = h1_results.get("metadata", {})
    L.append(f"  Total signals: {meta.get('total_signals', '?')}")
    L.append(f"  Split: {meta.get('split_timestamp', '?')}")
    L.append("")

    pair_labels = [p[2] for p in PAIRS] + ["Aggregate"]

    for pair in pair_labels:
        data = h1_results.get(pair, {})
        L += ["-" * 60, f"  {pair}", "-" * 60, ""]

        if data.get("skipped"):
            L.append(f"  SKIPPED: {data.get('reason', '')}")
            L.append("")
            continue

        L.append(f"  Bid signals: {data.get('bid_signals', 0)}, "
                 f"Ask signals: {data.get('ask_signals', 0)}")
        L.append(f"  Total 5-min windows: {data.get('total_windows', 0)}, "
                 f"Active (imbalance!=0): {data.get('active_windows', 0)}")
        L.append("")

        # Full results table
        results = data.get("results", {})
        if results:
            L.append("  [ALL DATA — threshold × mode × horizon]")
            hdr = "  {:>12s}  {:>6s}  {:>6s}  {:>8s}  {:>6s}  {:>8s}"
            L.append(hdr.format("Combo", "N", "WR%", "AvgRet%", "PF", "Sharpe"))
            L.append("  " + "-" * 56)

            for key in sorted(results.keys()):
                combo = results[key]
                for hz in ["5m", "15m", "1h", "4h"]:
                    s = combo.get(hz, {})
                    if s.get("trades", 0) < 5:
                        continue
                    label = f"{key}_{hz}"
                    row = "  {:>12s}  {:>6d}  {:>5.1f}%  {:>+8.4f}  {:>6.3f}  {:>8.3f}"
                    L.append(row.format(
                        label[:12], s["trades"], s["win_rate"],
                        s["avg_return"], s["profit_factor"], s["sharpe"]))
            L.append("")

        # Walk-forward
        wf = data.get("walk_forward", {})
        if wf.get("skipped"):
            L.append(f"  Walk-Forward: SKIPPED ({wf.get('reason', '')})")
        elif "best_params" in wf:
            bp = wf["best_params"]
            L.append(f"  [WALK-FORWARD]")
            L.append(f"  Best IS: threshold={bp['threshold']}, "
                     f"mode={bp['mode']}")

            is_1h = wf.get("is_1h", {})
            oos_1h = wf.get("oos_1h", {})
            if is_1h.get("trades", 0) > 0:
                L.append(f"    IS  1h:" + _fmt_s(is_1h, pad=1))
            if oos_1h.get("trades", 0) > 0:
                L.append(f"    OOS 1h:" + _fmt_s(oos_1h, pad=1))
            ovf = wf.get("overfitted")
            if ovf is not None:
                L.append(f"    Overfitted: {'YES' if ovf else 'NO'}")
        L.append("")

    return L


# ---- H2: Levels section ----

def _section_h2(h2_results):
    """Build report lines for Hypothesis 2."""
    L = [
        "", "=" * 70,
        "  HYPOTHESIS 2: LIMIT ORDERS AS SUPPORT/RESISTANCE LEVELS",
        "=" * 70, "",
    ]

    meta = h2_results.get("metadata", {})
    L.append(f"  Total signals: {meta.get('total_signals', '?')}")
    L.append(f"  Split: {meta.get('split_timestamp', '?')}")
    L.append("")

    for set_name in H2_CHANNEL_SETS:
        data = h2_results.get(set_name, {})
        L += ["-" * 60, f"  {set_name}", "-" * 60, ""]

        if data.get("skipped"):
            L.append(f"  SKIPPED: {data.get('reason', '')}")
            L.append("")
            continue

        L.append(f"  Signals with price: {data.get('total_signals', 0)}")
        L.append(f"  Levels built: {data.get('total_levels', 0)}")
        L.append("")

        # Top grid results
        grid = data.get("grid_top15", [])
        if grid:
            L.append("  [TOP PARAMETER COMBOS by 1h Sharpe]")
            hdr = "  {:>5s} {:>3s} {:>10s}  {:>5s} {:>5s}  {:>6s} {:>8s} {:>6s} {:>8s}"
            L.append(hdr.format(
                "Width", "Str", "Pattern", "N_lv", "N_ent",
                "WR_1h", "Avg_1h", "PF_1h", "Sh_1h"))
            L.append("  " + "-" * 70)

            for r in grid[:10]:
                h1 = r.get("hz", {}).get("1h", {})
                if h1.get("trades", 0) < 5:
                    continue
                row = ("  {:>5.2f} {:>3d} {:>10s}  {:>5d} {:>5d}  "
                       "{:>5.1f}% {:>+8.4f} {:>6.3f} {:>8.3f}")
                L.append(row.format(
                    r["zw"], r["ms"], r["pattern"][:10],
                    r["n_levels"], r["n_entries"],
                    h1["win_rate"], h1["avg_return"],
                    h1["profit_factor"], h1["sharpe"]))
            L.append("")

        # Walk-forward
        wf = data.get("walk_forward", [])
        L.append("  [WALK-FORWARD TOP-3 (70/30)]")
        if not wf:
            L.append("    No combos with enough IS trades")
        for i, w in enumerate(wf):
            p = w["params"]
            L.append(f"  #{i+1}: width=+-{p['zw']}%, min_str={p['ms']}, "
                     f"pattern={p['pattern']}")
            is_s = w.get("is_1h", {})
            oos_s = w.get("oos_1h", {})
            if is_s.get("trades", 0) > 0:
                L.append(f"    IS  1h:" + _fmt_s(is_s, pad=1))
            if oos_s.get("trades", 0) > 0:
                L.append(f"    OOS 1h:" + _fmt_s(oos_s, pad=1))
                ovf = w.get("overfitted")
                if ovf is not None:
                    L.append(f"    Overfitted: {'YES' if ovf else 'NO'}")
            else:
                L.append("    OOS 1h: insufficient data")
        L.append("")

        # Detailed breakdown
        detail = data.get("detail", {})
        if detail:
            bp = data.get("best_params", {})
            L.append(f"  [DETAILED BREAKDOWN — best combo: "
                     f"+-{bp.get('zw')}%, str>={bp.get('ms')}, "
                     f"{bp.get('pattern')}]")

            for section, title in [
                ("support_long", "SUPPORT -> LONG"),
                ("resist_short", "RESISTANCE -> SHORT"),
                ("str==1", "Strength == 1"),
                ("str>=2", "Strength >= 2"),
                ("str>=3", "Strength >= 3"),
                ("high_qty", "High quantity (above median)"),
                ("low_qty", "Low quantity (below median)"),
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

    return L


# ---- Summary ----

def _section_summary(h1_results, h2_results):
    """Build summary with verdicts."""
    L = [
        "", "=" * 70,
        "  SUMMARY & VERDICT",
        "=" * 70, "",
    ]

    # H1 summary
    L.append("  [H1: Bid/Ask Imbalance]")
    pair_labels = [p[2] for p in PAIRS] + ["Aggregate"]

    any_h1_edge = False
    for pair in pair_labels:
        data = h1_results.get(pair, {})
        if data.get("skipped"):
            continue
        wf = data.get("walk_forward", {})
        if wf.get("skipped") or "best_params" not in wf:
            continue
        bp = wf["best_params"]
        oos = wf.get("oos_1h", {})
        is_s = wf.get("is_1h", {})
        ovf = wf.get("overfitted", True)

        if oos.get("trades", 0) >= 10 and oos.get("sharpe", 0) > 0 and not ovf:
            verdict = "POSSIBLE SIGNAL"
            any_h1_edge = True
        elif is_s.get("sharpe", 0) > 0:
            verdict = "IS-only (overfitted or insufficient OOS)"
        else:
            verdict = "NO EDGE"

        L.append(f"    {pair}: thr={bp['threshold']}, mode={bp['mode']} "
                 f"-> {verdict}")
        if oos.get("trades", 0) > 0:
            L.append(f"      OOS: N={oos['trades']}, "
                     f"WR={oos.get('win_rate', 0):.1f}%, "
                     f"Sh={oos.get('sharpe', 0):.3f}")

    if not any_h1_edge:
        L.append("    VERDICT: NO ROBUST EDGE FOUND in imbalance signals")
    else:
        L.append("    VERDICT: POSSIBLE EDGE detected (verify with more data)")
    L.append("")

    # H2 summary
    L.append("  [H2: Limit Orders as S/R Levels]")
    any_h2_edge = False

    for set_name in H2_CHANNEL_SETS:
        data = h2_results.get(set_name, {})
        if data.get("skipped"):
            L.append(f"    {set_name}: SKIPPED ({data.get('reason', '')})")
            continue

        wf = data.get("walk_forward", [])
        if not wf:
            L.append(f"    {set_name}: no WF combos with enough trades")
            continue

        w = wf[0]
        p = w["params"]
        oos = w.get("oos_1h", {})
        ovf = w.get("overfitted", True)

        if oos.get("trades", 0) >= 10 and oos.get("sharpe", 0) > 0 and not ovf:
            verdict = "POSSIBLE SIGNAL"
            any_h2_edge = True
        elif w.get("is_1h", {}).get("sharpe", 0) > 0:
            verdict = "IS-only (overfitted or insufficient OOS)"
        else:
            verdict = "NO EDGE"

        L.append(f"    {set_name}: zw=+-{p['zw']}%, ms={p['ms']}, "
                 f"{p['pattern']} -> {verdict}")
        if oos.get("trades", 0) > 0:
            L.append(f"      OOS: N={oos['trades']}, "
                     f"WR={oos.get('win_rate', 0):.1f}%, "
                     f"Sh={oos.get('sharpe', 0):.3f}")

    if not any_h2_edge:
        L.append("    VERDICT: NO ROBUST EDGE FOUND in level-based signals")
    else:
        L.append("    VERDICT: POSSIBLE EDGE detected (verify with more data)")

    L.append("")

    # Overall
    if any_h1_edge or any_h2_edge:
        L.append("  OVERALL: Some hypotheses show potential — "
                 "requires live testing")
    else:
        L.append("  OVERALL: Neither hypothesis shows robust out-of-sample edge")
        L.append("  Orderbook signal channels do not provide reliable "
                 "alpha after fees")

    L += ["", "=" * 70, "END OF ORDERBOOK ANALYSIS", "=" * 70]
    return L


# ---- Entry point ----

def build_report(h1_results, h2_results, parse_stats=None):
    """Build and save the full report.

    Returns: (report_path, json_path)
    """
    lines = [
        "=" * 70,
        "ORDERBOOK CHANNEL ANALYSIS REPORT",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "Fee: 0.1% per side (0.2% round-trip)",
        "Walk-forward: 70% IS / 30% OOS",
        "=" * 70,
    ]

    # Parse stats section
    if parse_stats:
        lines += ["", "  [DATA SUMMARY]"]
        total_parsed = 0
        total_failed = 0
        for title, st in sorted(parse_stats.items()):
            p = st.get("parsed", 0)
            f = st.get("failed", 0)
            t = st.get("total", 0)
            total_parsed += p
            total_failed += f
            if t > 0:
                pct = p / t * 100 if t else 0
                lines.append(f"    {title:30s} {p:>5d}/{t:>5d} "
                             f"({pct:>5.1f}%) parsed")
        lines.append(f"    {'TOTAL':30s} {total_parsed:>5d} parsed, "
                     f"{total_failed} failed")
        lines.append("")

    lines += _section_h1(h1_results)
    lines += _section_h2(h2_results)
    lines += _section_summary(h1_results, h2_results)

    # Save
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_path = os.path.join(OUTPUT_DIR, "orderbook_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    all_results = {
        "h1_imbalance": h1_results,
        "h2_levels": h2_results,
        "parse_stats": parse_stats,
    }
    json_path = os.path.join(OUTPUT_DIR, "orderbook_results.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, default=str, ensure_ascii=False)

    print(f"Report: {report_path}")
    print(f"JSON:   {json_path}")
    return report_path, json_path
