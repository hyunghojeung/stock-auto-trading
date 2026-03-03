"""
rec_backtest.py - Recommended Stock Historical Pattern Backtest Engine
File: app/engine/rec_backtest.py
v1.0 - 2026-03-03
"""

import logging
import math
from typing import List, Dict, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

DEFAULT_STRATEGIES = {
    "smart": {"label": "S", "take_profit": 5.0, "stop_loss": 12.0, "max_hold_days": 30, "trailing": True, "trailing_pct": 5.0, "desc": "5%/12%/30d"},
    "aggressive": {"label": "A", "take_profit": 10.0, "stop_loss": 5.0, "max_hold_days": 5, "trailing": False, "trailing_pct": 0, "desc": "10/5/5d"},
    "standard": {"label": "B", "take_profit": 7.0, "stop_loss": 3.0, "max_hold_days": 10, "trailing": False, "trailing_pct": 0, "desc": "7/3/10d"},
    "conservative": {"label": "C", "take_profit": 5.0, "stop_loss": 2.0, "max_hold_days": 15, "trailing": False, "trailing_pct": 0, "desc": "5/2/15d"},
    "longterm": {"label": "L", "take_profit": 15.0, "stop_loss": 5.0, "max_hold_days": 30, "trailing": False, "trailing_pct": 0, "desc": "15/5/30d"},
}

FEE_RATE = 0.0021

def _z_normalize(seq):
    if not seq or len(seq) < 2:
        return list(seq) if seq else []
    mean = sum(seq) / len(seq)
    std = math.sqrt(sum((x - mean) ** 2 for x in seq) / len(seq))
    if std < 1e-10:
        return [0.0] * len(seq)
    return [(x - mean) / std for x in seq]

def _dtw_distance(s1, s2, window=None):
    n, m = len(s1), len(s2)
    if n == 0 or m == 0:
        return float('inf')
    if window is None:
        window = max(n, m)
    w = max(window, abs(n - m))
    dtw_m = [[float('inf')] * (m + 1) for _ in range(n + 1)]
    dtw_m[0][0] = 0.0
    for i in range(1, n + 1):
        for j in range(max(1, i - w), min(m, i + w) + 1):
            cost = abs(s1[i-1] - s2[j-1])
            dtw_m[i][j] = cost + min(dtw_m[i-1][j], dtw_m[i][j-1], dtw_m[i-1][j-1])
    return dtw_m[n][m]

def dtw_sim(s1, s2, normalize=True):
    if not s1 or not s2:
        return 0.0
    if normalize:
        s1 = _z_normalize(s1)
        s2 = _z_normalize(s2)
    dist = _dtw_distance(s1, s2)
    norm = max(len(s1), len(s2))
    if norm == 0:
        return 0.0
    return round(max(0.0, (1.0 - dist / norm / 3.0)) * 100, 2)

def _gc(candle, key):
    return candle.get(key, 0) if isinstance(candle, dict) else getattr(candle, key, 0)

def _extract_returns(candles, start_idx, length):
    result = []
    for k in range(length):
        idx = start_idx + k
        prev = idx - 1
        if prev < 0 or idx >= len(candles):
            result.append(0.0)
            continue
        pc = _gc(candles[prev], "close")
        cc = _gc(candles[idx], "close")
        result.append(round(((cc - pc) / pc) * 100, 4) if pc > 0 else 0.0)
    return result

def _extract_vol_ratios(candles, start_idx, length):
    result = []
    for k in range(length):
        idx = start_idx + k
        if idx >= len(candles):
            result.append(1.0)
            continue
        vol = _gc(candles[idx], "volume")
        vs = max(0, idx - 20)
        sl = candles[vs:idx]
        avg = sum(_gc(c, "volume") for c in sl) / len(sl) if sl else 1
        result.append(round(vol / avg, 4) if avg > 0 else 1.0)
    return result

def find_past_occurrences(candles, cluster_returns, cluster_volumes, pre_days=10, min_sim=55.0, min_gap=5, rw=0.6, vw=0.4):
    """과거 일봉에서 클러스터 패턴과 유사한 구간 모두 탐색"""
    if not candles or not cluster_returns:
        return []
    occs = []
    total = len(candles)
    last_idx = -min_gap
    for i in range(pre_days + 1, total - 20):
        if i - last_idx < min_gap:
            continue
        wr = _extract_returns(candles, i - pre_days, pre_days)
        wv = _extract_vol_ratios(candles, i - pre_days, pre_days)
        sr = dtw_sim(wr, cluster_returns)
        sv = dtw_sim(wv, cluster_volumes)
        tw = rw + vw
        sim = (rw * sr + vw * sv) / tw if tw > 0 else sr
        if sim >= min_sim:
            bp = _gc(candles[i], "close")
            if bp > 0:
                occs.append({"idx": i, "date": _gc(candles[i], "date"), "similarity": round(sim, 1), "buy_price": bp})
                last_idx = i
    return occs

def simulate_trade(candles, buy_idx, buy_price, tp=7.0, sl=3.0, max_days=10, trailing=False, trail_pct=5.0):
    """매수 시점부터 전략별 매매 시뮬레이션"""
    total = len(candles)
    max_p, max_l = 0.0, 0.0
    trail_high = buy_price
    for day in range(1, max_days + 1):
        idx = buy_idx + day
        if idx >= total:
            li = total - 1
            sp = _gc(candles[li], "close")
            rp = ((sp - buy_price) / buy_price) * 100 - FEE_RATE * 100
            return {"sell_idx": li, "sell_date": _gc(candles[li], "date"), "sell_price": sp, "return_pct": round(rp, 2), "hold_days": li - buy_idx, "exit_reason": "data_end", "max_profit_pct": round(max_p, 2), "max_loss_pct": round(max_l, 2)}
        h = _gc(candles[idx], "high")
        low = _gc(candles[idx], "low")
        cl = _gc(candles[idx], "close")
        hp = ((h - buy_price) / buy_price) * 100
        lp = ((low - buy_price) / buy_price) * 100
        max_p = max(max_p, hp)
        max_l = min(max_l, lp)
        if trailing:
            trail_high = max(trail_high, h)
            td = ((trail_high - low) / trail_high) * 100
            if td >= trail_pct and hp > 0:
                sp = trail_high * (1 - trail_pct / 100)
                rp = ((sp - buy_price) / buy_price) * 100 - FEE_RATE * 100
                return {"sell_idx": idx, "sell_date": _gc(candles[idx], "date"), "sell_price": round(sp), "return_pct": round(rp, 2), "hold_days": day, "exit_reason": "trailing", "max_profit_pct": round(max_p, 2), "max_loss_pct": round(max_l, 2)}
        if lp <= -sl:
            sp = buy_price * (1 - sl / 100)
            rp = -sl - FEE_RATE * 100
            return {"sell_idx": idx, "sell_date": _gc(candles[idx], "date"), "sell_price": round(sp), "return_pct": round(rp, 2), "hold_days": day, "exit_reason": "stop_loss", "max_profit_pct": round(max_p, 2), "max_loss_pct": round(max_l, 2)}
        if hp >= tp:
            sp = buy_price * (1 + tp / 100)
            rp = tp - FEE_RATE * 100
            return {"sell_idx": idx, "sell_date": _gc(candles[idx], "date"), "sell_price": round(sp), "return_pct": round(rp, 2), "hold_days": day, "exit_reason": "take_profit", "max_profit_pct": round(max_p, 2), "max_loss_pct": round(max_l, 2)}
    li = min(buy_idx + max_days, total - 1)
    sp = _gc(candles[li], "close")
    rp = ((sp - buy_price) / buy_price) * 100 - FEE_RATE * 100
    return {"sell_idx": li, "sell_date": _gc(candles[li], "date"), "sell_price": sp, "return_pct": round(rp, 2), "hold_days": li - buy_idx, "exit_reason": "max_hold", "max_profit_pct": round(max_p, 2), "max_loss_pct": round(max_l, 2)}

def _calc_pl_ratio(trades):
    wins = [t["return_pct"] for t in trades if t["return_pct"] > 0]
    losses = [abs(t["return_pct"]) for t in trades if t["return_pct"] <= 0]
    aw = sum(wins) / len(wins) if wins else 0
    al = sum(losses) / len(losses) if losses else 1
    return round(aw / al, 2) if al > 0 else 999.0

def backtest_recommended_stocks(recommendations, candles_by_code, clusters, pre_days=10, strategies=None, min_similarity=55.0, progress_callback=None):
    """
    Main: backtest recommended stocks against historical patterns
    """
    if strategies is None:
        strategies = DEFAULT_STRATEGIES
    stock_results = []
    all_strat_trades = {k: [] for k in strategies}
    total_recs = len(recommendations)

    for ri, rec in enumerate(recommendations):
        code = rec.get("code", "")
        name = rec.get("name", code)
        cid = rec.get("best_cluster_id", 0)
        sim = rec.get("similarity", 0)

        if progress_callback:
            progress_callback(int((ri / max(total_recs, 1)) * 100), f"[{ri+1}/{total_recs}] {name}")

        cluster = None
        if isinstance(clusters, list):
            for c in clusters:
                c_id = c.get("cluster_id", -1) if isinstance(c, dict) else getattr(c, "cluster_id", -1)
                if c_id == cid:
                    cluster = c
                    break
            if cluster is None and cid < len(clusters):
                cluster = clusters[cid]
        elif isinstance(clusters, dict):
            cluster = clusters.get(str(cid)) or clusters.get(cid)

        if not cluster:
            continue

        cr = cluster.get("avg_return_flow", []) if isinstance(cluster, dict) else getattr(cluster, "avg_return_flow", [])
        cv = cluster.get("avg_volume_flow", []) if isinstance(cluster, dict) else getattr(cluster, "avg_volume_flow", [])
        if not cr:
            continue

        candles = candles_by_code.get(code, [])
        if len(candles) < pre_days + 40:
            continue

        occs = find_past_occurrences(candles, cr, cv, pre_days=pre_days, min_sim=min_similarity)

        if not occs:
            stock_results.append({"code": code, "name": name, "current_similarity": sim, "occurrences": 0, "strategy_results": {}})
            continue

        strat_res = {}
        for sk, sc in strategies.items():
            trades = []
            for occ in occs:
                t = simulate_trade(candles, occ["idx"], occ["buy_price"], tp=sc["take_profit"], sl=sc["stop_loss"], max_days=sc["max_hold_days"], trailing=sc.get("trailing", False), trail_pct=sc.get("trailing_pct", 0))
                t["pattern_date"] = occ["date"]
                t["pattern_similarity"] = occ["similarity"]
                t["buy_price"] = occ["buy_price"]
                trades.append(t)
                all_strat_trades[sk].append(t)

            wins = [t for t in trades if t["return_pct"] > 0]
            rets = [t["return_pct"] for t in trades]
            strat_res[sk] = {
                "label": sc["label"], "desc": sc["desc"],
                "total_trades": len(trades), "wins": len(wins), "losses": len(trades) - len(wins),
                "win_rate": round(len(wins) / len(trades) * 100, 1) if trades else 0,
                "avg_return": round(sum(rets) / len(rets), 2) if rets else 0,
                "total_return": round(sum(rets), 2),
                "max_profit": round(max(rets), 2) if rets else 0,
                "max_loss": round(min(rets), 2) if rets else 0,
                "avg_hold_days": round(sum(t["hold_days"] for t in trades) / len(trades), 1) if trades else 0,
                "trades": trades,
            }

        best_sk = max(strat_res.items(), key=lambda x: x[1]["avg_return"])[0] if strat_res else None
        stock_results.append({
            "code": code, "name": name, "current_similarity": sim,
            "occurrences": len(occs),
            "occurrence_dates": [o["date"] for o in occs],
            "strategy_results": strat_res,
            "best_strategy": best_sk,
            "best_win_rate": strat_res[best_sk]["win_rate"] if best_sk else 0,
            "best_avg_return": strat_res[best_sk]["avg_return"] if best_sk else 0,
        })

    strat_summary = {}
    for sk, trades in all_strat_trades.items():
        if not trades:
            strat_summary[sk] = {"label": strategies[sk]["label"], "total_trades": 0, "win_rate": 0, "avg_return": 0}
            continue
        wins = [t for t in trades if t["return_pct"] > 0]
        rets = [t["return_pct"] for t in trades]
        cum, peak, mdd = 0, 0, 0
        for r in rets:
            cum += r
            peak = max(peak, cum)
            mdd = max(mdd, peak - cum)
        strat_summary[sk] = {
            "label": strategies[sk]["label"], "desc": strategies[sk]["desc"],
            "total_trades": len(trades), "wins": len(wins),
            "win_rate": round(len(wins) / len(trades) * 100, 1),
            "avg_return": round(sum(rets) / len(rets), 2),
            "total_return": round(sum(rets), 2),
            "max_profit": round(max(rets), 2), "max_loss": round(min(rets), 2),
            "mdd": round(mdd, 2), "profit_loss_ratio": _calc_pl_ratio(trades),
        }

    ranked = sorted(strat_summary.items(), key=lambda x: x[1].get("avg_return", 0), reverse=True)
    for rank, (k, _) in enumerate(ranked, 1):
        strat_summary[k]["rank"] = rank

    total_occ = sum(r["occurrences"] for r in stock_results)
    wr_stocks = [r for r in stock_results if r["occurrences"] > 0 and r.get("best_win_rate", 0) > 0]
    avg_wr = round(sum(r["best_win_rate"] for r in wr_stocks) / len(wr_stocks), 1) if wr_stocks else 0

    if progress_callback:
        progress_callback(100, f"backtest done: {total_occ} simulations")

    return {
        "stock_results": stock_results,
        "strategy_summary": strat_summary,
        "total_occurrences": total_occ,
        "total_stocks_tested": len([r for r in stock_results if r["occurrences"] > 0]),
        "avg_win_rate": avg_wr,
    }
