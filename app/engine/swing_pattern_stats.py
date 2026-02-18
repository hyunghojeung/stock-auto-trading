"""
스윙 매매 타이밍 패턴 통계 엔진 / Swing Timing Pattern Statistics Engine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
과거 1년 일봉 데이터에서 "성공한 진입점"을 역추적하여
어떤 조건에서 진입했을 때 승률이 높은지 통계적으로 분석합니다.
"""

from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass, field
import math

from app.engine.swing_discoverer import (
    calc_ma, calc_rsi, calc_bollinger, calc_atr, detect_candle_pattern
)


@dataclass
class TradeSimulation:
    """가상 매매 시뮬레이션 결과"""
    entry_idx: int
    entry_date: str
    entry_price: float
    exit_idx: int
    exit_date: str
    exit_price: float
    profit_pct: float
    holding_days: int
    is_win: bool
    exit_reason: str  # "trailing_stop" / "stop_loss" / "max_hold"
    # 진입 시 조건
    entry_conditions: Dict = field(default_factory=dict)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 스윙 백테스트 엔진 / Swing Backtest Engine
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_swing_backtest(
    candles: List[Dict],
    params: Dict = None
) -> List[TradeSimulation]:
    """
    일봉 데이터에 대해 스윙 매매 시뮬레이션을 실행한다.

    params:
        pullback_min: 최소 눌림 % (기본 3)
        pullback_max: 최대 눌림 % (기본 8)
        trailing_pct: 트레일링 스톱 % (기본 5)
        stop_loss_pct: 손절 % (기본 -7)
        max_hold_days: 최대 보유일 (기본 30)
        require_pattern: 봉 패턴 필수 여부 (기본 False)
        require_ma_align: MA 정배열 필수 (기본 True)
        require_volume: 거래량 조건 필수 (기본 False)
        commission_pct: 수수료 % (기본 0.015 편도)
        tax_pct: 매도세 % (기본 0.18)
    """
    if params is None:
        params = {}

    pullback_min = params.get("pullback_min", 3.0)
    pullback_max = params.get("pullback_max", 8.0)
    trailing_pct = params.get("trailing_pct", 5.0)
    stop_loss_pct = params.get("stop_loss_pct", -7.0)
    max_hold_days = params.get("max_hold_days", 30)
    require_pattern = params.get("require_pattern", False)
    require_ma_align = params.get("require_ma_align", True)
    require_volume = params.get("require_volume", False)
    commission = params.get("commission_pct", 0.015)
    tax = params.get("tax_pct", 0.18)

    if len(candles) < 60:
        return []

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    ma5 = calc_ma(closes, 5)
    ma20 = calc_ma(closes, 20)
    ma60 = calc_ma(closes, 60)
    rsi = calc_rsi(closes)
    _, _, _, bb_pos = calc_bollinger(closes)
    atr = calc_atr(highs, lows, closes)

    trades = []
    in_trade = False
    entry_idx = 0
    entry_price = 0
    highest_since_entry = 0

    for i in range(60, len(candles)):
        if in_trade:
            # ── 보유 중: 매도 판단 ──
            current = closes[i]
            highest_since_entry = max(highest_since_entry, highs[i])
            holding_days = i - entry_idx

            # 트레일링 스톱
            trail_trigger = highest_since_entry * (1 - trailing_pct / 100)
            if current <= trail_trigger:
                exit_price = current
                raw_pct = (exit_price - entry_price) / entry_price * 100
                net_pct = raw_pct - commission * 2 - tax
                trades.append(TradeSimulation(
                    entry_idx=entry_idx,
                    entry_date=candles[entry_idx].get("date", ""),
                    entry_price=entry_price,
                    exit_idx=i,
                    exit_date=candles[i].get("date", ""),
                    exit_price=exit_price,
                    profit_pct=round(net_pct, 2),
                    holding_days=holding_days,
                    is_win=net_pct > 0,
                    exit_reason="trailing_stop",
                    entry_conditions=_get_entry_conditions(
                        candles, entry_idx, closes, ma5, ma20, ma60,
                        rsi, bb_pos, volumes, atr
                    ),
                ))
                in_trade = False
                continue

            # 손절
            loss_pct = (current - entry_price) / entry_price * 100
            if loss_pct <= stop_loss_pct:
                exit_price = current
                raw_pct = (exit_price - entry_price) / entry_price * 100
                net_pct = raw_pct - commission * 2 - tax
                trades.append(TradeSimulation(
                    entry_idx=entry_idx,
                    entry_date=candles[entry_idx].get("date", ""),
                    entry_price=entry_price,
                    exit_idx=i,
                    exit_date=candles[i].get("date", ""),
                    exit_price=exit_price,
                    profit_pct=round(net_pct, 2),
                    holding_days=holding_days,
                    is_win=False,
                    exit_reason="stop_loss",
                    entry_conditions=_get_entry_conditions(
                        candles, entry_idx, closes, ma5, ma20, ma60,
                        rsi, bb_pos, volumes, atr
                    ),
                ))
                in_trade = False
                continue

            # 최대 보유일 초과
            if holding_days >= max_hold_days:
                exit_price = current
                raw_pct = (exit_price - entry_price) / entry_price * 100
                net_pct = raw_pct - commission * 2 - tax
                trades.append(TradeSimulation(
                    entry_idx=entry_idx,
                    entry_date=candles[entry_idx].get("date", ""),
                    entry_price=entry_price,
                    exit_idx=i,
                    exit_date=candles[i].get("date", ""),
                    exit_price=exit_price,
                    profit_pct=round(net_pct, 2),
                    holding_days=holding_days,
                    is_win=net_pct > 0,
                    exit_reason="max_hold",
                    entry_conditions=_get_entry_conditions(
                        candles, entry_idx, closes, ma5, ma20, ma60,
                        rsi, bb_pos, volumes, atr
                    ),
                ))
                in_trade = False
                continue

        else:
            # ── 미보유: 매수 판단 ──
            # 눌림 % 계산
            lookback = 20
            recent_high = max(closes[max(0, i - lookback):i + 1])
            pullback = (recent_high - closes[i]) / recent_high * 100

            if not (pullback_min <= pullback <= pullback_max):
                continue

            # MA 정배열 체크
            if require_ma_align:
                if not (ma5[i] and ma20[i] and ma5[i] > ma20[i]):
                    continue
                if ma20[i] and ma60[i] and ma20[i] < ma60[i]:
                    continue

            # 거래량 조건
            if require_volume and i >= 20:
                vol_5 = sum(volumes[i - 4:i + 1]) / 5
                vol_20 = sum(volumes[i - 19:i + 1]) / 20
                if vol_20 > 0 and vol_5 / vol_20 < 1.3:
                    continue

            # 봉 패턴 조건
            if require_pattern:
                pattern = detect_candle_pattern(candles, i)
                if not pattern:
                    continue

            # 매수 진입
            entry_price = closes[i]
            entry_idx = i
            highest_since_entry = highs[i]
            in_trade = True

    return trades


def _get_entry_conditions(candles, idx, closes, ma5, ma20, ma60,
                          rsi, bb_pos, volumes, atr) -> Dict:
    """진입 시점의 조건을 기록"""
    conds = {}

    # MA 배열
    if ma5[idx] and ma20[idx]:
        conds["ma5_above_ma20"] = ma5[idx] > ma20[idx]
    if ma20[idx] and ma60[idx]:
        conds["ma20_above_ma60"] = ma20[idx] > ma60[idx]

    # 거래량
    if idx >= 20:
        vol_5 = sum(volumes[idx - 4:idx + 1]) / 5
        vol_20 = sum(volumes[idx - 19:idx + 1]) / 20
        conds["volume_ratio"] = round(vol_5 / vol_20, 2) if vol_20 > 0 else 1.0

    # RSI
    if rsi[idx] is not None:
        conds["rsi"] = round(rsi[idx], 1)

    # 눌림 %
    lookback = 20
    recent_high = max(closes[max(0, idx - lookback):idx + 1])
    conds["pullback_pct"] = round((recent_high - closes[idx]) / recent_high * 100, 2)

    # 볼린저
    if bb_pos[idx] is not None:
        conds["bb_position"] = round(bb_pos[idx], 3)

    # 봉 패턴
    conds["candle_pattern"] = detect_candle_pattern(candles, idx)

    # 요일 (0=월~6=일)
    date_str = candles[idx].get("date", "")
    if date_str:
        try:
            from datetime import datetime
            dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            weekday_names = ["월", "화", "수", "목", "금", "토", "일"]
            conds["weekday"] = weekday_names[dt.weekday()]
        except:
            pass

    return conds


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 패턴 통계 분석 / Pattern Statistics Analysis
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def analyze_timing_patterns(all_trades: List[TradeSimulation]) -> Dict:
    """
    전체 시뮬레이션 결과에서 타이밍 패턴별 승률/수익률 통계를 추출한다.
    """
    if not all_trades:
        return {"total_trades": 0}

    total = len(all_trades)
    wins = [t for t in all_trades if t.is_win]
    losses = [t for t in all_trades if not t.is_win]

    # ── 전체 요약 ──
    summary = {
        "total_trades": total,
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": round(len(wins) / total * 100, 1),
        "avg_profit": round(sum(t.profit_pct for t in all_trades) / total, 2),
        "avg_win": round(sum(t.profit_pct for t in wins) / len(wins), 2) if wins else 0,
        "avg_loss": round(sum(t.profit_pct for t in losses) / len(losses), 2) if losses else 0,
        "max_profit": round(max(t.profit_pct for t in all_trades), 2),
        "max_loss": round(min(t.profit_pct for t in all_trades), 2),
        "avg_holding_days": round(sum(t.holding_days for t in all_trades) / total, 1),
        "total_return": round(sum(t.profit_pct for t in all_trades), 2),
    }

    # ── 눌림 % 구간별 승률 ──
    pullback_stats = _analyze_by_range(
        all_trades,
        lambda t: t.entry_conditions.get("pullback_pct", 0),
        ranges=[(0, 2, "0~2%"), (2, 4, "2~4%"), (4, 6, "4~6%"),
                (6, 8, "6~8%"), (8, 12, "8~12%"), (12, 100, "12%+")]
    )

    # ── 거래량 배수별 승률 ──
    volume_stats = _analyze_by_range(
        all_trades,
        lambda t: t.entry_conditions.get("volume_ratio", 1.0),
        ranges=[(0, 0.8, "감소(<0.8)"), (0.8, 1.2, "보통(0.8~1.2)"),
                (1.2, 1.5, "증가(1.2~1.5)"), (1.5, 2.0, "급증(1.5~2.0)"),
                (2.0, 100, "폭증(2.0+)")]
    )

    # ── RSI 구간별 승률 ──
    rsi_stats = _analyze_by_range(
        all_trades,
        lambda t: t.entry_conditions.get("rsi", 50),
        ranges=[(0, 30, "과매도(~30)"), (30, 40, "30~40"),
                (40, 50, "40~50"), (50, 60, "50~60"),
                (60, 70, "60~70"), (70, 100, "과매수(70~)")]
    )

    # ── 봉 패턴별 승률 ──
    pattern_stats = _analyze_by_category(
        all_trades,
        lambda t: t.entry_conditions.get("candle_pattern", "없음") or "없음"
    )

    # ── 요일별 승률 ──
    weekday_stats = _analyze_by_category(
        all_trades,
        lambda t: t.entry_conditions.get("weekday", "?")
    )

    # ── MA 배열별 승률 ──
    ma_stats = _analyze_by_category(
        all_trades,
        lambda t: "정배열" if t.entry_conditions.get("ma5_above_ma20", False) else "역배열"
    )

    # ── 볼린저 위치별 승률 ──
    bb_stats = _analyze_by_range(
        all_trades,
        lambda t: t.entry_conditions.get("bb_position", 0.5),
        ranges=[(0, 0.2, "극하단(~0.2)"), (0.2, 0.4, "하단(0.2~0.4)"),
                (0.4, 0.6, "중간(0.4~0.6)"), (0.6, 0.8, "상단(0.6~0.8)"),
                (0.8, 1.0, "극상단(0.8~)")]
    )

    # ── 매도 사유별 통계 ──
    exit_stats = _analyze_by_category(
        all_trades,
        lambda t: {
            "trailing_stop": "트레일링 스톱",
            "stop_loss": "손절",
            "max_hold": "최대보유일 초과",
        }.get(t.exit_reason, t.exit_reason)
    )

    # ── MDD 계산 ──
    equity = [0]
    for t in sorted(all_trades, key=lambda x: x.entry_idx):
        equity.append(equity[-1] + t.profit_pct)
    peak = equity[0]
    mdd = 0
    for e in equity:
        peak = max(peak, e)
        dd = e - peak
        mdd = min(mdd, dd)

    summary["mdd"] = round(mdd, 2)

    # ── 샤프 비율 ──
    profits = [t.profit_pct for t in all_trades]
    if len(profits) > 1:
        avg_ret = sum(profits) / len(profits)
        std_ret = (sum((p - avg_ret) ** 2 for p in profits) / (len(profits) - 1)) ** 0.5
        summary["sharpe_ratio"] = round(avg_ret / std_ret, 2) if std_ret > 0 else 0
    else:
        summary["sharpe_ratio"] = 0

    # ── 종목별 성과 통계 / Per-Stock Performance ──
    stock_groups = defaultdict(list)
    for t in all_trades:
        code = t.entry_conditions.get("stock_code", "?")
        stock_groups[code].append(t)

    stock_stats = []
    for code, trades_list in stock_groups.items():
        s_wins = [t for t in trades_list if t.is_win]
        s_total_return = sum(t.profit_pct for t in trades_list)
        s_dates = sorted([t.entry_date for t in trades_list if t.entry_date]
                         + [t.exit_date for t in trades_list if t.exit_date])
        # 매매 기간 계산 (첫 진입 ~ 마지막 청산)
        trade_days = 0
        if len(s_dates) >= 2:
            try:
                from datetime import datetime as _dt
                d1 = _dt.strptime(s_dates[0][:10], "%Y-%m-%d")
                d2 = _dt.strptime(s_dates[-1][:10], "%Y-%m-%d")
                trade_days = (d2 - d1).days
            except:
                trade_days = sum(t.holding_days for t in trades_list)

        stock_stats.append({
            "code": code,
            "name": trades_list[0].entry_conditions.get("stock_name", code),
            "total_trades": len(trades_list),
            "win_count": len(s_wins),
            "loss_count": len(trades_list) - len(s_wins),
            "win_rate": round(len(s_wins) / len(trades_list) * 100, 1),
            "total_return": round(s_total_return, 2),
            "avg_profit": round(s_total_return / len(trades_list), 2),
            "max_profit": round(max(t.profit_pct for t in trades_list), 2),
            "max_loss": round(min(t.profit_pct for t in trades_list), 2),
            "trade_period_days": trade_days,
            "avg_holding_days": round(
                sum(t.holding_days for t in trades_list) / len(trades_list), 1
            ),
        })

    # 총 수익률 내림차순 정렬
    stock_stats.sort(key=lambda x: x["total_return"], reverse=True)

    return {
        "summary": summary,
        "pullback_stats": pullback_stats,
        "volume_stats": volume_stats,
        "rsi_stats": rsi_stats,
        "pattern_stats": pattern_stats,
        "weekday_stats": weekday_stats,
        "ma_stats": ma_stats,
        "bb_stats": bb_stats,
        "exit_stats": exit_stats,
        "equity_curve": equity,
        "stock_stats": stock_stats,
    }


def _analyze_by_range(trades, key_fn, ranges):
    """구간별 승률/수익률 분석"""
    stats = []
    for low, high, label in ranges:
        group = [t for t in trades if low <= key_fn(t) < high]
        if not group:
            stats.append({"range": label, "count": 0, "win_rate": 0,
                          "avg_profit": 0, "avg_holding_days": 0})
            continue
        wins = [t for t in group if t.is_win]
        stats.append({
            "range": label,
            "count": len(group),
            "win_rate": round(len(wins) / len(group) * 100, 1),
            "avg_profit": round(sum(t.profit_pct for t in group) / len(group), 2),
            "avg_holding_days": round(sum(t.holding_days for t in group) / len(group), 1),
        })
    return stats


def _analyze_by_category(trades, key_fn):
    """카테고리별 승률/수익률 분석"""
    groups = defaultdict(list)
    for t in trades:
        groups[key_fn(t)].append(t)

    stats = []
    for cat, group in sorted(groups.items(), key=lambda x: -len(x[1])):
        wins = [t for t in group if t.is_win]
        stats.append({
            "category": cat,
            "count": len(group),
            "win_rate": round(len(wins) / len(group) * 100, 1),
            "avg_profit": round(sum(t.profit_pct for t in group) / len(group), 2),
            "avg_holding_days": round(sum(t.holding_days for t in group) / len(group), 1),
        })
    return stats


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 자동 교정 / Auto Calibration
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def auto_calibrate(candles_dict: Dict[str, List[Dict]],
                   generations: int = 10,
                   population_size: int = 20) -> Dict:
    """
    여러 파라미터 조합을 테스트하여 최적 파라미터를 찾는다.
    진화 알고리즘 방식.

    candles_dict: {"종목코드": [일봉데이터]}
    """
    import random

    # 초기 파라미터 범위
    param_ranges = {
        "pullback_min": (1.0, 5.0),
        "pullback_max": (5.0, 12.0),
        "trailing_pct": (3.0, 10.0),
        "stop_loss_pct": (-10.0, -3.0),
        "max_hold_days": (10, 40),
    }

    def random_params():
        return {
            k: round(random.uniform(v[0], v[1]), 1)
            if isinstance(v[0], float) else random.randint(v[0], v[1])
            for k, v in param_ranges.items()
        }

    def mutate(params, mutation_rate=0.3):
        new_params = params.copy()
        for k, (lo, hi) in param_ranges.items():
            if random.random() < mutation_rate:
                if isinstance(lo, float):
                    delta = (hi - lo) * 0.15 * random.choice([-1, 1])
                    new_params[k] = round(max(lo, min(hi, params[k] + delta)), 1)
                else:
                    delta = max(1, int((hi - lo) * 0.15))
                    new_params[k] = max(lo, min(hi, params[k] + random.randint(-delta, delta)))
        return new_params

    def evaluate(params):
        """파라미터로 전체 백테스트 실행 → 점수 계산"""
        all_trades = []
        for code, candles in candles_dict.items():
            trades = run_swing_backtest(candles, params)
            all_trades.extend(trades)

        if len(all_trades) < 5:
            return -999, {}

        wins = [t for t in all_trades if t.is_win]
        total = len(all_trades)
        win_rate = len(wins) / total
        avg_profit = sum(t.profit_pct for t in all_trades) / total
        total_return = sum(t.profit_pct for t in all_trades)

        # MDD
        equity = [0]
        for t in sorted(all_trades, key=lambda x: x.entry_idx):
            equity.append(equity[-1] + t.profit_pct)
        peak = 0
        mdd = 0
        for e in equity:
            peak = max(peak, e)
            mdd = min(mdd, e - peak)

        # 종합 점수: 수익률 * 승률 - MDD 패널티
        score = total_return * win_rate - abs(mdd) * 0.5

        return score, {
            "total_trades": total,
            "win_rate": round(win_rate * 100, 1),
            "avg_profit": round(avg_profit, 2),
            "total_return": round(total_return, 2),
            "mdd": round(mdd, 2),
        }

    # ── 진화 실행 ──
    generation_history = []

    # 1세대: 랜덤 초기화
    population = [random_params() for _ in range(population_size)]

    for gen in range(1, generations + 1):
        # 평가
        scored = []
        for params in population:
            score, metrics = evaluate(params)
            scored.append((score, params, metrics))
        scored.sort(key=lambda x: x[0], reverse=True)

        # 세대 기록
        best_score, best_params, best_metrics = scored[0]
        generation_history.append({
            "generation": gen,
            "best_score": round(best_score, 2),
            "best_params": best_params,
            "best_metrics": best_metrics,
            "avg_score": round(sum(s[0] for s in scored) / len(scored), 2),
        })

        # 상위 25% 생존
        survivors = [s[1] for s in scored[:max(2, population_size // 4)]]

        # 다음 세대 생성
        new_population = list(survivors)  # 엘리트 보존
        while len(new_population) < population_size:
            parent = random.choice(survivors)
            child = mutate(parent)
            new_population.append(child)

        population = new_population

    # 최종 결과
    final_best = generation_history[-1]
    return {
        "best_params": final_best["best_params"],
        "best_metrics": final_best["best_metrics"],
        "best_score": final_best["best_score"],
        "generations": generation_history,
        "total_generations": generations,
    }
