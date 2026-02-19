"""
스윙 매매 타이밍 패턴 통계 엔진 / Swing Timing Pattern Statistics Engine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
과거 1년 일봉 데이터에서 "성공한 진입점"을 역추적하여
어떤 조건에서 진입했을 때 승률이 높은지 통계적으로 분석합니다.

[변경사항 / Changes]
- MDD 계산: 단순 % 합산 → 자본금 기반 복리 계산 (절대 -100% 초과 안 함)
- Equity Curve: 자본금 기반 누적 수익률로 변경
- total_return: equity curve 기반으로 일관성 있게 계산

파일 경로: app/engine/swing_pattern_stats.py
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
            # "20260219" 형식 지원
            try:
                dt = datetime.strptime(date_str[:8], "%Y%m%d")
                weekday_names = ["월", "화", "수", "목", "금", "토", "일"]
                conds["weekday"] = weekday_names[dt.weekday()]
            except:
                pass

    return conds


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 패턴 통계 분석 / Pattern Statistics Analysis
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def analyze_timing_patterns(all_trades: List[TradeSimulation],
                           max_positions: int = 5) -> Dict:
    """
    전체 시뮬레이션 결과에서 타이밍 패턴별 승률/수익률 통계를 추출한다.

    ★ 포트폴리오 시뮬레이션 방식:
    - 동시 최대 max_positions 종목 보유
    - 자본금을 빈 슬롯 수로 분할하여 투입
    - 매도 시 원금+수익이 현금으로 복귀
    - 같은 날짜: 매도 먼저 → 매수 (슬롯 확보 후 투입)
    """
    if not all_trades:
        return {"total_trades": 0}

    total = len(all_trades)
    wins = [t for t in all_trades if t.is_win]
    losses = [t for t in all_trades if not t.is_win]

    # ── 포트폴리오 시뮬레이션 (★ 전면 교체) ──
    # 1) 매매 이벤트 생성 (진입/청산)
    events = []
    for i, t in enumerate(all_trades):
        entry_d = t.entry_date[:8].replace("-", "") if t.entry_date else "00000000"
        exit_d = t.exit_date[:8].replace("-", "") if t.exit_date else "99999999"
        # 같은 날짜면 매도(0)를 매수(1)보다 먼저 처리
        events.append(("entry", entry_d, i, t))
        events.append(("exit", exit_d, i, t))

    events.sort(key=lambda e: (e[1], 0 if e[0] == "exit" else 1))

    INITIAL_CAPITAL = 100.0
    cash = INITIAL_CAPITAL
    active = {}  # trade_index → invested_amount
    portfolio_values = [INITIAL_CAPITAL]
    executed_trades = 0
    skipped_trades = 0

    for event_type, date_str, trade_idx, trade in events:
        if event_type == "exit" and trade_idx in active:
            # ── 매도: 원금 + 수익 현금화 ──
            invested = active.pop(trade_idx)
            returned = invested * (1 + trade.profit_pct / 100)
            cash += returned
            executed_trades += 1

            # 포트폴리오 가치 기록 (매도 시점)
            total_value = cash + sum(active.values())
            portfolio_values.append(round(total_value, 4))

        elif event_type == "entry" and trade_idx not in active:
            # ── 매수: 빈 슬롯이 있고 현금이 있을 때만 ──
            if len(active) < max_positions and cash > 1.0:
                # 현금을 빈 슬롯 수로 분할
                available_slots = max_positions - len(active)
                invest_amount = cash / available_slots
                active[trade_idx] = invest_amount
                cash -= invest_amount
            else:
                skipped_trades += 1

    # 남은 포지션 강제 청산 (시뮬레이션 종료)
    for trade_idx, invested in list(active.items()):
        # 해당 매매의 수익률 적용
        trade = all_trades[trade_idx]
        returned = invested * (1 + trade.profit_pct / 100)
        cash += returned
    active.clear()

    final_capital = cash
    total_return_compound = round(final_capital - INITIAL_CAPITAL, 2)

    # ── 에퀴티 커브 (% 기준) ──
    equity = [round(v - INITIAL_CAPITAL, 2) for v in portfolio_values]

    # ── MDD 계산 (포트폴리오 가치 기반) ──
    peak = portfolio_values[0]
    max_drawdown = 0.0
    for v in portfolio_values:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (v - peak) / peak * 100
            if dd < max_drawdown:
                max_drawdown = dd

    # ── 매매 기간 계산 (실제 날짜 기반) ──
    trading_period_days = 0
    first_date_str = ""
    last_date_str = ""
    sorted_trades = sorted(all_trades, key=lambda x: x.entry_date if x.entry_date else "")
    if sorted_trades:
        all_dates = []
        for t in sorted_trades:
            if t.entry_date:
                all_dates.append(t.entry_date)
            if t.exit_date:
                all_dates.append(t.exit_date)
        if all_dates:
            all_dates.sort()
            first_date_str = all_dates[0]
            last_date_str = all_dates[-1]
            try:
                from datetime import datetime as _dt
                d1 = _dt.strptime(first_date_str[:8].replace("-", ""), "%Y%m%d")
                d2 = _dt.strptime(last_date_str[:8].replace("-", ""), "%Y%m%d")
                trading_period_days = (d2 - d1).days
            except:
                trading_period_days = 0

    # ── 연환산 수익률 ──
    annualized_return = 0.0
    if trading_period_days > 30 and final_capital > 0:
        years = trading_period_days / 365
        if years > 0:
            try:
                annualized_return = round(
                    ((final_capital / INITIAL_CAPITAL) ** (1 / years) - 1) * 100, 2
                )
            except:
                annualized_return = 0.0

    # ── 일평균 수익률 ──
    daily_return = 0.0
    if trading_period_days > 0:
        daily_return = round(total_return_compound / trading_period_days, 4)

    # ── 전체 요약 ──
    summary = {
        "total_trades": total,
        "executed_trades": executed_trades,          # ★ 추가: 실제 체결된 매매
        "skipped_trades": skipped_trades,            # ★ 추가: 슬롯 부족으로 건너뛴 매매
        "max_positions": max_positions,              # ★ 추가: 동시 최대 종목 수
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": round(len(wins) / total * 100, 1),
        "avg_profit": round(sum(t.profit_pct for t in all_trades) / total, 2),
        "avg_win": round(sum(t.profit_pct for t in wins) / len(wins), 2) if wins else 0,
        "avg_loss": round(sum(t.profit_pct for t in losses) / len(losses), 2) if losses else 0,
        "max_profit": round(max(t.profit_pct for t in all_trades), 2),
        "max_loss": round(min(t.profit_pct for t in all_trades), 2),
        "avg_holding_days": round(sum(t.holding_days for t in all_trades) / total, 1),
        "total_return": total_return_compound,       # ★ 포트폴리오 시뮬레이션 기반
        "mdd": round(max_drawdown, 2),               # ★ 포트폴리오 가치 기반
        "trading_period_days": trading_period_days,
        "first_trade_date": first_date_str,
        "last_trade_date": last_date_str,
        "annualized_return": annualized_return,
        "daily_return": daily_return,
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

    # ── 샤프 비율 ──
    profits = [t.profit_pct for t in all_trades]
    if len(profits) > 1:
        avg_ret = sum(profits) / len(profits)
        std_ret = (sum((p - avg_ret) ** 2 for p in profits) / (len(profits) - 1)) ** 0.5
        summary["sharpe_ratio"] = round(avg_ret / std_ret, 2) if std_ret > 0 else 0
    else:
        summary["sharpe_ratio"] = 0

    # ── 손익비 (★ 추가) ──
    if summary["avg_loss"] != 0:
        summary["profit_loss_ratio"] = round(
            abs(summary["avg_win"] / summary["avg_loss"]), 2
        )
    else:
        summary["profit_loss_ratio"] = 0

    # ── 종목별 성과 통계 / Per-Stock Performance ──
    stock_groups = defaultdict(list)
    for t in all_trades:
        code = t.entry_conditions.get("stock_code", "?")
        stock_groups[code].append(t)

    stock_stats = []
    for code, trades_list in stock_groups.items():
        s_wins = [t for t in trades_list if t.is_win]

        # ★ 수정: 종목별도 복리 수익률 계산
        s_capital = 100.0
        for t in sorted(trades_list, key=lambda x: x.entry_idx):
            s_capital = s_capital * (1 + t.profit_pct / 100)
        s_total_return = round(s_capital - 100, 2)

        s_dates = sorted([t.entry_date for t in trades_list if t.entry_date]
                         + [t.exit_date for t in trades_list if t.exit_date])
        # 매매 기간 계산 (첫 진입 ~ 마지막 청산)
        trade_days = 0
        if len(s_dates) >= 2:
            try:
                from datetime import datetime as _dt
                # "20260219" 또는 "2026-02-19" 형식 모두 지원
                d1_str = s_dates[0][:10].replace("-", "")
                d2_str = s_dates[-1][:10].replace("-", "")
                d1 = _dt.strptime(d1_str[:8], "%Y%m%d")
                d2 = _dt.strptime(d2_str[:8], "%Y%m%d")
                trade_days = (d2 - d1).days
            except:
                trade_days = sum(t.holding_days for t in trades_list)

        # ★ 수정: stock_name 가져오기 (fallback: code)
        stock_name = code
        for t in trades_list:
            name = t.entry_conditions.get("stock_name", "")
            if name and name != code:
                stock_name = name
                break

        stock_stats.append({
            "code": code,
            "name": stock_name,  # ★ 수정: 종목명 올바르게 표시
            "total_trades": len(trades_list),
            "win_count": len(s_wins),
            "loss_count": len(trades_list) - len(s_wins),
            "win_rate": round(len(s_wins) / len(trades_list) * 100, 1),
            "total_return": s_total_return,  # ★ 수정: 복리 기반
            "avg_profit": round(sum(t.profit_pct for t in trades_list) / len(trades_list), 2),
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
        "equity_curve": equity,  # ★ 수정: 자본금 기반 누적 수익률
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
        """파라미터로 전체 백테스트 실행 → 점수 계산 (포트폴리오 시뮬레이션)"""
        all_trades = []
        for code, candles in candles_dict.items():
            trades = run_swing_backtest(candles, params)
            for t in trades:
                t.entry_conditions["stock_code"] = code
            all_trades.extend(trades)

        if len(all_trades) < 5:
            return -999, {}

        wins = [t for t in all_trades if t.is_win]
        total = len(all_trades)
        win_rate = len(wins) / total
        avg_profit = sum(t.profit_pct for t in all_trades) / total

        # ★ 포트폴리오 시뮬레이션 (동시 최대 5종목)
        MAX_POS = 5
        events = []
        for i, t in enumerate(all_trades):
            ed = t.entry_date[:8].replace("-", "") if t.entry_date else "00000000"
            xd = t.exit_date[:8].replace("-", "") if t.exit_date else "99999999"
            events.append(("entry", ed, i, t))
            events.append(("exit", xd, i, t))
        events.sort(key=lambda e: (e[1], 0 if e[0] == "exit" else 1))

        cash = 100.0
        active = {}
        peak = 100.0
        mdd = 0.0

        for etype, _, tidx, trade in events:
            if etype == "exit" and tidx in active:
                invested = active.pop(tidx)
                cash += invested * (1 + trade.profit_pct / 100)
                total_val = cash + sum(active.values())
                if total_val > peak:
                    peak = total_val
                if peak > 0:
                    dd = (total_val - peak) / peak * 100
                    if dd < mdd:
                        mdd = dd
            elif etype == "entry" and tidx not in active:
                if len(active) < MAX_POS and cash > 1.0:
                    avail = MAX_POS - len(active)
                    amt = cash / avail
                    active[tidx] = amt
                    cash -= amt

        # 남은 포지션 청산
        for tidx, inv in list(active.items()):
            cash += inv * (1 + all_trades[tidx].profit_pct / 100)
        total_return = round(cash - 100, 2)

        # 종합 점수: 수익률 * 승률 - MDD 패널티
        score = total_return * win_rate - abs(mdd) * 0.5

        return score, {
            "total_trades": total,
            "win_rate": round(win_rate * 100, 1),
            "avg_profit": round(avg_profit, 2),
            "total_return": total_return,
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
