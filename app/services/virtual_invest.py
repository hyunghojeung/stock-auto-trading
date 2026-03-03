"""
가상투자 시뮬레이션 엔진 / Virtual Investment Simulation Engine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/services/virtual_invest.py

패턴분석기 매수추천 종목을 5가지 전략으로 동시 백테스트하고,
실시간 모의투자를 지원하는 엔진.

매매 규칙:
- 자본금 분할 투자 (자본금 ÷ 최대보유수)
- 익절/손절/최대보유일 기반 청산
- 수수료 0.015% + 매도세 0.23% 반영
"""

import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 상수 / Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMMISSION_RATE = 0.00015    # 매수/매도 수수료 0.015%
SELL_TAX_RATE = 0.0023       # 매도세 0.23%
MAX_POSITIONS = 10           # 최대 동시 보유 종목수
DEFAULT_CAPITAL = 1_000_000  # 기본 자본금

# 프리셋 전략 정의 / Preset Strategies
STRATEGY_PRESETS = {
    "aggressive": {
        "name": "🔥 공격형",
        "name_en": "Aggressive",
        "take_profit_pct": 10.0,
        "stop_loss_pct": 5.0,
        "max_hold_days": 5,
        "color": "#ff5252",
    },
    "standard": {
        "name": "⚖️ 기본형",
        "name_en": "Standard",
        "take_profit_pct": 7.0,
        "stop_loss_pct": 3.0,
        "max_hold_days": 10,
        "color": "#4fc3f7",
    },
    "conservative": {
        "name": "🛡️ 보수형",
        "name_en": "Conservative",
        "take_profit_pct": 5.0,
        "stop_loss_pct": 2.0,
        "max_hold_days": 15,
        "color": "#4cff8b",
    },
    "longterm": {
        "name": "🐢 장기형",
        "name_en": "Long-term",
        "take_profit_pct": 15.0,
        "stop_loss_pct": 5.0,
        "max_hold_days": 30,
        "color": "#ffd54f",
    },
    "custom": {
        "name": "🎛️ 커스텀",
        "name_en": "Custom",
        "take_profit_pct": 7.0,  # 사용자가 덮어쓰기
        "stop_loss_pct": 3.0,
        "max_hold_days": 10,
        "color": "#ce93d8",
    },
    "smart": {
        "name": "🧠 스마트형",
        "name_en": "Smart",
        "take_profit_pct": 0.0,    # 트레일링 스탑 사용 (고정 익절 없음)
        "stop_loss_pct": 12.0,     # 종가 기준 손절 (급등 전 눌림목 보호)
        "max_hold_days": 30,       # 최대 보유일 확대 (급등 대기)
        "trailing_stop_pct": 5.0,  # 최고가 대비 -5% 하락 시 매도 (여유)
        "grace_days": 7,           # 매수 후 7일간 손절 유예 (눌림목 보호)
        "profit_activation_pct": 15.0,  # ★ 15% 수익 달성 후에만 추적손절 활성화
        "use_close_stop": True,    # 종가 기준 손절 (장중 저점 무시)
        "color": "#ff9800",
    },
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 데이터 클래스 / Data Classes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@dataclass
class TradeResult:
    """개별 매매 결과 / Individual trade result"""
    stock_code: str
    stock_name: str
    buy_price: float
    buy_date: str
    sell_price: float
    sell_date: str
    profit_pct: float       # 수수료/세금 차감 후
    profit_won: int         # 원화 수익금
    hold_days: int
    result: str             # 'profit' | 'loss' | 'timeout' | 'trailing'
    invest_amount: float    # 투자금액
    re_entry_num: int = 0   # 재진입 번호 (0=최초, 1=1차재진입, ...)


@dataclass
class DailySnapshot:
    """일별 자산 스냅샷 / Daily asset snapshot"""
    date: str
    day_num: int
    total_asset: float
    cash: float
    holding_value: float


@dataclass
class StrategyResult:
    """전략별 시뮬레이션 결과 / Strategy simulation result"""
    strategy: str
    strategy_name: str
    color: str
    take_profit_pct: float
    stop_loss_pct: float
    max_hold_days: int
    # 요약
    initial_capital: float
    final_asset: float
    total_return_pct: float
    total_return_won: int
    win_rate: float
    win_count: int
    loss_count: int
    total_trades: int
    mdd_pct: float
    risk_reward_ratio: float
    score: float            # 종합점수
    ranking: int
    # 상세
    trades: List[Dict]
    daily_assets: List[Dict]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 일봉 데이터 수집 / Daily Candle Data
# 기존 naver_stock.py 재사용 (requests 기반, Railway에서 안정 동작)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
from app.services.naver_stock import get_daily_candles_naver

def _find_last_surge_start(candles: List[Dict], threshold_pct: float = 20.0) -> tuple:
    """
    캔들 데이터에서 가장 최근 급상승(threshold% 이상)의 시작점을 찾는다.
    Find the most recent surge start point from candle data.

    Returns: (start_date, start_price) or (None, 0)
    """
    if not candles or len(candles) < 20:
        return None, 0

    closes = [c["close"] for c in candles]

    best_start_idx = None
    best_rise_pct = 0

    # 뒤에서부터 탐색 (최근 급상승 우선)
    for i in range(len(closes) - 5, 19, -1):
        low_val = closes[i]
        if low_val <= 0:
            continue

        # i 이후 20일 내 최고점
        end = min(i + 20, len(closes))
        max_val = max(closes[i:end])
        rise_pct = ((max_val - low_val) / low_val) * 100

        if rise_pct >= threshold_pct and rise_pct > best_rise_pct:
            best_rise_pct = rise_pct
            best_start_idx = i

        # 충분히 좋은 급상승 찾으면 중단 (최근 것 우선)
        if best_rise_pct >= threshold_pct:
            break

    # 급상승 못 찾으면 기준 낮춰서 재시도
    if best_start_idx is None:
        for i in range(len(closes) - 5, 19, -1):
            low_val = closes[i]
            if low_val <= 0:
                continue
            end = min(i + 30, len(closes))
            max_val = max(closes[i:end])
            rise_pct = ((max_val - low_val) / low_val) * 100
            if rise_pct >= 10.0:
                best_start_idx = i
                break

    if best_start_idx is not None:
        date = candles[best_start_idx]["date"].replace("'", "").strip()
        price = closes[best_start_idx]
        return date, price

    return None, 0


def fetch_daily_candles(stock_code: str, days: int = 60) -> List[Dict]:
    """
    네이버 금융에서 일봉 데이터 수집 (naver_stock.py 재사용)
    Fetch daily candles from Naver Finance (reusing naver_stock.py)
    """
    try:
        candles = get_daily_candles_naver(stock_code, count=days)
        logger.info(f"[가상투자] {stock_code}: {len(candles)}개 일봉 수집")
        return candles
    except Exception as e:
        logger.error(f"[가상투자] {stock_code} 일봉 수집 오류: {e}")
        return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 핵심 시뮬레이션 엔진 / Core Simulation Engine
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def simulate_strategy(
    stocks_data: Dict[str, Dict],  # {code: {name, buy_price, signal_date, candles}}
    capital: float,
    take_profit_pct: float,
    stop_loss_pct: float,
    max_hold_days: int,
) -> Tuple[List[TradeResult], List[DailySnapshot]]:
    """
    단일 전략으로 포트폴리오 시뮬레이션 실행 (재진입 포함)
    Run portfolio simulation with re-entry support

    매수추천 종목들을 signal_date에 동시 매수하고,
    각 종목별로 익절/손절/만기 조건으로 청산.
    익절 후 조정 시 재진입하여 수익 극대화.
    """
    MAX_RE_ENTRIES = 3          # 최대 재진입 횟수
    RE_ENTRY_DIP_PCT = 2.0     # 매도가 대비 N% 하락 시 재진입 대기
    RE_ENTRY_COOLDOWN = 2      # 매도 후 최소 N일 쿨다운

    trades = []
    daily_snapshots = []

    if not stocks_data:
        return trades, daily_snapshots

    # 종목당 투자금 계산
    num_stocks = min(len(stocks_data), MAX_POSITIONS)
    per_stock_amount = capital / num_stocks
    cash = capital - (per_stock_amount * num_stocks)

    # 각 종목별 포지션 생성
    positions = []
    for code, info in list(stocks_data.items())[:MAX_POSITIONS]:
        buy_price = info["buy_price"]
        if buy_price <= 0:
            cash += per_stock_amount
            continue

        buy_commission = per_stock_amount * COMMISSION_RATE
        actual_invest = per_stock_amount - buy_commission
        quantity = actual_invest / buy_price

        positions.append({
            "code": code,
            "name": info.get("name", code),
            "buy_price": buy_price,
            "signal_date": info.get("signal_date", "") or (info.get("candles", [{}])[0].get("date", "") if info.get("candles") else ""),
            "candles": info.get("candles", []),
            "quantity": quantity,
            "invest_amount": per_stock_amount,
            "status": "holding",       # holding / waiting / done
            "buy_day_idx": 0,
            "hold_start_day": 0,       # 현재 포지션 시작일 (재진입 시 리셋)
            "re_entry_count": 0,       # 재진입 횟수
            "last_sell_price": 0,      # 마지막 매도가 (재진입 조건용)
            "last_sell_day": 0,        # 마지막 매도일 (쿨다운용)
        })

    if not positions:
        return trades, daily_snapshots

    # 재진입 포함 확장 시뮬레이션 기간
    max_sim_days = max_hold_days * (MAX_RE_ENTRIES + 1) + 10

    # 일별 시뮬레이션
    for day in range(max_sim_days):
        day_holding_value = 0
        all_done = True

        for pos in positions:
            candles = pos["candles"]
            candle_idx = pos["buy_day_idx"] + day

            # ── 데이터 소진 → 강제 종료 ──
            if candle_idx >= len(candles):
                if pos["status"] == "holding":
                    last_price = candles[-1]["close"] if candles else pos["buy_price"]
                    last_date = candles[-1].get("date", "") if candles else ""
                    trade = _close_position(pos, last_price, day - pos["hold_start_day"], "timeout", per_stock_amount, sell_date=last_date)
                    trades.append(trade)
                    cash += per_stock_amount + trade.profit_won
                pos["status"] = "done"
                continue

            candle = candles[candle_idx]
            current_price = candle["close"]
            high_price = candle["high"]
            low_price = candle["low"]

            # ━━━ 재진입 대기 상태 ━━━
            if pos["status"] == "waiting":
                all_done = False

                # 재진입 조건 체크
                days_since_sell = day - pos["last_sell_day"]
                if days_since_sell < RE_ENTRY_COOLDOWN:
                    continue

                # 매도가 대비 하락 후 양봉 출현 → 재매수
                dip_from_sell = ((current_price - pos["last_sell_price"]) / pos["last_sell_price"]) * 100
                is_bullish = candle["close"] > candle["open"]

                if dip_from_sell <= -RE_ENTRY_DIP_PCT and is_bullish:
                    # 재진입!
                    pos["status"] = "holding"
                    pos["buy_price"] = current_price
                    pos["hold_start_day"] = day
                    pos["signal_date"] = candle.get("date", "")  # 재진입 매수일
                    pos["re_entry_count"] += 1

                    # 새 수량 계산 (수수료 차감)
                    buy_commission = per_stock_amount * COMMISSION_RATE
                    actual_invest = per_stock_amount - buy_commission
                    pos["quantity"] = actual_invest / current_price
                    cash -= per_stock_amount

                continue

            # ━━━ 보유 중 상태 ━━━
            if pos["status"] == "holding":
                all_done = False
                hold_day = day - pos["hold_start_day"] + 1

                # 수익률 계산 (장중 고가/저가 체크)
                high_pct = ((high_price - pos["buy_price"]) / pos["buy_price"]) * 100
                low_pct = ((low_price - pos["buy_price"]) / pos["buy_price"]) * 100

                # 익절 체크 (장중 고가 기준)
                if high_pct >= take_profit_pct:
                    sell_price = pos["buy_price"] * (1 + take_profit_pct / 100)
                    trade = _close_position(pos, sell_price, hold_day, "profit", per_stock_amount, sell_date=candle.get("date", ""))
                    trades.append(trade)
                    cash += per_stock_amount + trade.profit_won

                    # 재진입 가능 여부 판단
                    if pos["re_entry_count"] < MAX_RE_ENTRIES:
                        pos["status"] = "waiting"
                        pos["last_sell_price"] = sell_price
                        pos["last_sell_day"] = day
                    else:
                        pos["status"] = "done"
                    continue

                # 손절 체크 (장중 저가 기준) — 손절 후 재진입 없음
                if low_pct <= -stop_loss_pct:
                    sell_price = pos["buy_price"] * (1 - stop_loss_pct / 100)
                    trade = _close_position(pos, sell_price, hold_day, "loss", per_stock_amount, sell_date=candle.get("date", ""))
                    trades.append(trade)
                    cash += per_stock_amount + trade.profit_won
                    pos["status"] = "done"  # 손절 → 재진입 없음
                    continue

                # 최대 보유일 초과 — 만기 후 재진입 없음
                if hold_day >= max_hold_days:
                    trade = _close_position(pos, current_price, hold_day, "timeout", per_stock_amount, sell_date=candle.get("date", ""))
                    trades.append(trade)
                    cash += per_stock_amount + trade.profit_won
                    pos["status"] = "done"  # 만기 → 재진입 없음
                    continue

                # 보유 중 → 현재 평가금액
                day_holding_value += pos["quantity"] * current_price

        # 일별 스냅샷 기록
        total_asset = cash + day_holding_value
        daily_snapshots.append(DailySnapshot(
            date=f"D{day+1}",
            day_num=day + 1,
            total_asset=round(total_asset),
            cash=round(cash),
            holding_value=round(day_holding_value),
        ))

        if all_done:
            break

    return trades, daily_snapshots


def _close_position(pos: Dict, sell_price: float, hold_days: int, result_type: str, invest_amount: float, sell_date: str = "") -> TradeResult:
    """포지션 청산 처리 / Close a position"""
    buy_price = pos["buy_price"]
    re_entry_num = pos.get("re_entry_count", 0)

    # 수익률 계산
    gross_pct = ((sell_price - buy_price) / buy_price) * 100

    # 수수료 + 세금 차감
    sell_amount = pos["quantity"] * sell_price
    sell_commission = sell_amount * COMMISSION_RATE
    sell_tax = sell_amount * SELL_TAX_RATE
    net_sell = sell_amount - sell_commission - sell_tax

    buy_amount = pos["quantity"] * buy_price
    buy_commission = buy_amount * COMMISSION_RATE
    net_buy = buy_amount + buy_commission

    profit_won = round(net_sell - net_buy)
    net_pct = round((profit_won / invest_amount) * 100, 2)

    # result_type 매핑 (재진입 번호 표시)
    re_tag = f"({re_entry_num}차)" if re_entry_num > 0 else ""
    if result_type == "profit":
        result_label = f"익절✅{re_tag}"
    elif result_type == "trailing":
        result_label = f"추적✅{re_tag}"
    elif result_type == "loss":
        result_label = f"손절❌{re_tag}"
    else:
        result_label = f"만기⏰{re_tag}"

    return TradeResult(
        stock_code=pos["code"],
        stock_name=pos["name"],
        buy_price=buy_price,
        buy_date=pos.get("signal_date", sell_date),
        sell_price=round(sell_price),
        sell_date=sell_date or "",
        profit_pct=net_pct,
        profit_won=profit_won,
        hold_days=hold_days,
        result=result_label,
        invest_amount=invest_amount,
        re_entry_num=re_entry_num,
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 스마트형 시뮬레이션 / Smart Strategy Simulation
# 1) 매수 후 5일간 손절 유예 (grace period — 급등 초기 변동성 보호)
# 2) 종가 기준 손절 (장중 저점 무시)
# 3) 트레일링 스탑 (보유 중 최고 종가 대비 -3% 하락 시 매도)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def simulate_smart_strategy(
    stocks_data: Dict[str, Dict],
    capital: float,
    stop_loss_pct: float = 12.0,
    trailing_stop_pct: float = 5.0,
    grace_days: int = 7,
    max_hold_days: int = 30,
    profit_activation_pct: float = 15.0,
) -> Tuple[List[TradeResult], List[DailySnapshot]]:
    """
    스마트형 전략 시뮬레이션 (재진입 포함)
    Smart strategy: grace period + profit-activated trailing stop + re-entry

    핵심 로직:
      1) grace_days 동안 손절/추적 모두 유예
      2) grace 이후에도 추적손절은 profit_activation_pct 달성 전까지 비활성
      3) 수익 활성화 후 peak 대비 trailing_stop_pct 하락 시 매도
      4) 손절은 grace 이후 매수가 대비 stop_loss_pct 적용
    """
    MAX_RE_ENTRIES = 3          # 최대 재진입 횟수
    RE_ENTRY_DIP_PCT = 2.0     # 매도가 대비 N% 하락 시 재진입 조건
    RE_ENTRY_COOLDOWN = 2      # 매도 후 최소 N일 쿨다운

    trades = []
    daily_snapshots = []

    if not stocks_data:
        return trades, daily_snapshots

    num_stocks = min(len(stocks_data), MAX_POSITIONS)
    per_stock_amount = capital / num_stocks
    cash = capital - (per_stock_amount * num_stocks)

    positions = []
    for code, info in list(stocks_data.items())[:MAX_POSITIONS]:
        buy_price = info["buy_price"]
        if buy_price <= 0:
            cash += per_stock_amount
            continue

        buy_commission = per_stock_amount * COMMISSION_RATE
        actual_invest = per_stock_amount - buy_commission
        quantity = actual_invest / buy_price

        positions.append({
            "code": code,
            "name": info.get("name", code),
            "buy_price": buy_price,
            "signal_date": info.get("signal_date", "") or (info.get("candles", [{}])[0].get("date", "") if info.get("candles") else ""),
            "candles": info.get("candles", []),
            "quantity": quantity,
            "invest_amount": per_stock_amount,
            "status": "holding",       # holding / waiting / done
            "buy_day_idx": 0,
            "peak_price": buy_price,   # 보유 중 최고가 추적
            "hold_start_day": 0,       # 현재 포지션 시작일
            "re_entry_count": 0,       # 재진입 횟수
            "last_sell_price": 0,      # 마지막 매도가
            "last_sell_day": 0,        # 마지막 매도일
        })

    if not positions:
        return trades, daily_snapshots

    max_sim_days = max_hold_days * (MAX_RE_ENTRIES + 1) + 10

    for day in range(max_sim_days):
        day_holding_value = 0
        all_done = True

        for pos in positions:
            candles = pos["candles"]
            candle_idx = pos["buy_day_idx"] + day

            # ── 데이터 소진 → 강제 종료 ──
            if candle_idx >= len(candles):
                if pos["status"] == "holding":
                    last_price = candles[-1]["close"] if candles else pos["buy_price"]
                    last_date = candles[-1].get("date", "") if candles else ""
                    trade = _close_position(pos, last_price, day - pos["hold_start_day"], "timeout", per_stock_amount, sell_date=last_date)
                    trades.append(trade)
                    cash += per_stock_amount + trade.profit_won
                pos["status"] = "done"
                continue

            candle = candles[candle_idx]
            current_price = candle["close"]
            high_price = candle["high"]
            low_price = candle["low"]

            # ━━━ 재진입 대기 상태 ━━━
            if pos["status"] == "waiting":
                all_done = False

                days_since_sell = day - pos["last_sell_day"]
                if days_since_sell < RE_ENTRY_COOLDOWN:
                    continue

                # 매도가 대비 하락 후 양봉 출현 → 재매수
                dip_from_sell = ((current_price - pos["last_sell_price"]) / pos["last_sell_price"]) * 100
                is_bullish = candle["close"] > candle["open"]

                if dip_from_sell <= -RE_ENTRY_DIP_PCT and is_bullish:
                    pos["status"] = "holding"
                    pos["buy_price"] = current_price
                    pos["hold_start_day"] = day
                    pos["peak_price"] = current_price  # 리셋
                    pos["signal_date"] = candle.get("date", "")  # 재진입 매수일
                    pos["re_entry_count"] += 1

                    buy_commission = per_stock_amount * COMMISSION_RATE
                    actual_invest = per_stock_amount - buy_commission
                    pos["quantity"] = actual_invest / current_price
                    cash -= per_stock_amount

                continue

            # ━━━ 보유 중 상태 ━━━
            if pos["status"] == "holding":
                all_done = False
                hold_day = day - pos["hold_start_day"] + 1

                # ── 최고가 업데이트 (★ 종가 기준 — 장중 고가 사용 시 조기 발동 방지) ──
                if current_price > pos["peak_price"]:
                    pos["peak_price"] = current_price

                # ── 1) 트레일링 스탑 체크 (★ 수익 활성화 후에만) ──
                # 매수가 대비 profit_activation_pct 이상 상승 경험 후에만 추적손절 활성화
                profit_from_buy = ((pos["peak_price"] - pos["buy_price"]) / pos["buy_price"]) * 100
                trailing_active = hold_day > grace_days and profit_from_buy >= profit_activation_pct

                if trailing_active:
                    drop_from_peak = ((current_price - pos["peak_price"]) / pos["peak_price"]) * 100
                    if drop_from_peak <= -trailing_stop_pct:
                        trade = _close_position(pos, current_price, hold_day, "trailing", per_stock_amount, sell_date=candle.get("date", ""))
                        trades.append(trade)
                        cash += per_stock_amount + trade.profit_won

                        # 수익이면 재진입 가능, 손실이면 종료
                        if trade.profit_won > 0 and pos["re_entry_count"] < MAX_RE_ENTRIES:
                            pos["status"] = "waiting"
                            pos["last_sell_price"] = current_price
                            pos["last_sell_day"] = day
                        else:
                            pos["status"] = "done"
                        continue

                # ── 2) 종가 기준 손절 (grace period 이후) — 재진입 없음 ──
                if hold_day > grace_days:
                    close_pct = ((current_price - pos["buy_price"]) / pos["buy_price"]) * 100
                    if close_pct <= -stop_loss_pct:
                        trade = _close_position(pos, current_price, hold_day, "loss", per_stock_amount, sell_date=candle.get("date", ""))
                        trades.append(trade)
                        cash += per_stock_amount + trade.profit_won
                        pos["status"] = "done"  # 손절 → 재진입 없음
                        continue

                # ── 3) 최대 보유일 초과 — 재진입 없음 ──
                if hold_day >= max_hold_days:
                    trade = _close_position(pos, current_price, hold_day, "timeout", per_stock_amount, sell_date=candle.get("date", ""))
                    trades.append(trade)
                    cash += per_stock_amount + trade.profit_won
                    pos["status"] = "done"  # 만기 → 재진입 없음
                    continue

                # 보유 중 자산 가치
                day_holding_value += pos["quantity"] * current_price

        daily_snapshots.append(DailySnapshot(
            date="",
            day_num=day + 1,
            total_asset=round(cash + day_holding_value),
            cash=round(cash),
            holding_value=round(day_holding_value),
        ))

        if all_done:
            break

    return trades, daily_snapshots


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MDD 계산 / Max Drawdown Calculation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def calc_mdd(daily_assets: List[DailySnapshot]) -> float:
    """MDD (최대 낙폭) 계산 / Calculate Maximum Drawdown"""
    if not daily_assets:
        return 0.0

    peak = daily_assets[0].total_asset
    max_dd = 0.0

    for snap in daily_assets:
        if snap.total_asset > peak:
            peak = snap.total_asset
        dd = ((snap.total_asset - peak) / peak) * 100
        if dd < max_dd:
            max_dd = dd

    return round(max_dd, 2)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6가지 전략 동시 비교 실행 / Run All 6 Strategies
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def run_comparison(
    stocks: List[Dict],         # [{code, name, buy_price, signal_date}]
    capital: float = DEFAULT_CAPITAL,
    custom_params: Dict = None,
) -> Dict:
    """
    6가지 전략을 동시에 실행하고 비교 결과 반환
    Run all 6 strategies and return comparison results
    """
    session_id = str(uuid.uuid4())[:8]
    logger.info(f"[가상투자] 비교 실행 시작 session={session_id}, 종목수={len(stocks)}")

    # 1. 일봉 데이터 수집 (종목별 1회만)
    stocks_data = {}
    for stock in stocks[:MAX_POSITIONS]:
        code = stock["code"]
        signal_date = stock.get("signal_date", "")

        # ★★★ 핵심 수정: signal_date가 최근 3일 이내면 "오늘 날짜 버그"로 판단
        # → 캔들에서 가장 최근 급상승 시작점을 자동 탐색
        is_signal_too_recent = False
        if signal_date:
            try:
                sd_clean = signal_date.replace("-", "").replace(".", "").strip()[:8]
                sd_dt = datetime.strptime(sd_clean, "%Y%m%d")
                days_from_now = (datetime.now() - sd_dt).days
                if days_from_now <= 3:
                    is_signal_too_recent = True
                    logger.warning(f"[가상투자] {code} signal_date={signal_date}가 최근 {days_from_now}일 → 자동 보정 시도")
            except Exception:
                is_signal_too_recent = True

        # 일봉 수집: signal_date 기반 기간 계산
        fetch_days = 120
        if signal_date and not is_signal_too_recent:
            try:
                sd_clean = signal_date.replace("-", "").replace(".", "").strip()[:8]
                sd_dt = datetime.strptime(sd_clean, "%Y%m%d")
                days_diff = (datetime.now() - sd_dt).days
                fetch_days = max(120, days_diff + 60)
                fetch_days = min(fetch_days, 600)
            except Exception:
                fetch_days = 600
        else:
            fetch_days = 400  # 최근 날짜면 넉넉히 수집

        candles = fetch_daily_candles(code, days=fetch_days)

        if not candles:
            logger.warning(f"[가상투자] {code} 일봉 데이터 없음, 스킵")
            continue

        # ★★★ signal_date가 너무 최근이면 → 캔들에서 급상승 시작점 자동 탐색
        if is_signal_too_recent and len(candles) >= 30:
            auto_date, auto_price = _find_last_surge_start(candles)
            if auto_date:
                logger.info(f"[가상투자] {code} 자동 보정: {signal_date} → {auto_date} (가격: {auto_price})")
                signal_date = auto_date
                stock = {**stock, "signal_date": auto_date, "buy_price": auto_price}
            else:
                # 급상승 못 찾으면 60일 전부터 시뮬레이션
                fallback_idx = max(0, len(candles) - 60)
                signal_date = candles[fallback_idx]["date"].replace("'", "").strip()
                auto_price = candles[fallback_idx]["close"]
                logger.info(f"[가상투자] {code} 급상승 미발견 → 60일 전({signal_date}) 사용")
                stock = {**stock, "signal_date": signal_date, "buy_price": auto_price}

        # signal_date 이후의 캔들만 추출
        if signal_date:
            filtered = []
            found = False
            sd_norm = signal_date.replace("-", "").replace(".", "").replace("'", "").strip()
            for c in candles:
                c_date = c["date"].replace("'", "").strip()
                if found:
                    filtered.append(c)
                elif c_date >= sd_norm:
                    found = True
                    filtered.append(c)
            if filtered and len(filtered) >= 2:
                candles = filtered
                logger.info(f"[가상투자] {code} signal_date={signal_date} 이후 캔들: {len(candles)}개")
            else:
                logger.warning(f"[가상투자] {code} signal_date={signal_date} 이후 캔들 부족({len(filtered)}개), 전체 사용")

        stocks_data[code] = {
            "name": stock.get("name", code),
            "buy_price": stock.get("buy_price", candles[0]["close"] if candles else 0),
            "signal_date": signal_date,
            "candles": candles,
        }

    if not stocks_data:
        return {"error": "일봉 데이터를 수집할 수 없습니다.", "session_id": session_id}

    # 2. 커스텀 파라미터 적용
    if custom_params:
        STRATEGY_PRESETS["custom"]["take_profit_pct"] = custom_params.get("take_profit_pct", 7.0)
        STRATEGY_PRESETS["custom"]["stop_loss_pct"] = custom_params.get("stop_loss_pct", 3.0)
        STRATEGY_PRESETS["custom"]["max_hold_days"] = custom_params.get("max_hold_days", 10)

    # 3. 6가지 전략 동시 실행 (스마트형은 별도 엔진)
    results = []
    for key, preset in STRATEGY_PRESETS.items():
        tp = preset["take_profit_pct"]
        sl = preset["stop_loss_pct"]
        mhd = preset["max_hold_days"]

        if key == "smart":
            # 스마트형: 수익활성화 + 트레일링 스탑 + 종가 손절 + 유예기간
            trades, daily_assets = simulate_smart_strategy(
                stocks_data=stocks_data,
                capital=capital,
                stop_loss_pct=sl,
                trailing_stop_pct=preset.get("trailing_stop_pct", 5.0),
                grace_days=preset.get("grace_days", 7),
                max_hold_days=mhd,
                profit_activation_pct=preset.get("profit_activation_pct", 15.0),
            )
        else:
            # 기존 전략: 고정 익절/손절
            trades, daily_assets = simulate_strategy(
                stocks_data=stocks_data,
                capital=capital,
                take_profit_pct=tp,
                stop_loss_pct=sl,
                max_hold_days=mhd,
            )

        # 결과 집계
        win_count = sum(1 for t in trades if t.profit_won > 0)
        loss_count = sum(1 for t in trades if t.profit_won <= 0)
        total_trades = len(trades)
        win_rate = round((win_count / total_trades * 100) if total_trades > 0 else 0, 1)

        total_return_won = sum(t.profit_won for t in trades)
        total_return_pct = round((total_return_won / capital) * 100, 2)
        final_asset = capital + total_return_won

        mdd = calc_mdd(daily_assets)
        risk_reward = round(tp / sl, 2) if (sl > 0 and tp > 0) else round(preset.get("trailing_stop_pct", 3.0) / sl, 2) if sl > 0 else 0

        # 종합점수: 수익률 × (승률/100) × (1 + 1/|MDD|) → 높을수록 좋음
        score = round(total_return_pct * (win_rate / 100) * (1 / max(abs(mdd), 0.1)), 2)

        strategy_result = StrategyResult(
            strategy=key,
            strategy_name=preset["name"],
            color=preset["color"],
            take_profit_pct=tp,
            stop_loss_pct=sl,
            max_hold_days=mhd,
            initial_capital=capital,
            final_asset=final_asset,
            total_return_pct=total_return_pct,
            total_return_won=total_return_won,
            win_rate=win_rate,
            win_count=win_count,
            loss_count=loss_count,
            total_trades=total_trades,
            mdd_pct=mdd,
            risk_reward_ratio=risk_reward,
            score=score,
            ranking=0,
            trades=[asdict(t) for t in trades],
            daily_assets=[asdict(s) for s in daily_assets],
        )
        results.append(strategy_result)

    # 4. 순위 매기기 (종합점수 기준)
    results.sort(key=lambda r: r.score, reverse=True)
    for i, r in enumerate(results):
        r.ranking = i + 1

    # 5. 최적 전략 결정
    best = results[0] if results else None

    # 6. 응답 구성
    strategies = {}
    rankings = []
    for r in results:
        strategies[r.strategy] = asdict(r)
        rank_item = {
            "strategy": r.strategy,
            "strategy_name": r.strategy_name,
            "color": r.color,
            "total_return_pct": r.total_return_pct,
            "total_return_won": r.total_return_won,
            "win_rate": r.win_rate,
            "win_count": r.win_count,
            "loss_count": r.loss_count,
            "total_trades": r.total_trades,
            "mdd_pct": r.mdd_pct,
            "risk_reward_ratio": r.risk_reward_ratio,
            "score": r.score,
            "ranking": r.ranking,
            "take_profit_pct": r.take_profit_pct,
            "stop_loss_pct": r.stop_loss_pct,
            "max_hold_days": r.max_hold_days,
        }
        # 스마트형 추가 필드
        if r.strategy == "smart":
            smart_preset = STRATEGY_PRESETS.get("smart", {})
            rank_item["trailing_stop_pct"] = smart_preset.get("trailing_stop_pct", 3.0)
            rank_item["grace_days"] = smart_preset.get("grace_days", 5)
            strategies[r.strategy]["trailing_stop_pct"] = rank_item["trailing_stop_pct"]
            strategies[r.strategy]["grace_days"] = rank_item["grace_days"]
        rankings.append(rank_item)

    response = {
        "session_id": session_id,
        "stocks_count": len(stocks_data),
        "stocks": [{"code": k, "name": v["name"]} for k, v in stocks_data.items()],
        "capital": capital,
        "rankings": sorted(rankings, key=lambda x: x["ranking"]),
        "strategies": strategies,
        "best_strategy": best.strategy if best else None,
        "best_strategy_name": best.strategy_name if best else None,
        "best_reason": f"종합점수 1위 (수익률 {best.total_return_pct}% × 승률 {best.win_rate}%)" if best else "",
    }

    logger.info(f"[가상투자] 비교 완료: 최적={best.strategy_name if best else 'N/A'}")
    return response


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 실시간 모의투자 관리 / Realtime Virtual Trading
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def start_realtime(
    stocks: List[Dict],
    capital: float = DEFAULT_CAPITAL,
    take_profit_pct: float = 7.0,
    stop_loss_pct: float = 3.0,
    max_hold_days: int = 10,
    supabase=None,
) -> Dict:
    """
    실시간 모의투자 시작
    Start realtime virtual trading
    """
    session_id = f"rt_{str(uuid.uuid4())[:8]}"

    if supabase:
        try:
            supabase.table("virtual_realtime_session").insert({
                "session_id": session_id,
                "status": "active",
                "capital": capital,
                "cash": capital,
                "start_date": datetime.now().strftime("%Y-%m-%d"),
                "take_profit_pct": take_profit_pct,
                "stop_loss_pct": stop_loss_pct,
                "max_hold_days": max_hold_days,
                "stocks": stocks,
            }).execute()

            # 포지션 생성
            num_stocks = min(len(stocks), MAX_POSITIONS)
            per_stock = capital / num_stocks

            for stock in stocks[:MAX_POSITIONS]:
                supabase.table("virtual_positions").insert({
                    "session_id": session_id,
                    "strategy": "realtime",
                    "mode": "realtime",
                    "stock_code": stock["code"],
                    "stock_name": stock.get("name", stock["code"]),
                    "buy_price": stock["buy_price"],
                    "buy_date": datetime.now().strftime("%Y-%m-%d"),
                    "current_price": stock["buy_price"],
                    "status": "holding",
                    "take_profit_pct": take_profit_pct,
                    "stop_loss_pct": stop_loss_pct,
                    "max_hold_days": max_hold_days,
                }).execute()

            logger.info(f"[실시간모의] 시작: session={session_id}, 종목수={num_stocks}")

        except Exception as e:
            logger.error(f"[실시간모의] 세션 생성 오류: {e}")
            return {"error": str(e)}

    return {
        "session_id": session_id,
        "status": "active",
        "stocks_count": min(len(stocks), MAX_POSITIONS),
        "capital": capital,
        "params": {
            "take_profit_pct": take_profit_pct,
            "stop_loss_pct": stop_loss_pct,
            "max_hold_days": max_hold_days,
        }
    }


def _is_market_open() -> bool:
    """한국 주식시장 운영시간 체크 (KST 기준) / Check Korean market hours"""
    from datetime import timezone, timedelta as td
    kst = datetime.now(timezone(td(hours=9)))
    h, m = kst.hour, kst.minute
    mins = h * 60 + m
    day = kst.weekday()  # 0=Mon, 6=Sun
    # 평일 09:00 ~ 15:30 KST
    return day <= 4 and 540 <= mins <= 930


async def update_realtime(session_id: str, supabase=None) -> Dict:
    """
    실시간 모의투자 현재가 업데이트 (장중에만 허용)
    Update realtime positions with current prices (market hours only)
    """
    if not supabase:
        return {"error": "DB 연결 없음"}

    # ★ 장 운영시간 체크 — 장 마감 후에는 가격 갱신 차단
    if not _is_market_open():
        logger.info(f"[실시간모의] 장 마감 상태 — 가격 갱신 스킵 (session={session_id})")
        return {
            "session_id": session_id,
            "updated": 0,
            "market_closed": True,
            "message": "장 마감 후에는 가격이 갱신되지 않습니다 (평일 09:00~15:30)"
        }

    try:
        # 활성 포지션 조회
        result = supabase.table("virtual_positions").select("*").eq(
            "session_id", session_id
        ).eq("status", "holding").execute()

        positions = result.data if result.data else []
        updated = 0

        for pos in positions:
            code = pos["stock_code"]
            candles = fetch_daily_candles(code, days=5)

            if not candles:
                continue

            current_price = candles[-1]["close"]
            buy_price = float(pos["buy_price"])
            tp = float(pos["take_profit_pct"])
            sl = float(pos["stop_loss_pct"])
            mhd = int(pos["max_hold_days"])

            pct = ((current_price - buy_price) / buy_price) * 100

            # 보유일 계산
            buy_date = datetime.strptime(pos["buy_date"], "%Y-%m-%d")
            hold_days = (datetime.now() - buy_date).days

            new_status = "holding"
            sell_price = None

            if pct >= tp:
                new_status = "sold_profit"
                sell_price = round(buy_price * (1 + tp / 100))
            elif pct <= -sl:
                new_status = "sold_loss"
                sell_price = round(buy_price * (1 - sl / 100))
            elif hold_days >= mhd:
                new_status = "sold_timeout"
                sell_price = current_price

            update_data = {
                "current_price": current_price,
                "hold_days": hold_days,
                "profit_pct": round(pct, 2),
            }

            if new_status != "holding":
                update_data["status"] = new_status
                update_data["sell_price"] = sell_price
                update_data["sell_date"] = datetime.now().strftime("%Y-%m-%d")

                # 수익금 계산
                invest = float(pos.get("invest_amount", 200000))
                quantity = invest / buy_price
                sell_amount = quantity * sell_price
                costs = sell_amount * (COMMISSION_RATE + SELL_TAX_RATE) + invest * COMMISSION_RATE
                profit_won = round(sell_amount - invest - costs)
                update_data["profit_won"] = profit_won

            supabase.table("virtual_positions").update(update_data).eq("id", pos["id"]).execute()
            updated += 1

        return {"session_id": session_id, "updated": updated, "positions": len(positions)}

    except Exception as e:
        logger.error(f"[실시간모의] 업데이트 오류: {e}")
        return {"error": str(e)}


async def get_realtime_status(session_id: str, supabase=None) -> Dict:
    """실시간 모의투자 현황 조회 / Get realtime trading status"""
    if not supabase:
        return {"error": "DB 연결 없음"}

    try:
        # 세션 조회
        sess = supabase.table("virtual_realtime_session").select("*").eq(
            "session_id", session_id
        ).single().execute()

        # 포지션 조회
        positions = supabase.table("virtual_positions").select("*").eq(
            "session_id", session_id
        ).execute()

        sess_data = sess.data
        pos_data = positions.data if positions.data else []

        holding = [p for p in pos_data if p["status"] == "holding"]
        closed = [p for p in pos_data if p["status"] != "holding"]

        capital = float(sess_data["capital"])
        holding_value = sum(
            float(p.get("current_price", 0)) * (float(sess_data["capital"]) / MAX_POSITIONS / float(p["buy_price"]))
            for p in holding if float(p.get("buy_price", 0)) > 0
        )
        closed_profit = sum(float(p.get("profit_won", 0)) for p in closed)
        cash = capital - (len(holding) * capital / MAX_POSITIONS) + closed_profit

        return {
            "session_id": session_id,
            "status": sess_data["status"],
            "capital": capital,
            "cash": round(cash),
            "holding_value": round(holding_value),
            "total_asset": round(cash + holding_value),
            "holding_count": len(holding),
            "closed_count": len(closed),
            "positions": pos_data,
            "params": {
                "take_profit_pct": sess_data["take_profit_pct"],
                "stop_loss_pct": sess_data["stop_loss_pct"],
                "max_hold_days": sess_data["max_hold_days"],
            }
        }

    except Exception as e:
        logger.error(f"[실시간모의] 상태 조회 오류: {e}")
        return {"error": str(e)}
