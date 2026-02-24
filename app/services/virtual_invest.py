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
MAX_POSITIONS = 5            # 최대 동시 보유 종목수
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
    result: str             # 'profit' | 'loss' | 'timeout'
    invest_amount: float    # 투자금액


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
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def fetch_daily_candles(stock_code: str, days: int = 60) -> List[Dict]:
    """
    네이버 금융에서 일봉 데이터 수집
    Fetch daily candles from Naver Finance
    """
    import aiohttp

    url = f"https://fchart.stock.naver.com/siseJson.nhn?symbol={stock_code}&requestType=1&startTime=20240101&endTime=20261231&timeframe=day"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.naver.com"
    }

    candles = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.error(f"[가상투자] {stock_code} 일봉 조회 실패: {resp.status}")
                    return []

                text = await resp.text()
                # 네이버 일봉 JSON 파싱
                lines = text.strip().split("\n")
                for line in lines[1:]:  # 헤더 스킵
                    line = line.strip().strip(",")
                    if not line or line.startswith("["):
                        continue
                    try:
                        # ['날짜', 시가, 고가, 저가, 종가, 거래량]
                        parts = line.strip("[]").split(",")
                        if len(parts) < 6:
                            continue
                        date_str = parts[0].strip().strip("'\"").strip()
                        if len(date_str) < 8:
                            continue
                        candles.append({
                            "date": date_str,
                            "open": int(float(parts[1].strip())),
                            "high": int(float(parts[2].strip())),
                            "low": int(float(parts[3].strip())),
                            "close": int(float(parts[4].strip())),
                            "volume": int(float(parts[5].strip())),
                        })
                    except (ValueError, IndexError):
                        continue

        # 최근 N일만
        if len(candles) > days:
            candles = candles[-days:]

        logger.info(f"[가상투자] {stock_code}: {len(candles)}개 일봉 수집")

    except Exception as e:
        logger.error(f"[가상투자] {stock_code} 일봉 수집 오류: {e}")

    return candles


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
    단일 전략으로 포트폴리오 시뮬레이션 실행
    Run portfolio simulation with a single strategy

    매수추천 종목들을 signal_date에 동시 매수하고,
    각 종목별로 익절/손절/만기 조건으로 청산.
    """
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

        # 매수 수수료 차감
        buy_commission = per_stock_amount * COMMISSION_RATE
        actual_invest = per_stock_amount - buy_commission
        quantity = actual_invest / buy_price

        positions.append({
            "code": code,
            "name": info.get("name", code),
            "buy_price": buy_price,
            "signal_date": info.get("signal_date", ""),
            "candles": info.get("candles", []),
            "quantity": quantity,
            "invest_amount": per_stock_amount,
            "status": "holding",
            "buy_day_idx": 0,
        })

    if not positions:
        return trades, daily_snapshots

    # 최대 시뮬레이션 일수 결정
    max_sim_days = max_hold_days + 5  # 여유분

    # 일별 시뮬레이션
    for day in range(max_sim_days):
        day_holding_value = 0
        all_closed = True

        for pos in positions:
            if pos["status"] != "holding":
                # 이미 청산된 포지션은 cash에 반영됨
                continue

            all_closed = False
            candles = pos["candles"]
            # signal_date 이후 day번째 캔들
            candle_idx = pos["buy_day_idx"] + day

            if candle_idx >= len(candles):
                # 데이터 부족 → 마지막 가격으로 강제 청산
                last_price = candles[-1]["close"] if candles else pos["buy_price"]
                trade = _close_position(pos, last_price, day, "timeout", per_stock_amount)
                trades.append(trade)
                cash += per_stock_amount + trade.profit_won
                pos["status"] = "closed"
                continue

            candle = candles[candle_idx]
            current_price = candle["close"]
            high_price = candle["high"]
            low_price = candle["low"]

            # 수익률 계산 (장중 고가/저가 체크)
            high_pct = ((high_price - pos["buy_price"]) / pos["buy_price"]) * 100
            low_pct = ((low_price - pos["buy_price"]) / pos["buy_price"]) * 100
            close_pct = ((current_price - pos["buy_price"]) / pos["buy_price"]) * 100

            # 익절 체크 (장중 고가 기준)
            if high_pct >= take_profit_pct:
                sell_price = pos["buy_price"] * (1 + take_profit_pct / 100)
                trade = _close_position(pos, sell_price, day + 1, "profit", per_stock_amount)
                trades.append(trade)
                cash += per_stock_amount + trade.profit_won
                pos["status"] = "closed"
                continue

            # 손절 체크 (장중 저가 기준)
            if low_pct <= -stop_loss_pct:
                sell_price = pos["buy_price"] * (1 - stop_loss_pct / 100)
                trade = _close_position(pos, sell_price, day + 1, "loss", per_stock_amount)
                trades.append(trade)
                cash += per_stock_amount + trade.profit_won
                pos["status"] = "closed"
                continue

            # 최대 보유일 초과
            if day + 1 >= max_hold_days:
                trade = _close_position(pos, current_price, day + 1, "timeout", per_stock_amount)
                trades.append(trade)
                cash += per_stock_amount + trade.profit_won
                pos["status"] = "closed"
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

        if all_closed:
            break

    return trades, daily_snapshots


def _close_position(pos: Dict, sell_price: float, hold_days: int, result_type: str, invest_amount: float) -> TradeResult:
    """포지션 청산 처리 / Close a position"""
    buy_price = pos["buy_price"]

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

    # result_type 매핑
    if result_type == "profit":
        result_label = "익절✅"
    elif result_type == "loss":
        result_label = "손절❌"
    else:
        result_label = "만기⏰"

    return TradeResult(
        stock_code=pos["code"],
        stock_name=pos["name"],
        buy_price=buy_price,
        buy_date=pos.get("signal_date", ""),
        sell_price=round(sell_price),
        sell_date="",
        profit_pct=net_pct,
        profit_won=profit_won,
        hold_days=hold_days,
        result=result_label,
        invest_amount=invest_amount,
    )


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
# 5가지 전략 동시 비교 실행 / Run All 5 Strategies
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def run_comparison(
    stocks: List[Dict],         # [{code, name, buy_price, signal_date}]
    capital: float = DEFAULT_CAPITAL,
    custom_params: Dict = None,
) -> Dict:
    """
    5가지 전략을 동시에 실행하고 비교 결과 반환
    Run all 5 strategies and return comparison results
    """
    session_id = str(uuid.uuid4())[:8]
    logger.info(f"[가상투자] 비교 실행 시작 session={session_id}, 종목수={len(stocks)}")

    # 1. 일봉 데이터 수집 (종목별 1회만)
    stocks_data = {}
    for stock in stocks[:MAX_POSITIONS]:
        code = stock["code"]
        candles = await fetch_daily_candles(code, days=60)

        if not candles:
            logger.warning(f"[가상투자] {code} 일봉 데이터 없음, 스킵")
            continue

        # signal_date 이후의 캔들만 추출
        signal_date = stock.get("signal_date", "")
        if signal_date:
            # signal_date 이후 캔들 필터링
            filtered = []
            found = False
            for c in candles:
                if found:
                    filtered.append(c)
                elif c["date"].replace("'", "").strip() >= signal_date.replace("-", ""):
                    found = True
                    filtered.append(c)
            if filtered:
                candles = filtered

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

    # 3. 5가지 전략 동시 실행
    results = []
    for key, preset in STRATEGY_PRESETS.items():
        tp = preset["take_profit_pct"]
        sl = preset["stop_loss_pct"]
        mhd = preset["max_hold_days"]

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
        risk_reward = round(tp / sl, 2) if sl > 0 else 0

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
        rankings.append({
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
        })

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


async def update_realtime(session_id: str, supabase=None) -> Dict:
    """
    실시간 모의투자 현재가 업데이트 (장 마감 후 호출)
    Update realtime positions with current prices
    """
    if not supabase:
        return {"error": "DB 연결 없음"}

    try:
        # 활성 포지션 조회
        result = supabase.table("virtual_positions").select("*").eq(
            "session_id", session_id
        ).eq("status", "holding").execute()

        positions = result.data if result.data else []
        updated = 0

        for pos in positions:
            code = pos["stock_code"]
            candles = await fetch_daily_candles(code, days=5)

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
