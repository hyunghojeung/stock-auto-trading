"""백테스트 시뮬레이션 엔진 / Backtest Simulation Engine

과거 분봉 데이터를 기반으로 눌림목/갭상승 전략을 시뮬레이션합니다.
Simulates dip-buying and gap-up strategies using historical minute candle data.

- KIS API 분봉 데이터 최근 30일 제한
- 수수료 0.015% + 매도세 0.18% 반영
- ATR 기반 익절 / 고정% 손절
"""

from datetime import datetime, date, timedelta
from app.services.kis_stock import get_current_price, get_minute_candles
from app.engine.dip_detector import detect_dip
from app.utils.indicators import sma, rsi, atr, vwap, ema
from app.utils.tax_calculator import calc_net_profit
from app.core.config import config

import traceback


async def run_backtest(
    strategy="dip",
    stock_codes=None,
    start_date=None,
    end_date=None,
    initial_capital=1_000_000,
    atr_multiplier=2.0,
    stop_loss_pct=3.0,
    max_holdings=5,
    per_trade_pct=20.0,
):
    """
    백테스트 메인 실행 함수
    Main backtest execution function
    
    Args:
        strategy: "dip" | "gap" | "both"
        stock_codes: 종목코드 리스트 ["005930", ...]
        start_date: 시작일 "2026-01-15"
        end_date: 종료일 "2026-02-15"
        initial_capital: 초기 자금
        atr_multiplier: ATR 익절 배수
        stop_loss_pct: 손절 %
        max_holdings: 최대 동시 보유
        per_trade_pct: 1회 매수 비중 %
    
    Returns:
        { summary: {...}, trades: [...], daily_assets: [...] }
    """
    if not stock_codes:
        stock_codes = ["005930", "000660", "035420"]
    
    print(f"[백테스트] 시작: 전략={strategy}, 종목수={len(stock_codes)}, 기간={start_date}~{end_date}")
    
    # ===== 분봉 데이터 수집 / Collect Minute Candle Data =====
    all_candles = {}
    for code in stock_codes:
        try:
            candles_1m = await get_minute_candles(code, period="1")
            if candles_1m and len(candles_1m) >= 10:
                all_candles[code] = candles_1m
                print(f"  [데이터] {code}: {len(candles_1m)}개 1분봉 수집")
            else:
                print(f"  [데이터] {code}: 데이터 부족 (건너뜀)")
        except Exception as e:
            print(f"  [데이터 오류] {code}: {e}")
    
    if not all_candles:
        return {
            "summary": _empty_summary(strategy, initial_capital),
            "trades": [],
            "daily_assets": [],
        }
    
    # ===== 시뮬레이션 실행 / Run Simulation =====
    trades = []
    holdings = {}  # code -> {buy_price, quantity, buy_date, highest}
    cash = initial_capital
    daily_assets = []
    
    # 모든 캔들을 일자별로 그룹핑 / Group candles by date
    date_groups = _group_candles_by_date(all_candles)
    sorted_days = sorted(date_groups.keys())
    
    if start_date:
        sorted_days = [d for d in sorted_days if d >= start_date]
    if end_date:
        sorted_days = [d for d in sorted_days if d <= end_date]
    
    print(f"[백테스트] 시뮬레이션 일수: {len(sorted_days)}일")
    
    for day_str in sorted_days:
        day_data = date_groups[day_str]
        
        for code, candles in day_data.items():
            if len(candles) < 5:
                continue
            
            # ---- 매도 체크 (보유 종목) / Sell Check ----
            if code in holdings:
                h = holdings[code]
                current_price = candles[-1]["close"]
                highest = max(h.get("highest", h["buy_price"]), max(c["high"] for c in candles))
                h["highest"] = highest
                
                # ATR 계산
                highs = [c["high"] for c in candles]
                lows = [c["low"] for c in candles]
                closes = [c["close"] for c in candles]
                atr_vals = atr(highs, lows, closes, min(14, len(candles) - 1))
                current_atr = next((v for v in reversed(atr_vals) if v is not None), 0)
                
                sell_reason = None
                sell_price = current_price
                
                # 익절: 고점 대비 ATR×배수 이상 상승 후 하락 시
                if current_atr > 0:
                    target_price = h["buy_price"] + current_atr * atr_multiplier
                    if highest >= target_price and current_price < highest - current_atr * 0.5:
                        sell_reason = f"익절(ATR×{atr_multiplier})"
                        sell_price = current_price
                
                # 손절: 매수가 대비 -X% 이하
                if not sell_reason:
                    loss_pct = (current_price - h["buy_price"]) / h["buy_price"] * 100
                    if loss_pct <= -stop_loss_pct:
                        sell_reason = f"손절(-{stop_loss_pct}%)"
                        sell_price = current_price
                
                # 장 마감 시 강제 매도 (당일 마지막 캔들)
                if not sell_reason and candles[-1].get("time", "15:30") >= "15:20":
                    sell_reason = "장마감매도"
                    sell_price = current_price
                
                if sell_reason:
                    profit_info = calc_net_profit(h["buy_price"], sell_price, h["quantity"])
                    cash += sell_price * h["quantity"] - profit_info.get("commission", 0) - profit_info.get("tax", 0)
                    
                    trades.append({
                        "date": day_str,
                        "stock_code": code,
                        "stock_name": code,  # 이름은 코드로 대체
                        "trade_type": "sell",
                        "buy_price": h["buy_price"],
                        "sell_price": sell_price,
                        "quantity": h["quantity"],
                        "net_profit": profit_info.get("net_profit", 0),
                        "profit_pct": profit_info.get("profit_pct", 0),
                        "fee": profit_info.get("commission", 0) + profit_info.get("tax", 0),
                        "reason": sell_reason,
                    })
                    del holdings[code]
            
            # ---- 매수 체크 / Buy Check ----
            if code not in holdings and len(holdings) < max_holdings:
                should_buy = False
                buy_reason = ""
                
                # 전략에 따른 매수 판단
                if strategy in ("dip", "both"):
                    should_buy, buy_reason = _check_dip_buy(candles)
                
                if not should_buy and strategy in ("gap", "both"):
                    should_buy, buy_reason = _check_gap_buy(candles, day_data.get(code, []))
                
                if should_buy:
                    buy_price = candles[-1]["close"]
                    trade_amount = cash * (per_trade_pct / 100)
                    quantity = int(trade_amount / buy_price)
                    
                    if quantity > 0 and buy_price * quantity <= cash:
                        commission = buy_price * quantity * config.COMMISSION_RATE
                        cash -= buy_price * quantity + commission
                        
                        holdings[code] = {
                            "buy_price": buy_price,
                            "quantity": quantity,
                            "buy_date": day_str,
                            "highest": buy_price,
                        }
                        
                        trades.append({
                            "date": day_str,
                            "stock_code": code,
                            "stock_name": code,
                            "trade_type": "buy",
                            "buy_price": buy_price,
                            "sell_price": None,
                            "quantity": quantity,
                            "net_profit": 0,
                            "profit_pct": 0,
                            "fee": commission,
                            "reason": buy_reason,
                        })
        
        # 일별 자산 기록 / Daily Asset Record
        holdings_value = 0
        for code, h in holdings.items():
            # 마지막 캔들 가격 사용
            if code in day_data and day_data[code]:
                last_price = day_data[code][-1]["close"]
            else:
                last_price = h["buy_price"]
            holdings_value += last_price * h["quantity"]
        
        total_asset = cash + holdings_value
        daily_assets.append({
            "date": day_str,
            "cash": round(cash),
            "holdings_value": round(holdings_value),
            "total_asset": round(total_asset),
            "holdings_count": len(holdings),
        })
    
    # ===== 결과 집계 / Aggregate Results =====
    sell_trades = [t for t in trades if t["trade_type"] == "sell"]
    wins = [t for t in sell_trades if t["net_profit"] > 0]
    losses = [t for t in sell_trades if t["net_profit"] <= 0]
    
    total_trades = len(sell_trades)
    win_count = len(wins)
    loss_count = len(losses)
    win_rate = round(win_count / total_trades * 100, 1) if total_trades > 0 else 0
    
    total_profit = sum(t["net_profit"] for t in sell_trades)
    total_fee = sum(t["fee"] for t in trades)
    avg_profit = round(total_profit / total_trades) if total_trades > 0 else 0
    avg_win = round(sum(t["net_profit"] for t in wins) / win_count) if win_count > 0 else 0
    avg_loss = round(sum(t["net_profit"] for t in losses) / loss_count) if loss_count > 0 else 0
    
    # MDD (최대 낙폭) 계산
    max_asset = initial_capital
    max_drawdown = 0
    for da in daily_assets:
        if da["total_asset"] > max_asset:
            max_asset = da["total_asset"]
        dd = (max_asset - da["total_asset"]) / max_asset * 100
        if dd > max_drawdown:
            max_drawdown = dd
    
    final_asset = daily_assets[-1]["total_asset"] if daily_assets else initial_capital
    total_return = round((final_asset - initial_capital) / initial_capital * 100, 2)
    
    # 손익비
    profit_loss_ratio = round(abs(avg_win / avg_loss), 2) if avg_loss != 0 else 0
    
    summary = {
        "strategy": strategy,
        "initial_capital": initial_capital,
        "final_asset": final_asset,
        "total_return_pct": total_return,
        "total_profit": total_profit,
        "total_fee": round(total_fee),
        "total_trades": total_trades,
        "win_count": win_count,
        "loss_count": loss_count,
        "win_rate": win_rate,
        "avg_profit": avg_profit,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "max_drawdown_pct": round(max_drawdown, 2),
        "profit_loss_ratio": profit_loss_ratio,
        "test_days": len(sorted_days),
        "stock_count": len(stock_codes),
        "atr_multiplier": atr_multiplier,
        "stop_loss_pct": stop_loss_pct,
    }
    
    print(f"[백테스트] 완료: 수익률={total_return}%, 승률={win_rate}%, 매매={total_trades}회")
    
    return {
        "summary": summary,
        "trades": trades,
        "daily_assets": daily_assets,
    }


# ============================================================
# 헬퍼 함수들 / Helper Functions
# ============================================================

def _group_candles_by_date(all_candles):
    """캔들 데이터를 일자별로 그룹핑"""
    date_groups = {}
    for code, candles in all_candles.items():
        for c in candles:
            # 캔들에서 날짜 추출 (time 필드 또는 datetime)
            day = c.get("date", c.get("stck_bsop_date", ""))
            if not day and "time" in c:
                day = datetime.now().strftime("%Y-%m-%d")
            if not day:
                continue
            
            if day not in date_groups:
                date_groups[day] = {}
            if code not in date_groups[day]:
                date_groups[day][code] = []
            date_groups[day][code].append(c)
    
    return date_groups


def _check_dip_buy(candles):
    """눌림목 매수 신호 체크 (간소화)"""
    if len(candles) < 20:
        return False, ""
    
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]
    
    current_price = closes[-1]
    recent_high = max(highs[-10:])
    
    # ATR 계산
    atr_vals = atr(highs, lows, closes, min(14, len(candles) - 1))
    current_atr = next((v for v in reversed(atr_vals) if v is not None), 0)
    
    score = 0
    signals = []
    
    # 1. 고점 대비 ATR 범위 내 하락
    if current_atr > 0:
        dip_pct = (recent_high - current_price) / recent_high * 100
        atr_pct = current_atr / recent_high * 100
        if 0.5 * atr_pct <= dip_pct <= 2.0 * atr_pct:
            signals.append("ATR하락")
            score += 1
    
    # 2. 하락 중 거래량 감소
    if len(volumes) >= 5:
        if volumes[-1] < volumes[-3] and closes[-1] < closes[-3]:
            signals.append("거래량감소")
            score += 1
    
    # 3. MA 지지
    ma20 = sma(closes, min(20, len(closes)))
    if ma20[-1] and current_price >= ma20[-1] * 0.995:
        signals.append("MA지지")
        score += 1
    
    # 4. RSI 반등
    rsi_vals = rsi(closes, min(14, len(closes) - 1))
    if rsi_vals[-1] and rsi_vals[-2]:
        if 30 <= rsi_vals[-1] <= 50 and rsi_vals[-1] > rsi_vals[-2]:
            signals.append("RSI반등")
            score += 1
    
    # 5. 양봉 확인
    if candles[-1]["close"] > candles[-1]["open"]:
        signals.append("양봉")
        score += 1
    
    is_dip = score >= 3
    reason = f"눌림목({score}점: {','.join(signals)})" if is_dip else ""
    return is_dip, reason


def _check_gap_buy(candles, day_candles=None):
    """갭상승 매수 신호 체크 (간소화)"""
    if len(candles) < 5:
        return False, ""
    
    # 전일 종가 vs 오늘 시가 비교
    # 캔들이 시간순으로 정렬되어 있다고 가정
    today_open = candles[0]["open"]
    
    # 이전 데이터에서 전일 종가 추정 (캔들 리스트의 초반부)
    prev_close = candles[0].get("prev_close")
    if not prev_close and len(candles) > 30:
        # 30개 전 캔들을 전일 종가로 추정
        prev_close = candles[-31]["close"] if len(candles) > 31 else candles[0]["close"]
    
    if not prev_close or prev_close <= 0:
        return False, ""
    
    gap_pct = (today_open - prev_close) / prev_close * 100
    
    score = 0
    signals = []
    
    # 1. 갭 상승 2% 이상
    if gap_pct >= 2.0:
        signals.append(f"갭+{gap_pct:.1f}%")
        score += 1
    
    # 2. 갭 이후 추가 상승
    if candles[-1]["close"] > today_open:
        signals.append("추가상승")
        score += 1
    
    # 3. 거래량 증가 (첫 5분)
    if len(candles) >= 5:
        early_vol = sum(c["volume"] for c in candles[:5])
        if early_vol > 0:
            signals.append("거래량동반")
            score += 1
    
    # 4. 양봉 유지
    if candles[-1]["close"] > candles[-1]["open"]:
        signals.append("양봉유지")
        score += 1
    
    is_gap = gap_pct >= 2.0 and score >= 3
    reason = f"갭상승({gap_pct:.1f}%, {score}점: {','.join(signals)})" if is_gap else ""
    return is_gap, reason


def _empty_summary(strategy, initial_capital):
    """데이터 없을 때 빈 요약"""
    return {
        "strategy": strategy,
        "initial_capital": initial_capital,
        "final_asset": initial_capital,
        "total_return_pct": 0,
        "total_profit": 0,
        "total_fee": 0,
        "total_trades": 0,
        "win_count": 0,
        "loss_count": 0,
        "win_rate": 0,
        "avg_profit": 0,
        "avg_win": 0,
        "avg_loss": 0,
        "max_drawdown_pct": 0,
        "profit_loss_ratio": 0,
        "test_days": 0,
        "stock_count": 0,
        "atr_multiplier": 2.0,
        "stop_loss_pct": 3.0,
    }
