"""매매 실행 총괄 모듈"""
from datetime import datetime, date, timedelta
from app.core.database import db
from app.services.kis_stock import get_current_price, get_minute_candles, get_orderbook
from app.services.kis_order import buy_stock, sell_stock
from app.services.kakao_alert import kakao
from app.engine.dip_detector import detect_dip
from app.engine.trailing_stop import TrailingStop
from app.engine.stop_loss import check_stop_loss
from app.utils.tax_calculator import calc_net_profit
from app.utils.indicators import atr, vwap

# 전략별 트레일링 스톱 인스턴스
trailing_stops = {}

async def execute_trading_cycle():
    """1분 간격 매매 사이클"""
    try:
        print(f"[매매사이클] 실행 시작")
        
        # 활성 전략 가져오기
        strategies = db.table("strategies").select("*").eq("is_active", True).execute()
        if not strategies.data:
            return

        # 감시종목 가져오기
        today = date.today().isoformat()
        watchlist = db.table("watchlist").select("*").eq("scan_date", today).eq("status", "감시중").execute()
        print(f"[매매사이클] 전략 {len(strategies.data)}개, 감시종목 {len(watchlist.data or [])}개")
        
        for strategy in strategies.data:
            await _process_strategy(strategy, watchlist.data or [])

    except Exception as e:
        print(f"[매매 사이클 오류] {e}")
        _log("에러", f"매매 사이클 오류: {e}")

async def _process_strategy(strategy, watchlist):
    """전략별 매매 처리"""
    sid = strategy["id"]
    is_live = strategy.get("is_live", False)

    # 1. 보유종목 매도 확인
    holdings = db.table("holdings").select("*").eq("strategy_id", sid).execute()
    for holding in (holdings.data or []):
        await _check_sell(strategy, holding, is_live)

    # 2. 신규 매수 확인
    for stock in watchlist:
        # 차단 종목 확인
        if await _is_blocked(sid, stock["stock_code"]):
            continue
        await _check_buy(strategy, stock, is_live)

async def _check_buy(strategy, stock, is_live):
    """매수 조건 확인"""
    code = stock["stock_code"]
    sid = strategy["id"]

    try:
        candles_1m = get_minute_candles(code, is_live=is_live)
        candles_3m = get_minute_candles(code, is_live=is_live)  # 3분봉 근사
        candles_5m = get_minute_candles(code, is_live=is_live)  # 5분봉 근사
        orderbook = get_orderbook(code, is_live=is_live)

        if not candles_3m or not candles_5m:
            return

        result = detect_dip(candles_1m, candles_3m, candles_5m, orderbook)

        if result["is_dip"]:
            price_info = get_current_price(code, is_live=is_live)
            if not price_info:
                return

            price = price_info["price"]
            # 투자금액 계산 (현재 자산의 일정 비율)
            capital = await _get_available_capital(sid)
            invest_amount = min(capital * 0.2, capital)  # 최대 20%
            quantity = int(invest_amount / price)

            if quantity <= 0:
                return

            # 매수 주문
            order = buy_stock(code, quantity, price, is_live=is_live)
            if order.get("success"):
                # DB 기록
                db.table("trades").insert({
                    "strategy_id": sid, "stock_code": code, "stock_name": stock["stock_name"],
                    "trade_type": "buy", "buy_price": price, "current_price": price,
                    "quantity": quantity, "trade_reason": f"눌림목매수: {','.join(result['signals'])}",
                }).execute()

                db.table("holdings").insert({
                    "strategy_id": sid, "stock_code": code, "stock_name": stock["stock_name"],
                    "buy_price": price, "current_price": price, "quantity": quantity,
                }).execute()

                # 트레일링 스톱 설정
                atr_val = result.get("atr", price * 0.02)
                ts = TrailingStop(price, atr_val, strategy.get("atr_multiplier", 2.0))
                trailing_stops[f"{sid}_{code}"] = ts

                # 감시종목 상태 업데이트
                db.table("watchlist").update({"status": "매수완료"}).eq("stock_code", code).execute()

                _log("매매판단", f"매수: {stock['stock_name']} {price}원 {quantity}주", sid)
                print(f"[매수] {stock['stock_name']} {price}원 {quantity}주")
                kakao.alert_buy(stock['stock_name'], price, quantity, ','.join(result['signals']))

    except Exception as e:
        print(f"[매수 확인 오류] {code}: {e}")

async def _check_sell(strategy, holding, is_live):
    """매도 조건 확인 (트레일링 스톱 + 손절)"""
    code = holding["stock_code"]
    sid = strategy["id"]
    key = f"{sid}_{code}"

    try:
        price_info = get_current_price(code, is_live=is_live)
        if not price_info:
            return

        current_price = price_info["price"]
        buy_price = holding["buy_price"]
        quantity = holding["quantity"]

        # 현재가 업데이트
        db.table("holdings").update({
            "current_price": current_price,
            "unrealized_profit": (current_price - buy_price) * quantity,
            "unrealized_pct": round((current_price - buy_price) / buy_price * 100, 4),
            "updated_at": datetime.now().isoformat(),
        }).eq("id", holding["id"]).execute()

        # 트레일링 스톱 확인
        should_sell = False
        sell_reason = ""

        if key in trailing_stops:
            ts_result = trailing_stops[key].update(current_price)
            if ts_result["should_sell"] and current_price > buy_price:
                should_sell = True
                sell_reason = f"트레일링스톱 (최고점:{ts_result['highest']})"

        # 손절 확인
        if not should_sell:
            candles_5m = get_minute_candles(code, is_live=is_live)
            if candles_5m:
                closes = [c["close"] for c in candles_5m]
                highs = [c["high"] for c in candles_5m]
                lows = [c["low"] for c in candles_5m]
                volumes = [c["volume"] for c in candles_5m]

                from app.utils.indicators import atr as calc_atr, vwap as calc_vwap
                atr_vals = calc_atr(highs, lows, closes, 14)
                vwap_vals = calc_vwap(highs, lows, closes, volumes)
                atr_val = atr_vals[-1] if atr_vals[-1] else 0
                vwap_val = vwap_vals[-1] if vwap_vals[-1] else 0

                stop_result = check_stop_loss(
                    buy_price, current_price, candles_5m,
                    atr_val, vwap_val, strategy.get("stop_loss_pct", -3.0)
                )
                if stop_result["should_stop"]:
                    should_sell = True
                    sell_reason = stop_result["reason"]

        # 매도 실행
        if should_sell:
            order = sell_stock(code, quantity, current_price, is_live=is_live)
            if order.get("success"):
                profit = calc_net_profit(buy_price, current_price, quantity)

                # 매매 기록
                db.table("trades").insert({
                    "strategy_id": sid, "stock_code": code,
                    "stock_name": holding["stock_name"], "trade_type": "sell",
                    "buy_price": buy_price, "sell_price": current_price,
                    "current_price": current_price, "quantity": quantity,
                    "commission": profit["commission"], "tax": profit["tax"],
                    "net_profit": profit["net_profit"], "profit_pct": profit["profit_pct"],
                    "trade_reason": sell_reason,
                }).execute()

                # 보유종목 삭제
                db.table("holdings").delete().eq("id", holding["id"]).execute()

                # 트레일링 스톱 제거
                if key in trailing_stops:
                    del trailing_stops[key]

                # 손절 시 차단 처리
                if profit["net_profit"] < 0:
                    await _handle_loss(sid, code, holding["stock_name"])

                _log("매매판단", f"매도: {holding['stock_name']} {current_price}원 순수익:{profit['net_profit']}원 ({sell_reason})", sid)
                print(f"[매도] {holding['stock_name']} {current_price}원 순수익:{profit['net_profit']}원")
                kakao.alert_sell(holding['stock_name'], buy_price, current_price, quantity, profit['net_profit'], sell_reason)

    except Exception as e:
        print(f"[매도 확인 오류] {code}: {e}")

async def _handle_loss(sid, code, name):
    """손절 처리: 연속 3회 당일 중지 + 3일 차단"""
    today = date.today().isoformat()

    # 당일 해당 종목 연속 손절 확인
    trades = db.table("trades").select("*").eq("strategy_id", sid).eq("stock_code", code).eq("trade_type", "sell").order("traded_at", desc=True).limit(3).execute()

    consecutive = 0
    for t in (trades.data or []):
        if t.get("net_profit", 0) < 0:
            consecutive += 1
        else:
            break

    if consecutive >= 3:
        # 당일 매매 중지 (다음날 자정까지)
        db.table("blocked_stocks").insert({
            "strategy_id": sid, "stock_code": code, "stock_name": name,
            "block_reason": "당일3연속손절", "consecutive_losses": consecutive,
            "unblock_at": (datetime.now().replace(hour=23, minute=59)).isoformat(),
        }).execute()
        kakao.alert_blocked(name, f"연속 {consecutive}회 손절 → 당일 매매 중지", "오늘 자정")
    else:
        # 3일 차단
        unblock = datetime.now() + timedelta(days=3)
        db.table("blocked_stocks").insert({
            "strategy_id": sid, "stock_code": code, "stock_name": name,
            "block_reason": "3일차단", "consecutive_losses": 1,
            "unblock_at": unblock.isoformat(),
        }).execute()
        kakao.alert_blocked(name, "손절 → 3일 재매수 금지", unblock.strftime("%m/%d"))

async def _is_blocked(sid, code):
    """차단 종목 확인"""
    now = datetime.now().isoformat()
    result = db.table("blocked_stocks").select("*").eq("strategy_id", sid).eq("stock_code", code).gt("unblock_at", now).execute()
    return bool(result.data)

async def _get_available_capital(sid):
    """사용 가능 자금 계산"""
    # 전략의 초기 자금 + 누적 수익
    strategy = db.table("strategies").select("initial_capital").eq("id", sid).execute()
    initial = strategy.data[0]["initial_capital"] if strategy.data else 1000000

    # 실현 수익 합산
    profits = db.table("trades").select("net_profit").eq("strategy_id", sid).eq("trade_type", "sell").execute()
    total_profit = sum(t["net_profit"] for t in (profits.data or []) if t.get("net_profit"))

    # 현재 보유 종목 매수금액
    holdings = db.table("holdings").select("buy_price,quantity").eq("strategy_id", sid).execute()
    invested = sum(h["buy_price"] * h["quantity"] for h in (holdings.data or []))

    return initial + total_profit - invested

async def generate_daily_report():
    """일일 리포트 생성"""
    today = date.today().isoformat()
    strategies = db.table("strategies").select("*").eq("is_active", True).execute()

    for s in (strategies.data or []):
        sid = s["id"]
        trades = db.table("trades").select("*").eq("strategy_id", sid).gte("traded_at", f"{today}T00:00:00").execute()
        sells = [t for t in (trades.data or []) if t["trade_type"] == "sell"]

        wins = len([t for t in sells if (t.get("net_profit") or 0) > 0])
        losses = len([t for t in sells if (t.get("net_profit") or 0) <= 0])
        total_profit = sum(t.get("net_profit", 0) for t in sells)
        total_commission = sum(t.get("commission", 0) for t in sells)
        total_tax = sum(t.get("tax", 0) for t in sells)
        win_rate = round(wins / len(sells) * 100, 2) if sells else 0

        db.table("daily_reports").upsert({
            "strategy_id": sid, "report_date": today,
            "total_trades": len(sells), "win_count": wins, "lose_count": losses,
            "win_rate": win_rate, "total_profit": total_profit,
            "total_commission": total_commission, "total_tax": total_tax,
        }).execute()

        # 자산 추이 기록
        capital = await _get_available_capital(sid)
        holdings = db.table("holdings").select("current_price,quantity").eq("strategy_id", sid).execute()
        holdings_value = sum(h["current_price"] * h["quantity"] for h in (holdings.data or []))
        total_asset = capital + holdings_value

        initial = s["initial_capital"]
        cum_return = round((total_asset - initial) / initial * 100, 4)
        progress = round(total_asset / 1000000000 * 100, 4)

        # 예상 남은 일수
        days_elapsed = (date.today() - datetime.fromisoformat(str(s["created_at"])).date()).days
        if days_elapsed > 0 and cum_return > 0:
            daily_return = cum_return / days_elapsed
            if daily_return > 0:
                import math
                days_left = int(math.log(1000000000 / total_asset) / math.log(1 + daily_return / 100))
            else:
                days_left = None
        else:
            days_left = None

        db.table("asset_history").upsert({
            "strategy_id": sid, "record_date": today,
            "total_asset": total_asset, "daily_profit": total_profit,
            "daily_return_pct": round(total_profit / (total_asset - total_profit) * 100, 4) if total_asset > total_profit else 0,
            "cumulative_return_pct": cum_return, "target_progress_pct": progress,
            "estimated_days_left": days_left,
        }).execute()

        print(f"[리포트] {s['name']}: 자산 {total_asset:,.0f}원, 오늘 수익 {total_profit:,.0f}원")
        kakao.alert_daily_report(s['name'], total_asset, total_profit, wins, losses, win_rate)

def _log(log_type, message, sid=None):
    try:
        db.table("system_logs").insert({
            "strategy_id": sid, "log_type": log_type, "message": message,
        }).execute()
    except:
        pass
