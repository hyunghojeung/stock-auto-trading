"""
갭상승전략 스케줄러 (Gap Strategy Scheduler)
=============================================
기존 scheduler.py에 추가할 갭상승전략 작업들

스케줄:
  [전일 18:00] 야간 데이터 사전 계산 (전종목 ATR/RSI/MACD)
  [09:00~09:01] 갭 탐지 + 유형 분류 + 1차 필터링
  [09:01~09:30] ORB 범위 수집 (1분 간격)
  [09:30~] 진입 판단 시작 (1분 간격)
  [~15:00] 매도 관리
  [15:00~15:30] 미체결 정리
"""
from datetime import datetime, date, time
from app.engine.gap_detector import (
    detect_gap_stocks,
    first_filter,
    ORBTracker,
    check_gap_and_go_entry,
    check_gap_fill_entry,
    check_gap_exit,
    precompute_gap_data,
    load_precomputed_data,
    save_gap_watchlist,
)
from app.services.kis_stock import (
    get_all_stock_prices,
    get_minute_candles,
    get_stock_price,
)
from app.services.kis_order import buy_stock, sell_stock
from app.services.kakao_alert import send_kakao
from app.core.database import db
from app.utils.kr_holiday import is_market_open_day
import traceback


# ── 글로벌 상태 ──
orb_tracker = ORBTracker()
gap_filtered_stocks = []   # 1차 필터 통과 종목
gap_holdings = {}           # 갭전략 보유종목 {code: {...}}
gap_phase = "idle"          # "idle" | "scanning" | "orb_collecting" | "trading" | "closing"


# ============================================================
# JOB 1: 야간 데이터 사전 계산 (전일 18:00)
# ============================================================
async def gap_night_precompute_job():
    """
    전일 18:00: 갭상승전략용 데이터 사전 계산
    - 전종목 ATR, RSI, MACD, 지지/저항선 → DB 저장
    - 종목 선별 안 함 (갭상승은 당일 09:00에 판단)
    """
    global gap_phase
    
    # 내일이 거래일인지 확인
    from datetime import timedelta
    tomorrow = date.today() + timedelta(days=1)
    if not is_market_open_day(tomorrow):
        print(f"[갭전략 야간] 내일({tomorrow})은 휴장일 → 스킵")
        return
    
    print(f"[갭전략 야간] 데이터 사전 계산 시작")
    gap_phase = "idle"
    
    try:
        count = await precompute_gap_data()
        msg = f"📊 [갭전략 야간준비] {count}개 종목 데이터 사전 계산 완료"
        print(msg)
        await send_kakao(msg)
    except Exception as e:
        print(f"[갭전략 야간 오류] {e}")
        traceback.print_exc()


# ============================================================
# JOB 2: 갭 탐지 + 필터링 (09:00~09:01)
# ============================================================
async def gap_scan_job():
    """
    09:00 장 개장 직후: 전종목 시가 확인 → +3% 갭상승 탐지 → 필터링
    """
    global gap_filtered_stocks, gap_phase, orb_tracker
    
    if not is_market_open_day(date.today()):
        return
    
    gap_phase = "scanning"
    orb_tracker.clear()
    gap_filtered_stocks = []
    
    print(f"[갭전략] ===== 갭 탐지 시작 (09:00) =====")
    
    try:
        # 1) 전종목 시가 조회
        all_stocks = await get_all_stock_prices()
        
        # 2) 사전 계산 데이터 로드
        precomputed = await load_precomputed_data()
        
        # 3) 전일 종가 맵 생성
        prev_close_map = {
            code: data.get("prev_close", 0) 
            for code, data in precomputed.items()
        }
        
        # 4) 갭 탐지 (+3% 이상)
        gap_candidates = detect_gap_stocks(all_stocks, prev_close_map)
        
        if not gap_candidates:
            print(f"[갭전략] 갭상승 종목 없음 → 대기")
            gap_phase = "idle"
            await send_kakao("📊 [갭전략] 오늘 +3% 이상 갭상승 종목 없음")
            return
        
        # 5) 실시간 데이터 (거래량, 거래대금)
        realtime_data = {}
        for stock in gap_candidates[:30]:  # 상위 30개만
            try:
                rt = await get_stock_price(stock["code"])
                if rt:
                    realtime_data[stock["code"]] = rt
            except:
                pass
        
        # 6) 1차 필터링
        gap_filtered_stocks = first_filter(gap_candidates, precomputed, realtime_data)
        
        if not gap_filtered_stocks:
            print(f"[갭전략] 필터 통과 종목 없음 → 대기")
            gap_phase = "idle"
            await send_kakao("📊 [갭전략] 갭상승 감지됐으나 필터 통과 종목 없음")
            return
        
        # 7) 감시종목 DB 저장
        await save_gap_watchlist(gap_filtered_stocks)
        
        # 8) ORB 추적 시작
        for stock in gap_filtered_stocks:
            orb_tracker.start_tracking(stock["code"], stock["open_price"])
        
        gap_phase = "orb_collecting"
        
        # 9) 알림
        names = ", ".join(f"{s['name']}(+{s['gap_pct']}%)" for s in gap_filtered_stocks[:5])
        msg = (f"🚀 [갭전략 09:00] 갭상승 {len(gap_filtered_stocks)}개 감지!\n"
               f"상위: {names}\n"
               f"ORB 수집 시작 (09:00~09:30)")
        print(msg)
        await send_kakao(msg)
        
    except Exception as e:
        print(f"[갭전략 스캔 오류] {e}")
        traceback.print_exc()
        gap_phase = "idle"


# ============================================================
# JOB 3: ORB 데이터 수집 (09:01~09:30, 1분 간격)
# ============================================================
async def gap_orb_collect_job():
    """
    09:01~09:30: 필터 통과 종목의 1분봉 수집 → ORB 범위 업데이트
    """
    global gap_phase
    
    if gap_phase != "orb_collecting" or not gap_filtered_stocks:
        return
    
    now = datetime.now()
    
    # 09:30 이후면 ORB 수집 종료 → 트레이딩 페이즈로 전환
    if now.hour == 9 and now.minute >= 30:
        gap_phase = "trading"
        
        # ORB 결과 출력
        for stock in gap_filtered_stocks:
            orb = orb_tracker.get_orb(stock["code"])
            if orb:
                print(f"  [ORB 확정] {stock['name']}: "
                      f"고점={orb['orb_high']:,} 저점={orb['orb_low']:,} "
                      f"범위={orb['orb_range']:,} VWAP={orb['vwap_30']:,}")
        
        msg = f"📊 [갭전략 09:30] ORB 확정! {len(gap_filtered_stocks)}개 종목 진입 판단 시작"
        await send_kakao(msg)
        return
    
    # 1분봉 수집
    for stock in gap_filtered_stocks:
        try:
            candles = await get_minute_candles(stock["code"], "1", count=1)
            if candles:
                orb_tracker.update(stock["code"], candles[-1])
        except Exception as e:
            pass


# ============================================================
# JOB 4: 진입 판단 (09:30~, 1분 간격)
# ============================================================
async def gap_entry_check_job():
    """
    09:30 이후: 1분 간격으로 진입 조건 확인
    """
    global gap_phase, gap_holdings
    
    if gap_phase != "trading" or not gap_filtered_stocks:
        return
    
    now = datetime.now()
    
    # 14:30 이후 신규 매수 중지
    if now.hour >= 14 and now.minute >= 30:
        return
    
    precomputed = await load_precomputed_data()
    
    for stock in gap_filtered_stocks:
        code = stock["code"]
        
        # 이미 보유 중이면 스킵
        if code in gap_holdings:
            continue
        
        try:
            # 최근 1분봉 조회
            candles = await get_minute_candles(code, "1", count=10)
            if not candles or len(candles) < 3:
                continue
            
            current_candle = candles[-1]
            orb = orb_tracker.get_orb(code)
            
            if not orb:
                continue
            
            # Gap and Go 진입 확인
            result = check_gap_and_go_entry(
                code, current_candle, orb, stock, candles[:-1]
            )
            
            if not result["entry"]:
                # Gap Fill 진입 확인
                result = check_gap_fill_entry(
                    code, current_candle, orb, stock, candles[:-1]
                )
            
            if result["entry"]:
                # ── 매수 실행 ──
                await _execute_gap_buy(code, stock, result, precomputed.get(code, {}))
                
        except Exception as e:
            print(f"[갭전략 진입확인 오류] {code}: {e}")


# ============================================================
# JOB 5: 매도 관리 (09:30~15:00, 1분 간격)
# ============================================================
async def gap_exit_check_job():
    """
    보유종목 매도 조건 확인 (트레일링 스톱, 손절)
    """
    global gap_holdings
    
    if not gap_holdings:
        return
    
    codes_to_remove = []
    
    for code, holding in gap_holdings.items():
        try:
            candles = await get_minute_candles(code, "1", count=5)
            if not candles:
                continue
            
            current_candle = candles[-1]
            current_price = current_candle["close"]
            
            result = check_gap_exit(holding, current_price, current_candle, candles[:-1])
            
            if result["exit"]:
                # ── 매도 실행 ──
                exit_pct = result.get("exit_pct", 100)
                await _execute_gap_sell(code, holding, current_price, result, exit_pct)
                
                if exit_pct >= 100:
                    codes_to_remove.append(code)
                    
        except Exception as e:
            print(f"[갭전략 매도확인 오류] {code}: {e}")
    
    for code in codes_to_remove:
        gap_holdings.pop(code, None)


# ============================================================
# JOB 6: 미체결 정리 (15:00~15:30)
# ============================================================
async def gap_close_job():
    """
    장 마감 전 미체결/보유 정리
    """
    global gap_phase, gap_holdings, gap_filtered_stocks
    
    if gap_phase == "idle":
        return
    
    print(f"[갭전략] ===== 장 마감 정리 (15:00) =====")
    
    # 남은 보유종목 전량 매도
    for code, holding in list(gap_holdings.items()):
        try:
            price_data = await get_stock_price(code)
            if price_data:
                current_price = price_data.get("price", 0)
                await _execute_gap_sell(
                    code, holding, current_price,
                    {"exit": True, "reason": "장마감 정리", "exit_type": "market_close"},
                    100
                )
        except Exception as e:
            print(f"[장마감 정리 오류] {code}: {e}")
    
    gap_holdings.clear()
    gap_filtered_stocks = []
    orb_tracker.clear()
    gap_phase = "idle"
    
    await send_kakao("📊 [갭전략] 장 마감 정리 완료")


# ============================================================
# 매수/매도 실행 헬퍼
# ============================================================
async def _execute_gap_buy(code, stock_info, entry_result, precomputed):
    """갭전략 매수 실행"""
    global gap_holdings
    
    strategy_name = entry_result["strategy"]
    entry_price = entry_result["entry_price"]
    
    try:
        # 매수 수량 계산 (1종목당 최대 투자금 제한)
        max_per_stock = 500_000  # 50만원
        quantity = int(max_per_stock / entry_price)
        
        if quantity <= 0:
            print(f"[갭매수 스킵] {stock_info['name']} 가격 너무 높음")
            return
        
        # KIS API 매수 주문
        order_result = await buy_stock(code, quantity, entry_price)
        
        if order_result and order_result.get("success"):
            # 보유 정보 저장
            gap_holdings[code] = {
                "code": code,
                "name": stock_info.get("name", ""),
                "strategy": strategy_name,
                "entry_price": entry_price,
                "quantity": quantity,
                "orb_low": entry_result.get("orb_low", 0),
                "orb_high": entry_result.get("orb_high", 0),
                "vwap": entry_result.get("vwap", 0),
                "target_price": entry_result.get("target_price", 0),
                "stop_loss_1": entry_result.get("stop_loss_1", 0),
                "stop_loss_2": entry_result.get("stop_loss_2", 0),
                "stop_loss_3": entry_result.get("stop_loss_3", 0),
                "highest_price": entry_price,
                "atr_val": precomputed.get("atr_20", 0),
                "partial_sold": False,
                "buy_time": datetime.now().isoformat(),
            }
            
            # DB 기록
            db.table("trades").insert({
                "stock_code": code,
                "stock_name": stock_info.get("name", ""),
                "trade_type": "buy",
                "strategy_type": strategy_name,
                "price": entry_price,
                "quantity": quantity,
                "total_amount": entry_price * quantity,
                "reason": entry_result.get("reason", ""),
                "trade_date": date.today().isoformat(),
                "trade_time": datetime.now().strftime("%H:%M:%S"),
            }).execute()
            
            # 알림
            emoji = "🚀" if strategy_name == "gap_and_go" else "🔄"
            msg = (f"{emoji} [갭전략 매수] {stock_info.get('name', '')}({code})\n"
                   f"전략: {strategy_name} | 갭: +{stock_info.get('gap_pct', 0)}%\n"
                   f"매수가: {entry_price:,}원 × {quantity}주\n"
                   f"사유: {entry_result.get('reason', '')}")
            print(msg)
            await send_kakao(msg)
            
    except Exception as e:
        print(f"[갭매수 오류] {code}: {e}")
        traceback.print_exc()


async def _execute_gap_sell(code, holding, current_price, exit_result, exit_pct=100):
    """갭전략 매도 실행"""
    try:
        total_qty = holding.get("quantity", 0)
        sell_qty = int(total_qty * exit_pct / 100)
        
        if sell_qty <= 0:
            return
        
        entry_price = holding.get("entry_price", 0)
        profit = (current_price - entry_price) * sell_qty
        profit_pct = (current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
        
        # KIS API 매도 주문
        order_result = await sell_stock(code, sell_qty, current_price)
        
        if order_result and order_result.get("success"):
            # 수량 업데이트
            if exit_pct < 100:
                holding["quantity"] = total_qty - sell_qty
            
            # DB 기록
            db.table("trades").insert({
                "stock_code": code,
                "stock_name": holding.get("name", ""),
                "trade_type": "sell",
                "strategy_type": holding.get("strategy", "gap"),
                "price": current_price,
                "quantity": sell_qty,
                "total_amount": current_price * sell_qty,
                "net_profit": round(profit),
                "profit_pct": round(profit_pct, 2),
                "reason": exit_result.get("reason", ""),
                "trade_date": date.today().isoformat(),
                "trade_time": datetime.now().strftime("%H:%M:%S"),
            }).execute()
            
            # 알림
            emoji = "💰" if profit > 0 else "🔻"
            msg = (f"{emoji} [갭전략 매도] {holding.get('name', '')}({code})\n"
                   f"매도가: {current_price:,}원 × {sell_qty}주 ({exit_pct}%)\n"
                   f"수익: {profit:+,.0f}원 ({profit_pct:+.2f}%)\n"
                   f"사유: {exit_result.get('reason', '')}")
            print(msg)
            await send_kakao(msg)
            
    except Exception as e:
        print(f"[갭매도 오류] {code}: {e}")
        traceback.print_exc()
