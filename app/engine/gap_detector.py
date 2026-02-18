"""
갭상승 감지 엔진 (Gap-Up Detection Engine)
==========================================
갭상승전략의 핵심 로직:
  STEP 1: 갭 탐지 (+3% 이상)
  STEP 2: 갭 유형 분류 (돌파갭/진행갭/보통갭/소멸갭)
  STEP 3: 1차 필터링 (RVOL, RSI, MACD, 거래대금)
  STEP 4: ORB 범위 설정 (09:00~09:30)
  STEP 5: 진입 판단 (Gap and Go / Gap Fill)
  STEP 6: 매도 전략 (트레일링 스톱 / 손절)
"""
from datetime import datetime, date, timedelta
from app.utils.indicators import sma, ema, rsi, atr, vwap, macd
from app.engine.pattern_detector import detect_patterns
from app.core.database import db
import traceback


# ============================================================
# STEP 1: 갭 탐지 (Gap Detection)
# ============================================================
def detect_gap_stocks(all_stocks_today, prev_close_map):
    """
    전종목 시가 확인 → 전일 종가 대비 +3% 이상 갭상승 종목 추출
    
    Parameters:
        all_stocks_today: list[dict] - 당일 시가 포함 전종목 데이터
            [{"code": "005930", "name": "삼성전자", "open_price": 85000, ...}, ...]
        prev_close_map: dict - 전일 종가 매핑
            {"005930": 82000, "000660": 175000, ...}
    
    Returns:
        list[dict] - 갭상승 후보 목록
    """
    GAP_THRESHOLD = 3.0  # +3% 최소 기준
    
    gap_candidates = []
    
    for stock in all_stocks_today:
        code = stock.get("code", "")
        open_price = stock.get("open_price", 0)
        prev_close = prev_close_map.get(code, 0)
        
        if prev_close <= 0 or open_price <= 0:
            continue
        
        gap_pct = (open_price - prev_close) / prev_close * 100
        
        if gap_pct >= GAP_THRESHOLD:
            gap_candidates.append({
                "code": code,
                "name": stock.get("name", ""),
                "market": stock.get("market", ""),
                "open_price": open_price,
                "prev_close": prev_close,
                "gap_pct": round(gap_pct, 2),
                "gap_amount": open_price - prev_close,
                "detected_at": datetime.now().isoformat(),
            })
    
    # 갭% 내림차순 정렬
    gap_candidates.sort(key=lambda x: x["gap_pct"], reverse=True)
    
    print(f"[갭탐지] 전종목 {len(all_stocks_today)}개 중 "
          f"+3% 이상 갭상승: {len(gap_candidates)}개")
    
    return gap_candidates


# ============================================================
# STEP 2: 갭 유형 분류 (Gap Type Classification)
# ============================================================
def classify_gap_type(code, gap_amount, precomputed_data):
    """
    ATR 기반 갭 유형 분류
    
    Parameters:
        code: str - 종목코드
        gap_amount: float - 갭 크기 (시가 - 전일종가)
        precomputed_data: dict - 야간 사전계산 데이터
            {"atr_20": float, "high_20d": float, "low_20d": float, 
             "resistance": float, "prev_close": float}
    
    Returns:
        str - "돌파갭" | "진행갭" | "보통갭" | "소멸갭"
    """
    data = precomputed_data.get(code, {})
    atr_20 = data.get("atr_20", 0)
    high_20d = data.get("high_20d", 0)
    prev_close = data.get("prev_close", 0)
    resistance = data.get("resistance", 0)
    
    if atr_20 <= 0:
        return "보통갭"  # 데이터 부족 시 기본값
    
    gap_abs = abs(gap_amount)
    
    # 소멸갭: 갭크기 > ATR×2.0 AND 전일 종가가 20일 고점 근처 (5% 이내)
    if gap_abs > atr_20 * 2.0:
        if high_20d > 0 and prev_close >= high_20d * 0.95:
            return "소멸갭"
    
    # 돌파갭: 갭크기 > ATR×1.5 AND 저항선 돌파
    if gap_abs > atr_20 * 1.5:
        if resistance > 0 and (prev_close + gap_amount) > resistance:
            return "돌파갭"
        # 저항선 데이터 없으면 갭 크기만으로 돌파갭 추정
        if resistance == 0 and gap_abs > atr_20 * 1.8:
            return "돌파갭"
    
    # 보통갭: 갭크기 < ATR×1.0
    if gap_abs < atr_20 * 1.0:
        return "보통갭"
    
    # 그 외: 진행갭
    return "진행갭"


# ============================================================
# STEP 3: 1차 필터링 (First Filter)
# ============================================================
def first_filter(gap_candidates, precomputed_data, realtime_data=None):
    """
    RVOL, RSI, MACD, 거래대금 기준 필터링
    
    Parameters:
        gap_candidates: list[dict] - 갭상승 후보
        precomputed_data: dict - 야간 사전계산 데이터 {code: {...}}
        realtime_data: dict - 실시간 데이터 (거래량, 거래대금 등)
    
    Returns:
        list[dict] - 필터 통과 종목
    """
    filtered = []
    
    for stock in gap_candidates:
        code = stock["code"]
        data = precomputed_data.get(code, {})
        rt = (realtime_data or {}).get(code, {})
        
        # ── 갭 유형 분류 ──
        gap_type = classify_gap_type(code, stock["gap_amount"], precomputed_data)
        
        # ❌ 소멸갭 즉시 제외
        if gap_type == "소멸갭":
            print(f"  [필터] {stock['name']}({code}) 소멸갭 → 제외")
            continue
        
        # ── RVOL >= 1.5 ──
        avg_vol_5d = data.get("avg_volume_5d", 0)
        current_vol = rt.get("volume", 0)
        rvol = current_vol / avg_vol_5d if avg_vol_5d > 0 else 0
        if rvol < 1.5 and current_vol > 0:
            continue
        
        # ── RSI(14) 40~75 ──
        rsi_val = data.get("rsi_14", 50)
        if rsi_val is not None and (rsi_val < 40 or rsi_val > 75):
            continue
        
        # ── MACD 히스토그램 > 0 ──
        macd_hist = data.get("macd_histogram", 0)
        if macd_hist is not None and macd_hist <= 0:
            continue
        
        # ── 거래대금 > 1억원 ──
        trade_amount = rt.get("trade_amount", 0)
        if trade_amount > 0 and trade_amount < 100_000_000:
            continue
        
        # 필터 통과
        stock["gap_type"] = gap_type
        stock["rvol"] = round(rvol, 2)
        stock["rsi_14"] = rsi_val
        stock["macd_histogram"] = macd_hist
        stock["trade_amount"] = trade_amount
        
        filtered.append(stock)
    
    print(f"[1차필터] {len(gap_candidates)}개 → {len(filtered)}개 통과")
    
    return filtered


# ============================================================
# STEP 4: ORB 범위 설정 (Opening Range Breakout)
# ============================================================
class ORBTracker:
    """09:00~09:30 ORB 범위를 추적하는 클래스"""
    
    def __init__(self):
        self.tracking = {}  # {code: {"high": x, "low": x, "vwap_num": x, "vwap_den": x, "candles": []}}
    
    def start_tracking(self, code, open_price):
        """ORB 추적 시작"""
        self.tracking[code] = {
            "high": open_price,
            "low": open_price,
            "vwap_numerator": 0.0,
            "vwap_denominator": 0.0,
            "candles": [],
            "start_time": datetime.now(),
        }
    
    def update(self, code, candle):
        """
        1분봉 데이터로 ORB 범위 업데이트
        
        candle: {"open": x, "high": x, "low": x, "close": x, "volume": x, "time": "09:05"}
        """
        if code not in self.tracking:
            return
        
        t = self.tracking[code]
        t["high"] = max(t["high"], candle["high"])
        t["low"] = min(t["low"], candle["low"])
        
        # VWAP 누적 계산
        typical_price = (candle["high"] + candle["low"] + candle["close"]) / 3
        t["vwap_numerator"] += typical_price * candle["volume"]
        t["vwap_denominator"] += candle["volume"]
        
        t["candles"].append(candle)
    
    def get_orb(self, code):
        """
        ORB 범위 조회
        
        Returns:
            dict: {"high": x, "low": x, "range": x, "vwap": x, "candle_count": n}
            None: 아직 추적 중이 아닌 종목
        """
        if code not in self.tracking:
            return None
        
        t = self.tracking[code]
        vwap_val = (t["vwap_numerator"] / t["vwap_denominator"] 
                    if t["vwap_denominator"] > 0 else 0)
        
        return {
            "orb_high": t["high"],
            "orb_low": t["low"],
            "orb_range": t["high"] - t["low"],
            "vwap_30": round(vwap_val, 2),
            "candle_count": len(t["candles"]),
        }
    
    def is_ready(self, code):
        """30분 ORB 데이터 수집 완료 여부"""
        if code not in self.tracking:
            return False
        return len(self.tracking[code]["candles"]) >= 25  # 최소 25개 1분봉
    
    def clear(self, code=None):
        """추적 데이터 초기화"""
        if code:
            self.tracking.pop(code, None)
        else:
            self.tracking.clear()


# ============================================================
# STEP 5: 진입 판단 (Entry Decision)
# ============================================================
def check_gap_and_go_entry(code, candle, orb_data, stock_info, recent_candles):
    """
    Gap and Go 진입 조건 확인 (추세 추종 매수)
    
    조건:
      ✅ 갭 유형 = 돌파갭 또는 진행갭
      ✅ 1분봉 종가 ORB_HIGH 돌파
      ✅ 현재가 > VWAP
      ✅ 돌파 시점 거래량 > 직전 5봉 평균×1.5
      ✅ 봉차트 패턴: 장대양봉 또는 상승장악형
    
    Returns:
        dict: {"entry": True, "reason": "...", "strategy": "gap_and_go", ...}
              {"entry": False, "reason": "...", ...}
    """
    gap_type = stock_info.get("gap_type", "")
    
    # ❌ 돌파갭/진행갭 아닌 경우
    if gap_type not in ("돌파갭", "진행갭"):
        return {"entry": False, "reason": f"Gap and Go 대상 아님 (유형: {gap_type})"}
    
    orb_high = orb_data.get("orb_high", 0)
    orb_low = orb_data.get("orb_low", 0)
    vwap_30 = orb_data.get("vwap_30", 0)
    current_price = candle["close"]
    
    # ✅ ORB 고점 돌파
    if current_price <= orb_high:
        return {"entry": False, "reason": f"ORB고점 미돌파 (현재가:{current_price} <= ORB고점:{orb_high})"}
    
    # ✅ VWAP 위
    if vwap_30 > 0 and current_price <= vwap_30:
        return {"entry": False, "reason": f"VWAP 하회 (현재가:{current_price} <= VWAP:{vwap_30})"}
    
    # ✅ 거래량 돌파 확인
    if len(recent_candles) >= 5:
        avg_vol_5 = sum(c["volume"] for c in recent_candles[-5:]) / 5
        if avg_vol_5 > 0 and candle["volume"] < avg_vol_5 * 1.5:
            return {"entry": False, "reason": f"거래량 부족 (현재:{candle['volume']}, 5봉평균×1.5:{avg_vol_5*1.5:.0f})"}
    
    # ✅ 봉차트 패턴 확인
    if len(recent_candles) >= 3:
        pattern = detect_patterns(recent_candles[-3:])
        bullish_patterns = pattern.get("bullish", [])
        bearish_patterns = pattern.get("bearish", [])
        
        # ❌ 하락 패턴 출현 시 매수 금지
        dangerous = {"하락장악형", "저녁별형"}
        if dangerous & set(bearish_patterns):
            return {"entry": False, "reason": f"하락 패턴 감지: {bearish_patterns}"}
    
    # ✅ RSI 과매수 체크 (>75이면 매수 금지)
    rsi_val = stock_info.get("rsi_14", 50)
    if rsi_val and rsi_val > 75:
        return {"entry": False, "reason": f"RSI 과매수 ({rsi_val})"}
    
    # ── 모든 조건 충족 → 매수 ──
    return {
        "entry": True,
        "strategy": "gap_and_go",
        "sub_type": gap_type,
        "entry_price": current_price,
        "orb_high": orb_high,
        "orb_low": orb_low,
        "vwap": vwap_30,
        "reason": f"Gap and Go 진입: {gap_type} ORB돌파 (현재가:{current_price} > ORB고점:{orb_high})",
        "stop_loss_1": orb_low,                           # 1차 손절: ORB 저점
        "stop_loss_2": vwap_30 if vwap_30 > 0 else None,  # 2차 손절: VWAP
        "stop_loss_3": current_price * 0.97,               # 3차 손절: -3% 절대
    }


def check_gap_fill_entry(code, candle, orb_data, stock_info, recent_candles):
    """
    Gap Fill 진입 조건 확인 (갭 메우기 매수)
    
    조건:
      ✅ 갭 유형 = 보통갭
      ✅ 09:00~09:30 시가 대비 하락 (갭 메우기 진행 중)
      ✅ ORB 저점 부근 반등 시작
      ✅ RSI 30~40 반등
      ✅ 봉차트 패턴: 망치형 또는 샛별형
    
    Returns:
        dict: {"entry": True, ...} or {"entry": False, ...}
    """
    gap_type = stock_info.get("gap_type", "")
    
    # ❌ 보통갭 아닌 경우
    if gap_type != "보통갭":
        return {"entry": False, "reason": f"Gap Fill 대상 아님 (유형: {gap_type})"}
    
    orb_low = orb_data.get("orb_low", 0)
    orb_high = orb_data.get("orb_high", 0)
    open_price = stock_info.get("open_price", 0)
    prev_close = stock_info.get("prev_close", 0)
    current_price = candle["close"]
    
    # ✅ 시가 대비 하락 확인 (갭 메우기 방향)
    if current_price >= open_price:
        return {"entry": False, "reason": "시가 대비 하락 아님 (갭 메우기 미진행)"}
    
    # ✅ ORB 저점 부근 (ORB 범위의 20% 이내)
    orb_range = orb_high - orb_low if orb_high > orb_low else 1
    distance_from_low = current_price - orb_low
    if distance_from_low > orb_range * 0.3:
        return {"entry": False, "reason": f"ORB 저점과 너무 먼 거리 ({distance_from_low:.0f})"}
    
    # ✅ RSI 30~40 반등
    rsi_val = stock_info.get("rsi_14", 50)
    if rsi_val and (rsi_val < 25 or rsi_val > 50):
        return {"entry": False, "reason": f"RSI 범위 밖 ({rsi_val})"}
    
    # ✅ 반등 확인 (최근 봉이 양봉)
    if candle["close"] <= candle["open"]:
        return {"entry": False, "reason": "반등 미확인 (음봉)"}
    
    # ✅ 봉차트 패턴 확인
    if len(recent_candles) >= 3:
        pattern = detect_patterns(recent_candles[-3:])
        bullish_patterns = pattern.get("bullish", [])
        has_reversal = any(p in bullish_patterns for p in ["망치형", "샛별형", "상승장악형"])
        
        if not has_reversal:
            return {"entry": False, "reason": f"반전 패턴 미감지 (감지패턴: {bullish_patterns})"}
    
    # ── 모든 조건 충족 → 매수 ──
    target_price = prev_close  # 목표가 = 전일 종가 (갭 메우기 완료 지점)
    
    return {
        "entry": True,
        "strategy": "gap_fill",
        "sub_type": gap_type,
        "entry_price": current_price,
        "target_price": target_price,
        "orb_high": orb_high,
        "orb_low": orb_low,
        "reason": f"Gap Fill 진입: ORB저점 반등 (현재가:{current_price}, 목표:{target_price})",
        "stop_loss_1": orb_low,                # 1차 손절: ORB 저점
        "stop_loss_2": current_price * 0.98,    # 2차 손절: -2% 절대
    }


# ============================================================
# STEP 6: 매도 관리 (Exit Management)
# ============================================================
def check_gap_exit(holding, current_price, candle, recent_candles):
    """
    갭상승전략 매도 조건 확인
    
    Parameters:
        holding: dict - 보유 종목 정보
            {"strategy": "gap_and_go"|"gap_fill", "entry_price": x, 
             "orb_low": x, "highest_price": x, "stop_loss_1": x, ...}
        current_price: float
        candle: dict - 현재 1분봉
        recent_candles: list[dict] - 최근 봉 목록
    
    Returns:
        dict: {"exit": True, "reason": "...", "exit_type": "profit"|"stop_loss"}
              {"exit": False, "reason": "..."}
    """
    strategy = holding.get("strategy", "gap_and_go")
    entry_price = holding.get("entry_price", 0)
    orb_low = holding.get("orb_low", 0)
    vwap_val = holding.get("vwap", 0)
    highest_price = holding.get("highest_price", entry_price)
    atr_val = holding.get("atr_val", 0)
    
    # ── 최고가 업데이트 ──
    if current_price > highest_price:
        highest_price = current_price
        holding["highest_price"] = highest_price
    
    # ================================================================
    # Gap and Go 매도
    # ================================================================
    if strategy == "gap_and_go":
        
        # 1차 손절: ORB_LOW 이탈
        if orb_low > 0 and current_price < orb_low:
            return {
                "exit": True,
                "reason": f"1차손절: ORB저점 이탈 (현재가:{current_price} < ORB저점:{orb_low})",
                "exit_type": "stop_loss",
                "exit_level": 1,
            }
        
        # 2차 손절: VWAP 이탈 + 2봉 연속 음봉
        if vwap_val > 0 and current_price < vwap_val:
            if len(recent_candles) >= 2:
                last_2 = recent_candles[-2:]
                both_bearish = all(c["close"] < c["open"] for c in last_2)
                if both_bearish:
                    return {
                        "exit": True,
                        "reason": f"2차손절: VWAP이탈+2봉연속음봉 (현재가:{current_price} < VWAP:{vwap_val})",
                        "exit_type": "stop_loss",
                        "exit_level": 2,
                    }
        
        # 3차 손절: -3% 절대 손절
        if entry_price > 0:
            loss_pct = (current_price - entry_price) / entry_price * 100
            if loss_pct <= -3.0:
                return {
                    "exit": True,
                    "reason": f"3차손절: 절대손절 -3% (손실률:{loss_pct:.2f}%)",
                    "exit_type": "stop_loss",
                    "exit_level": 3,
                }
        
        # 익절: 트레일링 스톱 (최고점 - ATR×2.0)
        if atr_val > 0 and highest_price > entry_price:
            trailing_stop = highest_price - (atr_val * 2.0)
            if current_price < trailing_stop:
                profit_pct = (current_price - entry_price) / entry_price * 100
                return {
                    "exit": True,
                    "reason": f"익절: 트레일링스톱 (최고점:{highest_price} - ATR×2.0={trailing_stop:.0f}, 수익:{profit_pct:.2f}%)",
                    "exit_type": "profit" if profit_pct > 0 else "stop_loss",
                    "exit_level": 0,
                }
    
    # ================================================================
    # Gap Fill 매도
    # ================================================================
    elif strategy == "gap_fill":
        
        target_price = holding.get("target_price", 0)
        
        # 익절: 전일 종가 도달 → 50% 매도 (partial_sold 플래그 관리)
        if target_price > 0 and current_price >= target_price:
            partial_sold = holding.get("partial_sold", False)
            if not partial_sold:
                holding["partial_sold"] = True
                return {
                    "exit": True,
                    "reason": f"익절: 목표가 도달 50% 매도 (전일종가:{target_price})",
                    "exit_type": "profit",
                    "exit_pct": 50,  # 50% 부분 매도
                }
            else:
                # 나머지 50%: 트레일링 스톱
                if atr_val > 0 and highest_price > entry_price:
                    trailing_stop = highest_price - (atr_val * 1.5)
                    if current_price < trailing_stop:
                        return {
                            "exit": True,
                            "reason": f"익절: 잔여 트레일링스톱 (최고점:{highest_price})",
                            "exit_type": "profit",
                            "exit_pct": 100,
                        }
        
        # 1차 손절: ORB_LOW 이탈
        if orb_low > 0 and current_price < orb_low:
            return {
                "exit": True,
                "reason": f"1차손절: ORB저점 이탈 (현재가:{current_price} < ORB저점:{orb_low})",
                "exit_type": "stop_loss",
                "exit_level": 1,
            }
        
        # 2차 손절: -2% 절대 손절
        if entry_price > 0:
            loss_pct = (current_price - entry_price) / entry_price * 100
            if loss_pct <= -2.0:
                return {
                    "exit": True,
                    "reason": f"2차손절: 절대손절 -2% (손실률:{loss_pct:.2f}%)",
                    "exit_type": "stop_loss",
                    "exit_level": 2,
                }
    
    return {"exit": False, "reason": "보유 유지"}


# ============================================================
# 야간 데이터 사전 계산 (Night Pre-calculation)
# ============================================================
async def precompute_gap_data():
    """
    전일 18:00 야간 스캔 시 갭상승전략용 데이터 사전 계산
    - 종목 선별 X, 데이터 준비만
    - 전종목 ATR(20), RSI(14), MACD, 지지/저항선 → DB 저장
    """
    from app.services.kis_stock import get_daily_candles
    from app.engine.scanner import scan_all_stocks
    
    print(f"[갭전략 야간준비] 전종목 데이터 사전 계산 시작")
    
    try:
        stocks = await scan_all_stocks()
        computed_count = 0
        batch = []
        
        for stock in stocks:
            code = stock.get("code", "")
            if not code:
                continue
            
            try:
                # 일봉 데이터 조회 (최근 30일)
                daily = await get_daily_candles(code, count=30)
                if not daily or len(daily) < 20:
                    continue
                
                closes = [d["close"] for d in daily]
                highs = [d["high"] for d in daily]
                lows = [d["low"] for d in daily]
                volumes = [d["volume"] for d in daily]
                
                # ATR(20)
                atr_vals = atr(highs, lows, closes, 20)
                atr_20 = atr_vals[-1] if atr_vals and atr_vals[-1] else 0
                
                # RSI(14)
                rsi_vals = rsi(closes, 14)
                rsi_14 = rsi_vals[-1] if rsi_vals and rsi_vals[-1] else None
                
                # MACD
                macd_data = macd(closes)
                macd_hist = macd_data["histogram"][-1] if macd_data["histogram"] else None
                
                # 20일 고점/저점 (지지/저항선 대용)
                high_20d = max(highs[-20:]) if len(highs) >= 20 else max(highs)
                low_20d = min(lows[-20:]) if len(lows) >= 20 else min(lows)
                
                # 5일 평균 거래량
                avg_vol_5d = sum(volumes[-5:]) / 5 if len(volumes) >= 5 else 0
                
                # 전일 종가
                prev_close = closes[-1]
                
                batch.append({
                    "stock_code": code,
                    "scan_date": date.today().isoformat(),
                    "atr_20": round(atr_20, 2),
                    "rsi_14": round(rsi_14, 2) if rsi_14 else None,
                    "macd_histogram": round(macd_hist, 4) if macd_hist else None,
                    "high_20d": high_20d,
                    "low_20d": low_20d,
                    "resistance": high_20d,
                    "support": low_20d,
                    "avg_volume_5d": round(avg_vol_5d),
                    "prev_close": prev_close,
                    "strategy_type": "gap",
                })
                
                computed_count += 1
                
                # 50개씩 배치 upsert
                if len(batch) >= 50:
                    _save_precomputed_batch(batch)
                    batch = []
                    
            except Exception as e:
                continue
        
        # 잔여 배치 저장
        if batch:
            _save_precomputed_batch(batch)
        
        print(f"[갭전략 야간준비] 완료: {computed_count}개 종목 데이터 저장")
        return computed_count
        
    except Exception as e:
        print(f"[갭전략 야간준비 오류] {e}")
        traceback.print_exc()
        return 0


def _save_precomputed_batch(batch):
    """사전계산 데이터 배치 저장"""
    try:
        for item in batch:
            db.table("gap_precomputed").upsert(
                item,
                on_conflict="stock_code,scan_date"
            ).execute()
    except Exception as e:
        print(f"[DB저장 오류] {e}")


async def load_precomputed_data(scan_date=None):
    """
    사전 계산된 데이터 로드
    
    Returns:
        dict: {code: {"atr_20": x, "rsi_14": x, ...}}
    """
    if scan_date is None:
        # 전날 데이터 로드 (야간 스캔은 전날 실행)
        scan_date = (date.today() - timedelta(days=1)).isoformat()
    
    try:
        result = db.table("gap_precomputed").select("*").eq(
            "scan_date", scan_date
        ).execute()
        
        data = {}
        for row in (result.data or []):
            code = row.get("stock_code", "")
            if code:
                data[code] = row
        
        print(f"[사전데이터] {len(data)}개 종목 로드 완료 (기준일: {scan_date})")
        return data
        
    except Exception as e:
        print(f"[사전데이터 로드 오류] {e}")
        return {}


# ============================================================
# 갭상승 감시종목 DB 저장
# ============================================================
async def save_gap_watchlist(filtered_stocks):
    """갭상승 감시종목을 DB에 저장"""
    today = date.today().isoformat()
    
    for stock in filtered_stocks:
        try:
            db.table("watchlist").upsert({
                "stock_code": stock["code"],
                "stock_name": stock["name"],
                "scan_date": today,
                "strategy_type": "gap",
                "gap_pct": stock.get("gap_pct", 0),
                "gap_type": stock.get("gap_type", ""),
                "score": stock.get("gap_pct", 0) * 10,  # 갭%를 점수로 환산
                "rvol": stock.get("rvol", 0),
                "rsi": stock.get("rsi_14"),
                "reason": stock.get("gap_type", "") + f" +{stock.get('gap_pct', 0)}%",
            }, on_conflict="stock_code,scan_date,strategy_type").execute()
        except Exception as e:
            print(f"[감시종목 저장 오류] {stock['code']}: {e}")
    
    print(f"[갭감시종목] {len(filtered_stocks)}개 저장 완료")
