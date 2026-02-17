"""눌림목 감지 엔진 (7가지 신호 복합 판단)"""
from app.utils.indicators import sma, rsi, atr, vwap
from app.engine.pattern_detector import detect_patterns

def detect_dip(candles_1m, candles_3m, candles_5m, orderbook=None):
    """눌림목 매수 신호 판단"""
    if len(candles_3m) < 20 or len(candles_5m) < 20:
        return {"is_dip": False, "signals": [], "score": 0, "reason": "데이터 부족"}

    signals = []
    score = 0

    closes_3m = [c["close"] for c in candles_3m]
    highs_3m = [c["high"] for c in candles_3m]
    lows_3m = [c["low"] for c in candles_3m]
    volumes_3m = [c["volume"] for c in candles_3m]

    closes_5m = [c["close"] for c in candles_5m]
    highs_5m = [c["high"] for c in candles_5m]
    lows_5m = [c["low"] for c in candles_5m]
    volumes_5m = [c["volume"] for c in candles_5m]

    # ATR 계산
    atr_vals = atr(highs_3m, lows_3m, closes_3m, 14)
    current_atr = atr_vals[-1] if atr_vals[-1] else 0

    # 현재가와 고점
    current_price = closes_3m[-1]
    recent_high = max(highs_3m[-10:])

    # ===== 7가지 신호 =====

    # 1. 고점 대비 ATR 범위 내 하락 (필수)
    if current_atr > 0:
        dip_pct = (recent_high - current_price) / recent_high * 100
        atr_pct = current_atr / recent_high * 100
        if 0.5 * atr_pct <= dip_pct <= 2.0 * atr_pct:
            signals.append("ATR범위내하락")
            score += 1

    # 2. 하락 중 거래량 감소
    if len(volumes_3m) >= 5:
        recent_vols = volumes_3m[-5:]
        if recent_vols[-1] < recent_vols[-3] and closes_3m[-1] < closes_3m[-3]:
            signals.append("거래량감소")
            score += 1

    # 3. 분봉 이동평균선 지지
    ma20 = sma(closes_5m, 20)
    if ma20[-1] and current_price >= ma20[-1] * 0.995:
        signals.append("MA지지")
        score += 1

    # 4. RSI 반등
    rsi_vals = rsi(closes_3m, 14)
    if rsi_vals[-1] and rsi_vals[-2]:
        if 30 <= rsi_vals[-1] <= 50 and rsi_vals[-1] > rsi_vals[-2]:
            signals.append("RSI반등")
            score += 1

    # 5. VWAP 지지
    vwap_vals = vwap(highs_3m, lows_3m, closes_3m, volumes_3m)
    if vwap_vals[-1] and current_price >= vwap_vals[-1] * 0.998:
        signals.append("VWAP지지")
        score += 1

    # 6. 봉차트 반등 패턴 (필수)
    patterns = detect_patterns(candles_3m)
    if patterns["bullish"]:
        signals.append(f"패턴:{','.join(patterns['bullish'])}")
        score += 1
    if patterns["bearish"]:
        return {"is_dip": False, "signals": signals, "score": 0,
                "reason": f"하락패턴감지:{','.join(patterns['bearish'])}"}

    # 7. 호가창 매수잔량 우세
    if orderbook and orderbook.get("bid_ratio", 0) > 1.2:
        signals.append("매수잔량우세")
        score += 1

    # ===== 매수 판단 =====
    has_required = "ATR범위내하락" in signals and any("패턴:" in s for s in signals)
    is_dip = has_required and score >= 4

    return {
        "is_dip": is_dip,
        "signals": signals,
        "score": score,
        "patterns": patterns,
        "atr": current_atr,
        "dip_pct": round((recent_high - current_price) / recent_high * 100, 2) if recent_high > 0 else 0,
        "reason": "매수신호" if is_dip else "조건미충족",
    }
