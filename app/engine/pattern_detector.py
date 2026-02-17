"""봉차트 패턴 감지 엔진"""

def detect_patterns(candles):
    """봉차트 패턴 감지 (반등 + 하락)"""
    if len(candles) < 3:
        return {"bullish": [], "bearish": [], "score": 0}

    bullish = []
    bearish = []
    score = 0

    # 최근 3봉 기준
    c = candles[-1]  # 현재봉
    p = candles[-2]  # 이전봉
    pp = candles[-3] if len(candles) >= 3 else None

    body = abs(c["close"] - c["open"])
    upper_wick = c["high"] - max(c["close"], c["open"])
    lower_wick = min(c["close"], c["open"]) - c["low"]
    is_bullish_candle = c["close"] > c["open"]

    p_body = abs(p["close"] - p["open"])

    # 1. 망치형 (Hammer) +20점
    if lower_wick > body * 2 and upper_wick < body * 0.5 and p["close"] < p["open"]:
        bullish.append("망치형")
        score += 20

    # 2. 상승 장악형 (Bullish Engulfing) +25점
    if (is_bullish_candle and not (p["close"] > p["open"]) and
        c["open"] <= p["close"] and c["close"] >= p["open"] and body > p_body):
        bullish.append("상승장악형")
        score += 25

    # 3. 샛별형 (Morning Star) +30점
    if pp and len(candles) >= 3:
        pp_body = abs(pp["close"] - pp["open"])
        if (pp["close"] < pp["open"] and  # 첫봉 음봉
            p_body < pp_body * 0.3 and     # 둘째봉 작은 몸통
            is_bullish_candle and body > pp_body * 0.5):  # 셋째봉 큰 양봉
            bullish.append("샛별형")
            score += 30

    # 4. 상승 잉태형 (Bullish Harami) +15점
    if (not (p["close"] > p["open"]) and is_bullish_candle and
        c["open"] >= p["close"] and c["close"] <= p["open"] and body < p_body):
        bullish.append("상승잉태형")
        score += 15

    # 5. 역망치형 (Inverted Hammer) +10점
    if upper_wick > body * 2 and lower_wick < body * 0.5 and p["close"] < p["open"]:
        bullish.append("역망치형")
        score += 10

    # === 하락 패턴 ===

    # 6. 하락 장악형 (Bearish Engulfing) → 매수 차단
    if (not is_bullish_candle and p["close"] > p["open"] and
        c["open"] >= p["close"] and c["close"] <= p["open"] and body > p_body):
        bearish.append("하락장악형")
        score -= 100  # 매수 차단

    # 7. 저녁별형 (Evening Star) → 매수 차단
    if pp and len(candles) >= 3:
        pp_body = abs(pp["close"] - pp["open"])
        if (pp["close"] > pp["open"] and
            p_body < pp_body * 0.3 and
            not is_bullish_candle and body > pp_body * 0.5):
            bearish.append("저녁별형")
            score -= 100

    # 8. 교수형 (Hanging Man) -20점
    if (lower_wick > body * 2 and upper_wick < body * 0.5 and
        p["close"] > p["open"] and is_bullish_candle):
        bearish.append("교수형")
        score -= 20

    return {"bullish": bullish, "bearish": bearish, "score": score}
