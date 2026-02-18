"""
스윙 종목 자동발굴 엔진 / Swing Stock Auto-Discovery Engine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
과거 1년간 크게 상승한 종목들의 "상승 직전" 공통 특징을 통계적으로 분석하여
현재 같은 조건을 충족하는 종목을 자동으로 발굴합니다.
"""

import requests
import numpy as np
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field, asdict
import json


@dataclass
class StockProfile:
    """종목 프로필"""
    code: str
    name: str
    market: str  # kospi / kosdaq
    prices: List[Dict] = field(default_factory=list)  # 일봉 데이터


@dataclass
class RisePattern:
    """상승 패턴 분석 결과"""
    code: str
    name: str
    rise_start_date: str
    rise_end_date: str
    rise_pct: float
    rise_days: int
    # 상승 직전 조건들
    pre_ma5_above_ma20: bool = False
    pre_ma20_above_ma60: bool = False
    pre_volume_surge: float = 0.0  # 20일 평균 대비 거래량 배수
    pre_rsi: float = 50.0
    pre_pullback_pct: float = 0.0  # 직전 눌림 %
    pre_pattern: str = ""  # 봉 패턴명
    pre_market_cap_range: str = ""  # 시가총액 구간
    pre_price_range: str = ""  # 가격대
    pre_consecutive_up_days: int = 0  # 연속 상승일
    pre_bb_position: float = 0.5  # 볼린저밴드 내 위치 (0~1)


@dataclass
class DiscoveryResult:
    """발굴 결과"""
    code: str
    name: str
    market: str
    current_price: int
    score: float  # 발굴 점수 (0~100)
    matched_conditions: List[str]  # 충족 조건 목록
    condition_details: Dict = field(default_factory=dict)
    signal_strength: str = ""  # 강/중/약


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 핵심 분석 함수들 / Core Analysis Functions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def calc_ma(prices: List[float], period: int) -> List[Optional[float]]:
    """이동평균 계산"""
    result = [None] * len(prices)
    for i in range(period - 1, len(prices)):
        result[i] = sum(prices[i - period + 1:i + 1]) / period
    return result


def calc_rsi(prices: List[float], period: int = 14) -> List[Optional[float]]:
    """RSI 계산"""
    result = [None] * len(prices)
    if len(prices) < period + 1:
        return result
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0:
        result[period] = 100
    else:
        rs = avg_gain / avg_loss
        result[period] = 100 - (100 / (1 + rs))
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            result[i + 1] = 100
        else:
            rs = avg_gain / avg_loss
            result[i + 1] = 100 - (100 / (1 + rs))
    return result


def calc_bollinger(prices: List[float], period: int = 20, std_mult: float = 2.0):
    """볼린저 밴드 계산 → (upper, middle, lower, position)"""
    n = len(prices)
    upper = [None] * n
    middle = [None] * n
    lower = [None] * n
    position = [None] * n
    for i in range(period - 1, n):
        window = prices[i - period + 1:i + 1]
        avg = sum(window) / period
        std = (sum((x - avg) ** 2 for x in window) / period) ** 0.5
        middle[i] = avg
        upper[i] = avg + std_mult * std
        lower[i] = avg - std_mult * std
        band_width = upper[i] - lower[i]
        if band_width > 0:
            position[i] = (prices[i] - lower[i]) / band_width
        else:
            position[i] = 0.5
    return upper, middle, lower, position


def calc_atr(highs, lows, closes, period=14):
    """ATR 계산"""
    n = len(closes)
    tr = [highs[0] - lows[0]]
    for i in range(1, n):
        tr.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1])
        ))
    atr = [None] * n
    if len(tr) >= period:
        atr[period - 1] = sum(tr[:period]) / period
        for i in range(period, n):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    return atr


def detect_candle_pattern(candles: List[Dict], idx: int) -> str:
    """일봉 패턴 감지 (idx 위치에서)"""
    if idx < 2 or idx >= len(candles):
        return ""
    c = candles[idx]
    p = candles[idx - 1]
    pp = candles[idx - 2]

    c_body = abs(c["close"] - c["open"])
    c_upper = c["high"] - max(c["close"], c["open"])
    c_lower = min(c["close"], c["open"]) - c["low"]
    c_is_bull = c["close"] > c["open"]

    p_body = abs(p["close"] - p["open"])
    p_is_bull = p["close"] > p["open"]

    # 망치형 (Hammer)
    if (c_lower > c_body * 2 and c_upper < c_body * 0.5
            and not p_is_bull):
        return "망치형"

    # 상승장악형 (Bullish Engulfing)
    if (c_is_bull and not p_is_bull
            and c["open"] <= p["close"] and c["close"] >= p["open"]
            and c_body > p_body):
        return "상승장악형"

    # 샛별형 (Morning Star)
    pp_body = abs(pp["close"] - pp["open"])
    pp_is_bull = pp["close"] > pp["open"]
    if (not pp_is_bull and pp_body > 0
            and p_body < pp_body * 0.3
            and c_is_bull and c_body > pp_body * 0.5):
        return "샛별형"

    # 상승잉태형 (Bullish Harami)
    if (not p_is_bull and c_is_bull
            and c["open"] > p["close"] and c["close"] < p["open"]
            and c_body < p_body * 0.5):
        return "상승잉태형"

    # 역망치형 (Inverted Hammer)
    if (c_upper > c_body * 2 and c_lower < c_body * 0.5
            and not p_is_bull):
        return "역망치형"

    return ""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1단계: 과거 상승 종목 분석 / Analyze Past Winners
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def find_big_rises(candles: List[Dict], threshold_pct: float = 30.0,
                   min_days: int = 3, max_days: int = 60) -> List[Dict]:
    """
    일봉 데이터에서 threshold_pct% 이상 상승한 구간을 찾는다.
    Returns: [{start_idx, end_idx, rise_pct, days}]
    """
    if len(candles) < min_days:
        return []

    closes = [c["close"] for c in candles]
    rises = []

    for i in range(len(closes) - min_days):
        low_price = closes[i]
        for j in range(i + min_days, min(i + max_days + 1, len(closes))):
            high_price = max(closes[i:j + 1])
            rise_pct = (high_price - low_price) / low_price * 100

            if rise_pct >= threshold_pct:
                peak_idx = i + closes[i:j + 1].index(high_price)
                rises.append({
                    "start_idx": i,
                    "end_idx": peak_idx,
                    "rise_pct": round(rise_pct, 2),
                    "days": peak_idx - i,
                    "start_date": candles[i].get("date", ""),
                    "end_date": candles[peak_idx].get("date", ""),
                })
                break  # 첫 번째 큰 상승만

    # 겹치는 구간 제거
    filtered = []
    for r in sorted(rises, key=lambda x: x["rise_pct"], reverse=True):
        overlap = False
        for f in filtered:
            if not (r["end_idx"] < f["start_idx"] or r["start_idx"] > f["end_idx"]):
                overlap = True
                break
        if not overlap:
            filtered.append(r)

    return filtered


def analyze_pre_rise_conditions(candles: List[Dict], start_idx: int) -> Dict:
    """
    상승 시작 직전(start_idx 기준 5~20일 전) 조건을 분석한다.
    """
    if start_idx < 60:
        return {}

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    # 이동평균
    ma5 = calc_ma(closes, 5)
    ma20 = calc_ma(closes, 20)
    ma60 = calc_ma(closes, 60)

    # RSI
    rsi = calc_rsi(closes)

    # 볼린저밴드
    _, _, _, bb_pos = calc_bollinger(closes)

    # ATR
    atr = calc_atr(highs, lows, closes)

    idx = start_idx
    conditions = {}

    # 1) MA 배열
    if ma5[idx] and ma20[idx]:
        conditions["ma5_above_ma20"] = ma5[idx] > ma20[idx]
    if ma20[idx] and ma60[idx]:
        conditions["ma20_above_ma60"] = ma20[idx] > ma60[idx]

    # 2) 거래량 서지 (최근 5일 평균 / 20일 평균)
    if idx >= 20:
        vol_5 = sum(volumes[idx - 4:idx + 1]) / 5
        vol_20 = sum(volumes[idx - 19:idx + 1]) / 20
        conditions["volume_surge"] = round(vol_5 / vol_20, 2) if vol_20 > 0 else 1.0

    # 3) RSI
    if rsi[idx] is not None:
        conditions["rsi"] = round(rsi[idx], 1)

    # 4) 직전 눌림 % (최근 고점 대비 하락폭)
    lookback = 20
    recent_high = max(closes[max(0, idx - lookback):idx + 1])
    conditions["pullback_pct"] = round((recent_high - closes[idx]) / recent_high * 100, 2)

    # 5) 봉 패턴
    conditions["candle_pattern"] = detect_candle_pattern(candles, idx)

    # 6) 볼린저밴드 위치
    if bb_pos[idx] is not None:
        conditions["bb_position"] = round(bb_pos[idx], 3)

    # 7) 연속 상승일
    consec = 0
    for k in range(idx, max(idx - 10, -1), -1):
        if closes[k] > closes[k - 1]:
            consec += 1
        else:
            break
    conditions["consecutive_up_days"] = consec

    # 8) 가격대
    price = closes[idx]
    if price < 5000:
        conditions["price_range"] = "5천원 미만"
    elif price < 20000:
        conditions["price_range"] = "5천~2만원"
    elif price < 50000:
        conditions["price_range"] = "2만~5만원"
    elif price < 100000:
        conditions["price_range"] = "5만~10만원"
    else:
        conditions["price_range"] = "10만원 이상"

    # 9) ATR 기반 변동성
    if atr[idx]:
        conditions["atr_pct"] = round(atr[idx] / closes[idx] * 100, 2)

    return conditions


def build_winner_profile(all_stocks_data: List[Dict],
                         rise_threshold: float = 30.0) -> Dict:
    """
    전체 종목 데이터에서 "우승 종목"들의 공통 프로필을 구축한다.

    Parameters:
        all_stocks_data: [{"code", "name", "candles": [...]}]
        rise_threshold: 상승률 기준 (%)

    Returns:
        {
            "total_winners": int,
            "condition_stats": {조건명: {값범위: 해당비율}},
            "top_conditions": [{"condition", "value_range", "match_pct"}],
        }
    """
    all_conditions = []

    for stock in all_stocks_data:
        candles = stock.get("candles", [])
        if len(candles) < 60:
            continue

        rises = find_big_rises(candles, rise_threshold)
        for rise in rises:
            conds = analyze_pre_rise_conditions(candles, rise["start_idx"])
            if conds:
                conds["code"] = stock["code"]
                conds["name"] = stock["name"]
                conds["rise_pct"] = rise["rise_pct"]
                all_conditions.append(conds)

    if not all_conditions:
        return {"total_winners": 0, "condition_stats": {}, "top_conditions": []}

    total = len(all_conditions)

    # 조건별 통계 집계
    stats = {}

    # MA 배열
    ma5_above = sum(1 for c in all_conditions if c.get("ma5_above_ma20", False))
    stats["MA5 > MA20"] = round(ma5_above / total * 100, 1)

    ma20_above = sum(1 for c in all_conditions if c.get("ma20_above_ma60", False))
    stats["MA20 > MA60"] = round(ma20_above / total * 100, 1)

    # 거래량 서지
    vol_surges = [c.get("volume_surge", 1) for c in all_conditions]
    vol_above_1_5 = sum(1 for v in vol_surges if v >= 1.5)
    stats["거래량 1.5배 이상"] = round(vol_above_1_5 / total * 100, 1)

    # RSI 구간
    rsi_vals = [c.get("rsi", 50) for c in all_conditions if c.get("rsi") is not None]
    if rsi_vals:
        rsi_40_60 = sum(1 for r in rsi_vals if 40 <= r <= 60)
        stats["RSI 40~60 구간"] = round(rsi_40_60 / len(rsi_vals) * 100, 1)

    # 눌림 %
    pullbacks = [c.get("pullback_pct", 0) for c in all_conditions]
    pb_3_8 = sum(1 for p in pullbacks if 3 <= p <= 8)
    stats["눌림 3~8%"] = round(pb_3_8 / total * 100, 1)

    # 볼린저 위치
    bb_vals = [c.get("bb_position", 0.5) for c in all_conditions
               if c.get("bb_position") is not None]
    if bb_vals:
        bb_lower = sum(1 for b in bb_vals if b < 0.3)
        stats["볼린저밴드 하단(0.3 미만)"] = round(bb_lower / len(bb_vals) * 100, 1)

    # 봉 패턴
    patterns = [c.get("candle_pattern", "") for c in all_conditions if c.get("candle_pattern")]
    if patterns:
        from collections import Counter
        pattern_counts = Counter(patterns)
        for p_name, count in pattern_counts.most_common(5):
            stats[f"패턴: {p_name}"] = round(count / total * 100, 1)

    # 상위 조건 정렬
    top_conditions = sorted(
        [{"condition": k, "match_pct": v} for k, v in stats.items()],
        key=lambda x: x["match_pct"],
        reverse=True
    )

    return {
        "total_winners": total,
        "total_stocks_analyzed": len(all_stocks_data),
        "rise_threshold_pct": rise_threshold,
        "condition_stats": stats,
        "top_conditions": top_conditions,
        "raw_conditions": all_conditions,  # 상세 데이터
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2단계: 현재 종목 스캔 / Scan Current Stocks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def score_stock_for_swing(candles: List[Dict],
                          winner_profile: Dict) -> Tuple[float, List[str], Dict]:
    """
    한 종목의 현재 상태를 위너 프로필과 비교하여 점수를 매긴다.

    Returns: (score, matched_conditions, details)
    """
    if len(candles) < 60:
        return 0, [], {}

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]
    idx = len(candles) - 1

    ma5 = calc_ma(closes, 5)
    ma20 = calc_ma(closes, 20)
    ma60 = calc_ma(closes, 60)
    rsi = calc_rsi(closes)
    _, _, _, bb_pos = calc_bollinger(closes)
    atr = calc_atr(highs, lows, closes)

    score = 0
    matched = []
    details = {}
    stats = winner_profile.get("condition_stats", {})

    # 1) MA5 > MA20 체크
    if ma5[idx] and ma20[idx] and ma5[idx] > ma20[idx]:
        weight = stats.get("MA5 > MA20", 50) / 100
        score += 15 * weight
        matched.append("MA5 > MA20 (단기 상승 추세)")
        details["ma5_above_ma20"] = True

    # 2) MA20 > MA60 체크
    if ma20[idx] and ma60[idx] and ma20[idx] > ma60[idx]:
        weight = stats.get("MA20 > MA60", 50) / 100
        score += 15 * weight
        matched.append("MA20 > MA60 (중기 상승 추세)")
        details["ma20_above_ma60"] = True

    # 3) 거래량 서지
    if idx >= 20:
        vol_5 = sum(volumes[idx - 4:idx + 1]) / 5
        vol_20 = sum(volumes[idx - 19:idx + 1]) / 20
        vol_ratio = vol_5 / vol_20 if vol_20 > 0 else 1.0
        details["volume_ratio"] = round(vol_ratio, 2)
        if vol_ratio >= 1.5:
            weight = stats.get("거래량 1.5배 이상", 50) / 100
            score += 20 * weight
            matched.append(f"거래량 급증 ({vol_ratio:.1f}배)")

    # 4) RSI 40~60 구간
    if rsi[idx] is not None:
        details["rsi"] = round(rsi[idx], 1)
        if 40 <= rsi[idx] <= 60:
            weight = stats.get("RSI 40~60 구간", 50) / 100
            score += 15 * weight
            matched.append(f"RSI 적정 구간 ({rsi[idx]:.0f})")

    # 5) 눌림 3~8%
    lookback = 20
    recent_high = max(closes[max(0, idx - lookback):idx + 1])
    pullback = (recent_high - closes[idx]) / recent_high * 100
    details["pullback_pct"] = round(pullback, 2)
    if 3 <= pullback <= 8:
        weight = stats.get("눌림 3~8%", 50) / 100
        score += 20 * weight
        matched.append(f"적정 눌림 ({pullback:.1f}%)")

    # 6) 볼린저밴드 하단
    if bb_pos[idx] is not None:
        details["bb_position"] = round(bb_pos[idx], 3)
        if bb_pos[idx] < 0.3:
            weight = stats.get("볼린저밴드 하단(0.3 미만)", 50) / 100
            score += 10 * weight
            matched.append(f"볼린저 하단 ({bb_pos[idx]:.2f})")

    # 7) 봉 패턴
    pattern = detect_candle_pattern(candles, idx)
    if pattern:
        details["candle_pattern"] = pattern
        score += 10
        matched.append(f"봉 패턴: {pattern}")

    # 8) ATR 변동성
    if atr[idx]:
        atr_pct = atr[idx] / closes[idx] * 100
        details["atr_pct"] = round(atr_pct, 2)

    # 신호 강도
    score = min(score, 100)
    if score >= 70:
        signal = "강"
    elif score >= 50:
        signal = "중"
    else:
        signal = "약"

    return round(score, 1), matched, {**details, "signal_strength": signal}


def discover_swing_candidates(all_stocks_data: List[Dict],
                              winner_profile: Dict,
                              top_n: int = 20) -> List[Dict]:
    """
    전체 종목을 스캔하여 스윙 후보 종목을 발굴한다.

    Returns: 점수 상위 top_n개 종목
    """
    candidates = []

    for stock in all_stocks_data:
        candles = stock.get("candles", [])
        if len(candles) < 60:
            continue

        score, matched, details = score_stock_for_swing(candles, winner_profile)

        if score >= 40:  # 최소 점수 기준
            candidates.append({
                "code": stock["code"],
                "name": stock["name"],
                "market": stock.get("market", ""),
                "current_price": candles[-1]["close"] if candles else 0,
                "score": score,
                "matched_conditions": matched,
                "details": details,
                "signal_strength": details.get("signal_strength", "약"),
            })

    # 점수 내림차순 정렬
    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:top_n]
