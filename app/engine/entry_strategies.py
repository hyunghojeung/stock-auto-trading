"""
entry_scorer.py — 매수 진입 품질 점수 산출기
Entry Quality Scorer for Pattern Analysis & Virtual Portfolio

배치 경로: app/engine/entry_scorer.py

[방법 C] 2단계(패턴분석) + 3단계(가상투자 진입) 모두에서 사용
- 2단계: DTW 유사도 + 진입품질점수 → 종합점수로 시그널 판정
- 3단계: 종합점수 기준 자동매수/감시/보류 분류

v1.0 — 2026-03-03
"""

import math
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 설정 / Configuration
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# 진입 품질 점수 배점 (총 100점)
ENTRY_SCORE_WEIGHTS = {
    "ma_arrangement":    15,   # MA 정배열 (MA5 > MA20)
    "ma5_slope":         10,   # MA5 기울기 (3일 연속 상승)
    "volume_increase":   15,   # 거래량 증가 (5일평균 > 20일평균 × 1.3)
    "bullish_volume":    10,   # 양봉 거래량 우위
    "price_position":    10,   # 가격 위치 (20일 저점 대비)
    "rsi_zone":          10,   # RSI 적정 구간 (40~60)
    "no_new_low":        10,   # 최근 5일 저점 미갱신
    "ma20_slope":        10,   # MA20 기울기 (상승 추세)
    "candle_strength":   10,   # 최근 캔들 강도 (양봉 비율)
}

# 종합점수 산출 비율 (DTW 유사도 vs 진입품질)
COMPOSITE_RATIO = {
    "dtw_similarity": 0.6,    # 기존 DTW 유사도 비중
    "entry_quality":  0.4,    # 진입 품질 점수 비중
}

# 가상투자 진입 기준 (3단계)
ENTRY_THRESHOLDS = {
    "auto_buy":   75,   # 자동 매수 (종합 75점 이상)
    "watch":      60,   # 감시 등록 (종합 60~74점)
    "hold":        0,   # 보류 (60점 미만)
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 데이터 클래스 / Data Classes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class EntryScoreResult:
    """진입 품질 점수 결과 / Entry Quality Score Result"""
    total_score: float = 0.0            # 진입 품질 총점 (0~100)
    composite_score: float = 0.0        # 종합점수 (DTW + 진입품질)
    entry_grade: str = "hold"           # auto_buy / watch / hold
    entry_label: str = "⬜ 보류"        # 표시 라벨

    # 세부 항목별 점수
    details: Dict[str, float] = field(default_factory=dict)

    # 세부 항목별 판정 사유
    reasons: List[str] = field(default_factory=list)

    # 원본 DTW 유사도 (참조용)
    dtw_similarity: float = 0.0


@dataclass
class CandleData:
    """캔들 데이터 (dict → 객체 변환용)"""
    date: str = ""
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 유틸 함수 / Utility Functions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _to_candle(c) -> CandleData:
    """dict 또는 객체를 CandleData로 변환"""
    if isinstance(c, dict):
        return CandleData(
            date=c.get("date", ""),
            open=float(c.get("open", 0)),
            high=float(c.get("high", 0)),
            low=float(c.get("low", 0)),
            close=float(c.get("close", 0)),
            volume=float(c.get("volume", 0)),
        )
    # 이미 객체인 경우 (CandleDay 등)
    return CandleData(
        date=getattr(c, "date", ""),
        open=float(getattr(c, "open", 0)),
        high=float(getattr(c, "high", 0)),
        low=float(getattr(c, "low", 0)),
        close=float(getattr(c, "close", 0)),
        volume=float(getattr(c, "volume", 0)),
    )


def _calc_ma(candles: List[CandleData], period: int) -> List[Optional[float]]:
    """이동평균 계산 / Calculate Moving Average"""
    result = []
    for i in range(len(candles)):
        if i < period - 1:
            result.append(None)
        else:
            avg = sum(c.close for c in candles[i - period + 1: i + 1]) / period
            result.append(avg)
    return result


def _calc_rsi(candles: List[CandleData], period: int = 14) -> List[Optional[float]]:
    """RSI 계산 / Calculate RSI"""
    if len(candles) < period + 1:
        return [None] * len(candles)

    result = [None] * period
    gains = []
    losses = []

    for i in range(1, len(candles)):
        change = candles[i].close - candles[i - 1].close
        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    if avg_loss == 0:
        result.append(100.0)
    else:
        rs = avg_gain / avg_loss
        result.append(100 - (100 / (1 + rs)))

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            result.append(100.0)
        else:
            rs = avg_gain / avg_loss
            result.append(100 - (100 / (1 + rs)))

    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 핵심: 진입 품질 점수 산출 / Entry Quality Scoring
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def calculate_entry_score(
    candles,
    dtw_similarity: float = 0.0,
    lookback: int = 60,
) -> EntryScoreResult:
    """
    매수 진입 품질 점수 산출 (9개 항목, 총 100점)

    Parameters:
        candles: 최근 캔들 데이터 (최소 60개 권장, dict 또는 객체)
        dtw_similarity: 기존 DTW 유사도 점수 (0~100)
        lookback: 분석에 사용할 최근 캔들 수

    Returns:
        EntryScoreResult: 진입 품질 점수 + 종합점수 + 등급
    """
    result = EntryScoreResult(dtw_similarity=dtw_similarity)

    # 캔들 변환 및 최소 데이터 확인
    clist = [_to_candle(c) for c in candles]
    if len(clist) < 25:
        result.reasons.append("❌ 캔들 데이터 부족 (최소 25개 필요)")
        result.entry_label = "⬜ 데이터부족"
        return result

    # 최근 lookback개만 사용
    recent = clist[-lookback:] if len(clist) > lookback else clist

    # MA 계산
    ma5 = _calc_ma(recent, 5)
    ma20 = _calc_ma(recent, 20)

    # RSI 계산
    rsi_list = _calc_rsi(recent, 14)

    details = {}
    reasons = []

    # ─── 1. MA 정배열 (15점) ───
    score_ma_arr = 0
    if ma5[-1] is not None and ma20[-1] is not None:
        if ma5[-1] > ma20[-1]:
            score_ma_arr = ENTRY_SCORE_WEIGHTS["ma_arrangement"]
            reasons.append("✅ MA정배열: MA5({:,.0f}) > MA20({:,.0f})".format(ma5[-1], ma20[-1]))
        else:
            gap_pct = ((ma5[-1] - ma20[-1]) / ma20[-1]) * 100
            if gap_pct > -1.0:
                # 거의 수렴 중 → 부분 점수
                score_ma_arr = ENTRY_SCORE_WEIGHTS["ma_arrangement"] * 0.5
                reasons.append("🟡 MA수렴 중: 이격 {:.1f}% (부분점수)".format(gap_pct))
            else:
                reasons.append("❌ MA역배열: MA5 < MA20 (이격 {:.1f}%)".format(gap_pct))
    details["ma_arrangement"] = round(score_ma_arr, 1)

    # ─── 2. MA5 기울기 (10점) ───
    score_ma5_slope = 0
    valid_ma5 = [v for v in ma5[-5:] if v is not None]
    if len(valid_ma5) >= 3:
        rising_count = sum(1 for i in range(1, len(valid_ma5)) if valid_ma5[i] > valid_ma5[i - 1])
        if rising_count >= len(valid_ma5) - 1:
            # 3일+ 연속 상승
            score_ma5_slope = ENTRY_SCORE_WEIGHTS["ma5_slope"]
            reasons.append("✅ MA5 상승세: {}일 연속 상승".format(rising_count))
        elif rising_count >= 2:
            score_ma5_slope = ENTRY_SCORE_WEIGHTS["ma5_slope"] * 0.6
            reasons.append("🟡 MA5 약상승: {}일/{}일 상승".format(rising_count, len(valid_ma5) - 1))
        else:
            reasons.append("❌ MA5 하락세: 상승일 {}일뿐".format(rising_count))
    details["ma5_slope"] = round(score_ma5_slope, 1)

    # ─── 3. 거래량 증가 (15점) ───
    score_vol = 0
    if len(recent) >= 20:
        vol_5d = sum(c.volume for c in recent[-5:]) / 5
        vol_20d = sum(c.volume for c in recent[-20:]) / 20

        if vol_20d > 0:
            vol_ratio = vol_5d / vol_20d
            if vol_ratio >= 1.5:
                score_vol = ENTRY_SCORE_WEIGHTS["volume_increase"]
                reasons.append("✅ 거래량 급증: 5일평균 = 20일평균 × {:.1f}배".format(vol_ratio))
            elif vol_ratio >= 1.3:
                score_vol = ENTRY_SCORE_WEIGHTS["volume_increase"] * 0.7
                reasons.append("🟡 거래량 증가: 5일평균 = 20일평균 × {:.1f}배".format(vol_ratio))
            elif vol_ratio >= 1.0:
                score_vol = ENTRY_SCORE_WEIGHTS["volume_increase"] * 0.3
                reasons.append("🟡 거래량 보합: 5일/20일 비율 {:.1f}".format(vol_ratio))
            else:
                reasons.append("❌ 거래량 감소: 5일/20일 비율 {:.1f}".format(vol_ratio))
    details["volume_increase"] = round(score_vol, 1)

    # ─── 4. 양봉 거래량 우위 (10점) ───
    score_bull_vol = 0
    recent_10 = recent[-10:] if len(recent) >= 10 else recent[-5:]
    bull_vol = sum(c.volume for c in recent_10 if c.close >= c.open)
    bear_vol = sum(c.volume for c in recent_10 if c.close < c.open)
    total_vol = bull_vol + bear_vol

    if total_vol > 0:
        bull_ratio = bull_vol / total_vol
        if bull_ratio >= 0.6:
            score_bull_vol = ENTRY_SCORE_WEIGHTS["bullish_volume"]
            reasons.append("✅ 양봉거래량 우위: {:.0f}%".format(bull_ratio * 100))
        elif bull_ratio >= 0.5:
            score_bull_vol = ENTRY_SCORE_WEIGHTS["bullish_volume"] * 0.5
            reasons.append("🟡 양봉/음봉 균형: {:.0f}%".format(bull_ratio * 100))
        else:
            reasons.append("❌ 음봉거래량 우위: 양봉비율 {:.0f}%".format(bull_ratio * 100))
    details["bullish_volume"] = round(score_bull_vol, 1)

    # ─── 5. 가격 위치 (10점) ───
    score_price_pos = 0
    if len(recent) >= 20:
        high_20d = max(c.high for c in recent[-20:])
        low_20d = min(c.low for c in recent[-20:])
        price_range = high_20d - low_20d

        if price_range > 0:
            current_price = recent[-1].close
            position = (current_price - low_20d) / price_range  # 0=저점, 1=고점

            if 0.15 <= position <= 0.55:
                # 바닥 탈출 구간 (저점에서 15~55% 위치) → 최적
                score_price_pos = ENTRY_SCORE_WEIGHTS["price_position"]
                reasons.append("✅ 가격위치 최적: 20일 범위 {:.0f}% (바닥 탈출 구간)".format(position * 100))
            elif 0.55 < position <= 0.75:
                # 중간 구간
                score_price_pos = ENTRY_SCORE_WEIGHTS["price_position"] * 0.5
                reasons.append("🟡 가격위치 중간: 20일 범위 {:.0f}%".format(position * 100))
            elif position > 0.75:
                # 고점 추격 구간
                reasons.append("❌ 고점 추격 위험: 20일 범위 {:.0f}% (고점 근처)".format(position * 100))
            else:
                # 너무 바닥 (하락 지속 가능)
                score_price_pos = ENTRY_SCORE_WEIGHTS["price_position"] * 0.3
                reasons.append("🟡 바닥 구간: 20일 범위 {:.0f}% (반등 미확인)".format(position * 100))
    details["price_position"] = round(score_price_pos, 1)

    # ─── 6. RSI 적정 구간 (10점) ───
    score_rsi = 0
    current_rsi = None
    for v in reversed(rsi_list):
        if v is not None:
            current_rsi = v
            break

    if current_rsi is not None:
        if 40 <= current_rsi <= 60:
            # 상승 초기 구간 → 최적
            score_rsi = ENTRY_SCORE_WEIGHTS["rsi_zone"]
            reasons.append("✅ RSI 적정: {:.1f} (상승 초기 구간)".format(current_rsi))
        elif 30 <= current_rsi < 40:
            # 과매도 근접 → 반등 가능
            score_rsi = ENTRY_SCORE_WEIGHTS["rsi_zone"] * 0.7
            reasons.append("🟡 RSI 과매도 근접: {:.1f} (반등 기대)".format(current_rsi))
        elif 60 < current_rsi <= 70:
            # 약간 과열
            score_rsi = ENTRY_SCORE_WEIGHTS["rsi_zone"] * 0.4
            reasons.append("🟡 RSI 약과열: {:.1f}".format(current_rsi))
        elif current_rsi > 70:
            # 과매수 → 진입 위험
            reasons.append("❌ RSI 과매수: {:.1f} (진입 위험)".format(current_rsi))
        else:
            # RSI < 30 과매도
            score_rsi = ENTRY_SCORE_WEIGHTS["rsi_zone"] * 0.5
            reasons.append("🟡 RSI 과매도: {:.1f} (급반등 or 추가하락)".format(current_rsi))
    details["rsi_zone"] = round(score_rsi, 1)

    # ─── 7. 최근 5일 저점 미갱신 (10점) ───
    score_no_low = 0
    if len(recent) >= 10:
        low_10d = min(c.low for c in recent[-10:-5]) if len(recent) >= 10 else min(c.low for c in recent[:-5])
        low_5d = min(c.low for c in recent[-5:])

        if low_5d >= low_10d:
            score_no_low = ENTRY_SCORE_WEIGHTS["no_new_low"]
            reasons.append("✅ 저점 미갱신: 최근5일 저점({:,.0f}) ≥ 이전 저점({:,.0f})".format(low_5d, low_10d))
        else:
            drop_pct = ((low_5d - low_10d) / low_10d) * 100
            if drop_pct > -2.0:
                score_no_low = ENTRY_SCORE_WEIGHTS["no_new_low"] * 0.4
                reasons.append("🟡 소폭 신저가: {:.1f}% (경미)".format(drop_pct))
            else:
                reasons.append("❌ 신저가 갱신: {:.1f}% 하락".format(drop_pct))
    details["no_new_low"] = round(score_no_low, 1)

    # ─── 8. MA20 기울기 (10점) ───
    score_ma20_slope = 0
    valid_ma20 = [v for v in ma20[-5:] if v is not None]
    if len(valid_ma20) >= 3:
        ma20_rising = sum(1 for i in range(1, len(valid_ma20)) if valid_ma20[i] > valid_ma20[i - 1])
        if ma20_rising >= len(valid_ma20) - 1:
            score_ma20_slope = ENTRY_SCORE_WEIGHTS["ma20_slope"]
            reasons.append("✅ MA20 상승: 중기 추세 양호")
        elif ma20_rising >= 2:
            score_ma20_slope = ENTRY_SCORE_WEIGHTS["ma20_slope"] * 0.5
            reasons.append("🟡 MA20 횡보: 추세 전환 가능")
        else:
            reasons.append("❌ MA20 하락: 중기 하락 추세")
    details["ma20_slope"] = round(score_ma20_slope, 1)

    # ─── 9. 최근 캔들 강도 (10점) ───
    score_candle = 0
    recent_5 = recent[-5:]
    bull_count = sum(1 for c in recent_5 if c.close >= c.open)

    if bull_count >= 4:
        score_candle = ENTRY_SCORE_WEIGHTS["candle_strength"]
        reasons.append("✅ 캔들 강세: 최근 5일 중 양봉 {}개".format(bull_count))
    elif bull_count >= 3:
        score_candle = ENTRY_SCORE_WEIGHTS["candle_strength"] * 0.6
        reasons.append("🟡 캔들 보통: 최근 5일 중 양봉 {}개".format(bull_count))
    else:
        reasons.append("❌ 캔들 약세: 최근 5일 중 양봉 {}개뿐".format(bull_count))
    details["candle_strength"] = round(score_candle, 1)

    # ━━━ 총점 계산 ━━━
    total_entry = sum(details.values())
    result.total_score = round(total_entry, 1)
    result.details = details
    result.reasons = reasons

    # ━━━ 종합점수 계산 (DTW × 0.6 + 진입품질 × 0.4) ━━━
    composite = (
        dtw_similarity * COMPOSITE_RATIO["dtw_similarity"]
        + total_entry * COMPOSITE_RATIO["entry_quality"]
    )
    result.composite_score = round(composite, 1)

    # ━━━ 등급 판정 ━━━
    if composite >= ENTRY_THRESHOLDS["auto_buy"]:
        result.entry_grade = "auto_buy"
        result.entry_label = "🟢 자동매수"
    elif composite >= ENTRY_THRESHOLDS["watch"]:
        result.entry_grade = "watch"
        result.entry_label = "🟡 감시"
    else:
        result.entry_grade = "hold"
        result.entry_label = "⬜ 보류"

    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 배치 처리 / Batch Processing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def score_recommendations(
    recommendations: List[Dict],
    candles_by_code: Dict,
) -> List[Dict]:
    """
    매수 추천 목록에 진입 품질 점수를 추가
    pattern_analyzer.py의 find_current_matches() 결과에 적용

    Parameters:
        recommendations: find_current_matches() 반환값
        candles_by_code: {종목코드: [캔들데이터]} 딕셔너리

    Returns:
        recommendations에 entry_score 관련 필드가 추가된 리스트
    """
    for rec in recommendations:
        code = rec.get("code", "")
        candles = candles_by_code.get(code, [])
        dtw_sim = rec.get("similarity", 0)

        if candles:
            score_result = calculate_entry_score(
                candles=candles,
                dtw_similarity=dtw_sim,
            )

            # 추천 항목에 진입 품질 정보 추가
            rec["entry_score"] = score_result.total_score
            rec["composite_score"] = score_result.composite_score
            rec["entry_grade"] = score_result.entry_grade
            rec["entry_label"] = score_result.entry_label
            rec["entry_details"] = score_result.details
            rec["entry_reasons"] = score_result.reasons

            # ★ 종합점수 기반으로 시그널 재판정 (기존 DTW만 → DTW + 진입품질)
            composite = score_result.composite_score
            if composite >= 75:
                rec["signal"] = "🟢 강력 매수"
                rec["signal_code"] = "strong_buy"
            elif composite >= 60:
                rec["signal"] = "🟡 관심"
                rec["signal_code"] = "watch"
            elif composite >= 45:
                rec["signal"] = "⚠️ 대기"
                rec["signal_code"] = "wait"
            else:
                rec["signal"] = "⬜ 미해당"
                rec["signal_code"] = "none"

            # 기존 similarity 필드를 종합점수로 업데이트 (UI 호환)
            rec["similarity_original"] = dtw_sim          # 원본 DTW 보존
            rec["similarity"] = score_result.composite_score  # 종합점수로 교체

        else:
            rec["entry_score"] = 0
            rec["composite_score"] = 0
            rec["entry_grade"] = "hold"
            rec["entry_label"] = "⬜ 데이터없음"
            rec["entry_details"] = {}
            rec["entry_reasons"] = ["❌ 캔들 데이터 없음"]

    # 종합점수 높은 순 재정렬
    recommendations.sort(key=lambda r: r.get("composite_score", 0), reverse=True)

    return recommendations


def filter_for_virtual_invest(
    recommendations: List[Dict],
    mode: str = "auto",
    min_score: Optional[float] = None,
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    가상투자 진입 필터링 (3단계 적용)

    Parameters:
        recommendations: score_recommendations() 결과
        mode: "auto" (자동분류) / "all" (전체 등록) / "strict" (75점↑만)
        min_score: 커스텀 최소 점수 (mode="auto"일 때 무시)

    Returns:
        (auto_buy_list, watch_list, hold_list) 3개 리스트 튜플
    """
    auto_buy = []
    watch = []
    hold = []

    for rec in recommendations:
        grade = rec.get("entry_grade", "hold")
        composite = rec.get("composite_score", 0)

        if mode == "all":
            # 전체 등록 모드 (기존 방식 호환)
            auto_buy.append(rec)
        elif mode == "strict":
            # 엄격 모드 (75점 이상만)
            if composite >= 75:
                auto_buy.append(rec)
            else:
                hold.append(rec)
        elif min_score is not None:
            # 커스텀 점수 기준
            if composite >= min_score:
                auto_buy.append(rec)
            elif composite >= min_score - 15:
                watch.append(rec)
            else:
                hold.append(rec)
        else:
            # 자동 분류 (기본)
            if grade == "auto_buy":
                auto_buy.append(rec)
            elif grade == "watch":
                watch.append(rec)
            else:
                hold.append(rec)

    return auto_buy, watch, hold


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API 응답용 요약 / Summary for API Response
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def summarize_entry_scores(recommendations: List[Dict]) -> Dict:
    """
    진입 품질 점수 전체 요약 (API 응답에 포함)
    """
    if not recommendations:
        return {"total": 0, "auto_buy": 0, "watch": 0, "hold": 0, "avg_score": 0}

    auto_buy = sum(1 for r in recommendations if r.get("entry_grade") == "auto_buy")
    watch = sum(1 for r in recommendations if r.get("entry_grade") == "watch")
    hold = sum(1 for r in recommendations if r.get("entry_grade") == "hold")
    avg_score = sum(r.get("composite_score", 0) for r in recommendations) / len(recommendations)

    return {
        "total": len(recommendations),
        "auto_buy": auto_buy,
        "watch": watch,
        "hold": hold,
        "avg_composite_score": round(avg_score, 1),
        "filter_thresholds": ENTRY_THRESHOLDS,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 하위 호환 별칭 (surge_scanner_routes.py 등에서 import)
# Backward-compatible aliases
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
evaluate_entry = calculate_entry_score
