"""눌림목 패턴 라이브러리 (3개 패턴 + 2개 공통 게이트)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
★ 핵심 신규 모듈 — 세력 매집 패턴을 자동 감지합니다.

공통 게이트:
  Gate 1: 거래량 절벽 (전일 대비 <20% 또는 5일 평균 <30%)
  Gate 2: 변동성 스퀴즈 (일중 변동률 20일 평균의 50% 이하, 2일 연속)

3개 패턴:
  P001: 기준봉 중심가 지지 (대량 장대양봉 후 50% 위 유지 + 거래량 감소)
  P002: 이평선 수렴 돌파 (급등 후 완만 하락 → MA20 터치 + 망치형)
  P003: 세이크아웃/언더슈팅 (지지선 이탈 후 양봉 회복)

파일경로: app/engine/pattern_library.py
"""

import logging
from typing import Dict, List, Tuple, Optional
from app.utils.indicators import sma, volume_ratio

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 공통 상수 / Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GATE1_PREV_DAY_RATIO = 0.20     # 전일 대비 거래량 20% 이하
GATE1_AVG5_RATIO = 0.30         # 5일 평균 대비 30% 이하
GATE2_SQUEEZE_RATIO = 0.50      # 변동률이 20일 평균의 50% 이하
GATE2_MIN_DAYS = 2              # 연속 스퀴즈 일수


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Gate 1: 거래량 절벽 / Volume Cliff Detection
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_volume_cliff(candles: List[Dict]) -> Tuple[bool, str]:
    """
    거래량 절벽 감지: 전일 대비 <20% 또는 최근 5일 평균 대비 <30%
    Gate 1: Volume Cliff — prev day <20% OR 5d avg <30%

    Args:
        candles: 일봉 리스트 [{close, open, high, low, volume, date}, ...]

    Returns:
        (passed: bool, detail: str)
    """
    if len(candles) < 6:
        return False, "데이터 부족 (6일 미만)"

    today_vol = candles[-1].get("volume", 0)
    prev_vol = candles[-2].get("volume", 0)

    if today_vol <= 0:
        return False, "당일 거래량 0"

    # 조건 A: 전일 대비 20% 이하
    cond_a = False
    prev_ratio = 0
    if prev_vol > 0:
        prev_ratio = today_vol / prev_vol
        cond_a = prev_ratio <= GATE1_PREV_DAY_RATIO

    # 조건 B: 5일 평균 대비 30% 이하
    cond_b = False
    avg5_ratio = 0
    recent_5 = [c.get("volume", 0) for c in candles[-6:-1]]
    avg_5 = sum(recent_5) / len(recent_5) if recent_5 else 0
    if avg_5 > 0:
        avg5_ratio = today_vol / avg_5
        cond_b = avg5_ratio <= GATE1_AVG5_RATIO

    passed = cond_a or cond_b
    detail = (f"전일비 {prev_ratio:.1%}, 5일평균비 {avg5_ratio:.1%}"
              f" → {'통과' if passed else '미충족'}")

    return passed, detail


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Gate 2: 변동성 스퀴즈 / Volatility Squeeze Detection
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_volatility_squeeze(candles: List[Dict]) -> Tuple[bool, str]:
    """
    변동성 스퀴즈 감지: (고가-저가)/종가 ≤ 20일 평균의 50%, 연속 2일 이상
    Gate 2: Volatility Squeeze — daily range ≤ 50% of 20d avg, ≥2 consecutive days

    Returns:
        (passed: bool, detail: str)
    """
    if len(candles) < 22:
        return False, "데이터 부족 (22일 미만)"

    # 일중 변동률 계산
    ranges = []
    for c in candles[-22:]:
        close = c.get("close", 0)
        if close > 0:
            daily_range = (c.get("high", 0) - c.get("low", 0)) / close
        else:
            daily_range = 0
        ranges.append(daily_range)

    # 20일 평균 변동률 (오늘·어제 제외)
    avg_range_20 = sum(ranges[:20]) / 20 if len(ranges) >= 22 else sum(ranges[:-2]) / max(len(ranges) - 2, 1)

    if avg_range_20 <= 0:
        return False, "평균 변동률 0"

    threshold = avg_range_20 * GATE2_SQUEEZE_RATIO

    # 최근 연속 스퀴즈 일수 카운트
    consecutive = 0
    for r in reversed(ranges[-5:]):  # 최근 5일 체크
        if r <= threshold:
            consecutive += 1
        else:
            break

    passed = consecutive >= GATE2_MIN_DAYS
    detail = (f"변동률 20일평균 {avg_range_20:.3%}, 임계 {threshold:.3%}, "
              f"연속 {consecutive}일 스퀴즈"
              f" → {'통과' if passed else '미충족'}")

    return passed, detail


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# P001: 기준봉 중심가 지지
# Base Candle Mid-Price Support
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_pattern_P001(candles: List[Dict]) -> Tuple[bool, int, str]:
    """
    기준봉 중심가 지지:
    1. 최근 20일 내 대량 장대양봉 찾기 (거래량 > 20일평균×2, 등락률 > 5%)
    2. 기준봉 이후 3~5일간 종가가 기준봉 (open+close)/2 위에 유지
    3. 해당 기간 거래량이 기준봉 대비 80% 이상 감소

    Returns:
        (matched: bool, score: int 0~100, detail: str)
    """
    if len(candles) < 25:
        return False, 0, "데이터 부족"

    recent_20 = candles[-20:]

    # 20일 평균 거래량
    volumes = [c.get("volume", 0) for c in candles[-25:-5]]
    avg_vol_20 = sum(volumes) / len(volumes) if volumes else 0

    # 기준봉 탐색 (최근 15~5일 전 범위)
    base_candle = None
    base_idx = -1

    for i in range(len(recent_20) - 5, max(len(recent_20) - 16, -1), -1):
        if i < 0:
            break
        c = recent_20[i]
        o, cl, vol = c.get("open", 0), c.get("close", 0), c.get("volume", 0)

        if o <= 0 or cl <= 0:
            continue

        change_pct = (cl - o) / o * 100
        is_big_yang = change_pct >= 5.0 and vol >= avg_vol_20 * 2

        if is_big_yang:
            base_candle = c
            base_idx = i
            break

    if not base_candle:
        return False, 0, "기준봉(대량 장대양봉) 미발견"

    # 기준봉 중심가
    mid_price = (base_candle["open"] + base_candle["close"]) / 2
    base_vol = base_candle["volume"]

    # 기준봉 이후 캔들
    after = recent_20[base_idx + 1:]
    if len(after) < 3:
        return False, 0, f"기준봉 이후 데이터 부족 ({len(after)}일)"

    # 조건 2: 종가가 중심가 위에 유지
    check_days = min(len(after), 5)
    above_count = sum(1 for c in after[:check_days] if c.get("close", 0) >= mid_price)
    above_ratio = above_count / check_days

    # 조건 3: 거래량 80% 감소
    vol_decrease_count = 0
    if base_vol > 0:
        for c in after[:check_days]:
            if c.get("volume", 0) <= base_vol * 0.20:
                vol_decrease_count += 1

    vol_decrease_ratio = vol_decrease_count / check_days if check_days > 0 else 0

    # 점수 계산
    score = 0
    if above_ratio >= 0.8:
        score += 50
    elif above_ratio >= 0.6:
        score += 30

    if vol_decrease_ratio >= 0.6:
        score += 30
    elif vol_decrease_ratio >= 0.4:
        score += 15

    # 기준봉 강도 보너스
    base_change = (base_candle["close"] - base_candle["open"]) / base_candle["open"] * 100
    if base_change >= 10:
        score += 20
    elif base_change >= 7:
        score += 10

    matched = score >= 60
    detail = (f"기준봉 {base_candle.get('date','')} (+{base_change:.1f}%), "
              f"중심가지지 {above_count}/{check_days}일, "
              f"거래량감소 {vol_decrease_count}/{check_days}일, 점수={score}")

    return matched, min(score, 100), detail


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# P002: 이평선 수렴 돌파
# Moving Average Convergence Breakout
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_pattern_P002(candles: List[Dict]) -> Tuple[bool, int, str]:
    """
    이평선 수렴 돌파:
    1. 최근 급등(10일 내 15%+) 후 완만 하락 구간 존재
    2. 종가가 MA20에 터치 (MA20 ± 1% 범위)
    3. 해당일 캔들이 망치형 (아래꼬리 ≥ 몸통×2)
    4. 거래량이 전일 대비 미증가 (20~50% 이하 변화)

    Returns:
        (matched: bool, score: int 0~100, detail: str)
    """
    if len(candles) < 25:
        return False, 0, "데이터 부족"

    closes = [c.get("close", 0) for c in candles]
    ma20_vals = sma(closes, 20)

    today = candles[-1]
    today_close = today.get("close", 0)
    today_open = today.get("open", 0)
    today_high = today.get("high", 0)
    today_low = today.get("low", 0)
    today_vol = today.get("volume", 0)
    prev_vol = candles[-2].get("volume", 0)

    ma20 = ma20_vals[-1]
    if not ma20 or ma20 <= 0 or today_close <= 0:
        return False, 0, "MA20 계산 불가"

    score = 0
    details = []

    # 조건 1: 최근 10일 내 급등 후 하락 구간
    has_surge = False
    for i in range(max(len(candles) - 15, 0), len(candles) - 3):
        window_start = max(i - 10, 0)
        window_low = min(c.get("close", 0) for c in candles[window_start:i+1] if c.get("close", 0) > 0)
        window_high = candles[i].get("close", 0)
        if window_low > 0 and (window_high - window_low) / window_low * 100 >= 15:
            has_surge = True
            break

    if has_surge:
        score += 20
        details.append("급등이력O")
    else:
        details.append("급등이력X")

    # 조건 2: MA20 터치 (±1%)
    ma20_diff = abs(today_close - ma20) / ma20 * 100
    if ma20_diff <= 1.0:
        score += 30
        details.append(f"MA20터치({ma20_diff:.1f}%)")
    elif ma20_diff <= 2.0:
        score += 15
        details.append(f"MA20근접({ma20_diff:.1f}%)")

    # 조건 3: 망치형 캔들 (아래꼬리 ≥ 몸통×2)
    body = abs(today_close - today_open)
    lower_shadow = min(today_open, today_close) - today_low
    is_hammer = lower_shadow >= body * 2 and body > 0

    if is_hammer:
        score += 30
        details.append("망치형O")
    else:
        # 긴 아래꼬리라도 가산
        if body > 0 and lower_shadow >= body * 1.2:
            score += 10
            details.append("아래꼬리")

    # 조건 4: 거래량 미증가
    if prev_vol > 0:
        vol_change = today_vol / prev_vol
        if 0.2 <= vol_change <= 0.5:
            score += 20
            details.append(f"거래량감소({vol_change:.0%})")
        elif 0.5 < vol_change <= 1.0:
            score += 10
            details.append(f"거래량유지({vol_change:.0%})")
        else:
            details.append(f"거래량증가({vol_change:.0%})")

    matched = score >= 60
    detail = f"P002: {', '.join(details)}, 점수={score}"
    return matched, min(score, 100), detail


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# P003: 세이크아웃 / 언더슈팅
# Shakeout / Undershoot
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_pattern_P003(candles: List[Dict]) -> Tuple[bool, int, str]:
    """
    세이크아웃/언더슈팅:
    1. 최근 20일 지지선 계산 (최저 종가 기준)
    2. 당일 저가가 지지선 대비 1~3% 이탈 (하방 돌파)
    3. 당일 종가가 지지선 위로 회복 (양봉)
    4. 또는 직전일이 이탈 + 당일 양봉으로 회복

    Returns:
        (matched: bool, score: int 0~100, detail: str)
    """
    if len(candles) < 22:
        return False, 0, "데이터 부족"

    # 지지선: 최근 20일 중 최저 종가 (오늘·어제 제외)
    support_candles = candles[-22:-2]
    support_closes = [c.get("close", 0) for c in support_candles if c.get("close", 0) > 0]
    if not support_closes:
        return False, 0, "지지선 계산 불가"

    support = min(support_closes)

    today = candles[-1]
    yesterday = candles[-2]

    today_close = today.get("close", 0)
    today_low = today.get("low", 0)
    today_open = today.get("open", 0)
    yest_close = yesterday.get("close", 0)
    yest_low = yesterday.get("low", 0)

    if support <= 0 or today_close <= 0:
        return False, 0, "가격 데이터 이상"

    score = 0
    details = []

    # 시나리오 A: 당일 지지선 이탈 + 회복 (V자 반전)
    today_break_pct = (support - today_low) / support * 100 if today_low < support else 0
    today_recovered = today_close >= support

    # 시나리오 B: 어제 이탈 + 오늘 양봉 회복
    yest_break_pct = (support - yest_low) / support * 100 if yest_low < support else 0
    yest_broke = yest_break_pct >= 1.0
    today_bullish = today_close > today_open and today_close >= support

    if 1.0 <= today_break_pct <= 5.0 and today_recovered:
        # 시나리오 A: 당일 V자 반전
        score += 50
        details.append(f"당일V자(이탈{today_break_pct:.1f}%→회복)")
        if today_close > today_open:
            score += 15
            details.append("양봉마감")
    elif yest_broke and today_bullish:
        # 시나리오 B: 전일이탈 + 오늘 회복
        score += 45
        details.append(f"전일이탈({yest_break_pct:.1f}%)+오늘회복")
    else:
        detail_str = f"지지선={support:,.0f}, 당일저가={today_low:,.0f}, 이탈{today_break_pct:.1f}%"
        return False, 0, f"P003: 이탈/회복 없음 ({detail_str})"

    # 거래량 증가 (반등 확인)
    prev_5_vol = sum(c.get("volume", 0) for c in candles[-6:-1]) / 5
    today_vol = today.get("volume", 0)
    if prev_5_vol > 0 and today_vol > prev_5_vol * 1.5:
        score += 20
        details.append("반등거래량O")
    elif prev_5_vol > 0 and today_vol > prev_5_vol:
        score += 10
        details.append("거래량소폭증")

    # 이탈 깊이 보너스 (1~3% 적정)
    break_pct = max(today_break_pct, yest_break_pct)
    if 1.0 <= break_pct <= 3.0:
        score += 15
        details.append(f"적정이탈({break_pct:.1f}%)")

    matched = score >= 55
    detail = f"P003: 지지선 {support:,.0f}, {', '.join(details)}, 점수={score}"
    return matched, min(score, 100), detail


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 통합 평가 함수 / Unified Evaluation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# 패턴 함수 매핑
PATTERN_FUNCTIONS = {
    "P001": check_pattern_P001,
    "P002": check_pattern_P002,
    "P003": check_pattern_P003,
}

PATTERN_NAMES = {
    "P001": "기준봉 중심가 지지",
    "P002": "이평선 수렴 돌파",
    "P003": "세이크아웃/언더슈팅",
}


def evaluate_dip_patterns(
    candles: List[Dict],
    active_patterns: List[str] = None,
    require_gates: bool = True,
) -> Dict:
    """
    ★ 종목 일봉에 눌림목 패턴 라이브러리를 적용하여 종합 평가
    Evaluate dip patterns on a stock's daily candles.

    Args:
        candles: 일봉 리스트 (최소 25일, date 기준 오름차순)
        active_patterns: 활성화된 패턴 코드 ["P001","P002","P003"]
        require_gates: True면 Gate 1 + Gate 2 필수

    Returns:
        {
            "is_dip": True/False,
            "gates": {
                "volume_cliff": {"passed": True, "detail": "..."},
                "volatility_squeeze": {"passed": True, "detail": "..."}
            },
            "gates_all_passed": True/False,
            "matched_patterns": [
                {"code": "P001", "name": "...", "score": 85, "detail": "..."}
            ],
            "total_score": 85,
            "best_pattern": "P001" or None,
        }
    """
    if active_patterns is None:
        active_patterns = ["P001", "P002", "P003"]

    result = {
        "is_dip": False,
        "gates": {
            "volume_cliff": {"passed": False, "detail": ""},
            "volatility_squeeze": {"passed": False, "detail": ""},
        },
        "gates_all_passed": False,
        "matched_patterns": [],
        "total_score": 0,
        "best_pattern": None,
    }

    if not candles or len(candles) < 22:
        return result

    # ── 공통 게이트 체크 ──
    gate1_passed, gate1_detail = check_volume_cliff(candles)
    gate2_passed, gate2_detail = check_volatility_squeeze(candles)

    result["gates"]["volume_cliff"] = {"passed": gate1_passed, "detail": gate1_detail}
    result["gates"]["volatility_squeeze"] = {"passed": gate2_passed, "detail": gate2_detail}
    result["gates_all_passed"] = gate1_passed and gate2_passed

    # 게이트 필수 모드: 둘 다 통과해야 패턴 체크 진행
    if require_gates and not result["gates_all_passed"]:
        return result

    # ── 개별 패턴 체크 ──
    matched_patterns = []
    for pcode in active_patterns:
        func = PATTERN_FUNCTIONS.get(pcode)
        if not func:
            continue

        try:
            matched, score, detail = func(candles)
            if matched:
                matched_patterns.append({
                    "code": pcode,
                    "name": PATTERN_NAMES.get(pcode, pcode),
                    "score": score,
                    "detail": detail,
                })
        except Exception as e:
            logger.warning(f"패턴 {pcode} 평가 오류: {e}")

    result["matched_patterns"] = matched_patterns

    if matched_patterns:
        best = max(matched_patterns, key=lambda p: p["score"])
        result["total_score"] = best["score"]
        result["best_pattern"] = best["code"]
        result["is_dip"] = True

    return result


def get_active_patterns_from_db() -> List[str]:
    """DB에서 활성화된 패턴 코드 목록 조회 / Get active pattern codes from DB"""
    try:
        from app.core.database import db
        resp = db.table("pattern_definitions") \
            .select("pattern_code") \
            .eq("is_active", True) \
            .execute()
        if resp.data:
            return [row["pattern_code"] for row in resp.data]
    except Exception as e:
        logger.warning(f"[패턴라이브러리] DB 조회 실패 (기본값 사용): {e}")

    return ["P001", "P002", "P003"]
