"""
진입 전략 엔진 — 조기 매수 시점 감지
Entry Strategy Engine — Early Buy Signal Detection
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/engine/entry_strategies.py

기존 DTW 매칭은 급등 직전 10일 패턴을 사용하여
매수 시점이 이미 상승 시작 후가 됨.

이 모듈은 3가지 진입 전략으로 2~4주 더 일찍 매수 시점을 감지:
  1) OBV 다이버전스 — 주가 횡보/하락 중 거래량 축적 감지
  2) VCP 볼린저 스퀴즈 — 변동성 수축 후 폭발 전 감지
  3) 부분 DTW 매칭 — 패턴 초기(앞 30~50%)만으로 조기 매칭

조합: 진입 전략(언제 사나) + 기존 매도 전략(언제 파나)
"""

import numpy as np
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field, asdict

from app.utils.indicators import z_normalize, sma, rsi, rsi_series

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 데이터 구조
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class EntrySignal:
    """진입 시그널 결과"""
    should_buy: bool
    entry_score: float           # 0~100 종합 점수
    signals: Dict                # 개별 전략 결과
    timing_estimate: str         # "급등 2~3주 전" 등
    detail: str                  # 요약 설명


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전략 1: OBV 다이버전스
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _compute_obv(candles: List[Dict]) -> List[float]:
    """OBV(On Balance Volume) 계산"""
    if not candles:
        return []

    obv = [0.0]
    for i in range(1, len(candles)):
        close_now = candles[i].get("close", 0)
        close_prev = candles[i - 1].get("close", 0)
        vol = candles[i].get("volume", 0)

        if close_now > close_prev:
            obv.append(obv[-1] + vol)
        elif close_now < close_prev:
            obv.append(obv[-1] - vol)
        else:
            obv.append(obv[-1])

    return obv


def _linear_regression_slope(values: List[float]) -> float:
    """최소자승법으로 기울기 계산 (정규화된 기울기)"""
    n = len(values)
    if n < 2:
        return 0.0

    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n

    numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))

    if denominator < 1e-10:
        return 0.0

    slope = numerator / denominator

    # 값 범위 대비 정규화 (기울기를 평균값 대비 비율로)
    if abs(y_mean) > 1e-10:
        return slope / abs(y_mean)
    return slope


def detect_obv_divergence(
    candles: List[Dict],
    lookback: int = 20,
    price_slope_threshold: float = 0.005,
    obv_slope_threshold: float = 0.005,
) -> Dict:
    """
    OBV 다이버전스 감지: 주가 횡보/하락 + OBV 상승
    세력의 사전 물량 축적 시그널

    Args:
        candles: 일봉 리스트 [{date, open, high, low, close, volume}, ...]
        lookback: 분석 기간 (거래일)
        price_slope_threshold: 주가 횡보 판정 기울기 임계값
        obv_slope_threshold: OBV 상승 판정 기울기 임계값

    Returns:
        {signal: bool, score: 0-100, price_slope: float, obv_slope: float, detail: str}
    """
    if len(candles) < lookback + 5:
        return {
            "signal": False, "score": 0, "price_slope": 0,
            "obv_slope": 0, "detail": f"데이터 부족 ({len(candles)}일 < {lookback + 5}일)"
        }

    recent = candles[-lookback:]
    closes = [c.get("close", 0) for c in recent]
    obv_full = _compute_obv(candles)
    obv_recent = obv_full[-lookback:]

    # 주가 기울기 (정규화)
    price_slope = _linear_regression_slope(closes)

    # OBV 기울기 (정규화)
    obv_slope = _linear_regression_slope(obv_recent)

    # 다이버전스 판정: 주가 횡보/하락(-threshold ~ +threshold) + OBV 상승
    price_flat_or_down = price_slope < price_slope_threshold
    obv_rising = obv_slope > obv_slope_threshold

    signal = price_flat_or_down and obv_rising

    # 스코어 계산 (0~100)
    # OBV 기울기가 클수록 + 주가가 하락할수록 다이버전스 강도 큼
    score = 0
    if signal:
        # OBV 상승 강도 (0~60)
        obv_strength = min(60, max(0, obv_slope / 0.05 * 60))

        # 주가 약세 강도 (0~40) — 더 약할수록 다이버전스 강함
        price_weakness = 0
        if price_slope < 0:
            price_weakness = min(40, max(0, abs(price_slope) / 0.03 * 40))
        elif price_slope < price_slope_threshold:
            price_weakness = 15  # 횡보는 기본 15점

        score = round(obv_strength + price_weakness)

    detail_parts = []
    if price_flat_or_down:
        if price_slope < 0:
            detail_parts.append(f"주가 하락세(기울기={price_slope:.4f})")
        else:
            detail_parts.append(f"주가 횡보(기울기={price_slope:.4f})")
    else:
        detail_parts.append(f"주가 상승세(기울기={price_slope:.4f})")

    if obv_rising:
        detail_parts.append(f"OBV 상승(기울기={obv_slope:.4f})")
    else:
        detail_parts.append(f"OBV 정체/하락(기울기={obv_slope:.4f})")

    return {
        "signal": signal,
        "score": min(100, score),
        "price_slope": round(price_slope, 6),
        "obv_slope": round(obv_slope, 6),
        "detail": " + ".join(detail_parts),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전략 2: VCP 볼린저 스퀴즈
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _bollinger_bandwidth(closes: List[float], period: int = 20) -> List[Optional[float]]:
    """
    볼린저 밴드 폭(%) 시계열 계산
    bandwidth = (upper - lower) / middle * 100
    """
    result = [None] * len(closes)

    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1: i + 1]
        middle = sum(window) / period
        if middle < 1e-10:
            continue

        std = (sum((x - middle) ** 2 for x in window) / period) ** 0.5
        upper = middle + 2 * std
        lower = middle - 2 * std

        result[i] = round((upper - lower) / middle * 100, 4)

    return result


def detect_vcp_squeeze(
    candles: List[Dict],
    lookback: int = 120,
    squeeze_threshold: float = 0.4,
    bb_period: int = 20,
) -> Dict:
    """
    VCP 볼린저 스퀴즈 감지: 변동성이 N개월 최저로 수축

    Args:
        candles: 일봉 리스트
        lookback: 볼린저 밴드폭 최저 판정 기간 (거래일, 기본 120 ≈ 6개월)
        squeeze_threshold: 현재 밴드폭이 lookback 최저값 대비 이 비율 이하면 스퀴즈
        bb_period: 볼린저 밴드 기간

    Returns:
        {signal: bool, score: 0-100, band_width: float, min_width: float, percentile: float, detail: str}
    """
    min_required = max(lookback, bb_period + 10)
    if len(candles) < min_required:
        return {
            "signal": False, "score": 0, "band_width": 0,
            "min_width": 0, "percentile": 100,
            "detail": f"데이터 부족 ({len(candles)}일 < {min_required}일)"
        }

    closes = [c.get("close", 0) for c in candles]
    bw = _bollinger_bandwidth(closes, bb_period)

    # 유효한 밴드폭만 추출
    valid_bw = [(i, v) for i, v in enumerate(bw) if v is not None]
    if len(valid_bw) < lookback // 2:
        return {
            "signal": False, "score": 0, "band_width": 0,
            "min_width": 0, "percentile": 100,
            "detail": "유효 밴드폭 데이터 부족"
        }

    # 최근 밴드폭
    current_bw = valid_bw[-1][1]

    # lookback 기간 내 밴드폭 통계
    recent_valid = [v for _, v in valid_bw[-lookback:]]
    min_bw = min(recent_valid)
    max_bw = max(recent_valid)

    # 백분위 계산 (낮을수록 수축)
    sorted_bw = sorted(recent_valid)
    rank = sum(1 for v in sorted_bw if v < current_bw)
    percentile = round(rank / len(sorted_bw) * 100, 1)

    # 스퀴즈 판정: 현재 밴드폭이 하위 squeeze_threshold * 100 % 이내
    signal = percentile <= squeeze_threshold * 100

    # 연속 수축일 계산
    consecutive_squeeze = 0
    threshold_value = sorted_bw[int(len(sorted_bw) * squeeze_threshold)] if len(sorted_bw) > 1 else current_bw
    for i in range(len(valid_bw) - 1, -1, -1):
        if valid_bw[i][1] <= threshold_value:
            consecutive_squeeze += 1
        else:
            break

    # 스코어 계산
    score = 0
    if signal:
        # 백분위가 낮을수록 높은 점수 (0~60)
        pct_score = max(0, (squeeze_threshold * 100 - percentile) / (squeeze_threshold * 100) * 60)

        # 연속 수축일 보너스 (0~40)
        consec_score = min(40, consecutive_squeeze * 8)

        score = round(pct_score + consec_score)

    detail_parts = [
        f"밴드폭={current_bw:.2f}%",
        f"백분위={percentile:.1f}%",
        f"최저={min_bw:.2f}%",
    ]
    if consecutive_squeeze > 0:
        detail_parts.append(f"연속수축={consecutive_squeeze}일")

    return {
        "signal": signal,
        "score": min(100, score),
        "band_width": round(current_bw, 4),
        "min_width": round(min_bw, 4),
        "percentile": percentile,
        "consecutive_squeeze": consecutive_squeeze,
        "detail": ", ".join(detail_parts),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전략 3: 부분 DTW 매칭
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def detect_partial_dtw(
    candles: List[Dict],
    clusters: List[Dict],
    match_pct: int = 40,
    min_similarity: float = 50.0,
    pre_days: int = 10,
) -> Dict:
    """
    부분 DTW 매칭: 패턴 초기 30~50%만으로 조기 매칭

    기존 DTW: 최근 10일 vs 클러스터 10일 → 100% 매칭 필요
    부분 DTW: 최근 3~5일 vs 클러스터 앞 3~5일 → 40% 매칭으로 조기 감지

    Args:
        candles: 일봉 리스트
        clusters: 클러스터 리스트 [{avg_return_flow, avg_volume_flow, avg_rsi_flow, ...}]
        match_pct: 매칭할 패턴 비율 (%) — 40이면 10일 중 앞 4일만 비교
        min_similarity: 최소 유사도 임계값 (%)
        pre_days: 기본 패턴 일수

    Returns:
        {signal: bool, score: 0-100, similarity: float, best_cluster: int, detail: str}
    """
    from app.engine.pattern_analyzer import dtw_similarity

    if not clusters:
        return {
            "signal": False, "score": 0, "similarity": 0,
            "best_cluster_id": -1, "detail": "클러스터 없음"
        }

    # 부분 매칭 일수 계산
    partial_days = max(3, int(pre_days * match_pct / 100))

    if len(candles) < partial_days + 20:  # RSI/MA 계산을 위한 여유 데이터
        return {
            "signal": False, "score": 0, "similarity": 0,
            "best_cluster_id": -1,
            "detail": f"데이터 부족 ({len(candles)}일 < {partial_days + 20}일)"
        }

    # 현재 종목의 최근 partial_days일 등락률
    recent_closes = [c.get("close", 0) for c in candles[-(partial_days + 1):]]
    current_returns = []
    for i in range(1, len(recent_closes)):
        prev = recent_closes[i - 1]
        if prev > 0:
            current_returns.append(round((recent_closes[i] - prev) / prev * 100, 4))
        else:
            current_returns.append(0.0)

    # 현재 거래량 비율 (20일 평균 대비)
    volumes = [c.get("volume", 0) for c in candles]
    current_vol_ratios = []
    for i in range(len(candles) - partial_days, len(candles)):
        avg_20 = sum(volumes[max(0, i - 20):i]) / min(20, max(1, i))
        if avg_20 > 0:
            current_vol_ratios.append(round(volumes[i] / avg_20, 4))
        else:
            current_vol_ratios.append(1.0)

    # 각 클러스터의 앞 partial_days일과 비교
    best_sim = 0.0
    best_cluster_id = -1
    match_details = []

    for cluster in clusters:
        cid = cluster.get("cluster_id", 0)
        avg_returns = cluster.get("avg_return_flow", [])
        avg_volume = cluster.get("avg_volume_flow", [])

        if not avg_returns or len(avg_returns) < partial_days:
            continue

        # 클러스터 패턴의 앞 partial_days일만 추출
        cluster_partial_returns = avg_returns[:partial_days]
        cluster_partial_volume = avg_volume[:partial_days] if avg_volume else []

        # 등락률 DTW 유사도 (가중치 0.6)
        sim_returns = dtw_similarity(current_returns, cluster_partial_returns, normalize=True)

        # 거래량 DTW 유사도 (가중치 0.4)
        sim_volume = 0.0
        if current_vol_ratios and cluster_partial_volume and len(cluster_partial_volume) >= partial_days:
            sim_volume = dtw_similarity(
                current_vol_ratios, cluster_partial_volume[:partial_days], normalize=True
            )

        # 가중 평균 유사도
        combined = sim_returns * 0.6 + sim_volume * 0.4

        match_details.append({
            "cluster_id": cid,
            "returns_sim": round(sim_returns, 2),
            "volume_sim": round(sim_volume, 2),
            "combined_sim": round(combined, 2),
        })

        if combined > best_sim:
            best_sim = combined
            best_cluster_id = cid

    signal = best_sim >= min_similarity

    # 스코어: 유사도 그대로 사용 (이미 0~100 범위)
    score = round(best_sim) if signal else round(best_sim * 0.5)

    detail = f"부분매칭({partial_days}일/{pre_days}일), 최고유사도={best_sim:.1f}%"
    if best_cluster_id >= 0:
        detail += f", 클러스터#{best_cluster_id}"

    return {
        "signal": signal,
        "score": min(100, score),
        "similarity": round(best_sim, 2),
        "best_cluster_id": best_cluster_id,
        "partial_days": partial_days,
        "match_details": match_details[:5],  # 상위 5개만
        "detail": detail,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 결합 진입 판정
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# 기본 가중치
DEFAULT_ENTRY_WEIGHTS = {
    "obv": 0.35,       # OBV 다이버전스 — 거래량 축적이 가장 중요
    "vcp": 0.30,       # VCP 볼린저 스퀴즈 — 변동성 수축
    "partial_dtw": 0.35,  # 부분 DTW — 패턴 초기 매칭
}

# 진입 점수 임계값
ENTRY_THRESHOLDS = {
    "aggressive": 30,   # 공격적: 1개 전략만 통과해도 매수
    "balanced": 45,     # 균형: 2개 이상 전략 통과 시 매수
    "conservative": 60, # 보수적: 3개 전략 모두 통과 시 매수
}


def evaluate_entry(
    candles: List[Dict],
    clusters: List[Dict],
    strategy_config: Optional[Dict] = None,
) -> Dict:
    """
    3개 전략을 결합하여 최종 진입 판정

    Args:
        candles: 일봉 데이터 (최소 120일 이상 권장)
        clusters: DTW 클러스터 리스트
        strategy_config: {
            "weights": {"obv": 0.35, "vcp": 0.30, "partial_dtw": 0.35},
            "threshold_mode": "balanced",  # aggressive / balanced / conservative
            "custom_threshold": None,      # 직접 지정 시 사용 (0~100)
            "obv_lookback": 20,
            "vcp_lookback": 120,
            "vcp_squeeze_threshold": 0.4,
            "partial_match_pct": 40,
            "partial_min_similarity": 50,
        }

    Returns:
        {
            should_buy: bool,
            entry_score: 0~100,
            signals: {obv: {...}, vcp: {...}, partial_dtw: {...}},
            active_signals: int,        # 통과한 전략 수
            timing_estimate: str,       # 예상 매수 타이밍
            threshold_used: float,
            detail: str,
        }
    """
    config = strategy_config or {}
    weights = config.get("weights", DEFAULT_ENTRY_WEIGHTS)
    threshold_mode = config.get("threshold_mode", "balanced")
    custom_threshold = config.get("custom_threshold")

    # 임계값 결정
    if custom_threshold is not None:
        threshold = custom_threshold
    else:
        threshold = ENTRY_THRESHOLDS.get(threshold_mode, 45)

    # ── 전략 1: OBV 다이버전스 ──
    obv_result = detect_obv_divergence(
        candles,
        lookback=config.get("obv_lookback", 20),
    )

    # ── 전략 2: VCP 볼린저 스퀴즈 ──
    vcp_result = detect_vcp_squeeze(
        candles,
        lookback=config.get("vcp_lookback", 120),
        squeeze_threshold=config.get("vcp_squeeze_threshold", 0.4),
    )

    # ── 전략 3: 부분 DTW 매칭 ──
    # ★ v5: skip_dtw 옵션 또는 clusters가 비어있으면 스킵 (불필요한 계산 제거)
    if config.get("skip_dtw") or not clusters:
        partial_dtw_result = {
            "signal": False, "score": 0, "similarity": 0,
            "best_cluster_id": -1, "detail": "DTW 스킵 (클러스터 없음)"
        }
    else:
        partial_dtw_result = detect_partial_dtw(
            candles,
            clusters,
            match_pct=config.get("partial_match_pct", 40),
            min_similarity=config.get("partial_min_similarity", 50),
        )

    # ── 가중 합산 ──
    w_obv = weights.get("obv", 0.35)
    w_vcp = weights.get("vcp", 0.30)
    w_dtw = weights.get("partial_dtw", 0.35)

    # 가중치 정규화
    total_w = w_obv + w_vcp + w_dtw
    if total_w > 0:
        w_obv /= total_w
        w_vcp /= total_w
        w_dtw /= total_w

    entry_score = round(
        obv_result["score"] * w_obv +
        vcp_result["score"] * w_vcp +
        partial_dtw_result["score"] * w_dtw,
        1
    )

    # 통과 전략 수
    active_signals = sum([
        obv_result["signal"],
        vcp_result["signal"],
        partial_dtw_result["signal"],
    ])

    should_buy = entry_score >= threshold

    # 타이밍 추정
    timing = _estimate_timing(obv_result, vcp_result, partial_dtw_result)

    # 상세 설명
    signal_names = []
    if obv_result["signal"]:
        signal_names.append("OBV축적")
    if vcp_result["signal"]:
        signal_names.append("밴드스퀴즈")
    if partial_dtw_result["signal"]:
        signal_names.append(f"DTW{partial_dtw_result.get('similarity', 0):.0f}%")

    if signal_names:
        detail = f"{' + '.join(signal_names)} → 점수 {entry_score:.0f}/{threshold}"
    else:
        detail = f"시그널 없음 (점수 {entry_score:.0f}/{threshold})"

    logger.info(
        f"[진입판정] score={entry_score:.1f}, threshold={threshold}, "
        f"buy={should_buy}, signals={active_signals}/3 "
        f"(OBV={obv_result['signal']}, VCP={vcp_result['signal']}, "
        f"DTW={partial_dtw_result['signal']})"
    )

    return {
        "should_buy": should_buy,
        "entry_score": entry_score,
        "signals": {
            "obv": obv_result,
            "vcp": vcp_result,
            "partial_dtw": partial_dtw_result,
        },
        "active_signals": active_signals,
        "timing_estimate": timing,
        "threshold_used": threshold,
        "threshold_mode": threshold_mode,
        "detail": detail,
    }


def _estimate_timing(obv: Dict, vcp: Dict, dtw: Dict) -> str:
    """통과한 전략 조합에 따라 예상 매수 타이밍 추정"""
    active = []
    if obv["signal"]:
        active.append("obv")
    if vcp["signal"]:
        active.append("vcp")
    if dtw["signal"]:
        active.append("dtw")

    if len(active) == 3:
        return "급등 2~4주 전 (강한 시그널)"
    elif len(active) == 2:
        if "obv" in active and "vcp" in active:
            return "급등 2~3주 전 (축적+수축)"
        elif "obv" in active and "dtw" in active:
            return "급등 1~3주 전 (축적+패턴초기)"
        elif "vcp" in active and "dtw" in active:
            return "급등 1~2주 전 (수축+패턴초기)"
    elif len(active) == 1:
        if "obv" in active:
            return "급등 3~4주 전 (축적 단독)"
        elif "vcp" in active:
            return "급등 2~3주 전 (수축 단독)"
        elif "dtw" in active:
            return "급등 1~2주 전 (패턴초기 단독)"

    return "시그널 없음"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 배치 평가 (전종목 스캔용)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def batch_evaluate_entry(
    stocks_candles: Dict[str, List[Dict]],
    clusters: List[Dict],
    strategy_config: Optional[Dict] = None,
) -> List[Dict]:
    """
    여러 종목을 한번에 진입 전략 평가

    Args:
        stocks_candles: {종목코드: 일봉리스트} 딕셔너리
        clusters: DTW 클러스터 리스트
        strategy_config: 전략 설정

    Returns:
        진입 시그널이 있는 종목 리스트 (점수 내림차순)
        [{code, should_buy, entry_score, active_signals, timing_estimate, signals, detail}]
    """
    results = []

    for code, candles in stocks_candles.items():
        try:
            result = evaluate_entry(candles, clusters, strategy_config)
            results.append({
                "code": code,
                **result,
            })
        except Exception as e:
            logger.warning(f"[진입평가 실패] {code}: {e}")
            continue

    # 점수 내림차순 정렬
    results.sort(key=lambda x: x["entry_score"], reverse=True)

    return results
