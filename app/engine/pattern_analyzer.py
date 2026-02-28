"""
급상승 패턴 탐지기 — DTW 기반 분석 엔진
Pattern Surge Detector — DTW-based Analysis Engine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/engine/pattern_analyzer.py

여러 종목의 일봉 데이터에서 급상승(5일 내 +30%) 구간을 자동 탐지하고,
상승 직전 N일의 일봉 패턴을 DTW(Dynamic Time Warping)로 비교하여
공통 패턴을 도출합니다.

[v2] 프리셋(우량주/작전주) 지원 — DTW 가중치를 외부에서 전달받음
[v3] 눌림목 패턴 라이브러리 연동 — Step 2.5 삽입 + 매수추천에 패턴 가산점
"""

import numpy as np
import math
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 기본 가중치 / Default Weights
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DEFAULT_WEIGHTS = {'returns': 0.5, 'candle': 0.2, 'volume': 0.3}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 데이터 클래스 / Data Classes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class CandleDay:
    """일봉 하나"""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass
class SurgeZone:
    """급상승 구간"""
    code: str
    name: str
    start_idx: int          # 급상승 시작 인덱스
    end_idx: int            # 급상승 종료 인덱스
    start_date: str
    end_date: str
    start_price: float
    peak_price: float
    rise_pct: float         # 상승률 %
    rise_days: int          # 상승 소요 거래일


@dataclass
class PreRisePattern:
    """급상승 직전 패턴"""
    code: str
    name: str
    surge: dict             # SurgeZone 정보
    # 3차원 패턴 벡터
    returns: List[float]    # 등락률 흐름 (%)
    candle_shapes: List[Dict]  # 봉 모양 (양/음, 꼬리비율, 몸통크기)
    volume_ratios: List[float]  # 거래량 변화 (20일평균 대비)
    # 원본 일봉
    candles: List[Dict]
    pre_days: int           # 분석 구간 (일)


@dataclass
class PatternCluster:
    """공통 패턴 클러스터"""
    cluster_id: int
    pattern_count: int          # 소속 패턴 수
    avg_similarity: float       # 평균 유사도
    avg_return_flow: List[float]  # 평균 등락률 흐름
    avg_volume_flow: List[float]  # 평균 거래량 흐름
    avg_rise_pct: float         # 평균 상승폭
    avg_rise_days: float        # 평균 상승 소요일
    win_rate: float             # 이 패턴 후 실제 상승 확률
    members: List[Dict]         # 소속 패턴 목록
    description: str            # 패턴 설명 (자동 생성)


@dataclass
class AnalysisResult:
    """전체 분석 결과"""
    total_stocks: int
    total_surges: int
    total_patterns: int
    clusters: List[Dict]
    all_patterns: List[Dict]
    recommendations: List[Dict]  # 현재 매수 추천
    summary: Dict               # 공통 패턴 요약
    raw_surges: List[Dict]      # 급상승 구간 목록
    dip_matches: Dict = field(default_factory=dict)  # ★ v3: 눌림목 패턴 매칭 결과


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DTW 구현 (Pure NumPy — 외부 의존성 없음)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def dtw_distance(s1: List[float], s2: List[float], window: int = None) -> float:
    """
    DTW (Dynamic Time Warping) 거리 계산
    - s1, s2: 비교할 두 시계열
    - window: Sakoe-Chiba 밴드 폭 (None이면 전체)
    - return: DTW 거리 (작을수록 유사)
    """
    n, m = len(s1), len(s2)
    if n == 0 or m == 0:
        return float('inf')

    if window is None:
        window = max(n, m)

    # 비용 행렬 초기화
    dtw_matrix = np.full((n + 1, m + 1), np.inf)
    dtw_matrix[0, 0] = 0.0

    for i in range(1, n + 1):
        j_start = max(1, i - window)
        j_end = min(m, i + window)
        for j in range(j_start, j_end + 1):
            cost = abs(s1[i - 1] - s2[j - 1])
            dtw_matrix[i, j] = cost + min(
                dtw_matrix[i - 1, j],      # 삽입
                dtw_matrix[i, j - 1],      # 삭제
                dtw_matrix[i - 1, j - 1]   # 일치
            )

    return dtw_matrix[n, m]


def dtw_similarity(s1: List[float], s2: List[float], window: int = None) -> float:
    """
    DTW 유사도 (0~100%, 높을수록 유사)
    """
    dist = dtw_distance(s1, s2, window)
    # 정규화: 길이로 나누어 스케일 통일
    norm = max(len(s1), len(s2))
    if norm == 0:
        return 0.0
    normalized = dist / norm
    # 유사도로 변환 (지수 감쇠)
    similarity = math.exp(-normalized) * 100
    return round(similarity, 2)


def multi_dim_dtw_similarity(
    p1: PreRisePattern,
    p2: PreRisePattern,
    weights: Dict[str, float] = None
) -> float:
    """
    다차원 DTW 유사도 — 등락률 + 봉모양 + 거래량 종합
    [v2] weights를 외부에서 전달받아 사용
    weights: {'returns': 0.5, 'candle': 0.2, 'volume': 0.3}
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS

    # 1) 등락률 DTW
    sim_returns = dtw_similarity(p1.returns, p2.returns)

    # 2) 봉 모양 DTW — 몸통 크기 비율로 비교
    body1 = [c.get('body_ratio', 0) for c in p1.candle_shapes]
    body2 = [c.get('body_ratio', 0) for c in p2.candle_shapes]
    sim_candle = dtw_similarity(body1, body2)

    # 3) 거래량 DTW
    sim_volume = dtw_similarity(p1.volume_ratios, p2.volume_ratios)

    # 가중 합산
    total = (
        weights.get('returns', 0.5) * sim_returns +
        weights.get('candle', 0.2) * sim_candle +
        weights.get('volume', 0.3) * sim_volume
    )
    return round(total, 2)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 급상승 구간 탐지 / Surge Detection
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def detect_surges(
    candles: List[CandleDay],
    code: str,
    name: str,
    rise_pct: float = 30.0,
    rise_days: int = 5
) -> List[SurgeZone]:
    """
    급상승 구간 탐지
    - rise_pct: 최소 상승률 (기본 30%)
    - rise_days: 상승 기간 (기본 5거래일)
    """
    surges = []
    n = len(candles)

    if n < rise_days + 1:
        return surges

    i = 0
    while i < n - rise_days:
        base_price = candles[i].close

        # 향후 rise_days 내 최고 종가 찾기
        max_price = base_price
        max_idx = i
        for j in range(i + 1, min(i + rise_days + 1, n)):
            if candles[j].close > max_price:
                max_price = candles[j].close
                max_idx = j

        pct = ((max_price - base_price) / base_price) * 100

        if pct >= rise_pct:
            surge = SurgeZone(
                code=code,
                name=name,
                start_idx=i,
                end_idx=max_idx,
                start_date=candles[i].date,
                end_date=candles[max_idx].date,
                start_price=base_price,
                peak_price=max_price,
                rise_pct=round(pct, 2),
                rise_days=max_idx - i
            )
            surges.append(surge)
            # 급상승 구간 이후로 건너뛰기 (중복 방지)
            i = max_idx + 1
        else:
            i += 1

    return surges


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 패턴 벡터 추출 / Pattern Vector Extraction
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def extract_pre_rise_pattern(
    candles: List[CandleDay],
    surge: SurgeZone,
    pre_days: int = 10
) -> Optional[PreRisePattern]:
    """
    급상승 직전 N일의 패턴 벡터 추출
    """
    start_idx = surge.start_idx
    pattern_start = start_idx - pre_days

    if pattern_start < 1:  # 최소 1일 전 데이터 필요 (등락률 계산)
        return None

    # 분석 구간 일봉 추출
    pattern_candles = candles[pattern_start:start_idx]
    if len(pattern_candles) < 3:  # 최소 3일은 있어야 의미
        return None

    # === 1) 등락률 흐름 ===
    returns = []
    for k in range(len(pattern_candles)):
        if k == 0:
            # 첫날은 전일 대비
            prev_close = candles[pattern_start - 1].close if pattern_start > 0 else pattern_candles[0].open
            if prev_close > 0:
                ret = ((pattern_candles[0].close - prev_close) / prev_close) * 100
            else:
                ret = 0.0
        else:
            prev_close = pattern_candles[k - 1].close
            if prev_close > 0:
                ret = ((pattern_candles[k].close - prev_close) / prev_close) * 100
            else:
                ret = 0.0
        returns.append(round(ret, 4))

    # === 2) 봉 모양 벡터 ===
    candle_shapes = []
    for c in pattern_candles:
        total_range = c.high - c.low if c.high > c.low else 0.001
        body = abs(c.close - c.open)
        upper_shadow = c.high - max(c.open, c.close)
        lower_shadow = min(c.open, c.close) - c.low

        shape = {
            'is_bullish': 1 if c.close >= c.open else -1,
            'body_ratio': round(body / total_range, 4) if total_range > 0 else 0,
            'upper_shadow_ratio': round(upper_shadow / total_range, 4) if total_range > 0 else 0,
            'lower_shadow_ratio': round(lower_shadow / total_range, 4) if total_range > 0 else 0,
        }
        candle_shapes.append(shape)

    # === 3) 거래량 변화 ===
    volume_ratios = []
    # 20일 이동평균 거래량 계산
    for k in range(len(pattern_candles)):
        abs_idx = pattern_start + k
        # 직전 20일 평균 거래량
        vol_start = max(0, abs_idx - 20)
        vol_slice = candles[vol_start:abs_idx]
        if vol_slice:
            avg_vol = sum(c.volume for c in vol_slice) / len(vol_slice)
        else:
            avg_vol = 1
        current_vol = pattern_candles[k].volume
        ratio = round(current_vol / avg_vol, 4) if avg_vol > 0 else 1.0
        volume_ratios.append(ratio)

    # 원본 일봉 dict 변환
    raw_candles = []
    for c in pattern_candles:
        raw_candles.append({
            'date': c.date,
            'open': c.open,
            'high': c.high,
            'low': c.low,
            'close': c.close,
            'volume': c.volume
        })

    return PreRisePattern(
        code=surge.code,
        name=surge.name,
        surge=asdict(surge),
        returns=returns,
        candle_shapes=candle_shapes,
        volume_ratios=volume_ratios,
        candles=raw_candles,
        pre_days=pre_days
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 패턴 클러스터링 / Pattern Clustering
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def cluster_patterns(
    patterns: List[PreRisePattern],
    similarity_threshold: float = 40.0,
    weights: Dict[str, float] = None
) -> List[PatternCluster]:
    """
    DTW 유사도 기반 단순 클러스터링 (Agglomerative 방식)
    similarity_threshold: 같은 클러스터로 묶을 최소 유사도 (%)
    [v2] weights: DTW 가중치 (프리셋에서 전달)
    """
    if not patterns:
        return []

    if weights is None:
        weights = DEFAULT_WEIGHTS

    n = len(patterns)

    # 유사도 행렬 계산 — [v2] weights 전달
    sim_matrix = np.zeros((n, n))
    for i in range(n):
        for j in range(i + 1, n):
            sim = multi_dim_dtw_similarity(patterns[i], patterns[j], weights)
            sim_matrix[i, j] = sim
            sim_matrix[j, i] = sim
        sim_matrix[i, i] = 100.0  # 자기 자신

    # 단순 클러스터링: 가장 유사한 것끼리 묶기
    assigned = [False] * n
    clusters = []
    cluster_id = 0

    for i in range(n):
        if assigned[i]:
            continue

        # i를 중심으로 유사한 패턴 모으기
        members_idx = [i]
        assigned[i] = True

        for j in range(i + 1, n):
            if assigned[j]:
                continue
            if sim_matrix[i, j] >= similarity_threshold:
                members_idx.append(j)
                assigned[j] = True

        # 클러스터 통계 계산
        member_patterns = [patterns[idx] for idx in members_idx]

        # 평균 등락률 흐름 (길이 맞추기: 최소 길이로)
        min_len = min(len(p.returns) for p in member_patterns)
        avg_returns = []
        avg_volumes = []
        for d in range(min_len):
            r_vals = [p.returns[d] for p in member_patterns if d < len(p.returns)]
            v_vals = [p.volume_ratios[d] for p in member_patterns if d < len(p.volume_ratios)]
            avg_returns.append(round(sum(r_vals) / len(r_vals), 4) if r_vals else 0)
            avg_volumes.append(round(sum(v_vals) / len(v_vals), 4) if v_vals else 1)

        # 평균 유사도
        sims = []
        for a in range(len(members_idx)):
            for b in range(a + 1, len(members_idx)):
                sims.append(sim_matrix[members_idx[a], members_idx[b]])
        avg_sim = round(sum(sims) / len(sims), 2) if sims else 100.0

        avg_rise = sum(p.surge['rise_pct'] for p in member_patterns) / len(member_patterns)
        avg_days = sum(p.surge['rise_days'] for p in member_patterns) / len(member_patterns)

        # 멤버 정보
        member_dicts = []
        for p in member_patterns:
            member_dicts.append({
                'code': p.code,
                'name': p.name,
                'surge_date': p.surge['start_date'],
                'rise_pct': p.surge['rise_pct'],
                'rise_days': p.surge['rise_days'],
            })

        # 패턴 설명 자동 생성
        desc = _generate_pattern_description(avg_returns, avg_volumes, member_patterns)

        cluster = PatternCluster(
            cluster_id=cluster_id,
            pattern_count=len(member_patterns),
            avg_similarity=avg_sim,
            avg_return_flow=avg_returns,
            avg_volume_flow=avg_volumes,
            avg_rise_pct=round(avg_rise, 2),
            avg_rise_days=round(avg_days, 1),
            win_rate=100.0,  # 이 패턴들은 모두 급상승한 것이므로
            members=member_dicts,
            description=desc
        )
        clusters.append(cluster)
        cluster_id += 1

    # 패턴 수가 많은 클러스터 우선 정렬
    clusters.sort(key=lambda c: c.pattern_count, reverse=True)
    return clusters


def _generate_pattern_description(
    avg_returns: List[float],
    avg_volumes: List[float],
    patterns: List[PreRisePattern]
) -> str:
    """패턴 설명 자동 생성"""
    parts = []

    # 등락률 흐름 분석
    if len(avg_returns) >= 3:
        neg_count = sum(1 for r in avg_returns if r < 0)
        pos_count = sum(1 for r in avg_returns if r >= 0)

        if neg_count > pos_count:
            # 마지막 봉이 양봉이면 → 눌림 후 반전
            if avg_returns[-1] > 0:
                parts.append(f"연속 {neg_count}일 하락 후 양봉 전환")
            else:
                parts.append(f"연속 {neg_count}일 하락세 유지")
        else:
            parts.append(f"상승세 유지 중 ({pos_count}일 양봉)")

        # 등락폭
        avg_drop = sum(r for r in avg_returns if r < 0)
        if avg_drop < -3:
            parts.append(f"누적 하락 {avg_drop:.1f}%")

    # 거래량 흐름 분석
    if len(avg_volumes) >= 3:
        early_vol = sum(avg_volumes[:len(avg_volumes)//2]) / max(1, len(avg_volumes)//2)
        late_vol = sum(avg_volumes[len(avg_volumes)//2:]) / max(1, len(avg_volumes) - len(avg_volumes)//2)

        if late_vol < early_vol * 0.7:
            parts.append("거래량 점진적 감소 (매도세 약화)")
        elif late_vol > early_vol * 1.5:
            parts.append("거래량 급증 (매수세 유입)")
        else:
            parts.append("거래량 보합")

    # 봉 모양 분석
    bullish_count = 0
    for p in patterns:
        if p.candle_shapes and p.candle_shapes[-1].get('is_bullish', 0) > 0:
            bullish_count += 1
    bullish_ratio = bullish_count / len(patterns) * 100 if patterns else 0
    if bullish_ratio >= 70:
        parts.append(f"마지막 봉 양봉 비율 {bullish_ratio:.0f}%")

    return " → ".join(parts) if parts else "패턴 분석 중"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 현재 매수 추천 / Current Buy Recommendation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def find_current_matches(
    candles_by_code: Dict[str, List[CandleDay]],
    names: Dict[str, str],
    clusters: List[PatternCluster],
    pre_days: int = 10,
    weights: Dict[str, float] = None,
    dip_results: Dict = None,  # ★ v3: 패턴 라이브러리 결과
) -> List[Dict]:
    """
    현재 일봉 흐름이 도출된 공통 패턴과 얼마나 유사한지 계산
    → 매수 추천 목록 생성
    [v2] weights를 사용하여 등락률/거래량 비중 조절
    [v3] dip_results: 패턴 라이브러리 매칭 결과 → 가산점 + 최소 임계 완화
    """
    recommendations = []

    if not clusters:
        return recommendations

    if weights is None:
        weights = DEFAULT_WEIGHTS

    if dip_results is None:
        dip_results = {}

    # [v2] 가중치 기반 추천 유사도 비율 계산
    # returns + volume 기준으로 비례 배분 (candle은 추천에서 제외)
    w_r = weights.get('returns', 0.5)
    w_v = weights.get('volume', 0.3)
    total_rv = w_r + w_v
    if total_rv > 0:
        rec_weight_returns = w_r / total_rv
        rec_weight_volume = w_v / total_rv
    else:
        rec_weight_returns = 0.6
        rec_weight_volume = 0.4

    for code, candles in candles_by_code.items():
        if len(candles) < pre_days + 20:
            continue

        # 현재(마지막 N일) 패턴 추출
        recent = candles[-pre_days:]
        name = names.get(code, code)

        # 등락률 계산
        current_returns = []
        for k in range(len(recent)):
            if k == 0:
                prev_close = candles[-(pre_days + 1)].close
                ret = ((recent[0].close - prev_close) / prev_close) * 100 if prev_close > 0 else 0
            else:
                prev_close = recent[k - 1].close
                ret = ((recent[k].close - prev_close) / prev_close) * 100 if prev_close > 0 else 0
            current_returns.append(round(ret, 4))

        # 거래량 비율
        current_volumes = []
        for k in range(len(recent)):
            abs_idx = len(candles) - pre_days + k
            vol_start = max(0, abs_idx - 20)
            vol_slice = candles[vol_start:abs_idx]
            avg_vol = sum(c.volume for c in vol_slice) / len(vol_slice) if vol_slice else 1
            ratio = round(recent[k].volume / avg_vol, 4) if avg_vol > 0 else 1.0
            current_volumes.append(ratio)

        # 각 클러스터와 DTW 유사도
        best_sim = 0
        best_cluster_id = 0

        for cluster in clusters:
            if not cluster.avg_return_flow:
                continue

            # 등락률 DTW
            sim_r = dtw_similarity(current_returns, cluster.avg_return_flow)
            # 거래량 DTW
            sim_v = dtw_similarity(current_volumes, cluster.avg_volume_flow)
            # [v2] 가중치 기반 종합 (기존 하드코딩 0.6/0.4 → 동적)
            sim = sim_r * rec_weight_returns + sim_v * rec_weight_volume

            if sim > best_sim:
                best_sim = sim
                best_cluster_id = cluster.cluster_id

        # ★ v3: 패턴 라이브러리 가산점
        dip_info = dip_results.get(code)
        dip_bonus = 0
        dip_pattern = None
        if dip_info and dip_info.get("is_dip"):
            dip_bonus = min(dip_info.get("total_score", 0) * 0.15, 15)  # 최대 +15
            dip_pattern = dip_info.get("best_pattern")
            best_sim = min(best_sim + dip_bonus, 100)

        # 시그널 판단 (★ v3: 패턴 매칭 시 임계 완화 50→40)
        min_threshold = 40 if dip_pattern else 50
        if best_sim >= 65:
            signal = "🟢 강력 매수"
            signal_code = "strong_buy"
        elif best_sim >= min_threshold:
            signal = "🟡 관심"
            signal_code = "watch"
        elif best_sim >= 40:
            signal = "⚠️ 대기"
            signal_code = "wait"
        else:
            signal = "⬜ 미해당"
            signal_code = "none"

        recommendations.append({
            'code': code,
            'name': name,
            'current_price': recent[-1].close if recent else 0,
            'similarity': round(best_sim, 1),
            'best_cluster_id': best_cluster_id,
            'signal': signal,
            'signal_code': signal_code,
            'current_returns': current_returns,
            'current_volumes': current_volumes,
            'last_date': recent[-1].date if recent else '',
            # ★ v3: 패턴 라이브러리 정보
            'dip_pattern': dip_pattern,
            'dip_bonus': round(dip_bonus, 1),
            'dip_matched_patterns': dip_info.get("matched_patterns", []) if dip_info else [],
        })

    # 유사도 높은 순 정렬
    recommendations.sort(key=lambda r: r['similarity'], reverse=True)
    return recommendations


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 통합 분석 함수 / Main Analysis Function
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_pattern_analysis(
    candles_by_code: Dict[str, List[CandleDay]],
    names: Dict[str, str],
    pre_days: int = 10,
    rise_pct: float = 30.0,
    rise_window: int = 5,
    progress_callback=None,
    weights: Dict[str, float] = None
) -> AnalysisResult:
    """
    전체 분석 실행
    - candles_by_code: {종목코드: [CandleDay, ...]}
    - names: {종목코드: 종목명}
    - pre_days: 급상승 직전 분석 일수
    - rise_pct: 급상승 기준 (%)
    - rise_window: 급상승 기간 (거래일)
    - progress_callback: 진행률 콜백 (pct, message)
    - [v2] weights: DTW 가중치 {'returns': 0.5, 'candle': 0.2, 'volume': 0.3}
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS

    all_surges = []
    all_patterns = []
    total_stocks = len(candles_by_code)

    # ── Step 1: 각 종목 급상승 구간 탐지 ──
    for idx, (code, candles) in enumerate(candles_by_code.items()):
        name = names.get(code, code)

        if progress_callback:
            pct = int((idx / total_stocks) * 40)  # 0~40%
            progress_callback(pct, f"급상승 구간 탐지 중: {name} ({idx+1}/{total_stocks})")

        surges = detect_surges(candles, code, name, rise_pct, rise_window)
        all_surges.extend(surges)

        # 각 급상승의 직전 패턴 추출
        for surge in surges:
            pattern = extract_pre_rise_pattern(candles, surge, pre_days)
            if pattern:
                all_patterns.append(pattern)

    logger.info(f"탐지 결과: {len(all_surges)}개 급상승, {len(all_patterns)}개 패턴")

    if progress_callback:
        progress_callback(45, f"패턴 추출 완료: {len(all_patterns)}개")

    # ── Step 2: DTW 클러스터링 — [v2] weights 전달 ──
    if progress_callback:
        progress_callback(50, "DTW 패턴 유사도 분석 중...")

    clusters_raw = cluster_patterns(all_patterns, weights=weights)
    clusters = [asdict(c) for c in clusters_raw]

    if progress_callback:
        progress_callback(75, f"클러스터링 완료: {len(clusters)}개 패턴 그룹")

    # ── ★ Step 2.5: 눌림목 패턴 라이브러리 필터 (v3 신규) ──
    dip_results = {}
    try:
        from app.engine.pattern_library import evaluate_dip_patterns, get_active_patterns_from_db
        active_patterns = get_active_patterns_from_db()

        if progress_callback:
            progress_callback(77, f"눌림목 패턴 라이브러리 적용 중... ({len(active_patterns)}개 패턴)")

        for code, candles_list in candles_by_code.items():
            # CandleDay → dict 변환
            candles_dict = [
                {"date": c.date, "open": c.open, "high": c.high,
                 "low": c.low, "close": c.close, "volume": c.volume}
                for c in candles_list
            ]
            result = evaluate_dip_patterns(candles_dict, active_patterns, require_gates=True)
            if result["is_dip"]:
                dip_results[code] = result

        if dip_results:
            dip_summary = ", ".join(f"{k}({v.get('best_pattern','')})" for k, v in list(dip_results.items())[:5])
            logger.info(f"[v3] 눌림목 패턴 매칭: {len(dip_results)}개 종목 ({dip_summary})")
            if progress_callback:
                progress_callback(79, f"눌림목 패턴 {len(dip_results)}개 종목 매칭")
    except ImportError:
        logger.info("[v3] pattern_library 미설치 — Step 2.5 스킵")
    except Exception as e:
        logger.warning(f"[v3] 패턴 라이브러리 오류 (무시): {e}")

    # ── Step 3: 현재 매수 추천 — [v2] weights 전달 ──
    if progress_callback:
        progress_callback(80, "현재 매수 추천 분석 중...")

    recommendations = find_current_matches(
        candles_by_code, names, clusters_raw, pre_days, weights=weights,
        dip_results=dip_results  # ★ v3: 패턴 라이브러리 결과 전달
    )

    # ── Step 4: 요약 생성 ──
    if progress_callback:
        progress_callback(90, "결과 정리 중...")

    summary = _generate_summary(all_surges, all_patterns, clusters_raw)

    # 패턴 직렬화
    patterns_dict = []
    for p in all_patterns:
        patterns_dict.append({
            'code': p.code,
            'name': p.name,
            'surge': p.surge,
            'returns': p.returns,
            'candle_shapes': p.candle_shapes,
            'volume_ratios': p.volume_ratios,
            'candles': p.candles,
            'pre_days': p.pre_days,
        })

    # 급상승 직렬화
    surges_dict = [asdict(s) for s in all_surges]

    if progress_callback:
        progress_callback(100, "분석 완료!")

    return AnalysisResult(
        total_stocks=total_stocks,
        total_surges=len(all_surges),
        total_patterns=len(all_patterns),
        clusters=clusters,
        all_patterns=patterns_dict,
        recommendations=recommendations,
        summary=summary,
        raw_surges=surges_dict,
        dip_matches=dip_results,  # ★ v3
    )


def _generate_summary(surges, patterns, clusters) -> Dict:
    """전체 요약 생성"""
    if not surges:
        return {
            'message': '급상승 구간이 발견되지 않았습니다.',
            'avg_rise_pct': 0,
            'avg_rise_days': 0,
            'total_surges': 0,
            'total_patterns': 0,
            'total_clusters': 0,
            'common_features': [],
        }

    avg_rise = sum(s.rise_pct for s in surges) / len(surges)
    avg_days = sum(s.rise_days for s in surges) / len(surges)

    # 공통 특징 추출
    features = []
    if clusters:
        top = clusters[0]
        features.append(f"가장 빈번한 패턴: {top.description}")
        features.append(f"대표 패턴 소속 {top.pattern_count}건 ({top.avg_similarity:.1f}% 유사도)")
        if top.avg_return_flow:
            neg_days = sum(1 for r in top.avg_return_flow if r < 0)
            features.append(f"급상승 전 평균 {neg_days}일 하락")

    return {
        'message': f'{len(surges)}개 급상승 구간에서 {len(patterns)}개 패턴 발견',
        'avg_rise_pct': round(avg_rise, 2),
        'avg_rise_days': round(avg_days, 1),
        'total_surges': len(surges),
        'total_patterns': len(patterns),
        'total_clusters': len(clusters),
        'common_features': features,
    }
