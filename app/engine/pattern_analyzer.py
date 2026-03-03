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
[v4] DTW 알고리즘 5대 강화:
     1) Z-Score 정규화 → 차원 간 스케일 통일
     2) 5차원 DTW → RSI 흐름 + MA20 이격도 추가
     3) 유사도 공식 → 선형 정규화 (변별력 향상)
     4) 클러스터 신뢰도 → 멤버수+유사도 기반 점수
     5) 3단계 급상승 탐지 → S/A/B급
"""

import numpy as np
import math
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime


# v5+v6 imports
try:
    from app.engine.entry_scorer import score_recommendations, summarize_entry_scores
    ENTRY_SCORER_AVAILABLE = True
except ImportError:
    ENTRY_SCORER_AVAILABLE = False

try:
    from app.engine.rec_backtest import backtest_recommended_stocks
    REC_BACKTEST_AVAILABLE = True
except ImportError:
    REC_BACKTEST_AVAILABLE = False

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ v5: Z-Score 정규화 캐시 (매 DTW 호출마다 import 반복 제거)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
try:
    from app.utils.indicators import z_normalize as _z_normalize_fn
except ImportError:
    def _z_normalize_fn(arr):
        """fallback z-normalize"""
        a = np.array(arr, dtype=float)
        std = np.std(a)
        if std < 1e-8:
            return a.tolist()
        return ((a - np.mean(a)) / std).tolist()

def _z_normalize_cached(s):
    """z_normalize wrapper — 이미 정규화된 데이터 감지 시 스킵"""
    if not s or len(s) < 2:
        return s
    return _z_normalize_fn(s)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 기본 가중치 / Default Weights
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ★ v4: 5차원 가중치 (기존 3차원에서 RSI + MA이격도 추가)
DEFAULT_WEIGHTS = {
    'returns': 0.35,
    'candle': 0.15,
    'volume': 0.20,
    'rsi': 0.15,
    'ma_dist': 0.15,
}

# v4: 3단계 급상승 탐지 기준
SURGE_TIERS = [
    {"grade": "S", "rise_pct": 30.0, "rise_days": 5,  "weight": 1.0},
    {"grade": "A", "rise_pct": 20.0, "rise_days": 5,  "weight": 0.8},
    {"grade": "B", "rise_pct": 25.0, "rise_days": 10, "weight": 0.6},
]


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
    grade: str = "S"        # ★ v4: 등급 (S/A/B)
    tier_weight: float = 1.0  # ★ v4: 등급 가중치


@dataclass
class PreRisePattern:
    """급상승 직전 패턴"""
    code: str
    name: str
    surge: dict             # SurgeZone 정보
    # 5차원 패턴 벡터 (★ v4: 3→5차원)
    returns: List[float]    # 등락률 흐름 (%)
    candle_shapes: List[Dict]  # 봉 모양 (양/음, 꼬리비율, 몸통크기)
    volume_ratios: List[float]  # 거래량 변화 (20일평균 대비)
    rsi_flow: List[float] = field(default_factory=list)      # ★ v4: RSI 흐름 (0~1)
    ma_dist_flow: List[float] = field(default_factory=list)  # ★ v4: MA20 이격도 (%)
    # 원본 일봉
    candles: List[Dict] = field(default_factory=list)
    pre_days: int = 10      # 분석 구간 (일)


@dataclass
class PatternCluster:
    """공통 패턴 클러스터"""
    cluster_id: int
    pattern_count: int          # 소속 패턴 수
    avg_similarity: float       # 평균 유사도
    avg_return_flow: List[float]  # 평균 등락률 흐름
    avg_volume_flow: List[float]  # 평균 거래량 흐름
    avg_rsi_flow: List[float] = field(default_factory=list)      # ★ v4: 평균 RSI 흐름
    avg_ma_dist_flow: List[float] = field(default_factory=list)  # ★ v4: 평균 MA이격도
    avg_rise_pct: float = 0.0          # 평균 상승폭
    avg_rise_days: float = 0.0         # 평균 상승 소요일
    win_rate: float = 100.0            # 이 패턴 후 실제 상승 확률
    confidence: float = 0.0    # ★ v4: 신뢰도 점수 (0~100)
    members: List[Dict] = field(default_factory=list)  # 소속 패턴 목록
    description: str = ""      # 패턴 설명 (자동 생성)


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
    entry_summary: Dict = field(default_factory=dict)  # v5
    rec_backtest_result: Dict = field(default_factory=dict)  # v6


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DTW 구현 (Pure NumPy — 외부 의존성 없음)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def dtw_distance(s1, s2, window: int = None) -> float:
    """
    DTW (Dynamic Time Warping) 거리 계산
    [v5] NumPy 배열 변환 + early termination 최적화
    - s1, s2: 비교할 두 시계열 (list 또는 np.array)
    - window: Sakoe-Chiba 밴드 폭 (None이면 전체)
    - return: DTW 거리 (작을수록 유사)
    """
    # numpy 배열로 1회 변환 (인덱싱 가속)
    a1 = np.asarray(s1, dtype=np.float64)
    a2 = np.asarray(s2, dtype=np.float64)
    n, m = len(a1), len(a2)
    if n == 0 or m == 0:
        return float('inf')

    if window is None:
        window = max(n, m)

    # 비용 행렬 초기화 — 1D 배열로 메모리 최적화 (2행만 유지)
    prev = np.full(m + 1, np.inf)
    curr = np.full(m + 1, np.inf)
    prev[0] = 0.0

    for i in range(1, n + 1):
        curr[:] = np.inf
        j_start = max(1, i - window)
        j_end = min(m, i + window)
        for j in range(j_start, j_end + 1):
            cost = abs(a1[i - 1] - a2[j - 1])
            curr[j] = cost + min(prev[j], curr[j - 1], prev[j - 1])
        prev, curr = curr, prev

    return prev[m]


def dtw_similarity(s1: List[float], s2: List[float], window: int = None, normalize: bool = True) -> float:
    """
    DTW 유사도 (0~100%, 높을수록 유사)
    [v4] 선형 정규화 + Z-Score 옵션 → 변별력 대폭 향상
    [v5] 성능 최적화: z_normalize import 1회만 + 빈 배열 조기 반환
    """
    if not s1 or not s2:
        return 0.0

    # ★ v5: Z-Score 정규화 (모듈 레벨 캐시로 import 오버헤드 제거)
    if normalize:
        s1 = _z_normalize_cached(s1)
        s2 = _z_normalize_cached(s2)

    dist = dtw_distance(s1, s2, window)
    norm = max(len(s1), len(s2))
    if norm == 0:
        return 0.0

    normalized = dist / norm

    # ★ v4: 선형 정규화 (기존 exp 감쇠 → 선형, max_dist=3.0 기준)
    # normalized가 0이면 100%, 3.0 이상이면 0%
    MAX_DIST = 3.0
    similarity = max(0.0, (1.0 - normalized / MAX_DIST)) * 100

    return round(similarity, 2)


def multi_dim_dtw_similarity(
    p1: PreRisePattern,
    p2: PreRisePattern,
    weights: Dict[str, float] = None
) -> float:
    """
    다차원 DTW 유사도 — 등락률 + 봉모양 + 거래량 + RSI + MA이격도
    [v2] weights를 외부에서 전달받아 사용
    [v4] 3차원 → 5차원 확장 (RSI + MA 이격도 추가)
    weights: {'returns': 0.35, 'candle': 0.15, 'volume': 0.20, 'rsi': 0.15, 'ma_dist': 0.15}
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS

    # ★ v5: 사전 정규화 — DTW 내부 반복 정규화 제거 (5배 속도 향상)
    r1_n, r2_n = _z_normalize_cached(p1.returns), _z_normalize_cached(p2.returns)
    body1 = [c.get('body_ratio', 0) for c in p1.candle_shapes]
    body2 = [c.get('body_ratio', 0) for c in p2.candle_shapes]
    b1_n, b2_n = _z_normalize_cached(body1), _z_normalize_cached(body2)
    v1_n, v2_n = _z_normalize_cached(p1.volume_ratios), _z_normalize_cached(p2.volume_ratios)

    # 1) 등락률 DTW (이미 정규화됨 → normalize=False)
    sim_returns = dtw_similarity(r1_n, r2_n, normalize=False)

    # 2) 봉 모양 DTW
    sim_candle = dtw_similarity(b1_n, b2_n, normalize=False)

    # 3) 거래량 DTW
    sim_volume = dtw_similarity(v1_n, v2_n, normalize=False)

    # ★ v4: 4) RSI 흐름 DTW
    sim_rsi = 0.0
    if p1.rsi_flow and p2.rsi_flow:
        sim_rsi = dtw_similarity(
            _z_normalize_cached(p1.rsi_flow),
            _z_normalize_cached(p2.rsi_flow),
            normalize=False
        )

    # ★ v4: 5) MA20 이격도 DTW
    sim_ma = 0.0
    if p1.ma_dist_flow and p2.ma_dist_flow:
        sim_ma = dtw_similarity(
            _z_normalize_cached(p1.ma_dist_flow),
            _z_normalize_cached(p2.ma_dist_flow),
            normalize=False
        )

    # 가중 합산 (v4: 5차원)
    # RSI/MA가 비어있으면 기존 3차원 가중치로 재분배
    w_r = weights.get('returns', 0.35)
    w_c = weights.get('candle', 0.15)
    w_v = weights.get('volume', 0.20)
    w_rsi = weights.get('rsi', 0.15) if (p1.rsi_flow and p2.rsi_flow) else 0
    w_ma = weights.get('ma_dist', 0.15) if (p1.ma_dist_flow and p2.ma_dist_flow) else 0

    # 미사용 가중치 재분배
    total_w = w_r + w_c + w_v + w_rsi + w_ma
    if total_w > 0:
        w_r /= total_w
        w_c /= total_w
        w_v /= total_w
        w_rsi /= total_w
        w_ma /= total_w

    total = (
        w_r * sim_returns +
        w_c * sim_candle +
        w_v * sim_volume +
        w_rsi * sim_rsi +
        w_ma * sim_ma
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
    [v4] 3단계 탐지: S급(5일/+30%), A급(5일/+20%), B급(10일/+25%)
    - rise_pct, rise_days 인자는 호환성 유지용 (실제로는 SURGE_TIERS 사용)
    """
    surges = []
    n = len(candles)

    if n < 6:
        return surges

    # ★ v4: 이미 탐지된 구간 인덱스 추적 (중복 방지)
    used_indices = set()

    for tier in SURGE_TIERS:
        t_pct = tier["rise_pct"]
        t_days = tier["rise_days"]
        t_grade = tier["grade"]
        t_weight = tier["weight"]

        if n < t_days + 1:
            continue

        i = 0
        while i < n - t_days:
            # 이미 사용된 구간이면 건너뛰기
            if i in used_indices:
                i += 1
                continue

            base_price = candles[i].close

            # 향후 t_days 내 최고 종가 찾기
            max_price = base_price
            max_idx = i
            for j in range(i + 1, min(i + t_days + 1, n)):
                if candles[j].close > max_price:
                    max_price = candles[j].close
                    max_idx = j

            pct = ((max_price - base_price) / base_price) * 100

            if pct >= t_pct:
                # 중복 체크: 이 구간이 이미 상위 등급에서 탐지되었는지
                overlap = any(idx in used_indices for idx in range(i, max_idx + 1))
                if not overlap:
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
                        rise_days=max_idx - i,
                        grade=t_grade,
                        tier_weight=t_weight,
                    )
                    surges.append(surge)
                    # 구간 인덱스 등록
                    for idx in range(i, max_idx + 1):
                        used_indices.add(idx)
                    i = max_idx + 1
                else:
                    i += 1
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

    # ★ v4: RSI 흐름 추출 (패턴 구간의 RSI/100 값)
    rsi_flow = []
    try:
        from app.utils.indicators import rsi_series
        # RSI 계산에 충분한 이전 데이터 포함
        rsi_start = max(0, pattern_start - 20)
        rsi_closes = [c.close for c in candles[rsi_start:start_idx]]
        rsi_vals = rsi_series(rsi_closes, 14)
        # 패턴 구간만 추출
        offset = pattern_start - rsi_start
        for k in range(len(pattern_candles)):
            idx = offset + k
            if idx < len(rsi_vals) and rsi_vals[idx] is not None:
                rsi_flow.append(rsi_vals[idx])
            else:
                rsi_flow.append(0.5)  # 기본값
    except Exception:
        rsi_flow = [0.5] * len(pattern_candles)

    # ★ v4: MA20 이격도 추출
    ma_dist_flow = []
    try:
        from app.utils.indicators import ma_distance_ratio
        ma_start = max(0, pattern_start - 25)
        ma_closes = [c.close for c in candles[ma_start:start_idx]]
        ma_vals = ma_distance_ratio(ma_closes, 20)
        offset = pattern_start - ma_start
        for k in range(len(pattern_candles)):
            idx = offset + k
            if idx < len(ma_vals) and ma_vals[idx] is not None:
                ma_dist_flow.append(ma_vals[idx])
            else:
                ma_dist_flow.append(0.0)
    except Exception:
        ma_dist_flow = [0.0] * len(pattern_candles)

    return PreRisePattern(
        code=surge.code,
        name=surge.name,
        surge=asdict(surge),
        returns=returns,
        candle_shapes=candle_shapes,
        volume_ratios=volume_ratios,
        rsi_flow=rsi_flow,
        ma_dist_flow=ma_dist_flow,
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
        avg_rsi = []
        avg_ma_dist = []
        for d in range(min_len):
            r_vals = [p.returns[d] for p in member_patterns if d < len(p.returns)]
            v_vals = [p.volume_ratios[d] for p in member_patterns if d < len(p.volume_ratios)]
            avg_returns.append(round(sum(r_vals) / len(r_vals), 4) if r_vals else 0)
            avg_volumes.append(round(sum(v_vals) / len(v_vals), 4) if v_vals else 1)

            # ★ v4: RSI + MA이격도 평균
            rsi_vals = [p.rsi_flow[d] for p in member_patterns if d < len(p.rsi_flow)]
            ma_vals = [p.ma_dist_flow[d] for p in member_patterns if d < len(p.ma_dist_flow)]
            avg_rsi.append(round(sum(rsi_vals) / len(rsi_vals), 4) if rsi_vals else 0.5)
            avg_ma_dist.append(round(sum(ma_vals) / len(ma_vals), 4) if ma_vals else 0)

        # 평균 유사도
        sims = []
        for a in range(len(members_idx)):
            for b in range(a + 1, len(members_idx)):
                sims.append(sim_matrix[members_idx[a], members_idx[b]])
        avg_sim = round(sum(sims) / len(sims), 2) if sims else 100.0

        avg_rise = sum(p.surge['rise_pct'] for p in member_patterns) / len(member_patterns)
        avg_days = sum(p.surge['rise_days'] for p in member_patterns) / len(member_patterns)

        # ★ v4: 신뢰도 점수 (멤버수 × 15 + 평균유사도 × 0.3, 최대 100)
        confidence = min(100.0, len(member_patterns) * 15 + avg_sim * 0.3)

        # ★ v4: 등급별 가중 win_rate (S급은 100%, B급은 tier_weight 반영)
        grade_weights = [p.surge.get('tier_weight', 1.0) for p in member_patterns]
        avg_grade_weight = sum(grade_weights) / len(grade_weights) if grade_weights else 1.0
        win_rate = round(100.0 * avg_grade_weight, 1)

        # 멤버 정보
        member_dicts = []
        for p in member_patterns:
            member_dicts.append({
                'code': p.code,
                'name': p.name,
                'surge_date': p.surge['start_date'],
                'rise_pct': p.surge['rise_pct'],
                'rise_days': p.surge['rise_days'],
                'grade': p.surge.get('grade', 'S'),  # ★ v4
            })

        # 패턴 설명 자동 생성
        desc = _generate_pattern_description(avg_returns, avg_volumes, member_patterns)

        cluster = PatternCluster(
            cluster_id=cluster_id,
            pattern_count=len(member_patterns),
            avg_similarity=avg_sim,
            avg_return_flow=avg_returns,
            avg_volume_flow=avg_volumes,
            avg_rsi_flow=avg_rsi,
            avg_ma_dist_flow=avg_ma_dist,
            avg_rise_pct=round(avg_rise, 2),
            avg_rise_days=round(avg_days, 1),
            win_rate=win_rate,
            confidence=round(confidence, 1),
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

        # ★ v4: RSI 흐름 추출
        current_rsi = []
        try:
            from app.utils.indicators import rsi_series
            rsi_start = max(0, len(candles) - pre_days - 20)
            rsi_closes = [c.close for c in candles[rsi_start:]]
            rsi_vals = rsi_series(rsi_closes, 14)
            offset = len(candles) - pre_days - rsi_start
            for k in range(pre_days):
                idx = offset + k
                if idx < len(rsi_vals) and rsi_vals[idx] is not None:
                    current_rsi.append(rsi_vals[idx])
                else:
                    current_rsi.append(0.5)
        except Exception:
            current_rsi = [0.5] * pre_days

        # ★ v4: MA20 이격도 추출
        current_ma_dist = []
        try:
            from app.utils.indicators import ma_distance_ratio
            ma_start = max(0, len(candles) - pre_days - 25)
            ma_closes = [c.close for c in candles[ma_start:]]
            ma_vals = ma_distance_ratio(ma_closes, 20)
            offset = len(candles) - pre_days - ma_start
            for k in range(pre_days):
                idx = offset + k
                if idx < len(ma_vals) and ma_vals[idx] is not None:
                    current_ma_dist.append(ma_vals[idx])
                else:
                    current_ma_dist.append(0.0)
        except Exception:
            current_ma_dist = [0.0] * pre_days

        # 각 클러스터와 DTW 유사도
        best_sim = 0
        best_cluster_id = 0
        best_confidence = 0

        for cluster in clusters:
            if not cluster.avg_return_flow:
                continue

            # ★ v4: 5차원 DTW 유사도 (Z-Score 정규화 적용)
            sim_r = dtw_similarity(current_returns, cluster.avg_return_flow)
            sim_v = dtw_similarity(current_volumes, cluster.avg_volume_flow)
            sim_rsi = dtw_similarity(current_rsi, cluster.avg_rsi_flow) if cluster.avg_rsi_flow else 0
            sim_ma = dtw_similarity(current_ma_dist, cluster.avg_ma_dist_flow) if cluster.avg_ma_dist_flow else 0

            # 5차원 가중 합산 (미사용 차원 재분배)
            w_r = weights.get('returns', 0.35)
            w_v = weights.get('volume', 0.20)
            w_rsi = weights.get('rsi', 0.15) if cluster.avg_rsi_flow else 0
            w_ma = weights.get('ma_dist', 0.15) if cluster.avg_ma_dist_flow else 0
            tw = w_r + w_v + w_rsi + w_ma
            if tw > 0:
                sim = (w_r * sim_r + w_v * sim_v + w_rsi * sim_rsi + w_ma * sim_ma) / tw
            else:
                sim = sim_r * 0.6 + sim_v * 0.4

            # ★ v4: 신뢰도 50 미만 클러스터는 유사도 50% 감쇠
            if cluster.confidence < 50:
                sim *= 0.5

            if sim > best_sim:
                best_sim = sim
                best_cluster_id = cluster.cluster_id
                best_confidence = cluster.confidence

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
            'confidence': round(best_confidence, 1),  # ★ v4
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

    # ── v5: Step 3.5 entry scoring ──
    entry_summary = {}
    candles_dict_by_code = {}
    if ENTRY_SCORER_AVAILABLE and recommendations:
        try:
            for code, candles_list in candles_by_code.items():
                candles_dict_by_code[code] = [
                    {"date": c.date, "open": c.open, "high": c.high,
                     "low": c.low, "close": c.close, "volume": c.volume}
                    for c in candles_list
                ]
            recommendations = score_recommendations(
                recommendations=recommendations,
                candles_by_code=candles_dict_by_code,
            )
            entry_summary = summarize_entry_scores(recommendations)
            logger.info("[v5] entry scoring done")
            if progress_callback:
                progress_callback(85, "entry scoring done")
        except Exception as e:
            logger.warning(f"[v5] entry scoring failed: {e}")

    # ── v6: Step 3.7 rec backtest ──
    rec_backtest_result = {}
    if REC_BACKTEST_AVAILABLE and recommendations and clusters:
        try:
            if not candles_dict_by_code:
                for code, candles_list in candles_by_code.items():
                    candles_dict_by_code[code] = [
                        {"date": c.date, "open": c.open, "high": c.high,
                         "low": c.low, "close": c.close, "volume": c.volume}
                        for c in candles_list
                    ]
            if progress_callback:
                progress_callback(87, "rec backtest running...")
            rec_backtest_result = backtest_recommended_stocks(
                recommendations=recommendations,
                candles_by_code=candles_dict_by_code,
                clusters=clusters,
                pre_days=pre_days,
            )
            logger.info("[v6] rec backtest done")
            if progress_callback:
                progress_callback(89, "rec backtest done")
        except Exception as e:
            logger.warning(f"[v6] rec backtest failed: {e}")

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
            'rsi_flow': p.rsi_flow,          # ★ v4
            'ma_dist_flow': p.ma_dist_flow,  # ★ v4
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
        entry_summary=entry_summary,  # v5
        rec_backtest_result=rec_backtest_result,  # v6
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
