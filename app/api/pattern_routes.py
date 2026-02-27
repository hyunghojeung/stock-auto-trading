"""
급상승 패턴 탐지기 — API 라우트
Pattern Surge Detector — API Routes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/api/pattern_routes.py

POST /api/pattern/analyze  — 분석 시작 (비동기)
GET  /api/pattern/progress  — 진행률 확인
GET  /api/pattern/result    — 결과 조회
POST /api/pattern/search    — 종목 검색

★ v2 수정사항: 매수 추천을 전종목 DB에서 스캔하여
  분석 대상이 아닌 "다른 종목" 중 유사 패턴 보유 종목을 추천
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Tuple
import asyncio
import logging
import traceback
import random
import re
import urllib.parse
import urllib.request
import json

from app.engine.pattern_analyzer import (
    CandleDay,
    run_pattern_analysis,
)

# dtw_similarity가 pattern_analyzer에서 import 가능하면 사용, 아니면 내장 버전 사용
try:
    from app.engine.pattern_analyzer import dtw_similarity
except ImportError:
    import math

    def _dtw_distance(s1, s2, window=None):
        n, m = len(s1), len(s2)
        if n == 0 or m == 0:
            return float('inf')
        if window is None:
            window = max(n, m)
        cost = [[float('inf')] * (m + 1) for _ in range(n + 1)]
        cost[0][0] = 0
        for i in range(1, n + 1):
            for j in range(max(1, i - window), min(m, i + window) + 1):
                d = (s1[i - 1] - s2[j - 1]) ** 2
                cost[i][j] = d + min(cost[i - 1][j], cost[i][j - 1], cost[i - 1][j - 1])
        return math.sqrt(cost[n][m]) if cost[n][m] < float('inf') else float('inf')

    def dtw_similarity(s1, s2) -> float:
        if not s1 or not s2:
            return 0.0
        s1_std = max(max(s1) - min(s1), 0.001)
        s2_std = max(max(s2) - min(s2), 0.001)
        s1_norm = [(v - sum(s1) / len(s1)) / s1_std for v in s1]
        s2_norm = [(v - sum(s2) / len(s2)) / s2_std for v in s2]
        dist = _dtw_distance(s1_norm, s2_norm, window=max(len(s1), len(s2)))
        max_len = max(len(s1), len(s2))
        similarity = max(0, 100 - (dist / max_len) * 30)
        return round(min(similarity, 100), 1)
from app.services.naver_stock import get_daily_candles_with_name
from app.core.database import db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/pattern", tags=["pattern"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전역 상태 (분석 진행률 + 결과 캐시)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_analysis_state = {
    "running": False,
    "progress": 0,
    "message": "",
    "result": None,
    "error": None,
    "has_result": False,
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 요청/응답 모델
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class AnalyzeRequest(BaseModel):
    codes: List[str]
    names: dict = {}
    period_days: int = 365
    pre_rise_days: int = 10
    rise_pct: float = 30.0
    rise_window: int = 5
    # 가중치 (프론트엔드 상세설정)
    weight_returns: float = 0.4
    weight_candle: float = 0.3
    weight_volume: float = 0.3


class SearchRequest(BaseModel):
    keyword: str


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 네이버 캔들 → CandleDay 변환
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_candles_for_code(code: str, period_days: int) -> Tuple[List[CandleDay], str]:
    """
    네이버 금융에서 일봉 데이터 조회 → CandleDay 리스트 + 종목명 변환
    """
    try:
        loop = asyncio.get_event_loop()
        raw_candles, stock_name = await loop.run_in_executor(
            None, lambda: get_daily_candles_with_name(code, count=period_days)
        )

        if not raw_candles:
            return [], code

        candles = []
        for item in raw_candles:
            try:
                candle = CandleDay(
                    date=str(item.get("date", "")),
                    open=float(item.get("open", 0)),
                    high=float(item.get("high", 0)),
                    low=float(item.get("low", 0)),
                    close=float(item.get("close", 0)),
                    volume=int(item.get("volume", 0)),
                )
                if candle.close > 0 and candle.volume >= 0:
                    candles.append(candle)
            except (ValueError, TypeError):
                continue

        candles.sort(key=lambda c: c.date)
        return candles, stock_name

    except Exception as e:
        logger.error(f"[{code}] 캔들 조회 실패: {e}")
        return [], code


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 전종목 매수 추천 스캔 (v2 핵심 추가)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _compute_pattern_vectors(candles: List[CandleDay], pre_days: int):
    """
    최근 pre_days일의 등락률 + 거래량비율 벡터 계산
    Compute return flow + volume ratio vectors for recent N days
    """
    if len(candles) < pre_days + 20:
        return None, None

    recent = candles[-pre_days:]

    # 등락률 벡터 / Return flow vector
    returns = []
    for k in range(len(recent)):
        if k == 0:
            idx_in_full = len(candles) - pre_days
            prev_close = candles[idx_in_full - 1].close if idx_in_full > 0 else recent[0].open
        else:
            prev_close = recent[k - 1].close
        ret = ((recent[k].close - prev_close) / prev_close) * 100 if prev_close > 0 else 0
        returns.append(round(ret, 4))

    # 거래량 비율 벡터 / Volume ratio vector
    volumes = []
    for k in range(len(recent)):
        abs_idx = len(candles) - pre_days + k
        vol_start = max(0, abs_idx - 20)
        vol_slice = candles[vol_start:abs_idx]
        avg_vol = sum(c.volume for c in vol_slice) / len(vol_slice) if vol_slice else 1
        ratio = round(recent[k].volume / avg_vol, 4) if avg_vol > 0 else 1.0
        volumes.append(ratio)

    return returns, volumes


async def _scan_recommendations(
    clusters_dicts: list,
    analyzed_codes: set,
    pre_days: int,
    progress_callback=None,
    max_candidates: int = 300,
) -> list:
    """
    ★ v3: stock_patterns DB에서 사전 수집된 벡터로 전종목 DTW 비교
    DB 미구축 시 기존 실시간 방식으로 fallback

    Args:
        clusters_dicts: 분석 결과의 클러스터 (dict 형태)
        analyzed_codes: 분석 대상 종목 코드 (제외 대상)
        pre_days: 패턴 분석 일수
        progress_callback: 진행률 콜백
        max_candidates: 최대 후보 종목 수 (fallback 전용)
    Returns:
        매수 추천 리스트 (유사도순 정렬)
    """
    # ── 유효 클러스터 확인 ──
    valid_clusters = [
        c for c in clusters_dicts
        if c.get("avg_return_flow") and c.get("avg_volume_flow")
    ]
    if not valid_clusters:
        logger.warning("유효한 클러스터가 없습니다")
        return []

    # ── DB 벡터 방식 시도 ──
    if progress_callback:
        progress_callback(78, "stock_patterns DB에서 전종목 벡터 로드 중...")

    try:
        resp = db.table("stock_patterns").select(
            "code, name, market, returns_30d, volumes_30d, last_close, last_date"
        ).execute()
        pattern_rows = resp.data or []
    except Exception as e:
        logger.warning(f"stock_patterns 조회 실패 (fallback 실행): {e}")
        pattern_rows = []

    if len(pattern_rows) >= 100:
        # ━━━ DB 벡터 방식 (빠른 경로) ━━━
        logger.info(f"[Phase2] DB 벡터 방식: {len(pattern_rows)}개 로드됨")
        return _match_from_db_vectors(
            pattern_rows, valid_clusters, analyzed_codes, pre_days, progress_callback
        )
    else:
        # ━━━ 실시간 방식 (fallback) ━━━
        logger.info(f"[Phase2] DB 벡터 부족({len(pattern_rows)}개), 실시간 fallback")
        return await _match_realtime_fallback(
            valid_clusters, analyzed_codes, pre_days, progress_callback, max_candidates
        )


def _match_from_db_vectors(
    pattern_rows: list,
    valid_clusters: list,
    analyzed_codes: set,
    pre_days: int,
    progress_callback=None,
) -> list:
    """
    ★ DB 벡터 기반 전종목 매칭 — 네이버 API 호출 없음, 수초 완료
    """
    if progress_callback:
        progress_callback(80, f"DB에서 {len(pattern_rows)}개 종목 벡터 비교 중...")

    recommendations = []
    total = len(pattern_rows)

    for idx, row in enumerate(pattern_rows):
        code = row.get("code", "")
        name = row.get("name", code)

        # 분석 대상 종목 제외
        if code in analyzed_codes:
            continue

        # 벡터 파싱
        try:
            returns_full = row.get("returns_30d", [])
            volumes_full = row.get("volumes_30d", [])

            # JSONB가 문자열로 올 수 있음
            if isinstance(returns_full, str):
                returns_full = json.loads(returns_full)
            if isinstance(volumes_full, str):
                volumes_full = json.loads(volumes_full)

            if not returns_full or not volumes_full:
                continue

            # pre_days에 맞게 슬라이스 (30일 중 최근 N일)
            current_returns = returns_full[-pre_days:] if len(returns_full) >= pre_days else returns_full
            current_volumes = volumes_full[-pre_days:] if len(volumes_full) >= pre_days else volumes_full

        except Exception:
            continue

        # 각 클러스터와 DTW 유사도 비교
        best_sim = 0
        best_cluster_id = 0

        for cluster in valid_clusters:
            try:
                sim_r = dtw_similarity(current_returns, cluster["avg_return_flow"])
                sim_v = dtw_similarity(current_volumes, cluster["avg_volume_flow"])
                sim = sim_r * 0.6 + sim_v * 0.4

                if sim > best_sim:
                    best_sim = sim
                    best_cluster_id = cluster.get("cluster_id", 0)
            except Exception:
                continue

        # 시그널 판단
        if best_sim >= 65:
            signal = "🟢 강력 매수"
            signal_code = "strong_buy"
        elif best_sim >= 50:
            signal = "🟡 관심"
            signal_code = "watch"
        elif best_sim >= 40:
            signal = "⚠️ 대기"
            signal_code = "wait"
        else:
            signal = "⬜ 미해당"
            signal_code = "none"

        if best_sim >= 35:
            recommendations.append({
                "code": code,
                "name": name,
                "current_price": row.get("last_close", 0),
                "similarity": round(best_sim, 1),
                "best_cluster_id": best_cluster_id,
                "signal": signal,
                "signal_code": signal_code,
                "current_returns": current_returns[-5:],
                "current_volumes": current_volumes[-5:],
                "last_date": row.get("last_date", ""),
                "signal_date": row.get("last_date", ""),
            })

        # 진행률 업데이트 (500개마다)
        if progress_callback and idx % 500 == 0:
            pct = 80 + int((idx / total) * 17)  # 80~97%
            progress_callback(pct, f"벡터 비교: {idx}/{total}")

    # 유사도 높은 순 정렬, 상위 30개
    recommendations.sort(key=lambda r: r["similarity"], reverse=True)
    recommendations = recommendations[:30]

    if progress_callback:
        progress_callback(98, f"전종목 {total}개 스캔 완료 → {len(recommendations)}개 추천")

    logger.info(f"[Phase2-DB] 전종목 {total}개 스캔, {len(recommendations)}개 추천")
    return recommendations


async def _match_realtime_fallback(
    valid_clusters: list,
    analyzed_codes: set,
    pre_days: int,
    progress_callback=None,
    max_candidates: int = 300,
) -> list:
    """
    실시간 네이버 캔들 수집 방식 (fallback — DB 미구축 시)
    """
    # ── stock_list에서 후보 로드 ──
    if progress_callback:
        progress_callback(79, "stock_list에서 후보 종목 로드 중 (fallback)...")

    try:
        resp = db.table("stock_list").select("code, name, market").execute()
        all_stocks = resp.data or []
    except Exception as e:
        logger.error(f"stock_list 조회 실패: {e}")
        return []

    if not all_stocks:
        return []

    # ── 비주식 종목 필터링 ──
    from app.services.stock_pattern_collector import is_regular_stock
    filtered = [s for s in all_stocks if is_regular_stock(s)]
    candidates = [s for s in filtered if s["code"] not in analyzed_codes]

    # ── 샘플링 ──
    if len(candidates) > max_candidates:
        kospi = [s for s in candidates if s.get("market", "").lower() == "kospi"]
        kosdaq = [s for s in candidates if s.get("market", "").lower() == "kosdaq"]

        kospi_n = int(max_candidates * 0.5)
        kosdaq_n = max_candidates - kospi_n

        sampled = []
        if kospi:
            sampled.extend(random.sample(kospi, min(kospi_n, len(kospi))))
        if kosdaq:
            sampled.extend(random.sample(kosdaq, min(kosdaq_n, len(kosdaq))))

        candidates = sampled

    # ── 캔들 수집 + 벡터 계산 + DTW 비교 ──
    if progress_callback:
        progress_callback(80, f"후보 {len(candidates)}개 캔들 수집 중 (fallback)...")

    recommendations = []
    total_cands = len(candidates)

    for idx, stock in enumerate(candidates):
        code = stock["code"]
        name = stock.get("name", code)

        if progress_callback and idx % 20 == 0:
            pct = 80 + int((idx / total_cands) * 12)
            progress_callback(pct, f"후보 캔들 수집: {name} ({idx+1}/{total_cands})")

        try:
            candles, fetched_name = await fetch_candles_for_code(code, pre_days + 30)
            if not candles or len(candles) < pre_days + 20:
                continue

            current_returns, current_volumes = _compute_pattern_vectors(candles, pre_days)
            if current_returns is None:
                continue

            name = fetched_name or name
        except Exception:
            continue

        await asyncio.sleep(0.15)

        # DTW 비교
        best_sim = 0
        best_cluster_id = 0

        for cluster in valid_clusters:
            try:
                sim_r = dtw_similarity(current_returns, cluster["avg_return_flow"])
                sim_v = dtw_similarity(current_volumes, cluster["avg_volume_flow"])
                sim = sim_r * 0.6 + sim_v * 0.4
                if sim > best_sim:
                    best_sim = sim
                    best_cluster_id = cluster.get("cluster_id", 0)
            except Exception:
                continue

        if best_sim >= 65:
            signal, signal_code = "🟢 강력 매수", "strong_buy"
        elif best_sim >= 50:
            signal, signal_code = "🟡 관심", "watch"
        elif best_sim >= 40:
            signal, signal_code = "⚠️ 대기", "wait"
        else:
            signal, signal_code = "⬜ 미해당", "none"

        if best_sim >= 35:
            recommendations.append({
                "code": code,
                "name": name,
                "current_price": candles[-1].close if candles else 0,
                "similarity": round(best_sim, 1),
                "best_cluster_id": best_cluster_id,
                "signal": signal,
                "signal_code": signal_code,
                "current_returns": current_returns[-5:],
                "current_volumes": current_volumes[-5:],
                "last_date": candles[-1].date if candles else "",
                "signal_date": candles[-1].date if candles else "",
            })

    recommendations.sort(key=lambda r: r["similarity"], reverse=True)
    recommendations = recommendations[:30]

    logger.info(f"[Phase2-fallback] {total_cands}개 스캔, {len(recommendations)}개 추천")
    return recommendations


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API 엔드포인트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/analyze")
async def start_analysis(req: AnalyzeRequest, background_tasks: BackgroundTasks):
    """분석 시작 (백그라운드 실행)"""
    global _analysis_state

    if _analysis_state.get("running"):
        raise HTTPException(status_code=409, detail="이미 분석이 진행 중입니다.")

    if not req.codes:
        raise HTTPException(status_code=400, detail="종목 코드를 1개 이상 입력하세요.")

    if len(req.codes) > 20:
        raise HTTPException(status_code=400, detail="최대 20개 종목까지 분석 가능합니다.")

    _analysis_state = {
        "running": True,
        "progress": 0,
        "message": "분석 준비 중...",
        "result": None,
        "error": None,
        "has_result": False,
        "saved_to_db": False,
        "codes": req.codes,
        "names": dict(req.names),
        "params": {
            "period_days": req.period_days,
            "pre_rise_days": req.pre_rise_days,
            "rise_pct": req.rise_pct,
            "rise_window": req.rise_window,
            "weight_returns": req.weight_returns,
            "weight_candle": req.weight_candle,
            "weight_volume": req.weight_volume,
        },
    }

    background_tasks.add_task(
        _run_analysis_task,
        req.codes,
        req.names,
        req.period_days,
        req.pre_rise_days,
        req.rise_pct,
        req.rise_window,
    )

    return {"status": "started", "message": f"{len(req.codes)}개 종목 분석 시작"}


async def _run_analysis_task(
    codes: List[str],
    names: dict,
    period_days: int,
    pre_rise_days: int,
    rise_pct: float,
    rise_window: int,
):
    """
    백그라운드 분석 태스크
    ★ Phase 1: 패턴 분석 (기존)
    ★ Phase 2: 전종목 매수 추천 스캔 (신규)
    """
    global _analysis_state

    try:
        # ══════════════════════════════════════════
        # Phase 1: 데이터 수집 + 패턴 분석
        # ══════════════════════════════════════════
        candles_by_code = {}
        total = len(codes)

        for idx, code in enumerate(codes):
            _analysis_state["progress"] = int((idx / total) * 25)
            _analysis_state["message"] = f"데이터 수집 중: {names.get(code, code)} ({idx+1}/{total})"

            candles, fetched_name = await fetch_candles_for_code(code, period_days)
            if candles:
                candles_by_code[code] = candles
                # 네이버에서 가져온 종목명으로 업데이트
                if fetched_name and fetched_name != code:
                    names[code] = fetched_name
            else:
                logger.warning(f"[{code}] 데이터 없음 — 스킵")

            await asyncio.sleep(0.3)

        if not candles_by_code:
            _analysis_state["running"] = False
            _analysis_state["error"] = "조회된 종목 데이터가 없습니다."
            _analysis_state["progress"] = 100
            return

        _analysis_state["progress"] = 28
        _analysis_state["message"] = f"{len(candles_by_code)}개 종목 데이터 수집 완료, 패턴 분석 시작..."

        # 패턴 분석 실행
        def progress_cb_phase1(pct, msg):
            # Phase1: 28~75% 범위
            mapped_pct = 28 + int(pct * 0.47)
            _analysis_state["progress"] = min(mapped_pct, 75)
            _analysis_state["message"] = msg

        result = run_pattern_analysis(
            candles_by_code=candles_by_code,
            names=names,
            pre_days=pre_rise_days,
            rise_pct=rise_pct,
            rise_window=rise_window,
            progress_callback=progress_cb_phase1,
        )


        # ══════════════════════════════════════════
        # Phase 2: 전종목 매수 추천 스캔 (★ 핵심 수정)
        # ══════════════════════════════════════════
        _analysis_state["progress"] = 76
        _analysis_state["message"] = "전종목 매수 추천 스캔 시작..."

        analyzed_codes = set(codes)
        clusters_dicts = result.clusters  # 이미 dict 리스트


        def progress_cb_phase2(pct, msg):
            _analysis_state["progress"] = pct
            _analysis_state["message"] = msg

        # 클러스터가 있을 때만 전종목 스캔 실행
        if clusters_dicts:
            new_recommendations = await _scan_recommendations(
                clusters_dicts=clusters_dicts,
                analyzed_codes=analyzed_codes,
                pre_days=pre_rise_days,
                progress_callback=progress_cb_phase2,
                max_candidates=300,
            )
        else:
            new_recommendations = []

        # ══════════════════════════════════════════
        # 결과 저장
        # ══════════════════════════════════════════

        # ★ 가상투자용 백테스트 추천 (과거 패턴 기반 — 역사적 날짜)
        # 종목별 가장 최근 패턴 1건만 사용 (중복 제거)
        backtest_by_code = {}
        for p in result.all_patterns:
            surge = p.get("surge", {})
            code = p.get("code", "")
            signal_date = surge.get("start_date", "")

            # 같은 종목이면 가장 최근 패턴만 유지
            if code not in backtest_by_code or signal_date > backtest_by_code[code]["signal_date"]:
                backtest_by_code[code] = {
                    "code": code,
                    "name": p.get("name", ""),
                    "signal_date": signal_date,
                    "buy_price": surge.get("start_price", 0),
                    "current_price": surge.get("start_price", 0),
                    "similarity": 100.0,
                    "signal": "📊 백테스트",
                    "signal_code": "backtest",
                    "surge_pct": surge.get("rise_pct", 0),
                    "surge_days": surge.get("rise_days", 0),
                    "candles": p.get("candles", []),
                }

        backtest_recs = list(backtest_by_code.values())

        _analysis_state["result"] = {
            "status": "done",
            "total_stocks": result.total_stocks,
            "total_surges": result.total_surges,
            "total_patterns": result.total_patterns,
            "clusters": result.clusters,
            "all_patterns": result.all_patterns,
            "recommendations": new_recommendations,  # ★ 전종목 스캔 결과 (매수 추천 탭)
            "backtest_recommendations": backtest_recs,  # ★ 가상투자용 (과거 날짜)
            "summary": result.summary,
            "raw_surges": result.raw_surges,
            # 추가 메타정보
            "scanned_candidates": len(new_recommendations),
            "analyzed_codes": list(analyzed_codes),
        }
        _analysis_state["progress"] = 100
        _analysis_state["message"] = "분석 완료!"
        _analysis_state["has_result"] = True
        _analysis_state["running"] = False

        logger.info(
            f"분석 완료: {result.total_surges}개 급상승, "
            f"{result.total_patterns}개 패턴, "
            f"{len(new_recommendations)}개 매수 추천"
        )

    except Exception as e:
        logger.error(f"분석 실패: {traceback.format_exc()}")
        _analysis_state["running"] = False
        _analysis_state["error"] = str(e)
        _analysis_state["progress"] = 100


@router.get("/progress")
async def get_progress():
    """진행률 조회"""
    return {
        "running": _analysis_state.get("running", False),
        "progress": _analysis_state.get("progress", 0),
        "message": _analysis_state.get("message", ""),
        "error": _analysis_state.get("error"),
        "has_result": _analysis_state.get("has_result", False),
    }


@router.get("/result")
async def get_result():
    """분석 결과 조회 (최초 조회 시 DB 자동 저장)"""
    if _analysis_state.get("error"):
        return {"status": "error", "error": _analysis_state["error"]}
    if _analysis_state.get("result"):
        # ━━━ DB 저장 (최초 1회만) ━━━
        if not _analysis_state.get("saved_to_db"):
            try:
                result = _analysis_state["result"]
                codes = _analysis_state.get("codes", [])
                names = _analysis_state.get("names", {})
                params = _analysis_state.get("params", {})

                pattern_count = result.get("total_patterns", 0)

                save_data = {
                    "preset": params.get("preset", "custom"),
                    "params": params,
                    "stock_codes": codes,
                    "stock_names": names,
                    "stock_count": len(codes),
                    "pattern_count": pattern_count,
                    "result_summary": {
                        "total_stocks": result.get("total_stocks", 0),
                        "total_surges": result.get("total_surges", 0),
                        "total_patterns": pattern_count,
                        "clusters": len(result.get("clusters", [])),
                        "recommendations": len(result.get("recommendations", [])),
                    },
                    "full_result": result,
                }

                db.table("pattern_analysis_sessions").insert(save_data).execute()
                _analysis_state["saved_to_db"] = True
                logger.info(f"✅ 패턴 분석 결과 DB 저장 완료 ({len(codes)}종목, {pattern_count}패턴)")
            except Exception as e:
                logger.error(f"⚠️ 패턴 분석 결과 DB 저장 실패: {e}")

        return _analysis_state["result"]
    return {"status": "waiting", "message": "분석 결과가 없습니다."}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 이전 분석 결과 조회 / Previous Results
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/previous")
async def get_previous_results():
    """이전 패턴 분석 결과 목록 (최근 10개)"""
    try:
        resp = (
            db.table("pattern_analysis_sessions")
            .select("id, created_at, preset, stock_codes, stock_names, stock_count, pattern_count, result_summary")
            .order("created_at", desc=True)
            .limit(10)
            .execute()
        )
        sessions = resp.data or []
        return {"status": "ok", "sessions": sessions, "count": len(sessions)}
    except Exception as e:
        logger.error(f"이전 분석 목록 조회 실패: {e}")
        return {"status": "error", "message": str(e), "sessions": []}


@router.get("/previous/{session_id}")
async def get_previous_result_detail(session_id: int):
    """특정 분석 세션의 전체 결과 조회"""
    try:
        resp = (
            db.table("pattern_analysis_sessions")
            .select("*")
            .eq("id", session_id)
            .single()
            .execute()
        )
        if not resp.data:
            return {"status": "not_found", "message": f"세션 {session_id}을 찾을 수 없습니다."}

        session = resp.data
        full_result = session.get("full_result", {})

        return {
            "status": "done",
            "session_id": session["id"],
            "created_at": session["created_at"],
            "preset": session.get("preset"),
            "stock_names": session.get("stock_names", {}),
            "params": session.get("params", {}),
            **full_result,
        }
    except Exception as e:
        logger.error(f"분석 세션 {session_id} 조회 실패: {e}")
        return {"status": "error", "message": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 종목 검색 (네이버 자동완성 or DB)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/search")
async def search_stock(req: SearchRequest):
    """종목명/코드 검색 (DB 우선, 네이버 폴백)"""
    keyword = req.keyword.strip()
    if not keyword:
        return {"results": []}

    results = []

    # 1차: DB 검색 (stock_list)
    try:
        if keyword.isdigit() and len(keyword) <= 6:
            resp = db.table("stock_list").select("code, name, market").ilike("code", f"%{keyword}%").limit(20).execute()
        else:
            resp = db.table("stock_list").select("code, name, market").ilike("name", f"%{keyword}%").limit(20).execute()

        if resp.data:
            for row in resp.data:
                results.append({"code": row["code"], "name": row["name"]})
    except Exception as e:
        logger.error(f"DB 검색 실패: {e}")

    # 2차: DB에 없으면 네이버 자동완성 폴백
    if not results:
        try:
            encoded = urllib.parse.quote(keyword, encoding="euc-kr")
            url = (
                f"https://ac.finance.naver.com/ac?"
                f"q={encoded}&q_enc=euc-kr&t_koreng=1&st=111&r_lt=111"
                f"&frm=stock&r_format=json&r_enc=utf-8&r_unicode=0&r_query=1"
            )
            req_obj = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Referer": "https://finance.naver.com/",
            })
            with urllib.request.urlopen(req_obj, timeout=5) as response:
                data = json.loads(response.read().decode("utf-8"))

            items = data.get("items", [[]])[0] if data.get("items") else []
            for item in items[:20]:
                if len(item) >= 2:
                    name = item[0][0] if isinstance(item[0], list) else str(item[0])
                    code = item[1][0] if isinstance(item[1], list) else str(item[1])
                    if len(code) == 6 and code.isdigit():
                        results.append({"code": code, "name": name})
        except Exception as e:
            logger.error(f"네이버 검색 실패: {e}")

    # 3차: 직접 코드 입력 (6자리)
    if not results and keyword.isdigit() and len(keyword) == 6:
        results.append({"code": keyword, "name": f"종목코드 {keyword}"})

    return {"results": results}
