"""
급상승 패턴 탐지기 — API 라우트
Pattern Surge Detector — API Routes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/api/pattern_routes.py

POST /api/pattern/analyze  — 분석 시작 (비동기)
GET  /api/pattern/progress  — 진행률 확인
GET  /api/pattern/result    — 결과 조회
POST /api/pattern/search    — 종목 검색 (네이버)
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Tuple
import asyncio
import logging
import traceback
import re
import requests
import urllib.parse

from app.engine.pattern_analyzer import (
    CandleDay,
    run_pattern_analysis,
)
from app.services.naver_stock import get_daily_candles_with_name

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
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 요청/응답 모델
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class AnalyzeRequest(BaseModel):
    codes: List[str]            # 종목코드 리스트 ["005930", "000660"]
    names: dict = {}            # 종목명 {"005930": "삼성전자"}
    period_days: int = 365      # 조회 기간 (일)
    pre_rise_days: int = 10     # 급상승 전 분석 구간
    rise_pct: float = 30.0      # 급상승 기준 (%)
    rise_window: int = 5        # 급상승 판단 기간 (거래일)


class SearchRequest(BaseModel):
    keyword: str                # 검색어


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 네이버 일봉 데이터 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_candles_for_code(code: str, period_days: int) -> Tuple[List[CandleDay], str]:
    """
    네이버 금융에서 일봉 데이터 조회 → CandleDay 리스트 + 종목명 변환
    기존 app/services/naver_stock.py의 get_daily_candles_with_name 활용 (sync → async 래핑)
    """
    try:
        # sync 함수를 스레드풀에서 실행 (이벤트루프 블로킹 방지)
        # 네이버 API 최대 600거래일 제한
        capped_count = min(period_days, 600)
        loop = asyncio.get_event_loop()
        raw_candles, stock_name = await loop.run_in_executor(
            None, lambda: get_daily_candles_with_name(code, count=capped_count)
        )

        if not raw_candles:
            logger.warning(f"[{code}] 네이버 일봉 데이터 없음")
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

        # 이미 naver_stock.py에서 날짜순 정렬되어 있지만 안전하게
        candles.sort(key=lambda c: c.date)
        logger.info(f"[{code}({stock_name})] 일봉 {len(candles)}개 변환 완료")
        return candles, stock_name

    except Exception as e:
        logger.error(f"[{code}] 일봉 조회 실패: {e}")
        return [], code


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 종목 검색 (네이버)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/search")
async def search_stock(req: SearchRequest):
    """
    종목 검색 — 키워드로 종목코드+종목명 반환
    네이버 금융 자동완성 API 활용
    """
    keyword = req.keyword.strip()
    if not keyword:
        return {"results": []}

    try:
        encoded = urllib.parse.quote(keyword)
        url = f"https://ac.finance.naver.com/ac?q={encoded}&q_enc=euc-kr&t_koreng=1&st=111&r_lt=111"

        loop = asyncio.get_event_loop()

        def _search():
            resp = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
            return resp.json()

        data = await loop.run_in_executor(None, _search)

        results = []
        items = data.get("items", [])

        # items는 [키워드매칭, 코드매칭, ...] 구조
        for group in items:
            for item in group:
                if len(item) >= 2:
                    name = item[0]
                    code = item[1]
                    # 6자리 숫자인 코드만 (ETF, 주식)
                    if re.match(r'^\d{6}$', code):
                        results.append({
                            "code": code,
                            "name": name,
                        })

        # 중복 제거
        seen = set()
        unique = []
        for r in results:
            if r["code"] not in seen:
                seen.add(r["code"])
                unique.append(r)

        return {"results": unique[:20]}

    except Exception as e:
        logger.error(f"종목 검색 실패: {e}")
        return {"results": []}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 분석 시작 (비동기 백그라운드)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/analyze")
async def start_analysis(req: AnalyzeRequest, background_tasks: BackgroundTasks):
    """
    분석 시작 — 백그라운드에서 실행
    """
    global _analysis_state

    if _analysis_state["running"]:
        raise HTTPException(status_code=409, detail="이미 분석이 진행 중입니다.")

    if not req.codes:
        raise HTTPException(status_code=400, detail="종목 코드를 1개 이상 입력하세요.")

    if len(req.codes) > 20:
        raise HTTPException(status_code=400, detail="최대 20개 종목까지 분석 가능합니다.")

    # 상태 초기화
    _analysis_state = {
        "running": True,
        "progress": 0,
        "message": "분석 준비 중...",
        "result": None,
        "error": None,
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
    """백그라운드 분석 태스크"""
    global _analysis_state

    try:
        # ── 데이터 수집 ──
        candles_by_code = {}
        total = len(codes)

        for idx, code in enumerate(codes):
            _analysis_state["progress"] = int((idx / total) * 30)
            _analysis_state["message"] = f"데이터 수집 중: {names.get(code, code)} ({idx+1}/{total})"

            candles, stock_name = await fetch_candles_for_code(code, period_days)
            if candles:
                candles_by_code[code] = candles
                # 네이버에서 가져온 종목명으로 보완 (프론트에서 미전달 시)
                if code not in names or not names[code]:
                    names[code] = stock_name
            else:
                logger.warning(f"[{code}] 데이터 없음 — 스킵")

            # API 부하 방지 딜레이
            await asyncio.sleep(0.3)

        if not candles_by_code:
            _analysis_state["running"] = False
            _analysis_state["error"] = "조회된 종목 데이터가 없습니다."
            _analysis_state["progress"] = 100
            return

        _analysis_state["progress"] = 35
        _analysis_state["message"] = f"{len(candles_by_code)}개 종목 데이터 수집 완료, 분석 시작..."

        # ── 분석 실행 ──
        def progress_cb(pct, msg):
            # 35~100% 범위로 매핑
            mapped_pct = 35 + int(pct * 0.65)
            _analysis_state["progress"] = min(mapped_pct, 100)
            _analysis_state["message"] = msg

        result = run_pattern_analysis(
            candles_by_code=candles_by_code,
            names=names,
            pre_days=pre_rise_days,
            rise_pct=rise_pct,
            rise_window=rise_window,
            progress_callback=progress_cb,
        )

        # 결과 저장
        _analysis_state["result"] = {
            "total_stocks": result.total_stocks,
            "total_surges": result.total_surges,
            "total_patterns": result.total_patterns,
            "clusters": result.clusters,
            "all_patterns": result.all_patterns,
            "recommendations": result.recommendations,
            "summary": result.summary,
            "raw_surges": result.raw_surges,
        }
        _analysis_state["progress"] = 100
        _analysis_state["message"] = "분석 완료!"
        _analysis_state["running"] = False

        logger.info(
            f"분석 완료: {result.total_surges}개 급상승, "
            f"{result.total_patterns}개 패턴, {len(result.clusters)}개 클러스터"
        )

    except Exception as e:
        logger.error(f"분석 실패: {e}\n{traceback.format_exc()}")
        _analysis_state["running"] = False
        _analysis_state["error"] = str(e)
        _analysis_state["progress"] = 100
        _analysis_state["message"] = f"분석 실패: {str(e)}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 진행률 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/progress")
async def get_progress():
    """분석 진행률 반환"""
    return {
        "running": _analysis_state["running"],
        "progress": _analysis_state["progress"],
        "message": _analysis_state["message"],
        "error": _analysis_state["error"],
        "has_result": _analysis_state["result"] is not None,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 결과 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/result")
async def get_result():
    """분석 결과 반환"""
    if _analysis_state["running"]:
        return {
            "status": "running",
            "progress": _analysis_state["progress"],
            "message": _analysis_state["message"],
        }

    if _analysis_state["error"]:
        return {
            "status": "error",
            "error": _analysis_state["error"],
        }

    if _analysis_state["result"] is None:
        return {
            "status": "idle",
            "message": "분석을 시작해주세요.",
        }

    return {
        "status": "done",
        **_analysis_state["result"],
    }
