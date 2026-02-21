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
    종목 검색 — stock_list DB 조회 (전종목 ~2500개)
    1) 6자리 숫자 → 코드 직접 매칭
    2) 텍스트 → DB에서 종목명/코드 LIKE 검색
    3) DB 실패 시 → fallback 리스트 검색
    """
    keyword = req.keyword.strip()
    if not keyword:
        return {"results": []}

    # ── 1) 6자리 코드 직접 입력 ──
    if re.match(r'^\d{6}$', keyword):
        try:
            from app.core.database import db
            data = db.table("stock_list").select("code, name").eq("code", keyword).eq("is_active", True).execute().data
            if data:
                return {"results": [{"code": data[0]["code"], "name": data[0]["name"]}]}
        except Exception:
            pass
        # DB에 없으면 네이버에서 조회
        try:
            loop = asyncio.get_event_loop()
            _, stock_name = await loop.run_in_executor(
                None, lambda: get_daily_candles_with_name(keyword, count=1)
            )
            if stock_name and stock_name != keyword:
                return {"results": [{"code": keyword, "name": stock_name}]}
            else:
                return {"results": [{"code": keyword, "name": keyword}]}
        except Exception:
            return {"results": [{"code": keyword, "name": keyword}]}

    # ── 2) 텍스트 검색 → stock_list DB ──
    try:
        from app.core.database import db
        data = db.table("stock_list").select("code, name, market").eq("is_active", True).ilike("name", f"%{keyword}%").limit(20).execute().data
        if data:
            return {"results": [{"code": s["code"], "name": s["name"]} for s in data]}
    except Exception as e:
        logger.debug(f"stock_list DB 검색 실패: {e}")

    # ── 3) DB 실패 시 fallback ──
    query_upper = keyword.upper()
    results = []
    for s in _FALLBACK_STOCKS:
        if query_upper in s["name"].upper() or query_upper in s["code"]:
            results.append({"code": s["code"], "name": s["name"]})
    return {"results": results[:20]}


# 대표종목 리스트 (네이버 자동완성 API가 Railway에서 차단되어 fallback용)
_FALLBACK_STOCKS = [
    {"code": "005930", "name": "삼성전자"},
    {"code": "000660", "name": "SK하이닉스"},
    {"code": "373220", "name": "LG에너지솔루션"},
    {"code": "207940", "name": "삼성바이오로직스"},
    {"code": "005380", "name": "현대차"},
    {"code": "006400", "name": "삼성SDI"},
    {"code": "035420", "name": "NAVER"},
    {"code": "000270", "name": "기아"},
    {"code": "068270", "name": "셀트리온"},
    {"code": "035720", "name": "카카오"},
    {"code": "051910", "name": "LG화학"},
    {"code": "105560", "name": "KB금융"},
    {"code": "055550", "name": "신한지주"},
    {"code": "003670", "name": "포스코퓨처엠"},
    {"code": "096770", "name": "SK이노베이션"},
    {"code": "028260", "name": "삼성물산"},
    {"code": "012330", "name": "현대모비스"},
    {"code": "066570", "name": "LG전자"},
    {"code": "003550", "name": "LG"},
    {"code": "034730", "name": "SK"},
    {"code": "015760", "name": "한국전력"},
    {"code": "032830", "name": "삼성생명"},
    {"code": "011200", "name": "HMM"},
    {"code": "010130", "name": "고려아연"},
    {"code": "033780", "name": "KT&G"},
    {"code": "009150", "name": "삼성전기"},
    {"code": "018260", "name": "삼성에스디에스"},
    {"code": "086790", "name": "하나금융지주"},
    {"code": "316140", "name": "우리금융지주"},
    {"code": "017670", "name": "SK텔레콤"},
    {"code": "030200", "name": "KT"},
    {"code": "010950", "name": "S-Oil"},
    {"code": "247540", "name": "에코프로비엠"},
    {"code": "086520", "name": "에코프로"},
    {"code": "377300", "name": "카카오페이"},
    {"code": "259960", "name": "크래프톤"},
    {"code": "352820", "name": "하이브"},
    {"code": "263750", "name": "펄어비스"},
    {"code": "112040", "name": "위메이드"},
    {"code": "041510", "name": "에스엠"},
    {"code": "293490", "name": "카카오게임즈"},
    {"code": "036570", "name": "엔씨소프트"},
    {"code": "251270", "name": "넷마블"},
    {"code": "090430", "name": "아모레퍼시픽"},
    {"code": "005490", "name": "POSCO홀딩스"},
    {"code": "042700", "name": "한미반도체"},
    {"code": "196170", "name": "알테오젠"},
    {"code": "000100", "name": "유한양행"},
    {"code": "004020", "name": "현대제철"},
    {"code": "009540", "name": "HD한국조선해양"},
    {"code": "267260", "name": "HD현대일렉트릭"},
    {"code": "003490", "name": "대한항공"},
    {"code": "180640", "name": "한진칼"},
    {"code": "000810", "name": "삼성화재"},
    {"code": "036460", "name": "한국가스공사"},
    {"code": "161390", "name": "한국타이어앤테크놀로지"},
]


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
