"""
전종목 급상승 스캐너 — API 라우트
All-Stock Surge Scanner — API Routes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/api/surge_scanner_routes.py

전체 ~2,500개 종목을 자동 스캔하여 급상승 이력이 있는 종목을 발굴합니다.
특히 작전 세력의 급상승 패턴(단기 급등 + 거래량 폭증)을 탐지합니다.

POST /api/scanner/start     — 스캔 시작 (비동기)
GET  /api/scanner/progress   — 진행률 확인
GET  /api/scanner/result     — 결과 조회
POST /api/scanner/stop       — 스캔 중지
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict
import asyncio
import logging
import traceback
from datetime import datetime, timedelta

from app.engine.pattern_analyzer import CandleDay, detect_surges

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/scanner", tags=["scanner"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전역 상태 / Global State
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_scanner_state = {
    "running": False,
    "stop_requested": False,
    "progress": 0,
    "message": "",
    "scanned": 0,           # 스캔 완료 종목 수
    "total": 0,             # 전체 종목 수
    "found": 0,             # 급상승 발견 종목 수
    "result": None,
    "error": None,
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 요청 모델 / Request Model
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ScanRequest(BaseModel):
    market: str = "ALL"         # ALL / KOSPI / KOSDAQ
    period_days: int = 365      # 조회 기간 (일)
    rise_pct: float = 30.0      # 급상승 기준 (%)
    rise_window: int = 5        # 급상승 판단 기간 (거래일)
    min_volume_ratio: float = 2.0   # 최소 거래량 배율 (평소 대비)
    batch_size: int = 10        # 동시 처리 수 (API 부하 방지)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 네이버 일봉 조회 (단일 종목)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _fetch_candles(code: str, period_days: int) -> List[CandleDay]:
    """네이버에서 일봉 데이터 조회 → CandleDay 리스트"""
    try:
        from app.services.naver_stock import get_daily_candles_with_name
        capped = min(period_days, 600)
        loop = asyncio.get_event_loop()
        raw, _ = await loop.run_in_executor(
            None, lambda: get_daily_candles_with_name(code, count=capped)
        )
        if not raw:
            return []

        candles = []
        for item in raw:
            try:
                c = CandleDay(
                    date=str(item.get("date", "")),
                    open=float(item.get("open", 0)),
                    high=float(item.get("high", 0)),
                    low=float(item.get("low", 0)),
                    close=float(item.get("close", 0)),
                    volume=int(item.get("volume", 0)),
                )
                if c.close > 0:
                    candles.append(c)
            except (ValueError, TypeError):
                continue
        candles.sort(key=lambda c: c.date)
        return candles
    except Exception as e:
        logger.debug(f"[{code}] 일봉 조회 실패: {e}")
        return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 종목 리스트 조회 (stock_list DB)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_stock_list(market: str = "ALL") -> List[Dict]:
    """
    stock_list DB에서 전종목 리스트 조회
    market: ALL / KOSPI / KOSDAQ
    """
    try:
        from app.core.database import db
        query = db.table("stock_list").select("code, name, market").eq("is_active", True)
        if market == "KOSPI":
            query = query.eq("market", "KOSPI")
        elif market == "KOSDAQ":
            query = query.eq("market", "KOSDAQ")
        data = query.order("code").execute().data
        return data or []
    except Exception as e:
        logger.error(f"stock_list DB 조회 실패: {e}")
        return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 급상승 + 작전주 특성 분석
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _analyze_surge_detail(candles: List[CandleDay], surge, min_volume_ratio: float) -> Dict:
    """
    급상승 구간의 상세 분석 — 작전주 특성 판별
    - 거래량 폭증 정도
    - 급상승 후 급락 여부 (세력 이탈 징후)
    - 상승 전 매집 흔적 (저거래량 횡보)
    """
    si = surge.start_idx
    ei = surge.end_idx
    n = len(candles)

    # ── 1) 급상승 구간 거래량 분석 ──
    # 상승 직전 20일 평균 거래량
    pre_start = max(0, si - 20)
    pre_vols = [c.volume for c in candles[pre_start:si]]
    avg_pre_vol = sum(pre_vols) / len(pre_vols) if pre_vols else 1

    # 급상승 구간 평균 거래량
    surge_vols = [c.volume for c in candles[si:ei + 1]]
    avg_surge_vol = sum(surge_vols) / len(surge_vols) if surge_vols else 1
    max_surge_vol = max(surge_vols) if surge_vols else 0

    volume_ratio = round(avg_surge_vol / avg_pre_vol, 2) if avg_pre_vol > 0 else 0
    max_volume_ratio = round(max_surge_vol / avg_pre_vol, 2) if avg_pre_vol > 0 else 0

    # 거래량 기준 미달 시 제외
    if volume_ratio < min_volume_ratio:
        return None

    # ── 2) 급상승 후 급락 분석 (세력 이탈 징후) ──
    after_drop_pct = 0
    after_days_checked = 0
    current_price = candles[-1].close
    peak_price = surge.peak_price

    if ei + 1 < n:
        # 급상승 후 최대 20일간 하락 체크
        check_end = min(ei + 21, n)
        post_prices = [c.close for c in candles[ei + 1:check_end]]
        if post_prices:
            min_after = min(post_prices)
            after_drop_pct = round(((min_after - peak_price) / peak_price) * 100, 2)
            after_days_checked = len(post_prices)

    # 현재가 대비 고점 이격률
    from_peak_pct = round(((current_price - peak_price) / peak_price) * 100, 2) if peak_price > 0 else 0

    # ── 3) 매집 흔적 (급상승 전 저거래량 횡보) ──
    accumulation_score = 0
    if si >= 20:
        pre_20 = candles[si - 20:si]
        pre_20_returns = []
        for k in range(1, len(pre_20)):
            if pre_20[k - 1].close > 0:
                ret = abs((pre_20[k].close - pre_20[k - 1].close) / pre_20[k - 1].close * 100)
                pre_20_returns.append(ret)
        # 변동성 낮고 거래량 작으면 → 매집 흔적
        avg_volatility = sum(pre_20_returns) / len(pre_20_returns) if pre_20_returns else 0
        if avg_volatility < 2.0 and volume_ratio >= 3.0:
            accumulation_score = 3  # 강한 매집 의심
        elif avg_volatility < 3.0 and volume_ratio >= 2.0:
            accumulation_score = 2  # 매집 가능성
        elif volume_ratio >= 2.0:
            accumulation_score = 1  # 약한 징후

    # ── 4) 작전주 의심 점수 (0~100) ──
    manip_score = 0
    # 거래량 폭증 → 최대 30점
    manip_score += min(30, int(volume_ratio * 5))
    # 급상승 후 급락 → 최대 30점
    if after_drop_pct < -20:
        manip_score += 30
    elif after_drop_pct < -10:
        manip_score += 20
    elif after_drop_pct < -5:
        manip_score += 10
    # 매집 흔적 → 최대 20점
    manip_score += accumulation_score * 7
    # 단기 급등폭 → 최대 20점
    if surge.rise_pct >= 100:
        manip_score += 20
    elif surge.rise_pct >= 50:
        manip_score += 15
    elif surge.rise_pct >= 30:
        manip_score += 10

    manip_score = min(100, manip_score)

    # 작전주 판정
    if manip_score >= 70:
        manip_label = "🔴 세력 의심"
        manip_level = "high"
    elif manip_score >= 45:
        manip_label = "🟡 주의 필요"
        manip_level = "medium"
    else:
        manip_label = "🟢 일반 급등"
        manip_level = "low"

    return {
        "volume_ratio": volume_ratio,
        "max_volume_ratio": max_volume_ratio,
        "after_drop_pct": after_drop_pct,
        "from_peak_pct": from_peak_pct,
        "current_price": current_price,
        "accumulation_score": accumulation_score,
        "manip_score": manip_score,
        "manip_label": manip_label,
        "manip_level": manip_level,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 스캔 시작 / Start Scan
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/start")
async def start_scan(req: ScanRequest, background_tasks: BackgroundTasks):
    """전종목 급상승 스캔 시작"""
    global _scanner_state

    if _scanner_state["running"]:
        raise HTTPException(status_code=409, detail="이미 스캔이 진행 중입니다.")

    # stock_list에서 종목 조회
    stock_list = _get_stock_list(req.market)
    if not stock_list:
        raise HTTPException(
            status_code=400,
            detail="stock_list DB에 종목이 없습니다. 먼저 POST /api/stocks/update를 실행하세요."
        )

    # 상태 초기화
    _scanner_state = {
        "running": True,
        "stop_requested": False,
        "progress": 0,
        "message": f"스캔 준비 중... ({len(stock_list)}개 종목)",
        "scanned": 0,
        "total": len(stock_list),
        "found": 0,
        "result": None,
        "error": None,
    }

    background_tasks.add_task(
        _run_scan_task,
        stock_list,
        req.period_days,
        req.rise_pct,
        req.rise_window,
        req.min_volume_ratio,
        req.batch_size,
    )

    return {
        "status": "started",
        "message": f"{len(stock_list)}개 종목 스캔 시작 ({req.market})",
        "total": len(stock_list),
    }


async def _run_scan_task(
    stock_list: List[Dict],
    period_days: int,
    rise_pct: float,
    rise_window: int,
    min_volume_ratio: float,
    batch_size: int,
):
    """백그라운드 스캔 태스크"""
    global _scanner_state

    try:
        total = len(stock_list)
        all_results = []
        scanned = 0
        found = 0

        # 배치 처리 (batch_size개씩)
        for batch_start in range(0, total, batch_size):
            # 중지 요청 체크
            if _scanner_state["stop_requested"]:
                _scanner_state["message"] = f"사용자 중지 — {scanned}개 스캔, {found}개 발견"
                break

            batch = stock_list[batch_start:batch_start + batch_size]

            # 배치 내 종목 동시 처리
            tasks = []
            for stock in batch:
                tasks.append(_scan_single_stock(
                    stock["code"], stock["name"], stock.get("market", ""),
                    period_days, rise_pct, rise_window, min_volume_ratio
                ))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                scanned += 1
                if isinstance(r, dict) and r.get("surges"):
                    all_results.append(r)
                    found += 1

            # 진행률 업데이트
            pct = int((scanned / total) * 100)
            _scanner_state["progress"] = min(pct, 99)
            _scanner_state["scanned"] = scanned
            _scanner_state["found"] = found

            current_stock = batch[-1]["name"] if batch else ""
            _scanner_state["message"] = (
                f"스캔 중: {current_stock} ({scanned}/{total}) — "
                f"급상승 {found}개 발견"
            )

            # API 부하 방지 — 배치 간 딜레이
            await asyncio.sleep(0.5)

        # ── 결과 정리 ──
        # 작전주 의심 점수 높은 순 정렬
        all_results.sort(key=lambda r: r.get("top_manip_score", 0), reverse=True)

        # 통계
        total_surges = sum(len(r["surges"]) for r in all_results)
        high_manip = sum(1 for r in all_results if r.get("top_manip_level") == "high")
        med_manip = sum(1 for r in all_results if r.get("top_manip_level") == "medium")

        _scanner_state["result"] = {
            "stocks": all_results,
            "stats": {
                "total_scanned": scanned,
                "total_found": found,
                "total_surges": total_surges,
                "high_manip_count": high_manip,
                "medium_manip_count": med_manip,
                "scan_params": {
                    "period_days": period_days,
                    "rise_pct": rise_pct,
                    "rise_window": rise_window,
                    "min_volume_ratio": min_volume_ratio,
                },
            },
        }
        _scanner_state["progress"] = 100
        _scanner_state["message"] = f"스캔 완료! {scanned}개 스캔, {found}개 급상승 종목 발견"
        _scanner_state["running"] = False

        logger.info(
            f"스캔 완료: {scanned}개 종목 중 {found}개 급상승 발견, "
            f"총 {total_surges}건, 세력의심 {high_manip}건"
        )

    except Exception as e:
        logger.error(f"스캔 실패: {e}\n{traceback.format_exc()}")
        _scanner_state["running"] = False
        _scanner_state["error"] = str(e)
        _scanner_state["progress"] = 100
        _scanner_state["message"] = f"스캔 실패: {str(e)}"


async def _scan_single_stock(
    code: str, name: str, market: str,
    period_days: int, rise_pct: float, rise_window: int,
    min_volume_ratio: float
) -> Dict:
    """단일 종목 급상승 스캔"""
    try:
        candles = await _fetch_candles(code, period_days)
        if len(candles) < rise_window + 20:
            return {"code": code, "name": name, "surges": []}

        # 급상승 구간 탐지
        surges = detect_surges(candles, code, name, rise_pct, rise_window)
        if not surges:
            return {"code": code, "name": name, "surges": []}

        # 각 급상승의 상세 분석
        surge_details = []
        top_manip_score = 0
        top_manip_level = "low"

        for surge in surges:
            detail = _analyze_surge_detail(candles, surge, min_volume_ratio)
            if detail is None:
                continue  # 거래량 기준 미달

            surge_info = {
                "start_date": surge.start_date,
                "end_date": surge.end_date,
                "start_price": surge.start_price,
                "peak_price": surge.peak_price,
                "rise_pct": surge.rise_pct,
                "rise_days": surge.rise_days,
                **detail,
            }
            surge_details.append(surge_info)

            if detail["manip_score"] > top_manip_score:
                top_manip_score = detail["manip_score"]
                top_manip_level = detail["manip_level"]

        if not surge_details:
            return {"code": code, "name": name, "surges": []}

        # 최근 급상승 우선 정렬
        surge_details.sort(key=lambda s: s["start_date"], reverse=True)

        # 현재 정보
        current_price = candles[-1].close
        last_date = candles[-1].date

        return {
            "code": code,
            "name": name,
            "market": market,
            "current_price": current_price,
            "last_date": last_date,
            "surge_count": len(surge_details),
            "surges": surge_details,
            "top_manip_score": top_manip_score,
            "top_manip_level": top_manip_level,
            "top_manip_label": surge_details[0]["manip_label"] if surge_details else "",
            "latest_rise_pct": surge_details[0]["rise_pct"] if surge_details else 0,
            "latest_surge_date": surge_details[0]["start_date"] if surge_details else "",
            "latest_from_peak": surge_details[0]["from_peak_pct"] if surge_details else 0,
        }

    except Exception as e:
        logger.debug(f"[{code}] 스캔 실패: {e}")
        return {"code": code, "name": name, "surges": []}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 진행률 / Progress
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/progress")
async def get_scan_progress():
    """스캔 진행률 반환"""
    return {
        "running": _scanner_state["running"],
        "progress": _scanner_state["progress"],
        "message": _scanner_state["message"],
        "scanned": _scanner_state["scanned"],
        "total": _scanner_state["total"],
        "found": _scanner_state["found"],
        "error": _scanner_state["error"],
        "has_result": _scanner_state["result"] is not None,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 결과 조회 / Result
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/result")
async def get_scan_result():
    """스캔 결과 반환"""
    if _scanner_state["running"]:
        return {
            "status": "running",
            "progress": _scanner_state["progress"],
            "message": _scanner_state["message"],
        }

    if _scanner_state["error"]:
        return {
            "status": "error",
            "error": _scanner_state["error"],
        }

    if _scanner_state["result"] is None:
        return {
            "status": "idle",
            "message": "스캔을 시작해주세요.",
        }

    return {
        "status": "done",
        **_scanner_state["result"],
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 스캔 중지 / Stop Scan
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/stop")
async def stop_scan():
    """스캔 중지 요청"""
    if not _scanner_state["running"]:
        return {"status": "not_running", "message": "진행 중인 스캔이 없습니다."}

    _scanner_state["stop_requested"] = True
    return {"status": "stopping", "message": "스캔 중지 요청됨. 현재 배치 완료 후 중지됩니다."}
