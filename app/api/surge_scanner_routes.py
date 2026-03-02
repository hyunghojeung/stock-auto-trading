"""
전종목 급상승 스캐너 — API 라우트 (v2 DB 저장)
All-Stock Surge Scanner — API Routes (v2 with DB persistence)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/api/surge_scanner_routes.py

스캔 결과를 Supabase에 저장하여 재접속 시 자동 로드합니다.
네이버 차단 방지: batch_size=3, delay=1.5초, 재시도 3회, 차단감지 60초대기

POST /api/scanner/start      — 스캔 시작 (비동기)
GET  /api/scanner/progress    — 진행률 확인
GET  /api/scanner/result      — 현재 스캔 결과
GET  /api/scanner/latest      — DB에서 최근 스캔 결과 로드
POST /api/scanner/stop        — 스캔 중지
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict
import asyncio
import logging
import traceback
import json
from datetime import datetime, timedelta

from app.engine.pattern_analyzer import CandleDay, detect_surges
from app.engine.entry_strategies import evaluate_entry
from app.services.stock_pattern_collector import is_regular_stock

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
    "scanned": 0,
    "total": 0,
    "found": 0,
    "result": None,
    "error": None,
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 요청 모델 / Request Model
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ScanRequest(BaseModel):
    market: str = "ALL"
    period_days: int = 365
    rise_pct: float = 30.0
    rise_window: int = 5
    min_volume_ratio: float = 2.0
    batch_size: int = 3          # ← 최안전 설정 (동시 3개씩)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 네이버 일봉 조회 (단일 종목, 재시도 포함)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _fetch_candles_safe(code: str, period_days: int, max_retries: int = 3) -> List[CandleDay]:
    """
    네이버에서 일봉 데이터 조회 — 실패 시 재시도
    Fetch daily candles from Naver Finance with retry on failure
    """
    for attempt in range(max_retries + 1):
        try:
            from app.services.naver_stock import get_daily_candles_with_name
            capped = min(period_days, 600)
            loop = asyncio.get_event_loop()
            raw, _ = await loop.run_in_executor(
                None, lambda: get_daily_candles_with_name(code, count=capped)
            )
            if not raw:
                if attempt < max_retries:
                    await asyncio.sleep(3)  # 재시도 전 대기
                    continue
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
            if attempt < max_retries:
                logger.debug(f"[{code}] 재시도 {attempt+1}/{max_retries}: {e}")
                await asyncio.sleep(3 + attempt * 2)  # 점진적 대기 (3초, 5초)
            else:
                logger.debug(f"[{code}] 일봉 조회 최종 실패: {e}")
                return []
    return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 종목 리스트 조회 (stock_list DB)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_stock_list(market: str = "ALL") -> List[Dict]:
    """stock_list DB에서 전종목 리스트 조회 (페이지네이션 적용)
    ★ ETF/ETN/인버스/레버리지/우선주 자동 제외
    """
    try:
        from app.core.database import db
        all_data = []
        page_size = 1000
        offset = 0

        while True:
            query = db.table("stock_list").select("code, name, market") \
                .eq("is_active", True) \
                .eq("is_etf", False) \
                .eq("is_preferred", False)
            if market == "KOSPI":
                query = query.eq("market", "kospi")
            elif market == "KOSDAQ":
                query = query.eq("market", "kosdaq")
            result = query.order("code").range(offset, offset + page_size - 1).execute()

            if not result.data:
                break
            all_data.extend(result.data)
            if len(result.data) < page_size:
                break
            offset += page_size

        # ★ 2차 필터: 이름 기반 ETF/ETN/스팩/리츠/우선주 제거
        before_count = len(all_data)
        all_data = [s for s in all_data if is_regular_stock(s)]
        filtered_count = before_count - len(all_data)

        logger.info(f"stock_list 조회: {len(all_data)}개 ({market}) "
                    f"[DB필터 후 {before_count}개 → 이름필터로 {filtered_count}개 추가 제외]")
        return all_data
    except Exception as e:
        logger.error(f"stock_list DB 조회 실패: {e}")
        return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB 저장/로드 함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

INTERMEDIATE_SAVE_INTERVAL = 500  # 500개마다 중간 저장


def _create_scan_session(scan_params: Dict) -> Optional[int]:
    """스캔 세션 생성 (status=running) → session_id 반환"""
    try:
        from app.core.database import db
        session_data = {
            "market": scan_params.get("market", "ALL"),
            "period_days": scan_params.get("period_days", 365),
            "rise_pct": scan_params.get("rise_pct", 30.0),
            "rise_window": scan_params.get("rise_window", 5),
            "min_volume_ratio": scan_params.get("min_volume_ratio", 2.0),
            "total_scanned": 0,
            "total_found": 0,
            "total_surges": 0,
            "high_manip_count": 0,
            "medium_manip_count": 0,
            "status": "running",
        }
        resp = db.table("surge_scan_sessions").insert(session_data).execute()
        if not resp.data:
            return None
        session_id = resp.data[0]["id"]
        logger.info(f"스캔 세션 생성: session_id={session_id}")
        return session_id
    except Exception as e:
        logger.error(f"세션 생성 실패: {e}")
        return None


def _save_intermediate_stocks(session_id: int, stocks: List[Dict]) -> int:
    """중간 결과 DB 저장 (종목 배치 추가) → 저장된 개수 반환"""
    if not stocks or not session_id:
        return 0
    try:
        from app.core.database import db
        batch = []
        saved = 0
        for stock in stocks:
            row = {
                "session_id": session_id,
                "code": stock.get("code", ""),
                "name": stock.get("name", ""),
                "market": stock.get("market", ""),
                "current_price": stock.get("current_price", 0),
                "last_date": stock.get("last_date", ""),
                "surge_count": stock.get("surge_count", 0),
                "top_manip_score": stock.get("top_manip_score", 0),
                "top_manip_level": stock.get("top_manip_level", "low"),
                "top_manip_label": stock.get("top_manip_label", ""),
                "latest_rise_pct": stock.get("latest_rise_pct", 0),
                "latest_surge_date": stock.get("latest_surge_date", ""),
                "latest_from_peak": stock.get("latest_from_peak", 0),
                "surges_json": json.dumps(stock.get("surges", []), ensure_ascii=False),
            }
            batch.append(row)
            if len(batch) >= 50:
                db.table("surge_scan_stocks").insert(batch).execute()
                saved += len(batch)
                batch = []
        if batch:
            db.table("surge_scan_stocks").insert(batch).execute()
            saved += len(batch)
        logger.info(f"중간 저장: session_id={session_id}, {saved}개 종목 추가")
        return saved
    except Exception as e:
        logger.error(f"중간 저장 실패: {e}")
        return 0


def _finalize_scan_session(session_id: int, stats: Dict):
    """스캔 완료 시 세션 통계 업데이트 + status=done"""
    if not session_id:
        return
    try:
        from app.core.database import db
        db.table("surge_scan_sessions").update({
            "total_scanned": stats.get("total_scanned", 0),
            "total_found": stats.get("total_found", 0),
            "total_surges": stats.get("total_surges", 0),
            "high_manip_count": stats.get("high_manip_count", 0),
            "medium_manip_count": stats.get("medium_manip_count", 0),
            "status": "done",
        }).eq("id", session_id).execute()
        logger.info(f"세션 완료 처리: session_id={session_id}")
    except Exception as e:
        logger.error(f"세션 완료 처리 실패: {e}")


def _save_scan_to_db(scan_params: Dict, stocks: List[Dict], stats: Dict) -> Optional[int]:
    """스캔 결과를 Supabase에 저장 → session_id 반환 (호환용 유지)"""
    try:
        from app.core.database import db

        # 1) 세션 생성
        session_data = {
            "market": scan_params.get("market", "ALL"),
            "period_days": scan_params.get("period_days", 365),
            "rise_pct": scan_params.get("rise_pct", 30.0),
            "rise_window": scan_params.get("rise_window", 5),
            "min_volume_ratio": scan_params.get("min_volume_ratio", 2.0),
            "total_scanned": stats.get("total_scanned", 0),
            "total_found": stats.get("total_found", 0),
            "total_surges": stats.get("total_surges", 0),
            "high_manip_count": stats.get("high_manip_count", 0),
            "medium_manip_count": stats.get("medium_manip_count", 0),
            "status": "done",
        }
        resp = db.table("surge_scan_sessions").insert(session_data).execute()
        if not resp.data:
            logger.error("세션 저장 실패: 응답 데이터 없음")
            return None
        session_id = resp.data[0]["id"]

        # 2) 종목별 결과 저장 (50개씩 배치)
        batch = []
        for stock in stocks:
            row = {
                "session_id": session_id,
                "code": stock.get("code", ""),
                "name": stock.get("name", ""),
                "market": stock.get("market", ""),
                "current_price": stock.get("current_price", 0),
                "last_date": stock.get("last_date", ""),
                "surge_count": stock.get("surge_count", 0),
                "top_manip_score": stock.get("top_manip_score", 0),
                "top_manip_level": stock.get("top_manip_level", "low"),
                "top_manip_label": stock.get("top_manip_label", ""),
                "latest_rise_pct": stock.get("latest_rise_pct", 0),
                "latest_surge_date": stock.get("latest_surge_date", ""),
                "latest_from_peak": stock.get("latest_from_peak", 0),
                "surges_json": json.dumps(stock.get("surges", []), ensure_ascii=False),
            }
            batch.append(row)

            if len(batch) >= 50:
                db.table("surge_scan_stocks").insert(batch).execute()
                batch = []

        if batch:
            db.table("surge_scan_stocks").insert(batch).execute()

        logger.info(f"스캔 결과 DB 저장 완료: session_id={session_id}, {len(stocks)}개 종목")
        return session_id

    except Exception as e:
        logger.error(f"스캔 결과 DB 저장 실패: {e}\n{traceback.format_exc()}")
        return None


def _load_latest_scan_from_db() -> Optional[Dict]:
    """DB에서 가장 최근 스캔 결과 로드"""
    try:
        from app.core.database import db

        # 최근 세션 조회
        resp = db.table("surge_scan_sessions") \
            .select("*") \
            .eq("status", "done") \
            .order("scan_date", desc=True) \
            .limit(1) \
            .execute()

        if not resp.data:
            return None

        session = resp.data[0]
        session_id = session["id"]

        # 해당 세션의 종목 결과 로드 (페이지네이션)
        all_stock_rows = []
        page_size = 1000
        offset = 0
        while True:
            stocks_resp = db.table("surge_scan_stocks") \
                .select("*") \
                .eq("session_id", session_id) \
                .order("top_manip_score", desc=True) \
                .range(offset, offset + page_size - 1) \
                .execute()
            if not stocks_resp.data:
                break
            all_stock_rows.extend(stocks_resp.data)
            if len(stocks_resp.data) < page_size:
                break
            offset += page_size

        stocks = []
        for row in all_stock_rows:
            surges = []
            try:
                surges = json.loads(row.get("surges_json", "[]"))
            except:
                pass

            stocks.append({
                "code": row["code"],
                "name": row["name"],
                "market": row.get("market", ""),
                "current_price": row.get("current_price", 0),
                "last_date": row.get("last_date", ""),
                "surge_count": row.get("surge_count", 0),
                "top_manip_score": row.get("top_manip_score", 0),
                "top_manip_level": row.get("top_manip_level", "low"),
                "top_manip_label": row.get("top_manip_label", ""),
                "latest_rise_pct": row.get("latest_rise_pct", 0),
                "latest_surge_date": row.get("latest_surge_date", ""),
                "latest_from_peak": row.get("latest_from_peak", 0),
                "surges": surges,
            })

        return {
            "stocks": stocks,
            "stats": {
                "total_scanned": session.get("total_scanned", 0),
                "total_found": session.get("total_found", 0),
                "total_surges": session.get("total_surges", 0),
                "high_manip_count": session.get("high_manip_count", 0),
                "medium_manip_count": session.get("medium_manip_count", 0),
                "scan_params": {
                    "period_days": session.get("period_days", 365),
                    "rise_pct": session.get("rise_pct", 30.0),
                    "rise_window": session.get("rise_window", 5),
                    "min_volume_ratio": session.get("min_volume_ratio", 2.0),
                },
            },
            "scan_date": session.get("scan_date", ""),
            "market": session.get("market", "ALL"),
        }

    except Exception as e:
        logger.error(f"DB 로드 실패: {e}")
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 급상승 + 작전주 특성 분석
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _analyze_surge_detail(candles: List[CandleDay], surge, min_volume_ratio: float) -> Dict:
    """급상승 구간의 상세 분석 — 작전주 특성 판별"""
    si = surge.start_idx
    ei = surge.end_idx
    n = len(candles)

    # 1) 급상승 구간 거래량 분석
    pre_start = max(0, si - 20)
    pre_vols = [c.volume for c in candles[pre_start:si]]
    avg_pre_vol = sum(pre_vols) / len(pre_vols) if pre_vols else 1

    surge_vols = [c.volume for c in candles[si:ei + 1]]
    avg_surge_vol = sum(surge_vols) / len(surge_vols) if surge_vols else 1
    max_surge_vol = max(surge_vols) if surge_vols else 0

    volume_ratio = round(avg_surge_vol / avg_pre_vol, 2) if avg_pre_vol > 0 else 0
    max_volume_ratio = round(max_surge_vol / avg_pre_vol, 2) if avg_pre_vol > 0 else 0

    if volume_ratio < min_volume_ratio:
        return None

    # 2) 급상승 후 급락 분석
    after_drop_pct = 0
    current_price = candles[-1].close
    peak_price = surge.peak_price

    if ei + 1 < n:
        check_end = min(ei + 21, n)
        post_prices = [c.close for c in candles[ei + 1:check_end]]
        if post_prices:
            min_after = min(post_prices)
            after_drop_pct = round(((min_after - peak_price) / peak_price) * 100, 2)

    from_peak_pct = round(((current_price - peak_price) / peak_price) * 100, 2) if peak_price > 0 else 0

    # 3) 매집 흔적
    accumulation_score = 0
    if si >= 20:
        pre_20 = candles[si - 20:si]
        pre_20_returns = []
        for k in range(1, len(pre_20)):
            if pre_20[k - 1].close > 0:
                ret = abs((pre_20[k].close - pre_20[k - 1].close) / pre_20[k - 1].close * 100)
                pre_20_returns.append(ret)
        avg_volatility = sum(pre_20_returns) / len(pre_20_returns) if pre_20_returns else 0
        if avg_volatility < 2.0 and volume_ratio >= 3.0:
            accumulation_score = 3
        elif avg_volatility < 3.0 and volume_ratio >= 2.0:
            accumulation_score = 2
        elif volume_ratio >= 2.0:
            accumulation_score = 1

    # 4) 작전주 의심 점수
    manip_score = 0
    manip_score += min(30, int(volume_ratio * 5))
    if after_drop_pct < -20:
        manip_score += 30
    elif after_drop_pct < -10:
        manip_score += 20
    elif after_drop_pct < -5:
        manip_score += 10
    manip_score += accumulation_score * 7
    if surge.rise_pct >= 100:
        manip_score += 20
    elif surge.rise_pct >= 50:
        manip_score += 15
    elif surge.rise_pct >= 30:
        manip_score += 10
    manip_score = min(100, manip_score)

    if manip_score >= 70:
        manip_label, manip_level = "🔴 세력 의심", "high"
    elif manip_score >= 45:
        manip_label, manip_level = "🟡 주의 필요", "medium"
    else:
        manip_label, manip_level = "🟢 일반 급등", "low"

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

    stock_list = _get_stock_list(req.market)
    if not stock_list:
        raise HTTPException(
            status_code=400,
            detail="stock_list DB에 종목이 없습니다. 먼저 POST /api/stocks/update를 실행하세요."
        )

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
        req.market,
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
    market: str,
    period_days: int,
    rise_pct: float,
    rise_window: int,
    min_volume_ratio: float,
    batch_size: int,
):
    """백그라운드 스캔 태스크 (500개마다 중간 저장)"""
    global _scanner_state

    try:
        total = len(stock_list)
        all_results = []
        scanned = 0
        found = 0
        deactivated_total = 0  # ★ 비활성화된 종목 수
        consecutive_failures = 0

        # ── 세션 생성 (status=running) ──
        scan_params = {
            "market": market,
            "period_days": period_days,
            "rise_pct": rise_pct,
            "rise_window": rise_window,
            "min_volume_ratio": min_volume_ratio,
        }
        session_id = _create_scan_session(scan_params)
        unsaved_results = []       # 아직 DB에 저장 안 된 종목
        last_save_scanned = 0      # 마지막 중간 저장 시점

        for batch_start in range(0, total, batch_size):
            if _scanner_state["stop_requested"]:
                _scanner_state["message"] = f"사용자 중지 — {scanned}개 스캔, {found}개 발견"
                break

            batch = stock_list[batch_start:batch_start + batch_size]

            tasks = []
            for stock in batch:
                tasks.append(_scan_single_stock(
                    stock["code"], stock["name"], stock.get("market", ""),
                    period_days, rise_pct, rise_window, min_volume_ratio
                ))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_failures = 0
            deactivated_in_batch = 0
            for r in results:
                scanned += 1
                if isinstance(r, dict) and r.get("surges"):
                    all_results.append(r)
                    unsaved_results.append(r)
                    found += 1
                elif isinstance(r, dict) and r.get("deactivated"):
                    deactivated_in_batch += 1
                    deactivated_total += 1
                elif isinstance(r, Exception):
                    batch_failures += 1

            # ── 네이버 차단 감지 ──
            if batch_failures == len(batch) and len(batch) > 1:
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    _scanner_state["message"] = f"⚠️ 네이버 차단 감지 — 60초 대기 중... ({scanned}/{total})"
                    await asyncio.sleep(60)
                    consecutive_failures = 0
                else:
                    await asyncio.sleep(10)
            else:
                consecutive_failures = 0

            # ── 500개마다 중간 저장 ──
            if scanned - last_save_scanned >= INTERMEDIATE_SAVE_INTERVAL:
                if session_id and unsaved_results:
                    saved = _save_intermediate_stocks(session_id, unsaved_results)
                    logger.info(f"중간 저장 완료: {scanned}/{total} 스캔, {saved}개 종목 저장")
                    unsaved_results = []
                last_save_scanned = scanned

                # 메모리에도 중간 결과 반영 (프론트에서 조회 가능)
                _scanner_state["result"] = {
                    "stocks": sorted(all_results, key=lambda r: r.get("top_manip_score", 0), reverse=True),
                    "stats": {
                        "total_scanned": scanned,
                        "total_found": found,
                        "total_surges": sum(len(r["surges"]) for r in all_results),
                        "high_manip_count": sum(1 for r in all_results if r.get("top_manip_level") == "high"),
                        "medium_manip_count": sum(1 for r in all_results if r.get("top_manip_level") == "medium"),
                        "scan_params": scan_params,
                    },
                    "scan_date": datetime.now().isoformat(),
                    "market": market,
                    "partial": True,  # 아직 진행 중 표시
                }

            # 진행률 업데이트
            pct = int((scanned / total) * 100)
            _scanner_state["progress"] = min(pct, 99)
            _scanner_state["scanned"] = scanned
            _scanner_state["found"] = found

            current_stock = batch[-1]["name"] if batch else ""
            _scanner_state["message"] = (
                f"스캔 중: {current_stock} ({scanned}/{total}) — "
                f"급상승 {found}개 발견"
                + (f", {deactivated_total}개 비활성화" if deactivated_total else "")
            )

            # ── 안전 딜레이: 1.5초 (네이버 차단 방지) ──
            await asyncio.sleep(1.5)

        # ── 결과 정리 ──
        all_results.sort(key=lambda r: r.get("top_manip_score", 0), reverse=True)

        total_surges = sum(len(r["surges"]) for r in all_results)
        high_manip = sum(1 for r in all_results if r.get("top_manip_level") == "high")
        med_manip = sum(1 for r in all_results if r.get("top_manip_level") == "medium")

        # ★ 진입 시그널 통계
        entry_signal_count = sum(
            1 for r in all_results
            if r.get("entry_signals") and r["entry_signals"].get("should_buy")
        )

        stats = {
            "total_scanned": scanned,
            "total_found": found,
            "total_surges": total_surges,
            "deactivated_count": deactivated_total,  # ★ 비활성화 종목 수
            "high_manip_count": high_manip,
            "medium_manip_count": med_manip,
            "entry_signal_count": entry_signal_count,  # ★ 진입 시그널 종목 수
            "scan_params": scan_params,
        }

        # ── 남은 미저장 종목 DB 저장 ──
        if session_id:
            if unsaved_results:
                _save_intermediate_stocks(session_id, unsaved_results)
            _finalize_scan_session(session_id, stats)
            logger.info(f"스캔 결과 DB 최종 저장 완료 (session_id={session_id})")
        else:
            # session 생성 실패 시 → 기존 방식으로 한번에 저장
            fallback_id = _save_scan_to_db(scan_params, all_results, stats)
            if fallback_id:
                logger.info(f"폴백 저장 완료 (session_id={fallback_id})")
            else:
                logger.warning("스캔 결과 DB 저장 실패 — 메모리에만 보관")

        _scanner_state["result"] = {
            "stocks": all_results,
            "stats": stats,
            "scan_date": datetime.now().isoformat(),
            "market": market,
        }
        _scanner_state["progress"] = 100
        deact_msg = f", {deactivated_total}개 비활성화" if deactivated_total else ""
        _scanner_state["message"] = f"스캔 완료! {scanned}개 스캔, {found}개 급상승 종목 발견{deact_msg}"
        _scanner_state["running"] = False

        logger.info(
            f"스캔 완료: {scanned}개 종목 중 {found}개 급상승 발견, "
            f"총 {total_surges}건, 세력의심 {high_manip}건"
            + (f", 비활성화 {deactivated_total}건" if deactivated_total else "")
        )

    except Exception as e:
        logger.error(f"스캔 실패: {e}\n{traceback.format_exc()}")
        _scanner_state["running"] = False
        _scanner_state["error"] = str(e)
        _scanner_state["progress"] = 100
        _scanner_state["message"] = f"스캔 실패: {str(e)}"


async def _deactivate_stock(code: str, name: str, reason: str):
    """거래정지/상장폐지 종목 자동 비활성화
    Auto-deactivate delisted or suspended stocks"""
    try:
        from app.core.database import db
        db.table("stock_list").update({"is_active": False}).eq("code", code).execute()
        logger.info(f"★ 종목 비활성화: {name}({code}) — {reason}")
    except Exception as e:
        logger.debug(f"비활성화 실패 {code}: {e}")


async def _scan_single_stock(
    code: str, name: str, market: str,
    period_days: int, rise_pct: float, rise_window: int,
    min_volume_ratio: float
) -> Dict:
    """단일 종목 급상승 스캔"""
    try:
        candles = await _fetch_candles_safe(code, period_days)

        # ★ 캔들 0개 = 거래정지/상장폐지 → 자동 비활성화
        if len(candles) == 0:
            await _deactivate_stock(code, name, "캔들 데이터 0개 (거래정지/상장폐지 추정)")
            return {"code": code, "name": name, "surges": [], "deactivated": True}

        if len(candles) < rise_window + 20:
            return {"code": code, "name": name, "surges": []}

        surges = detect_surges(candles, code, name, rise_pct, rise_window)
        if not surges:
            return {"code": code, "name": name, "surges": []}

        surge_details = []
        top_manip_score = 0
        top_manip_level = "low"

        for surge in surges:
            detail = _analyze_surge_detail(candles, surge, min_volume_ratio)
            if detail is None:
                continue

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

        surge_details.sort(key=lambda s: s["start_date"], reverse=True)
        current_price = candles[-1].close
        last_date = candles[-1].date

        # ★ 진입 전략 평가 (OBV + VCP + 부분 DTW)
        entry_result = None
        try:
            candle_dicts = [
                {"date": c.date, "open": c.open, "high": c.high,
                 "low": c.low, "close": c.close, "volume": c.volume}
                for c in candles
            ]
            entry_result = evaluate_entry(candle_dicts, clusters=[], strategy_config=None)
        except Exception as ee:
            logger.debug(f"[{code}] 진입전략 평가 실패: {ee}")

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
            "entry_signals": entry_result if entry_result else None,
        }

    except Exception as e:
        logger.debug(f"[{code}] 스캔 실패: {e}")
        return {"code": code, "name": name, "surges": []}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 진행률 / Progress
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/progress")
async def get_scan_progress():
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
# 현재 스캔 결과 / Current Result (메모리)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/result")
async def get_scan_result():
    """현재 세션의 스캔 결과 반환 (메모리)"""
    if _scanner_state["running"]:
        return {"status": "running", "progress": _scanner_state["progress"]}

    if _scanner_state["error"]:
        return {"status": "error", "error": _scanner_state["error"]}

    if _scanner_state["result"] is None:
        return {"status": "idle", "message": "스캔을 시작해주세요."}

    return {"status": "done", **_scanner_state["result"]}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB에서 최근 스캔 결과 로드 / Load Latest from DB
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/latest")
async def get_latest_scan():
    """
    DB에 저장된 가장 최근 스캔 결과를 로드합니다.
    재접속 시 자동으로 호출하여 이전 스캔 결과를 복원합니다.
    """
    # 1) 먼저 메모리에 있으면 메모리 결과 반환
    if _scanner_state["result"] is not None:
        return {
            "status": "done",
            "source": "memory",
            **_scanner_state["result"],
        }

    # 2) 메모리에 없으면 DB에서 로드
    data = _load_latest_scan_from_db()
    if data is None:
        return {
            "status": "empty",
            "message": "저장된 스캔 결과가 없습니다. 스캔을 시작해주세요.",
        }

    # 메모리에도 복원
    _scanner_state["result"] = data

    return {
        "status": "done",
        "source": "db",
        **data,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 스캔 중지 / Stop Scan
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/stop")
async def stop_scan():
    if not _scanner_state["running"]:
        return {"status": "not_running", "message": "진행 중인 스캔이 없습니다."}

    _scanner_state["stop_requested"] = True
    return {"status": "stopping", "message": "스캔 중지 요청됨. 현재 배치 완료 후 중지됩니다."}
