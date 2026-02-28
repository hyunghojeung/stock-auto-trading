"""눌림목 패턴 라이브러리 API / Dip Pattern Library Routes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- 패턴 목록 조회 / 토글
- 종목별 패턴 평가 (단건 테스트)
- ★ 전종목 눌림목 스캔 (일괄)
- 시장 상태 조회

파일경로: app/api/pattern_lib_routes.py
"""

import asyncio
import logging
import traceback
from datetime import datetime
from typing import Dict, List

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/pattern-library", tags=["pattern-library"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전종목 스캔 상태 / Scan State
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_dip_scan_state: Dict = {
    "running": False,
    "progress": 0,
    "scanned": 0,
    "total": 0,
    "found": 0,
    "deactivated": 0,
    "message": "",
    "result": None,
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 패턴 목록 / Pattern List
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.get("/list")
async def get_pattern_list():
    """패턴 정의 목록 조회"""
    try:
        from app.core.database import db
        resp = db.table("pattern_definitions") \
            .select("*") \
            .order("pattern_code") \
            .execute()
        return {"success": True, "patterns": resp.data or []}
    except Exception as e:
        logger.error(f"[패턴 목록] 오류: {e}")
        raise HTTPException(500, f"패턴 목록 조회 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 패턴 토글 / Pattern Toggle
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.put("/{pattern_code}/toggle")
async def toggle_pattern(pattern_code: str):
    """패턴 활성/비활성 토글"""
    try:
        from app.core.database import db
        resp = db.table("pattern_definitions") \
            .select("is_active") \
            .eq("pattern_code", pattern_code) \
            .single() \
            .execute()
        if not resp.data:
            raise HTTPException(404, f"패턴 {pattern_code} 없음")

        current = resp.data["is_active"]
        new_state = not current
        db.table("pattern_definitions") \
            .update({"is_active": new_state}) \
            .eq("pattern_code", pattern_code) \
            .execute()
        logger.info(f"[패턴 토글] {pattern_code}: {current} → {new_state}")
        return {"success": True, "pattern_code": pattern_code, "is_active": new_state}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[패턴 토글] 오류: {e}")
        raise HTTPException(500, f"토글 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 종목 패턴 평가 (단건 테스트)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class EvaluateRequest(BaseModel):
    stock_code: str
    require_gates: bool = True


@router.post("/evaluate")
async def evaluate_stock(req: EvaluateRequest):
    """특정 종목에 눌림목 패턴 라이브러리 적용 (테스트)"""
    try:
        from app.services.naver_stock import get_daily_candles_naver
        from app.engine.pattern_library import evaluate_dip_patterns, get_active_patterns_from_db
        from app.core.database import db

        # ★ 종목명 → 종목코드 변환 (6자리 숫자가 아니면 DB에서 검색)
        stock_code = req.stock_code.strip()
        stock_name = stock_code

        if not (len(stock_code) == 6 and stock_code.isdigit()):
            try:
                resp = db.table("stock_list").select("code, name") \
                    .eq("is_active", True) \
                    .ilike("name", f"%{stock_code}%") \
                    .limit(1).execute()
                if resp.data:
                    stock_name = resp.data[0]["name"]
                    stock_code = resp.data[0]["code"]
                    logger.info(f"[패턴 평가] 종목명 변환: {req.stock_code} → {stock_name}({stock_code})")
                else:
                    raise HTTPException(400, f"종목 '{req.stock_code}'을(를) DB에서 찾을 수 없습니다")
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(400, f"종목 검색 실패: {e}")

        candles = get_daily_candles_naver(stock_code, count=60)
        if not candles or len(candles) < 22:
            if not candles or len(candles) == 0:
                try:
                    db.table("stock_list").update({"is_active": False}).eq("code", stock_code).execute()
                    logger.info(f"★ 종목 비활성화: {stock_name}({stock_code}) — 캔들 데이터 0개")
                except Exception:
                    pass
            raise HTTPException(400, f"종목 {stock_name}({stock_code}): 캔들 데이터 부족 ({len(candles) if candles else 0}일)")

        active = get_active_patterns_from_db()
        result = evaluate_dip_patterns(candles, active_patterns=active, require_gates=req.require_gates)

        return {
            "success": True,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "candle_count": len(candles),
            "active_patterns": active,
            "evaluation": result,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[패턴 평가] 오류: {e}")
        raise HTTPException(500, f"평가 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 전종목 눌림목 스캔 / Full Stock Dip Scan
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class ScanRequest(BaseModel):
    require_gates: bool = True
    min_score: int = 55


@router.post("/scan-start")
async def start_dip_scan(req: ScanRequest, bg: BackgroundTasks):
    """전종목 눌림목 스캔 시작"""
    if _dip_scan_state["running"]:
        return {"success": False, "message": "이미 스캔 진행 중", "progress": _dip_scan_state["progress"]}

    bg.add_task(_run_dip_scan, req.require_gates, req.min_score)
    return {"success": True, "message": "전종목 눌림목 스캔 시작"}


@router.get("/scan-progress")
async def get_dip_scan_progress():
    """스캔 진행률 조회"""
    return {
        "running": _dip_scan_state["running"],
        "progress": _dip_scan_state["progress"],
        "scanned": _dip_scan_state["scanned"],
        "total": _dip_scan_state["total"],
        "found": _dip_scan_state["found"],
        "deactivated": _dip_scan_state["deactivated"],
        "message": _dip_scan_state["message"],
    }


@router.get("/scan-result")
async def get_dip_scan_result():
    """스캔 결과 조회"""
    if _dip_scan_state["running"]:
        return {"status": "running", "progress": _dip_scan_state["progress"]}
    if _dip_scan_state["result"] is None:
        return {"status": "no_data", "message": "스캔 결과 없음. 먼저 스캔을 실행하세요."}
    return {"status": "done", **_dip_scan_state["result"]}


@router.post("/scan-stop")
async def stop_dip_scan():
    """스캔 중지"""
    if _dip_scan_state["running"]:
        _dip_scan_state["running"] = False
        _dip_scan_state["message"] = "사용자 중지"
        return {"success": True, "message": "스캔 중지 요청"}
    return {"success": False, "message": "실행 중인 스캔 없음"}


async def _run_dip_scan(require_gates: bool, min_score: int):
    """★ 전종목 눌림목 스캔 백그라운드 작업"""
    global _dip_scan_state
    _dip_scan_state = {
        "running": True, "progress": 0, "scanned": 0, "total": 0,
        "found": 0, "deactivated": 0, "message": "스캔 준비 중...", "result": None,
    }

    try:
        from app.core.database import db
        from app.services.naver_stock import get_daily_candles_naver
        from app.engine.pattern_library import evaluate_dip_patterns, get_active_patterns_from_db

        active_patterns = get_active_patterns_from_db()
        if not active_patterns:
            _dip_scan_state["running"] = False
            _dip_scan_state["message"] = "❌ 활성화된 패턴 없음"
            return

        _dip_scan_state["message"] = "종목 목록 조회 중..."
        resp = db.table("stock_list") \
            .select("code, name, market, price, volume, market_cap") \
            .eq("is_active", True) \
            .eq("is_etf", False) \
            .eq("is_preferred", False) \
            .order("market_cap", desc=True) \
            .execute()

        stocks = resp.data or []
        if not stocks:
            _dip_scan_state["running"] = False
            _dip_scan_state["message"] = "❌ 스캔할 종목 없음"
            return

        total = len(stocks)
        _dip_scan_state["total"] = total
        _dip_scan_state["message"] = f"총 {total}개 종목 스캔 시작..."

        matched_stocks = []
        scanned = 0
        found = 0
        deactivated = 0
        batch_size = 5

        for i in range(0, total, batch_size):
            if not _dip_scan_state["running"]:
                break

            batch = stocks[i:i + batch_size]
            tasks = [_evaluate_single(
                s["code"], s["name"], s.get("market", ""),
                s.get("price", 0), s.get("market_cap", 0),
                active_patterns, require_gates, min_score
            ) for s in batch]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                scanned += 1
                if isinstance(r, dict):
                    if r.get("matched"):
                        matched_stocks.append(r)
                        found += 1
                    if r.get("deactivated"):
                        deactivated += 1

            _dip_scan_state["scanned"] = scanned
            _dip_scan_state["found"] = found
            _dip_scan_state["deactivated"] = deactivated
            _dip_scan_state["progress"] = min(int(scanned / total * 100), 99)

            current_name = batch[-1]["name"] if batch else ""
            _dip_scan_state["message"] = (
                f"스캔 중: {current_name} ({scanned}/{total}) — "
                f"눌림목 {found}개 발견"
                + (f", {deactivated}개 비활성화" if deactivated else "")
            )

            await asyncio.sleep(1.5)

        matched_stocks.sort(key=lambda x: x.get("score", 0), reverse=True)

        deact_msg = f", {deactivated}개 비활성화" if deactivated else ""
        _dip_scan_state["result"] = {
            "stocks": matched_stocks,
            "stats": {
                "total_scanned": scanned,
                "total_found": found,
                "deactivated": deactivated,
                "active_patterns": active_patterns,
                "require_gates": require_gates,
                "min_score": min_score,
            },
            "scan_date": datetime.now().isoformat(),
        }
        _dip_scan_state["progress"] = 100
        _dip_scan_state["message"] = f"스캔 완료! {scanned}개 스캔, {found}개 눌림목 발견{deact_msg}"
        _dip_scan_state["running"] = False

        logger.info(f"눌림목 스캔 완료: {scanned}/{total} 스캔, {found}개 발견, {deactivated}개 비활성화")

    except Exception as e:
        logger.error(f"눌림목 스캔 오류: {e}\n{traceback.format_exc()}")
        _dip_scan_state["running"] = False
        _dip_scan_state["message"] = f"❌ 스캔 오류: {e}"


async def _evaluate_single(
    code: str, name: str, market: str,
    price: int, market_cap: int,
    active_patterns: List[str],
    require_gates: bool, min_score: int,
) -> Dict:
    """단일 종목 눌림목 평가"""
    try:
        from app.services.naver_stock import get_daily_candles_naver
        from app.engine.pattern_library import evaluate_dip_patterns

        loop = asyncio.get_event_loop()
        candles = await loop.run_in_executor(
            None, lambda: get_daily_candles_naver(code, count=60)
        )

        if not candles or len(candles) == 0:
            try:
                from app.core.database import db
                db.table("stock_list").update({"is_active": False}).eq("code", code).execute()
                logger.info(f"★ 종목 비활성화: {name}({code}) — 캔들 0개")
            except Exception:
                pass
            return {"code": code, "name": name, "deactivated": True, "matched": False}

        if len(candles) < 22:
            return {"code": code, "name": name, "matched": False}

        result = evaluate_dip_patterns(candles, active_patterns=active_patterns, require_gates=require_gates)

        if result["is_dip"] and result["total_score"] >= min_score:
            last_close = candles[-1].get("close", 0)
            last_change = 0
            if len(candles) >= 2 and candles[-2].get("close", 0) > 0:
                last_change = round((candles[-1]["close"] - candles[-2]["close"]) / candles[-2]["close"] * 100, 2)

            return {
                "matched": True,
                "code": code,
                "name": name,
                "market": market,
                "price": last_close,
                "change_pct": last_change,
                "market_cap": market_cap,
                "score": result["total_score"],
                "best_pattern": result["best_pattern"],
                "matched_patterns": result["matched_patterns"],
                "gates": result["gates"],
                "gates_passed": result["gates_all_passed"],
            }

        return {"code": code, "name": name, "matched": False}

    except Exception as e:
        logger.debug(f"[{code}] 평가 오류: {e}")
        return {"code": code, "name": name, "matched": False}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 시장 상태 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.get("/market-status")
async def get_market_status():
    """KOSPI/KOSDAQ 시장 상태 조회"""
    try:
        from app.services.naver_stock import get_daily_candles_naver

        loop = asyncio.get_event_loop()

        kospi_raw = await loop.run_in_executor(
            None, lambda: get_daily_candles_naver("KOSPI", count=25)
        )
        kosdaq_raw = await loop.run_in_executor(
            None, lambda: get_daily_candles_naver("KOSDAQ", count=25)
        )

        def _build_index(raw):
            if not raw or len(raw) < 21:
                return {}
            closes = [c.get("close", 0) for c in raw]
            ma5 = sum(closes[-5:]) / 5 if len(closes) >= 5 else None
            ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else None
            last = raw[-1]
            prev = raw[-2] if len(raw) >= 2 else {}
            close_val = last.get("close", 0)
            prev_close = prev.get("close", 0)
            pct = round((close_val - prev_close) / prev_close * 100, 2) if prev_close else 0

            trend = "보합"
            if ma5 and ma20:
                if close_val > ma5 > ma20:
                    trend = "상승"
                elif close_val < ma5 < ma20:
                    trend = "하락"

            return {
                "close": close_val, "change_pct": pct,
                "ma5": round(ma5, 2) if ma5 else None,
                "ma20": round(ma20, 2) if ma20 else None,
                "trend": trend,
            }

        kospi = _build_index(kospi_raw)
        kosdaq = _build_index(kosdaq_raw)

        can_buy = True
        reason = ""
        k_pct = kospi.get("change_pct", 0)
        q_pct = kosdaq.get("change_pct", 0)
        if k_pct < -2 or q_pct < -2:
            can_buy = False
            reason = f"시장 급락 (KOSPI {k_pct}%, KOSDAQ {q_pct}%)"
        elif kospi.get("trend") == "하락" and kosdaq.get("trend") == "하락":
            can_buy = False
            reason = "KOSPI·KOSDAQ 모두 하락 추세"

        return {
            "success": True,
            "market": {"kospi": kospi, "kosdaq": kosdaq, "can_buy": can_buy, "reason": reason},
        }
    except Exception as e:
        logger.error(f"[시장 상태] 오류: {e}")
        return {"success": False, "error": str(e)}
