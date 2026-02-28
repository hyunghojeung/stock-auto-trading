"""눌림목 패턴 라이브러리 API
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- GET  /api/pattern-library/list       → 패턴 정의 목록 조회
- PUT  /api/pattern-library/{code}/toggle → 패턴 활성화/비활성화
- POST /api/pattern-library/evaluate    → 특정 종목 패턴 평가 (테스트)
- GET  /api/pattern-library/market-status → 시장 상태 조회

파일경로: app/api/pattern_lib_routes.py
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pattern-library", tags=["패턴 라이브러리"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 패턴 정의 목록 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.get("/list")
async def get_pattern_list():
    """DB에서 전체 패턴 정의 목록 조회"""
    try:
        from app.core.database import db
        resp = db.table("pattern_definitions") \
            .select("*") \
            .order("pattern_code") \
            .execute()

        patterns = resp.data or []

        # 각 패턴에 런타임 정보 추가
        from app.engine.pattern_library import PATTERN_FUNCTIONS
        for p in patterns:
            p["has_engine"] = p["pattern_code"] in PATTERN_FUNCTIONS

        return {
            "success": True,
            "patterns": patterns,
            "total": len(patterns),
        }
    except Exception as e:
        logger.error(f"[패턴 목록] 오류: {e}")
        raise HTTPException(500, f"패턴 목록 조회 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 패턴 활성화/비활성화 토글
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.put("/{pattern_code}/toggle")
async def toggle_pattern(pattern_code: str):
    """패턴 활성화/비활성화 토글"""
    try:
        from app.core.database import db

        # 현재 상태 조회
        resp = db.table("pattern_definitions") \
            .select("is_active") \
            .eq("pattern_code", pattern_code.upper()) \
            .execute()

        if not resp.data:
            raise HTTPException(404, f"패턴 코드 '{pattern_code}'를 찾을 수 없습니다")

        current = resp.data[0]["is_active"]
        new_state = not current

        # 토글
        db.table("pattern_definitions") \
            .update({"is_active": new_state}) \
            .eq("pattern_code", pattern_code.upper()) \
            .execute()

        logger.info(f"[패턴 토글] {pattern_code}: {current} → {new_state}")
        return {
            "success": True,
            "pattern_code": pattern_code.upper(),
            "is_active": new_state,
            "message": f"{pattern_code} {'활성화' if new_state else '비활성화'} 완료",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[패턴 토글] 오류: {e}")
        raise HTTPException(500, f"토글 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 특정 종목 패턴 평가 (테스트용)
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
        stock_name = stock_code  # 기본값: 입력 그대로

        if not (len(stock_code) == 6 and stock_code.isdigit()):
            # 이름으로 검색
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

        # 일봉 데이터 가져오기
        candles = get_daily_candles_naver(stock_code, count=60)
        if not candles or len(candles) < 22:
            # ★ 캔들 0개 = 거래정지/상장폐지 → 자동 비활성화
            if not candles or len(candles) == 0:
                try:
                    db.table("stock_list").update({"is_active": False}).eq("code", stock_code).execute()
                    logger.info(f"★ 종목 비활성화: {stock_name}({stock_code}) — 캔들 데이터 0개")
                except Exception:
                    pass
            raise HTTPException(400, f"종목 {stock_name}({stock_code}): 캔들 데이터 부족 ({len(candles) if candles else 0}일)")

        # 활성 패턴 조회
        active = get_active_patterns_from_db()

        # 평가 실행
        result = evaluate_dip_patterns(
            candles,
            active_patterns=active,
            require_gates=req.require_gates,
        )

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
# 시장 상태 조회 (시장지수 데이터)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.get("/market-status")
async def get_market_status():
    """현재 시장 상태 (KOSPI/KOSDAQ 지수 + 매수 허용 여부) 조회"""
    try:
        from app.services.market_index import get_market_status as fetch_status
        status = fetch_status()
        return {
            "success": True,
            "market": status,
        }
    except Exception as e:
        logger.error(f"[시장 상태] 오류: {e}")
        return {
            "success": False,
            "error": str(e),
            "market": {"can_buy": True, "reason": "조회 실패"},
        }
