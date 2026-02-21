"""
종목 DB API 라우트 / Stock DB API Routes
전종목 검색, 통계, 업데이트 엔드포인트

파일 위치: app/api/stock_routes.py

main.py에 추가 필요:
    from app.api.stock_routes import router as stock_router
    app.include_router(stock_router)
"""

from fastapi import APIRouter, BackgroundTasks, Query
from typing import Optional
from datetime import datetime

from app.services.stock_fetcher import (
    update_stock_list,
    search_stocks_from_db,
    get_stock_by_code,
    get_all_active_stocks,
    get_stock_stats,
)

router = APIRouter(prefix="/api/stocks", tags=["종목DB"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 종목 검색 (자동완성용) / Stock Search (Autocomplete)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/search")
async def search_stocks(
    q: str = Query("", min_length=1, description="검색어 (종목명 또는 코드)"),
    market: Optional[str] = Query(None, description="시장 필터: kospi / kosdaq"),
    limit: int = Query(20, ge=1, le=50, description="최대 결과 수"),
    exclude_etf: bool = Query(False, description="ETF 제외"),
    exclude_preferred: bool = Query(False, description="우선주 제외"),
):
    """
    종목 검색 API (DB 기반)
    패턴탐지기, 스윙백테스트, 자동매매 등 모든 검색에서 호출

    GET /api/stocks/search?q=삼성&limit=10
    GET /api/stocks/search?q=005930
    GET /api/stocks/search?q=반도체&market=kospi&exclude_etf=true
    """
    results = search_stocks_from_db(
        query=q,
        market=market,
        limit=limit,
        active_only=True,
        exclude_etf=exclude_etf,
        exclude_preferred=exclude_preferred,
    )

    return {
        "results": results,
        "count": len(results),
        "query": q,
        "source": "db",
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. 단일 종목 조회 / Get Stock by Code
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/info/{code}")
async def get_stock_info(code: str):
    """
    종목코드로 상세 정보 조회

    GET /api/stocks/info/005930
    """
    stock = get_stock_by_code(code)
    if not stock:
        return {"error": f"종목코드 {code}를 찾을 수 없습니다", "found": False}
    return {"stock": stock, "found": True}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. 종목 통계 / Stock Statistics
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/stats")
async def stock_statistics():
    """
    전종목 DB 통계

    GET /api/stocks/stats
    → {"total": 2847, "active": 2531, "kospi": 980, "kosdaq": 1551, "etf": 316}
    """
    stats = get_stock_stats()
    return stats


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. 전종목 리스트 / Full Stock List
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/list")
async def list_stocks(
    market: Optional[str] = Query(None, description="kospi / kosdaq"),
    exclude_etf: bool = Query(True, description="ETF 제외"),
    exclude_preferred: bool = Query(True, description="우선주 제외"),
    min_price: int = Query(0, description="최소 가격"),
    min_volume: int = Query(0, description="최소 거래량"),
):
    """
    전체 활성 종목 리스트
    패턴분석, 스윙발굴의 전종목 스캔에서 사용

    GET /api/stocks/list?exclude_etf=true&min_price=1000&min_volume=100000
    """
    stocks = get_all_active_stocks(
        market=market,
        exclude_etf=exclude_etf,
        exclude_preferred=exclude_preferred,
        min_price=min_price,
        min_volume=min_volume,
    )

    return {
        "stocks": stocks,
        "count": len(stocks),
        "filters": {
            "market": market,
            "exclude_etf": exclude_etf,
            "exclude_preferred": exclude_preferred,
            "min_price": min_price,
            "min_volume": min_volume,
        }
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 수동 업데이트 / Manual Update
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# 진행 상태 저장
_update_progress = {
    "status": "idle",
    "message": "",
    "started_at": None,
    "result": None,
}

@router.post("/update")
async def trigger_stock_update(background_tasks: BackgroundTasks):
    """
    전종목 DB 수동 업데이트 실행 (백그라운드)
    KRX에서 전종목 수집 → DB 저장

    POST /api/stocks/update
    """
    if _update_progress["status"] == "running":
        return {"error": "이미 업데이트가 진행 중입니다", "status": "running"}

    _update_progress["status"] = "running"
    _update_progress["message"] = "KRX에서 전종목 데이터를 수집 중입니다..."
    _update_progress["started_at"] = datetime.now().isoformat()
    _update_progress["result"] = None

    background_tasks.add_task(_run_update_task)

    return {"status": "started", "message": "전종목 업데이트를 시작합니다"}


@router.get("/update/progress")
async def update_progress():
    """업데이트 진행 상태 조회"""
    return _update_progress


async def _run_update_task():
    """백그라운드 업데이트 태스크"""
    try:
        _update_progress["message"] = "KRX에서 코스피 + 코스닥 전종목 수집 중..."
        result = await update_stock_list()
        _update_progress["status"] = "done"
        _update_progress["message"] = "업데이트 완료"
        _update_progress["result"] = result
    except Exception as e:
        _update_progress["status"] = "error"
        _update_progress["message"] = f"오류: {str(e)}"
        _update_progress["result"] = {"error": str(e)}
