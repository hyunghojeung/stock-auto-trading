"""FastAPI 메인 앱"""
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic
from contextlib import asynccontextmanager
from app.core.config import config, KST
from app.core.scheduler import setup_scheduler
from app.api import stock_routes, trade_routes, portfolio_routes, watchlist_routes, strategy_routes, kakao_routes
from app.utils.kr_holiday import get_market_status, is_market_open_now, get_holiday_name, get_next_market_day
from datetime import datetime
from app.api.backtest_routes import router as backtest_router
from app.api.swing_routes import router as swing_router
from app.api.pattern_routes import router as pattern_router
from app.api.surge_scanner_routes import router as scanner_router
from app.api.virtual_invest_routes import router as virtual_invest_router
from app.api.virtual_portfolio_routes import router as virtual_portfolio_router
from app.api.pattern_lib_routes import router as pattern_lib_router  # ★ v3: 패턴 라이브러리


# ★ 패턴 벡터 수집기
from app.services.stock_pattern_collector import run_pattern_collection

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_scheduler()

    # ★ 전종목 패턴 벡터 수집 스케줄러 등록 (매일 18:30 KST)
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        import pytz

        pattern_scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Seoul"))
        pattern_scheduler.add_job(
            run_pattern_collection,
            CronTrigger(hour=18, minute=30, timezone=pytz.timezone("Asia/Seoul")),
            id="pattern_vector_collection",
            name="전종목 패턴 벡터 수집",
            replace_existing=True,
        )
        pattern_scheduler.start()
        print("[스케줄러] 전종목 패턴 벡터 수집 등록 (매일 18:30 KST)")
    except Exception as e:
        print(f"[스케줄러] 패턴 수집 스케줄러 등록 실패 (무시): {e}")

    # ★ 가상포트폴리오 일괄 가격 갱신 (매일 18:35 KST)
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        import pytz
        from app.api.virtual_portfolio_routes import update_all_active_portfolios

        pf_scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Seoul"))
        pf_scheduler.add_job(
            update_all_active_portfolios,
            CronTrigger(hour=18, minute=35, timezone=pytz.timezone("Asia/Seoul")),
            id="virtual_portfolio_update",
            name="가상포트 일괄 가격 갱신",
            replace_existing=True,
        )
        pf_scheduler.start()
        print("[스케줄러] 가상포트 일괄 가격 갱신 등록 (매일 18:35 KST)")
    except Exception as e:
        print(f"[스케줄러] 가상포트 갱신 스케줄러 등록 실패 (무시): {e}")

    print("[서버] 10억 만들기 자동매매 서버 시작")
    yield
    print("[서버] 서버 종료")

app = FastAPI(title="10억 만들기 - 주식 자동매매", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API 라우터 등록
app.include_router(stock_routes.router)
app.include_router(trade_routes.router)
app.include_router(portfolio_routes.router)
app.include_router(watchlist_routes.router)
app.include_router(strategy_routes.router)
app.include_router(kakao_routes.router)
app.include_router(backtest_router)
app.include_router(swing_router)
app.include_router(scanner_router)
app.include_router(pattern_router)
app.include_router(virtual_invest_router)
app.include_router(virtual_portfolio_router)
app.include_router(pattern_lib_router)  # ★ v3: 패턴 라이브러리
@app.get("/")
async def root():
    now = datetime.now(KST)
    return {"name": "10억 만들기", "status": "running", "market": get_market_status(now)}

@app.get("/api/auth")
async def authenticate(password: str = ""):
    return {"authenticated": True}

@app.get("/api/system/status")
async def system_status():
    now = datetime.now(KST)
    holiday = get_holiday_name(now.date())
    return {
        "datetime": now.isoformat(),
        "date_kr": now.strftime("%Y년 %m월 %d일 (%a)"),
        "time_kr": now.strftime("%H:%M:%S"),
        "market_status": get_market_status(now),
        "is_market_open": is_market_open_now(now),
        "holiday": holiday,
        "next_market_day": str(get_next_market_day(now.date())) if not is_market_open_now(now) else None,
    }

@app.get("/api/scan/trigger")
async def trigger_scan(password: str = ""):
    """수동 전종목 스캔 트리거"""
    if password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")
    try:
        from app.engine.scanner import scan_all_stocks
        from app.engine.scorer import score_and_select
        stocks = await scan_all_stocks()
        candidates = await score_and_select(stocks, top_n=30)
        return {"success": True, "message": f"스캔 완료: 후보 {len(candidates)}개", "count": len(candidates)}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/trading/trigger")
async def trigger_trading(password: str = ""):
    if password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")
    from app.engine.trade_executor import execute_trading_cycle
    await execute_trading_cycle()
    return {"success": True, "message": "매매사이클 수동 실행 완료"}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 전종목 패턴 벡터 수집 엔드포인트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_pattern_collect_state = {"running": False, "result": None, "started_at": None}

@app.post("/api/patterns/collect")
async def collect_patterns(background_tasks: BackgroundTasks):
    """전종목 패턴 벡터 수동 수집 (백그라운드 실행, 약 8~10분 소요)"""
    if _pattern_collect_state["running"]:
        return {
            "status": "already_running",
            "message": "이미 수집 중입니다",
            "started_at": _pattern_collect_state["started_at"],
        }

    def _run():
        _pattern_collect_state["running"] = True
        _pattern_collect_state["started_at"] = datetime.now(KST).isoformat()
        _pattern_collect_state["result"] = None
        try:
            result = run_pattern_collection()
            _pattern_collect_state["result"] = result
        except Exception as e:
            _pattern_collect_state["result"] = {"error": str(e)}
        finally:
            _pattern_collect_state["running"] = False

    background_tasks.add_task(_run)
    return {
        "status": "started",
        "message": "전종목 패턴 벡터 수집 시작 (약 8~10분 소요)",
    }

@app.get("/api/patterns/collect/status")
async def collect_patterns_status():
    """패턴 벡터 수집 상태 확인"""
    return {
        "running": _pattern_collect_state["running"],
        "started_at": _pattern_collect_state["started_at"],
        "result": _pattern_collect_state["result"],
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORT)
