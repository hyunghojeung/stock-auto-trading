"""자동매매 스케줄러"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from app.utils.kr_holiday import is_market_open_day, is_market_open_now

scheduler = AsyncIOScheduler(timezone="Asia/Seoul")

def is_trading_day():
    return is_market_open_day(datetime.now().date())

async def night_scan_job():
    """전날 18시: 전종목 정밀 분석"""
    if not is_market_open_day((datetime.now().date())):
        return
    from app.engine.scanner import scan_all_stocks
    from app.engine.scorer import score_and_select
    print(f"[{datetime.now()}] 야간 전종목 스캔 시작")
    stocks = await scan_all_stocks()
    candidates = await score_and_select(stocks, top_n=30)
    print(f"[{datetime.now()}] 야간 스캔 완료: 후보 {len(candidates)}개")

async def pre_market_job():
    """장전 08:30: 최종 감시종목 확정"""
    if not is_trading_day():
        return
    from app.engine.scanner import refine_watchlist
    print(f"[{datetime.now()}] 장전 최종 확인 시작")
    await refine_watchlist()
    print(f"[{datetime.now()}] 감시종목 확정 완료")

async def market_scan_job():
    """장중 30분 간격: 전종목 재스캔"""
    if not is_trading_day():
        return
    from app.engine.scanner import scan_all_stocks
    from app.engine.scorer import score_and_select
    print(f"[{datetime.now()}] 장중 재스캔")
    stocks = await scan_all_stocks()
    await score_and_select(stocks, top_n=10)

async def trading_job():
    """장중 1분 간격: 눌림목 감지 및 자동매매"""
    # ★ 공휴일 + 주말 + 장시간 종합 체크 / Comprehensive market check
    if not is_market_open_now():
        return
    from app.engine.trade_executor import execute_trading_cycle
    await execute_trading_cycle()

async def daily_report_job():
    """장 마감 후 16시: 일일 리포트 생성"""
    if not is_trading_day():
        return
    from app.engine.trade_executor import generate_daily_report
    print(f"[{datetime.now()}] 일일 리포트 생성")
    await generate_daily_report()

def setup_scheduler():
    # 전날 18시: 야간 전종목 스캔
    scheduler.add_job(night_scan_job, CronTrigger(hour=18, minute=0, day_of_week="mon-fri"))
    # 장전 08:30: 최종 확인
    scheduler.add_job(pre_market_job, CronTrigger(hour=8, minute=30, day_of_week="mon-fri"))
    # 장중 30분 간격 재스캔
    scheduler.add_job(market_scan_job, CronTrigger(minute="*/30", hour="9-15", day_of_week="mon-fri"))
    # 장중 1분 간격 매매 실행
    scheduler.add_job(trading_job, CronTrigger(minute="*", hour="9-15", day_of_week="mon-fri"))
    # 장 마감 후 리포트
    scheduler.add_job(daily_report_job, CronTrigger(hour=16, minute=0, day_of_week="mon-fri"))
    scheduler.start()
    print("[스케줄러] 자동매매 스케줄러 시작됨")
