"""자동매매 스케줄러 (눌림목 + 갭상승전략 통합)"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, date, timedelta
from app.utils.kr_holiday import is_market_open_day

scheduler = AsyncIOScheduler(timezone="Asia/Seoul")

def is_trading_day():
    return is_market_open_day(datetime.now().date())

# ============================================================
# 눌림목전략 작업 (기존)
# ============================================================

async def night_scan_job():
    """전날 18시: 전종목 정밀 분석"""
    if not is_market_open_day(datetime.now().date()):
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
    if not is_trading_day():
        return
    now = datetime.now()
    if now.hour < 9 or (now.hour == 15 and now.minute > 30) or now.hour > 15:
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

# ============================================================
# 갭상승전략 작업 (신규)
# ============================================================

async def gap_night_precompute_job():
    """전날 18시: 갭상승전략용 데이터 사전 계산"""
    tomorrow = date.today() + timedelta(days=1)
    if not is_market_open_day(tomorrow):
        print(f"[갭전략 야간] 내일({tomorrow})은 휴장일 → 스킵")
        return
    from app.engine.gap_scheduler import gap_night_precompute_job as job
    await job()

async def gap_scan_job():
    """09:00: 갭 탐지 + 유형 분류 + 1차 필터링"""
    if not is_trading_day():
        return
    from app.engine.gap_scheduler import gap_scan_job as job
    await job()

async def gap_orb_collect_job():
    """09:01~09:30: ORB 범위 수집"""
    if not is_trading_day():
        return
    from app.engine.gap_scheduler import gap_orb_collect_job as job
    await job()

async def gap_entry_check_job():
    """09:30~: 갭전략 진입 판단"""
    if not is_trading_day():
        return
    from app.engine.gap_scheduler import gap_entry_check_job as job
    await job()

async def gap_exit_check_job():
    """09:30~15:00: 갭전략 매도 관리"""
    if not is_trading_day():
        return
    from app.engine.gap_scheduler import gap_exit_check_job as job
    await job()

async def gap_close_job():
    """15:00: 갭전략 장마감 정리"""
    if not is_trading_day():
        return
    from app.engine.gap_scheduler import gap_close_job as job
    await job()

# ============================================================
# 스케줄러 설정 (눌림목 + 갭상승 통합)
# ============================================================

def setup_scheduler():
    # ── 눌림목전략 (기존) ──
    scheduler.add_job(night_scan_job, CronTrigger(hour=18, minute=0, day_of_week="mon-fri"))
    scheduler.add_job(pre_market_job, CronTrigger(hour=8, minute=30, day_of_week="mon-fri"))
    scheduler.add_job(market_scan_job, CronTrigger(minute="*/30", hour="9-15", day_of_week="mon-fri"))
    scheduler.add_job(trading_job, CronTrigger(minute="*", hour="9-15", day_of_week="mon-fri"))
    scheduler.add_job(daily_report_job, CronTrigger(hour=16, minute=0, day_of_week="mon-fri"))

    # ── 갭상승전략 (신규) ──
    scheduler.add_job(gap_night_precompute_job, CronTrigger(hour=18, minute=5, day_of_week="mon-fri"),
                      id="gap_night", name="갭전략 야간 데이터 준비", replace_existing=True)
    scheduler.add_job(gap_scan_job, CronTrigger(hour=9, minute=0, second=30, day_of_week="mon-fri"),
                      id="gap_scan", name="갭전략 09:00 스캔", replace_existing=True)
    scheduler.add_job(gap_orb_collect_job, CronTrigger(hour=9, minute="1-30", day_of_week="mon-fri"),
                      id="gap_orb", name="갭전략 ORB 수집", replace_existing=True)
    scheduler.add_job(gap_entry_check_job, CronTrigger(hour="9-14", minute="*", day_of_week="mon-fri"),
                      id="gap_entry", name="갭전략 진입 판단", replace_existing=True)
    scheduler.add_job(gap_exit_check_job, CronTrigger(hour="9-14", minute="*", day_of_week="mon-fri"),
                      id="gap_exit", name="갭전략 매도 관리", replace_existing=True)
    scheduler.add_job(gap_close_job, CronTrigger(hour=15, minute=0, day_of_week="mon-fri"),
                      id="gap_close", name="갭전략 장마감 정리", replace_existing=True)

    scheduler.start()
    print("[스케줄러] 자동매매 스케줄러 시작됨 (눌림목 + 갭상승전략)")
