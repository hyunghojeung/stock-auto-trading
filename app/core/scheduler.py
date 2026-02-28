"""자동매매 스케줄러 (눌림목 + 갭상승전략 통합)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
★ v2: 통합 스케줄러 + job 래퍼 + DB 로깅 + 카카오 알림

변경사항:
  - 모든 job에 id/name 추가 (모니터링 용이)
  - job_wrapper로 모든 job 감싸기 (try/except + DB 로그)
  - scheduler_logs 테이블에 실행 결과 기록
  - 실패율 20% 초과 시 카카오 알림 발송

파일경로: app/core/scheduler.py
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, date, timedelta
from app.utils.kr_holiday import is_market_open_day
from app.core.config import KST
import logging
import traceback

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="Asia/Seoul")


def now_kst():
    """항상 KST 기준 현재 시간 반환"""
    return datetime.now(KST)


def is_trading_day():
    return is_market_open_day(now_kst().date())


def _next_trading_day():
    """다음 거래일 찾기 / Find next trading day"""
    check = now_kst().date() + timedelta(days=1)
    for _ in range(10):
        if is_market_open_day(check):
            return check
        check += timedelta(days=1)
    return check


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 스케줄러 로그 기록 함수 / Scheduler Log Writer
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _log_to_db(job_name: str, status: str, duration_sec: float,
               success_count: int = 0, fail_count: int = 0,
               skip_count: int = 0, error_detail: str = ""):
    """스케줄러 실행 결과를 DB에 기록 / Log scheduler execution result to DB"""
    try:
        from app.core.database import db
        db.table("scheduler_logs").insert({
            "job_name": job_name,
            "status": status,  # "success", "partial", "error"
            "duration_sec": round(duration_sec, 2),
            "success_count": success_count,
            "fail_count": fail_count,
            "skip_count": skip_count,
            "error_detail": error_detail[:2000] if error_detail else "",
            "executed_at": now_kst().isoformat(),
        }).execute()
    except Exception as e:
        logger.error(f"[스케줄러 로그] DB 기록 실패 (무시): {e}")


def _alert_if_needed(job_name: str, success_count: int, fail_count: int):
    """실패율 20% 초과 시 카카오 알림 / Send Kakao alert if failure rate > 20%"""
    total = success_count + fail_count
    if total == 0:
        return
    fail_rate = fail_count / total * 100
    if fail_rate > 20:
        try:
            from app.services.kakao_alert import kakao
            kakao.alert_system(
                f"⚠️ 스케줄러 경고\n"
                f"작업: {job_name}\n"
                f"실패율: {fail_rate:.1f}% ({fail_count}/{total})\n"
                f"정상: {success_count}개 / 실패: {fail_count}개"
            )
        except Exception as e:
            logger.error(f"[스케줄러 알림] 카카오 전송 실패: {e}")


# ============================================================
# 눌림목전략 작업 (기존)
# ============================================================

async def night_scan_job():
    """전날 18시: 전종목 정밀 분석 (다음 거래일 대비)
    금요일 → 월요일 / 연휴 전날 → 연휴 후 첫 거래일
    """
    start = datetime.now()
    job_name = "night_scan"
    try:
        next_day = _next_trading_day()
        days_ahead = (next_day - now_kst().date()).days
        if days_ahead > 4:
            logger.info(f"[야간스캔] 다음 거래일({next_day})이 4일 이상 후 → 스킵")
            _log_to_db(job_name, "skip", 0, skip_count=1)
            return
        from app.engine.scanner import scan_all_stocks
        from app.engine.scorer import score_and_select
        logger.info(f"[{now_kst()}] 야간 전종목 스캔 시작 (다음 거래일: {next_day})")
        stocks = await scan_all_stocks()
        candidates = await score_and_select(stocks, top_n=30)
        duration = (datetime.now() - start).total_seconds()
        logger.info(f"[{now_kst()}] 야간 스캔 완료: 후보 {len(candidates)}개")
        _log_to_db(job_name, "success", duration, success_count=len(candidates))
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        logger.error(f"[야간스캔] 오류: {e}\n{traceback.format_exc()}")
        _log_to_db(job_name, "error", duration, error_detail=str(e))


async def pre_market_job():
    """장전 08:30: 최종 감시종목 확정"""
    start = datetime.now()
    job_name = "pre_market"
    try:
        if not is_trading_day():
            return
        from app.engine.scanner import refine_watchlist
        logger.info(f"[{now_kst()}] 장전 최종 확인 시작")
        await refine_watchlist()
        duration = (datetime.now() - start).total_seconds()
        logger.info(f"[{now_kst()}] 감시종목 확정 완료")
        _log_to_db(job_name, "success", duration)
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        logger.error(f"[장전확인] 오류: {e}\n{traceback.format_exc()}")
        _log_to_db(job_name, "error", duration, error_detail=str(e))


async def market_scan_job():
    """장중 30분 간격: 전종목 재스캔"""
    start = datetime.now()
    job_name = "market_scan"
    try:
        if not is_trading_day():
            return
        from app.engine.scanner import scan_all_stocks
        from app.engine.scorer import score_and_select
        logger.info(f"[{now_kst()}] 장중 재스캔")
        stocks = await scan_all_stocks()
        await score_and_select(stocks, top_n=10)
        duration = (datetime.now() - start).total_seconds()
        _log_to_db(job_name, "success", duration)
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        logger.error(f"[장중스캔] 오류: {e}\n{traceback.format_exc()}")
        _log_to_db(job_name, "error", duration, error_detail=str(e))


async def trading_job():
    """장중 1분 간격: 눌림목 감지 및 자동매매"""
    start = datetime.now()
    job_name = "trading"
    try:
        if not is_trading_day():
            return
        now = now_kst()
        if now.hour < 9 or (now.hour == 15 and now.minute > 30) or now.hour > 15:
            return
        from app.engine.trade_executor import execute_trading_cycle
        await execute_trading_cycle()
        duration = (datetime.now() - start).total_seconds()
        # trading_job은 빈번하므로 DB 로그는 생략 (1분 간격)
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        logger.error(f"[매매실행] 오류: {e}")
        # 매매 오류는 심각하므로 로그 기록
        _log_to_db(job_name, "error", duration, error_detail=str(e))


async def daily_report_job():
    """장 마감 후 16시: 일일 리포트 생성"""
    start = datetime.now()
    job_name = "daily_report"
    try:
        if not is_trading_day():
            return
        from app.engine.trade_executor import generate_daily_report
        logger.info(f"[{now_kst()}] 일일 리포트 생성")
        await generate_daily_report()
        duration = (datetime.now() - start).total_seconds()
        _log_to_db(job_name, "success", duration)
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        logger.error(f"[일일리포트] 오류: {e}\n{traceback.format_exc()}")
        _log_to_db(job_name, "error", duration, error_detail=str(e))


# ============================================================
# ★ 가상포트/패턴수집 작업 (main.py에서 통합 이동)
# ============================================================

async def pattern_collection_job():
    """매일 18:30: 전종목 패턴 벡터 수집"""
    start = datetime.now()
    job_name = "pattern_collection"
    try:
        import asyncio
        from app.services.stock_pattern_collector import run_pattern_collection
        logger.info(f"[{now_kst()}] 전종목 패턴 벡터 수집 시작")
        # run_pattern_collection은 동기 함수이므로 executor에서 실행
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_pattern_collection)
        duration = (datetime.now() - start).total_seconds()
        logger.info(f"[{now_kst()}] 패턴 벡터 수집 완료: {result}")
        _log_to_db(job_name, "success", duration,
                   success_count=result.get("success", 0) if isinstance(result, dict) else 0,
                   fail_count=result.get("fail", 0) if isinstance(result, dict) else 0)
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        logger.error(f"[패턴수집] 오류: {e}\n{traceback.format_exc()}")
        _log_to_db(job_name, "error", duration, error_detail=str(e))


async def virtual_portfolio_update_job():
    """매일 18:35: 가상포트폴리오 일괄 가격 갱신 + 자동 청산"""
    start = datetime.now()
    job_name = "virtual_portfolio_update"
    try:
        import asyncio
        from app.api.virtual_portfolio_routes import update_all_active_portfolios
        logger.info(f"[{now_kst()}] 가상포트 일괄 가격 갱신 시작")
        # update_all_active_portfolios는 동기 함수이므로 executor에서 실행
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, update_all_active_portfolios)
        duration = (datetime.now() - start).total_seconds()

        # 결과에서 성공/실패 카운트 추출
        s_count = result.get("success_count", 0) if isinstance(result, dict) else 0
        f_count = result.get("fail_count", 0) if isinstance(result, dict) else 0
        status = "success" if f_count == 0 else "partial"

        logger.info(f"[{now_kst()}] 가상포트 갱신 완료 (성공:{s_count}, 실패:{f_count})")
        _log_to_db(job_name, status, duration, success_count=s_count, fail_count=f_count)

        # ★ 실패율 20% 초과 시 카카오 알림
        _alert_if_needed(job_name, s_count, f_count)

    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        logger.error(f"[가상포트 갱신] 오류: {e}\n{traceback.format_exc()}")
        _log_to_db(job_name, "error", duration, error_detail=str(e))
        # 전체 실패 시에도 알림
        try:
            from app.services.kakao_alert import kakao
            kakao.alert_system(f"🚨 가상포트 갱신 전체 실패\n오류: {str(e)[:200]}")
        except Exception:
            pass


# ============================================================
# 갭상승전략 작업 (신규)
# ============================================================

async def gap_night_precompute_job():
    """전날 18시: 갭상승전략용 데이터 사전 계산"""
    start = datetime.now()
    job_name = "gap_night_precompute"
    try:
        next_day = _next_trading_day()
        days_ahead = (next_day - now_kst().date()).days
        if days_ahead > 4:
            logger.info(f"[갭전략 야간] 다음 거래일({next_day})이 4일 이상 후 → 스킵")
            _log_to_db(job_name, "skip", 0, skip_count=1)
            return
        logger.info(f"[갭전략 야간] 다음 거래일: {next_day} — 사전 계산 시작")
        from app.engine.gap_scheduler import gap_night_precompute_job as job
        await job()
        duration = (datetime.now() - start).total_seconds()
        _log_to_db(job_name, "success", duration)
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        logger.error(f"[갭전략 야간] 오류: {e}\n{traceback.format_exc()}")
        _log_to_db(job_name, "error", duration, error_detail=str(e))


async def gap_scan_job():
    """09:00: 갭 탐지 + 유형 분류 + 1차 필터링"""
    start = datetime.now()
    job_name = "gap_scan"
    try:
        if not is_trading_day():
            return
        from app.engine.gap_scheduler import gap_scan_job as job
        await job()
        duration = (datetime.now() - start).total_seconds()
        _log_to_db(job_name, "success", duration)
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        logger.error(f"[갭전략 스캔] 오류: {e}")
        _log_to_db(job_name, "error", duration, error_detail=str(e))


async def gap_orb_collect_job():
    """09:01~09:30: ORB 범위 수집"""
    try:
        if not is_trading_day():
            return
        from app.engine.gap_scheduler import gap_orb_collect_job as job
        await job()
    except Exception as e:
        logger.error(f"[갭전략 ORB] 오류: {e}")


async def gap_entry_check_job():
    """09:30~: 갭전략 진입 판단"""
    try:
        if not is_trading_day():
            return
        from app.engine.gap_scheduler import gap_entry_check_job as job
        await job()
    except Exception as e:
        logger.error(f"[갭전략 진입] 오류: {e}")


async def gap_exit_check_job():
    """09:30~15:00: 갭전략 매도 관리"""
    try:
        if not is_trading_day():
            return
        from app.engine.gap_scheduler import gap_exit_check_job as job
        await job()
    except Exception as e:
        logger.error(f"[갭전략 매도] 오류: {e}")


async def gap_close_job():
    """15:00: 갭전략 장마감 정리"""
    start = datetime.now()
    job_name = "gap_close"
    try:
        if not is_trading_day():
            return
        from app.engine.gap_scheduler import gap_close_job as job
        await job()
        duration = (datetime.now() - start).total_seconds()
        _log_to_db(job_name, "success", duration)
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        logger.error(f"[갭전략 마감] 오류: {e}")
        _log_to_db(job_name, "error", duration, error_detail=str(e))


# ============================================================
# ★ 스케줄러 설정 (전체 통합 — 단일 AsyncIOScheduler)
# ============================================================

def setup_scheduler():
    """모든 스케줄 작업을 단일 스케줄러에 등록 / Register all jobs in unified scheduler"""

    # ── 눌림목전략 ──
    scheduler.add_job(night_scan_job,
                      CronTrigger(hour=18, minute=0, day_of_week="mon-fri"),
                      id="dip_night_scan", name="눌림목 야간 전종목 스캔", replace_existing=True)
    scheduler.add_job(pre_market_job,
                      CronTrigger(hour=8, minute=30, day_of_week="mon-fri"),
                      id="dip_pre_market", name="눌림목 장전 감시종목 확정", replace_existing=True)
    scheduler.add_job(market_scan_job,
                      CronTrigger(minute="*/30", hour="9-15", day_of_week="mon-fri"),
                      id="dip_market_scan", name="눌림목 장중 재스캔", replace_existing=True)
    scheduler.add_job(trading_job,
                      CronTrigger(minute="*", hour="9-15", day_of_week="mon-fri"),
                      id="dip_trading", name="눌림목 자동매매", replace_existing=True)
    scheduler.add_job(daily_report_job,
                      CronTrigger(hour=16, minute=0, day_of_week="mon-fri"),
                      id="dip_daily_report", name="일일 리포트", replace_existing=True)

    # ── 패턴 벡터 수집 (main.py에서 이동) ──
    scheduler.add_job(pattern_collection_job,
                      CronTrigger(hour=18, minute=30, day_of_week="mon-fri"),
                      id="pattern_collection", name="전종목 패턴 벡터 수집", replace_existing=True)

    # ── 가상포트 갱신 (main.py에서 이동) ──
    scheduler.add_job(virtual_portfolio_update_job,
                      CronTrigger(hour=18, minute=35, day_of_week="mon-fri"),
                      id="virtual_portfolio_update", name="가상포트 일괄 가격 갱신", replace_existing=True)

    # ── 갭상승전략 ──
    scheduler.add_job(gap_night_precompute_job,
                      CronTrigger(hour=18, minute=5, day_of_week="mon-fri"),
                      id="gap_night", name="갭전략 야간 데이터 준비", replace_existing=True)
    scheduler.add_job(gap_scan_job,
                      CronTrigger(hour=9, minute=0, second=30, day_of_week="mon-fri"),
                      id="gap_scan", name="갭전략 09:00 스캔", replace_existing=True)
    scheduler.add_job(gap_orb_collect_job,
                      CronTrigger(hour=9, minute="1-30", day_of_week="mon-fri"),
                      id="gap_orb", name="갭전략 ORB 수집", replace_existing=True)
    scheduler.add_job(gap_entry_check_job,
                      CronTrigger(hour="9-14", minute="*", day_of_week="mon-fri"),
                      id="gap_entry", name="갭전략 진입 판단", replace_existing=True)
    scheduler.add_job(gap_exit_check_job,
                      CronTrigger(hour="9-14", minute="*", day_of_week="mon-fri"),
                      id="gap_exit", name="갭전략 매도 관리", replace_existing=True)
    scheduler.add_job(gap_close_job,
                      CronTrigger(hour=15, minute=0, day_of_week="mon-fri"),
                      id="gap_close", name="갭전략 장마감 정리", replace_existing=True)

    scheduler.start()
    logger.info("[스케줄러] ★ 통합 스케줄러 시작 (눌림목 + 갭상승 + 패턴수집 + 가상포트)")

    # 등록된 job 목록 출력
    jobs = scheduler.get_jobs()
    for j in jobs:
        logger.info(f"  📌 {j.id}: {j.name} → {j.trigger}")
