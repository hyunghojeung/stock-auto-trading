"""
급등패턴 매매 시뮬레이터 — API 라우트
Surge Pattern Trade Simulator — API Routes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/api/surge_sim_routes.py

POST /api/simulation/run       — 시뮬레이션 실행 (비동기)
GET  /api/simulation/progress  — 진행률 확인
GET  /api/simulation/result    — 결과 조회

※ main.py에 등록 필요:
   from app.api.surge_sim_routes import router as sim_router
   app.include_router(sim_router)
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict
import asyncio
import logging
import traceback

from app.engine.surge_simulator import (
    run_surge_simulation,
    SimConfig,
)
from app.engine.pattern_analyzer import (
    CandleDay,
    run_pattern_analysis,
)
from app.services.naver_stock import fetch_naver_candles_raw

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/simulation", tags=["simulation"])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전역 상태 / Global State
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_sim_state = {
    "running": False,
    "progress": 0,
    "message": "",
    "result": None,
    "error": None,
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 요청 모델 / Request Models
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SimulationRequest(BaseModel):
    codes: List[str]                    # 종목코드 리스트
    names: Dict[str, str] = {}          # 종목명 매핑
    period_days: int = 365              # 데이터 조회 기간 (일)

    # 패턴 분석 파라미터
    pre_rise_days: int = 10             # 급상승 전 분석 구간
    rise_pct: float = 30.0              # 급상승 기준 (%)
    rise_window: int = 5                # 급상승 판단 기간 (거래일)

    # 시뮬레이션 파라미터
    initial_capital: float = 10_000_000  # 초기 자금
    take_profit_pct: float = 7.0        # 익절 %
    stop_loss_pct: float = 3.0          # 손절 %
    max_hold_days: int = 10             # 최대 보유일
    max_positions: int = 5              # 최대 동시 보유
    similarity_threshold: float = 65.0  # DTW 유사도 기준
    trailing_stop: bool = False         # 트레일링 스톱
    trailing_pct: float = 3.0          # 트레일링 %


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 엔드포인트 / Endpoints
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/run")
async def start_simulation(req: SimulationRequest, background_tasks: BackgroundTasks):
    """
    시뮬레이션 시작 (비동기 백그라운드)
    Start simulation (async background)
    """
    if _sim_state["running"]:
        raise HTTPException(status_code=409, detail="이미 시뮬레이션이 실행 중입니다")

    # 상태 초기화
    _sim_state["running"] = True
    _sim_state["progress"] = 0
    _sim_state["message"] = "시뮬레이션 준비 중..."
    _sim_state["result"] = None
    _sim_state["error"] = None

    background_tasks.add_task(_run_simulation_task, req)

    return {
        "status": "started",
        "message": f"{len(req.codes)}개 종목 시뮬레이션 시작",
        "config": {
            "initial_capital": req.initial_capital,
            "take_profit": req.take_profit_pct,
            "stop_loss": req.stop_loss_pct,
            "max_hold_days": req.max_hold_days,
            "max_positions": req.max_positions,
            "similarity_threshold": req.similarity_threshold,
        }
    }


@router.get("/progress")
async def get_progress():
    """진행률 확인 / Check progress"""
    return {
        "running": _sim_state["running"],
        "progress": _sim_state["progress"],
        "message": _sim_state["message"],
        "has_result": _sim_state["result"] is not None,
        "error": _sim_state["error"],
    }


@router.get("/result")
async def get_result():
    """결과 조회 / Get result"""
    if _sim_state["running"]:
        return {"status": "running", "message": "시뮬레이션 진행 중입니다"}

    if _sim_state["error"]:
        return {"status": "error", "error": _sim_state["error"]}

    if _sim_state["result"] is None:
        return {"status": "empty", "message": "실행된 시뮬레이션이 없습니다"}

    return {
        "status": "completed",
        "result": _sim_state["result"],
    }


@router.post("/stop")
async def stop_simulation():
    """시뮬레이션 중단 / Stop simulation"""
    if _sim_state["running"]:
        _sim_state["running"] = False
        _sim_state["message"] = "사용자에 의해 중단됨"
        return {"status": "stopped"}
    return {"status": "not_running"}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 백그라운드 태스크 / Background Task
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _run_simulation_task(req: SimulationRequest):
    """시뮬레이션 전체 파이프라인 실행"""
    try:
        def update_progress(pct, msg):
            _sim_state["progress"] = pct
            _sim_state["message"] = msg

        # ── Phase 1: 일봉 데이터 수집 (0~25%) ──
        update_progress(2, f"일봉 데이터 수집 중... (0/{len(req.codes)})")

        candles_by_code = {}
        names = dict(req.names)

        for idx, code in enumerate(req.codes):
            if not _sim_state["running"]:
                return

            update_progress(
                2 + int((idx / len(req.codes)) * 23),
                f"일봉 수집 중: {names.get(code, code)} ({idx + 1}/{len(req.codes)})"
            )

            try:
                raw_candles = await asyncio.to_thread(
                    fetch_naver_candles_raw, code, req.period_days
                )

                if raw_candles and len(raw_candles) >= 30:
                    candle_objects = []
                    for c in raw_candles:
                        candle_objects.append(CandleDay(
                            date=c.get('date', ''),
                            open=float(c.get('open', 0)),
                            high=float(c.get('high', 0)),
                            low=float(c.get('low', 0)),
                            close=float(c.get('close', 0)),
                            volume=int(c.get('volume', 0)),
                        ))
                    candles_by_code[code] = candle_objects

                    # 종목명 자동 추출
                    if code not in names and raw_candles:
                        name_from_data = raw_candles[0].get('name', '')
                        if name_from_data:
                            names[code] = name_from_data

                    logger.info(f"  {code} ({names.get(code, '?')}): {len(candle_objects)}일봉")

                await asyncio.sleep(0.5)  # API 부하 방지

            except Exception as e:
                logger.warning(f"  {code} 데이터 수집 실패: {e}")

        if not candles_by_code:
            _sim_state["error"] = "수집된 일봉 데이터가 없습니다"
            _sim_state["running"] = False
            return

        update_progress(25, f"데이터 수집 완료: {len(candles_by_code)}개 종목")

        # ── Phase 2: DTW 패턴 분석 (25~55%) ──
        update_progress(27, "DTW 패턴 분석 시작...")

        def pattern_progress(pct, msg):
            # 25~55% 범위로 매핑
            mapped_pct = 25 + int(pct * 0.30)
            update_progress(mapped_pct, msg)

        analysis_result = run_pattern_analysis(
            candles_by_code=candles_by_code,
            names=names,
            pre_days=req.pre_rise_days,
            rise_pct=req.rise_pct,
            rise_window=req.rise_window,
            progress_callback=pattern_progress,
        )

        if analysis_result.total_patterns == 0:
            _sim_state["error"] = "급상승 패턴이 발견되지 않았습니다. 기준을 완화해보세요."
            _sim_state["running"] = False
            return

        update_progress(55, f"패턴 분석 완료: {analysis_result.total_patterns}개 패턴, "
                           f"{len(analysis_result.clusters)}개 클러스터")

        # ── Phase 3: 매매 시뮬레이션 (55~95%) ──
        update_progress(57, "매매 시뮬레이션 시작...")

        sim_config = SimConfig(
            initial_capital=req.initial_capital,
            take_profit_pct=req.take_profit_pct,
            stop_loss_pct=req.stop_loss_pct,
            max_hold_days=req.max_hold_days,
            max_positions=req.max_positions,
            similarity_threshold=req.similarity_threshold,
            trailing_stop=req.trailing_stop,
            trailing_pct=req.trailing_pct,
        )

        # 클러스터 객체 복원 (dict → 적절한 형태)
        clusters_for_sim = analysis_result.clusters  # 이미 dict 리스트

        def sim_progress(pct, msg):
            mapped_pct = 55 + int(pct * 0.40)
            update_progress(mapped_pct, msg)

        sim_result = run_surge_simulation(
            candles_by_code=candles_by_code,
            names=names,
            clusters=clusters_for_sim,
            config=sim_config,
            progress_callback=sim_progress,
        )

        # ── Phase 4: 결과 저장 (95~100%) ──
        update_progress(95, "결과 정리 중...")

        from dataclasses import asdict
        result_dict = asdict(sim_result)

        # 패턴 분석 요약도 포함
        result_dict['pattern_summary'] = {
            'total_surges': analysis_result.total_surges,
            'total_patterns': analysis_result.total_patterns,
            'total_clusters': len(analysis_result.clusters),
            'summary': analysis_result.summary,
        }

        _sim_state["result"] = result_dict
        _sim_state["progress"] = 100
        _sim_state["message"] = "시뮬레이션 완료!"

        logger.info(f"시뮬레이션 완료: {sim_result.total_trades}건 매매, "
                    f"수익률 {sim_result.total_return_pct:+.1f}%, "
                    f"승률 {sim_result.win_rate:.1f}%")

    except Exception as e:
        logger.error(f"시뮬레이션 오류: {e}\n{traceback.format_exc()}")
        _sim_state["error"] = f"시뮬레이션 오류: {str(e)}"
    finally:
        _sim_state["running"] = False
