"""
스윙 백테스트 API 라우트 / Swing Backtest API Routes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
기존 라우트를 전혀 수정하지 않는 독립 API 모듈입니다.
main.py에 app.include_router(swing_router) 한 줄만 추가하면 됩니다.
"""

from fastapi import APIRouter, BackgroundTasks, Query
from typing import List, Dict
from datetime import datetime, timedelta, date
import asyncio
import json
import traceback

from app.core.database import db
from app.services.kis_stock import get_daily_candles

router = APIRouter(prefix="/api/swing", tags=["스윙백테스트"])

# 진행 상태 저장 (메모리)
_progress = {
    "status": "idle",  # idle / running / done / error
    "step": "",
    "pct": 0,
    "message": "",
    "result": None,
    "error": None,
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 전체 분석 실행 / Run Full Analysis
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/run")
async def run_swing_analysis(background_tasks: BackgroundTasks,
                             rise_threshold: float = 30.0,
                             generations: int = 10,
                             top_n: int = 20):
    """
    전체 스윙 분석을 백그라운드로 실행한다.
    1) 전종목 일봉 수집
    2) 과거 우승 종목 프로필 구축
    3) 현재 후보 종목 발굴
    4) 백테스트 + 패턴 통계
    5) 자동 교정 (진화 알고리즘)
    """
    if _progress["status"] == "running":
        return {"error": "이미 분석이 진행 중입니다", "status": "running"}

    _progress["status"] = "running"
    _progress["step"] = "초기화"
    _progress["pct"] = 0
    _progress["message"] = "분석을 시작합니다..."
    _progress["result"] = None
    _progress["error"] = None

    background_tasks.add_task(
        _run_analysis_task,
        rise_threshold=rise_threshold,
        generations=generations,
        top_n=top_n
    )

    return {"status": "started", "message": "백그라운드 분석 시작"}


async def _run_analysis_task(rise_threshold=30.0, generations=10, top_n=20):
    """백그라운드 분석 태스크"""
    try:
        from app.engine.swing_discoverer import (
            build_winner_profile, discover_swing_candidates, find_big_rises
        )
        from app.engine.swing_pattern_stats import (
            run_swing_backtest, analyze_timing_patterns, auto_calibrate
        )

        # ── Step 1: 전종목 일봉 데이터 수집 ──
        _update_progress("데이터 수집", 5, "KRX에서 종목 목록을 가져옵니다...")
        stocks = await _fetch_stock_list()

        if not stocks:
            _progress["status"] = "error"
            _progress["error"] = "종목 목록을 가져올 수 없습니다"
            return

        _update_progress("데이터 수집", 10,
                         f"{len(stocks)}개 종목의 일봉 데이터를 수집합니다...")

        all_stocks_data = []
        batch_size = 10
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i + batch_size]
            for stock in batch:
                try:
                    candles = get_daily_candles(stock["code"], period=250)
                    if candles and len(candles) >= 60:
                        all_stocks_data.append({
                            "code": stock["code"],
                            "name": stock["name"],
                            "market": stock.get("market", ""),
                            "candles": candles,
                        })
                except Exception as e:
                    pass  # 개별 종목 실패는 무시

            pct = 10 + int((i / len(stocks)) * 30)
            _update_progress("데이터 수집", pct,
                             f"{len(all_stocks_data)}/{len(stocks)} 종목 수집 완료")
            await asyncio.sleep(0.1)  # Rate limit 방지

        _update_progress("데이터 수집", 40,
                         f"총 {len(all_stocks_data)}개 종목 데이터 수집 완료")

        # ── Step 2: 우승 종목 프로필 구축 ──
        _update_progress("패턴 분석", 45,
                         f"상승률 {rise_threshold}% 이상 종목의 공통 패턴을 분석합니다...")

        winner_profile = build_winner_profile(all_stocks_data, rise_threshold)
        _update_progress("패턴 분석", 55,
                         f"{winner_profile.get('total_winners', 0)}개 우승 패턴 발견")

        # ── Step 3: 현재 후보 종목 발굴 ──
        _update_progress("종목 발굴", 60, "현재 조건 충족 종목을 스캔합니다...")

        candidates = discover_swing_candidates(
            all_stocks_data, winner_profile, top_n=top_n
        )
        _update_progress("종목 발굴", 65,
                         f"{len(candidates)}개 후보 종목 발굴 완료")

        # ── Step 4: 백테스트 + 패턴 통계 ──
        _update_progress("백테스트", 70, "시뮬레이션을 실행합니다...")

        all_trades_sim = []
        candles_dict = {}
        for stock in all_stocks_data[:100]:  # 상위 100종목만
            candles = stock["candles"]
            trades = run_swing_backtest(candles)
            for t in trades:
                t.entry_conditions["stock_code"] = stock["code"]
                t.entry_conditions["stock_name"] = stock["name"]
            all_trades_sim.extend(trades)
            candles_dict[stock["code"]] = candles

        pattern_stats = analyze_timing_patterns(all_trades_sim)
        _update_progress("백테스트", 80,
                         f"{len(all_trades_sim)}건 시뮬레이션 완료")

        # ── Step 5: 자동 교정 ──
        _update_progress("자동 교정", 85,
                         f"{generations}세대 진화 최적화를 시작합니다...")

        # 상위 종목 30개만으로 교정 (속도)
        top_candles = {k: v for k, v in list(candles_dict.items())[:30]}
        calibration = auto_calibrate(
            top_candles,
            generations=generations,
            population_size=20
        )
        _update_progress("자동 교정", 95, "최적 파라미터 도출 완료")

        # ── Step 6: 최적 파라미터로 최종 백테스트 ──
        _update_progress("최종 검증", 97, "최적 파라미터로 재검증합니다...")

        best_params = calibration["best_params"]
        final_trades = []
        for code, candles in list(candles_dict.items())[:50]:
            trades = run_swing_backtest(candles, best_params)
            for t in trades:
                t.entry_conditions["stock_code"] = code
            final_trades.extend(trades)

        final_stats = analyze_timing_patterns(final_trades)

        # ── 결과 저장 ──
        result = {
            "timestamp": datetime.now().isoformat(),
            "stocks_analyzed": len(all_stocks_data),
            "winner_profile": {
                "total_winners": winner_profile.get("total_winners", 0),
                "rise_threshold": rise_threshold,
                "top_conditions": winner_profile.get("top_conditions", []),
                "condition_stats": winner_profile.get("condition_stats", {}),
            },
            "candidates": candidates,
            "pattern_stats": _serialize_pattern_stats(pattern_stats),
            "calibration": {
                "best_params": calibration["best_params"],
                "best_metrics": calibration["best_metrics"],
                "generations": calibration["generations"],
            },
            "final_stats": _serialize_pattern_stats(final_stats),
        }

        # DB에 저장
        try:
            db.table("swing_analysis").insert({
                "analysis_date": date.today().isoformat(),
                "result": json.dumps(result, ensure_ascii=False, default=str),
                "best_params": json.dumps(calibration["best_params"]),
                "created_at": datetime.now().isoformat(),
            }).execute()
        except Exception:
            pass  # DB 저장 실패해도 메모리에는 결과 있음

        _progress["status"] = "done"
        _progress["pct"] = 100
        _progress["step"] = "완료"
        _progress["message"] = "분석이 완료되었습니다"
        _progress["result"] = result

    except Exception as e:
        _progress["status"] = "error"
        _progress["error"] = str(e)
        _progress["message"] = f"오류 발생: {str(e)}"
        print(f"[스윙분석 오류] {traceback.format_exc()}")


def _update_progress(step, pct, message):
    _progress["step"] = step
    _progress["pct"] = pct
    _progress["message"] = message


def _serialize_pattern_stats(stats):
    """TradeSimulation 직렬화"""
    serialized = {}
    for key, val in stats.items():
        if key == "equity_curve":
            serialized[key] = val
        elif isinstance(val, list):
            serialized[key] = val
        elif isinstance(val, dict):
            serialized[key] = val
        else:
            serialized[key] = val
    return serialized


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. 진행 상태 조회 / Get Progress
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/progress")
async def get_progress():
    """분석 진행 상태"""
    return {
        "status": _progress["status"],
        "step": _progress["step"],
        "pct": _progress["pct"],
        "message": _progress["message"],
        "error": _progress["error"],
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. 결과 조회 / Get Results
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/result")
async def get_result():
    """최신 분석 결과"""
    # 메모리에 있으면 반환
    if _progress["result"]:
        return _progress["result"]

    # DB에서 가져오기
    try:
        data = db.table("swing_analysis") \
            .select("*") \
            .order("created_at", desc=True) \
            .limit(1) \
            .execute().data

        if data:
            return json.loads(data[0]["result"])
    except Exception:
        pass

    return {"error": "분석 결과가 없습니다. 먼저 분석을 실행해주세요."}


@router.get("/result/candidates")
async def get_candidates():
    """발굴된 후보 종목만"""
    result = await get_result()
    return result.get("candidates", [])


@router.get("/result/pattern-stats")
async def get_pattern_stats():
    """패턴 통계만"""
    result = await get_result()
    return result.get("pattern_stats", {})


@router.get("/result/calibration")
async def get_calibration():
    """자동 교정 결과만"""
    result = await get_result()
    return result.get("calibration", {})


@router.get("/result/winner-profile")
async def get_winner_profile():
    """우승 종목 프로필만"""
    result = await get_result()
    return result.get("winner_profile", {})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. 이력 조회 / History
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/history")
async def get_history(limit: int = 10):
    """과거 분석 이력"""
    try:
        data = db.table("swing_analysis") \
            .select("id, analysis_date, best_params, created_at") \
            .order("created_at", desc=True) \
            .limit(limit) \
            .execute().data
        return data or []
    except Exception:
        return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 단일 종목 테스트 / Single Stock Test
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/test/{code}")
async def test_single_stock(code: str,
                            trailing_pct: float = 5.0,
                            stop_loss_pct: float = -7.0,
                            pullback_min: float = 3.0,
                            pullback_max: float = 8.0):
    """단일 종목 스윙 백테스트"""
    from app.engine.swing_pattern_stats import run_swing_backtest, analyze_timing_patterns

    try:
        candles = get_daily_candles(code, period=250)
        if not candles or len(candles) < 60:
            return {"error": "일봉 데이터가 부족합니다"}

        params = {
            "trailing_pct": trailing_pct,
            "stop_loss_pct": stop_loss_pct,
            "pullback_min": pullback_min,
            "pullback_max": pullback_max,
        }

        trades = run_swing_backtest(candles, params)
        stats = analyze_timing_patterns(trades)

        # 매매 포인트 (차트 표시용)
        trade_points = []
        for t in trades:
            trade_points.append({
                "type": "buy",
                "date": t.entry_date,
                "price": t.entry_price,
                "idx": t.entry_idx,
            })
            trade_points.append({
                "type": "sell",
                "date": t.exit_date,
                "price": t.exit_price,
                "idx": t.exit_idx,
                "profit_pct": t.profit_pct,
                "is_win": t.is_win,
                "reason": t.exit_reason,
            })

        return {
            "code": code,
            "candles": candles[-120:],  # 최근 120일만
            "trade_points": trade_points,
            "stats": _serialize_pattern_stats(stats),
            "params": params,
        }

    except Exception as e:
        return {"error": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 헬퍼 / Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _fetch_stock_list() -> List[Dict]:
    """전종목 리스트 가져오기 (기존 스캐너 활용)"""
    try:
        from app.engine.scanner import scan_all_stocks
        stocks = await scan_all_stocks()
        # 기본 필터: 거래량 10만주 이상, 가격 1000원 이상
        filtered = [
            s for s in stocks
            if s.get("volume", 0) >= 100000 and s.get("price", 0) >= 1000
        ]
        return filtered[:200]  # 상위 200종목만 (서버 부하 방지)
    except Exception as e:
        print(f"[스윙] 종목 리스트 가져오기 실패: {e}")
        # fallback: DB의 watchlist에서
        try:
            data = db.table("watchlist").select("code, name, market").execute().data
            return data or []
        except:
            return []
