"""
스윙 백테스트 API 라우트 / Swing Backtest API Routes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
기존 라우트를 전혀 수정하지 않는 독립 API 모듈입니다.
main.py에 app.include_router(swing_router) 한 줄만 추가하면 됩니다.

[변경사항 / Changes]
- KIS API get_daily_candles → 네이버 get_daily_candles_naver 교체
- 배치 수집 시 delay 추가 (네이버 서버 보호)
- 종목 리스트 수집 안정화 (fallback 강화)

파일 경로: app/api/swing_routes.py
"""

from fastapi import APIRouter, BackgroundTasks, Query
from typing import List, Dict
from datetime import datetime, timedelta, date
import asyncio
import json
import traceback

from app.core.database import db
# ★ 변경: KIS → 네이버 일봉 수집기
from app.services.naver_stock import get_daily_candles_naver

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
    1) 전종목 일봉 수집 (네이버 금융)
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

        # ── Step 1: 전종목 일봉 데이터 수집 (네이버 금융) ──
        _update_progress("데이터 수집", 5, "종목 목록을 가져옵니다...")
        stocks = await _fetch_stock_list()

        if not stocks:
            _progress["status"] = "error"
            _progress["error"] = "종목 목록을 가져올 수 없습니다"
            return

        _update_progress("데이터 수집", 10,
                         f"{len(stocks)}개 종목의 일봉 데이터를 네이버에서 수집합니다...")

        all_stocks_data = []
        total = len(stocks)
        for i, stock in enumerate(stocks):
            try:
                # ★ 변경: KIS → 네이버 일봉 수집
                candles = get_daily_candles_naver(stock["code"], count=250)
                if candles and len(candles) >= 60:
                    all_stocks_data.append({
                        "code": stock["code"],
                        "name": stock.get("name", stock["code"]),
                        "market": stock.get("market", ""),
                        "candles": candles,
                    })
            except Exception as e:
                print(f"[스윙] {stock['code']} 일봉 수집 실패: {e}")

            # 진행률 업데이트 (10개마다)
            if (i + 1) % 10 == 0 or i == total - 1:
                pct = 10 + int((i / total) * 30)
                _update_progress("데이터 수집", pct,
                                 f"{len(all_stocks_data)}/{i + 1} 종목 수집 완료")

            # ★ 네이버 서버 보호 (0.05초 간격)
            await asyncio.sleep(0.05)

        if not all_stocks_data:
            _progress["status"] = "error"
            _progress["error"] = "일봉 데이터를 수집하지 못했습니다"
            return

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
            "data_source": "naver",  # ★ 추가: 데이터 소스 표시
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
        except Exception as e:
            print(f"[스윙] DB 저장 실패 (무시): {e}")

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
        # ★ 변경: KIS → 네이버 일봉
        candles = get_daily_candles_naver(code, count=250)
        if not candles or len(candles) < 60:
            return {"error": f"일봉 데이터가 부족합니다 ({len(candles) if candles else 0}개)"}

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
            "data_source": "naver",  # ★ 추가
            "total_candles": len(candles),
            "candles": candles[-120:],  # 최근 120일만
            "trade_points": trade_points,
            "stats": _serialize_pattern_stats(stats),
            "params": params,
        }

    except Exception as e:
        print(f"[스윙 단일테스트] {code} 오류: {traceback.format_exc()}")
        return {"error": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 헬퍼 / Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _fetch_stock_list() -> List[Dict]:
    """
    전종목 리스트 가져오기
    우선순위: 1) scanner.py → 2) DB watchlist → 3) 하드코딩 대표 종목

    Returns: [{"code": "005930", "name": "삼성전자", "market": "kospi"}, ...]
    """
    # 방법 1: 기존 스캐너 활용
    try:
        from app.engine.scanner import scan_all_stocks
        stocks = await scan_all_stocks()
        if stocks and len(stocks) > 0:
            # 기본 필터: 거래량 10만주 이상, 가격 1000원 이상
            filtered = [
                s for s in stocks
                if s.get("volume", 0) >= 100000 and s.get("price", 0) >= 1000
            ]
            if filtered:
                result = filtered[:200]  # 상위 200종목 (서버 부하 방지)
                print(f"[스윙] 스캐너에서 {len(result)}개 종목 확보")
                return result
    except Exception as e:
        print(f"[스윙] 스캐너 실패: {e}")

    # 방법 2: DB watchlist
    try:
        data = db.table("watchlist").select("code, name, market").execute().data
        if data and len(data) > 0:
            print(f"[스윙] DB watchlist에서 {len(data)}개 종목 확보")
            return data
    except Exception as e:
        print(f"[스윙] DB watchlist 실패: {e}")

    # 방법 3: DB candidates (이전 분석 결과)
    try:
        data = db.table("candidates").select("code, name, market").execute().data
        if data and len(data) > 0:
            print(f"[스윙] DB candidates에서 {len(data)}개 종목 확보")
            return data
    except Exception:
        pass

    # 방법 4: 대표 종목 하드코딩 (최후의 수단)
    print("[스윙] 종목 목록 fallback: 대표 50종목 사용")
    return [
        {"code": "005930", "name": "삼성전자", "market": "kospi"},
        {"code": "000660", "name": "SK하이닉스", "market": "kospi"},
        {"code": "373220", "name": "LG에너지솔루션", "market": "kospi"},
        {"code": "207940", "name": "삼성바이오로직스", "market": "kospi"},
        {"code": "005380", "name": "현대차", "market": "kospi"},
        {"code": "006400", "name": "삼성SDI", "market": "kospi"},
        {"code": "035420", "name": "NAVER", "market": "kospi"},
        {"code": "000270", "name": "기아", "market": "kospi"},
        {"code": "068270", "name": "셀트리온", "market": "kospi"},
        {"code": "035720", "name": "카카오", "market": "kospi"},
        {"code": "051910", "name": "LG화학", "market": "kospi"},
        {"code": "105560", "name": "KB금융", "market": "kospi"},
        {"code": "055550", "name": "신한지주", "market": "kospi"},
        {"code": "003670", "name": "포스코퓨처엠", "market": "kospi"},
        {"code": "096770", "name": "SK이노베이션", "market": "kospi"},
        {"code": "028260", "name": "삼성물산", "market": "kospi"},
        {"code": "012330", "name": "현대모비스", "market": "kospi"},
        {"code": "066570", "name": "LG전자", "market": "kospi"},
        {"code": "003550", "name": "LG", "market": "kospi"},
        {"code": "034730", "name": "SK", "market": "kospi"},
        {"code": "015760", "name": "한국전력", "market": "kospi"},
        {"code": "032830", "name": "삼성생명", "market": "kospi"},
        {"code": "011200", "name": "HMM", "market": "kospi"},
        {"code": "010130", "name": "고려아연", "market": "kospi"},
        {"code": "033780", "name": "KT&G", "market": "kospi"},
        {"code": "009150", "name": "삼성전기", "market": "kospi"},
        {"code": "018260", "name": "삼성에스디에스", "market": "kospi"},
        {"code": "086790", "name": "하나금융지주", "market": "kospi"},
        {"code": "316140", "name": "우리금융지주", "market": "kospi"},
        {"code": "017670", "name": "SK텔레콤", "market": "kospi"},
        {"code": "030200", "name": "KT", "market": "kospi"},
        {"code": "010950", "name": "S-Oil", "market": "kospi"},
        {"code": "247540", "name": "에코프로비엠", "market": "kosdaq"},
        {"code": "086520", "name": "에코프로", "market": "kosdaq"},
        {"code": "377300", "name": "카카오페이", "market": "kospi"},
        {"code": "259960", "name": "크래프톤", "market": "kospi"},
        {"code": "352820", "name": "하이브", "market": "kospi"},
        {"code": "263750", "name": "펄어비스", "market": "kospi"},
        {"code": "112040", "name": "위메이드", "market": "kosdaq"},
        {"code": "041510", "name": "에스엠", "market": "kosdaq"},
        {"code": "293490", "name": "카카오게임즈", "market": "kosdaq"},
        {"code": "036570", "name": "엔씨소프트", "market": "kospi"},
        {"code": "251270", "name": "넷마블", "market": "kospi"},
        {"code": "090430", "name": "아모레퍼시픽", "market": "kospi"},
        {"code": "005490", "name": "POSCO홀딩스", "market": "kospi"},
        {"code": "042700", "name": "한미반도체", "market": "kosdaq"},
        {"code": "196170", "name": "알테오젠", "market": "kosdaq"},
        {"code": "000100", "name": "유한양행", "market": "kospi"},
        {"code": "004020", "name": "현대제철", "market": "kospi"},
        {"code": "009540", "name": "HD한국조선해양", "market": "kospi"},
    ]
