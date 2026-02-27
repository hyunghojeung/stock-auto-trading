"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  pattern_routes.py 수정 가이드
  pattern_routes.py Modification Guide
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[변경 1] 기존 import에 추가:
  import json
  from datetime import datetime

[변경 2] 기존 /result 엔드포인트의 분석 완료 시 DB 저장 추가

[변경 3] 새 엔드포인트 2개 추가:
  - GET /api/pattern/previous  (이전 결과 목록)
  - GET /api/pattern/previous/{session_id}  (특정 결과 상세)
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# [변경 2] 기존 /api/pattern/result 엔드포인트 수정
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# 기존 코드에서 result를 반환하기 직전에 DB 저장 로직을 추가합니다.
# 아래는 수정된 전체 /result 엔드포인트입니다.
# (기존 /result 엔드포인트를 이것으로 교체하세요)

"""
@router.get("/result")
async def get_result():
    if _analysis_state.get("running"):
        return {"status": "running", "message": "분석 진행 중..."}

    result = _analysis_state.get("result")
    if not result:
        return {"status": "no_result", "message": "분석 결과가 없습니다."}

    # ━━━ DB 저장 (최초 1회만) ━━━
    if not _analysis_state.get("saved_to_db"):
        try:
            from app.services.supabase_client import supabase

            # 분석에 사용된 파라미터 복원
            params = _analysis_state.get("params", {})
            codes = _analysis_state.get("codes", [])
            names = _analysis_state.get("names", {})

            # 패턴 수 계산
            pattern_count = 0
            if "stock_results" in result:
                for sr in result["stock_results"]:
                    pattern_count += len(sr.get("surge_episodes", []))

            save_data = {
                "preset": params.get("preset", "custom"),
                "params": params,
                "stock_codes": codes,
                "stock_names": names,
                "stock_count": len(codes),
                "pattern_count": pattern_count,
                "result_summary": {
                    "stock_count": len(codes),
                    "pattern_count": pattern_count,
                    "common_patterns": len(result.get("common_patterns", [])),
                    "buy_signals": len(result.get("buy_signals", [])),
                },
                "full_result": result,
            }

            supabase.table("pattern_analysis_sessions").insert(save_data).execute()
            _analysis_state["saved_to_db"] = True
            print(f"✅ 패턴 분석 결과 DB 저장 완료 ({len(codes)}종목, {pattern_count}패턴)")
        except Exception as e:
            print(f"⚠️ 패턴 분석 결과 DB 저장 실패: {e}")

    return {"status": "done", **result}
"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# [변경 3] 새 엔드포인트 추가 — 파일 하단에 추가
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# ── 이전 분석 결과 목록 (최근 10개) ──────────────────
@router.get("/previous")
async def get_previous_results():
    """
    이전 패턴 분석 결과 목록 (최근 10개)
    Previous pattern analysis results (last 10)
    """
    try:
        from app.services.supabase_client import supabase

        resp = (
            supabase.table("pattern_analysis_sessions")
            .select("id, created_at, preset, stock_codes, stock_names, stock_count, pattern_count, result_summary")
            .order("created_at", desc=True)
            .limit(10)
            .execute()
        )

        sessions = resp.data or []
        return {
            "status": "ok",
            "sessions": sessions,
            "count": len(sessions),
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "sessions": []}


# ── 특정 분석 결과 상세 조회 ─────────────────────────
@router.get("/previous/{session_id}")
async def get_previous_result_detail(session_id: int):
    """
    특정 분석 세션의 전체 결과 조회
    Get full result of a specific analysis session
    """
    try:
        from app.services.supabase_client import supabase

        resp = (
            supabase.table("pattern_analysis_sessions")
            .select("*")
            .eq("id", session_id)
            .single()
            .execute()
        )

        if not resp.data:
            return {"status": "not_found", "message": f"세션 {session_id}을 찾을 수 없습니다."}

        session = resp.data
        full_result = session.get("full_result", {})

        return {
            "status": "done",
            "session_id": session["id"],
            "created_at": session["created_at"],
            "preset": session.get("preset"),
            "stock_names": session.get("stock_names", {}),
            "params": session.get("params", {}),
            **full_result,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# [변경 2 보충] _analysis_state 초기화 시 saved_to_db 추가
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# 기존 analyze 엔드포인트 (POST /api/pattern/analyze)에서
# _analysis_state를 초기화하는 부분에 아래 필드를 추가:
#
#   _analysis_state["saved_to_db"] = False
#   _analysis_state["params"] = { ...요청 파라미터... }
#   _analysis_state["codes"] = codes
#   _analysis_state["names"] = names
#
# 예시:
"""
@router.post("/analyze")
async def start_analysis(req: AnalyzeRequest, background_tasks: BackgroundTasks):
    if _analysis_state.get("running"):
        raise HTTPException(status_code=409, detail="이미 분석이 진행 중입니다.")

    _analysis_state["running"] = True
    _analysis_state["progress"] = 0
    _analysis_state["message"] = "분석 시작..."
    _analysis_state["result"] = None
    _analysis_state["error"] = None
    _analysis_state["saved_to_db"] = False          # ← 추가
    _analysis_state["codes"] = req.codes             # ← 추가
    _analysis_state["names"] = req.names             # ← 추가
    _analysis_state["params"] = {                    # ← 추가
        "preset": getattr(req, "preset", "custom"),
        "period_days": req.period_days,
        "pre_rise_days": req.pre_rise_days,
        "rise_pct": req.rise_pct,
        "rise_window": req.rise_window,
        "weight_returns": req.weight_returns,
        "weight_candle": req.weight_candle,
        "weight_volume": req.weight_volume,
    }

    background_tasks.add_task(run_analysis, req)
    return {"status": "started"}
"""
