"""백테스트 API 라우트 / Backtest API Routes
backtest_engine.run_backtest()를 호출하여 실제 시뮬레이션 실행
"""
from fastapi import APIRouter, HTTPException
from app.engine.backtest_engine import run_backtest

router = APIRouter(prefix="/api/backtest", tags=["백테스트"])


# ============================================================
# 프리셋 정의 / Preset Definitions
# ============================================================
PRESETS = {
    "conservative": {
        "name": "보수적",
        "strategy": "dip",
        "atr_multiplier": 1.5,
        "stop_loss_pct": 2.0,
        "max_holdings": 3,
        "per_trade_pct": 15.0,
        "description": "ATR×1.5, 손절-2%, 최소신호 4개",
    },
    "standard": {
        "name": "기본형",
        "strategy": "dip",
        "atr_multiplier": 2.0,
        "stop_loss_pct": 3.0,
        "max_holdings": 5,
        "per_trade_pct": 20.0,
        "description": "ATR×2.0, 손절-3%, 최소신호 3개",
    },
    "aggressive": {
        "name": "공격적",
        "strategy": "dip",
        "atr_multiplier": 2.5,
        "stop_loss_pct": 4.0,
        "max_holdings": 7,
        "per_trade_pct": 25.0,
        "description": "ATR×2.5, 손절-4%, 최소신호 2개",
    },
    "balanced": {
        "name": "균형형",
        "strategy": "dip",
        "atr_multiplier": 2.0,
        "stop_loss_pct": 3.0,
        "max_holdings": 5,
        "per_trade_pct": 20.0,
        "description": "ATR×2.0, 손절-3%, 균형 전략",
    },
    "gap": {
        "name": "갭상승",
        "strategy": "gap",
        "atr_multiplier": 1.5,
        "stop_loss_pct": 2.5,
        "max_holdings": 5,
        "per_trade_pct": 20.0,
        "description": "ATR×1.5, 손절-2.5%, 갭상승전략",
    },
    "combined": {
        "name": "혼합",
        "strategy": "both",
        "atr_multiplier": 2.0,
        "stop_loss_pct": 3.0,
        "max_holdings": 5,
        "per_trade_pct": 20.0,
        "description": "눌림목+갭상승 혼합전략",
    },
}


# ============================================================
# 프리셋 백테스트 실행 / Run preset backtest
# ============================================================
@router.api_route("/quick/{preset}", methods=["GET", "POST"])
async def quick_backtest(preset: str):
    """프리셋 백테스트 실행 - backtest_engine 호출"""
    if preset not in PRESETS:
        raise HTTPException(400, f"알 수 없는 프리셋: {preset}. 사용 가능: {list(PRESETS.keys())}")

    p = PRESETS[preset]

    try:
        result = await run_backtest(
            strategy=p["strategy"],
            stock_codes=None,
            initial_capital=1_000_000,
            atr_multiplier=p["atr_multiplier"],
            stop_loss_pct=p["stop_loss_pct"],
            max_holdings=p["max_holdings"],
            per_trade_pct=p["per_trade_pct"],
        )

        # 프리셋 정보 추가
        if result and "summary" in result:
            result["summary"]["preset"] = preset
            result["summary"]["preset_name"] = p["name"]
            result["summary"]["description"] = p["description"]

        return result

    except Exception as e:
        print(f"[백테스트 오류] {preset}: {e}")
        return {
            "summary": {
                "preset": preset,
                "preset_name": p["name"],
                "description": p["description"],
                "initial_capital": 1000000,
                "final_asset": 1000000,
                "total_return_pct": 0,
                "total_profit": 0,
                "total_trades": 0,
                "win_count": 0,
                "loss_count": 0,
                "win_rate": 0,
                "avg_win": 0,
                "avg_loss": 0,
                "max_drawdown_pct": 0,
                "profit_loss_ratio": 0,
                "test_days": 0,
                "atr_multiplier": p["atr_multiplier"],
                "stop_loss_pct": p["stop_loss_pct"],
            },
            "trades": [],
            "daily_assets": [],
            "error": str(e),
        }


# ============================================================
# 커스텀 백테스트 실행 / Run custom backtest
# ============================================================
@router.api_route("/custom", methods=["GET", "POST"])
async def custom_backtest(
    strategy: str = "dip",
    stock_codes: str = "",
    initial_capital: int = 1000000,
    atr_multiplier: float = 2.0,
    stop_loss_pct: float = 3.0,
    max_holdings: int = 5,
    per_trade_pct: float = 20.0,
):
    """커스텀 파라미터로 백테스트 실행"""
    codes = [c.strip() for c in stock_codes.split(",") if c.strip()] if stock_codes else None

    try:
        result = await run_backtest(
            strategy=strategy,
            stock_codes=codes,
            initial_capital=initial_capital,
            atr_multiplier=atr_multiplier,
            stop_loss_pct=stop_loss_pct,
            max_holdings=max_holdings,
            per_trade_pct=per_trade_pct,
        )
        return result

    except Exception as e:
        print(f"[커스텀 백테스트 오류] {e}")
        return {
            "summary": {
                "strategy": strategy,
                "initial_capital": initial_capital,
                "final_asset": initial_capital,
                "total_return_pct": 0,
                "total_profit": 0,
                "total_trades": 0,
                "win_count": 0,
                "loss_count": 0,
                "win_rate": 0,
                "atr_multiplier": atr_multiplier,
                "stop_loss_pct": stop_loss_pct,
            },
            "trades": [],
            "daily_assets": [],
            "error": str(e),
        }


# ============================================================
# 백테스트 이력 조회 / Get backtest history
# ============================================================
@router.api_route("/history", methods=["GET", "POST"])
async def backtest_history():
    """저장된 백테스트 결과 이력 조회"""
    try:
        from app.core.database import db
        results = db.table("backtest_results").select("*").order("created_at", desc=True).limit(20).execute()
        return results.data or []
    except Exception:
        return []
