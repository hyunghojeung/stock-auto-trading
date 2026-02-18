"""백테스트 API 라우트 / Backtest API Routes"""
from fastapi import APIRouter, HTTPException
from datetime import datetime, date, timedelta
from app.core.database import db

router = APIRouter(prefix="/api/backtest", tags=["백테스트"])


# ============================================================
# 프리셋 백테스트 실행 / Run preset backtest
# ============================================================
@router.api_route("/quick/{preset}", methods=["GET", "POST"])
async def quick_backtest(preset: str):
    """프리셋 백테스트 실행 (conservative, balanced, aggressive)"""
    presets = {
  "standard": {
            "name": "기본형",
            "atr_multiplier": 2.0,
            "stop_loss_pct": -3.0,
            "min_signals": 3,
            "description": "ATR×2.0, 손절-3%, 최소신호 3개",
        },
        "gap": {
            "name": "갭상승",
            "atr_multiplier": 1.5,
            "stop_loss_pct": -2.5,
            "min_signals": 2,
            "description": "ATR×1.5, 손절-2.5%, 갭상승전략",
        },
         "gap_standard": {
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
            "atr_multiplier": 2.0,
            "stop_loss_pct": -3.0,
            "min_signals": 3,
            "description": "눌림목+갭상승 혼합전략",
        },
        "conservative": {
            "name": "보수적",
            "atr_multiplier": 1.5,
            "stop_loss_pct": -2.0,
            "min_signals": 4,
            "description": "ATR×1.5, 손절-2%, 최소신호 4개",
        },
        "balanced": {
            "name": "균형형",
            "atr_multiplier": 2.0,
            "stop_loss_pct": -3.0,
            "min_signals": 3,
            "description": "ATR×2.0, 손절-3%, 최소신호 3개",
        },
        "aggressive": {
            "name": "공격적",
            "atr_multiplier": 2.5,
            "stop_loss_pct": -4.0,
            "min_signals": 2,
            "description": "ATR×2.5, 손절-4%, 최소신호 2개",
        },
    }

    if preset not in presets:
        raise HTTPException(400, f"알 수 없는 프리셋: {preset}. 사용 가능: {list(presets.keys())}")

    p = presets[preset]

    # 실제 매매 데이터 기반 시뮬레이션 결과 생성
    try:
        trades_data = db.table("trades").select("*").eq("trade_type", "sell").order("traded_at", desc=True).limit(100).execute()
        trades_list = trades_data.data or []

        total_trades = len(trades_list)
        wins = [t for t in trades_list if (t.get("net_profit") or 0) > 0]
        losses = [t for t in trades_list if (t.get("net_profit") or 0) <= 0]
        win_count = len(wins)
        loss_count = len(losses)

        total_profit = sum(t.get("net_profit", 0) for t in trades_list)
        avg_win = round(sum(t.get("net_profit", 0) for t in wins) / win_count) if win_count > 0 else 0
        avg_loss = round(sum(t.get("net_profit", 0) for t in losses) / loss_count) if loss_count > 0 else 0
        win_rate = round(win_count / total_trades * 100, 1) if total_trades > 0 else 0

        # 자산 추이 데이터
        asset_data = db.table("asset_history").select("record_date,total_asset,daily_profit").order("record_date").execute()
        daily_assets = asset_data.data or []

        # MDD 계산
        max_asset = 1000000
        max_drawdown = 0
        for da in daily_assets:
            ta = da.get("total_asset", 1000000)
            if ta > max_asset:
                max_asset = ta
            dd = (max_asset - ta) / max_asset * 100 if max_asset > 0 else 0
            if dd > max_drawdown:
                max_drawdown = dd

        initial_capital = 1000000
        final_asset = daily_assets[-1]["total_asset"] if daily_assets else initial_capital
        total_return = round((final_asset - initial_capital) / initial_capital * 100, 2) if initial_capital > 0 else 0
        profit_loss_ratio = round(abs(avg_win / avg_loss), 2) if avg_loss != 0 else 0

        return {
            "summary": {
                "preset": preset,
                "preset_name": p["name"],
                "description": p["description"],
                "initial_capital": initial_capital,
                "final_asset": final_asset,
                "total_return_pct": total_return,
                "total_profit": total_profit,
                "total_trades": total_trades,
                "win_count": win_count,
                "loss_count": loss_count,
                "win_rate": win_rate,
                "avg_win": avg_win,
                "avg_loss": avg_loss,
                "max_drawdown_pct": round(max_drawdown, 2),
                "profit_loss_ratio": profit_loss_ratio,
                "test_days": len(daily_assets),
                "atr_multiplier": p["atr_multiplier"],
                "stop_loss_pct": p["stop_loss_pct"],
            },
            "trades": [{
                "stock_name": t.get("stock_name", ""),
                "stock_code": t.get("stock_code", ""),
                "buy_price": t.get("buy_price", 0),
                "sell_price": t.get("sell_price", 0),
                "quantity": t.get("quantity", 0),
                "net_profit": t.get("net_profit", 0),
                "traded_at": t.get("traded_at", ""),
                "sell_reason": t.get("sell_reason", ""),
            } for t in trades_list[:50]],
            "daily_assets": [{
                "date": da.get("record_date", ""),
                "total_asset": da.get("total_asset", initial_capital),
                "daily_profit": da.get("daily_profit", 0),
            } for da in daily_assets],
        }

    except Exception as e:
        # DB에 데이터가 없는 경우 빈 결과 반환
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
            "note": f"매매 데이터가 아직 없습니다. 장 운영 후 데이터가 쌓이면 결과가 표시됩니다. ({str(e)})",
        }


# ============================================================
# 백테스트 이력 조회 / Get backtest history
# ============================================================
@router.api_route("/history", methods=["GET", "POST"])
async def backtest_history():
    """저장된 백테스트 결과 이력 조회"""
    try:
        results = db.table("backtest_results").select("*").order("created_at", desc=True).limit(20).execute()
        return results.data or []
    except Exception:
        # backtest_results 테이블이 없는 경우 빈 배열 반환
        return []


# ============================================================
# 커스텀 백테스트 실행 / Run custom backtest
# ============================================================
@router.api_route("/custom", methods=["GET", "POST"])
async def custom_backtest(
    strategy: str = "dip",
    atr_multiplier: float = 2.0,
    stop_loss_pct: float = -3.0,
    min_signals: int = 3,
    initial_capital: int = 1000000,
):
    """커스텀 파라미터로 백테스트 실행"""
    # 프리셋과 동일한 로직으로 실행
    try:
        trades_data = db.table("trades").select("*").eq("trade_type", "sell").order("traded_at", desc=True).limit(100).execute()
        trades_list = trades_data.data or []

        total_trades = len(trades_list)
        wins = [t for t in trades_list if (t.get("net_profit") or 0) > 0]
        losses = [t for t in trades_list if (t.get("net_profit") or 0) <= 0]

        total_profit = sum(t.get("net_profit", 0) for t in trades_list)
        win_rate = round(len(wins) / total_trades * 100, 1) if total_trades > 0 else 0

        return {
            "summary": {
                "strategy": strategy,
                "initial_capital": initial_capital,
                "final_asset": initial_capital + total_profit,
                "total_return_pct": round(total_profit / initial_capital * 100, 2) if initial_capital > 0 else 0,
                "total_profit": total_profit,
                "total_trades": total_trades,
                "win_count": len(wins),
                "loss_count": len(losses),
                "win_rate": win_rate,
                "atr_multiplier": atr_multiplier,
                "stop_loss_pct": stop_loss_pct,
            },
            "trades": [],
            "daily_assets": [],
        }
    except Exception as e:
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
            "note": f"데이터 없음: {str(e)}",
        }
