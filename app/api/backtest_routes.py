"""백테스트 API 라우트 / Backtest API Routes"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime, timedelta
from app.engine.backtest_engine import run_backtest
from app.core.database import db

router = APIRouter(prefix="/api/backtest", tags=["백테스트"])


# ============================================================
# 요청/응답 모델 / Request/Response Models
# ============================================================
class BacktestRequest(BaseModel):
    """백테스트 실행 요청 / Backtest run request"""
    strategy: str = "dip"                    # "dip" | "gap" | "both"
    stock_code: Optional[str] = None         # 특정 종목 (없으면 감시종목 전체)
    start_date: Optional[str] = None         # "2026-01-01" (없으면 30일 전)
    end_date: Optional[str] = None           # "2026-02-18" (없으면 오늘)
    initial_capital: int = 1_000_000         # 초기 자금
    atr_multiplier: float = 2.0             # ATR 배수 (익절)
    stop_loss_pct: float = 3.0              # 손절 %
    max_holdings: int = 5                    # 최대 동시 보유 종목수
    per_trade_pct: float = 20.0             # 1회 매수 비중 (%)


# ============================================================
# 1. 백테스트 실행 / Run Backtest
# ============================================================
@router.post("/run")
async def run_backtest_api(req: BacktestRequest):
    """
    백테스트 실행 API
    Run backtest simulation with given parameters
    
    - 눌림목/갭상승/둘다 전략 선택 가능
    - KIS API 분봉 데이터 기반 (최근 30일)
    - 수수료 0.015% + 세금 0.18% 반영
    """
    try:
        # 날짜 기본값 설정 / Set default dates
        end_dt = date.fromisoformat(req.end_date) if req.end_date else date.today()
        start_dt = date.fromisoformat(req.start_date) if req.start_date else end_dt - timedelta(days=30)
        
        # KIS API 분봉 데이터 30일 제한 체크
        if (end_dt - start_dt).days > 30:
            raise HTTPException(
                status_code=400,
                detail="KIS API 분봉 데이터는 최근 30일까지만 가능합니다 / Max 30 days for minute candle data"
            )
        
        # 종목 리스트 결정 / Determine stock list
        stock_codes = []
        if req.stock_code:
            stock_codes = [req.stock_code]
        else:
            # 감시종목에서 가져오기 / Get from watchlist
            watchlist = db.table("watchlist").select("stock_code, stock_name, score") \
                .order("score", desc=True).limit(20).execute()
            if watchlist.data:
                stock_codes = [w["stock_code"] for w in watchlist.data]
            else:
                # 감시종목 없으면 기본 대형주 / Default blue chips
                stock_codes = ["005930", "000660", "035420", "035720", "051910"]
        
        # 백테스트 실행 / Run backtest
        result = await run_backtest(
            strategy=req.strategy,
            stock_codes=stock_codes,
            start_date=start_dt.isoformat(),
            end_date=end_dt.isoformat(),
            initial_capital=req.initial_capital,
            atr_multiplier=req.atr_multiplier,
            stop_loss_pct=req.stop_loss_pct,
            max_holdings=req.max_holdings,
            per_trade_pct=req.per_trade_pct,
        )
        
        return {
            "success": True,
            "params": {
                "strategy": req.strategy,
                "stock_codes": stock_codes,
                "start_date": start_dt.isoformat(),
                "end_date": end_dt.isoformat(),
                "initial_capital": req.initial_capital,
                "atr_multiplier": req.atr_multiplier,
                "stop_loss_pct": req.stop_loss_pct,
                "max_holdings": req.max_holdings,
                "per_trade_pct": req.per_trade_pct,
            },
            "result": result,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[백테스트 오류] {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"백테스트 실행 오류: {str(e)}")


# ============================================================
# 2. 빠른 백테스트 (프리셋) / Quick Backtest Presets
# ============================================================
@router.get("/quick/{preset}")
async def quick_backtest(preset: str):
    """
    프리셋 백테스트 / Preset backtest configurations
    
    - conservative: 보수형 (ATR×2.5, 손절 -4%)
    - standard: 기본형 (ATR×2.0, 손절 -3%)
    - aggressive: 공격형 (ATR×1.5, 손절 -2%)
    """
    presets = {
        "conservative": {
            "strategy": "dip",
            "atr_multiplier": 2.5,
            "stop_loss_pct": 4.0,
            "max_holdings": 3,
            "per_trade_pct": 15.0,
            "label": "보수형",
        },
        "standard": {
            "strategy": "dip",
            "atr_multiplier": 2.0,
            "stop_loss_pct": 3.0,
            "max_holdings": 5,
            "per_trade_pct": 20.0,
            "label": "기본형",
        },
        "aggressive": {
            "strategy": "dip",
            "atr_multiplier": 1.5,
            "stop_loss_pct": 2.0,
            "max_holdings": 7,
            "per_trade_pct": 25.0,
            "label": "공격형",
        },
        "gap_standard": {
            "strategy": "gap",
            "atr_multiplier": 1.5,
            "stop_loss_pct": 2.5,
            "max_holdings": 5,
            "per_trade_pct": 20.0,
            "label": "갭상승 기본형",
        },
        "combined": {
            "strategy": "both",
            "atr_multiplier": 2.0,
            "stop_loss_pct": 3.0,
            "max_holdings": 5,
            "per_trade_pct": 20.0,
            "label": "눌림목+갭상승 혼합",
        },
    }
    
    if preset not in presets:
        raise HTTPException(
            status_code=400,
            detail=f"사용 가능한 프리셋: {', '.join(presets.keys())}"
        )
    
    config = presets[preset]
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=30)
    
    # 감시종목 가져오기
    watchlist = db.table("watchlist").select("stock_code").order("score", desc=True).limit(15).execute()
    stock_codes = [w["stock_code"] for w in (watchlist.data or [])]
    if not stock_codes:
        stock_codes = ["005930", "000660", "035420", "035720", "051910"]
    
    try:
        result = await run_backtest(
            strategy=config["strategy"],
            stock_codes=stock_codes,
            start_date=start_dt.isoformat(),
            end_date=end_dt.isoformat(),
            initial_capital=1_000_000,
            atr_multiplier=config["atr_multiplier"],
            stop_loss_pct=config["stop_loss_pct"],
            max_holdings=config["max_holdings"],
            per_trade_pct=config["per_trade_pct"],
        )
        
        return {
            "success": True,
            "preset": preset,
            "label": config["label"],
            "params": config,
            "result": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 3. 전략 비교 백테스트 / Compare Strategies
# ============================================================
@router.get("/compare")
async def compare_strategies():
    """
    보수형/기본형/공격형 3가지 전략을 한번에 비교
    Compare conservative/standard/aggressive strategies simultaneously
    """
    presets = ["conservative", "standard", "aggressive"]
    results = {}
    
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=30)
    
    # 감시종목
    watchlist = db.table("watchlist").select("stock_code").order("score", desc=True).limit(10).execute()
    stock_codes = [w["stock_code"] for w in (watchlist.data or [])]
    if not stock_codes:
        stock_codes = ["005930", "000660", "035420"]
    
    configs = {
        "conservative": {"atr": 2.5, "sl": 4.0, "mh": 3, "pt": 15.0, "label": "보수형"},
        "standard": {"atr": 2.0, "sl": 3.0, "mh": 5, "pt": 20.0, "label": "기본형"},
        "aggressive": {"atr": 1.5, "sl": 2.0, "mh": 7, "pt": 25.0, "label": "공격형"},
    }
    
    for name, cfg in configs.items():
        try:
            result = await run_backtest(
                strategy="dip",
                stock_codes=stock_codes,
                start_date=start_dt.isoformat(),
                end_date=end_dt.isoformat(),
                initial_capital=1_000_000,
                atr_multiplier=cfg["atr"],
                stop_loss_pct=cfg["sl"],
                max_holdings=cfg["mh"],
                per_trade_pct=cfg["pt"],
            )
            results[name] = {
                "label": cfg["label"],
                "params": cfg,
                "summary": result.get("summary", {}),
            }
        except Exception as e:
            results[name] = {
                "label": cfg["label"],
                "error": str(e),
            }
    
    return {
        "success": True,
        "period": f"{start_dt} ~ {end_dt}",
        "stock_count": len(stock_codes),
        "strategies": results,
    }


# ============================================================
# 4. 백테스트 이력 저장/조회 / Save/Load Backtest History
# ============================================================
@router.get("/history")
async def get_backtest_history(limit: int = Query(default=10, le=50)):
    """최근 백테스트 이력 조회 / Get recent backtest history"""
    try:
        history = db.table("backtest_history").select("*") \
            .order("created_at", desc=True).limit(limit).execute()
        return history.data or []
    except Exception as e:
        # 테이블 없으면 빈 배열 반환
        return []


@router.post("/save")
async def save_backtest_result(data: dict):
    """백테스트 결과 저장 / Save backtest result"""
    try:
        record = {
            "strategy": data.get("strategy", "dip"),
            "params": data.get("params", {}),
            "summary": data.get("summary", {}),
            "total_return_pct": data.get("summary", {}).get("total_return_pct", 0),
            "win_rate": data.get("summary", {}).get("win_rate", 0),
            "total_trades": data.get("summary", {}).get("total_trades", 0),
            "max_drawdown_pct": data.get("summary", {}).get("max_drawdown_pct", 0),
            "created_at": datetime.now().isoformat(),
        }
        result = db.table("backtest_history").insert(record).execute()
        return {"success": True, "id": result.data[0]["id"] if result.data else None}
    except Exception as e:
        # 테이블 없어도 에러 안 냄
        return {"success": False, "error": str(e)}
