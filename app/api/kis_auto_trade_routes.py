"""KIS 자동매매 규칙 CRUD API
프론트엔드에서 규칙을 저장/조회/삭제하는 REST API.
백엔드 스케줄러가 이 규칙을 읽어 자동 손절/익절 실행.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from app.core.database import db
from app.core.config import KST

router = APIRouter(prefix="/api/kis/auto-trade", tags=["KIS 자동매매"])


class RuleCreate(BaseModel):
    mode: str = "virtual"
    stock_code: str
    stock_name: str = ""
    buy_price: float = 0
    quantity: int = 0
    tp_pct: float = 10
    sl_pct: float = 5
    max_hold_days: int = 30
    buy_date: str = ""
    enabled: bool = True


class RuleSyncRequest(BaseModel):
    mode: str = "virtual"
    rules: List[RuleCreate]


@router.get("/rules")
async def get_rules(mode: str = "virtual"):
    """규칙 목록 조회"""
    try:
        r = db.table("kis_auto_trade_rules") \
            .select("*") \
            .eq("mode", mode) \
            .order("created_at", desc=True) \
            .execute()
        return {"success": True, "rules": r.data or []}
    except Exception as e:
        return {"success": False, "error": str(e), "rules": []}


@router.post("/rules")
async def upsert_rule(rule: RuleCreate):
    """규칙 추가 (동일 종목 있으면 업데이트)"""
    try:
        now = datetime.now(KST).isoformat()
        # 동일 mode + stock_code 존재 확인
        existing = db.table("kis_auto_trade_rules") \
            .select("id") \
            .eq("mode", rule.mode) \
            .eq("stock_code", rule.stock_code) \
            .execute()

        data = {
            "mode": rule.mode,
            "stock_code": rule.stock_code,
            "stock_name": rule.stock_name,
            "buy_price": rule.buy_price,
            "quantity": rule.quantity,
            "tp_pct": rule.tp_pct,
            "sl_pct": rule.sl_pct,
            "max_hold_days": rule.max_hold_days,
            "buy_date": rule.buy_date,
            "enabled": rule.enabled,
            "updated_at": now,
        }

        if existing.data and len(existing.data) > 0:
            r = db.table("kis_auto_trade_rules") \
                .update(data) \
                .eq("id", existing.data[0]["id"]) \
                .execute()
        else:
            data["created_at"] = now
            r = db.table("kis_auto_trade_rules").insert(data).execute()

        return {"success": True, "rule": r.data[0] if r.data else data}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: int):
    """규칙 삭제"""
    try:
        db.table("kis_auto_trade_rules").delete().eq("id", rule_id).execute()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/rules/sync")
async def sync_rules(req: RuleSyncRequest):
    """프론트엔드 규칙 일괄 동기화.
    해당 mode의 기존 활성 규칙을 모두 비활성화하고 새 규칙으로 교체.
    """
    try:
        now = datetime.now(KST).isoformat()
        mode = req.mode

        # 기존 활성 규칙 비활성화
        db.table("kis_auto_trade_rules") \
            .update({"enabled": False, "updated_at": now}) \
            .eq("mode", mode) \
            .eq("enabled", True) \
            .execute()

        # 새 규칙 삽입
        inserted = []
        for rule in req.rules:
            # 동일 종목 기존 규칙 확인
            existing = db.table("kis_auto_trade_rules") \
                .select("id") \
                .eq("mode", mode) \
                .eq("stock_code", rule.stock_code) \
                .execute()

            data = {
                "mode": mode,
                "stock_code": rule.stock_code,
                "stock_name": rule.stock_name,
                "buy_price": rule.buy_price,
                "quantity": rule.quantity,
                "tp_pct": rule.tp_pct,
                "sl_pct": rule.sl_pct,
                "max_hold_days": rule.max_hold_days,
                "buy_date": rule.buy_date,
                "enabled": True,
                "updated_at": now,
            }

            if existing.data and len(existing.data) > 0:
                r = db.table("kis_auto_trade_rules") \
                    .update(data) \
                    .eq("id", existing.data[0]["id"]) \
                    .execute()
            else:
                data["created_at"] = now
                r = db.table("kis_auto_trade_rules").insert(data).execute()

            if r.data:
                inserted.append(r.data[0])

        return {"success": True, "synced": len(inserted), "rules": inserted}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/status")
async def auto_trade_status():
    """자동매매 상태 확인"""
    try:
        # 활성 규칙 수
        virtual_rules = db.table("kis_auto_trade_rules") \
            .select("id", count="exact") \
            .eq("mode", "virtual") \
            .eq("enabled", True) \
            .execute()
        real_rules = db.table("kis_auto_trade_rules") \
            .select("id", count="exact") \
            .eq("mode", "real") \
            .eq("enabled", True) \
            .execute()

        # 최근 실행 로그
        logs = db.table("scheduler_logs") \
            .select("*") \
            .like("job_name", "kis_auto_trade%") \
            .order("executed_at", desc=True) \
            .limit(10) \
            .execute()

        return {
            "success": True,
            "virtual_active_rules": virtual_rules.count or 0,
            "real_active_rules": real_rules.count or 0,
            "recent_logs": logs.data or [],
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
