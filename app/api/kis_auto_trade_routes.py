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
    # ★ 스마트형 트레일링 스탑 필드
    strategy: str = "fixed"            # "fixed" | "smart"
    trailing_stop_pct: float = 5.0
    profit_activation_pct: float = 15.0
    grace_days: int = 7
    peak_price: float = 0


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
            "strategy": rule.strategy,
            "trailing_stop_pct": rule.trailing_stop_pct,
            "profit_activation_pct": rule.profit_activation_pct,
            "grace_days": rule.grace_days,
            "peak_price": rule.peak_price,
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
                "strategy": rule.strategy,
                "trailing_stop_pct": rule.trailing_stop_pct,
                "profit_activation_pct": rule.profit_activation_pct,
                "grace_days": rule.grace_days,
                "peak_price": rule.peak_price,
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


@router.post("/migrate")
async def migrate_schema():
    """스마트형 트레일링 스탑 컬럼 마이그레이션 (1회성)"""
    try:
        from app.services.kis_auto_trade import _migrate_columns
        _migrate_columns()
        return {"success": True, "message": "마이그레이션 완료 (strategy, trailing_stop_pct, profit_activation_pct, grace_days, peak_price)"}
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


@router.get("/credentials-check")
async def check_credentials(mode: str = "virtual"):
    """인증정보 진단: Supabase에 저장된 KIS 인증정보 상태 확인"""
    try:
        r = db.table("kis_credentials").select("*").eq("mode", mode).execute()
        if not r.data or len(r.data) == 0:
            return {
                "success": False,
                "status": "NOT_FOUND",
                "message": f"kis_credentials 테이블에 mode={mode} 레코드가 없습니다. "
                           f"프론트엔드 KIS 페이지에서 API 설정을 저장하세요.",
            }

        cred = r.data[0]
        issues = []
        if not cred.get("app_key"):
            issues.append("app_key 비어있음")
        if not cred.get("app_secret"):
            issues.append("app_secret 비어있음")
        if not cred.get("access_token"):
            issues.append("access_token 비어있음")
        account_no = cred.get("account_no", "")
        if not account_no:
            issues.append("account_no 비어있음 ← INVALID_CHECK_ACNO 원인")
        elif len(account_no) < 10:
            issues.append(f"account_no 길이 부족 ({len(account_no)}자, 10자 이상 필요)")

        return {
            "success": len(issues) == 0,
            "status": "OK" if len(issues) == 0 else "ISSUES_FOUND",
            "mode": mode,
            "app_key": f"{cred.get('app_key', '')[:8]}..." if cred.get("app_key") else "(비어있음)",
            "app_secret": f"{cred.get('app_secret', '')[:4]}..." if cred.get("app_secret") else "(비어있음)",
            "access_token": f"{cred.get('access_token', '')[:20]}..." if cred.get("access_token") else "(비어있음)",
            "account_no": account_no or "(비어있음)",
            "account_no_length": len(account_no),
            "is_virtual": cred.get("is_virtual"),
            "updated_at": cred.get("updated_at"),
            "issues": issues,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


class CredentialUpdate(BaseModel):
    mode: str = "virtual"
    account_no: Optional[str] = None
    app_key: Optional[str] = None
    app_secret: Optional[str] = None
    access_token: Optional[str] = None


@router.post("/credentials-update")
async def update_credentials(req: CredentialUpdate):
    """인증정보 수동 업데이트 (계좌번호 등 누락된 필드 보정)"""
    try:
        now = datetime.now(KST).isoformat()
        update_data = {"updated_at": now}

        if req.account_no is not None:
            # 하이픈 제거하고 저장
            update_data["account_no"] = req.account_no.replace("-", "")
        if req.app_key is not None:
            update_data["app_key"] = req.app_key
        if req.app_secret is not None:
            update_data["app_secret"] = req.app_secret
        if req.access_token is not None:
            update_data["access_token"] = req.access_token

        # 기존 레코드 확인
        existing = db.table("kis_credentials").select("id").eq("mode", req.mode).execute()
        if existing.data and len(existing.data) > 0:
            db.table("kis_credentials").update(update_data).eq("mode", req.mode).execute()
        else:
            update_data["mode"] = req.mode
            update_data["created_at"] = now
            db.table("kis_credentials").insert(update_data).execute()

        return {"success": True, "message": f"{req.mode} 인증정보 업데이트 완료", "updated_fields": list(update_data.keys())}
    except Exception as e:
        return {"success": False, "error": str(e)}
