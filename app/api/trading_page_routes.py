"""
한국투자증권 API 매매 실행 페이지 & API
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/api/trading_page_routes.py

페이지:
  GET /trading                    — 매매 실행 페이지 (HTML)

API:
  GET  /api/trading/status        — 자동매매 상태 조회
  POST /api/trading/start         — 자동매매 시작 (모의/실전)
  POST /api/trading/stop          — 자동매매 중지
  GET  /api/trading/account       — KIS 계좌 잔고 조회
  GET  /api/trading/holdings      — DB 보유종목 조회
  GET  /api/trading/history       — 매매 내역 조회
  POST /api/trading/manual-buy    — 수동 매수
  POST /api/trading/manual-sell   — 수동 매도
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date
import asyncio
import logging

from app.core.config import config, KST
from app.core.database import db
from app.utils.kr_holiday import is_market_open_now, get_market_status

logger = logging.getLogger(__name__)
router = APIRouter(tags=["trading-page"])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 자동매매 상태 관리 (메모리)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
trading_state = {
    "running": False,
    "mode": None,        # "mock" or "live"
    "started_at": None,
    "cycle_count": 0,
    "last_cycle": None,
    "error": None,
    "task": None,
}


async def _auto_trading_loop(mode: str):
    """자동매매 루프 (60초 간격)"""
    is_live = (mode == "live")
    trading_state["cycle_count"] = 0
    trading_state["error"] = None

    while trading_state["running"]:
        try:
            now = datetime.now(KST)
            if not is_market_open_now(now):
                trading_state["last_cycle"] = now.isoformat()
                await asyncio.sleep(60)
                continue

            from app.engine.trade_executor import execute_trading_cycle
            await execute_trading_cycle()
            trading_state["cycle_count"] += 1
            trading_state["last_cycle"] = datetime.now(KST).isoformat()
            trading_state["error"] = None

        except Exception as e:
            logger.error(f"[자동매매] 오류: {e}")
            trading_state["error"] = str(e)

        await asyncio.sleep(60)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Request 모델
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class StartRequest(BaseModel):
    mode: str = "mock"       # "mock" or "live"
    password: str = ""


class ManualOrderRequest(BaseModel):
    code: str
    quantity: int
    price: int = 0           # 0이면 시장가
    is_live: bool = False
    password: str = ""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API 엔드포인트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/api/trading/status")
async def get_trading_status():
    """자동매매 상태 조회"""
    now = datetime.now(KST)
    return {
        "running": trading_state["running"],
        "mode": trading_state["mode"],
        "mode_label": "모의투자" if trading_state["mode"] == "mock" else "실제투자" if trading_state["mode"] == "live" else None,
        "started_at": trading_state["started_at"],
        "cycle_count": trading_state["cycle_count"],
        "last_cycle": trading_state["last_cycle"],
        "error": trading_state["error"],
        "market_status": get_market_status(now),
        "is_market_open": is_market_open_now(now),
        "server_time": now.strftime("%Y-%m-%d %H:%M:%S"),
    }


@router.post("/api/trading/start")
async def start_trading(req: StartRequest):
    """자동매매 시작"""
    if req.password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")

    if trading_state["running"]:
        return {"success": False, "message": f"이미 {trading_state['mode']} 모드로 실행 중입니다"}

    if req.mode not in ("mock", "live"):
        raise HTTPException(400, "mode는 'mock' 또는 'live'만 가능합니다")

    trading_state["running"] = True
    trading_state["mode"] = req.mode
    trading_state["started_at"] = datetime.now(KST).isoformat()
    trading_state["cycle_count"] = 0
    trading_state["error"] = None

    # 비동기 루프 시작
    loop = asyncio.get_event_loop()
    task = loop.create_task(_auto_trading_loop(req.mode))
    trading_state["task"] = task

    mode_label = "모의투자" if req.mode == "mock" else "실제투자"
    return {"success": True, "message": f"{mode_label} 자동매매 시작", "mode": req.mode}


@router.post("/api/trading/stop")
async def stop_trading(password: str = ""):
    """자동매매 중지"""
    if password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")

    if not trading_state["running"]:
        return {"success": False, "message": "실행 중인 자동매매가 없습니다"}

    trading_state["running"] = False
    if trading_state.get("task"):
        trading_state["task"].cancel()
        trading_state["task"] = None

    mode_label = "모의투자" if trading_state["mode"] == "mock" else "실제투자"
    trading_state["mode"] = None
    return {"success": True, "message": f"{mode_label} 자동매매 중지 완료"}


@router.get("/api/trading/account")
async def get_account(mode: str = "mock"):
    """KIS 계좌 잔고 조회"""
    is_live = (mode == "live")
    try:
        from app.services.kis_stock import get_account_balance
        result = get_account_balance(is_live=is_live)
        if result:
            return {"success": True, "mode": mode, **result}
        return {"success": False, "error": "계좌 조회 실패 (API 키 확인 필요)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/trading/holdings")
async def get_holdings(strategy_id: int = None):
    """DB 보유종목 조회"""
    try:
        q = db.table("holdings").select("*").order("updated_at", desc=True)
        if strategy_id:
            q = q.eq("strategy_id", strategy_id)
        data = q.execute().data or []
        return {"holdings": data}
    except Exception as e:
        return {"holdings": [], "error": str(e)}


@router.get("/api/trading/history")
async def get_history(limit: int = 30, mode: str = "all"):
    """매매 내역 조회"""
    try:
        q = db.table("trades").select("*").order("traded_at", desc=True).limit(limit)
        data = q.execute().data or []
        return {"trades": data}
    except Exception as e:
        return {"trades": [], "error": str(e)}


@router.post("/api/trading/manual-buy")
async def manual_buy(req: ManualOrderRequest):
    """수동 매수"""
    if req.password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")
    try:
        from app.services.kis_order import buy_stock
        result = buy_stock(req.code, req.quantity, req.price, is_live=req.is_live)
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/api/trading/manual-sell")
async def manual_sell(req: ManualOrderRequest):
    """수동 매도"""
    if req.password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")
    try:
        from app.services.kis_order import sell_stock
        result = sell_stock(req.code, req.quantity, req.price, is_live=req.is_live)
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTML 페이지
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/trading", response_class=HTMLResponse)
async def trading_page():
    """매매 실행 페이지"""
    return TRADING_HTML


TRADING_HTML = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>10억 만들기 - 매매 실행</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, sans-serif; background: #0a0e27; color: #e0e0e0; min-height: 100vh; }
.header { background: linear-gradient(135deg, #1a1f3a 0%, #0d1117 100%); padding: 20px 24px; border-bottom: 1px solid #2d3548; display: flex; justify-content: space-between; align-items: center; }
.header h1 { font-size: 22px; color: #fff; }
.header h1 span { color: #ffd700; }
.server-time { color: #8b95a5; font-size: 13px; }
.market-badge { display: inline-block; padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; margin-left: 8px; }
.market-open { background: rgba(0,200,83,0.15); color: #00c853; }
.market-closed { background: rgba(255,82,82,0.15); color: #ff5252; }
.container { max-width: 1200px; margin: 0 auto; padding: 20px; }

/* 비밀번호 입력 */
.pw-section { text-align: center; padding: 8px 0; }
.pw-section input { background: #151b30; border: 1px solid #2d3548; color: #fff; padding: 8px 16px; border-radius: 8px; width: 200px; text-align: center; font-size: 14px; }

/* 매매 버튼 영역 */
.trading-buttons { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0; }
.trade-card { background: #151b30; border-radius: 16px; padding: 28px; text-align: center; border: 2px solid transparent; transition: all 0.3s; cursor: pointer; position: relative; overflow: hidden; }
.trade-card:hover { transform: translateY(-2px); box-shadow: 0 8px 32px rgba(0,0,0,0.3); }
.trade-card.mock { border-color: #2196f3; }
.trade-card.mock:hover { background: #151b30; border-color: #42a5f5; }
.trade-card.live { border-color: #ff5252; }
.trade-card.live:hover { background: #151b30; border-color: #ff6b6b; }
.trade-card .icon { font-size: 48px; margin-bottom: 12px; }
.trade-card .title { font-size: 20px; font-weight: 700; margin-bottom: 8px; }
.trade-card .desc { color: #8b95a5; font-size: 13px; margin-bottom: 16px; line-height: 1.5; }
.trade-card .url-badge { font-size: 11px; color: #6b7280; background: #0d1117; padding: 4px 10px; border-radius: 6px; display: inline-block; }

.btn { padding: 12px 28px; border: none; border-radius: 10px; font-size: 16px; font-weight: 700; cursor: pointer; transition: all 0.2s; display: inline-flex; align-items: center; gap: 8px; }
.btn-mock { background: linear-gradient(135deg, #1565c0, #1e88e5); color: #fff; }
.btn-mock:hover { background: linear-gradient(135deg, #1e88e5, #42a5f5); transform: scale(1.02); }
.btn-live { background: linear-gradient(135deg, #c62828, #e53935); color: #fff; }
.btn-live:hover { background: linear-gradient(135deg, #e53935, #ef5350); transform: scale(1.02); }
.btn-stop { background: linear-gradient(135deg, #424242, #616161); color: #fff; }
.btn-stop:hover { background: linear-gradient(135deg, #616161, #757575); }
.btn-sm { padding: 8px 16px; font-size: 13px; border-radius: 8px; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none !important; }

/* 상태 표시 */
.status-bar { background: #151b30; border-radius: 12px; padding: 16px 20px; margin: 16px 0; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; }
.status-item { text-align: center; }
.status-item .label { font-size: 11px; color: #6b7280; text-transform: uppercase; margin-bottom: 4px; }
.status-item .value { font-size: 16px; font-weight: 700; }
.status-running { color: #00c853; }
.status-stopped { color: #ff5252; }

/* 탭 */
.tabs { display: flex; gap: 4px; margin: 24px 0 16px; background: #151b30; border-radius: 10px; padding: 4px; }
.tab { flex: 1; padding: 10px; text-align: center; border-radius: 8px; cursor: pointer; font-size: 14px; font-weight: 600; color: #6b7280; transition: all 0.2s; }
.tab.active { background: #1e88e5; color: #fff; }
.tab:hover:not(.active) { color: #e0e0e0; }

/* 테이블 */
.table-wrap { background: #151b30; border-radius: 12px; overflow: hidden; }
table { width: 100%; border-collapse: collapse; }
th { background: #1a2040; padding: 12px 16px; text-align: left; font-size: 12px; color: #8b95a5; font-weight: 600; text-transform: uppercase; }
td { padding: 12px 16px; font-size: 13px; border-top: 1px solid #1e2642; }
tr:hover { background: rgba(30,136,229,0.05); }
.profit-plus { color: #ff1744; }
.profit-minus { color: #2979ff; }
.empty-msg { text-align: center; padding: 40px; color: #4a5568; }

/* 계좌 요약 */
.account-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin: 16px 0; }
.account-card { background: #151b30; border-radius: 12px; padding: 16px; }
.account-card .label { font-size: 12px; color: #6b7280; margin-bottom: 4px; }
.account-card .value { font-size: 20px; font-weight: 700; }

/* 수동 주문 */
.manual-order { background: #151b30; border-radius: 12px; padding: 20px; margin: 16px 0; }
.manual-order h3 { font-size: 15px; margin-bottom: 16px; color: #e0e0e0; }
.order-form { display: flex; gap: 10px; flex-wrap: wrap; align-items: flex-end; }
.form-group { display: flex; flex-direction: column; gap: 4px; }
.form-group label { font-size: 11px; color: #6b7280; }
.form-group input, .form-group select { background: #0d1117; border: 1px solid #2d3548; color: #fff; padding: 8px 12px; border-radius: 8px; font-size: 13px; width: 140px; }

/* 로그 */
.log-area { background: #0d1117; border-radius: 8px; padding: 12px 16px; max-height: 200px; overflow-y: auto; font-family: monospace; font-size: 12px; line-height: 1.8; margin-top: 16px; }
.log-area .log-time { color: #4a5568; }
.log-area .log-info { color: #42a5f5; }
.log-area .log-success { color: #00c853; }
.log-area .log-error { color: #ff5252; }
.log-area .log-warn { color: #ffc107; }

/* 반응형 */
@media (max-width: 768px) {
  .trading-buttons { grid-template-columns: 1fr; }
  .account-grid { grid-template-columns: 1fr 1fr; }
  .order-form { flex-direction: column; }
  .form-group input { width: 100%; }
}

/* 확인 모달 */
.modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.7); z-index: 1000; align-items: center; justify-content: center; }
.modal-overlay.show { display: flex; }
.modal { background: #1a2040; border-radius: 16px; padding: 32px; max-width: 440px; width: 90%; text-align: center; }
.modal h2 { font-size: 20px; margin-bottom: 12px; }
.modal p { color: #8b95a5; font-size: 14px; margin-bottom: 24px; line-height: 1.6; }
.modal .warn { color: #ff5252; font-weight: 700; font-size: 13px; }
.modal-buttons { display: flex; gap: 12px; justify-content: center; }
.pulse { animation: pulse 2s infinite; }
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.6; } }
</style>
</head>
<body>

<div class="header">
  <h1>10억 만들기 <span>매매 실행</span></h1>
  <div>
    <span class="server-time" id="serverTime">--</span>
    <span class="market-badge market-closed" id="marketBadge">--</span>
  </div>
</div>

<div class="container">

  <!-- 비밀번호 -->
  <div class="pw-section">
    <input type="password" id="password" placeholder="비밀번호 입력" autocomplete="off">
  </div>

  <!-- 매매 시작 버튼 -->
  <div class="trading-buttons">
    <div class="trade-card mock" onclick="startTrading('mock')">
      <div class="icon">🧪</div>
      <div class="title">모의투자 실시</div>
      <div class="desc">한국투자증권 모의투자 API를 사용합니다.<br>실제 자금이 투입되지 않습니다.</div>
      <button class="btn btn-mock" id="btnMock">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5,3 19,12 5,21"/></svg>
        모의투자 시작
      </button>
      <div class="url-badge" style="margin-top:12px">openapivts.koreainvestment.com</div>
    </div>
    <div class="trade-card live" onclick="startTrading('live')">
      <div class="icon">💰</div>
      <div class="title">실제투자 실시</div>
      <div class="desc">한국투자증권 실전투자 API를 사용합니다.<br><strong style="color:#ff5252">실제 자금이 투입됩니다!</strong></div>
      <button class="btn btn-live" id="btnLive">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5,3 19,12 5,21"/></svg>
        실제투자 시작
      </button>
      <div class="url-badge" style="margin-top:12px">openapi.koreainvestment.com</div>
    </div>
  </div>

  <!-- 자동매매 상태바 -->
  <div class="status-bar" id="statusBar">
    <div class="status-item">
      <div class="label">상태</div>
      <div class="value status-stopped" id="statusText">중지됨</div>
    </div>
    <div class="status-item">
      <div class="label">모드</div>
      <div class="value" id="statusMode">-</div>
    </div>
    <div class="status-item">
      <div class="label">실행 횟수</div>
      <div class="value" id="statusCycles">0</div>
    </div>
    <div class="status-item">
      <div class="label">마지막 실행</div>
      <div class="value" id="statusLast">-</div>
    </div>
    <div>
      <button class="btn btn-stop btn-sm" id="btnStop" onclick="stopTrading()" disabled>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg>
        중지
      </button>
    </div>
  </div>

  <!-- 탭 메뉴 -->
  <div class="tabs">
    <div class="tab active" data-tab="account" onclick="switchTab('account')">계좌 잔고</div>
    <div class="tab" data-tab="holdings" onclick="switchTab('holdings')">보유종목</div>
    <div class="tab" data-tab="history" onclick="switchTab('history')">매매내역</div>
    <div class="tab" data-tab="manual" onclick="switchTab('manual')">수동주문</div>
  </div>

  <!-- 계좌 잔고 탭 -->
  <div id="tab-account" class="tab-content">
    <div style="display:flex;gap:8px;margin-bottom:12px;">
      <button class="btn btn-mock btn-sm" onclick="loadAccount('mock')">모의계좌 조회</button>
      <button class="btn btn-live btn-sm" onclick="loadAccount('live')">실전계좌 조회</button>
    </div>
    <div class="account-grid" id="accountGrid">
      <div class="account-card"><div class="label">예수금</div><div class="value" id="accDeposit">-</div></div>
      <div class="account-card"><div class="label">총평가금액</div><div class="value" id="accTotal">-</div></div>
      <div class="account-card"><div class="label">총매입금액</div><div class="value" id="accBuy">-</div></div>
      <div class="account-card"><div class="label">평가손익</div><div class="value" id="accProfit">-</div></div>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr><th>종목</th><th>수량</th><th>매입가</th><th>현재가</th><th>수익률</th><th>평가손익</th></tr></thead>
        <tbody id="accHoldings"><tr><td colspan="6" class="empty-msg">계좌를 조회해주세요</td></tr></tbody>
      </table>
    </div>
  </div>

  <!-- 보유종목 탭 (DB) -->
  <div id="tab-holdings" class="tab-content" style="display:none;">
    <div class="table-wrap">
      <table>
        <thead><tr><th>종목</th><th>전략</th><th>수량</th><th>매입가</th><th>현재가</th><th>수익률</th><th>미실현손익</th></tr></thead>
        <tbody id="holdingsBody"><tr><td colspan="7" class="empty-msg">로딩 중...</td></tr></tbody>
      </table>
    </div>
  </div>

  <!-- 매매내역 탭 -->
  <div id="tab-history" class="tab-content" style="display:none;">
    <div class="table-wrap">
      <table>
        <thead><tr><th>시간</th><th>종목</th><th>구분</th><th>가격</th><th>수량</th><th>순수익</th><th>사유</th></tr></thead>
        <tbody id="historyBody"><tr><td colspan="7" class="empty-msg">로딩 중...</td></tr></tbody>
      </table>
    </div>
  </div>

  <!-- 수동주문 탭 -->
  <div id="tab-manual" class="tab-content" style="display:none;">
    <div class="manual-order">
      <h3>수동 주문</h3>
      <div class="order-form">
        <div class="form-group">
          <label>종목코드</label>
          <input type="text" id="orderCode" placeholder="005930">
        </div>
        <div class="form-group">
          <label>수량</label>
          <input type="number" id="orderQty" placeholder="1" min="1">
        </div>
        <div class="form-group">
          <label>가격 (0=시장가)</label>
          <input type="number" id="orderPrice" placeholder="0" min="0">
        </div>
        <div class="form-group">
          <label>모드</label>
          <select id="orderMode">
            <option value="false">모의투자</option>
            <option value="true">실제투자</option>
          </select>
        </div>
        <button class="btn btn-mock btn-sm" onclick="manualOrder('buy')">매수</button>
        <button class="btn btn-live btn-sm" onclick="manualOrder('sell')">매도</button>
      </div>
    </div>
  </div>

  <!-- 로그 -->
  <div class="log-area" id="logArea">
    <div><span class="log-time">[시스템]</span> <span class="log-info">매매 실행 페이지 로드 완료</span></div>
  </div>
</div>

<!-- 확인 모달 -->
<div class="modal-overlay" id="confirmModal">
  <div class="modal">
    <h2 id="modalTitle">확인</h2>
    <p id="modalDesc"></p>
    <div class="modal-buttons">
      <button class="btn btn-stop btn-sm" onclick="closeModal()">취소</button>
      <button class="btn btn-sm" id="modalConfirmBtn" onclick="confirmAction()">확인</button>
    </div>
  </div>
</div>

<script>
const API = '';
let pendingAction = null;
let statusInterval = null;

// ── 유틸 ──
function pw() { return document.getElementById('password').value; }
function fmt(n) { return n != null ? Number(n).toLocaleString() + '원' : '-'; }
function log(msg, type='info') {
  const area = document.getElementById('logArea');
  const time = new Date().toLocaleTimeString('ko-KR');
  area.innerHTML += '<div><span class="log-time">[' + time + ']</span> <span class="log-' + type + '">' + msg + '</span></div>';
  area.scrollTop = area.scrollHeight;
}

// ── 모달 ──
function showModal(title, desc, btnClass, action) {
  document.getElementById('modalTitle').textContent = title;
  document.getElementById('modalDesc').innerHTML = desc;
  const btn = document.getElementById('modalConfirmBtn');
  btn.className = 'btn btn-sm ' + btnClass;
  btn.textContent = '확인, 시작합니다';
  pendingAction = action;
  document.getElementById('confirmModal').classList.add('show');
}
function closeModal() {
  document.getElementById('confirmModal').classList.remove('show');
  pendingAction = null;
}
function confirmAction() {
  closeModal();
  if (pendingAction) pendingAction();
}

// ── 자동매매 시작 ──
function startTrading(mode) {
  if (!pw()) { log('비밀번호를 입력해주세요', 'warn'); return; }

  if (mode === 'live') {
    showModal(
      '실제투자 시작',
      '한국투자증권 <strong>실전투자 API</strong>로 자동매매를 시작합니다.<br><br><span class="warn">실제 자금이 투입되며, 매수/매도 주문이 실행됩니다.<br>발생하는 손실에 대한 책임은 사용자에게 있습니다.</span>',
      'btn-live',
      () => doStart('live')
    );
  } else {
    showModal(
      '모의투자 시작',
      '한국투자증권 <strong>모의투자 API</strong>로 자동매매를 시작합니다.<br><br>실제 자금은 투입되지 않습니다.',
      'btn-mock',
      () => doStart('mock')
    );
  }
}

async function doStart(mode) {
  try {
    const res = await fetch(API + '/api/trading/start', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ mode, password: pw() })
    });
    const data = await res.json();
    if (data.success) {
      log(data.message, 'success');
      refreshStatus();
    } else {
      log(data.message || data.detail || '시작 실패', 'error');
    }
  } catch(e) { log('통신 오류: ' + e.message, 'error'); }
}

async function stopTrading() {
  if (!pw()) { log('비밀번호를 입력해주세요', 'warn'); return; }
  try {
    const res = await fetch(API + '/api/trading/stop?password=' + encodeURIComponent(pw()), { method: 'POST' });
    const data = await res.json();
    log(data.message, data.success ? 'success' : 'error');
    refreshStatus();
  } catch(e) { log('통신 오류: ' + e.message, 'error'); }
}

// ── 상태 갱신 ──
async function refreshStatus() {
  try {
    const res = await fetch(API + '/api/trading/status');
    const d = await res.json();

    document.getElementById('serverTime').textContent = d.server_time || '--';

    const badge = document.getElementById('marketBadge');
    if (d.is_market_open) { badge.textContent = '장 운영중'; badge.className = 'market-badge market-open'; }
    else { badge.textContent = d.market_status || '장 마감'; badge.className = 'market-badge market-closed'; }

    const st = document.getElementById('statusText');
    if (d.running) { st.textContent = '실행중'; st.className = 'value status-running pulse'; }
    else { st.textContent = '중지됨'; st.className = 'value status-stopped'; }

    document.getElementById('statusMode').textContent = d.mode_label || '-';
    document.getElementById('statusCycles').textContent = d.cycle_count || 0;
    document.getElementById('statusLast').textContent = d.last_cycle ? new Date(d.last_cycle).toLocaleTimeString('ko-KR') : '-';

    document.getElementById('btnStop').disabled = !d.running;
    document.getElementById('btnMock').disabled = d.running;
    document.getElementById('btnLive').disabled = d.running;

    if (d.error) log('오류: ' + d.error, 'error');
  } catch(e) { /* silent */ }
}

// ── 계좌 조회 ──
async function loadAccount(mode) {
  log(mode === 'live' ? '실전계좌 조회 중...' : '모의계좌 조회 중...', 'info');
  try {
    const res = await fetch(API + '/api/trading/account?mode=' + mode);
    const d = await res.json();
    if (!d.success) { log('계좌 조회 실패: ' + (d.error || ''), 'error'); return; }

    document.getElementById('accDeposit').textContent = fmt(d.total_deposit);
    document.getElementById('accTotal').textContent = fmt(d.total_eval);
    document.getElementById('accBuy').textContent = fmt(d.total_buy_amount);
    const profitEl = document.getElementById('accProfit');
    profitEl.textContent = fmt(d.total_profit);
    profitEl.className = 'value ' + (d.total_profit >= 0 ? 'profit-plus' : 'profit-minus');

    const tbody = document.getElementById('accHoldings');
    if (!d.holdings || d.holdings.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" class="empty-msg">보유종목 없음</td></tr>';
    } else {
      tbody.innerHTML = d.holdings.map(h => `<tr>
        <td><strong>${h.name}</strong><br><span style="color:#6b7280;font-size:11px">${h.code}</span></td>
        <td>${h.quantity}주</td>
        <td>${fmt(Math.round(h.buy_price))}</td>
        <td>${fmt(h.current_price)}</td>
        <td class="${h.profit_pct >= 0 ? 'profit-plus' : 'profit-minus'}">${h.profit_pct.toFixed(2)}%</td>
        <td class="${h.profit_won >= 0 ? 'profit-plus' : 'profit-minus'}">${fmt(h.profit_won)}</td>
      </tr>`).join('');
    }
    log(`${mode === 'live' ? '실전' : '모의'}계좌 조회 완료 (보유 ${d.holdings?.length || 0}종목)`, 'success');
  } catch(e) { log('계좌 조회 오류: ' + e.message, 'error'); }
}

// ── DB 보유종목 ──
async function loadHoldings() {
  try {
    const res = await fetch(API + '/api/trading/holdings');
    const d = await res.json();
    const tbody = document.getElementById('holdingsBody');
    if (!d.holdings || d.holdings.length === 0) {
      tbody.innerHTML = '<tr><td colspan="7" class="empty-msg">보유종목 없음</td></tr>';
    } else {
      tbody.innerHTML = d.holdings.map(h => `<tr>
        <td><strong>${h.stock_name || ''}</strong><br><span style="color:#6b7280;font-size:11px">${h.stock_code || ''}</span></td>
        <td>${h.strategy_id || '-'}</td>
        <td>${h.quantity || 0}주</td>
        <td>${fmt(h.buy_price)}</td>
        <td>${fmt(h.current_price)}</td>
        <td class="${(h.unrealized_pct||0) >= 0 ? 'profit-plus' : 'profit-minus'}">${(h.unrealized_pct||0).toFixed(2)}%</td>
        <td class="${(h.unrealized_profit||0) >= 0 ? 'profit-plus' : 'profit-minus'}">${fmt(h.unrealized_profit)}</td>
      </tr>`).join('');
    }
  } catch(e) { /* silent */ }
}

// ── 매매내역 ──
async function loadHistory() {
  try {
    const res = await fetch(API + '/api/trading/history?limit=30');
    const d = await res.json();
    const tbody = document.getElementById('historyBody');
    if (!d.trades || d.trades.length === 0) {
      tbody.innerHTML = '<tr><td colspan="7" class="empty-msg">매매내역 없음</td></tr>';
    } else {
      tbody.innerHTML = d.trades.map(t => {
        const isSell = t.trade_type === 'sell';
        const price = isSell ? t.sell_price : t.buy_price;
        return `<tr>
          <td style="font-size:11px">${t.traded_at ? new Date(t.traded_at).toLocaleString('ko-KR') : '-'}</td>
          <td>${t.stock_name || ''}</td>
          <td style="color:${isSell ? '#2979ff' : '#ff1744'}">${isSell ? '매도' : '매수'}</td>
          <td>${fmt(price)}</td>
          <td>${t.quantity || 0}주</td>
          <td class="${(t.net_profit||0) >= 0 ? 'profit-plus' : 'profit-minus'}">${isSell ? fmt(t.net_profit) : '-'}</td>
          <td style="font-size:11px;color:#6b7280">${t.trade_reason || ''}</td>
        </tr>`;
      }).join('');
    }
  } catch(e) { /* silent */ }
}

// ── 수동 주문 ──
async function manualOrder(type) {
  if (!pw()) { log('비밀번호를 입력해주세요', 'warn'); return; }
  const code = document.getElementById('orderCode').value;
  const qty = parseInt(document.getElementById('orderQty').value);
  const price = parseInt(document.getElementById('orderPrice').value) || 0;
  const isLive = document.getElementById('orderMode').value === 'true';
  if (!code || !qty) { log('종목코드와 수량을 입력해주세요', 'warn'); return; }

  const endpoint = type === 'buy' ? '/api/trading/manual-buy' : '/api/trading/manual-sell';
  try {
    const res = await fetch(API + endpoint, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ code, quantity: qty, price, is_live: isLive, password: pw() })
    });
    const d = await res.json();
    if (d.success) log(`${type === 'buy' ? '매수' : '매도'} 주문 성공: ${code} ${qty}주`, 'success');
    else log(`주문 실패: ${d.error || JSON.stringify(d)}`, 'error');
  } catch(e) { log('주문 오류: ' + e.message, 'error'); }
}

// ── 탭 전환 ──
function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === name));
  document.querySelectorAll('.tab-content').forEach(c => c.style.display = 'none');
  document.getElementById('tab-' + name).style.display = 'block';

  if (name === 'holdings') loadHoldings();
  if (name === 'history') loadHistory();
}

// ── 초기화 ──
refreshStatus();
loadHoldings();
statusInterval = setInterval(refreshStatus, 10000);
</script>
</body>
</html>
"""
