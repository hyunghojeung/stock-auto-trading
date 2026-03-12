"""KIS 서버사이드 자동 손절/익절 모듈
브라우저 접속 없이 Railway 서버에서 자동 실행.
Supabase kis_credentials에서 인증정보를 읽고,
kis_auto_trade_rules 테이블의 규칙에 따라 매도 실행.
"""
import requests
import logging
from datetime import datetime, date
from app.core.database import db
from app.core.config import KST

logger = logging.getLogger(__name__)

# ★ 토큰 갱신 시각 캐시 (1분당 1회 제한 방지)
_last_token_refresh = {}  # { "virtual": datetime, "real": datetime }
# ★ 모니터링 로그 시각 캐시 (5분 간격으로만 DB 기록)
_last_monitor_log = {}  # { "virtual": datetime, "real": datetime }
# ★ 매도 실패 카운터 (연속 3회 실패 시 해당 종목 스킵)
_sell_fail_count = {}  # { "virtual_389030": 3 }

# KIS Base URLs
VIRTUAL_BASE = "https://openapivts.koreainvestment.com:29443"
REAL_BASE = "https://openapi.koreainvestment.com:9443"


_columns_migrated = False  # 한 번만 마이그레이션 시도


def _ensure_table():
    """kis_auto_trade_rules 테이블 존재 확인 (없으면 생성 시도) + 컬럼 마이그레이션"""
    global _columns_migrated
    try:
        db.table("kis_auto_trade_rules").select("id").limit(1).execute()
        # ★ 기존 테이블에 새 컬럼 추가 (strategy, trailing 관련)
        if not _columns_migrated:
            _migrate_columns()
            _columns_migrated = True
        return True
    except Exception:
        try:
            db.postgrest.rpc("exec_sql", {
                "query": """
                CREATE TABLE IF NOT EXISTS kis_auto_trade_rules (
                    id SERIAL PRIMARY KEY,
                    mode TEXT NOT NULL DEFAULT 'virtual',
                    stock_code TEXT NOT NULL,
                    stock_name TEXT DEFAULT '',
                    buy_price NUMERIC DEFAULT 0,
                    quantity INTEGER DEFAULT 0,
                    tp_pct NUMERIC DEFAULT 10,
                    sl_pct NUMERIC DEFAULT 5,
                    max_hold_days INTEGER DEFAULT 30,
                    buy_date TEXT DEFAULT '',
                    enabled BOOLEAN DEFAULT TRUE,
                    strategy TEXT DEFAULT 'fixed',
                    trailing_stop_pct NUMERIC DEFAULT 5,
                    profit_activation_pct NUMERIC DEFAULT 15,
                    grace_days INTEGER DEFAULT 7,
                    peak_price NUMERIC DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                ALTER TABLE kis_auto_trade_rules ENABLE ROW LEVEL SECURITY;
                CREATE POLICY IF NOT EXISTS "allow_all" ON kis_auto_trade_rules FOR ALL USING (true) WITH CHECK (true);
                """
            }).execute()
            _columns_migrated = True
            return True
        except Exception as e:
            logger.warning(f"[자동매매] 테이블 생성 실패: {e}")
            return False


def _migrate_columns():
    """기존 테이블에 스마트형 트레일링 스탑 컬럼 추가 (IF NOT EXISTS)"""
    try:
        db.postgrest.rpc("exec_sql", {
            "query": """
            ALTER TABLE kis_auto_trade_rules ADD COLUMN IF NOT EXISTS strategy TEXT DEFAULT 'fixed';
            ALTER TABLE kis_auto_trade_rules ADD COLUMN IF NOT EXISTS trailing_stop_pct NUMERIC DEFAULT 5;
            ALTER TABLE kis_auto_trade_rules ADD COLUMN IF NOT EXISTS profit_activation_pct NUMERIC DEFAULT 15;
            ALTER TABLE kis_auto_trade_rules ADD COLUMN IF NOT EXISTS grace_days INTEGER DEFAULT 7;
            ALTER TABLE kis_auto_trade_rules ADD COLUMN IF NOT EXISTS peak_price NUMERIC DEFAULT 0;
            """
        }).execute()
        logger.info("[자동매매] 스마트형 트레일링 스탑 컬럼 마이그레이션 완료")
    except Exception as e:
        logger.warning(f"[자동매매] 컬럼 마이그레이션 실패 (이미 존재할 수 있음): {e}")


def _get_credentials(mode: str):
    """Supabase kis_credentials 테이블에서 KIS 인증정보 읽기"""
    try:
        r = db.table("kis_credentials").select("*").eq("mode", mode).execute()
        if r.data and len(r.data) > 0:
            return r.data[0]
    except Exception as e:
        logger.error(f"[자동매매] {mode} 인증정보 조회 실패: {e}")
    return None


def _refresh_token(cred):
    """토큰 갱신 후 Supabase에 업데이트"""
    is_virtual = cred.get("is_virtual", True)
    base_url = VIRTUAL_BASE if is_virtual else REAL_BASE
    app_key = cred.get("app_key", "")
    app_secret = cred.get("app_secret", "")

    if not app_key or not app_secret:
        return None

    try:
        r = requests.post(
            f"{base_url}/oauth2/tokenP",
            json={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "appsecret": app_secret,
            },
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        new_token = data.get("access_token")
        if new_token:
            db.table("kis_credentials").update({
                "access_token": new_token,
                "updated_at": datetime.now(KST).isoformat(),
            }).eq("mode", cred["mode"]).execute()
            logger.info(f"[자동매매] {cred['mode']} 토큰 갱신 성공")
            return new_token
    except Exception as e:
        logger.error(f"[자동매매] {cred['mode']} 토큰 갱신 실패: {e}")
    return None


def _make_headers(cred, token=None):
    """KIS API 헤더 생성"""
    return {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token or cred.get('access_token', '')}",
        "appkey": cred.get("app_key", ""),
        "appsecret": cred.get("app_secret", ""),
    }


def _parse_account_no(cred):
    """계좌번호를 CANO(8자리) + ACNT_PRDT_CD(2자리)로 분리.
    공백, 하이픈 등을 정제한 뒤 파싱."""
    raw = cred.get("account_no", "")
    # 공백, 하이픈 제거
    cleaned = raw.strip().replace("-", "").replace(" ", "")
    cano = cleaned[:8] if len(cleaned) >= 8 else cleaned
    acnt_prdt_cd = cleaned[8:10] if len(cleaned) > 8 else "01"
    return cano, acnt_prdt_cd


def _get_balance(cred):
    """KIS 잔고 조회 (inquire-balance)"""
    is_virtual = cred.get("is_virtual", True)
    base_url = VIRTUAL_BASE if is_virtual else REAL_BASE
    tr_id = "VTTC8434R" if is_virtual else "TTTC8434R"
    cano, acnt_prdt_cd = _parse_account_no(cred)

    headers = _make_headers(cred)
    headers["tr_id"] = tr_id
    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }

    try:
        r = requests.get(
            f"{base_url}/uapi/domestic-stock/v1/trading/inquire-balance",
            headers=headers, params=params, timeout=10,
        )
        d = r.json()
        if d.get("rt_cd") == "0":
            holdings = []
            for item in d.get("output1", []):
                qty = int(item.get("hldg_qty", 0))
                if qty > 0:
                    holdings.append({
                        "code": item.get("pdno", ""),
                        "name": item.get("prdt_name", ""),
                        "quantity": qty,
                        "buy_price": float(item.get("pchs_avg_pric", 0)),
                        "current_price": int(item.get("prpr", 0)),
                        "profit_pct": float(item.get("evlu_pfls_rt", 0)),
                    })
            return holdings
        elif d.get("msg_cd") == "EGW00123":
            # 토큰 만료 → 갱신 시도
            new_token = _refresh_token(cred)
            if new_token:
                headers["authorization"] = f"Bearer {new_token}"
                r2 = requests.get(
                    f"{base_url}/uapi/domestic-stock/v1/trading/inquire-balance",
                    headers=headers, params=params, timeout=10,
                )
                d2 = r2.json()
                if d2.get("rt_cd") == "0":
                    holdings = []
                    for item in d2.get("output1", []):
                        qty = int(item.get("hldg_qty", 0))
                        if qty > 0:
                            holdings.append({
                                "code": item.get("pdno", ""),
                                "name": item.get("prdt_name", ""),
                                "quantity": qty,
                                "buy_price": float(item.get("pchs_avg_pric", 0)),
                                "current_price": int(item.get("prpr", 0)),
                                "profit_pct": float(item.get("evlu_pfls_rt", 0)),
                            })
                    return holdings
    except Exception as e:
        logger.error(f"[자동매매] 잔고조회 예외 ({cred.get('mode')}): {e}")
    return None


def _get_balance_with_detail(cred):
    """잔고조회 + 실패 시 상세 에러 반환"""
    is_virtual = cred.get("is_virtual", True)
    base_url = VIRTUAL_BASE if is_virtual else REAL_BASE
    tr_id = "VTTC8434R" if is_virtual else "TTTC8434R"
    cano, acnt_prdt_cd = _parse_account_no(cred)

    headers = _make_headers(cred)
    headers["tr_id"] = tr_id
    params = {
        "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd,
        "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02",
        "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
    }

    try:
        r = requests.get(
            f"{base_url}/uapi/domestic-stock/v1/trading/inquire-balance",
            headers=headers, params=params, timeout=10,
        )
        d = r.json()
        logger.info(f"[자동매매] 잔고조회 응답: rt_cd={d.get('rt_cd')}, msg_cd={d.get('msg_cd')}, msg1={d.get('msg1','')[:50]}")
        return d
    except Exception as e:
        logger.error(f"[자동매매] 잔고조회 예외: {e}")
        return None


def _sell_stock(cred, code, qty, current_price=0):
    """KIS 매도 주문.
    모의투자: 지정가(00) + 현재가 사용 (시장가 미지원)
    실전투자: 시장가(01) 사용
    """
    is_virtual = cred.get("is_virtual", True)
    base_url = VIRTUAL_BASE if is_virtual else REAL_BASE
    tr_id = "VTTC0801U" if is_virtual else "TTTC0801U"
    cano, acnt_prdt_cd = _parse_account_no(cred)

    headers = _make_headers(cred)
    headers["tr_id"] = tr_id

    if is_virtual:
        # ★ 모의투자: 시장가(06) 미지원 → 지정가(00) + 현재가 사용
        body = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt_cd,
            "PDNO": code,
            "ORD_DVSN": "00",  # 지정가
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(int(current_price)) if current_price > 0 else "0",
        }
    else:
        # 실전투자: 시장가(01) 사용
        body = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt_cd,
            "PDNO": code,
            "ORD_DVSN": "01",  # 시장가
            "ORD_QTY": str(qty),
            "ORD_UNPR": "0",
        }

    logger.info(f"[자동매매] 매도 주문: {code} qty={qty} type={'지정가' if is_virtual else '시장가'} price={current_price}")

    try:
        r = requests.post(
            f"{base_url}/uapi/domestic-stock/v1/trading/order-cash",
            headers=headers, json=body, timeout=10,
        )
        d = r.json()
        success = d.get("rt_cd") == "0"
        if not success:
            logger.error(f"[자동매매] 매도 응답: rt_cd={d.get('rt_cd')}, msg_cd={d.get('msg_cd')}, msg1={d.get('msg1','')}")
        return {"success": success, "data": d}
    except Exception as e:
        return {"success": False, "error": str(e)}


def _send_alert(rule, reason, result):
    """카카오 알림 전송"""
    try:
        from app.services.kakao_alert import kakao
        mode_str = "모의" if rule.get("mode") == "virtual" else "실전"
        success_str = "성공" if result.get("success") else "실패"
        kakao.alert_system(
            f"{'[자동매도]' } {mode_str}투자\n"
            f"종목: {rule.get('stock_name', '')} ({rule.get('stock_code', '')})\n"
            f"사유: {reason}\n"
            f"결과: {success_str}"
        )
    except Exception as e:
        logger.warning(f"[자동매매] 카카오 알림 실패: {e}")


def _log_execution(mode, action, detail=""):
    """실행 로그를 DB에 기록"""
    try:
        db.table("scheduler_logs").insert({
            "job_name": f"kis_auto_trade_{mode}",
            "status": action,
            "duration_sec": 0,
            "success_count": 1 if action == "sell" else 0,
            "error_detail": detail[:2000] if detail else "",
            "executed_at": datetime.now(KST).isoformat(),
        }).execute()
    except Exception:
        pass


async def check_auto_trade_rules():
    """메인 체크 함수: 양 모드(virtual/real)의 활성 규칙을 체크하고 조건 충족 시 매도"""
    if not _ensure_table():
        return

    for mode in ["virtual", "real"]:
        try:
            rules_resp = db.table("kis_auto_trade_rules") \
                .select("*") \
                .eq("mode", mode) \
                .eq("enabled", True) \
                .execute()
            rules = rules_resp.data or []
            if not rules:
                continue

            logger.info(f"[자동매매] {mode} 활성 규칙 {len(rules)}개 확인")

            cred = _get_credentials(mode)
            if not cred or not cred.get("access_token"):
                logger.warning(f"[자동매매] {mode} 인증정보 없음 — Supabase kis_credentials 테이블에 {mode} 모드 데이터가 필요합니다")
                _log_execution(mode, "skip", f"인증정보 없음 (kis_credentials 테이블에 mode={mode} 없음)")
                continue

            # ★ account_no 검증: 비어있으면 잔고조회 불가
            cano, acnt_prdt_cd = _parse_account_no(cred)
            if not cano or len(cano) < 8:
                raw_acct = cred.get("account_no", "")
                logger.error(f"[자동매매] {mode} 계좌번호 누락 또는 형식 오류 (raw='{raw_acct}', parsed_cano='{cano}'). "
                             f"프론트엔드 KIS 페이지에서 API 설정을 저장하여 계좌번호를 동기화하세요.")
                _log_execution(mode, "skip", f"계좌번호 누락/오류: raw='{raw_acct}', cano='{cano}'. 프론트엔드에서 API 설정 재저장 필요.")
                continue

            logger.info(f"[자동매매] {mode} 인증정보 로드 완료 (app_key={cred.get('app_key','')[:8]}..., account={cano[:4]}****{acnt_prdt_cd})")

            # ★ 토큰 자동 갱신 (KIS 1분당 1회 제한 → 5분 간격으로만 갱신)
            now = datetime.now(KST)
            last_refresh = _last_token_refresh.get(mode)
            should_refresh = last_refresh is None or (now - last_refresh).total_seconds() > 300
            new_token = None
            if should_refresh:
                new_token = _refresh_token(cred)
                _last_token_refresh[mode] = now
                if new_token:
                    cred["access_token"] = new_token
                    logger.info(f"[자동매매] {mode} 토큰 자동 갱신 성공")
                else:
                    logger.warning(f"[자동매매] {mode} 토큰 갱신 실패, 기존 토큰으로 시도...")

            # 먼저 상세 로그로 잔고 조회 시도
            raw_resp = _get_balance_with_detail(cred)
            if raw_resp and raw_resp.get("rt_cd") == "0":
                holdings = []
                for item in raw_resp.get("output1", []):
                    qty = int(item.get("hldg_qty", 0))
                    if qty > 0:
                        holdings.append({
                            "code": item.get("pdno", ""),
                            "name": item.get("prdt_name", ""),
                            "quantity": qty,
                            "buy_price": float(item.get("pchs_avg_pric", 0)),
                            "current_price": int(item.get("prpr", 0)),
                            "profit_pct": float(item.get("evlu_pfls_rt", 0)),
                        })
            elif raw_resp and raw_resp.get("msg_cd") == "EGW00123":
                # 토큰 만료 → 갱신 후 재시도
                logger.info(f"[자동매매] {mode} 토큰 만료 감지, 강제 갱신 시도...")
                forced_token = _refresh_token(cred)
                if forced_token:
                    cred["access_token"] = forced_token
                    holdings = _get_balance(cred)
                else:
                    holdings = None
            else:
                err_msg = raw_resp.get("msg1", "") if raw_resp else "응답 없음"
                logger.warning(f"[자동매매] {mode} 잔고조회 실패: {err_msg}")
                _log_execution(mode, "skip", f"잔고조회 실패: {err_msg[:100]}")
                continue

            if holdings is None:
                _log_execution(mode, "skip", f"잔고조회 최종 실패 (토큰 갱신{'성공' if new_token else '실패'})")
                continue

            logger.info(f"[자동매매] {mode} 보유종목 {len(holdings)}개 조회 완료")

            # ★ 모니터링 현황 수집 (로그용)
            monitor_details = []

            for rule in rules:
                stock_code = rule.get("stock_code", "")
                stock_name = rule.get("stock_name", stock_code)
                holding = next(
                    (h for h in holdings if h["code"] == stock_code), None
                )

                if not holding:
                    # 보유종목에 없음 → 이미 매도됨, 규칙 비활성화
                    db.table("kis_auto_trade_rules") \
                        .update({"enabled": False, "updated_at": datetime.now(KST).isoformat()}) \
                        .eq("id", rule["id"]).execute()
                    monitor_details.append(f"{stock_code} 보유없음(비활성화)")
                    continue

                current_price = holding["current_price"]
                buy_price = float(rule.get("buy_price", 0))
                if buy_price <= 0:
                    buy_price = holding["buy_price"]

                profit_pct = ((current_price - buy_price) / buy_price * 100) if buy_price > 0 else 0

                # 보유일 계산
                hold_days = 0
                buy_date_str = rule.get("buy_date", "")
                if buy_date_str:
                    try:
                        hold_days = (date.today() - date.fromisoformat(buy_date_str)).days
                    except ValueError:
                        pass

                max_days = int(rule.get("max_hold_days", 30))
                strategy = rule.get("strategy", "fixed")
                now_iso = datetime.now(KST).isoformat()

                sell_reason = None

                if strategy == "smart":
                    # ━━━ 스마트형: 트레일링 스탑 ━━━
                    # grace = int(rule.get("grace_days", 7))  # ★ 유예기간 보류
                    sl = float(rule.get("sl_pct", 12))
                    trailing = float(rule.get("trailing_stop_pct", 5))
                    activation = float(rule.get("profit_activation_pct", 15))
                    peak = float(rule.get("peak_price", 0))

                    # ★ peak_price 업데이트 (현재가가 최고가보다 높으면 갱신)
                    if current_price > peak:
                        peak = float(current_price)
                        try:
                            db.table("kis_auto_trade_rules") \
                                .update({"peak_price": peak, "updated_at": now_iso}) \
                                .eq("id", rule["id"]).execute()
                        except Exception:
                            pass

                    if hold_days > 0:  # ★ 유예기간 보류 (기존: hold_days > grace)
                        # 1) 트레일링 스탑 (수익 활성화 후)
                        peak_profit = ((peak - buy_price) / buy_price * 100) if buy_price > 0 else 0
                        if peak_profit >= activation and peak > 0:
                            drop_from_peak = ((current_price - peak) / peak * 100)
                            if drop_from_peak <= -trailing:
                                sell_reason = (
                                    f"트레일링 (최고{peak_profit:.1f}%→현재{profit_pct:.1f}%, "
                                    f"하락{drop_from_peak:.1f}%<=-{trailing}%)"
                                )
                        # 2) 손절
                        if not sell_reason and profit_pct <= -sl:
                            sell_reason = f"손절 ({profit_pct:.1f}% <= -{sl}%)"

                    # 3) 만기
                    if not sell_reason and max_days > 0 and hold_days >= max_days:
                        sell_reason = f"최대보유일 ({hold_days}일 >= {max_days}일)"

                else:
                    # ━━━ 고정형: 기존 로직 그대로 ━━━
                    tp = float(rule.get("tp_pct", 10))
                    sl = float(rule.get("sl_pct", 5))
                    if profit_pct >= tp:
                        sell_reason = f"익절 ({profit_pct:.1f}% >= {tp}%)"
                    elif profit_pct <= -sl:
                        sell_reason = f"손절 ({profit_pct:.1f}% <= -{sl}%)"
                    elif max_days > 0 and hold_days >= max_days:
                        sell_reason = f"최대보유일 ({hold_days}일 >= {max_days}일)"

                if sell_reason:
                    # ★ 매도 실패 카운터 확인 (연속 실패 시 스킵)
                    fail_key = f"{mode}_{stock_code}"
                    fail_cnt = _sell_fail_count.get(fail_key, 0)
                    if fail_cnt >= 3:
                        monitor_details.append(f"{stock_code} {sell_reason} → 매도3회실패(스킵)")
                        continue

                    logger.info(
                        f"[자동매매] {mode} [{strategy}] 매도 실행: "
                        f"{stock_name}({stock_code}) "
                        f"수량={holding['quantity']} 사유={sell_reason}"
                    )
                    result = _sell_stock(cred, stock_code, holding["quantity"], current_price)

                    if result.get("success"):
                        # ★ 매도 성공 시에만 규칙 비활성화
                        db.table("kis_auto_trade_rules") \
                            .update({"enabled": False, "updated_at": now_iso}) \
                            .eq("id", rule["id"]).execute()
                        monitor_details.append(f"{stock_code} [{strategy}] {sell_reason} → 매도성공")
                        _sell_fail_count.pop(fail_key, None)  # 성공 시 카운터 리셋
                    else:
                        # ★ 매도 실패 시 규칙 유지, 실패 카운터 증가
                        _sell_fail_count[fail_key] = fail_cnt + 1
                        err_info = result.get("data", {}).get("msg1", "") or result.get("error", "")
                        monitor_details.append(f"{stock_code} [{strategy}] {sell_reason} → 매도실패({fail_cnt+1}/3, {err_info[:30]})")
                        logger.error(f"[자동매매] {mode} [{strategy}] 매도 실패({fail_cnt+1}/3): {stock_code} → {err_info}")

                    # 카카오 알림
                    _send_alert(rule, sell_reason, result)

                    # 로그 기록
                    _log_execution(mode, "sell" if result.get("success") else "sell_fail",
                                   f"[{strategy}] {stock_code} {sell_reason} → {'성공' if result.get('success') else '실패'}")
                else:
                    # ★ 매도 미충족 — 모니터링 현황 기록
                    if strategy == "smart":
                        peak = float(rule.get("peak_price", 0))
                        peak_profit = ((peak - buy_price) / buy_price * 100) if buy_price > 0 and peak > 0 else 0
                        trailing_active = hold_days > 0 and peak_profit >= float(rule.get("profit_activation_pct", 15))  # ★ 유예기간 보류
                        monitor_details.append(
                            f"{stock_code} [스마트] 수익{profit_pct:+.1f}% 최고{peak:,.0f}원({peak_profit:+.1f}%) "
                            f"{'추적ON' if trailing_active else '추적OFF'} {hold_days}일차"
                        )
                    else:
                        tp = float(rule.get("tp_pct", 10))
                        sl = float(rule.get("sl_pct", 5))
                        monitor_details.append(
                            f"{stock_code} [고정] 수익{profit_pct:+.1f}%(TP:{tp}/SL:-{sl}) {hold_days}일차"
                        )

            # ★ 모니터링 하트비트 로그 (5분마다 DB에 기록, 매도 발생 시 즉시 기록)
            now_for_log = datetime.now(KST)
            last_log_time = _last_monitor_log.get(mode)
            has_sell = any("매도" in d for d in monitor_details)
            should_log_monitor = has_sell or last_log_time is None or (now_for_log - last_log_time).total_seconds() > 300
            if should_log_monitor:
                _log_execution(mode, "monitor",
                               f"보유{len(holdings)}종목 | " + " | ".join(monitor_details) if monitor_details else "체크완료")
                _last_monitor_log[mode] = now_for_log

        except Exception as e:
            logger.error(f"[자동매매] {mode} 체크 오류: {e}")
            _log_execution(mode, "error", str(e))
