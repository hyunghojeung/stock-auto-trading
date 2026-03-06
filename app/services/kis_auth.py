"""KIS API 인증 — 한국투자증권 Open API OAuth2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
★ v2: API 문서 기반 전체 인증 체계 구현
  - 접근토큰발급 (인증-001): POST /oauth2/tokenP
  - 접근토큰폐기 (인증-002): POST /oauth2/revokeP
  - Hashkey: POST /uapi/hashkey (POST 주문 보안)
  - 실시간 웹소켓 접속키 발급 (실시간-000): POST /oauth2/Approval
"""
import requests
import logging
from datetime import datetime, timedelta
from app.core.config import config

logger = logging.getLogger(__name__)


class KISAuth:
    def __init__(self, is_live=False):
        self.is_live = is_live
        self.base_url = config.KIS_LIVE_BASE_URL if is_live else config.KIS_BASE_URL
        self.app_key = config.KIS_LIVE_APP_KEY if is_live else config.KIS_APP_KEY
        self.app_secret = config.KIS_LIVE_APP_SECRET if is_live else config.KIS_APP_SECRET
        self.access_token = None
        self.token_expired_at = None
        self.websocket_approval_key = None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 접근토큰발급 (인증-001)
    # POST /oauth2/tokenP
    # 토큰 유효기간: 일반고객 1일, 6시간 내 재호출 시 직전 토큰 리턴
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_token(self):
        """접근토큰 발급 (캐시됨, 만료 10분 전 자동 갱신)"""
        if self.access_token and self.token_expired_at and datetime.now() < self.token_expired_at - timedelta(minutes=10):
            return self.access_token

        if not self.app_key or not self.app_secret:
            mode = "실전" if self.is_live else "모의"
            logger.warning(f"[KIS] {mode} API Key 미설정")
            return None

        url = f"{self.base_url}/oauth2/tokenP"
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        try:
            res = requests.post(url, json=body, timeout=10)
            res.raise_for_status()
            data = res.json()
            self.access_token = data["access_token"]
            # expires_in: 유효기간(초), 예: 7776000 (약 90일, 제휴법인) 또는 86400 (1일, 일반)
            self.token_expired_at = datetime.now() + timedelta(seconds=int(data.get("expires_in", 86400)))
            mode = "실전" if self.is_live else "모의"
            logger.info(f"[KIS] {mode} 토큰 발급 성공 (만료: {self.token_expired_at})")
            return self.access_token
        except Exception as e:
            mode = "실전" if self.is_live else "모의"
            logger.error(f"[KIS] {mode} 토큰 발급 실패: {e}")
            raise

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 접근토큰폐기 (인증-002)
    # POST /oauth2/revokeP
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def revoke_token(self):
        """접근토큰 폐기 — 더 이상 사용하지 않을 때 호출"""
        if not self.access_token:
            return {"success": True, "message": "폐기할 토큰 없음"}

        url = f"{self.base_url}/oauth2/revokeP"
        body = {
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "token": self.access_token,
        }
        try:
            res = requests.post(url, json=body, timeout=10)
            data = res.json()
            self.access_token = None
            self.token_expired_at = None
            mode = "실전" if self.is_live else "모의"
            logger.info(f"[KIS] {mode} 토큰 폐기 완료: {data.get('message', '')}")
            return {"success": True, "code": data.get("code"), "message": data.get("message", "")}
        except Exception as e:
            logger.error(f"[KIS] 토큰 폐기 실패: {e}")
            return {"success": False, "error": str(e)}

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Hashkey (보안)
    # POST /uapi/hashkey
    # POST API 호출 시 Request Body를 hashkey로 변환하여 보안 강화
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_hashkey(self, body: dict) -> str:
        """POST 주문 요청의 body를 hashkey로 변환 (보안용)
        주문 API(매수/매도) 호출 시 헤더에 hashkey를 추가하면 보안 강화
        """
        url = f"{self.base_url}/uapi/hashkey"
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        try:
            res = requests.post(url, headers=headers, json=body, timeout=10)
            data = res.json()
            return data.get("HASH", "")
        except Exception as e:
            logger.debug(f"[KIS] hashkey 발급 실패 (무시): {e}")
            return ""

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 실시간 웹소켓 접속키 발급 (실시간-000)
    # POST /oauth2/Approval
    # 웹소켓 실시간 시세 수신 시 appkey/appsecret 대신 approval_key 사용
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_websocket_approval_key(self):
        """웹소켓 접속키 발급 — 실시간 시세 수신용"""
        if self.websocket_approval_key:
            return self.websocket_approval_key

        url = f"{self.base_url}/oauth2/Approval"
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,  # ★ 주의: secretkey (appsecret과 동일한 값)
        }
        try:
            res = requests.post(url, json=body, timeout=10)
            data = res.json()
            self.websocket_approval_key = data.get("approval_key", "")
            mode = "실전" if self.is_live else "모의"
            logger.info(f"[KIS] {mode} 웹소켓 접속키 발급 성공")
            return self.websocket_approval_key
        except Exception as e:
            logger.error(f"[KIS] 웹소켓 접속키 발급 실패: {e}")
            return ""

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 공통 헤더
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_headers(self):
        """GET API 호출용 공통 헤더"""
        return {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.get_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

    def get_order_headers(self, body: dict):
        """POST 주문용 헤더 (hashkey 포함)"""
        headers = self.get_headers()
        hashkey = self.get_hashkey(body)
        if hashkey:
            headers["hashkey"] = hashkey
        return headers


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 싱글톤 인스턴스
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

kis_mock = KISAuth(False)   # 모의투자
kis_live = KISAuth(True)    # 실전투자

def get_kis(is_live=False):
    return kis_live if is_live else kis_mock
