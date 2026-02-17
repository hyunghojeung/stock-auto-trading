"""KIS API 인증"""
import requests
from datetime import datetime, timedelta
from app.core.config import config

class KISAuth:
    def __init__(self, is_live=False):
        self.is_live = is_live
        self.base_url = config.KIS_LIVE_BASE_URL if is_live else config.KIS_BASE_URL
        self.app_key = config.KIS_LIVE_APP_KEY if is_live else config.KIS_APP_KEY
        self.app_secret = config.KIS_LIVE_APP_SECRET if is_live else config.KIS_APP_SECRET
        self.access_token = None
        self.token_expired_at = None

    def get_token(self):
        if self.access_token and self.token_expired_at and datetime.now() < self.token_expired_at - timedelta(minutes=10):
            return self.access_token
        url = f"{self.base_url}/oauth2/tokenP"
        body = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}
        res = requests.post(url, json=body, timeout=10)
        res.raise_for_status()
        data = res.json()
        self.access_token = data["access_token"]
        self.token_expired_at = datetime.now() + timedelta(seconds=int(data.get("expires_in", 86400)))
        return self.access_token

    def get_headers(self):
        return {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.get_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

kis_mock = KISAuth(False)
kis_live = KISAuth(True)
def get_kis(is_live=False): return kis_live if is_live else kis_mock
