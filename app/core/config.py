"""환경변수 및 설정 관리"""
import os
from dotenv import load_dotenv
from datetime import timezone, timedelta

load_dotenv()

# ============================================================
# 한국 표준시 (KST = UTC+9) — 모든 파일에서 이것을 사용
# ============================================================
KST = timezone(timedelta(hours=9))


class Config:
    # ── KIS 모의투자 (MCP 호환: KIS_PAPER_APP_KEY / KIS_PAPER_APP_SECRET)
    KIS_APP_KEY = os.getenv("KIS_APP_KEY", "") or os.getenv("KIS_PAPER_APP_KEY", "")
    KIS_APP_SECRET = os.getenv("KIS_APP_SECRET", "") or os.getenv("KIS_PAPER_APP_SECRET", "")
    KIS_CANO = os.getenv("KIS_CANO", "") or os.getenv("KIS_PAPER_STOCK", "")
    KIS_ACNT_PRDT_CD = os.getenv("KIS_ACNT_PRDT_CD", "") or os.getenv("KIS_PROD_TYPE", "01")
    KIS_BASE_URL = os.getenv("KIS_BASE_URL", "https://openapivts.koreainvestment.com:29443")

    # ── KIS 실전투자 (MCP 호환: KIS_APP_KEY / KIS_APP_SECRET)
    KIS_LIVE_APP_KEY = os.getenv("KIS_LIVE_APP_KEY", "")
    KIS_LIVE_APP_SECRET = os.getenv("KIS_LIVE_APP_SECRET", "")
    KIS_LIVE_BASE_URL = os.getenv("KIS_LIVE_BASE_URL", "https://openapi.koreainvestment.com:9443")
    KIS_LIVE_CANO = os.getenv("KIS_LIVE_CANO", "") or os.getenv("KIS_ACCT_STOCK", "")

    # ── KIS HTS ID (MCP 호환, 일부 API에 필요)
    KIS_HTS_ID = os.getenv("KIS_HTS_ID", "")

    SUPABASE_URL = os.getenv("SUPABASE_URL", "")
    SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")
    SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

    SITE_PASSWORD = os.getenv("SITE_PASSWORD", "4332")
    PORT = int(os.getenv("PORT", "8000"))

    MARKET_OPEN = "09:00"
    MARKET_CLOSE = "15:30"

    TARGET_ASSET = 1_000_000_000
    INITIAL_CAPITAL = 1_000_000
    COMMISSION_RATE = 0.00015
    SELL_TAX_RATE = 0.0018


config = Config()
