"""점수제 종목 선별 (100점 만점)"""
from datetime import datetime, date
from app.core.database import db
from app.services.kis_stock import get_daily_candles
from app.utils.indicators import sma, rsi, macd, volume_ratio
import time

async def score_and_select(stocks, top_n=10):
    """전종목 점수 계산 후 상위 N개 선별"""
    scored = []
    for stock in stocks:
        score = calculate_score(stock)
        if score > 30:  # 최소 점수 이상만
            stock["score"] = score
            scored.append(stock)

    scored.sort(key=lambda x: x["score"], reverse=True)
    top = scored[:top_n]

    # DB 저장
    today = date.today().isoformat()
    for s in top:
        try:
            db.table("watchlist").upsert({
                "stock_code": s["code"],
                "stock_name": s["name"],
                "score": s["score"],
                "volume_score": s.get("volume_score", 0),
                "trend_score": s.get("trend_score", 0),
                "theme_score": s.get("theme_score", 0),
                "technical_score": s.get("technical_score", 0),
                "supply_score": s.get("supply_score", 0),
                "status": "감시중",
                "scan_date": today,
            }).execute()
        except Exception as e:
            print(f"[DB 저장 오류] {s['name']}: {e}")

    print(f"[선별] 상위 {len(top)}개 종목 선별 완료")
    return top

def calculate_score(stock):
    """개별 종목 점수 계산 (100점 만점)"""
    total = 0

    # 1. 거래량 점수 (30점)
    vol_score = _volume_score(stock)
    stock["volume_score"] = vol_score
    total += vol_score

    # 2. 가격 상승 추세 점수 (25점)
    trend_score = _trend_score(stock)
    stock["trend_score"] = trend_score
    total += trend_score

    # 3. 테마/뉴스 점수 (20점) - 거래량 급증으로 대체
    theme_score = _theme_score(stock)
    stock["theme_score"] = theme_score
    total += theme_score

    # 4. 기술적 신호 점수 (15점)
    tech_score = _technical_score(stock)
    stock["technical_score"] = tech_score
    total += tech_score

    # 5. 수급 점수 (10점)
    supply_score = _supply_score(stock)
    stock["supply_score"] = supply_score
    total += supply_score

    return round(total, 2)

def _volume_score(stock):
    """거래량 추세 점수 (30점)"""
    vol = stock.get("volume", 0)
    if vol > 10000000: return 30  # 1천만주 이상
    elif vol > 5000000: return 25
    elif vol > 1000000: return 20
    elif vol > 500000: return 15
    elif vol > 100000: return 10
    return 5

def _trend_score(stock):
    """상승 추세 점수 (25점)"""
    change = stock.get("change_pct", 0)
    if change > 5: return 25
    elif change > 3: return 20
    elif change > 1: return 15
    elif change > 0: return 10
    elif change > -2: return 5
    return 0

def _theme_score(stock):
    """테마/관심도 점수 (20점) - 거래대금 기반"""
    vol = stock.get("volume", 0)
    price = stock.get("price", 0)
    trade_value = vol * price
    if trade_value > 100000000000: return 20  # 1000억 이상
    elif trade_value > 50000000000: return 15
    elif trade_value > 10000000000: return 10
    elif trade_value > 1000000000: return 5
    return 0

def _technical_score(stock):
    """기술적 신호 점수 (15점)"""
    change = stock.get("change_pct", 0)
    if change > 0 and stock.get("volume", 0) > 500000:
        return 10
    elif change > 0:
        return 5
    return 0

def _supply_score(stock):
    """수급 점수 (10점)"""
    if stock.get("change_pct", 0) > 0 and stock.get("volume", 0) > 1000000:
        return 7
    elif stock.get("change_pct", 0) > 0:
        return 3
    return 0
