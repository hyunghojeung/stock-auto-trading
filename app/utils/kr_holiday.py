"""한국 공휴일/휴장일 판별 / Korean Market Holiday Checker
★ v9: 모든 시간은 KST(UTC+9) 기준
"""
from datetime import date, datetime, timedelta, timezone

# ★ v9: 한국 표준시 (KST = UTC+9)
KST = timezone(timedelta(hours=9))

def now_kst():
    """현재 한국 시간 반환 / Returns current Korean Standard Time"""
    return datetime.now(KST)

# 공휴일 DB: {날짜: 공휴일명} / Holiday DB: {date: holiday_name}
HOLIDAYS = {
    # 2025
    date(2025,1,1): "신정",
    date(2025,1,28): "설날연휴", date(2025,1,29): "설날", date(2025,1,30): "설날연휴",
    date(2025,3,1): "삼일절",
    date(2025,5,5): "어린이날·부처님오신날", date(2025,5,6): "대체공휴일",
    date(2025,6,6): "현충일", date(2025,8,15): "광복절",
    date(2025,10,3): "개천절",
    date(2025,10,5): "추석연휴", date(2025,10,6): "추석", date(2025,10,7): "추석연휴", date(2025,10,8): "대체공휴일(추석)",
    date(2025,10,9): "한글날", date(2025,12,25): "크리스마스",
    # 2026
    date(2026,1,1): "신정",
    date(2026,2,16): "설날연휴", date(2026,2,17): "설날", date(2026,2,18): "대체공휴일(설날)",
    date(2026,3,1): "삼일절", date(2026,3,2): "대체공휴일(삼일절)",
    date(2026,5,5): "어린이날",
    date(2026,5,24): "부처님오신날", date(2026,5,25): "대체공휴일(부처님오신날)",
    date(2026,6,6): "현충일",
    date(2026,8,15): "광복절", date(2026,8,17): "대체공휴일(광복절)",
    date(2026,9,24): "추석연휴", date(2026,9,25): "추석", date(2026,9,26): "추석연휴",
    date(2026,10,3): "개천절", date(2026,10,5): "대체공휴일(개천절)",
    date(2026,10,9): "한글날", date(2026,12,25): "크리스마스",
    # 2027
    date(2027,1,1): "신정",
    date(2027,2,5): "설날연휴", date(2027,2,6): "설날", date(2027,2,7): "설날연휴", date(2027,2,8): "대체공휴일(설날)",
    date(2027,3,1): "삼일절",
    date(2027,5,5): "어린이날", date(2027,5,13): "부처님오신날",
    date(2027,6,6): "현충일", date(2027,8,15): "광복절",
    date(2027,9,14): "추석연휴", date(2027,9,15): "추석", date(2027,9,16): "추석연휴",
    date(2027,10,3): "개천절", date(2027,10,9): "한글날", date(2027,12,25): "크리스마스",
}

def is_market_open_day(d):
    """장 운영일 여부 (주말/공휴일 제외) / Check if market is open on this date"""
    return d.weekday() < 5 and d not in HOLIDAYS

def get_holiday_name(d):
    """공휴일명 반환 (공휴일 아니면 None) / Returns holiday name or None"""
    return HOLIDAYS.get(d, None)

def get_market_status(now=None):
    """시장 상태 문자열 반환 / Returns market status string"""
    if now is None: now = now_kst()
    d = now.date()
    
    if d.weekday() >= 5:
        return "휴장 (주말)"
    
    holiday = HOLIDAYS.get(d)
    if holiday:
        return f"휴장 ({holiday})"
    
    t = now.strftime("%H:%M")
    if t < "09:00": return "장 시작 전"
    elif t <= "15:30": return "장 운영 중"
    else: return "장 마감"

def is_market_open_now(now=None):
    """현재 매매 가능 여부 종합 판단 / Check if trading is allowed right now"""
    if now is None: now = now_kst()
    if not is_market_open_day(now.date()):
        return False
    t = now.strftime("%H:%M")
    return "09:00" <= t <= "15:30"

def get_next_market_day(d):
    """다음 장 운영일 반환 / Returns next trading day"""
    nd = d + timedelta(days=1)
    while not is_market_open_day(nd): nd += timedelta(days=1)
    return nd
