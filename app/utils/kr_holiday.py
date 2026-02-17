"""한국 공휴일/휴장일 판별"""
from datetime import date, datetime, timedelta

HOLIDAYS = {
    date(2026,1,1), date(2026,2,16), date(2026,2,17), date(2026,2,18),
    date(2026,3,1), date(2026,5,5), date(2026,5,24), date(2026,6,6),
    date(2026,8,15), date(2026,9,24), date(2026,9,25), date(2026,9,26),
    date(2026,10,3), date(2026,10,9), date(2026,12,25),
}

def is_market_open_day(d):
    return d.weekday() < 5 and d not in HOLIDAYS

def get_market_status(now=None):
    if now is None: now = datetime.now()
    if not is_market_open_day(now.date()):
        return "휴장 (주말)" if now.weekday() >= 5 else "휴장 (공휴일)"
    t = now.strftime("%H:%M")
    if t < "09:00": return "장 시작 전"
    elif t <= "15:30": return "장 운영 중"
    else: return "장 마감"

def get_next_market_day(d):
    nd = d + timedelta(days=1)
    while not is_market_open_day(nd): nd += timedelta(days=1)
    return nd
