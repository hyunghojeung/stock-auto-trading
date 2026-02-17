"""수수료/세금 계산"""
from app.core.config import config

def calc_net_profit(buy_price, sell_price, quantity):
    gross = (sell_price - buy_price) * quantity
    buy_comm = round(buy_price * quantity * config.COMMISSION_RATE, 2)
    sell_comm = round(sell_price * quantity * config.COMMISSION_RATE, 2)
    tax = round(sell_price * quantity * config.SELL_TAX_RATE, 2)
    total_cost = buy_comm + sell_comm + tax
    net = gross - total_cost
    pct = ((sell_price - buy_price) / buy_price * 100) - (total_cost / (buy_price * quantity) * 100)
    return {"gross": round(gross,2), "commission": round(buy_comm+sell_comm,2),
            "tax": tax, "net_profit": round(net,2), "profit_pct": round(pct,4)}
