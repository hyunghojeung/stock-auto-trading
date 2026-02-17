"""트레일링 스톱 익절 로직"""

class TrailingStop:
    def __init__(self, buy_price, atr_value, multiplier=2.0):
        self.buy_price = buy_price
        self.atr_value = atr_value
        self.multiplier = multiplier
        self.highest_price = buy_price
        self.stop_price = buy_price - (atr_value * multiplier)

    def update(self, current_price):
        """현재가 업데이트 → 스톱 가격 조정"""
        if current_price > self.highest_price:
            self.highest_price = current_price
            self.stop_price = self.highest_price - (self.atr_value * self.multiplier)
        return {
            "highest": self.highest_price,
            "stop_price": self.stop_price,
            "should_sell": current_price <= self.stop_price,
            "profit_pct": round((current_price - self.buy_price) / self.buy_price * 100, 2),
        }
