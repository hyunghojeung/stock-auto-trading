"""카카오톡 알림 모듈 (나에게 보내기)"""
import requests
import os
from datetime import datetime


class KakaoAlert:
    """카카오톡 나에게 보내기 알림"""
    
    def __init__(self):
        self.rest_api_key = os.getenv("KAKAO_REST_API_KEY", "")
        self.redirect_uri = os.getenv("KAKAO_REDIRECT_URI", "")
        self.access_token = os.getenv("KAKAO_ACCESS_TOKEN", "")
        self.refresh_token = os.getenv("KAKAO_REFRESH_TOKEN", "")
    
    def is_configured(self):
        """카카오 알림 설정 여부"""
        return bool(self.access_token)
    
    def refresh_access_token(self):
        """토큰 갱신"""
        if not self.refresh_token or not self.rest_api_key:
            return False
        url = "https://kauth.kakao.com/oauth/token"
        data = {
            "grant_type": "refresh_token",
            "client_id": self.rest_api_key,
            "refresh_token": self.refresh_token,
        }
        try:
            res = requests.post(url, data=data, timeout=10)
            result = res.json()
            if "access_token" in result:
                self.access_token = result["access_token"]
                os.environ["KAKAO_ACCESS_TOKEN"] = self.access_token
                if "refresh_token" in result:
                    self.refresh_token = result["refresh_token"]
                    os.environ["KAKAO_REFRESH_TOKEN"] = self.refresh_token
                return True
        except Exception as e:
            print(f"[카카오 토큰 갱신 오류] {e}")
        return False
    
    def send_message(self, text):
        """나에게 텍스트 메시지 보내기"""
        if not self.is_configured():
            print("[카카오] 알림 미설정")
            return False
        
        url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        import json
        template = {
            "object_type": "text",
            "text": text,
            "link": {
                "web_url": os.getenv("DASHBOARD_URL", ""),
                "mobile_web_url": os.getenv("DASHBOARD_URL", ""),
            },
            "button_title": "대시보드 열기",
        }
        
        try:
            res = requests.post(url, headers=headers, data={"template_object": json.dumps(template)}, timeout=10)
            if res.status_code == 200:
                print(f"[카카오] 알림 전송 성공")
                return True
            elif res.status_code == 401:
                # 토큰 만료 → 갱신 시도
                if self.refresh_access_token():
                    return self.send_message(text)
                print("[카카오] 토큰 만료, 갱신 실패")
            else:
                print(f"[카카오] 전송 실패: {res.status_code} {res.text}")
        except Exception as e:
            print(f"[카카오] 오류: {e}")
        return False
    
    # ===== 매매 알림 템플릿 =====
    
    def alert_buy(self, stock_name, price, quantity, signals):
        """매수 알림"""
        now = datetime.now().strftime("%H:%M:%S")
        text = (
            f"🟢 매수 체결\n"
            f"━━━━━━━━━━━━\n"
            f"종목: {stock_name}\n"
            f"매수가: {price:,}원\n"
            f"수량: {quantity}주\n"
            f"투자금: {price * quantity:,}원\n"
            f"신호: {signals}\n"
            f"시간: {now}"
        )
        return self.send_message(text)
    
    def alert_sell(self, stock_name, buy_price, sell_price, quantity, net_profit, reason):
        """매도 알림"""
        now = datetime.now().strftime("%H:%M:%S")
        profit_pct = (sell_price - buy_price) / buy_price * 100
        emoji = "🔴" if net_profit < 0 else "🟢"
        text = (
            f"{emoji} 매도 체결\n"
            f"━━━━━━━━━━━━\n"
            f"종목: {stock_name}\n"
            f"매수가: {buy_price:,}원\n"
            f"매도가: {sell_price:,}원\n"
            f"수량: {quantity}주\n"
            f"순수익: {net_profit:+,.0f}원 ({profit_pct:+.2f}%)\n"
            f"사유: {reason}\n"
            f"시간: {now}"
        )
        return self.send_message(text)
    
    def alert_daily_report(self, strategy_name, total_asset, daily_profit, win_count, lose_count, win_rate):
        """일일 리포트 알림"""
        today = datetime.now().strftime("%Y년 %m월 %d일")
        progress = total_asset / 1_000_000_000 * 100
        text = (
            f"📊 일일 리포트\n"
            f"━━━━━━━━━━━━\n"
            f"날짜: {today}\n"
            f"전략: {strategy_name}\n"
            f"총 자산: {total_asset:,.0f}원\n"
            f"오늘 수익: {daily_profit:+,.0f}원\n"
            f"매매: {win_count}승 {lose_count}패 (승률 {win_rate:.1f}%)\n"
            f"목표 진행률: {progress:.4f}%\n"
            f"━━━━━━━━━━━━\n"
            f"🎯 100만원 → 10억"
        )
        return self.send_message(text)
    
    def alert_stop_loss(self, stock_name, buy_price, current_price, reason):
        """손절 경고 알림"""
        loss_pct = (current_price - buy_price) / buy_price * 100
        text = (
            f"⚠️ 손절 실행\n"
            f"━━━━━━━━━━━━\n"
            f"종목: {stock_name}\n"
            f"매수가: {buy_price:,}원\n"
            f"현재가: {current_price:,}원\n"
            f"손실률: {loss_pct:.2f}%\n"
            f"사유: {reason}"
        )
        return self.send_message(text)
    
    def alert_blocked(self, stock_name, reason, unblock_date):
        """종목 차단 알림"""
        text = (
            f"🚫 종목 차단\n"
            f"━━━━━━━━━━━━\n"
            f"종목: {stock_name}\n"
            f"사유: {reason}\n"
            f"차단 해제: {unblock_date}"
        )
        return self.send_message(text)
    
    def alert_system(self, message):
        """시스템 알림"""
        now = datetime.now().strftime("%H:%M:%S")
        text = f"⚙️ 시스템 알림\n━━━━━━━━━━━━\n{message}\n시간: {now}"
        return self.send_message(text)


# 싱글턴 인스턴스
kakao = KakaoAlert()
