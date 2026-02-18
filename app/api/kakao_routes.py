"""카카오 인증 API (토큰 발급용)"""
from fastapi import APIRouter, Query
from fastapi.responses import RedirectResponse
import requests
import os

router = APIRouter(prefix="/api/kakao", tags=["카카오"])


@router.get("/auth")
async def kakao_auth():
    """카카오 로그인 페이지로 리다이렉트"""
    rest_api_key = os.getenv("KAKAO_REST_API_KEY", "")
    redirect_uri = os.getenv("KAKAO_REDIRECT_URI", "")
    if not rest_api_key:
        return {"error": "KAKAO_REST_API_KEY 미설정"}
    url = (
        f"https://kauth.kakao.com/oauth/authorize"
        f"?client_id={rest_api_key}"
        f"&redirect_uri={redirect_uri}"
        f"&response_type=code"
        f"&scope=talk_message"
    )
    return RedirectResponse(url)


@router.get("/callback")
async def kakao_callback(code: str = Query(...)):
    """카카오 인증 콜백 → 토큰 발급"""
    rest_api_key = os.getenv("KAKAO_REST_API_KEY", "")
    redirect_uri = os.getenv("KAKAO_REDIRECT_URI", "")
    
    url = "https://kauth.kakao.com/oauth/token"
    data = {
        "grant_type": "authorization_code",
        "client_id": rest_api_key,
        "redirect_uri": redirect_uri,
        "code": code,
    }
    
    res = requests.post(url, data=data, timeout=10)
    result = res.json()
    
    if "access_token" in result:
        os.environ["KAKAO_ACCESS_TOKEN"] = result["access_token"]
        os.environ["KAKAO_REFRESH_TOKEN"] = result.get("refresh_token", "")
        
        # 카카오 인스턴스 업데이트
        from app.services.kakao_alert import kakao
        kakao.access_token = result["access_token"]
        kakao.refresh_token = result.get("refresh_token", "")
        
        return {
            "success": True,
            "message": "카카오 알림 연결 완료!",
            "access_token": result["access_token"],
            "refresh_token": result.get("refresh_token", ""),
            "note": "이 토큰들을 Railway 환경변수에 저장하세요: KAKAO_ACCESS_TOKEN, KAKAO_REFRESH_TOKEN"
        }
    
    return {"success": False, "error": result}


@router.get("/status")
async def kakao_status():
    """카카오 알림 상태 확인"""
    from app.services.kakao_alert import kakao
    return {
        "configured": kakao.is_configured(),
        "rest_api_key": bool(kakao.rest_api_key),
        "access_token": bool(kakao.access_token),
        "refresh_token": bool(kakao.refresh_token),
    }


@router.get("/tokens")
async def kakao_tokens():
    """현재 저장된 토큰 확인"""
    from app.services.kakao_alert import kakao
    return {
        "access_token": kakao.access_token,
        "refresh_token": kakao.refresh_token,
    }


@router.get("/test")
async def kakao_test():
    """테스트 메시지 전송"""
    from app.services.kakao_alert import kakao
    if not kakao.is_configured():
        return {"success": False, "error": "카카오 알림이 설정되지 않았습니다"}
    result = kakao.alert_system("테스트 알림입니다. 10억 만들기 자동매매 시스템이 정상 작동 중입니다! 🚀")
    return {"success": result}
