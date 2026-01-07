import os
import requests
import google.auth
import subprocess
import platform  # OS 감지를 위해 추가
from google.auth.transport.requests import Request
from google.oauth2 import id_token

def get_id_token(url):
    """
    환경(로컬 vs 서버, Windows vs Linux)을 감지하여 적절한 방식으로 ID Token을 가져옵니다.
    """
    try:
        # 1. 정석적인 방법 (Cloud Run / 서비스 계정)
        auth_req = Request()
        return id_token.fetch_id_token(auth_req, url)
    except Exception:
        # 2. 로컬 개발 환경 (gcloud CLI Fallback)
        try:
            print("🔄 Switching to local gcloud token...")
            
            # [Fix] Windows에서는 'gcloud.cmd', Mac/Linux에서는 'gcloud'를 사용해야 함
            gcloud_cmd = "gcloud.cmd" if platform.system() == "Windows" else "gcloud"
            
            token = subprocess.check_output(
                [gcloud_cmd, "auth", "print-identity-token"], 
                text=True
            ).strip()
            return token
        except FileNotFoundError:
            # gcloud.cmd도 못 찾을 경우의 예외 처리
            raise Exception(f"❌ '{gcloud_cmd}' 명령어를 찾을 수 없습니다. 환경 변수 PATH를 확인하세요.")
        except subprocess.CalledProcessError:
            raise Exception("❌ gcloud 인증 실패. 'gcloud auth login'을 먼저 수행하세요.")

def call_private_cloud_run(service_url):
    print(f"🔒 Authenticating to: {service_url}")
    
    try:
        token = get_id_token(service_url)
    except Exception as e:
        print(f"⛔ Auth Error: {e}")
        return

    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        resp = requests.get(service_url, headers=headers)
        print(f"✅ Status: {resp.status_code}")
        print(f"📄 Response: {resp.text[:200]}") 
    except Exception as e:
        print(f"🔥 Request Failed: {e}")

if __name__ == "__main__":
    TARGET_URL = "https://marketing-sync-656774857370.asia-northeast3.run.app"
    call_private_cloud_run(TARGET_URL)