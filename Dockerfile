# 1. Python 3.11 공식 슬림 이미지 사용 (용량 최적화)
FROM python:3.11-slim

# 2. 시스템 환경 변수 설정
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

# 3. 작업 디렉토리 생성
WORKDIR /app

# 4. 필수 시스템 패키지 설치 (의존성 해결)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 5. 의존성 파일 먼저 복사 (캐싱 활용으로 빌드 속도 향상)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. 소스 코드 전체 복사
COPY . .

# 7. 포트 개방
EXPOSE 8080

# 8. Streamlit 실행 설정
# --server.address=0.0.0.0은 컨테이너 외부 접속을 위해 필수입니다.
CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]