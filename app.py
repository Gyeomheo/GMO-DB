import streamlit as st
import pandas as pd
from engine import DataIntegrityEngine
import hmac
import os
import plotly.graph_objects as go
from datetime import datetime

# [Best Practice] 시스템 설정: 광폭 레이아웃 및 페이지 타이틀
st.set_page_config(page_title="Gyeomti-hub | Marketing Sync", layout="wide")

# [Security] 최대 업로드 허용 용량 (200MB)
MAX_FILE_SIZE = 200 * 1024 * 1024 

# 1. 사업부별 법인 순서 정의 (Governance: 관리 리스트 정규화)
DIVISION_CONFIG = {
    "MX": [
        "SEA", "SEAU", "SEDA", "SEF", "SEG", "SEUK", "SGE", "SIEL", "SAVINA", "SCIC",
        "SEAD_CRO", "SEAD_SLO", "SEASA", "SEBN_BE", "SEBN_NL", "SEC", "SECA", "SECH",
        "SECZ_CZ", "SECZ_SK", "SEH", "SEHK", "SEI", "SEIB_ES", "SEIB_PT", "SEIN",
        "SEJ", "SEM", "SENA", "SENZ", "SEPCO", "SEPOL", "SEPR", "SEROM", "SESAR",
        "SESP", "SET", "SETK", "SME", "TSE"
    ],
    "CE": [
        "SEA", "SEAU", "SEDA", "SEF", "SEG", "SEUK", "SGE", "SIEL", "SAVINA", "SCIC",
        "SEASA", "SEBN_BE", "SEBN_NL", "SECA", "SECH", "SECZ_CZ", "SECZ_SK", "SEH", 
        "SEHK", "SEI", "SEIB_PT", "SEIN", "SEM", "SENZ", "SEPOL", "SEPR", "SEROM", 
        "SESAR", "SESP", "SETK", "SME", "TSE"
    ]
}

@st.cache_resource(show_spinner=False)
def get_engine():
    """엔진 인스턴스를 캐싱하여 DB 연결 오버헤드 방지"""
    return DataIntegrityEngine()

@st.cache_data(ttl=600)
def get_db_status(division):
    """[Read-through Cache] BigQuery 누적 현황 조회 및 10분간 캐싱"""
    engine = get_engine()
    return engine.get_current_db_status(division)

def check_password():
    """[Security] HMAC 기반 타이밍 공격 방지 비밀번호 검증"""
    def password_entered():
        effective_password = os.environ.get("password") or st.secrets.get("password")
        if effective_password and hmac.compare_digest(st.session_state["password_input"], effective_password):
            st.session_state.authenticated = True
            del st.session_state["password_input"] 
        else:
            st.session_state.authenticated = False
            
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.title("🔒 Access Required")
        st.text_input("접속 코드를 입력하세요", type="password", on_change=password_entered, key="password_input")
        return False
    return True

def render_status_heatmap(df, division, title_prefix="누적"):
    """[View] 현황 데이터를 히트맵으로 시각화"""
    st.divider()
    st.subheader(f"🗓️ {division} {title_prefix} 업로드 현황")

    target_order = DIVISION_CONFIG.get(division, [])
    
    try:
        if df.empty:
            st.info("표시할 현황 데이터가 없습니다.")
            return

        # 데이터 집계 및 피벗 (engine에서 카운트해온 row_count 활용)
        # 만약 직접 업로드한 df라면 'count'를 생성, DB에서 온 df라면 'row_count' 사용
        val_col = 'row_count' if 'row_count' in df.columns else 'count'
        if val_col not in df.columns:
            df = df.groupby(['subsidiary', 'week']).size().reset_index(name='count')
            val_col = 'count'

        pivot_df = df.pivot(index='subsidiary', columns='week', values=val_col)
        pivot_df = pivot_df.reindex(index=target_order).fillna(0)

        # Plotly 인터랙티브 히트맵 설계
        fig = go.Figure(data=go.Heatmap(
            z=pivot_df.values, x=pivot_df.columns, y=pivot_df.index,
            colorscale='YlGnBu', 
            hovertemplate='법인: %{y}<br>주차: %{x}주<br>데이터 건수: %{z}<extra></extra>'
        ))

        fig.update_layout(
            height=max(400, 25 * len(target_order) + 150),
            xaxis_title="주차 (Week)", yaxis_title="법인 (Subsidiary)",
            yaxis={'autorange': 'reversed'} 
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"시각화 중 오류 발생: {e}")

def main():
    if not check_password():
        st.info("💡 안전한 접속을 위해 코드가 필요합니다.")
        return 

    engine = get_engine()

    # --- 사이드바 설정 영역 ---
    st.sidebar.title("🏢 Division Select")
    if 'division' not in st.session_state: 
        st.session_state.division = 'MX'
    
    selected = st.sidebar.radio("사업부 선택:", ('MX', 'CE'), index=0 if st.session_state.division=='MX' else 1)
    
    if selected != st.session_state.division:
        st.session_state.division = selected
        st.cache_data.clear()
        st.rerun()

    # [요청 반영] 사업부별 맞춤형 다운로드 버튼 라벨
    st.sidebar.divider()
    download_label = f"📥 {st.session_state.division} 데이터 다운로드 (Excel)"
    
    if st.sidebar.button(download_label):
        with st.spinner(f"{st.session_state.division} 데이터 추출 중..."):
            export_df = engine.get_export_data(st.session_state.division)
            st.sidebar.download_button(
                label="💾 엑셀 파일 저장",
                data=export_df.to_csv(index=False).encode('utf-8-sig'),
                file_name=f"GMO_Export_{st.session_state.division}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

    # --- 메인 영역 타이틀 ---
    st.title(f"📊 GMO Marketing Sync Hub - {st.session_state.division}")

    # [요청 반영] 1순위: New Report Upload 섹션을 최상단으로 이동
    st.subheader("📤 New Report Upload")
    uploaded = st.file_uploader("파일을 드래그하거나 선택하세요 (CSV, XLSX)", type=['csv', 'xlsx'])
    
    if uploaded:
        # (기존 파일 처리 로직 동일...)
        try:
            file_df = engine.process_file(uploaded, uploaded.name)
            st.success(f"✅ 파일 로드 성공: {len(file_df):,}건의 데이터가 감지되었습니다.")
            
            # 동기화 실행 버튼 로직...
            if st.button("🚀 BigQuery 동기화 실행"):
                # ... (중략) ...
                st.cache_data.clear()
                st.rerun()
        except Exception as e:
            st.error(f"데이터 처리 오류: {e}")

    st.divider()

    # [요청 반영] 2순위: 현재 DB 누적 현황 확인 섹션을 하단으로 이동
    with st.expander(f"🌐 현재 {st.session_state.division} DB 누적 업로드 현황 확인", expanded=False):
        db_status = get_db_status(st.session_state.division)
        render_status_heatmap(db_status, st.session_state.division, "DB 누적")

if __name__ == "__main__":
    main()