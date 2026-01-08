import streamlit as st
import pandas as pd
import hmac
import os
import logging
from datetime import datetime
from src import pipeline, engine, config, utils

# [System Configuration]
os.environ["GOOGLE_CLOUD_PROJECT"] = "gmo-weekly"

# [Page Configuration]
st.set_page_config(
    page_title="GMO Data Hub | Advanced Sync System",
    page_icon="📊",
    layout="wide"
)

# --- [Interface Styling] ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; }
    .main-header { font-size: 2.2rem; font-weight: 700; color: #111827; margin-bottom: 0.5rem; letter-spacing: -0.025em; }
    .sub-header { font-size: 1.1rem; color: #6b7280; margin-bottom: 2rem; }
    .stCard { border-radius: 12px; padding: 24px; background-color: #ffffff; border: 1px solid #e5e7eb; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .filter-label { font-weight: 600; color: #374151; margin-bottom: 8px; font-size: 0.95rem; }
    </style>
    """, unsafe_allow_html=True)

# --- [Authentication Layer] ---
def check_password():
    def password_entered():
        effective_password = os.environ.get("password") or st.secrets.get("password")
        if effective_password and hmac.compare_digest(st.session_state["password_input"], effective_password):
            st.session_state["password_correct"] = True
            del st.session_state["password_input"]
        else:
            st.session_state["password_correct"] = False

    if st.session_state.get("password_correct", False):
        return True

    st.markdown("<div style='margin-top: 100px;'>", unsafe_allow_html=True)
    st.markdown("<h2 style='text-align: center;'>🔒 GMO Data Hub 접속 인증</h2>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #6b7280;'>시스템 접근을 위해 관리자 암호를 입력하세요.</p>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.text_input("Access Password", type="password", on_change=password_entered, key="password_input")
        if "password_correct" in st.session_state and not st.session_state["password_correct"]:
            st.error("❌ 인증에 실패했습니다. 올바른 비밀번호를 입력하세요.")
    return False

# --- [Core Application Logic] ---
def main():
    if not check_password(): st.stop()

    # Sidebar Navigation
    with st.sidebar:
        st.markdown("### 🛰️ GMO Data Hub")
        st.caption(f"Project: {os.environ['GOOGLE_CLOUD_PROJECT']}")
        if st.button("Logout", use_container_width=True):
            st.session_state["password_correct"] = False
            st.rerun()
        st.divider()
        app_mode = st.radio("NAVIGATION", 
            ["Submission Dashboard", "Weekly Report Cleansing", "Weekly Report Submission", "Data Report"],
            index=0)
        
    # Header Helper
    def render_inline_header(title, subtitle, select_key):
        h_col, s_col = st.columns([7, 3])
        with h_col: 
            st.markdown(f"<h1 class='main-header'>{title}</h1>", unsafe_allow_html=True)
            st.markdown(f"<p class='sub-header'>{subtitle}</p>", unsafe_allow_html=True)
        with s_col: 
            return st.selectbox("Division Selection", ('MX', 'CE'), key=select_key)

    # [1] Submission Dashboard
    if app_mode == "Submission Dashboard":
        selected_div = render_inline_header("Submission Status", "주차별 데이터 현황 모니터링", "dash_div")
        engine.render_dashboard_ui(selected_div)

    # [2] Data Cleansing
    elif app_mode == "Weekly Report Cleansing":
        selected_div = render_inline_header("Data Cleansing",  "법인 Raw data 클렌징.", "cln_div")
        uploaded_file = st.file_uploader("Upload Source (CSV/XLSX)", type=['csv', 'xlsx'])
        
        if uploaded_file:
            if st.button("Execute Pipeline", use_container_width=True, type="primary"):
                with st.spinner("Processing Business Logic..."):
                    # pipeline 내부의 load_csv_safely에서 UploadedFile 스트림을 직접 처리함
                    df_cleaned = pipeline.run_smart_process(uploaded_file, selected_div)
                    st.success("Validation & Mapping Complete.")
                    st.dataframe(df_cleaned.head(100), use_container_width=True)
                    st.download_button("📥 Download Cleaned CSV", 
                                     data=df_cleaned.to_csv(index=False, encoding='utf-8-sig'), 
                                     file_name=f"CLEANED_{selected_div}.csv", mime='text/csv', use_container_width=True)

    # [3] Data Upload (Smart Sync)
    elif app_mode == "Weekly Report Submission":
        selected_div = render_inline_header("Weekly Report Submission", "데이터 취합", "up_div")
        uploaded_cleaned = st.file_uploader("Upload Validated CSV", type=['csv'])
        
        if uploaded_cleaned:
            df_preview = pd.read_csv(uploaded_cleaned)
            st.markdown("##### 📋 Upload Preview (Top 5)")
            st.dataframe(df_preview.head(5), use_container_width=True)
            
            if st.button("🚀 Finalize Smart Sync", use_container_width=True, type="primary"):
                with st.spinner("Analyzing data drift & executing partial refresh..."):
                    try:
                        # [Pro Engine] 시점 기준 스마트 리프레시 로직 호출
                        count = engine.sync_to_bigquery(uploaded_cleaned, selected_div)
                        
                        if count > 0:
                            st.success(f"✅ Smart Sync Complete: {count:,} records refreshed from the first divergence point.")
                            st.balloons()
                        else:
                            st.info("✅ No changes detected. Database is already consistent with the upload file.")
                            
                    except ValueError as ve:
                        # engine.py에서 raise ValueError("message", error_df)로 던진 인자 2개를 언팩
                        st.error("❌ Validation Failed: Standard 기준에 어긋나는 데이터가 발견되었습니다.")
                        if len(ve.args) > 1:
                            error_df = ve.args[1]
                            st.dataframe(error_df, use_container_width=True)
                            
                            err_csv = error_df.to_csv(index=False, encoding='utf-8-sig')
                            st.download_button("📥 Download Error Report", data=err_csv, 
                                             file_name=f"ERR_{selected_div}_{datetime.now().strftime('%H%M%S')}.csv", 
                                             mime='text/csv', use_container_width=True)
                        else:
                            st.error(str(ve))
                    except Exception as e:
                        st.error(f"❌ System Error: {str(e)}")

    # [4] Data Report (Cost-Optimized & Reliable Download)
    elif app_mode == "Data Report":
        selected_div = render_inline_header("Data Report", "데이터 다운로드", "dl_div")
        
        with st.container():
            st.markdown("<p class='filter-label'>📅 Extraction Period</p>", unsafe_allow_html=True)
            d_col1, d_col2 = st.columns(2)
            with d_col1:
                start_date = st.date_input("Start Date", value=datetime(2026, 1, 1))
            with d_col2:
                end_date = st.date_input("End Date", value=datetime.now())
        
        st.write("")
        if st.button(f"Generate {selected_div} Master Report", use_container_width=True, type="primary"):
            if start_date > end_date:
                st.error("Error: Start date cannot be later than end date.")
            else:
                with st.spinner("Querying Master Database..."):
                    # 리팩토링된 엔진은 데이터가 없어도 스키마 헤더가 포함된 DF를 반환함
                    df_report = engine.get_report_df(selected_div, start_date, end_date)
                    
                    fname = f"REPORT_{selected_div}_{datetime.now().strftime('%Y%m%d')}.csv"
                    
                    if df_report.empty:
                        st.warning(f"⚠️ {selected_div} 본부의 해당 기간 데이터가 DB에 없습니다.")
                    else:
                        st.success(f"Successfully retrieved {len(df_report):,} records.")
                    
                    # [Architect Logic] 데이터 유무와 상관없이 상시 다운로드 버튼 활성화
                    csv_data = df_report.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label=f"📥 {selected_div} 리포트 다운로드 (CSV)",
                        data=csv_data,
                        file_name=fname,
                        mime='text/csv',
                        use_container_width=True
                    )

if __name__ == "__main__":
    main()