import streamlit as st
import pandas as pd
import hmac
import os
from datetime import datetime
from src import pipeline, engine, config, utils

# [System Configuration]
os.environ["GOOGLE_CLOUD_PROJECT"] = "gmo-weekly"

# [Page Configuration]
st.set_page_config(page_title="GMO Data Hub", layout="wide")

# --- [Interface Styling] ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; }
    .main-header { font-size: 2.0rem; font-weight: 700; color: #111827; margin: 0; letter-spacing: -0.025em; }
    .stCard { border-radius: 8px; padding: 24px; background-color: #ffffff; border: 1px solid #e5e7eb; margin-bottom: 24px; }
    </style>
    """, unsafe_allow_html=True)

# --- [Authentication Layer] ---
def check_password():
    def password_entered():
        if hmac.compare_digest(st.session_state["password"], st.secrets["password"]):
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False
    if st.session_state.get("password_correct", False): return True
    st.markdown("<h2 style='text-align: center;'>GMO Data Hub</h2>", unsafe_allow_html=True)
    st.text_input("Access Password", type="password", on_change=password_entered, key="password")
    if "password_correct" in st.session_state: st.error("Authentication Failed.")
    return False

# --- [Core Application Logic] ---
def main():
    if not check_password(): st.stop()

    # Sidebar Navigation
    with st.sidebar:
        st.markdown("### GMO Data Hub")
        if st.button("Logout", use_container_width=True):
            st.session_state["password_correct"] = False
            st.rerun()
        st.divider()
        app_mode = st.radio("NAVIGATION", ["Submission Dashboard", "Weekly Report Cleansing", "Weekly Report Submission", "Data Report"])

    # Header Helper
    def render_inline_header(title, select_key):
        h_col, s_col = st.columns([8, 2])
        with h_col: st.markdown(f"<h1 class='main-header'>{title}</h1>", unsafe_allow_html=True)
        with s_col: return st.selectbox("Division Selection", ('MX', 'CE'), key=select_key, label_visibility="collapsed")

    # [1] Submission Dashboard
    if app_mode == "Submission Dashboard":
        selected_div = render_inline_header(f"{st.session_state.get('dash_div', 'MX')} Submission Status", "dash_div")
        st.divider()
        engine.render_dashboard_ui(selected_div)

    # [2] Data Cleansing
    elif app_mode == "Weekly Report Cleansing":
        selected_div = render_inline_header(f"{st.session_state.get('cln_div', 'MX')} Data Cleansing", "cln_div")
        uploaded_file = st.file_uploader("Upload Source", type=['csv', 'xlsx'], label_visibility="collapsed")
        if uploaded_file:
            if st.button("Execute Pipeline", use_container_width=True, type="primary"):
                with st.spinner("Processing..."):
                    df_cleaned = pipeline.run_smart_process(uploaded_file, selected_div)
                    st.success("Validation Complete.")
                    st.dataframe(df_cleaned.head(100), use_container_width=True)
                    st.download_button("📥 Download Cleaned CSV", data=df_cleaned.to_csv(index=False, encoding='utf-8-sig'), 
                                     file_name=f"CLEANED_{selected_div}.csv", mime='text/csv', use_container_width=True)

    # [3] Data Upload
    elif app_mode == "Weekly Report Submission":
        selected_div = render_inline_header(f"{st.session_state.get('up_div', 'MX')} Weekly Report Submission", "up_div")
        uploaded_cleaned = st.file_uploader("Upload Validated CSV", type=['csv'], label_visibility="collapsed")
        if uploaded_cleaned:
            st.dataframe(pd.read_csv(uploaded_cleaned).head(5), use_container_width=True)
            if st.button("Finalize Database Sync", use_container_width=True, type="primary"):
                with st.spinner("Synchronizing..."):
                    count = engine.sync_to_bigquery(uploaded_cleaned, selected_div)
                    st.success(f"Sync Successful: {count:,} records committed.")

    # [4] Data Report (Backend Logic 호출)
    elif app_mode == "Data Report":
        selected_div = render_inline_header(f"{st.session_state.get('dl_div', 'MX')} Data Report", "dl_div")
        
        if st.button(f"Download {selected_div} Data Report", use_container_width=True, type="primary"):
            with st.spinner("Preparing Report Format..."):
                # [Architect Fix] 복잡한 매핑 로직은 엔진 내부 get_report_df에서 처리
                df_report = engine.get_report_df(selected_div)
                
                csv_data = df_report.to_csv(index=False, encoding='utf-8-sig')
                fname = f"REPORT_{selected_div}_{datetime.now().strftime('%m%d_%H%M')}.csv"
                
                if df_report.empty:
                    st.warning("Notice: No records found. Template provided.")
                else:
                    st.success(f"Process Complete: {len(df_report):,} records formatted.")
                
                st.download_button(label=f"📥 Download {fname}", data=csv_data, file_name=fname, mime='text/csv', use_container_width=True)

if __name__ == "__main__":
    main()