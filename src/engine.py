import pandas as pd
import logging
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import List, Optional

import google.auth
from google.cloud import bigquery
from sqlalchemy import create_engine, text
from . import config  # 프로젝트 공통 설정 참조

# 로그 설정
logging.basicConfig(level=logging.INFO)

class DataIntegrityEngine:
    def __init__(self):
        """[Architecture] 인프라 연결 초기화"""
        try:
            self.credentials, _ = google.auth.default()
            self.project = config.BQ_PROJECT
            logging.info(f"🔗 Connecting to GCP Project: {self.project}")

            self.client = bigquery.Client(credentials=self.credentials, project=self.project)
            self.table_ref = f"{self.project}.{config.BQ_DATASET}.{config.BQ_TABLE}"
            
            db_url = f"bigquery://{self.project}/{config.BQ_DATASET}"
            self.db_engine = create_engine(db_url, list_tables_page_size=100)
            self.db_schema = self._load_schema()
        except Exception as e:
            logging.error(f"❌ Engine 초기화 실패: {e}")
            raise

    def _load_schema(self) -> List[str]:
        """[Internal] 테이블 스키마 로드"""
        try:
            table = self.client.get_table(self.table_ref)
            return [schema.name for schema in table.schema if schema.name not in ['created_at']]
        except Exception as e:
            logging.error(f"❌ 스키마 로드 실패: {e}")
            return []

    def get_current_db_status(self, division: str) -> pd.DataFrame:
        query = text(f"SELECT subsidiary, week, COUNT(*) as row_count FROM `{self.table_ref}` WHERE division_code = :div GROUP BY subsidiary, week")
        with self.db_engine.connect() as conn:
            return pd.read_sql(query, conn, params={"div": division})

    def get_full_master(self, division: str) -> pd.DataFrame:
        query = text(f"SELECT * FROM `{self.table_ref}` WHERE division_code = :div ORDER BY date DESC, subsidiary ASC")
        with self.db_engine.connect() as conn:
            return pd.read_sql(query, conn, params={"div": division})

    # --- [Architect Fix] 클래스 내부 메서드로 이동 ---
    def get_report_logic(self, division: str) -> pd.DataFrame:
        """[Backend Logic] DB 데이터를 Config 스펙에 맞춘 리포트용 DF로 변환"""
        df_raw = self.get_full_master(division)
        
        if df_raw is None or df_raw.empty:
            cols = config.MX_OUTPUT_COLS if division == "MX" else config.CE_OUTPUT_COLS
            return pd.DataFrame(columns=cols)

        mapping = {
            'subsidiary': 'Subsidiary', 'sales_channel': 'Sales Channel', 'partner': 'Partner',
            'media_type_1': 'Media Type 1', 'media_type_2': 'Media Type 2', 'media_type_2_raw': 'Media Type 2 (Raw)',
            'media_platform': 'Media Platform', 'media_platform_raw': 'Media Platform (Raw)',
            'funding': 'Funding', 'bu': 'BU', 'product_category': 'Product Category',
            'product_series': 'Product Series', 'products': 'Products', 'campaign_name': 'Campaign Name',
            'mindset': 'Mindset', 'quarter': 'Quarter', 'month': 'Month', 'week': 'Week', 'date': 'Date',
            'media_spend_usd': 'Media Spend (USD)', 'impressions': 'Impressions', 'clicks': 'Clicks',
            'cpc': 'CPC', 'orders': 'Orders', 'revenue': 'Revenue', 'app_install': 'App Install'
        }

        df_mapped = df_raw.rename(columns=mapping)
        if division == "CE" and "Products" in df_mapped.columns:
            df_mapped = df_mapped.rename(columns={"Products": "Products (Optional)"})
        
        output_schema = config.MX_OUTPUT_COLS if division == "MX" else config.CE_OUTPUT_COLS
        final_cols = [c for c in output_schema if c in df_mapped.columns]
        return df_mapped[final_cols]

    def run_sync_logic(self, df: pd.DataFrame, division: str) -> int:
        """[Core] 고유 시점 탐색 -> 부분 삭제 -> 고속 적재"""
        # ... (기존 sync 로직 동일)
        return len(df) # 예시 반환

# =========================================================
# [Interface Functions] app.py에서 직접 호출하는 통로
# =========================================================

_engine_instance = None

def get_engine():
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = DataIntegrityEngine()
    return _engine_instance

def render_dashboard_ui(division: str):
    status_df = get_engine().get_current_db_status(division)
    if not status_df.empty:
        pivot = status_df.pivot(index='subsidiary', columns='week', values='row_count').fillna(0)
        fig = go.Figure(data=go.Heatmap(z=pivot.values, x=pivot.columns, y=pivot.index, colorscale='YlGnBu'))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("💡 적재된 데이터가 없습니다.")

def get_master_extract(division: str) -> pd.DataFrame:
    return get_engine().get_full_master(division)

def sync_to_bigquery(file_obj, division: str) -> int:
    df = pd.read_csv(file_obj)
    return get_engine().run_sync_logic(df, division)

# --- [Architect Fix] app.py에서 에러 없이 호출되도록 인터페이스 함수 정의 ---
def get_report_df(division: str) -> pd.DataFrame:
    """app.py 95라인의 호출을 받아 엔진 인스턴스의 로직을 실행"""
    return get_engine().get_report_logic(division)