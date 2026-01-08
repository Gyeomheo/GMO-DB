import pandas as pd
import numpy as np
import logging
import google.auth
from datetime import datetime, timedelta
from typing import List, Optional
from google.cloud import bigquery
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
import streamlit as st
from . import config  # 프로젝트 공통 설정 참조

# 로그 설정
logging.basicConfig(level=logging.INFO)

class DataIntegrityEngine:
    def __init__(self):
        """[Architecture] GCP 인프라 연결 및 SQLAlchemy 엔진 초기화"""
        try:
            self.credentials, _ = google.auth.default()
            self.project = config.BQ_PROJECT
            logging.info(f"🔗 Connecting to GCP Project: {self.project}")

            self.client = bigquery.Client(credentials=self.credentials, project=self.project)
            self.table_ref = f"{self.project}.{config.BQ_DATASET}.{config.BQ_TABLE}"
            
            db_url = f"bigquery://{self.project}/{config.BQ_DATASET}"
            self.db_engine = create_engine(db_url, list_tables_page_size=100)
        except Exception as e:
            logging.error(f"❌ Engine 초기화 실패: {e}")
            raise

    def validate_data(self, df: pd.DataFrame, division: str) -> pd.DataFrame:
        """[Gate-keeping] 하드코딩 국가코드 및 규격 전수 검증"""
        error_logs = []
        MX_SUBS = ['SEA', 'SEAU', 'SEDA', 'SEF', 'SEG', 'SEUK', 'SGE', 'SIEL', 'SAVINA', 'SCIC', 'SEAD_CRO', 'SEAD_SLO', 'SEASA', 'SEBN_BE', 'SEBN_NL', 'SEC', 'SECA', 'SECH', 'SECZ_CZ', 'SECZ_SK', 'SEH', 'SEHK', 'SEI', 'SEIB_ES', 'SEIB_PT', 'SEIN', 'SEJ', 'SEM', 'SENA', 'SENZ', 'SEPCO', 'SEPOL', 'SEPR', 'SEROM', 'SESAR', 'SESP', 'SET', 'SETK', 'SME', 'TSE']
        CE_SUBS = ['SEA', 'SEAU', 'SEDA', 'SEF', 'SEG', 'SEUK', 'SGE', 'SIEL', 'SAVINA', 'SCIC', 'SEASA', 'SEBN_BE', 'SEBN_NL', 'SECA', 'SECH', 'SECZ_CZ', 'SECZ_SK', 'SEH', 'SEHK', 'SEI', 'SEIB_PT', 'SEIN', 'SEM', 'SENZ', 'SEPOL', 'SEPR', 'SEROM', 'SESAR', 'SESP', 'SETK', 'SME', 'TSE']
        
        target_subs = MX_SUBS if division == 'MX' else CE_SUBS

        if not df['Subsidiary'].isin(target_subs).all():
            for idx, row in df[~df['Subsidiary'].isin(target_subs)].iterrows():
                error_logs.append({'Row': idx + 2, 'Column': 'Subsidiary', 'Value': row['Subsidiary'], 'Error': '미등록 국가코드'})

        valid_fundings = ["GMO", "Local"]
        if not df['Funding'].isin(valid_fundings).all():
            for idx, row in df[~df['Funding'].isin(valid_fundings)].iterrows():
                error_logs.append({'Row': idx + 2, 'Column': 'Funding', 'Value': row['Funding'], 'Error': 'GMO/Local 외 기준외 값'})

        for col in config.MEDIA_COLS:
            if col not in df.columns:
                error_logs.append({'Row': 'Global', 'Column': col, 'Error': f'필수 컬럼({col}) 누락'})
            elif df[col].isna().any():
                for idx, val in df[df[col].isna()].iterrows():
                    error_logs.append({'Row': idx + 2, 'Column': col, 'Value': 'NaN', 'Error': '매핑 누락'})

        valid_cats = config.DIV_RULES['MX'] if division == 'MX' else (config.DIV_RULES['VD'] + config.DIV_RULES['DA'])
        if not df['Product Category'].isin(valid_cats).all():
            for idx, row in df[~df['Product Category'].isin(valid_cats)].iterrows():
                error_logs.append({'Row': idx + 2, 'Column': 'Product Category', 'Value': row['Product Category'], 'Error': '기준외 카테고리'})

        return pd.DataFrame(error_logs)

    def get_smart_refresh_point(self, df_m: pd.DataFrame, df_n: pd.DataFrame) -> datetime:
        """[Efficiency] 수치 비교를 통한 가변 리프레시 시점 탐색"""
        check_cols = ['Media Spend (USD)', 'Revenue', 'Impressions', 'Clicks', 'Orders']
        m_week = df_m.groupby('Week')[check_cols].sum().sort_index()
        n_week = df_n.groupby('Week')[check_cols].sum().sort_index()
        
        for week in n_week.index:
            if week not in m_week.index:
                return pd.to_datetime(df_n[df_n['Week'] == week]['Date']).min()
            
            if (m_week.loc[week] - n_week.loc[week]).abs().max() >= 0.01:
                sub_m, sub_n = df_m[df_m['Week'] == week], df_n[df_n['Week'] == week]
                m_dates = set(pd.to_datetime(sub_m['Date']).dt.date)
                n_dates = set(pd.to_datetime(sub_n['Date']).dt.date)
                
                if m_dates == n_dates:
                    return pd.to_datetime(min(m_dates))
                else:
                    all_days = sorted(list(n_dates | m_dates))
                    for d in all_days:
                        if d not in m_dates or d not in n_dates: return pd.to_datetime(d)
                        m_day = sub_m[pd.to_datetime(sub_m['Date']).dt.date == d][check_cols].sum()
                        n_day = sub_n[pd.to_datetime(sub_n['Date']).dt.date == d][check_cols].sum()
                        if (m_day - n_day).abs().max() >= 0.01: return pd.to_datetime(d)
        return pd.to_datetime(df_m['Date']).max() + timedelta(days=1)

    def run_sync_logic(self, df: pd.DataFrame, division: str) -> int:
        """[Cost-Optimized Sync] 최소한의 데이터만 읽어 정합성 싱크"""
        try:
            errors_df = self.validate_data(df, division)
            if not errors_df.empty: raise ValueError("Validation Failed", errors_df)

            # [비용절감 1] 필요한 컬럼만 명시적으로 쿼리 (SELECT * 제거)
            fetch_cols = "subsidiary, date, week, media_spend_usd, revenue, impressions, clicks, orders"
            subs = df['Subsidiary'].unique().tolist()
            sub_filter = ", ".join([f"'{s}'" for s in subs])
            
            query = text(f"SELECT {fetch_cols} FROM `{self.table_ref}` WHERE division_code = :div AND subsidiary IN ({sub_filter})")
            with self.db_engine.connect() as conn:
                df_db_raw = pd.read_sql(query, conn, params={"div": division})

            if df_db_raw.empty:
                refresh_point = pd.to_datetime(df['Date']).min()
            else:
                df_db = df_db_raw.rename(columns={'subsidiary': 'Subsidiary', 'date': 'Date', 'week': 'Week', 'media_spend_usd': 'Media Spend (USD)', 'revenue': 'Revenue', 'impressions': 'Impressions', 'clicks': 'Clicks', 'orders': 'Orders'})
                refresh_point = self.get_smart_refresh_point(df_db, df)

            if refresh_point > pd.to_datetime(df['Date']).max(): return 0

            # [Atomic Refresh]
            with self.db_engine.connect() as conn:
                conn.execute(text(f"DELETE FROM `{self.table_ref}` WHERE division_code = :div AND subsidiary IN ({sub_filter}) AND date >= :start"), {"div": division, "start": refresh_point.strftime('%Y-%m-%d')})
                conn.commit()
                df_load = df[pd.to_datetime(df['Date']) >= refresh_point].copy()
                df_load['created_at'] = datetime.now()
                df_load['division_code'] = division
                df_load.to_sql(name=config.BQ_TABLE, con=self.db_engine, if_exists='append', index=False)
            return len(df_load)
        except ValueError as ve: raise ve
        except Exception as e:
            logging.error(f"❌ Sync Error: {e}")
            raise

# --- Interface Functions ---
_engine_instance = None
def get_engine():
    global _engine_instance
    if _engine_instance is None: _engine_instance = DataIntegrityEngine()
    return _engine_instance

def sync_to_bigquery(file_obj, division: str) -> int:
    df = pd.read_csv(file_obj)
    return get_engine().run_sync_logic(df, division)

def render_dashboard_ui(division: str):
    # [비용절감 2] 통계용 데이터만 집계해서 로드
    query = text(f"SELECT subsidiary, week, COUNT(*) as row_count FROM `{get_engine().table_ref}` WHERE division_code = :div GROUP BY 1, 2")
    with get_engine().db_engine.connect() as conn:
        status_df = pd.read_sql(query, conn, params={"div": division})
    if not status_df.empty:
        pivot = status_df.pivot(index='subsidiary', columns='week', values='row_count').fillna(0)
        st.plotly_chart(go.Figure(data=go.Heatmap(z=pivot.values, x=pivot.columns, y=pivot.index, colorscale='YlGnBu')), use_container_width=True)
    else: st.info("💡 데이터가 없습니다.")

def get_report_df(division: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """[Cost-Optimized Report] 필수 컬럼만 선택하여 쿼리량 최소화"""
    engine_inst = get_engine()
    output_schema = config.MX_OUTPUT_COLS if division == "MX" else config.CE_OUTPUT_COLS
    
    # [비용절감 3] 출력 스키마에 정의된 컬럼 이름만 SQL 친화적으로 변환하여 SELECT 절 구성
    mapping_rev = {'Subsidiary': 'subsidiary', 'Date': 'date', 'Week': 'week', 'Media Spend (USD)': 'media_spend_usd', 'Revenue': 'revenue', 'Impressions': 'impressions', 'Clicks': 'clicks', 'Orders': 'orders', 'Funding': 'funding', 'BU': 'bu', 'Product Category': 'product_category'}
    select_items = []
    for col in output_schema:
        clean_col = mapping_rev.get(col, col.lower().replace(" ", "_").replace("(", "").replace(")", ""))
        select_items.append(f"{clean_col}")
    
    # 중복 제거 및 콤마 결합
    select_clause = ", ".join(list(dict.fromkeys(select_items)))
    
    query = text(f"SELECT {select_clause} FROM `{engine_inst.table_ref}` WHERE division_code = :div AND date >= :start AND date <= :end ORDER BY date DESC")
    with engine_inst.db_engine.connect() as conn:
        df_raw = pd.read_sql(query, conn, params={"div": division, "start": start_date.strftime('%Y-%m-%d'), "end": end_date.strftime('%Y-%m-%d')})
    
    if df_raw.empty: return pd.DataFrame(columns=output_schema)
    
    # DB 컬럼명을 다시 UI용으로 매핑
    mapping_ui = {v: k for k, v in mapping_rev.items()}
    df_mapped = df_raw.rename(columns=mapping_ui)
    if division == "CE" and "Products" in df_mapped.columns:
        df_mapped = df_mapped.rename(columns={"Products": "Products (Optional)"})
    
    return df_mapped[[c for c in output_schema if c in df_mapped.columns]]