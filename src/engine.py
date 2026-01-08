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
            
            # BigQuery용 SQLAlchemy Dialect 엔진 생성
            db_url = f"bigquery://{self.project}/{config.BQ_DATASET}"
            self.db_engine = create_engine(db_url, list_tables_page_size=100)
        except Exception as e:
            logging.error(f"❌ Engine 초기화 실패: {e}")
            raise

    def validate_data(self, df: pd.DataFrame, division: str) -> pd.DataFrame:
        """
        [Gate-keeping] 하드코딩 국가코드 및 config 기준 전수 유효성 검증
        - 하나라도 어긋나면 취합하지 않고 에러 리포트 반환
        """
        error_logs = []

        # 1. Subsidiary 하드코딩 리스트 (사용자 제공)
        MX_SUBS = [
            'SEA', 'SEAU', 'SEDA', 'SEF', 'SEG', 'SEUK', 'SGE', 'SIEL', 'SAVINA', 
            'SCIC', 'SEAD_CRO', 'SEAD_SLO', 'SEASA', 'SEBN_BE', 'SEBN_NL', 'SEC', 
            'SECA', 'SECH', 'SECZ_CZ', 'SECZ_SK', 'SEH', 'SEHK', 'SEI', 'SEIB_ES', 
            'SEIB_PT', 'SEIN', 'SEJ', 'SEM', 'SENA', 'SENZ', 'SEPCO', 'SEPOL', 
            'SEPR', 'SEROM', 'SESAR', 'SESP', 'SET', 'SETK', 'SME', 'TSE'
        ]
        CE_SUBS = [
            'SEA', 'SEAU', 'SEDA', 'SEF', 'SEG', 'SEUK', 'SGE', 'SIEL', 'SAVINA', 
            'SCIC', 'SEASA', 'SEBN_BE', 'SEBN_NL', 'SECA', 'SECH', 'SECZ_CZ', 
            'SECZ_SK', 'SEH', 'SEHK', 'SEI', 'SEIB_PT', 'SEIN', 'SEM', 'SENZ', 
            'SEPOL', 'SEPR', 'SEROM', 'SESAR', 'SESP', 'SETK', 'SME', 'TSE'
        ]
        
        target_subs = MX_SUBS if division == 'MX' else CE_SUBS

        # A. Subsidiary 검증
        invalid_sub_mask = ~df['Subsidiary'].isin(target_subs)
        if invalid_sub_mask.any():
            for idx, row in df[invalid_sub_mask].iterrows():
                error_logs.append({'Row': idx + 2, 'Column': 'Subsidiary', 'Value': row['Subsidiary'], 'Error': '미등록 국가코드'})

        # B. Funding 검증 (GMO, Local 고정)
        valid_fundings = ["GMO", "Local"]
        invalid_funding_mask = ~df['Funding'].isin(valid_fundings)
        if invalid_funding_mask.any():
            for idx, row in df[invalid_funding_mask].iterrows():
                error_logs.append({'Row': idx + 2, 'Column': 'Funding', 'Value': row['Funding'], 'Error': 'GMO/Local 외 기준외 값'})

        # C. Media Standard 검증
        for col in config.MEDIA_COLS:
            if col not in df.columns:
                error_logs.append({'Row': 'Global', 'Column': col, 'Error': f'필수 미디어 컬럼({col}) 누락'})
            else:
                if df[col].isna().any():
                    for idx, val in df[df[col].isna()].iterrows():
                        error_logs.append({'Row': idx + 2, 'Column': col, 'Value': 'NaN', 'Error': '미디어 매핑값 누락'})

        # D. Product Category & 본부 룰 검증 (DIV_RULES 참조)
        valid_cats = config.DIV_RULES['MX'] if division == 'MX' else (config.DIV_RULES['VD'] + config.DIV_RULES['DA'])
        invalid_cat_mask = ~df['Product Category'].isin(valid_cats)
        if invalid_cat_mask.any():
            for idx, row in df[invalid_cat_mask].iterrows():
                error_logs.append({'Row': idx + 2, 'Column': 'Product Category', 'Value': row['Product Category'], 'Error': f'{division} 본부 기준외 카테고리'})

        # E. BU 검증 (CE 전용)
        if division == 'CE':
            valid_bus = ["VD", "DA"]
            if 'BU' not in df.columns:
                error_logs.append({'Row': 'Global', 'Column': 'BU', 'Error': 'CE 본부는 BU 컬럼 필수'})
            else:
                invalid_bu_mask = ~df['BU'].isin(valid_bus)
                if invalid_bu_mask.any():
                    for idx, row in df[invalid_bu_mask].iterrows():
                        error_logs.append({'Row': idx + 2, 'Column': 'BU', 'Value': row['BU'], 'Error': 'BU는 VD 또는 DA만 가능'})

        return pd.DataFrame(error_logs)

    def get_smart_refresh_point(self, df_m: pd.DataFrame, df_n: pd.DataFrame) -> datetime:
        """[Precision Sync] 최초 불일치 주차 탐색 및 가변 리프레시 시점 결정"""
        check_cols = ['Media Spend (USD)', 'Revenue', 'Impressions', 'Clicks', 'Orders']
        
        m_week = df_m.groupby('Week')[check_cols].sum().sort_index()
        n_week = df_n.groupby('Week')[check_cols].sum().sort_index()
        
        for week in n_week.index:
            if week not in m_week.index:
                return pd.to_datetime(df_n[df_n['Week'] == week]['Date']).min()
            
            # 1. 주차별 수치 비교 (0.01 오차 허용)
            diff = (m_week.loc[week] - n_week.loc[week]).abs()
            if (diff >= 0.01).any():
                sub_m, sub_n = df_m[df_m['Week'] == week], df_n[df_n['Week'] == week]
                m_dates = set(pd.to_datetime(sub_m['Date']).dt.date)
                n_dates = set(pd.to_datetime(sub_n['Date']).dt.date)
                
                # 2. 주차 내 일자 구성 비교
                if m_dates == n_dates:
                    return pd.to_datetime(min(m_dates))
                else:
                    # 일자 구성 다를 시 일자별 정밀 스캔
                    all_days = sorted(list(n_dates | m_dates))
                    for d in all_days:
                        if d not in m_dates or d not in n_dates: return pd.to_datetime(d)
                        m_day = sub_m[pd.to_datetime(sub_m['Date']).dt.date == d][check_cols].sum()
                        n_day = sub_n[pd.to_datetime(sub_n['Date']).dt.date == d][check_cols].sum()
                        if (m_day - n_day).abs().max() >= 0.01: return pd.to_datetime(d)
                        
        return pd.to_datetime(df_m['Date']).max() + timedelta(days=1)

    def run_sync_logic(self, df: pd.DataFrame, division: str) -> int:
        """[Main Pipeline] 검증 -> 시점 탐색 -> 삭제/적재 트랜잭션"""
        try:
            # Step 1: Standard 정합성 검증
            errors_df = self.validate_data(df, division)
            if not errors_df.empty:
                raise ValueError("Validation Failed", errors_df)

            # Step 2: 비교 데이터 확보
            subs = df['Subsidiary'].unique().tolist()
            sub_filter = ", ".join([f"'{s}'" for s in subs])
            query = text(f"SELECT * FROM `{self.table_ref}` WHERE division_code = :div AND subsidiary IN ({sub_filter})")
            with self.db_engine.connect() as conn:
                df_db_raw = pd.read_sql(query, conn, params={"div": division})

            if df_db_raw.empty:
                refresh_point = pd.to_datetime(df['Date']).min()
            else:
                df_db = df_db_raw.rename(columns={
                    'subsidiary': 'Subsidiary', 'date': 'Date', 'week': 'Week',
                    'media_spend_usd': 'Media Spend (USD)', 'revenue': 'Revenue',
                    'impressions': 'Impressions', 'clicks': 'Clicks', 'orders': 'Orders'
                })
                # Step 3: 스마트 리프레시 포인트 탐색
                refresh_point = self.get_smart_refresh_point(df_db, df)

            if refresh_point > pd.to_datetime(df['Date']).max():
                return 0

            # Step 4: [Atomic] 삭제 후 적재
            with self.db_engine.connect() as conn:
                conn.execute(text(f"""
                    DELETE FROM `{self.table_ref}` 
                    WHERE division_code = :div AND subsidiary IN ({sub_filter}) AND date >= :start
                """), {"div": division, "start": refresh_point.strftime('%Y-%m-%d')})
                conn.commit()

                df_load = df[pd.to_datetime(df['Date']) >= refresh_point].copy()
                df_load['created_at'] = datetime.now()
                df_load['division_code'] = division
                df_load.to_sql(name=config.BQ_TABLE, con=self.db_engine, if_exists='append', index=False)
                
            return len(df_load)

        except ValueError as ve:
            raise ve
        except Exception as e:
            logging.error(f"❌ Smart Sync Failed: {e}")
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
    query = text(f"SELECT subsidiary, week, COUNT(*) as row_count FROM `{get_engine().table_ref}` WHERE division_code = :div GROUP BY subsidiary, week")
    with get_engine().db_engine.connect() as conn:
        status_df = pd.read_sql(query, conn, params={"div": division})
    if not status_df.empty:
        pivot = status_df.pivot(index='subsidiary', columns='week', values='row_count').fillna(0)
        fig = go.Figure(data=go.Heatmap(z=pivot.values, x=pivot.columns, y=pivot.index, colorscale='YlGnBu'))
        st.plotly_chart(fig, use_container_width=True)
    else: st.info("💡 적재된 데이터가 없습니다.")

def get_report_df(self, division: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        query = text(f"SELECT * FROM `{self.table_ref}` WHERE division_code = :div AND date >= :start AND date <= :end ORDER BY date DESC")
        with self.db_engine.connect() as conn:
            df_raw = pd.read_sql(query, conn, params={"div": division, "start": start_date.strftime('%Y-%m-%d'), "end": end_date.strftime('%Y-%m-%d')})
        
        if df_raw.empty: 
            return pd.DataFrame(columns=config.MX_OUTPUT_COLS if division == "MX" else config.CE_OUTPUT_COLS)
        
        # [Fix] 컬럼 매핑 최적화 (CE 본부의 Optional 컬럼명 대응)
        mapping = {
            'subsidiary': 'Subsidiary', 'sales_channel': 'Sales Channel', 'partner': 'Partner',
            'media_type_1': 'Media Type 1', 'media_type_2': 'Media Type 2', 'media_platform': 'Media Platform',
            'funding': 'Funding', 'bu': 'BU', 'product_category': 'Product Category',
            'product_series': 'Product Series', 'products': 'Products', 'campaign_name': 'Campaign Name',
            'mindset': 'Mindset', 'quarter': 'Quarter', 'month': 'Month', 'week': 'Week', 'date': 'Date',
            'media_spend_usd': 'Media Spend (USD)', 'impressions': 'Impressions', 'clicks': 'Clicks', 'orders': 'Orders', 'revenue': 'Revenue'
        }
        
        df_mapped = df_raw.rename(columns=mapping)

        # [Logic 추가] CE 본부 전용 컬럼명 보정 (config 스펙 강제 일치)
        if division == "CE" and "Products" in df_mapped.columns:
            df_mapped = df_mapped.rename(columns={"Products": "Products (Optional)"})
        
        output_schema = config.MX_OUTPUT_COLS if division == "MX" else config.CE_OUTPUT_COLS
        
        # 존재하지 않는 컬럼은 무시하고, 스키마에 정의된 순서대로 추출
        existing_cols = [c for c in output_schema if c in df_mapped.columns]
        return df_mapped[existing_cols]