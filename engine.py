import pandas as pd
import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional

import google.auth
from google.cloud import bigquery
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# [Configuration] 전역 설정 및 상수
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "gmo-weekly")
DATASET_ID = os.getenv("BQ_DATASET_ID", "marketing_dw")
TABLE_ID = "global_performance"

logging.basicConfig(level=logging.INFO)

class DataIntegrityEngine:
    def __init__(self):
        """
        [Architecture] 인프라 연결 초기화
        - Native Client: 대량 적재(Load Job) 및 스키마 관리
        - SQLAlchemy: 보안 쿼리, 트랜잭션 및 유연한 데이터 조회
        """
        # 1. Google Auth 및 Project ID 확정
        self.credentials, _ = google.auth.default()
        self.project = PROJECT_ID
        logging.info(f"🔗 Connecting to GCP Project: {self.project}")

        # 2. BigQuery Native Client 초기화
        self.client = bigquery.Client(
            credentials=self.credentials, 
            project=self.project
        )
        self.table_ref = f"{self.project}.{DATASET_ID}.{TABLE_ID}"
        
        # 3. SQLAlchemy Engine 초기화 (BigQuery Dialect 사용)
        db_url = f"bigquery://{self.project}/{DATASET_ID}"
        self.db_engine = create_engine(
            db_url,
            credentials_path=None,
            list_tables_page_size=100
        )

        # 4. 초기 구동 시 스키마 캐싱
        self.db_schema = self._load_schema()

    def _load_schema(self) -> List[str]:
        """[Internal] 테이블 스키마 안전하게 로드 (시스템 컬럼 제외)"""
        try:
            table = self.client.get_table(self.table_ref)
            # 'created_at'과 같은 자동 생성 컬럼은 데이터 적재 대상에서 제외
            return [schema.name for schema in table.schema if schema.name not in ['created_at']]
        except Exception as e:
            logging.error(f"❌ 테이블 접속 실패 (초기화 중): {e}")
            return []

    def get_current_db_status(self, division: str) -> pd.DataFrame:
        """
        [Heatmap Logic] BigQuery에서 현재 사업부의 법인별/주차별 데이터 적재 현황 조회
        """
        query = text(f"""
            SELECT subsidiary, week, COUNT(*) as row_count
            FROM `{self.table_ref}`
            WHERE division_code = :div
            GROUP BY subsidiary, week
        """)
        try:
            with self.db_engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"div": division})
                logging.info(f"📊 현황 조회 완료: {division} 사업부")
                return df
        except Exception as e:
            logging.error(f"❌ 현황 조회 중 오류 발생: {e}")
            return pd.DataFrame(columns=['subsidiary', 'week', 'row_count'])

    def process_file(self, file_object, file_name: str) -> pd.DataFrame:
        """[API] 파일 파싱 및 전처리 (Robust Parsing)"""
        try:
            if file_name.lower().endswith(('.csv', '.txt')):
                try:
                    df = pd.read_csv(file_object, encoding='utf-8')
                except UnicodeDecodeError:
                    file_object.seek(0)
                    df = pd.read_csv(file_object, encoding='cp949')
            else:
                df = pd.read_excel(file_object)
        except Exception as e:
            raise ValueError(f"파일 포맷 오류: {e}")

        # 컬럼명 정규화 (공백 제거 및 소문자화)
        df.columns = df.columns.str.strip().str.lower()
        col_map = {c: c for c in df.columns}
        
        # 필수 컬럼 검증
        sub_key = next((k for k in col_map.keys() if 'subsidiary' in k), None)
        date_key = next((k for k in col_map.keys() if 'date' in k), None)

        if not sub_key or not date_key:
            raise ValueError("필수 컬럼(Subsidiary, Date) 누락")

        try:
            df['date'] = pd.to_datetime(df[date_key]).dt.normalize()
            df = df.rename(columns={sub_key: 'subsidiary'})
        except Exception as e:
            raise ValueError(f"날짜 변환 오류: {e}")

        return df

    def _normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """[Internal] 데이터프레임을 DB 스키마 구조로 강제 변환 및 결측치 처리"""
        # 컬럼 매핑 전략 (현업 양식 -> DB 컬럼명)
        rename_map = {
            'media spend\n(usd)': 'media_spend_usd', 'media spend (usd)': 'media_spend_usd',
            '" media spend \n (usd) "': 'media_spend_usd', 'products (optional)': 'products',
            'bu': 'bu', 'app install': 'app_install', 'revenue': 'revenue', 
            'orders': 'orders', 'impressions': 'impressions', 'clicks': 'clicks', 'cpc': 'cpc'
        }
        df = df.rename(columns=rename_map)
        df.columns = [c.lower().replace(' ', '_').replace('(', '').replace(')', '') for c in df.columns]

        # 스키마 불일치 대응: 없는 컬럼은 0 또는 None으로 채움
        metrics = {'media_spend_usd', 'impressions', 'clicks', 'orders', 'revenue', 'cpc', 'app_install'}
        for col in self.db_schema:
            if col not in df.columns:
                df[col] = 0.0 if col in metrics else None
                    
        return df[self.db_schema]

    def run_sync_logic(self, df: pd.DataFrame, division: str, subsidiary: str) -> int:
        """[Core] 데이터 비교 -> 부분 삭제 -> 고속 적재 (Idempotency 보장)"""
        # 1. DB 기존 요약 가져오기
        check_query = text(f"""
            SELECT date, 
                   SUM(media_spend_usd) as db_spend, 
                   SUM(revenue) as db_rev
            FROM `{self.table_ref}`
            WHERE division_code = :div AND subsidiary = :sub
            GROUP BY date ORDER BY date
        """)
        
        with self.db_engine.connect() as conn:
            db_sum = pd.read_sql(check_query, conn, params={"div": division, "sub": subsidiary})

        # 2. 엡실론(0.01) 기반 정밀도 비교 (Floating Point Trap 방지)
        metrics = ['media_spend_usd', 'revenue']
        up_agg = df.groupby('date')[metrics].sum().sort_index()
        d_min = df['date'].min()

        if not db_sum.empty:
            db_sum['date'] = pd.to_datetime(db_sum['date'])
            db_agg = db_sum.set_index('date')
            common = up_agg.index.intersection(db_agg.index)
            
            if not common.empty:
                # 0.01달러 이상의 오차가 있는 시점을 찾음
                diff_mask = (up_agg.loc[common, metrics] - db_agg.loc[common, ['db_spend', 'db_rev']].values).abs().max(axis=1) > 0.01
                if diff_mask.any():
                    d_min = diff_mask.idxmax()
                else:
                    d_min = db_agg.index.max() + timedelta(days=1)

        # 3. 적재 대상 필터링 및 스키마 정규화
        target = df[df['date'] >= d_min].copy()
        if target.empty: return 0

        target['division_code'] = division
        final_target = self._normalize_schema(target)

        # 4. 트랜잭션 기반 삭제 (기존 데이터 클린업)
        delete_sql = text(f"""
            DELETE FROM `{self.table_ref}` 
            WHERE division_code = :div AND subsidiary = :sub AND date >= :target_date
        """)
        
        try:
            with self.db_engine.begin() as conn:
                conn.execute(delete_sql, {
                    "div": division, "sub": subsidiary, "target_date": d_min.date()
                })
            
            # 5. Native Load Job을 이용한 고속 적재 (성능 최적화)
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = self.client.load_table_from_dataframe(final_target, self.table_ref, job_config=job_config)
            job.result() 
            
            logging.info(f"✅ Synced {len(final_target)} rows for {subsidiary} (Start: {d_min.date()})")
            return len(final_target)
        except Exception as e:
            logging.error(f"❌ Sync Failed: {e}")
            raise

    def get_export_data(self, division: str) -> pd.DataFrame:
        """[API] 사업부(MX/CE)별 맞춤형 양식 추출 및 비용 최적화 조회"""
        base_cols = [
            'subsidiary', 'sales_channel', 'partner', 'media_type_1', 'media_type_2', 
            'media_platform', 'funding', 'product_category', 'product_series', 
            'campaign_name', 'mindset', 'quarter', 'month', 'week', 'date'
        ]
        
        # 사업부별 전략적 매핑
        if division == 'MX':
            spec_map = {
                'products': 'Products', 'media_spend_usd': 'Media Spend (USD)', 
                'impressions': 'Impressions', 'clicks': 'Clicks', 'cpc': 'CPC', 
                'orders': 'Orders', 'revenue': 'Revenue', 'app_install': 'App Install'
            }
        else: # CE
            spec_map = {
                'bu': 'BU', 'products': 'Products (Optional)',
                'media_spend_usd': '" Media Spend \n (USD) "',
                'impressions': ' Impressions ', 'clicks': ' Clicks ', 'cpc': ' CPC ', 
                'orders': ' Orders ', 'revenue': ' Revenue ', 'app_install': ' App Install '
            }

        # 비용 최적화: 필요한 컬럼만 SELECT 생성
        target_columns = base_cols + list(spec_map.keys())
        valid_columns = [f"`{c}`" for c in target_columns if c in self.db_schema]
        cols_str = ", ".join(valid_columns)

        query = text(f"SELECT {cols_str} FROM `{self.table_ref}` WHERE division_code = :div ORDER BY date DESC, subsidiary ASC")
        
        with self.db_engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"div": division})

        # 비즈니스 명칭 변경
        business_map = {
            'subsidiary': 'Subsidiary', 'sales_channel': 'Sales Channel', 'partner': 'Partner',
            'media_type_1': 'Media Type 1', 'media_type_2': 'Media Type 2', 
            'media_platform': 'Media Platform', 'funding': 'Funding',
            'product_category': 'Product Category', 'product_series': 'Product Series', 
            'campaign_name': 'Campaign Name', 'mindset': 'Mindset', 'quarter': 'Quarter', 
            'month': 'Month', 'week': 'Week', 'date': 'Date', **spec_map
        }
        
        final_df = df.rename(columns=business_map)
        # 정의된 순서대로 컬럼 정렬
        ordered_cols = [business_map[c] for c in target_columns if business_map.get(c) in final_df.columns]
        return final_df[ordered_cols]