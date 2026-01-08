# src/utils.py
import pandas as pd
import numpy as np
import logging
import re
from pathlib import Path
from typing import Union, Optional

def load_csv_safely(file_obj):
    """
    [Fix] Streamlit UploadedFile 객체와 로컬 경로 모두 대응하도록 수정
    """
    try:
        # 1. 파일 이름 추출 (UploadedFile 혹은 문자열 경로 대응)
        file_name = getattr(file_obj, 'name', str(file_obj))
        ext = file_name.split('.')[-1].lower()

        # 2. 파일 스트림의 위치를 처음으로 리셋 (재읽기 대비)
        if hasattr(file_obj, 'seek'):
            file_obj.seek(0)

        # 3. 확장자에 따른 로드 (경로가 아닌 파일 객체 자체를 전달)
        if ext in ['xlsx', 'xls']:
            return pd.read_excel(file_obj, engine='openpyxl')
        else:
            # CSV 로드 (encoding 에러 방지를 위해 utf-8-sig 권장)
            return pd.read_csv(file_obj, encoding='utf-8-sig')

    except Exception as e:
        logging.error(f"❌ 파일 로드 실패: {e}")
        raise ValueError(f"파일을 읽을 수 없습니다: {str(e)}")

def process_and_filter_dates(df: pd.DataFrame, date_col: str = 'Date') -> pd.DataFrame:
    """
    날짜 초고속 파싱 및 필터링 (Vectorized)
    """
    if date_col not in df.columns:
        logging.warning(f"  -> [주의] '{date_col}' 컬럼이 없어 날짜 처리를 건너뜁니다.")
        return df

    logging.info(f"  -> (v25) '{date_col}' 파싱 및 필터링...")

    # 1. 숫자형 변환 시도 (Excel Serial Date)
    raw_dates = df[date_col].astype(str).str.strip()
    temp_num = pd.to_numeric(raw_dates, errors='coerce')
    mask_num = temp_num.notna()
    df['__date_obj'] = pd.NaT 
    
    if mask_num.any():
        valid_nums = (temp_num > 0) & (temp_num < 73050) 
        mask_valid_num = mask_num & valid_nums
        df.loc[mask_valid_num, '__date_obj'] = pd.to_datetime(
            temp_num[mask_valid_num], unit='D', origin='1899-12-30'
        )

    # 2. 텍스트형 변환 시도
    mask_text = ~mask_num
    if mask_text.any():
        df.loc[mask_text, '__date_obj'] = pd.to_datetime(
            df.loc[mask_text, date_col], errors='coerce', dayfirst=False
        )
        
        # 3. 실패 건에 대한 정규식 정리 후 재시도
        mask_fail = mask_text & df['__date_obj'].isna()
        if mask_fail.any():
            # 여기는 실패한 소수 데이터만 처리하므로 apply 허용 (Vectorization 복잡도 대비 이득)
            def clean_date_str(s):
                if pd.isna(s) or s == 'nan': return np.nan
                s_clean = re.sub(r'[^0-9]', '-', s)
                s_clean = re.sub(r'-+', '-', s_clean).strip('-')
                return s_clean

            cleaned_dates = df.loc[mask_fail, date_col].astype(str).apply(clean_date_str)
            df.loc[mask_fail, '__date_obj'] = pd.to_datetime(
                cleaned_dates, errors='coerce', format='mixed' 
            )

    # 4. 필터링 및 컬럼 생성
    target_date = pd.Timestamp('2025-01-01')
    mask_keep = (df['__date_obj'].notna()) & (df['__date_obj'] >= target_date)
    
    dropped_count = len(df) - mask_keep.sum()
    if dropped_count > 0:
        df = df[mask_keep].copy()

    if df.empty:
        return df

    df[date_col] = df['__date_obj'].dt.strftime('%Y-%m-%d')
    
    if 'Week' in df.columns:
        df['Week'] = df['__date_obj'].dt.isocalendar().week.astype(int)
    if 'Month' in df.columns:
        df['Month'] = df['__date_obj'].dt.strftime('%B')
    if 'Quarter' in df.columns:
        df['Quarter'] = "Q" + df['__date_obj'].dt.quarter.astype(str)

    df.drop(columns=['__date_obj'], inplace=True, errors='ignore')
    return df