# src/utils.py
import pandas as pd
import numpy as np
import logging
import re
from pathlib import Path
from typing import Union, Optional

def load_csv_safely(file_path: Union[str, Path], header=0) -> Optional[pd.DataFrame]:
    """
    CSV 안전 로드 (권한 에러 방어, Path 객체 지원, 인코딩 자동 탐지)
    """
    path_obj = Path(file_path) # 타입 정규화
    
    if not path_obj.exists():
        logging.error(f"❌ 파일이 존재하지 않습니다: {path_obj}")
        return None

    encodings = ['utf-8-sig', 'cp949']
    
    for enc in encodings:
        try:
            # low_memory=False: 타입 추론 정확도 우선
            df = pd.read_csv(path_obj, encoding=enc, header=header, low_memory=False)
            
            # 불필요한 시스템 컬럼 제거
            cols = [c for c in df.columns if not str(c).startswith('Unnamed:') and not str(c).startswith('Colonne')]
            return df[cols].copy()
            
        except UnicodeDecodeError:
            continue
            
        except PermissionError:
            logging.error(f"⛔ 권한 거부: '{path_obj.name}' 파일이 엑셀에서 열려있지 않은지 확인해주세요.")
            return None
            
        except Exception as e:
            logging.error(f"CSV 로드 실패 ({path_obj.name}): {e}")
            return None
            
    logging.error(f"❌ 모든 인코딩 시도 실패: {path_obj.name}")
    return None

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