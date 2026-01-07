"""
reporting.py
~~~~~~~~~~~~
데이터 처리 결과 리포팅 및 파일 저장 모듈.
[Integration]
1. Index Alignment: 행 삭제(Filter) 시 인덱스 불일치로 인한 ValueError 방어.
2. Wide Context: 연관된 컬럼들(Cleaned, Raw)을 한 줄로 나열하여 문맥 유지.
3. Counts: 집계 컬럼명을 'Counts'로 통일.
"""

import pandas as pd
import os
import logging
from . import config

def create_change_summary(df_raw: pd.DataFrame, df_cleaned: pd.DataFrame, check_cols: list) -> pd.DataFrame:
    """
    [Context Change Summary]
    Raw와 Cleaned의 인덱스를 동기화한 후,
    여러 컬럼의 변경 사항을 가로로 나열하여 빈도수를 집계합니다.
    
    Output Headers: [Col1_cleaned, Col1, Col2_cleaned, Col2, ..., Counts]
    """
    # ---------------------------------------------------------
    # ⭐️ [FIX] 인덱스 동기화 (Alignment)
    # 필터링으로 인해 df_cleaned의 행이 줄어들었을 수 있으므로,
    # 살아남은 행(교집합 인덱스)만 기준으로 비교해야 에러가 안 납니다.
    # ---------------------------------------------------------
    common_indices = df_raw.index.intersection(df_cleaned.index)
    
    if len(common_indices) == 0:
        return pd.DataFrame() # 비교할 공통 행이 없음

    # 교집합 인덱스로 데이터 필터링 및 정렬
    r = df_raw.loc[common_indices].sort_index()
    c = df_cleaned.loc[common_indices].sort_index()
    
    # 2. 비교할 컬럼 쌍(Pair) 구성
    compare_pairs = []
    for col in check_cols:
        clean_name = f"{col}_cleaned"
        # 두 데이터프레임에 모두 컬럼이 존재해야 함
        if col in r.columns and clean_name in c.columns:
            compare_pairs.append((clean_name, col))
            
    if not compare_pairs:
        return pd.DataFrame()

    # 3. 데이터 추출 및 통합 (Wide Format)
    temp_df = pd.DataFrame(index=common_indices)
    has_change_mask = pd.Series(False, index=common_indices)
    group_cols_order = []
    
    for clean_col, raw_col in compare_pairs:
        # 데이터 정제 (공백 제거 및 문자열 변환)
        val_clean = c[clean_col].fillna('').astype(str).str.strip()
        val_raw = r[raw_col].fillna('').astype(str).str.strip()
        
        # DataFrame에 컬럼 추가 (Cleaned 먼저, Raw 나중에)
        temp_df[clean_col] = val_clean
        temp_df[raw_col] = val_raw
        
        group_cols_order.append(clean_col)
        group_cols_order.append(raw_col)
        
        # 변경 여부 마킹 (Context 유지: 하나라도 다르면 변경된 것으로 간주)
        has_change_mask |= (val_clean != val_raw)

    # 4. 변경이 발생한 행만 필터링
    df_changes = temp_df[has_change_mask]
    
    if df_changes.empty:
        return pd.DataFrame()
        
    # 5. [Grouping] 패턴 집계 및 카운트 컬럼명 'Counts' 지정
    summary_df = df_changes.groupby(group_cols_order).size().reset_index(name='Counts')
    
    # 6. 정렬 (빈도수 높은 순)
    summary_df = summary_df.sort_values(by='Counts', ascending=False)
    
    return summary_df

def save_to_csv_separated(data_dict: dict, output_base_path: str):
    """
    [Multi-File Saver]
    딕셔너리에 담긴 여러 DataFrame을 각각 별도의 CSV 파일로 저장합니다.
    """
    # 1. 메인 결과 저장 (Cleaned_Result)
    if "Cleaned_Result" in data_dict:
        df_main = data_dict["Cleaned_Result"]
        if not df_main.empty:
            main_path = f"{output_base_path}.csv"
            df_main.to_csv(main_path, index=False, encoding='utf-8-sig')
    
    # 2. 요약 리포트 저장 (Summary_Prod, Summary_Media 등)
    for key, df in data_dict.items():
        if key == "Cleaned_Result": continue 
        
        if df is not None and not df.empty:
            sub_path = f"{output_base_path}_{key}.csv"
            df.to_csv(sub_path, index=False, encoding='utf-8-sig')
            logging.info(f"    📝 [Report] 변경 요약 저장됨: {os.path.basename(sub_path)} (유형: {len(df)}개)")

def save_unmapped_reports(unmapped_prod_set: set, unmapped_media_set: set):
    """
    [Final Report] 미매핑 키 저장
    """
    report_dir = config.OUTPUT_DIR / "Reports"
    os.makedirs(report_dir, exist_ok=True)
    
    if unmapped_prod_set:
        df_prod = pd.DataFrame(list(unmapped_prod_set), columns=['Unmapped_Product_Key'])
        path_prod = report_dir / "Unmapped_Products.csv"
        df_prod.to_csv(path_prod, index=False, encoding='utf-8-sig')
        logging.warning(f"⚠️  [Alert] 매핑 실패 제품 {len(df_prod)}건 저장됨")
        
    if unmapped_media_set:
        df_media = pd.DataFrame(list(unmapped_media_set), columns=['Unmapped_Media_Key'])
        path_media = report_dir / "Unmapped_Media.csv"
        df_media.to_csv(path_media, index=False, encoding='utf-8-sig')
        logging.warning(f"⚠️  [Alert] 매핑 실패 미디어 {len(df_media)}건 저장됨")