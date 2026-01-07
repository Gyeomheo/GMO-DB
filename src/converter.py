# src/converter.py
"""
'엑셀 마스터'를 'JSON 캐시'로 변환하는 로직 (v-Final)
"""
import pandas as pd
import json
import logging
from pathlib import Path

def convert_excel_to_json(excel_path: Path, json_path: Path, key_col: str, std_cols: list) -> bool:
    try:
        df = pd.read_excel(excel_path)
    except FileNotFoundError:
        logging.error(f"FATAL: 마스터 엑셀 파일을 찾을 수 없습니다: {excel_path}")
        return False
    except Exception as e:
        logging.error(f"엑셀 파일 읽기 오류: {e}")
        return False

    # '공란'을 ''로 통일 (필수)
    df = df.fillna('')
    logging.info(f"'{key_col}' 기준으로 JSON 구조 생성 중...")
    
    grouped = df.groupby(key_col)[std_cols]
    mapping_dict = {key: group.values.tolist() for key, group in grouped}

    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_dict, f, ensure_ascii=False) # indent 제거 (성능)
        logging.info(f"✅ JSON 캐시 생성 성공: {json_path.name}")
        return True
    except Exception as e:
        logging.error(f"JSON 캐시 저장 오류: {e}")
        return False

def load_map_from_json(json_path: Path, map_cols: dict) -> pd.DataFrame:
    """
    'JSON 캐시'를 'merge'용 DataFrame(df_map)으로 로드합니다.
    """
    logging.info(f"'{json_path.name}' 캐시 파일 로드 중...")
    with open(json_path, 'r', encoding='utf-8') as f:
        mapping_dict = json.load(f)

    map_records = []
    key_col_name = map_cols['key']
    std_col_names = map_cols['std_cols']
    
    for key_value, candidates in mapping_dict.items():
        for cand_list in candidates:
            map_records.append([key_value] + cand_list)
    
    df_map = pd.DataFrame(map_records, columns=[key_col_name] + std_col_names)
    logging.info(f"'{json_path.name}' 로드 완료. (총 {len(df_map)}개 규칙)")
    return df_map