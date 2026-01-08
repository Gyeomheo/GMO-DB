import os
from pathlib import Path

# =========================================================
# 1. 경로 설정 (Path Configuration)
# =========================================================
# __file__ 기준 절대 경로 추출 (배포 환경 호환성 확보)
BASE_DIR = Path(__file__).resolve().parent.parent

# 런타임 폴더 구조 정의
INPUT_DIR = BASE_DIR / "1_Input"
OUTPUT_DIR = BASE_DIR / "2_Output"
CONFIG_DIR = BASE_DIR / "3_Config"  # 마스터 엑셀 파일들이 위치할 곳
BACKUP_DIR = OUTPUT_DIR / "Backup"

# 폴더 자동 생성 (배포 환경에서 쓰기 권한이 있을 경우)
for folder in [INPUT_DIR, OUTPUT_DIR, CONFIG_DIR, BACKUP_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# =========================================================
# 2. 파일명 설정 (Master Reference Files)
# =========================================================
# 엔진이 검증 시 참조할 마스터 파일들
MEDIA_MAP_EXCEL = CONFIG_DIR / "media_mapping_MASTER.xlsx"
PRODUCT_MX_EXCEL = CONFIG_DIR / "product_mapping_MX.xlsx"
PRODUCT_CE_EXCEL = CONFIG_DIR / "product_mapping_CE.xlsx"

# 캐싱용 JSON 경로
MEDIA_MAP_JSON = CONFIG_DIR / "media_mapping.json"
PRODUCT_MX_JSON = CONFIG_DIR / "product_mapping_MX.json"
PRODUCT_CE_JSON = CONFIG_DIR / "product_mapping_CE.json"

# =========================================================
# 3. BigQuery 인프라 설정 (GCP)
# =========================================================
BQ_PROJECT = "gmo-weekly"      # GCP 프로젝트 ID
BQ_DATASET = "marketing_dw"    # 데이터세트 이름
BQ_TABLE = "global_performance" # 최종 마스터 테이블 이름

# =========================================================
# 4. 검증 및 전처리용 상수 (Magic Strings)
# =========================================================
COL_BU = 'BU'
COL_SUB = 'Subsidiary'
COL_DATE = 'Date'

# 데이터 정제 시 제외할 키워드
IGNORE_KEYWORDS = ['smartphones']

# =========================================================
# 5. 비즈니스 검증 규칙 (DIV_RULES)
# =========================================================
# engine.validate_data에서 본부별 카테고리 적합성을 판단하는 기준
DIV_RULES = {
    "VD": ["Lifestyle TV", "Monitor", "Sound device", "TV"],
    "DA": ["Air Dresser", "Air Purifier", "Cooking", "Dishwasher", "Dryer", 
           "Heating & Cooling", "Refrigerator", "Vacuum", "Washer", "Washer & Dryer"],
    "MX": ["Hearables", "PC", "Smartphones", "Tablets", "Wearables"]
}

# 중복 카테고리 혹은 예외 처리용
AMBIGUOUS_CATS = ["Multi", "APS"]

# =========================================================
# 6. 컬럼 매핑 설정 (Column Mapping)
# =========================================================
# 검증 타겟 컬럼 리스트 (engine.py 필수 참조)
PRODUCT_COLS = ['Product Category', 'Product Series', 'Products']
MEDIA_COLS = ['Media Type 1', 'Media Type 2', 'Media Platform']

# 파이프라인 매핑 규칙
MEDIA_COLS_MAP = {
    'raw_cols': ['Media Type 1', 'Media Type 2', 'Media Platform'],
    'std_cols': ['D_Standard', 'E_Standard', 'F_Standard'],
    'key': 'F_Key',
    'normalize_cols': ['Media Type 1', 'Media Type 2', 'Media Platform'] 
}

PRODUCT_COLS_MAP_MX = {
    'raw_cols': ['Product Category', 'Product Series', 'Products'],
    'std_cols': ['A_Standard', 'B_Standard', 'C_Standard'],
    'key': 'C_Key',
    'normalize_cols': ['Product Category', 'Product Series', 'Products']
}

PRODUCT_COLS_MAP_CE = {
    'raw_cols': ['Product Category', 'Product Series', 'Products'],
    'std_cols': ['A_Standard', 'B_Standard', 'C_Standard'],
    'key': 'A_Key',
    'normalize_cols': ['Product Category', 'Product Series']
}

# =========================================================
# 7. 최종 출력 스키마 (Master DB Schema)
# =========================================================
# [MX] Output Specification
MX_OUTPUT_COLS = [
    'Subsidiary', 'Sales Channel', 'Partner', 
    'Media Type 1', 'Media Type 2', 'Media Type 2 (Raw)', 
    'Media Platform', 'Media Platform (Raw)', 
    'Funding', 
    'Product Category', 'Product Series', 'Products', 
    'Campaign Name', 'Mindset', 
    'Quarter', 'Month', 'Week', 'Date', 
    'Media Spend (USD)', 'Impressions', 'Clicks', 'CPC', 
    'Orders', 'Revenue', 'App Install'
]

# [CE] Output Specification
CE_OUTPUT_COLS = [
    'Subsidiary', 'Sales Channel', 'Partner', 
    'Media Type 1', 'Media Type 2', 'Media Type 2 (Raw)', 
    'Media Platform', 'Media Platform (Raw)', 
    'Funding', 'BU', 
    'Product Category', 'Product Series', 'Products (Optional)', 
    'Campaign Name', 'Mindset', 
    'Quarter', 'Month', 'Week', 'Date', 
    'Media Spend (USD)', 'Impressions', 'Clicks', 'CPC', 
    'Orders', 'Revenue', 'App Install'
]

# =========================================================
# 8. 시스템 설정
# =========================================================
WRITE_ENGINE = 'openpyxl'