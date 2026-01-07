from pathlib import Path

# =========================================================
# 1. 경로 설정 (Path Configuration)
# =========================================================
BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_DIR = BASE_DIR / "1_Input"
OUTPUT_DIR = BASE_DIR / "2_Output"
CONFIG_DIR = BASE_DIR / "3_Config"
BACKUP_DIR = OUTPUT_DIR / "Backup"

# 폴더 자동 생성
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

# =========================================================
# 2. 파일명 설정 (Excel Files)
# =========================================================
MEDIA_MAP_EXCEL = CONFIG_DIR / "media_mapping_MASTER.xlsx"
PRODUCT_MX_EXCEL = CONFIG_DIR / "product_mapping_MX.xlsx"
PRODUCT_CE_EXCEL = CONFIG_DIR / "product_mapping_CE.xlsx"

MEDIA_MAP_JSON = CONFIG_DIR / "media_mapping.json"
PRODUCT_MX_JSON = CONFIG_DIR / "product_mapping_MX.json"
PRODUCT_CE_JSON = CONFIG_DIR / "product_mapping_CE.json"

# =========================================================
# 3. 마스터 DB 경로 설정
# =========================================================
MASTER_FILES = {
    "MX": OUTPUT_DIR / "TOTAL_MASTER_MX.csv",
    "CE": OUTPUT_DIR / "TOTAL_MASTER_CE.csv"
}

# =========================================================
# 4. [필수] 코드 호환용 상수 (Magic Strings)
# =========================================================
COL_BU = 'BU'
COL_SUB = 'Subsidiary'
COL_DATE = 'Date'

# 삭제할 키워드 (소문자 기준)
IGNORE_KEYWORDS = ['smartphones']

# =========================================================
# 5. 컬럼 매핑 설정 (Column Mapping)
# =========================================================
# (1) Media Mapping
MEDIA_COLS_MAP = {
    'raw_cols': ['Media Type 1', 'Media Type 2', 'Media Platform'],
    'std_cols': ['D_Standard', 'E_Standard', 'F_Standard'],
    'key': 'F_Key',
    'normalize_cols': ['Media Type 1', 'Media Type 2', 'Media Platform'] 
}

# (2) Product Mapping (MX)
PRODUCT_COLS_MAP_MX = {
    'raw_cols': ['Product Category', 'Product Series', 'Products'],
    'std_cols': ['A_Standard', 'B_Standard', 'C_Standard'],
    'key': 'C_Key',
    'normalize_cols': ['Product Category', 'Product Series', 'Products']
}

# (3) Product Mapping (CE)
PRODUCT_COLS_MAP_CE = {
    'raw_cols': ['Product Category', 'Product Series', 'Products'],
    'std_cols': ['A_Standard', 'B_Standard', 'C_Standard'],
    'key': 'A_Key',
    'normalize_cols': ['Product Category', 'Product Series']
}

# 리포트용 헤더
PRODUCT_COLS = ['Product Category', 'Product Series', 'Products']
MEDIA_COLS = ['Media Type 1', 'Media Type 2', 'Media Platform']
CE_PRODUCT_COLS = ['Product Category', 'Product Series', 'Products']

# =========================================================
# 6. 비즈니스 로직 설정
# =========================================================
DIV_RULES = {
    "VD": ["Lifestyle TV", "Monitor","Sound device","TV"],
    "DA": ["Air Dresser", "Air Purifier","Cooking", "Dishwasher", "Dryer", "Heating & Cooling", "Refrigerator", "Vacuum", "Washer", "Washer & Dryer"],
    "MX": ["Hearables", "PC","Smartphones","Tablets", "Wearables"]
}

AMBIGUOUS_CATS = ["Multi", "APS", ]

# =========================================================
# 7. [최종 출력 스펙] 마스터 DB용 컬럼 순서 (User Defined)
# =========================================================
# [MX]
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

# [CE]
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
# 8. 시스템 설정 (System Config)
# =========================================================
WRITE_ENGINE = 'openpyxl'