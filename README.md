---
marp: false
---

# GMO_Cleansing Pipeline (클렌징 중심)

이 문서는 `C:\GMO_Cleansing`의 실제 코드 중 **클렌징 로직**만 설명합니다.  
범위: 전처리, 정규화, 매핑, 보정, 리포트용 출력  
제외: 마스터 업데이트/동기화(`updater.update_smart_db`) 상세

---

## 1. 클렌징 실행 흐름

```text
run.py
 -> 매핑 엑셀을 JSON 캐시로 변환
 -> Input 폴더 CSV 순회
 -> 컬럼 위생 정리
 -> 날짜 클렌징 (파싱/필터링 + Week/Month/Quarter 재계산)
 -> 수치 컬럼 정리 (CPC 포함)
 -> Media 클렌징
 -> BU 컬럼 존재 여부로 MX/CE 분기
 -> Product 클렌징
 -> 포맷팅 + 파일 저장 + 변화요약/Unmapped 리포트 생성
```

핵심 역할:
- `src/pipeline.py`: 클렌징 코어 엔진
- `src/utils.py`: CSV 로드, 날짜 클렌징
- `run.py`: 실행 오케스트레이션(분기/저장 포함)

---

## 2. 전처리

### 2.1 컬럼 위생 정리 (`sanitize_column_headers`)

- BOM 제거
- 줄바꿈/중복공백/따옴표 제거
- 중복 컬럼명은 첫 컬럼 유지

효과:
- merge 키 컬럼명이 깨져 매핑이 실패하는 문제를 선제 차단

### 2.2 날짜 클렌징 (`process_and_filter_dates`)

순서:
1. 숫자형 값은 Excel serial date로 파싱 (`origin='1899-12-30'`)
2. 비숫자 값은 텍스트 날짜 파싱
3. 파싱 실패 건은 특수문자 정리 후 재파싱
4. 유효하지 않거나 `2025-01-01` 이전 날짜 제거
5. `Date`를 `YYYY-MM-DD`로 표준화
6. `Week/Month/Quarter` 컬럼이 있으면 재계산


### 2.3 수치 컬럼 정리 (`process_metric_columns`)

#### 수치 변환
- 대상: `Impressions`, `Clicks`, `Media Spend (USD)`, `Media Spend`, `Orders`, `Revenue`, `App Install`
- 콤마 제거 후 `to_numeric(errors='coerce')`
- 결측/문자값은 `0`으로 보정
- `-`, `–`, `—`도 `0`으로 처리

#### CPC 계산
- 클릭 0 또는 무한대/결측은 0 처리

---

## 3. 텍스트 정규화 (`fast_normalize_text`)

아래 규칙을 순차 적용합니다.

1. 결측/공백 정리
2. `([a-z])([A-Z])` 소문자-대문자 경계 분리
3. `([A-Z])([A-Z][a-z])` 약어-일반단어 경계 분리
4. `(\d+)([a-zA-Z]+)` 숫자-문자 경계 분리
5. 전체 소문자/대문자 문자열만 Title Case(단어 앞 첫글자 대문자) 보정

예시:
- `s25ultra` -> `s25 ultra` -> `S25 Ultra`(4,5)
- `QLEDUltra` -> `QLED Ultra`(3)
- `ZFold7Fe` -> `Z Fold7 Fe` (2,4)

---

## 4. 공통 매핑 엔진 (Media + MX Product)

함수: `run_cleansing_pipeline`

### 4.1 처리 단계
1. 원본 index를 `original_index`로 보존
2. `_norm_*` 컬럼 생성
3. 매핑 테이블 merge
4. 우선순위 점수 계산(`priority`)
5. 원본 행 단위 최적 후보 1건 선택
6. fallback/rescue/pass-through 적용
7. unmapped 리포트 산출

### 4.2 점수 규칙

| 점수 | 조건 
|---|---|
| 4 | Cat + Series + Product 일치 
| 3 | Cat + Series 일치 
| 2 | Series 일치 
| 1 | key만 존재   
| 0 | key 없음 

매칭 특수값:
- `NONE`: 해당 레벨 검증 생략(CE - Product)
- `ANY`: 값이 비어있지 않으면 통과(Affiliate - Media Platform)

동점 처리:
- 같은 점수 다중 후보 시 매핑 파일내 선입력순으로 채택됨 -> 최종 더블체크 필수

### 4.3 Media 특수 처리

- 기본 key: `Media Platform`
- 단, Affiliate는 key를 `Media Type 2`로 변경
- Affiliate 매핑 성공 시 `Media Platform_cleaned`는 원문 플랫폼 복원
- Affiliate 매핑 실패 시:
  - raw 값으로 fallback
  - `Media Type 2_cleaned = 'Others'`
  - `Media Platform_cleaned = 원문`

### 4.4 MX Rescue

대상:
- 1차 결과가 `Multi` 또는 null인 행

보정 방식:
- `series_reverse_map`: 시리즈를 기준으로 카테고리 역추론
- `fallback_map`: 카테고리별 기본 시리즈(Other 우선) 지정
- rescue 성공 시 `Category/Series/Product_cleaned` 재기입
  - Product는 시리즈 값으로 채워 정규화(Fall back)

ex) input : Smartphones / S25 Series / Multi
    Standard Mapping : Multi : Multi / Multi / Multi 우선 매핑
    RESCUE : Smartphones / S25 Series / S25 Series

### 4.5 Valid Combination Pass-through

원본 `Category|Series|Product` 조합이 매핑 표준 조합에 이미 존재하면,
클렌징 결과를 원본으로 유지(불필요한 변형 방지)

### 4.6 Unmapped 판정 (MX/Media)

단순 `priority==0`이 아니라 아래 조건을 모두 만족해야 실제 Unmapped:
1. `priority == 0`
2. rescue 미성공
3. valid-combination pass-through 아님

---

## 5. CE 전용 Product 클렌징

함수: `run_ce_product_cleansing`

핵심 원칙:
- 카테고리 중심 매핑 + 중복 시리즈 오매핑 방지

### 5.1 로직 구조
1. 카테고리/시리즈 정규화
2. 카테고리 키(`A_Key`) 기준 1차 매핑
3. 카테고리별 시리즈 맵 생성(`series_map_by_cat`)
4. 시리즈 출현 빈도 집계(`seen_series_counts`)
5. 유일 시리즈만 전역 rescue 맵에 등록(`global_series_lookup`)
6. 케이스별 결과 결정

### 5.2 케이스별 처리
- 카테고리 정상 + 시리즈 정상 -> 표준값
- 카테고리 정상 + 시리즈 비정상 -> 카테고리 fallback(`Others` 우선, 없으면 `Multi`)
- 카테고리 비정상 + 시리즈 전역 유일 -> 전역 rescue
- 둘 다 비정상 -> `NON_CE_CATEGORY`

후처리:
- `NON_CE_CATEGORY`는 최종 표시 시 원본 카테고리/시리즈로 복구
- Unmapped 리포트는 실패 키를 별도 저장

### 5.3 CE 분기 추가 필터 (`run.py`)
- `IGNORE_KEYWORDS = ['smartphones']`
- CE에서 해당 카테고리(cleaned)가 있으면 후단에서 삭제

---

## 6. 후처리

### 6.1 공통
- `process_subsidiary_column`: 법인명 공백 정리

### 6.2 MX
- `process_mindset_column`: `cold` -> `Cold`
- `process_funding_column`: `gmo`/`local` 표준화

### 6.3 CE
- `assign_ce_division`
  - raw BU를 대문자로 초기화
  - `DIV_RULES`에 따라 카테고리 기반 BU override (APS/Mulit 원본유지)

---

## 7. 출력 파일 생성물

- `Cleaned_Result` CSV
- `Summary_Prod` CSV (제품 컬럼 변화 집계)
- `Summary_Media` CSV (미디어 컬럼 변화 집계)
- Unmapped 리포트 CSV (`Reports/Unmapped_Products.csv`, `Reports/Unmapped_Media.csv`)

---


