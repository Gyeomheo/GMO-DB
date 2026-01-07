"""
[Smart Updater - Final Robust Ver.]
- Fix: Floating Point Noise (소수점 정밀도 차이로 인한 허위 업데이트 방지)
- Fix: Rounding Logic (비교 전 소수점 2자리 통일)
- Improvement: Audit Log 메시지 명확화 (Update -> Candidates)
"""
import pandas as pd
import logging
import shutil
from datetime import datetime, timedelta
from . import config

def safe_parse_dates(series: pd.Series) -> pd.Series:
    """날짜 파싱 유틸리티"""
    s_clean = series.fillna('').astype(str).str.strip()
    mask_numeric = s_clean.str.match(r'^\d+(\.\d+)?$')
    result = pd.to_datetime(s_clean, errors='coerce')
    
    if mask_numeric.any():
        try:
            numeric_vals = pd.to_numeric(s_clean[mask_numeric], errors='coerce')
            excel_dates = pd.to_datetime(numeric_vals, unit='D', origin='1899-12-30')
            result.loc[mask_numeric] = excel_dates
        except Exception: pass
    return result

def force_numeric_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """수치 강제 변환 및 소수점 정리"""
    check_metrics = ['Media Spend (USD)', 'Revenue', 'Impressions', 'Clicks', 'Orders']
    for col in check_metrics:
        if col in df.columns:
            # 1. 수치 변환
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(',', '').str.strip(), errors='coerce'
            ).fillna(0)
            # 2. [핵심] 소수점 2자리 반올림 (미세 오차 제거)
            df[col] = df[col].round(2)
        else:
            df[col] = 0.0
    return df

def get_safe_divergence_date(df_master, df_new):
    """
    [Core Logic] 변경 시작점 감지
    """
    check = ['Media Spend (USD)', 'Revenue', 'Impressions', 'Clicks', 'Orders']
    
    # 원본 보호
    df_m = df_master.copy()
    df_n = df_new.copy()
    
    # 1. 주차별 스캔
    m_week = df_m.groupby('Week')[check].sum()
    n_week = df_n.groupby('Week')[check].sum()
    
    # 정렬된 주차 순으로 비교
    for week in sorted(n_week.index):
        if week not in m_week.index:
            return df_new[df_new['Week']==week]['Date'].min()
        
        # [Fix] 이미 round(2)가 되어 있으므로 단순 비교 가능하지만, 안전하게 np.isclose 사용
        # (혹은 0.01 차이 비교 유지)
        diff = (m_week.loc[week] - n_week.loc[week]).abs()
        
        if (diff >= 0.01).any():
            # 2. 일자별 상세 스캔
            sub_n = df_n[df_n['Week']==week]
            sub_m = df_m[df_m['Week']==week]
            
            m_day = sub_m.groupby('Date')[check].sum()
            n_day = sub_n.groupby('Date')[check].sum()
            
            for d in sorted(sub_n['Date'].unique()):
                if d not in m_day.index: return d
                
                delta = m_day.loc[d] - n_day.loc[d]
                day_diff = delta.abs()
                
                if (day_diff >= 0.01).any():
                    # 상세 로그 출력
                    diff_cols = day_diff[day_diff >= 0.01].index.tolist()
                    logging.info(f"    🔍 [Diff Detail] {d.date()} 변경 감지!")
                    for col in diff_cols:
                        old_val = m_day.loc[d, col]
                        new_val = n_day.loc[d, col]
                        logging.info(f"       👉 {col}: Master({old_val}) -> New({new_val})")
                    return d
                
    return df_master['Date'].max() + timedelta(days=1)

def update_smart_db(new_df: pd.DataFrame, division: str):
    master_path = config.MASTER_FILES.get(division)
    if not master_path: return
    save_cols = new_df.columns.tolist()

    if not master_path.exists():
        logging.info(f"✨ [{division}] 신규 생성")
        new_df.to_csv(master_path, index=False, encoding='utf-8-sig')
        return

    try:
        # 백업
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        config.BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(master_path, config.BACKUP_DIR / f"Backup_{division}_{ts}.csv")
        
        # 로드
        df_master = pd.read_csv(master_path, low_memory=False)
        df_master.columns = df_master.columns.str.strip()
        new_df.columns = new_df.columns.str.strip()

        if 'Subsidiary' not in df_master.columns:
             if df_master.empty: new_df.to_csv(master_path, index=False, encoding='utf-8-sig'); return
             else: raise KeyError("Master missing 'Subsidiary'")

        # 전처리
        for df in [df_master, new_df]:
            df['Date'] = safe_parse_dates(df['Date'])
            df['Week'] = df['Week'].astype(str)
            df['Subsidiary'] = df['Subsidiary'].fillna('Unknown').astype(str).str.strip()
        
        # [핵심] 비교 전 소수점 정리 (Rounding)
        df_master = force_numeric_metrics(df_master)
        new_df = force_numeric_metrics(new_df)

        targets = new_df['Subsidiary'].unique()
        mask = df_master['Subsidiary'].isin(targets)
        df_others = df_master[~mask].copy()
        df_targets = df_master[mask].copy()

        # [Log 수정] Update -> Candidates (오해 방지)
        logging.info(f"📊 [Audit] Total: {len(df_master):,} | Keep: {len(df_others):,} | Candidates: {len(df_targets):,}")

        final_dfs = [df_others]
        for sub in targets:
            s_new = new_df[new_df['Subsidiary']==sub].sort_values('Date')
            s_old = df_targets[df_targets['Subsidiary']==sub].sort_values('Date')
            
            if s_old.empty: 
                final_dfs.append(s_new)
                logging.info(f"   ➕ [{sub}] 신규 추가")
                continue
            
            cutoff = get_safe_divergence_date(s_old, s_new)
            
            final_dfs.append(s_old[s_old['Date'] < cutoff])
            final_dfs.append(s_new[s_new['Date'] >= cutoff])
            
            if cutoff <= s_old['Date'].max(): 
                logging.info(f"   🔄 [{sub}] 업데이트: {cutoff.date()}~")

        final = pd.concat(final_dfs, ignore_index=True)
        remains = [c for c in df_master.columns if c not in save_cols]
        final = final.reindex(columns=save_cols + remains)
        
        sort_k = [c for c in ['Date', 'Subsidiary', 'Week'] if c in final.columns]
        final.sort_values(sort_k, inplace=True)
        
        final.to_csv(master_path, index=False, encoding='utf-8-sig')
        logging.info(f"✅ [{division}] 완료. Total: {len(final):,} rows")
        
    except Exception as e:
        logging.error(f"🚨 [{division}] 업데이트 실패: {e}", exc_info=True)