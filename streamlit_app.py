import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime
from dateutil.relativedelta import relativedelta
import uuid
import re

# =============================================================================
# 0. 기본 설정 및 상수 정의
# =============================================================================
st.set_page_config(page_title="통합 정산 관리 시스템", page_icon="🏢", layout="wide")

SHEET_NAMES = {
    "SETTINGS": "시스템_설정", "LOCATIONS": "사업장_마스터", "ACCOUNTS": "계정과목_마스터",
    "RULES": "자동분류_규칙", "TRANSACTIONS": "통합거래_원장", "INVENTORY": "월별재고_자산",
    "FORMATS": "파일_포맷_마스터"
}

# 파싱 상수 정의
OKPOS_DATA_START_ROW, OKPOS_COL_DATE, OKPOS_COL_DINE_IN, OKPOS_COL_TAKEOUT, OKPOS_COL_DELIVERY = 7, 0, 34, 36, 38
WOORI_DATA_START_ROW, WOORI_COL_CHECK, WOORI_COL_DATETIME, WOORI_COL_DESC, WOORI_COL_AMOUNT = 4, 0, 1, 3, 4

# =============================================================================
# ★★★ 전용 파서 및 헬퍼 함수들 ★★★
# =============================================================================
def parse_okpos(df_raw):
    out = []
    try:
        end_row_series = df_raw[df_raw.iloc[:, OKPOS_COL_DATE].astype(str).str.contains("합계", na=False)].index
        end_row = end_row_series[0] if not end_row_series.empty else df_raw.shape[0]
    except Exception: end_row = df_raw.shape[0]
    df_data = df_raw.iloc[OKPOS_DATA_START_ROW:end_row]
    for _, row in df_data.iterrows():
        try:
            date_cell = row.iloc[OKPOS_COL_DATE]
            if pd.isna(date_cell): continue
            cleaned_date_str = str(date_cell).replace("소계:", "").strip()
            date = pd.to_datetime(cleaned_date_str).strftime('%Y-%m-%d')
            홀매출 = pd.to_numeric(row.iloc[OKPOS_COL_DINE_IN], errors='coerce')
            포장매출 = pd.to_numeric(row.iloc[OKPOS_COL_TAKEOUT], errors='coerce')
            배달매출 = pd.to_numeric(row.iloc[OKPOS_COL_DELIVERY], errors='coerce')
            if pd.notna(홀매출) and 홀매출 != 0: out.append({'거래일자': date, '거래내용': 'OKPOS 홀매출', '금액': 홀매출})
            if pd.notna(포장매출) and 포장매출 != 0: out.append({'거래일자': date, '거래내용': 'OKPOS 포장매출', '금액': 포장매출})
            if pd.notna(배달매출) and 배달매출 != 0: out.append({'거래일자': date, '거래내용': 'OKPOS 배달매출', '금액': 배달매출})
        except Exception: continue
    return pd.DataFrame(out)

def parse_woori_bank(df_raw):
    out, error_rows = [], []
    df_data = df_raw.iloc[WOORI_DATA_START_ROW:].copy()
    for index, row in df_data.iterrows():
        excel_row_num = index + 1
        try:
            check_val = row.iloc[WOORI_COL_CHECK]
            if pd.isna(pd.to_numeric(check_val, errors='coerce')): break
            datetime_str = str(row.iloc[WOORI_COL_DATETIME]).split(' ')[0]
            date = pd.to_datetime(datetime_str).strftime('%Y-%m-%d')
            description = str(row.iloc[WOORI_COL_DESC])
            amount_str = str(row.iloc[WOORI_COL_AMOUNT]).replace(',', '')
            amount = pd.to_numeric(amount_str, errors='coerce')
            if pd.notna(amount) and amount > 0 and description.strip() != '':
                out.append({'거래일자': date, '거래내용': description, '금액': amount})
            else: error_rows.append(excel_row_num)
        except Exception: error_rows.append(excel_row_num); continue
    if error_rows: st.warning(f"⚠️ **{len(error_rows)}개 행 변환 누락:** 원본 파일의 다음 행들을 확인해주세요: {', '.join(map(str, error_rows[:10]))}{'...' if len(error_rows) > 10 else ''}")
    return pd.DataFrame(out)

# =============================================================================
# 1. 구글 시트 연결
# =============================================================================
def get_spreadsheet_key():
    try: return st.secrets["gcp_service_account"]["SPREADSHEET_KEY"]
    except KeyError:
        try: return st.secrets["SPREADSHEET_KEY"]
        except KeyError: st.error("Streamlit Secrets에 'SPREADSHEET_KEY'를 찾을 수 없습니다."); st.stop()

@st.cache_resource
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)

@st.cache_data(ttl=60)
def load_data(sheet_name):
    try:
        spreadsheet_key = get_spreadsheet_key()
        spreadsheet = get_gspread_client().open_by_key(spreadsheet_key)
        worksheet = spreadsheet.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records(head=1))
        for col in df.columns: df[col] = df[col].astype(str).str.strip()
        numeric_cols = ['금액', '기말재고액']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce').fillna(0)
        return df
    except gspread.exceptions.WorksheetNotFound: st.error(f"'{sheet_name}' 시트를 찾을 수 없습니다."); return pd.DataFrame()
    except Exception as e: st.error(f"'{sheet_name}' 시트 로딩 중 오류: {e}"); return pd.DataFrame()

def update_sheet(sheet_name, df):
    try:
        spreadsheet_key = get_spreadsheet_key()
        spreadsheet = get_gspread_client().open_by_key(spreadsheet_key)
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        # 날짜 형식 명시적으로 변환하여 저장
        if '거래일자' in df.columns:
            df['거래일자'] = pd.to_datetime(df['거래일자']).dt.strftime('%Y-%m-%d')
        df_str = df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.update([df_str.columns.values.tolist()] + df_str.values.tolist(), value_input_option='USER_ENTERED')
        st.cache_data.clear(); return True
    except Exception as e: st.error(f"'{sheet_name}' 시트 업데이트 중 오류: {e}"); return False

# =============================================================================
# 2. 로그인, 3. 핵심 로직
# =============================================================================
def login_screen():
    st.title("🏢 통합 정산 관리 시스템")
    settings_df = load_data(SHEET_NAMES["SETTINGS"])
    if settings_df.empty: st.error("`시스템_설정` 시트가 비어있습니다."); st.stop()
    admin_id_row = settings_df[settings_df['Key'] == 'ADMIN_ID']
    admin_pw_row = settings_df[settings_df['Key'] == 'ADMIN_PW']
    if admin_id_row.empty or admin_pw_row.empty: st.error("`시스템_설정` 시트에 ADMIN_ID/PW Key가 없습니다."); st.stop()
    admin_id, admin_pw = admin_id_row['Value'].iloc[0], admin_pw_row['Value'].iloc[0]
    with st.form("login_form"):
        username, password = st.text_input("아이디"), st.text_input("비밀번호", type="password")
        if st.form_submit_button("로그인", use_container_width=True):
            if username == admin_id and password == admin_pw: st.session_state['logged_in'] = True; st.rerun()
            else: st.error("아이디 또는 비밀번호가 올바르지 않습니다.")

def auto_categorize(df, rules_df):
    if rules_df.empty: return df
    categorized_df = df.copy()
    for index, row in categorized_df.iterrows():
        if pd.notna(row.get('계정ID')) and row.get('계정ID') != '': continue
        description = str(row['거래내용'])
        for _, rule in rules_df.iterrows():
            keyword = str(rule['키워드'])
            if keyword and keyword in description:
                categorized_df.loc[index, '계정ID'] = rule['계정ID']
                categorized_df.loc[index, '처리상태'] = '자동분류'; break
    return categorized_df

def calculate_pnl(transactions_df, inventory_df, accounts_df, selected_month, selected_location):
    required_cols = {'transactions': ['사업장명', '거래일자', '계정ID', '금액'], 'inventory': ['사업장명', '기준년월', '기말재고액']}
    if transactions_df.empty or not all(col in transactions_df.columns for col in required_cols['transactions']):
        return pd.DataFrame(), {}, pd.DataFrame()
    if selected_location != "전체":
        transactions_df = transactions_df[transactions_df['사업장명'] == selected_location]
        if not inventory_df.empty and all(col in inventory_df.columns for col in required_cols['inventory']):
            inventory_df = inventory_df[inventory_df['사업장명'] == selected_location]
    
    transactions_df['거래일자'] = pd.to_datetime(transactions_df['거래일자'], errors='coerce')
    
    month_trans = transactions_df[transactions_df['거래일자'].dt.strftime('%Y-%m') == selected_month].copy()
    if month_trans.empty: return pd.DataFrame(), {}, pd.DataFrame()
    pnl_data = pd.merge(month_trans, accounts_df, on='계정ID', how='left')
    pnl_summary = pnl_data.groupby(['대분류', '소분류'])['금액'].sum().reset_index()
    sales = pnl_summary[pnl_summary['대분류'].str.contains('매출', na=False)]['금액'].sum()
    cogs_purchase = pnl_summary[pnl_summary['대분류'].str.contains('원가', na=False)]['금액'].sum()
    begin_inv, end_inv = 0, 0
    if not inventory_df.empty and all(col in inventory_df.columns for col in required_cols['inventory']):
        prev_month = (datetime.strptime(selected_month + '-01', '%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m')
        begin_inv_data = inventory_df[inventory_df['기준년월'] == prev_month]
        begin_inv = begin_inv_data['기말재고액'].sum() if not begin_inv_data.empty else 0
        end_inv_data = inventory_df[inventory_df['기준년월'] == selected_month]
        end_inv = end_inv_data['기말재고액'].sum() if not end_inv_data.empty else 0
    cogs = begin_inv + cogs_purchase - end_inv
    gross_profit = sales - cogs
    expenses = pnl_summary[~pnl_summary['대분류'].str.contains('매출|원가', na=False)]
    total_expenses = expenses['금액'].sum()
    operating_profit = gross_profit - total_expenses
    pnl_final = pd.DataFrame([{'항목': 'Ⅰ. 총매출', '금액': sales}, {'항목': 'Ⅱ. 매출원가', '금액': cogs}, {'항목': 'Ⅲ. 매출총이익', '금액': gross_profit}])
    expense_details = []
    for _, major_cat in expenses.groupby('대분류'):
        major_sum = major_cat['금액'].sum()
        expense_details.append({'항목': f'Ⅳ. {major_cat.iloc[0]["대분류"]}', '금액': major_sum})
        for _, minor_cat in major_cat.iterrows(): expense_details.append({'항목': f' - {minor_cat["소분류"]}', '금액': minor_cat["금액"]})
    if expense_details: pnl_final = pd.concat([pnl_final, pd.DataFrame(expense_details)], ignore_index=True)
    pnl_final = pd.concat([pnl_final, pd.DataFrame([{'항목': 'Ⅴ. 영업이익', '금액': operating_profit}])], ignore_index=True)
    metrics = {"총매출": sales, "매출총이익": gross_profit, "영업이익": operating_profit, "영업이익률": (operating_profit / sales) * 100 if sales > 0 else 0}
    expense_chart_data = expenses.groupby('대분류')['금액'].sum().reset_index()
    return pnl_final, metrics, expense_chart_data

# =============================================================================
# 4. UI 렌더링 함수
# =============================================================================
def render_pnl_page(data):
    st.header("📅 월별 정산표")
    col1, col2 = st.columns(2)
    if not data["LOCATIONS"].empty and '사업장명' in data["LOCATIONS"].columns:
        location_list = ["전체"] + data["LOCATIONS"]['사업장명'].tolist()
    else: location_list = ["전체"]; st.sidebar.warning("`사업장_마스터`에 데이터를 추가해주세요.")
    selected_location = col1.selectbox("사업장 선택", location_list)
    today = datetime.now()
    month_options = [(today - relativedelta(months=i)).strftime('%Y-%m') for i in range(12)]
    selected_month = col2.selectbox("조회 년/월 선택", month_options)
    st.markdown("---")
    if selected_month:
        pnl_df, metrics, expense_chart_data = calculate_pnl(data["TRANSACTIONS"], data["INVENTORY"], data["ACCOUNTS"], selected_month, selected_location)
        if pnl_df.empty: st.warning(f"'{selected_location}'의 {selected_month} 데이터가 없습니다.")
        else:
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("총매출", f"{metrics['총매출']:,.0f} 원"); m2.metric("매출총이익", f"{metrics['매출총이익']:,.0f} 원")
            m3.metric("영업이익", f"{metrics['영업이익']:,.0f} 원"); m4.metric("영업이익률", f"{metrics['영업이익률']:.1f} %")
            st.dataframe(pnl_df.style.format({'금액': '{:,.0f}'}), use_container_width=True, hide_index=True)
            if not expense_chart_data.empty: st.subheader("비용 구성 시각화"); st.bar_chart(expense_chart_data, x='대분류', y='금액')

def render_data_page(data):
    st.header("✍️ 데이터 관리")

    if 'current_step' not in st.session_state: st.session_state.current_step = 'upload'
    
    if st.session_state.current_step == 'upload':
        st.subheader("🏢 데이터 현황")
        if data["TRANSACTIONS"].empty: st.info("아직 등록된 거래내역이 없습니다. 아래에서 파일을 업로드해주세요.")
        else:
            trans_df_copy = data["TRANSACTIONS"].copy()
            trans_df_copy['거래일자'] = pd.to_datetime(trans_df_copy['거래일자'], errors='coerce').dt.normalize()
            summary = trans_df_copy.groupby(['사업장명', '데이터소스']).agg(
                건수=('거래ID', 'count'), 
                최초거래일=('거래일자', 'min'), 
                최종거래일=('거래일자', 'max')
            ).reset_index()
            
            for location in data["LOCATIONS"]['사업장명']:
                st.markdown(f"**{location}**")
                loc_summary = summary[summary['사업장명'] == location]
                if loc_summary.empty: st.write("└ 데이터 없음")
                else:
                    for _, row in loc_summary.iterrows(): 
                        st.write(f"└ `{row['데이터소스']}`: {row['최초거래일'].strftime('%Y-%m-%d')} ~ {row['최종거래일'].strftime('%Y-%m-%d')} (총 {row['건수']}건)")
        st.markdown("---")
        
        if data["LOCATIONS"].empty or data["ACCOUNTS"].empty or data["FORMATS"].empty:
            st.error("`설정 관리`에서 `사업장`, `계정과목`, `파일 포맷`을 먼저 등록해야 합니다."); st.stop()

        tab1, tab2 = st.tabs(["거래내역 관리 (파일 업로드)", "월별재고 관리"])
        with tab1:
            st.subheader("파일 기반 거래내역 관리")
            format_list = data["FORMATS"]['포맷명'].tolist()
            selected_format_name = st.selectbox("1. 처리할 파일 포맷을 선택하세요.", format_list)
            location_list = data["LOCATIONS"]['사업장명'].tolist()
            upload_location = st.selectbox("2. 데이터를 귀속시킬 사업장을 선택하세요.", location_list)
            uploaded_file = st.file_uploader("3. 해당 포맷의 파일을 업로드하세요.", type=["xlsx", "xls", "csv"])

            if st.button("4. 파일 처리 및 확인 단계로 이동", type="primary", use_container_width=True):
                if not uploaded_file: st.error("파일을 먼저 업로드해주세요.")
                else:
                    with st.spinner("파일을 처리하는 중입니다..."):
                        df_raw = None
                        if uploaded_file.name.endswith('.csv'):
                            try: df_raw = pd.read_csv(uploaded_file, encoding='utf-8', header=None)
                            except UnicodeDecodeError: uploaded_file.seek(0); df_raw = pd.read_csv(uploaded_file, encoding='cp949', header=None)
                        else: df_raw = pd.read_excel(uploaded_file, header=None)
                        if df_raw is None: st.error("지원하지 않는 파일 형식입니다."); return

                        df_parsed = pd.DataFrame()
                        if selected_format_name == "OKPOS 매출": df_parsed = parse_okpos(df_raw)
                        elif selected_format_name == "우리은행 지출": df_parsed = parse_woori_bank(df_raw)
                        if df_parsed.empty: st.warning("파일에서 처리할 데이터를 찾지 못했습니다."); return

                        df_final = df_parsed.copy()
                        df_final['사업장명'] = upload_location
                        df_final['구분'] = data["FORMATS"][data["FORMATS"]['포맷명'] == selected_format_name].iloc[0]['데이터구분']
                        df_final['데이터소스'] = selected_format_name
                        df_final['처리상태'] = '미분류'
                        df_final['계정ID'] = ''
                        df_final['거래ID'] = [str(uuid.uuid4()) for _ in range(len(df_final))]
                        
                        st.session_state.df_processed = df_final
                        st.session_state.current_step = 'confirm'
                        st.rerun()
        with tab2:
            st.subheader("월별재고 관리")
            if data["LOCATIONS"].empty:
                st.warning("`설정 관리` 탭에서 `사업장`을 먼저 추가해주세요.")
            else:
                edited_inv = st.data_editor(data["INVENTORY"], num_rows="dynamic", use_container_width=True, hide_index=True,
                    column_config={"사업장명": st.column_config.SelectboxColumn("사업장명", options=data["LOCATIONS"]['사업장명'].tolist(), required=True)})
                if st.button("💾 월별재고 저장"):
                    if update_sheet(SHEET_NAMES["INVENTORY"], edited_inv): st.success("저장되었습니다."); st.rerun()

    elif st.session_state.current_step == 'confirm':
        st.subheader("✅ 1단계: 확인 및 확정")
        df_processed = st.session_state.get('df_processed', pd.DataFrame())
        
        df_duplicates = pd.DataFrame()
        if not df_processed.empty and df_processed['구분'].iloc[0] == '비용':
            existing = data["TRANSACTIONS"]
            if not existing.empty:
                existing['duplicate_key'] = existing['사업장명'] + existing['거래내용'] + existing['금액'].astype(str)
                df_processed['duplicate_key'] = df_processed['사업장명'] + df_processed['거래내용'] + df_processed['금액'].astype(str)
                df_duplicates = df_processed[df_processed['duplicate_key'].isin(existing['duplicate_key'])]
                df_processed = df_processed[~df_processed['duplicate_key'].isin(existing['duplicate_key'])]
        
        df_processed = auto_categorize(df_processed, data["RULES"])
        df_auto = df_processed[df_processed['처리상태'] == '자동분류']
        df_manual = df_processed[df_processed['처리상태'] == '미분류']

        if not df_duplicates.empty:
            with st.expander(f"⚠️ **{len(df_duplicates)}건의 중복 의심 거래**가 발견되었습니다. (날짜와 상관없이 내용, 금액 일치)"):
                st.dataframe(df_duplicates[['거래일자', '거래내용', '금액']])
        
        if not df_auto.empty:
            with st.expander(f"🤖 **{len(df_auto)}**건이 자동으로 분류됩니다."):
                df_auto_display = pd.merge(df_auto, data["ACCOUNTS"], on="계정ID", how="left")
                st.dataframe(df_auto_display[['거래일자', '거래내용', '금액', '대분류', '소분류']], hide_index=True)

        col1, col2 = st.columns(2)
        if col1.button("🔙 이전 단계로", use_container_width=True):
            st.session_state.current_step = 'upload'; st.rerun()

        if col2.button("2단계: 분류 작업대 열기 ➡️", type="primary", use_container_width=True):
            st.session_state.workbench_data = pd.concat([df_auto, df_manual], ignore_index=True).drop(columns=['duplicate_key'], errors='ignore')
            st.session_state.current_step = 'workbench'
            st.rerun()

    elif st.session_state.current_step == 'workbench':
        if 'workbench_data' not in st.session_state or st.session_state.workbench_data.empty:
            st.success("모든 내역 처리가 완료되었습니다.")
            if st.button("초기 화면으로 돌아가기", use_container_width=True):
                st.session_state.current_step = 'upload'
                st.rerun()
            return

        st.subheader(f"✍️ 분류 작업대 (남은 내역: {len(st.session_state.workbench_data)}건)")
        st.info("변경을 원하는 행의 계정과목을 지정하고 저장하세요. 일부만 처리할 수 있습니다.")
        
        # --- 데이터 준비 ---
        accounts_df = data["ACCOUNTS"]
        account_options = [""] + [f"[{r['대분류']}/{r['소분류']}] ({r['계정ID']})" for _, r in accounts_df.iterrows()]
        account_map = {f"[{r['대분류']}/{r['소분류']}] ({r['계정ID']})": r['계정ID'] for _, r in accounts_df.iterrows()}
        id_to_account = {v: k for k, v in account_map.items()}

        df_original_workbench = st.session_state.workbench_data.copy()
        df_display = df_original_workbench.copy()
        
        # --- BUG FIX: 날짜에서 시간 정보 제거 ---
        df_display['거래일자'] = pd.to_datetime(df_display['거래일자']).dt.normalize()
        df_display['계정과목_선택'] = df_display['계정ID'].map(id_to_account).fillna("")
        
        # --- Data Editor UI ---
        edited_df = st.data_editor(
            df_display[['거래일자', '거래내용', '금액', '계정과목_선택']],
            hide_index=True, use_container_width=True, key="workbench_editor", num_rows="dynamic",
            # --- BUG FIX: 계정과목 ID 표시 및 드롭다운 복원 ---
            column_config={
                "거래일자": st.column_config.DateColumn("거래일자", format="YYYY-MM-DD"),
                "거래내용": st.column_config.TextColumn("거래내용"),
                "금액": st.column_config.NumberColumn("금액"),
                "계정과목_선택": st.column_config.SelectboxColumn("계정과목 선택", options=account_options)
            }
        )

        st.markdown("---")
        if st.button("💾 변경된 내용 저장하기", type="primary", use_container_width=True):
            # --- 부분 저장 로직 (재설계) ---
            
            # 1. 원본/수정본 데이터 정제 및 병합
            df_original_workbench['계정과목_선택'] = df_original_workbench['계정ID'].map(id_to_account).fillna("")
            df_original_workbench['거래일자'] = pd.to_datetime(df_original_workbench['거래일자']).dt.normalize()
            
            edited_df['거래일자'] = pd.to_datetime(edited_df['거래일자']).dt.normalize()
            
            # 비교를 위해 원본의 거래ID를 인덱스로 사용
            df_merged = df_original_workbench.set_index('거래ID').join(
                edited_df.rename(columns=lambda c: f"{c}_ed"),
                how='outer'
            )
            
            # 2. 변경 여부 확인
            # np.isclose은 숫자 타입 비교, 나머지는 직접 비교
            numeric_cols_equal = np.isclose(df_merged['금액'], df_merged['금액_ed'])
            other_cols_equal = (df_merged['거래일자'] == df_merged['거래일자_ed']) & \
                               (df_merged['거래내용'] == df_merged['거래내용_ed']) & \
                               (df_merged['계정과목_선택'] == df_merged['계정과목_선택_ed'])
            
            is_changed = ~ (numeric_cols_equal & other_cols_equal)
            
            # 3. 저장 가능 여부 확인 (완성도)
            is_complete = (df_merged['계정과목_선택_ed'] != "") & \
                          df_merged['거래일자_ed'].notna() & \
                          df_merged['금액_ed'].notna()

            # 4. 저장/유지/경고 대상 선정
            to_save_ids = df_merged[is_changed & is_complete].index
            to_keep_ids = df_merged[~is_changed | (is_changed & ~is_complete)].index
            incomplete_edits_exist = is_changed[is_changed & ~is_complete].any()

            if incomplete_edits_exist:
                st.warning("⚠️ 일부 수정되었지만 내용이 불완전한 항목은 저장되지 않고 작업대에 남습니다.")

            if to_save_ids.empty:
                st.info("저장할 만큼 충분히 수정된 항목이 없습니다.")
            else:
                # 5. 데이터 처리 및 저장
                df_to_save = df_merged.loc[to_save_ids].copy()
                df_to_save.reset_index(inplace=True)

                # 수정된 값으로 최종 데이터 업데이트
                df_to_save['거래일자'] = df_to_save['거래일자_ed']
                df_to_save['거래내용'] = df_to_save['거래내용_ed']
                df_to_save['금액'] = df_to_save['금액_ed']
                df_to_save['계정ID'] = df_to_save['계정과목_선택_ed'].map(account_map)
                df_to_save['처리상태'] = '수동확인'

                # 최종 시트에 맞게 컬럼 정리
                final_cols = data["TRANSACTIONS"].columns
                df_to_save = df_to_save[final_cols]

                with st.spinner(f"{len(df_to_save)}건의 항목을 저장하는 중입니다..."):
                    combined_trans = pd.concat([data["TRANSACTIONS"], df_to_save], ignore_index=True)
                    if update_sheet(SHEET_NAMES["TRANSACTIONS"], combined_trans):
                        st.success(f"{len(df_to_save)}건을 성공적으로 저장했습니다.")
                        
                        # 작업대에 남길 데이터 업데이트
                        if not to_keep_ids.empty:
                            st.session_state.workbench_data = st.session_state.workbench_data[st.session_state.workbench_data['거래ID'].isin(to_keep_ids)]
                        else:
                            del st.session_state.workbench_data
                        
                        st.rerun()

def render_settings_page(data):
    st.header("⚙️ 설정 관리")
    tab1, tab2, tab3, tab4 = st.tabs(["🏢 사업장 관리", "📚 계정과목 관리", "🤖 자동분류 규칙", "📄 파일 포맷 관리"])
    with tab1:
        edited_locs = st.data_editor(data["LOCATIONS"], num_rows="dynamic", use_container_width=True, hide_index=True)
        if st.button("사업장 정보 저장"):
            if update_sheet(SHEET_NAMES["LOCATIONS"], edited_locs): st.success("저장되었습니다."); st.rerun()
    with tab2:
        edited_accs = st.data_editor(data["ACCOUNTS"], num_rows="dynamic", use_container_width=True, hide_index=True)
        if st.button("계정과목 저장"):
            if update_sheet(SHEET_NAMES["ACCOUNTS"], edited_accs): st.success("저장되었습니다."); st.rerun()
    with tab3:
        if data["ACCOUNTS"].empty: st.warning("`계정과목 관리` 탭에서 계정과목을 먼저 추가해주세요.")
        else:
            edited_rules = st.data_editor(data["RULES"], num_rows="dynamic", use_container_width=True, hide_index=True,
                column_config={"계정ID": st.column_config.SelectboxColumn("계정ID", options=data["ACCOUNTS"]['계정ID'].tolist(), required=True)})
            if st.button("자동분류 규칙 저장"):
                if update_sheet(SHEET_NAMES["RULES"], edited_rules): st.success("저장되었습니다."); st.rerun()
    with tab4:
        edited_formats = st.data_editor(data["FORMATS"], num_rows="dynamic", use_container_width=True, hide_index=True,
            column_config={"데이터구분": st.column_config.SelectboxColumn("데이터구분", options=["수익", "비용"], required=True)})
        if st.button("파일 포맷 저장"):
            if update_sheet(SHEET_NAMES["FORMATS"], edited_formats): st.success("저장되었습니다."); st.rerun()
            
# =============================================================================
# 5. 메인 실행 로직
# =============================================================================
def main():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if not st.session_state['logged_in']:
        login_screen()
    else:
        st.sidebar.title("🏢 통합 정산 시스템")
        with st.spinner("데이터를 불러오는 중입니다..."):
            data = {name: load_data(sheet) for name, sheet in SHEET_NAMES.items()}
        
        menu = ["📅 월별 정산표", "✍️ 데이터 관리", "⚙️ 설정 관리"]
        choice = st.sidebar.radio("메뉴를 선택하세요.", menu)
        
        st.sidebar.markdown("---")
        if st.sidebar.button("🔃 데이터 새로고침"):
            keys_to_delete = [k for k in st.session_state.keys() if k != 'logged_in']
            for key in keys_to_delete:
                del st.session_state[key]
            st.cache_data.clear()
            st.rerun()

        if st.sidebar.button("로그아웃"): 
            st.session_state.clear()
            st.rerun()
            
        if choice == "📅 월별 정산표": render_pnl_page(data)
        elif choice == "✍️ 데이터 관리": render_data_page(data)
        elif choice == "⚙️ 설정 관리": render_settings_page(data)

if __name__ == "__main__":
    main()
