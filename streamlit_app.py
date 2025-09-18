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

def suggest_keywords(description):
    text = re.sub(r'\(.+?\)|\[.+?\]', '', description).strip()
    text = re.sub(r'[^A-Za-z0-9가-힣\s]', ' ', text).strip()
    keywords = [word for word in text.split() if len(word) > 1 and not word.isdigit()]
    return list(dict.fromkeys(keywords))

# =============================================================================
# 1. 구글 시트 연결 (이하 동일)
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
        df_str = df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.update([df_str.columns.values.tolist()] + df_str.values.tolist(), value_input_option='USER_ENTERED')
        st.cache_data.clear(); return True
    except Exception as e: st.error(f"'{sheet_name}' 시트 업데이트 중 오류: {e}"); return False

# =============================================================================
# 2. 로그인, 3. 핵심 로직 (이하 동일)
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
    if selected_location != "전체":
        transactions_df = transactions_df[transactions_df['사업장명'] == selected_location]
        inventory_df = inventory_df[inventory_df['사업장명'] == selected_location]
    if '거래일자' not in transactions_df.columns: return pd.DataFrame(), {}, pd.DataFrame()
    transactions_df['거래일자'] = pd.to_datetime(transactions_df['거래일자'])
    month_trans = transactions_df[transactions_df['거래일자'].dt.strftime('%Y-%m') == selected_month].copy()
    if month_trans.empty: return pd.DataFrame(), {}, pd.DataFrame()
    pnl_data = pd.merge(month_trans, accounts_df, on='계정ID', how='left')
    pnl_summary = pnl_data.groupby(['대분류', '소분류'])['금액'].sum().reset_index()
    sales = pnl_summary[pnl_summary['대분류'].str.contains('매출', na=False)]['금액'].sum()
    cogs_purchase = pnl_summary[pnl_summary['대분류'].str.contains('원가', na=False)]['금액'].sum()
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
    # (이전과 동일)
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

# 기존 코드에서 이 함수 전체를 아래 내용으로 교체해주세요.

def render_data_page(data):
    st.header("✍️ 데이터 관리")

    # --- Session State 초기화 ---
    if 'current_step' not in st.session_state:
        st.session_state.current_step = 'upload'

    # --- 1단계: 파일 업로드 ---
    if st.session_state.current_step == 'upload':
        st.subheader("🏢 데이터 현황")
        if data["TRANSACTIONS"].empty:
            st.info("아직 등록된 거래내역이 없습니다. 아래에서 파일을 업로드해주세요.")
        else:
            summary = data["TRANSACTIONS"].groupby(['사업장명', '데이터소스']).agg(
                건수=('거래ID', 'count'), 최초거래일=('거래일자', 'min'), 최종거래일=('거래일자', 'max')
            ).reset_index()
            for location in data["LOCATIONS"]['사업장명']:
                st.markdown(f"**{location}**")
                loc_summary = summary[summary['사업장명'] == location]
                if loc_summary.empty:
                    st.write("└ 데이터 없음")
                else:
                    for _, row in loc_summary.iterrows():
                        st.write(f"└ `{row['데이터소스']}`: {row['최초거래일']} ~ {row['최종거래일']} (총 {row['건수']}건)")
        st.markdown("---")

        if data["LOCATIONS"].empty or data["ACCOUNTS"].empty or data["FORMATS"].empty:
            st.error("`설정 관리`에서 `사업장`, `계정과목`, `파일 포맷`을 먼저 등록해야 합니다.")
            st.stop()

        tab1, tab2 = st.tabs(["거래내역 관리 (파일 업로드)", "월별재고 관리"])
        with tab1:
            st.subheader("파일 기반 거래내역 관리")
            format_list = data["FORMATS"]['포맷명'].tolist()
            selected_format_name = st.selectbox("1. 처리할 파일 포맷을 선택하세요.", format_list)
            
            location_list = data["LOCATIONS"]['사업장명'].tolist()
            upload_location = st.selectbox("2. 데이터를 귀속시킬 사업장을 선택하세요.", location_list)
            uploaded_file = st.file_uploader("3. 해당 포맷의 파일을 업로드하세요.", type=["xlsx", "xls", "csv"])

            if st.button("4. 파일 처리 및 분류 작업대 열기", type="primary", use_container_width=True):
                if not uploaded_file:
                    st.error("파일을 먼저 업로드해주세요.")
                else:
                    with st.spinner("파일을 처리하는 중입니다..."):
                        df_raw = None
                        if uploaded_file.name.endswith('.csv'):
                            try:
                                df_raw = pd.read_csv(uploaded_file, encoding='utf-8', header=None)
                            except UnicodeDecodeError:
                                uploaded_file.seek(0)
                                df_raw = pd.read_csv(uploaded_file, encoding='cp949', header=None)
                        else:
                            df_raw = pd.read_excel(uploaded_file, header=None)

                        if df_raw is None:
                            st.error("지원하지 않는 파일 형식입니다.")
                            return

                        df_parsed = pd.DataFrame()
                        if selected_format_name == "OKPOS 매출":
                            df_parsed = parse_okpos(df_raw)
                        elif selected_format_name == "우리은행 지출":
                            df_parsed = parse_woori_bank(df_raw)
                        
                        if df_parsed.empty:
                            st.warning("파일에서 처리할 데이터를 찾지 못했습니다.")
                            return

                        df_final = df_parsed.copy()
                        df_final.loc[:, '사업장명'] = upload_location
                        df_final.loc[:, '구분'] = data["FORMATS"][data["FORMATS"]['포맷명'] == selected_format_name].iloc[0]['데이터구분']
                        df_final.loc[:, '데이터소스'] = selected_format_name
                        df_final.loc[:, '처리상태'] = '미분류'
                        df_final.loc[:, '계정ID'] = ''
                        df_final.loc[:, '거래ID'] = [str(uuid.uuid4()) for _ in range(len(df_final))]
                        
                        df_to_process = df_final
                        if df_final['구분'].iloc[0] == '비용':
                            existing = data["TRANSACTIONS"]
                            if not existing.empty:
                                existing['unique_key'] = existing['사업장명'] + existing['거래일자'].astype(str) + existing['거래내용'] + existing['금액'].astype(str)
                                df_final['unique_key'] = df_final['사업장명'] + df_final['거래일자'].astype(str) + df_final['거래내용'] + df_final['금액'].astype(str)
                                df_to_process = df_final[~df_final['unique_key'].isin(existing['unique_key'])].drop(columns=['unique_key'])
                        
                        df_processed = auto_categorize(df_to_process, data["RULES"])
                        st.session_state.df_auto = df_processed[df_processed['처리상태'] == '자동분류']
                        st.session_state.df_manual_workbench = df_processed[df_processed['처리상태'] == '미분류']
                        st.session_state.current_step = 'workbench'
                        st.rerun()
        
        with tab2:
            st.subheader("월별재고 관리")
            edited_inv = st.data_editor(data["INVENTORY"], num_rows="dynamic", use_container_width=True, hide_index=True,
                column_config={"사업장명": st.column_config.SelectboxColumn("사업장명", options=data["LOCATIONS"]['사업장명'].tolist(), required=True)})
            if st.button("💾 월별재고 저장"):
                if update_sheet(SHEET_NAMES["INVENTORY"], edited_inv):
                    st.success("저장되었습니다.")
                    st.rerun()

    # --- 2단계 & 3단계: 분류 작업대 & 규칙 추천 ---
    elif st.session_state.current_step == 'workbench':
        if 'workbench_data' not in st.session_state:
            st.session_state.workbench_data = st.session_state.get('df_manual_workbench', pd.DataFrame())

        df_workbench = st.session_state.workbench_data
        
        if df_workbench.empty:
            st.success("모든 내역 처리가 완료되었습니다.")
            if st.button("초기 화면으로 돌아가기"):
                for key in ['current_step', 'df_auto', 'df_manual_workbench', 'workbench_data']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
            return

        st.subheader(f"✍️ 분류 작업대 (남은 내역: {len(df_workbench)}건)")

        col1, col2 = st.columns([2.5, 1.5])
        with col1:
            st.info("데이터를 편집하고 `계정과목`을 선택하세요. 일부만 처리하고 저장할 수 있습니다.")
            
            accounts_df = data["ACCOUNTS"]
            account_options = [""] + [f"[{r['대분류']}/{r['소분류']}] ({r['계정ID']})" for _, r in accounts_df.iterrows()]
            
            # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
            # 오류 수정: 괄호를 ']'에서 '}'로 변경
            # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
            account_map = {f"[{r['대분류']}/{r['소분류']}] ({r['계정ID']})": r['계정ID'] for _, r in accounts_df.iterrows()}
            
            df_workbench['계정과목_선택'] = ""
            
            edited_workbench = st.data_editor(df_workbench[['거래일자', '거래내용', '금액', '계정과목_선택']], hide_index=True, use_container_width=True,
                key="workbench_editor", num_rows="dynamic",
                column_config={"계정과목_선택": st.column_config.SelectboxColumn("계정과목 선택", options=account_options, required=True)})
            
            classified_rows = edited_workbench[edited_workbench['계정과목_선택'] != ""].copy()
            if not classified_rows.empty:
                if st.button(f"💾 분류된 {len(classified_rows)}개 항목 저장하기", type="primary"):
                    classified_rows['계정ID'] = classified_rows['계정과목_선택'].map(account_map)
                    classified_rows['처리상태'] = '수동확인'
                    
                    final_to_save = df_workbench.copy()
                    final_to_save.update(classified_rows)
                    final_to_save = final_to_save[final_to_save['계정ID'] != ""].drop(columns=['계정과목_선택'])

                    df_auto = st.session_state.get('df_auto', pd.DataFrame())
                    combined_trans = pd.concat([data["TRANSACTIONS"], df_auto, final_to_save], ignore_index=True)
                    
                    if update_sheet(SHEET_NAMES["TRANSACTIONS"], combined_trans):
                        st.success(f"{len(final_to_save)}건이 저장되었습니다.")
                        st.session_state.df_auto = pd.DataFrame() # 자동 분류 내역은 한 번만 저장
                        st.session_state.workbench_data = edited_workbench[edited_workbench['계정과목_선택'] == ""].copy()
                        st.rerun()

        with col2:
            st.subheader("🤖 규칙 추천")
            classified_for_rules = edited_workbench[edited_workbench['계정과목_선택'] != ""].copy()
            if len(classified_for_rules) < 2:
                st.info("계정과목을 2개 이상 선택하면, 규칙을 추천해 드립니다.")
            else:
                classified_for_rules['계정ID'] = classified_for_rules['계정과목_선택'].map(account_map)
                suggestions = classified_for_rules.groupby('계정ID')['거래내용'].apply(list).reset_index()
                accounts_map = data["ACCOUNTS"].set_index('계정ID').to_dict('index')

                for _, row in suggestions.iterrows():
                    if len(row['거래내용']) < 2: continue
                    account_info = accounts_map.get(row['계정ID'])
                    if not account_info: continue
                    
                    common_keywords = suggest_keywords(' '.join(row['거래내용']))
                    if common_keywords:
                        st.markdown(f"**`[{account_info['대분류']}/{account_info['소분류']}]`**으로 분류한 내역:")
                        keyword_to_add = st.selectbox("어떤 키워드로 규칙을 만들까요?", [""] + common_keywords, key=f"kw_sel_{row['계정ID']}")
                        if keyword_to_add:
                            if st.button(f"✨ '{keyword_to_add}' 규칙 추가하고 바로 적용하기", key=f"kw_btn_{row['계정ID']}"):
                                new_rule = {'데이터소스': '*', '키워드': keyword_to_add, '계정ID': row['계정ID']}
                                updated_rules = pd.concat([data["RULES"], pd.DataFrame([new_rule])], ignore_index=True).drop_duplicates()
                                if update_sheet(SHEET_NAMES["RULES"], updated_rules):
                                    st.success(f"✅ 규칙 추가 완료: '{keyword_to_add}'")
                                    st.session_state.workbench_data = auto_categorize(st.session_state.workbench_data, updated_rules)
                                    st.rerun()
        st.markdown("---")
        if st.button("완료하고 돌아가기", use_container_width=True):
            for key in ['current_step', 'df_auto', 'df_manual_workbench', 'workbench_data']:
                if key in st.session_state:
                    del st.session_state[key]
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
            for key in list(st.session_state.keys()):
                if key != 'logged_in': del st.session_state[key]
            st.cache_data.clear(); st.rerun()
        if st.sidebar.button("로그아웃"): st.session_state.clear(); st.rerun()
            
        if choice == "📅 월별 정산표": render_pnl_page(data)
        elif choice == "✍️ 데이터 관리": render_data_page(data)
        elif choice == "⚙️ 설정 관리": render_settings_page(data)

if __name__ == "__main__":
    main()

