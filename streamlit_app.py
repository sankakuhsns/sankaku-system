import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime
from dateutil.relativedelta import relativedelta
import uuid
import io

# =============================================================================
# 0. 기본 설정 및 상수 정의
# =============================================================================
st.set_page_config(page_title="통합 정산 관리 시스템", page_icon="🏢", layout="wide")

# -- 시트 이름 상수 --
SHEET_NAMES = {
    "SETTINGS": "시스템_설정",
    "LOCATIONS": "사업장_마스터",
    "ACCOUNTS": "계정과목_마스터",
    "RULES": "자동분류_규칙",
    "TRANSACTIONS": "통합거래_원장",
    "INVENTORY": "월별재고_자산",
    "FORMATS": "파일_포맷_마스터" # 파일 포맷 인덱스 시트 추가
}

# =============================================================================
# 1. 구글 시트 연결 및 데이터 처리 함수
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
        df = pd.DataFrame(worksheet.get_all_records())
        for col in df.columns: df[col] = df[col].astype(str).str.strip()
        numeric_cols = ['금액', '기말재고액']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce').fillna(0)
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{sheet_name}' 시트를 찾을 수 없습니다. 시트가 생성되었는지, 이름이 정확한지 확인해주세요.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 로딩 중 오류: {e}"); return pd.DataFrame()

def update_sheet(sheet_name, df):
    try:
        spreadsheet_key = get_spreadsheet_key()
        spreadsheet = get_gspread_client().open_by_key(spreadsheet_key)
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        df_str = df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.update([df_str.columns.values.tolist()] + df_str.values.tolist(), value_input_option='USER_ENTERED')
        st.cache_data.clear(); return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 업데이트 중 오류: {e}"); return False

# =============================================================================
# 2. 로그인 및 인증
# =============================================================================
def login_screen():
    st.title("🏢 통합 정산 관리 시스템")
    settings_df = load_data(SHEET_NAMES["SETTINGS"])
    if settings_df.empty: st.error("`시스템_설정` 시트가 비어있습니다."); st.stop()
    
    admin_id_row = settings_df[settings_df['Key'] == 'ADMIN_ID']
    admin_pw_row = settings_df[settings_df['Key'] == 'ADMIN_PW']
    if admin_id_row.empty or admin_pw_row.empty:
        st.error("`시스템_설정` 시트에 ADMIN_ID/PW Key가 없습니다."); st.stop()
    
    admin_id = admin_id_row['Value'].iloc[0]
    admin_pw = admin_pw_row['Value'].iloc[0]

    with st.form("login_form"):
        username = st.text_input("아이디")
        password = st.text_input("비밀번호", type="password")
        submitted = st.form_submit_button("로그인", use_container_width=True)
        if submitted:
            if username == admin_id and password == admin_pw:
                st.session_state['logged_in'] = True; st.rerun()
            else:
                st.error("아이디 또는 비밀번호가 올바르지 않습니다.")

# =============================================================================
# 3. 핵심 로직 함수
# =============================================================================
# auto_categorize, calculate_pnl 함수는 이전과 동일하여 생략...
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
                categorized_df.loc[index, '처리상태'] = '자동분류'
                break
    return categorized_df

def calculate_pnl(transactions_df, inventory_df, accounts_df, selected_month, selected_location):
    if selected_location != "전체":
        transactions_df = transactions_df[transactions_df['사업장명'] == selected_location]
        inventory_df = inventory_df[inventory_df['사업장명'] == selected_location]

    if '거래일자' not in transactions_df.columns: return pd.DataFrame(), {}
        
    transactions_df['거래일자'] = pd.to_datetime(transactions_df['거래일자'])
    month_trans = transactions_df[transactions_df['거래일자'].dt.strftime('%Y-%m') == selected_month].copy()

    if month_trans.empty: return pd.DataFrame(), {}

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

    pnl_final = pd.DataFrame([{'항목': 'Ⅰ. 총매출', '금액': sales}])
    pnl_final = pd.concat([pnl_final, pd.DataFrame([{'항목': 'Ⅱ. 매출원가', '금액': cogs}])], ignore_index=True)
    pnl_final = pd.concat([pnl_final, pd.DataFrame([{'항목': 'Ⅲ. 매출총이익', '금액': gross_profit}])], ignore_index=True)
    
    expense_details = []
    for _, major_cat in expenses.groupby('대분류'):
        major_sum = major_cat['금액'].sum()
        expense_details.append({'항목': f'Ⅳ. {major_cat.iloc[0]["대분류"]}', '금액': major_sum})
        for _, minor_cat in major_cat.iterrows():
            expense_details.append({'항목': f' - {minor_cat["소분류"]}', '금액': minor_cat["금액"]})
    
    if expense_details:
        pnl_final = pd.concat([pnl_final, pd.DataFrame(expense_details)], ignore_index=True)

    pnl_final = pd.concat([pnl_final, pd.DataFrame([{'항목': 'Ⅴ. 영업이익', '금액': operating_profit}])], ignore_index=True)

    metrics = {"총매출": sales, "매출총이익": gross_profit, "영업이익": operating_profit, "영업이익률": (operating_profit / sales) * 100 if sales > 0 else 0}
    return pnl_final, metrics

# =============================================================================
# 4. UI 렌더링 함수
# =============================================================================
def render_pnl_page(data):
    st.header("📅 월별 정산표")
    col1, col2 = st.columns(2)
    if not data["LOCATIONS"].empty and '사업장명' in data["LOCATIONS"].columns:
        location_list = ["전체"] + data["LOCATIONS"]['사업장명'].tolist()
    else:
        location_list = ["전체"]; st.sidebar.warning("`사업장_마스터`에 데이터를 추가해주세요.")
    selected_location = col1.selectbox("사업장 선택", location_list)
    today = datetime.now()
    month_options = [(today - relativedelta(months=i)).strftime('%Y-%m') for i in range(12)]
    selected_month = col2.selectbox("조회 년/월 선택", month_options)
    st.markdown("---")
    if selected_month:
        pnl_df, metrics = calculate_pnl(data["TRANSACTIONS"], data["INVENTORY"], data["ACCOUNTS"], selected_month, selected_location)
        if pnl_df.empty:
            st.warning(f"'{selected_location}'의 {selected_month} 데이터가 없습니다.")
        else:
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("총매출", f"{metrics['총매출']:,.0f} 원")
            m2.metric("매출총이익", f"{metrics['매출총이익']:,.0f} 원")
            m3.metric("영업이익", f"{metrics['영업이익']:,.0f} 원")
            m4.metric("영업이익률", f"{metrics['영업이익률']:.1f} %")
            st.dataframe(pnl_df.style.format({'금액': '{:,.0f}'}), use_container_width=True, hide_index=True)

def render_data_page(data):
    st.header("✍️ 데이터 관리")

    if data["LOCATIONS"].empty or data["ACCOUNTS"].empty or data["FORMATS"].empty:
        st.error("`설정 관리`에서 `사업장`, `계정과목`, `파일 포맷`을 먼저 등록해야 합니다."); st.stop()

    tab1, tab2 = st.tabs(["거래내역 관리 (파일 업로드)", "월별재고 관리"])

    with tab1:
        st.subheader("파일 기반 거래내역 관리")
        
        c1, c2 = st.columns([0.6, 0.4])
        with c1:
            format_list = data["FORMATS"]['포맷명'].tolist()
            selected_format_name = st.selectbox("1. 처리할 파일 포맷을 선택하세요.", format_list)
        
        selected_format = data["FORMATS"][data["FORMATS"]['포맷명'] == selected_format_name].iloc[0]

        with c2:
            st.write(" ") # 줄바꿈용
            # --- 양식 다운로드 ---
            output = io.BytesIO()
            template_df = pd.DataFrame(columns=[
                selected_format['날짜컬럼명'], 
                selected_format['내용컬럼명'], 
                selected_format['금액컬럼명']
            ])
            template_df.to_excel(output, index=False, sheet_name='양식')
            st.download_button(
                label=f"📄 '{selected_format_name}' 양식 다운로드",
                data=output.getvalue(),
                file_name=f"{selected_format_name}_양식.xlsx"
            )

        location_list = data["LOCATIONS"]['사업장명'].tolist()
        upload_location = st.selectbox("2. 데이터를 귀속시킬 사업장을 선택하세요.", location_list)
        uploaded_file = st.file_uploader("3. 해당 포맷의 엑셀 파일을 업로드하세요.", type=["xlsx"])

        if uploaded_file and upload_location and selected_format_name:
            st.markdown("---")
            st.subheader("4. 데이터 처리 및 저장")
            
            try:
                df_raw = pd.read_excel(uploaded_file, engine='openpyxl')
                st.write("✅ 원본 파일 미리보기 (상위 5개)"); st.dataframe(df_raw.head())
                
                # 포맷 마스터를 기준으로 컬럼 이름 매핑
                rename_map = {
                    selected_format['날짜컬럼명']: '거래일자',
                    selected_format['내용컬럼명']: '거래내용',
                    selected_format['금액컬럼명']: '금액'
                }
                df_processed = df_raw.rename(columns=rename_map)

                # 필수 컬럼 확인
                required_cols = ['거래일자', '거래내용', '금액']
                if not all(col in df_processed.columns for col in required_cols):
                    st.error(f"업로드한 파일에 필수 컬럼({', '.join(rename_map.keys())})이 없습니다. `파일 포맷 마스터` 설정을 확인하세요.")
                    st.stop()

                # 데이터 정제
                df_final = df_processed[required_cols].dropna(subset=['거래일자', '금액']).copy()
                df_final['거래일자'] = pd.to_datetime(df_final['거래일자'], errors='coerce').dt.strftime('%Y-%m-%d')
                df_final['금액'] = pd.to_numeric(df_final['금액'], errors='coerce')
                df_final['사업장명'] = upload_location
                df_final['구분'] = selected_format['데이터구분']
                df_final['데이터소스'] = selected_format_name
                df_final['처리상태'] = '미분류'
                df_final['계정ID'] = ''
                df_final['거래ID'] = [str(uuid.uuid4()) for _ in range(len(df_final))]
                
                st.write("✅ 시스템 형식 변환 완료 (미리보기)")
                st.dataframe(df_final.head())

                # --- 조건부 처리 로직 ---
                # 1. 지출(비용) 파일일 경우, 중복 검사 수행
                if selected_format['데이터구분'] == '비용':
                    existing_trans = data["TRANSACTIONS"]
                    # 중복 비교를 위한 유니크 키 생성 (사업장+날짜+내용+금액)
                    existing_trans['unique_key'] = existing_trans['사업장명'] + existing_trans['거래일자'].astype(str) + existing_trans['거래내용'] + existing_trans['금액'].astype(str)
                    df_final['unique_key'] = df_final['사업장명'] + df_final['거래일자'].astype(str) + df_final['거래내용'] + df_final['금액'].astype(str)
                    
                    new_transactions = df_final[~df_final['unique_key'].isin(existing_trans['unique_key'])].drop(columns=['unique_key'])
                    num_duplicates = len(df_final) - len(new_transactions)
                    if num_duplicates > 0:
                        st.warning(f"**{num_duplicates}**건의 중복 거래를 제외했습니다.")
                    
                    df_to_process = new_transactions
                else: # 수익 파일은 중복 검사 없이 진행
                    df_to_process = df_final

                # 2. 자동 분류
                df_processed_final = auto_categorize(df_to_process, data["RULES"])
                num_auto = len(df_processed_final[df_processed_final['처리상태'] == '자동분류'])
                st.info(f"총 **{len(df_processed_final)}**건의 신규 거래 중 **{num_auto}**건이 자동으로 분류되었습니다.")
                
                # 3. 수동 처리 및 저장
                st.write("미분류된 내역의 계정ID를 수동으로 지정한 후 저장하세요.")
                edited_final = st.data_editor(df_processed_final, hide_index=True, use_container_width=True,
                    column_config={"계정ID": st.column_config.SelectboxColumn("계정ID", options=data["ACCOUNTS"]['계정ID'].tolist(), required=True)})

                if st.button("💾 위 내역 `통합거래_원장`에 최종 저장하기", type="primary"):
                    if edited_final['계정ID'].isnull().any() or (edited_final['계정ID'] == '').any():
                        st.error("모든 항목의 `계정ID`를 선택해야 저장이 가능합니다.")
                    else:
                        edited_final['처리상태'] = edited_final.apply(lambda row: '수동확인' if row['처리상태'] == '미분류' else row['처리상태'], axis=1)
                        combined = pd.concat([data["TRANSACTIONS"], edited_final], ignore_index=True)
                        if update_sheet(SHEET_NAMES["TRANSACTIONS"], combined):
                            st.success("새로운 거래내역이 성공적으로 저장되었습니다."); st.rerun()

            except Exception as e:
                st.error(f"파일 처리 중 오류: {e}")

    with tab2:
        # 월별재고 관리 로직은 이전과 동일
        st.subheader("월별재고 관리")
        st.info("매출원가 계산을 위해 사업장별 월말 재고액을 입력합니다.")
        edited_inv = st.data_editor(data["INVENTORY"], num_rows="dynamic", use_container_width=True, hide_index=True,
            column_config={"사업장명": st.column_config.SelectboxColumn("사업장명", options=data["LOCATIONS"]['사업장명'].tolist(), required=True)})
        if st.button("💾 월별재고 저장"):
            if update_sheet(SHEET_NAMES["INVENTORY"], edited_inv):
                st.success("월별재고가 저장되었습니다."); st.rerun()

def render_settings_page(data):
    st.header("⚙️ 설정 관리")
    st.info("시스템의 기준이 되는 정보를 관리합니다. 이곳의 정보는 자주 바뀌지 않습니다.")
    
    tab1, tab2, tab3, tab4 = st.tabs(["🏢 사업장 관리", "📚 계정과목 관리", "🤖 자동분류 규칙", "📄 파일 포맷 관리"])

    with tab1:
        st.subheader("사업장 관리")
        edited_locs = st.data_editor(data["LOCATIONS"], num_rows="dynamic", use_container_width=True, hide_index=True)
        if st.button("사업장 정보 저장"):
            if update_sheet(SHEET_NAMES["LOCATIONS"], edited_locs): st.success("저장되었습니다."); st.rerun()
    with tab2:
        st.subheader("계정과목 관리")
        edited_accs = st.data_editor(data["ACCOUNTS"], num_rows="dynamic", use_container_width=True, hide_index=True)
        if st.button("계정과목 저장"):
            if update_sheet(SHEET_NAMES["ACCOUNTS"], edited_accs): st.success("저장되었습니다."); st.rerun()
    with tab3:
        st.subheader("자동분류 규칙 관리")
        if data["ACCOUNTS"].empty:
            st.warning("`계정과목 관리` 탭에서 계정과목을 먼저 추가해주세요.")
        else:
            edited_rules = st.data_editor(data["RULES"], num_rows="dynamic", use_container_width=True, hide_index=True,
                column_config={"계정ID": st.column_config.SelectboxColumn("계정ID", options=data["ACCOUNTS"]['계정ID'].tolist(), required=True)})
            if st.button("자동분류 규칙 저장"):
                if update_sheet(SHEET_NAMES["RULES"], edited_rules): st.success("저장되었습니다."); st.rerun()
    with tab4:
        st.subheader("파일 포맷 관리")
        st.info("업로드할 엑셀 파일의 포맷(양식) 정보를 관리합니다.")
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
        if st.sidebar.button("🔃 데이터 새로고침"): st.cache_data.clear(); st.rerun()
        if st.sidebar.button("로그아웃"): st.session_state.clear(); st.rerun()
            
        if choice == "📅 월별 정산표": render_pnl_page(data)
        elif choice == "✍️ 데이터 관리": render_data_page(data)
        elif choice == "⚙️ 설정 관리": render_settings_page(data)

if __name__ == "__main__":
    main()
