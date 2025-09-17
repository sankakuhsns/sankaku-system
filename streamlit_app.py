import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime
from dateutil.relativedelta import relativedelta
import uuid

# =============================================================================
# 0. 기본 설정 및 상수 정의
# =============================================================================
st.set_page_config(page_title="간편 월말 정산 시스템", page_icon="💰", layout="wide")

# -- 시트 이름 상수 --
SHEET_NAMES = {
    "SETTINGS": "시스템_설정",
    "ACCOUNTS": "계정과목",
    "TRANSACTIONS": "통합거래",
    "INVENTORY": "월별재고",
    "RULES": "자동분류_규칙"
}

# =============================================================================
# 1. 구글 시트 연결 및 데이터 처리 함수 (★최초 코드로 복원된 부분)
# =============================================================================
@st.cache_resource
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)

def get_spreadsheet_key():
    """최초 코드의 키 검색 방식을 그대로 복원"""
    try:
        # [gcp_service_account] 섹션 안에 SPREADSHEET_KEY가 있는지 먼저 확인
        return st.secrets["gcp_service_account"]["SPREADSHEET_KEY"]
    except KeyError:
        try:
            # 섹션 밖에 SPREADSHEET_KEY가 있는지 확인
            return st.secrets["SPREADSHEET_KEY"]
        except KeyError:
            st.error("Streamlit Secrets에 'SPREADSHEET_KEY'를 찾을 수 없습니다. 키 이름과 위치를 확인해주세요.")
            st.stop()

@st.cache_data(ttl=60)
def load_data(sheet_name):
    try:
        spreadsheet_key = get_spreadsheet_key()
        spreadsheet = get_gspread_client().open_by_key(spreadsheet_key)
        worksheet = spreadsheet.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        
        for col in df.columns:
            if 'Amount' in col or 'Value' in col:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            else:
                df[col] = df[col].astype(str).str.strip()
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{sheet_name}' 시트를 찾을 수 없습니다. 구글 스프레드시트에서 시트 이름을 확인해주세요.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 로딩 중 오류: {e}")
        return pd.DataFrame()

def update_sheet(sheet_name, df):
    try:
        spreadsheet_key = get_spreadsheet_key()
        spreadsheet = get_gspread_client().open_by_key(spreadsheet_key)
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        df_str = df.astype(str).replace('nan', '')
        worksheet.update([df_str.columns.values.tolist()] + df_str.values.tolist(), value_input_option='USER_ENTERED')
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 업데이트 중 오류: {e}")
        return False

# =============================================================================
# 2. 로그인 및 인증
# =============================================================================
def login_screen():
    st.title("💰 간편 월말 정산 시스템")
    settings_df = load_data(SHEET_NAMES["SETTINGS"])
    if settings_df.empty:
        st.error("`시스템_설정` 시트를 찾을 수 없거나 비어있습니다.")
        st.stop()

    try:
        admin_id = settings_df[settings_df['Key'] == 'ADMIN_ID']['Value'].iloc[0]
        admin_pw = settings_df[settings_df['Key'] == 'ADMIN_PW']['Value'].iloc[0]
    except IndexError:
        st.error("`시스템_설정` 시트에 ADMIN_ID 또는 ADMIN_PW가 없습니다.")
        st.info("Key 컬럼에 ADMIN_ID, ADMIN_PW를 추가하고 Value 컬럼에 값을 입력해주세요.")
        st.stop()


    with st.form("login_form"):
        username = st.text_input("아이디")
        password = st.text_input("비밀번호", type="password")
        submitted = st.form_submit_button("로그인", use_container_width=True)

        if submitted:
            if username == admin_id and password == admin_pw:
                st.session_state['logged_in'] = True
                st.session_state['business_name'] = settings_df[settings_df['Key'] == 'BUSINESS_NAME']['Value'].iloc[0]
                st.rerun()
            else:
                st.error("아이디 또는 비밀번호가 올바르지 않습니다.")

# =============================================================================
# 3. 핵심 로직 함수
# =============================================================================
def auto_categorize(df, rules_df):
    """거래내용(Description)을 기반으로 계정ID(Account_ID)를 자동 할당"""
    if rules_df.empty:
        return df

    categorized_df = df.copy()
    
    for index, row in categorized_df.iterrows():
        if pd.notna(row.get('Account_ID')) and row.get('Account_ID') != '':
            continue
        
        description = str(row['Description'])
        for _, rule in rules_df.iterrows():
            keyword = str(rule['Keyword'])
            if keyword and keyword in description:
                categorized_df.loc[index, 'Account_ID'] = rule['Account_ID']
                break
    return categorized_df

def calculate_pnl(transactions_df, inventory_df, accounts_df, selected_month):
    """선택된 월의 손익계산서(P&L) 데이터프레임 생성"""
    
    transactions_df['Date'] = pd.to_datetime(transactions_df['Date'])
    month_trans = transactions_df[transactions_df['Date'].dt.strftime('%Y-%m') == selected_month].copy()
    
    if month_trans.empty:
        return pd.DataFrame(), {}
        
    pnl_data = pd.merge(month_trans, accounts_df, on='Account_ID', how='left')
    pnl_summary = pnl_data.groupby(['Category_Major', 'Category_Minor'])['Amount'].sum().reset_index()
    sales = pnl_summary[pnl_summary['Category_Major'] == '매출']['Amount'].sum()
    
    cogs_items_purchase = pnl_summary[pnl_summary['Category_Major'] == '매출원가']['Amount'].sum()
    
    inventory_month = inventory_df[inventory_df['YearMonth'] == selected_month]
    begin_inv = inventory_month['Begin_Value'].sum()
    end_inv = inventory_month['End_Value'].sum()
    cogs = begin_inv + cogs_items_purchase - end_inv
    
    gross_profit = sales - cogs
    
    expenses = pnl_summary[~pnl_summary['Category_Major'].isin(['매출', '매출원가'])]
    total_expenses = expenses['Amount'].sum()
    operating_profit = gross_profit - total_expenses
    
    pnl_final = pd.DataFrame([
        {'항목': 'Ⅰ. 총매출', '금액': sales},
        {'항목': 'Ⅱ. 매출원가', '금액': cogs},
        {'항목': 'Ⅲ. 매출총이익', '금액': gross_profit},
    ])

    expense_details = []
    for _, row in expenses.iterrows():
        expense_details.append({'항목': f" - {row['Category_Minor']}", '금액': row['Amount']})

    if expense_details:
        pnl_final = pd.concat([pnl_final, pd.DataFrame([{'항목': 'Ⅳ. 비용', '금액': total_expenses}])], ignore_index=True)
        pnl_final = pd.concat([pnl_final, pd.DataFrame(expense_details)], ignore_index=True)

    pnl_final = pd.concat([pnl_final, pd.DataFrame([{'항목': 'Ⅴ. 영업이익', '금액': operating_profit}])], ignore_index=True)

    metrics = {
        "총매출": sales,
        "매출총이익": gross_profit,
        "영업이익": operating_profit,
        "영업이익률": (operating_profit / sales) * 100 if sales > 0 else 0
    }
    
    return pnl_final, metrics

# =============================================================================
# 4. UI 렌더링 함수
# =============================================================================
def render_dashboard(data):
    st.header("📊 월별 손익(P&L) 대시보드")
    
    today = datetime.now()
    month_options = [(today - relativedelta(months=i)).strftime('%Y-%m') for i in range(12)]
    selected_month = st.selectbox("조회할 년/월을 선택하세요.", month_options)
    
    if selected_month:
        pnl_df, metrics = calculate_pnl(data["TRANSACTIONS"], data["INVENTORY"], data["ACCOUNTS"], selected_month)
        
        if pnl_df.empty:
            st.warning(f"{selected_month}에 해당하는 거래 내역이 없습니다.")
            st.info("`거래내역 관리` 메뉴에서 데이터를 업로드하거나 추가해주세요.")
        else:
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("총매출", f"{metrics['총매출']:,.0f} 원")
            m2.metric("매출총이익", f"{metrics['매출총이익']:,.0f} 원")
            m3.metric("영업이익", f"{metrics['영업이익']:,.0f} 원")
            m4.metric("영업이익률", f"{metrics['영업이익률']:.1f} %")
            
            st.dataframe(pnl_df.style.format({'금액': '{:,.0f}'}), use_container_width=True, hide_index=True)

def render_transaction_manager(data):
    st.header("🗂️ 거래내역 관리")
    
    with st.expander("📥 신규 거래내역(엑셀/CSV) 업로드", expanded=False):
        uploaded_file = st.file_uploader("은행, 카드사, 포스 등 거래내역 파일을 업로드하세요.", type=["csv", "xlsx"])
        if uploaded_file:
            try:
                new_df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('xlsx') else pd.read_csv(uploaded_file)
                st.write("✅ 파일 미리보기 (상위 5개)")
                st.dataframe(new_df.head())
                
                st.info("업로드된 파일의 컬럼을 아래 시스템 컬럼에 맞게 선택해주세요.")
                col1, col2, col3, col4 = st.columns(4)
                date_col = col1.selectbox("날짜 컬럼", new_df.columns)
                desc_col = col2.selectbox("거래내용 컬럼", new_df.columns)
                
                type_method = col3.radio("입/출금 구분 방식", ["단일 금액 컬럼", "입금/출금 컬럼 분리"])
                if type_method == "단일 금액 컬럼":
                    amount_col = col4.selectbox("금액 컬럼", new_df.columns)
                    new_df['Type'] = new_df[amount_col].apply(lambda x: '입금' if pd.to_numeric(x, errors='coerce') > 0 else '출금')
                    new_df['Amount'] = pd.to_numeric(new_df[amount_col], errors='coerce').abs()
                else:
                    deposit_col = col3.selectbox("입금액 컬럼", new_df.columns)
                    withdraw_col = col4.selectbox("출금액 컬럼", new_df.columns)
                    new_df[deposit_col] = pd.to_numeric(new_df[deposit_col], errors='coerce').fillna(0)
                    new_df[withdraw_col] = pd.to_numeric(new_df[withdraw_col], errors='coerce').fillna(0)
                    new_df['Amount'] = new_df[deposit_col] + new_df[withdraw_col]
                    new_df['Type'] = new_df.apply(lambda row: '입금' if row[deposit_col] > 0 else '출금', axis=1)

                final_upload_df = new_df[[date_col, desc_col, 'Type', 'Amount']].copy()
                final_upload_df.columns = ['Date', 'Description', 'Type', 'Amount']
                final_upload_df['Date'] = pd.to_datetime(final_upload_df['Date']).dt.strftime('%Y-%m-%d')
                final_upload_df['Account_ID'] = ''
                final_upload_df['Transaction_ID'] = [str(uuid.uuid4()) for _ in range(len(final_upload_df))]

                final_upload_df = auto_categorize(final_upload_df, data["RULES"])

                if st.button("📈 위 내역 `통합거래` 시트에 추가하기", type="primary"):
                    combined_df = pd.concat([data["TRANSACTIONS"], final_upload_df], ignore_index=True)
                    if update_sheet(SHEET_NAMES["TRANSACTIONS"], combined_df):
                        st.success("새로운 거래내역이 성공적으로 추가되었습니다.")
                        st.rerun()

            except Exception as e:
                st.error(f"파일 처리 중 오류: {e}")

    st.markdown("---")
    st.subheader("📋 전체 거래내역 편집")
    
    show_uncategorized = st.checkbox("미분류 내역만 보기")
    
    editable_df = data["TRANSACTIONS"].copy()
    if show_uncategorized:
        editable_df = editable_df[editable_df['Account_ID'].isnull() | (editable_df['Account_ID'] == '')].copy()
    
    account_options = data["ACCOUNTS"]['Account_ID'].tolist()
    
    edited_df = st.data_editor(
        editable_df,
        column_config={
            "Transaction_ID": st.column_config.TextColumn(disabled=True),
            "Account_ID": st.column_config.SelectboxColumn("계정ID", options=account_options, required=False)
        },
        use_container_width=True, hide_index=True, num_rows="dynamic"
    )
    
    if st.button("💾 변경사항 저장", type="primary"):
        new_rows = edited_df[edited_df['Transaction_ID'].isnull() | (edited_df['Transaction_ID'] == '')]
        for i in new_rows.index:
            edited_df.loc[i, 'Transaction_ID'] = str(uuid.uuid4())
            edited_df.loc[i, 'Date'] = pd.to_datetime(edited_df.loc[i, 'Date']).strftime('%Y-%m-%d') if pd.notna(edited_df.loc[i, 'Date']) else datetime.now().strftime('%Y-%m-%d')

        if update_sheet(SHEET_NAMES["TRANSACTIONS"], edited_df):
            st.success("거래내역이 성공적으로 업데이트되었습니다.")
            st.rerun()

def render_settings(data):
    st.header("⚙️ 시스템 설정")
    
    tab1, tab2, tab3, tab4 = st.tabs(["계정과목 관리", "자동분류 규칙 관리", "월별재고 관리", "시스템 설정"])

    with tab1:
        st.info("손익계산서의 분류 기준이 되는 계정과목을 관리합니다.")
        edited_accounts = st.data_editor(data["ACCOUNTS"], num_rows="dynamic", use_container_width=True, hide_index=True)
        if st.button("계정과목 저장"):
            if update_sheet(SHEET_NAMES["ACCOUNTS"], edited_accounts):
                st.success("계정과목이 저장되었습니다."); st.rerun()
    
    with tab2:
        st.info("거래내용의 키워드를 기반으로 계정과목을 자동 분류하는 규칙을 관리합니다.")
        edited_rules = st.data_editor(data["RULES"], num_rows="dynamic", use_container_width=True, hide_index=True,
            column_config={
                "Account_ID": st.column_config.SelectboxColumn("계정ID", options=data["ACCOUNTS"]['Account_ID'].tolist(), required=True)
            })
        if st.button("자동분류 규칙 저장"):
            if update_sheet(SHEET_NAMES["RULES"], edited_rules):
                st.success("자동분류 규칙이 저장되었습니다."); st.rerun()

    with tab3:
        st.info("매출원가 계산을 위해 월별 기초/기말 재고액을 입력합니다.")
        edited_inventory = st.data_editor(data["INVENTORY"], num_rows="dynamic", use_container_width=True, hide_index=True)
        if st.button("월별재고 저장"):
            if update_sheet(SHEET_NAMES["INVENTORY"], edited_inventory):
                st.success("월별재고가 저장되었습니다."); st.rerun()
                
    with tab4:
        st.info("관리자 계정, 사업장 이름 등 시스템의 기본 정보를 관리합니다.")
        edited_settings = st.data_editor(data["SETTINGS"], num_rows="dynamic", use_container_width=True, hide_index=True, disabled=["Key"])
        if st.button("시스템 설정 저장"):
            if update_sheet(SHEET_NAMES["SETTINGS"], edited_settings):
                st.success("시스템 설정이 저장되었습니다."); st.rerun()

# =============================================================================
# 5. 메인 실행 로직
# =============================================================================
def main():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    if not st.session_state['logged_in']:
        login_screen()
    else:
        st.sidebar.title(f"{st.session_state.get('business_name', '정산 시스템')}")
        
        with st.spinner("데이터를 불러오는 중입니다..."):
            data = {
                "SETTINGS": load_data(SHEET_NAMES["SETTINGS"]),
                "ACCOUNTS": load_data(SHEET_NAMES["ACCOUNTS"]),
                "TRANSACTIONS": load_data(SHEET_NAMES["TRANSACTIONS"]),
                "INVENTORY": load_data(SHEET_NAMES["INVENTORY"]),
                "RULES": load_data(SHEET_NAMES["RULES"])
            }
        
        menu = ["📊 대시보드", "🗂️ 거래내역 관리", "⚙️ 설정"]
        choice = st.sidebar.radio("메뉴를 선택하세요.", menu)
        
        st.sidebar.markdown("---")
        if st.sidebar.button("🔃 데이터 새로고침"):
            st.cache_data.clear()
            st.rerun()

        if st.sidebar.button("로그아웃"):
            st.session_state.clear()
            st.rerun()
            
        if choice == "📊 대시보드":
            render_dashboard(data)
        elif choice == "🗂️ 거래내역 관리":
            render_transaction_manager(data)
        elif choice == "⚙️ 설정":
            render_settings(data)

if __name__ == "__main__":
    main()
