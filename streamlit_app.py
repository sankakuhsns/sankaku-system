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
    "INVENTORY": "월별재고"
}

# =============================================================================
# ★★★ 자동분류 규칙 (코드에서 직접 관리) ★★★
# =============================================================================
# 사용자가 직접 이 부분을 수정하여 규칙을 추가/변경할 수 있습니다.
# 형식: {"keyword": "찾을 단어", "account_id": "매핑할 계정ID"}
AUTO_CATEGORIZE_RULES = [
    {"keyword": "배달의민족", "account_id": "EXP-04"}, # 예시: 플랫폼수수료
    {"keyword": "쿠팡이츠", "account_id": "EXP-04"}, # 예시: 플랫폼수수료
    {"keyword": "나이스정보통신", "account_id": "SALE-01"}, # 예시: 카드매출
    {"keyword": "한국전력", "account_id": "EXP-02"}, # 예시: 공과금
    {"keyword": "가스", "account_id": "EXP-02"}, # 예시: 공과금
    # 필요에 따라 규칙을 계속 추가하세요.
]


# =============================================================================
# 1. 구글 시트 연결 및 데이터 처리 함수
# =============================================================================
@st.cache_resource
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)

@st.cache_data(ttl=60)
def load_data(sheet_name):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["spreadsheet_key"])
        worksheet = spreadsheet.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        
        # 데이터 타입 정리
        for col in df.columns:
            # 금액 관련 컬럼 숫자형으로 변환
            if 'Amount' in col or 'Value' in col:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            else:
                df[col] = df[col].astype(str).str.strip()
        return df
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 로딩 중 오류: {e}")
        return pd.DataFrame()

def update_sheet(sheet_name, df):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["spreadsheet_key"])
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        df_str = df.astype(str).replace('nan', '')
        worksheet.update([df_str.columns.values.tolist()] + df_str.values.tolist(), value_input_option='USER_ENTERED')
        st.cache_data.clear() # 데이터 변경 후 캐시 초기화
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
def auto_categorize(df, rules):
    """거래내용(Description)을 기반으로 계정ID(Account_ID)를 자동 할당"""
    for index, row in df.iterrows():
        # 이미 계정ID가 있는 경우 건너뛰기
        if pd.notna(row.get('Account_ID')) and row.get('Account_ID') != '':
            continue
        
        description = row['Description']
        for rule in rules:
            if rule['keyword'] in description:
                df.loc[index, 'Account_ID'] = rule['account_id']
                break # 첫 번째 일치하는 규칙 적용 후 중단
    return df

def calculate_pnl(transactions_df, inventory_df, accounts_df, selected_month):
    """선택된 월의 손익계산서(P&L) 데이터프레임 생성"""
    
    # 1. 해당 월 거래내역 필터링
    transactions_df['Date'] = pd.to_datetime(transactions_df['Date'])
    month_trans = transactions_df[transactions_df['Date'].dt.strftime('%Y-%m') == selected_month].copy()
    
    if month_trans.empty:
        return pd.DataFrame(), {} # 데이터 없으면 빈 테이블과 요약 반환
        
    # 2. 계정과목 정보와 병합
    pnl_data = pd.merge(month_trans, accounts_df, on='Account_ID', how='left')
    
    # 3. 대분류/소분류별 집계
    pnl_summary = pnl_data.groupby(['Category_Major', 'Category_Minor'])['Amount'].sum().reset_index()

    # 4. 손익계산서 항목 계산
    sales = pnl_summary[pnl_summary['Category_Major'] == '매출']['Amount'].sum()
    
    # 5. 매출원가 계산
    cogs_items_purchase = pnl_summary[pnl_summary['Category_Major'] == '매출원가']['Amount'].sum()
    
    inventory_month = inventory_df[inventory_df['YearMonth'] == selected_month]
    begin_inv = inventory_month['Begin_Value'].sum()
    end_inv = inventory_month['End_Value'].sum()
    
    cogs = begin_inv + cogs_items_purchase - end_inv
    
    # 6. 최종 P&L 데이터프레임 구성
    gross_profit = sales - cogs
    
    # 기타 비용 항목 추출
    expenses = pnl_summary[~pnl_summary['Category_Major'].isin(['매출', '매출원가'])]
    total_expenses = expenses['Amount'].sum()
    operating_profit = gross_profit - total_expenses
    
    pnl_final = pd.DataFrame([
        {'항목': 'Ⅰ. 총매출', '금액': sales},
        {'항목': 'Ⅱ. 매출원가', '금액': cogs},
        {'항목': 'Ⅲ. 매출총이익', '금액': gross_profit},
    ])

    # 비용 항목 추가
    expense_details = []
    for _, row in expenses.iterrows():
        expense_details.append({'항목': f" - {row['Category_Minor']}", '금액': row['Amount']})

    if expense_details:
        pnl_final = pd.concat([pnl_final, pd.DataFrame([{'항목': 'Ⅳ. 비용', '금액': total_expenses}])], ignore_index=True)
        pnl_final = pd.concat([pnl_final, pd.DataFrame(expense_details)], ignore_index=True)

    pnl_final = pd.concat([pnl_final, pd.DataFrame([{'항목': 'Ⅴ. 영업이익', '금액': operating_profit}])], ignore_index=True)

    # 요약 지표
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
    
    # 년/월 선택
    today = datetime.now()
    month_options = [(today - relativedelta(months=i)).strftime('%Y-%m') for i in range(12)]
    selected_month = st.selectbox("조회할 년/월을 선택하세요.", month_options)
    
    if selected_month:
        pnl_df, metrics = calculate_pnl(data["TRANSACTIONS"], data["INVENTORY"], data["ACCOUNTS"], selected_month)
        
        if pnl_df.empty:
            st.warning(f"{selected_month}에 해당하는 거래 내역이 없습니다.")
            st.info("`거래내역 관리` 메뉴에서 데이터를 업로드하거나 추가해주세요.")
        else:
            # 메트릭 표시
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("총매출", f"{metrics['총매출']:,.0f} 원")
            m2.metric("매출총이익", f"{metrics['매출총이익']:,.0f} 원")
            m3.metric("영업이익", f"{metrics['영업이익']:,.0f} 원")
            m4.metric("영업이익률", f"{metrics['영업이익률']:.1f} %")
            
            # P&L 테이블 표시
            st.dataframe(
                pnl_df.style.format({'금액': '{:,.0f}'}),
                use_container_width=True,
                hide_index=True
            )

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
                
                # 입/출금 처리
                type_method = col3.radio("입/출금 구분 방식", ["단일 금액 컬럼", "입금/출금 컬럼 분리"])
                if type_method == "단일 금액 컬럼":
                    amount_col = col4.selectbox("금액 컬럼", new_df.columns)
                    new_df['Type'] = new_df[amount_col].apply(lambda x: '입금' if x > 0 else '출금')
                    new_df['Amount'] = new_df[amount_col].abs()
                else:
                    deposit_col = col3.selectbox("입금액 컬럼", new_df.columns)
                    withdraw_col = col4.selectbox("출금액 컬럼", new_df.columns)
                    
                    new_df[deposit_col] = pd.to_numeric(new_df[deposit_col], errors='coerce').fillna(0)
                    new_df[withdraw_col] = pd.to_numeric(new_df[withdraw_col], errors='coerce').fillna(0)

                    new_df['Amount'] = new_df[deposit_col] + new_df[withdraw_col]
                    new_df['Type'] = new_df.apply(lambda row: '입금' if row[deposit_col] > 0 else '출금', axis=1)

                # 시스템 형식에 맞게 데이터프레임 변환
                final_upload_df = new_df[[date_col, desc_col, 'Type', 'Amount']].copy()
                final_upload_df.columns = ['Date', 'Description', 'Type', 'Amount']
                final_upload_df['Date'] = pd.to_datetime(final_upload_df['Date']).dt.strftime('%Y-%m-%d')
                final_upload_df['Account_ID'] = ''
                final_upload_df['Transaction_ID'] = [str(uuid.uuid4()) for _ in range(len(final_upload_df))]

                # 자동 분류 적용
                final_upload_df = auto_categorize(final_upload_df, AUTO_CATEGORIZE_RULES)

                if st.button("📈 위 내역 `통합거래` 시트에 추가하기", type="primary"):
                    combined_df = pd.concat([data["TRANSACTIONS"], final_upload_df], ignore_index=True)
                    if update_sheet(SHEET_NAMES["TRANSACTIONS"], combined_df):
                        st.success("새로운 거래내역이 성공적으로 추가되었습니다.")
                        st.rerun()

            except Exception as e:
                st.error(f"파일 처리 중 오류: {e}")


    st.markdown("---")
    st.subheader("📋 전체 거래내역 편집")
    
    # 필터
    show_uncategorized = st.checkbox("미분류 내역만 보기")
    
    editable_df = data["TRANSACTIONS"].copy()
    if show_uncategorized:
        editable_df = editable_df[editable_df['Account_ID'] == ''].copy()
    
    # 계정과목 옵션
    account_options = data["ACCOUNTS"]['Account_ID'].tolist()
    
    edited_df = st.data_editor(
        editable_df,
        column_config={
            "Transaction_ID": st.column_config.TextColumn(disabled=True),
            "Account_ID": st.column_config.SelectboxColumn(
                "계정ID",
                options=account_options,
                required=False,
            )
        },
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic" # 행 추가/삭제 기능 활성화
    )
    
    if st.button("💾 변경사항 저장", type="primary"):
        # 새로 추가된 행에 ID 부여
        new_rows = edited_df[edited_df['Transaction_ID'].isnull() | (edited_df['Transaction_ID'] == '')]
        for i in new_rows.index:
            edited_df.loc[i, 'Transaction_ID'] = str(uuid.uuid4())
            edited_df.loc[i, 'Date'] = pd.to_datetime(edited_df.loc[i, 'Date']).strftime('%Y-%m-%d') if pd.notna(edited_df.loc[i, 'Date']) else datetime.now().strftime('%Y-%m-%d')


        if update_sheet(SHEET_NAMES["TRANSACTIONS"], edited_df):
            st.success("거래내역이 성공적으로 업데이트되었습니다.")
            st.rerun()

def render_settings(data):
    st.header("⚙️ 시스템 설정")
    
    tab1, tab2, tab3 = st.tabs(["계정과목 관리", "월별재고 관리", "시스템 설정"])

    with tab1:
        st.info("손익계산서의 분류 기준이 되는 계정과목을 관리합니다.")
        edited_accounts = st.data_editor(
            data["ACCOUNTS"],
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True
        )
        if st.button("계정과목 저장"):
            if update_sheet(SHEET_NAMES["ACCOUNTS"], edited_accounts):
                st.success("계정과목이 저장되었습니다.")
                st.rerun()

    with tab2:
        st.info("매출원가 계산을 위해 월별 기초/기말 재고액을 입력합니다.")
        edited_inventory = st.data_editor(
            data["INVENTORY"],
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True
        )
        if st.button("월별재고 저장"):
            if update_sheet(SHEET_NAMES["INVENTORY"], edited_inventory):
                st.success("월별재고가 저장되었습니다.")
                st.rerun()
                
    with tab3:
        st.info("관리자 계정, 사업장 이름 등 시스템의 기본 정보를 관리합니다.")
        edited_settings = st.data_editor(
            data["SETTINGS"],
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True
        )
        if st.button("시스템 설정 저장"):
            if update_sheet(SHEET_NAMES["SETTINGS"], edited_settings):
                st.success("시스템 설정이 저장되었습니다.")
                st.rerun()

# =============================================================================
# 5. 메인 실행 로직
# =============================================================================
def main():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    if not st.session_state['logged_in']:
        login_screen()
    else:
        st.sidebar.title(f"{st.session_state['business_name']} 정산")
        
        # 데이터 로딩
        with st.spinner("데이터를 불러오는 중입니다..."):
            data = {
                "SETTINGS": load_data(SHEET_NAMES["SETTINGS"]),
                "ACCOUNTS": load_data(SHEET_NAMES["ACCOUNTS"]),
                "TRANSACTIONS": load_data(SHEET_NAMES["TRANSACTIONS"]),
                "INVENTORY": load_data(SHEET_NAMES["INVENTORY"])
            }
        
        # 네비게이션
        menu = ["📊 대시보드", "🗂️ 거래내역 관리", "⚙️ 설정"]
        choice = st.sidebar.radio("메뉴를 선택하세요.", menu)
        
        st.sidebar.markdown("---")
        if st.sidebar.button("🔃 데이터 새로고침"):
            st.cache_data.clear()
            st.rerun()

        if st.sidebar.button("로그아웃"):
            st.session_state.clear()
            st.rerun()
            
        # 페이지 렌더링
        if choice == "📊 대시보드":
            render_dashboard(data)
        elif choice == "🗂️ 거래내역 관리":
            render_transaction_manager(data)
        elif choice == "⚙️ 설정":
            render_settings(data)

if __name__ == "__main__":
    main()
