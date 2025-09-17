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
st.set_page_config(page_title="통합 정산 관리 시스템", page_icon="🏢", layout="wide")

# -- 시트 이름 상수 --
SHEET_NAMES = {
    "SETTINGS": "시스템_설정",
    "LOCATIONS": "사업장_마스터",
    "ACCOUNTS": "계정과목_마스터",
    "RULES": "자동분류_규칙",
    "TRANSACTIONS": "통합거래_원장",
    "INVENTORY": "월별재고_자산"
}

# =============================================================================
# 1. 구글 시트 연결 및 데이터 처리 함수
# =============================================================================
def get_spreadsheet_key():
    try:
        return st.secrets["gcp_service_account"]["SPREADSHEET_KEY"]
    except KeyError:
        try:
            return st.secrets["SPREADSHEET_KEY"]
        except KeyError:
            st.error("Streamlit Secrets에 'SPREADSHEET_KEY'를 찾을 수 없습니다.")
            st.stop()

@st.cache_resource
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)

@st.cache_data(ttl=60)
def load_data(sheet_name):
    try:
        spreadsheet_key = get_spreadsheet_key()
        spreadsheet = get_gspread_client().open_by_key(spreadsheet_key)
        worksheet = spreadsheet.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        # 모든 컬럼을 문자열로 우선 변환하여 공백제거
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
        # 숫자 변환이 필요한 컬럼 지정
        numeric_cols = ['금액', '기말재고액']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce').fillna(0)
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{sheet_name}' 시트를 찾을 수 없습니다. 시트가 생성되었는지, 이름이 정확한지 확인해주세요.")
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
        df_str = df.astype(str).replace('nan', '').replace('NaT', '')
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
    st.title("🏢 통합 정산 관리 시스템")
    settings_df = load_data(SHEET_NAMES["SETTINGS"])
    if settings_df.empty:
        st.error("`시스템_설정` 시트가 비어있습니다. 로그인 정보를 입력해주세요.")
        st.stop()
    try:
        admin_id = settings_df[settings_df['Key'] == 'ADMIN_ID']['Value'].iloc[0]
        admin_pw = settings_df[settings_df['Key'] == 'ADMIN_PW']['Value'].iloc[0]
    except (IndexError, KeyError):
        st.error("`시스템_설정` 시트에 ADMIN_ID 또는 ADMIN_PW Key가 없습니다.")
        st.stop()

    with st.form("login_form"):
        username = st.text_input("아이디")
        password = st.text_input("비밀번호", type="password")
        submitted = st.form_submit_button("로그인", use_container_width=True)
        if submitted:
            if username == admin_id and password == admin_pw:
                st.session_state['logged_in'] = True
                st.rerun()
            else:
                st.error("아이디 또는 비밀번호가 올바르지 않습니다.")

# =============================================================================
# 3. 핵심 로직 함수
# =============================================================================
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
    """선택된 월과 사업장의 손익계산서(P&L) 데이터프레임 생성"""
    # 1. 데이터 필터링
    if selected_location != "전체":
        transactions_df = transactions_df[transactions_df['사업장명'] == selected_location]
        inventory_df = inventory_df[inventory_df['사업장명'] == selected_location]

    transactions_df['거래일자'] = pd.to_datetime(transactions_df['거래일자'])
    month_trans = transactions_df[transactions_df['거래일자'].dt.strftime('%Y-%m') == selected_month].copy()

    if month_trans.empty:
        return pd.DataFrame(), {}

    # 2. 계정과목 정보 병합 및 집계
    pnl_data = pd.merge(month_trans, accounts_df, on='계정ID', how='left')
    pnl_summary = pnl_data.groupby(['대분류', '소분류'])['금액'].sum().reset_index()

    # 3. 손익계산서 항목 계산
    sales = pnl_summary[pnl_summary['대분류'].str.contains('매출', na=False)]['금액'].sum()
    cogs_purchase = pnl_summary[pnl_summary['대분류'].str.contains('원가', na=False)]['금액'].sum()

    # 4. 재고액 계산
    prev_month = (datetime.strptime(selected_month + '-01', '%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m')
    begin_inv = inventory_df[inventory_df['기준년월'] == prev_month]['기말재고액'].sum()
    end_inv = inventory_df[inventory_df['기준년월'] == selected_month]['기말재고액'].sum()
    cogs = begin_inv + cogs_purchase - end_inv
    gross_profit = sales - cogs
    
    # 5. 비용 집계 및 영업이익 계산
    expenses = pnl_summary[~pnl_summary['대분류'].str.contains('매출|원가', na=False)]
    total_expenses = expenses['금액'].sum()
    operating_profit = gross_profit - total_expenses

    # 6. 최종 P&L 데이터프레임 구성
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
def render_dashboard(data):
    st.header("📊 월별 손익(P&L) 대시보드")
    
    col1, col2 = st.columns(2)
    location_list = ["전체"] + data["LOCATIONS"]['사업장명'].tolist()
    selected_location = col1.selectbox("사업장 선택", location_list)
    
    today = datetime.now()
    month_options = [(today - relativedelta(months=i)).strftime('%Y-%m') for i in range(12)]
    selected_month = col2.selectbox("조회 년/월 선택", month_options)
    
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

def render_transaction_manager(data):
    st.header("🗂️ 거래내역 관리")
    
    with st.expander("📥 신규 거래내역(엑셀/CSV) 일괄 업로드"):
        location_list = data["LOCATIONS"]['사업장명'].tolist()
        upload_location = st.selectbox("어느 사업장의 파일인가요?", location_list, key="upload_loc")
        uploaded_file = st.file_uploader("OKPOS, 은행 등 거래내역 파일을 업로드하세요.", type=["csv", "xlsx"])

        if uploaded_file and upload_location:
            try:
                df_raw = pd.read_excel(uploaded_file, engine='openpyxl') if uploaded_file.name.endswith('xlsx') else pd.read_csv(uploaded_file, encoding='cp949')
                st.write("✅ 파일 미리보기 (상위 5개)"); st.dataframe(df_raw.head())
                
                st.info("업로드된 파일의 컬럼을 시스템 컬럼에 맞게 선택해주세요.")
                c1, c2, c3, c4 = st.columns(4)
                date_col = c1.selectbox("거래일자 컬럼", df_raw.columns)
                desc_col = c2.selectbox("거래내용 컬럼", df_raw.columns)
                type_col = c3.selectbox("구분(수익/비용) 컬럼", [None] + list(df_raw.columns))
                amount_col = c4.selectbox("금액 컬럼", df_raw.columns)
                
                # 데이터 정제
                df_processed = df_raw[[date_col, desc_col, amount_col]].copy()
                df_processed.columns = ['거래일자', '거래내용', '금액']
                df_processed['구분'] = df_raw[type_col] if type_col else '비용' # 구분 컬럼 없으면 '비용'으로 간주
                
                # 시스템 형식에 맞게 변환
                df_final = df_processed.dropna(subset=['거래일자', '금액']).copy()
                df_final['거래일자'] = pd.to_datetime(df_final['거래일자'], errors='coerce').dt.strftime('%Y-%m-%d')
                df_final['금액'] = pd.to_numeric(df_final['금액'], errors='coerce')
                df_final['사업장명'] = upload_location
                df_final['데이터소스'] = uploaded_file.name.split('.')[0]
                df_final['처리상태'] = '미분류'
                df_final['계정ID'] = ''
                df_final['거래ID'] = [str(uuid.uuid4()) for _ in range(len(df_final))]
                
                df_final = auto_categorize(df_final, data["RULES"])

                if st.button(f"'{upload_location}'의 거래내역으로 추가하기", type="primary"):
                    combined = pd.concat([data["TRANSACTIONS"], df_final], ignore_index=True)
                    if update_sheet(SHEET_NAMES["TRANSACTIONS"], combined):
                        st.success("새로운 거래내역이 추가되었습니다."); st.rerun()
            except Exception as e:
                st.error(f"파일 처리 중 오류: {e}")

    st.markdown("---")
    st.subheader("📋 전체 거래내역 편집 및 수기 입력")
    
    # 필터
    f1, f2, f3 = st.columns(3)
    filter_loc = f1.selectbox("사업장 필터", ["전체"] + data["LOCATIONS"]["사업장명"].tolist())
    filter_status = f2.selectbox("처리상태 필터", ["전체", "미분류", "자동분류", "수동확인"])
    
    df_editor = data["TRANSACTIONS"].copy()
    if filter_loc != "전체": df_editor = df_editor[df_editor['사업장명'] == filter_loc]
    if filter_status != "전체": df_editor = df_editor[df_editor['처리상태'] == filter_status]
    
    # Data Editor
    edited_df = st.data_editor(df_editor, num_rows="dynamic", use_container_width=True, hide_index=True,
        column_config={
            "거래ID": st.column_config.TextColumn(disabled=True),
            "사업장명": st.column_config.SelectboxColumn("사업장명", options=data["LOCATIONS"]['사업장명'].tolist(), required=True),
            "계정ID": st.column_config.SelectboxColumn("계정ID", options=data["ACCOUNTS"]['계정ID'].tolist(), required=True),
            "처리상태": st.column_config.SelectboxColumn("처리상태", options=["미분류", "수동확인", "자동분류"], required=True)
        })

    if st.button("💾 변경사항 저장", type="primary"):
        # 원본 데이터와 비교하여 변경/추가/삭제된 행 식별
        original_ids = set(data["TRANSACTIONS"]['거래ID'])
        edited_ids = set(edited_df['거래ID'].dropna())
        
        # 새로 추가된 행에 ID 및 기본값 할당
        new_rows_mask = edited_df['거래ID'].isnull() | (edited_df['거래ID'] == '')
        for i in edited_df[new_rows_mask].index:
            edited_df.loc[i, '거래ID'] = str(uuid.uuid4())
            if pd.isna(edited_df.loc[i, '거래일자']): edited_df.loc[i, '거래일자'] = datetime.now().strftime('%Y-%m-%d')
            if pd.isna(edited_df.loc[i, '처리상태']): edited_df.loc[i, '처리상태'] = '수동확인'
        
        # 시트 업데이트
        if update_sheet(SHEET_NAMES["TRANSACTIONS"], edited_df):
            st.success("거래내역이 업데이트되었습니다."); st.rerun()

def render_settings(data):
    st.header("⚙️ 기준정보 관리")
    
    tab1, tab2, tab3, tab4 = st.tabs(["🏢 사업장 관리", "📚 계정과목 관리", "🤖 자동분류 규칙", "📦 월별재고 관리"])

    with tab1:
        st.info("관리할 지점, 공장 등 사업장 목록을 관리합니다.")
        edited_locs = st.data_editor(data["LOCATIONS"], num_rows="dynamic", use_container_width=True, hide_index=True)
        if st.button("사업장 정보 저장"):
            if update_sheet(SHEET_NAMES["LOCATIONS"], edited_locs):
                st.success("사업장 정보가 저장되었습니다."); st.rerun()
    with tab2:
        st.info("정산표의 분류 기준이 되는 계정과목을 관리합니다.")
        edited_accs = st.data_editor(data["ACCOUNTS"], num_rows="dynamic", use_container_width=True, hide_index=True)
        if st.button("계정과목 저장"):
            if update_sheet(SHEET_NAMES["ACCOUNTS"], edited_accs):
                st.success("계정과목이 저장되었습니다."); st.rerun()
    with tab3:
        st.info("거래내용의 키워드를 기반으로 계정과목을 자동 분류하는 규칙을 관리합니다.")
        edited_rules = st.data_editor(data["RULES"], num_rows="dynamic", use_container_width=True, hide_index=True,
            column_config={"계정ID": st.column_config.SelectboxColumn("계정ID", options=data["ACCOUNTS"]['계정ID'].tolist(), required=True)})
        if st.button("자동분류 규칙 저장"):
            if update_sheet(SHEET_NAMES["RULES"], edited_rules):
                st.success("자동분류 규칙이 저장되었습니다."); st.rerun()
    with tab4:
        st.info("매출원가 계산을 위해 사업장별 월말 재고액을 입력합니다.")
        edited_inv = st.data_editor(data["INVENTORY"], num_rows="dynamic", use_container_width=True, hide_index=True,
            column_config={"사업장명": st.column_config.SelectboxColumn("사업장명", options=data["LOCATIONS"]['사업장명'].tolist(), required=True)})
        if st.button("월별재고 저장"):
            if update_sheet(SHEET_NAMES["INVENTORY"], edited_inv):
                st.success("월별재고가 저장되었습니다."); st.rerun()
                
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
        
        menu = ["📊 대시보드", "🗂️ 거래내역 관리", "⚙️ 기준정보 관리"]
        choice = st.sidebar.radio("메뉴를 선택하세요.", menu)
        
        st.sidebar.markdown("---")
        if st.sidebar.button("🔃 데이터 새로고침"):
            st.cache_data.clear(); st.rerun()
        if st.sidebar.button("로그아웃"):
            st.session_state.clear(); st.rerun()
            
        if choice == "📊 대시보드":
            render_dashboard(data)
        elif choice == "🗂️ 거래내역 관리":
            render_transaction_manager(data)
        elif choice == "⚙️ 기준정보 관리":
            render_settings(data)

if __name__ == "__main__":
    main()
