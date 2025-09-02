# streamlit_app.py

import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import io
import plotly.express as px

# =============================================================================
# 0. 기본 설정 및 구글 시트 연결
# =============================================================================

# --- Streamlit 페이지 설정 ---
st.set_page_config(page_title="산카쿠 통합 관리 시스템", page_icon="🏢", layout="wide")

# --- 구글 시트 연결 ---
@st.cache_resource
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)

@st.cache_data(ttl=300)
def load_data(sheet_name):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        for col in df.columns:
            if '금액' in col or '평가액' in col:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        return df
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 로딩 오류: {e}")
        return pd.DataFrame()

def update_sheet(sheet_name, df):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        df_str = df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.update([df_str.columns.values.tolist()] + df_str.values.tolist())
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 업데이트 오류: {e}")
        return False

def append_rows(sheet_name, rows_df):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        rows_df_str = rows_df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.append_rows(rows_df_str.values.tolist())
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 행 추가 오류: {e}")
        return False

# --- 헬퍼 함수 ---
def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def extract_okpos_data(uploaded_file):
    st.warning("OKPOS 파일 파싱 로직이 구현되지 않았습니다. (현재는 예시 데이터로 동작)")
    try:
        data = {
            '매출일자': [date(2025, 8, 1), date(2025, 8, 1)], '지점명': ['강남점', '강남점'], 
            '매출유형': ['홀매출', '포장매출'], '금액': [500000, 150000], '요일': ['금요일', '금요일']
        }
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"파일 파싱 중 오류: {e}")
        return pd.DataFrame()

# =============================================================================
# 1. 로그인 화면
# =============================================================================

def login_screen():
    st.title("🏢 산카쿠 통합 관리 시스템")
    st.markdown("---")
    
    users_df = load_data("지점마스터")
    if users_df.empty:
        st.error("'지점마스터' 시트를 확인해주세요.")
        st.stop()

    with st.form("login_form"):
        username = st.text_input("아이디 (지점ID)")
        password = st.text_input("비밀번호", type="password")
        submitted = st.form_submit_button("로그인", use_container_width=True)

        if submitted:
            user_info = users_df[(users_df['지점ID'] == username) & (users_df['지점PW'] == password)]
            if not user_info.empty:
                st.session_state['logged_in'] = True
                st.session_state['user_info'] = user_info.iloc[0].to_dict()
                st.rerun()
            else:
                st.error("아이디 또는 비밀번호가 올바르지 않습니다.")

# =============================================================================
# 2. 지점 (Store) 페이지 기능
# =============================================================================

# streamlit_app.py 파일에서 이 함수를 찾아 아래 코드로 교체하세요.

def render_store_attendance(user_info):
    st.subheader("⏰ 월별 근무기록")
    store_name = user_info['지점명']

    employees_df = load_data("직원마스터")
    store_employees = employees_df[employees_df['소속지점'] == store_name]['이름'].tolist()

    if not store_employees:
        st.warning("먼저 '직원마스터' 시트에 해당 지점의 직원을 등록해주세요.")
        return

    today = date.today()
    options = [(today - relativedelta(months=i)).strftime('%Y년 / %m월') for i in range(12)]
    selected_month_str_display = st.selectbox("근무 기록 년/월 선택", options=options)
    selected_month_str = datetime.strptime(selected_month_str_display, '%Y년 / %m월').strftime('%Y-%m')

    st.markdown("---")
    st.markdown("##### 근무 기록 입력")

    col_config = {
        "일": st.column_config.TextColumn("일 (DD)", max_chars=2, required=True),
        "직원 이름": st.column_config.SelectboxColumn("직원 이름", options=store_employees, required=True),
        "출근 시간": st.column_config.TextColumn("출근 시간 (HHMM)", max_chars=4, required=True),
        "퇴근 시간": st.column_config.TextColumn("퇴근 시간 (HHMM)", max_chars=4, required=True),
    }

    if 'attendance_df' not in st.session_state:
        st.session_state.attendance_df = pd.DataFrame(columns=col_config.keys())

    edited_df = st.data_editor(
        st.session_state.attendance_df, num_rows="dynamic", use_container_width=True,
        column_config=col_config, key="attendance_editor"
    )
    
    if st.button("💾 근무기록 저장하기", use_container_width=True, type="primary"):
        df_to_save = edited_df.dropna(how='all').reset_index(drop=True)
        if not df_to_save.empty:
            preview_entries = []
            is_valid = True
            
            # --- ↓↓↓ 컬럼 이름을 직접 참조하지 않고, 순서(index)로 참조하도록 수정 ---
            date_col = df_to_save.columns[0]       # 첫 번째 열을 '일'로 인식
            name_col = df_to_save.columns[1]       # 두 번째 열을 '이름'으로 인식
            in_time_col = df_to_save.columns[2]    # 세 번째 열을 '출근시간'으로 인식
            out_time_col = df_to_save.columns[3]   # 네 번째 열을 '퇴근시간'으로 인식
            
            for index, row in df_to_save.iterrows():
                row_num = index + 1
                try:
                    day_str = f"{int(row[date_col]):02d}"
                    full_date_str = f"{selected_month_str}-{day_str}"
                    datetime.strptime(full_date_str, '%Y-%m-%d')
                    
                    in_time_str = f"{str(row[in_time_col])[:2]}:{str(row[in_time_col])[2:]}"
                    out_time_str = f"{str(row[out_time_col])[:2]}:{str(row[out_time_col])[2:]}"
                    datetime.strptime(in_time_str, '%H:%M')
                    datetime.strptime(out_time_str, '%H:%M')
                    
                    preview_entries.append({
                        '근무일자': full_date_str, '직원 이름': row[name_col],
                        '출근': in_time_str, '퇴근': out_time_str,
                    })
                except Exception:
                    st.error(f"{row_num}번째 행의 날짜(DD) 또는 시간(HHMM) 입력값이 유효하지 않습니다.")
                    is_valid = False
                    break
            # --- ↑↑↑ 로직 개선 완료 ---

            if is_valid:
                preview_df = pd.DataFrame(preview_entries)
                st.session_state['preview_attendance'] = preview_df
                st.markdown("---")
                st.markdown("##### 📥 저장될 내용 미리보기")
                st.dataframe(preview_df, use_container_width=True, hide_index=True)
        else:
            st.warning("입력된 근무기록이 없습니다.")

    if 'preview_attendance' in st.session_state and not st.session_state['preview_attendance'].empty:
        if st.button("✅ 최종 확정 및 저장", use_container_width=True, type="primary"):
            preview_df = st.session_state['preview_attendance']
            store_name = user_info['지점명']
            log_entries = []
            for _, row in preview_df.iterrows():
                clock_in = f"{row['근무일자']} {row['출근']}:00"
                clock_out = f"{row['근무일자']} {row['퇴근']}:00"
                log_entries.append([datetime.now(), store_name, row['직원 이름'], '출근', clock_in])
                log_entries.append([datetime.now(), store_name, row['직원 이름'], '퇴근', clock_out])

            log_df = pd.DataFrame(log_entries, columns=['기록일시', '지점명', '직원이름', '출/퇴근', '근무시각'])
            if append_rows("출근부_로그", log_df):
                st.success("근무기록이 성공적으로 저장되었습니다.")
                del st.session_state['preview_attendance']
                st.session_state.attendance_df = pd.DataFrame(columns=col_config.keys())
                st.rerun()

def render_store_settlement(user_info):
    st.subheader("💰 정산 및 재고")
    store_name = user_info['지점명']
    
    today = date.today()
    options = [(today - relativedelta(months=i)).strftime('%Y-%m') for i in range(12)]
    
    with st.expander("월말 재고 자산 평가액 입력", expanded=True):
        selected_month_inv = st.selectbox("재고 평가 년/월 선택", options=options, key="inv_month")
        inventory_value = st.number_input("해당 월의 최종 재고 평가액(원)을 입력하세요.", min_value=0, step=10000)

        if st.button("💾 재고액 저장", type="primary"):
            inventory_log_df = load_data("월말재고_로그")
            if '평가년월' in inventory_log_df.columns:
                 inventory_log_df['평가년월'] = pd.to_datetime(inventory_log_df['평가년월']).dt.strftime('%Y-%m')
            
            existing_indices = inventory_log_df[(inventory_log_df['평가년월'] == selected_month_inv) & (inventory_log_df['지점명'] == store_name)].index
            
            if not existing_indices.empty:
                inventory_log_df.loc[existing_indices, ['재고평가액', '입력일시']] = [inventory_value, datetime.now()]
            else:
                new_row = pd.DataFrame([{'평가년월': selected_month_inv, '지점명': store_name, '재고평가액': inventory_value,
                                       '입력일시': datetime.now(), '입력자': user_info['지점ID']}])
                inventory_log_df = pd.concat([inventory_log_df, new_row], ignore_index=True)
            
            if update_sheet("월말재고_로그", inventory_log_df):
                st.success(f"{selected_month_inv}의 재고 평가액이 {inventory_value:,.0f}원으로 저장되었습니다.")

    st.markdown("---")
    st.markdown("##### 🧾 최종 정산표 확인")
    selected_month_pl = st.selectbox("정산표 조회 년/월 선택", options=options, key="pl_month")
    
    sales_log = load_data("매출_로그")
    settlement_log = load_data("일일정산_로그")
    inventory_log = load_data("월말재고_로그")

    if sales_log.empty or settlement_log.empty or inventory_log.empty:
        st.warning("정산표를 생성하기 위한 데이터(매출, 지출, 재고)가 부족합니다.")
        return

    selected_dt = datetime.strptime(selected_month_pl, '%Y-%m')
    prev_month_str = (selected_dt - relativedelta(months=1)).strftime('%Y-%m')

    # 데이터 타입 변환 및 필터링
    sales_log['매출일자'] = pd.to_datetime(sales_log['매출일자'], errors='coerce').dt.strftime('%Y-%m')
    total_sales = sales_log[(sales_log['매출일자'] == selected_month_pl) & (sales_log['지점명'] == store_name)]['금액'].sum()
    
    settlement_log['정산일자'] = pd.to_datetime(settlement_log['정산일자'], errors='coerce').dt.strftime('%Y-%m')
    store_settlement = settlement_log[(settlement_log['정산일자'] == selected_month_pl) & (settlement_log['지점명'] == store_name)]
    
    food_purchase = store_settlement[store_settlement['대분류'] == '식자재']['금액'].sum()
    sga_expenses = store_settlement[store_settlement['대분류'] != '식자재']['금액'].sum()
    
    inventory_log['평가년월'] = pd.to_datetime(inventory_log['평가년월'], errors='coerce').dt.strftime('%Y-%m')
    begin_inv_series = inventory_log[(inventory_log['평가년월'] == prev_month_str) & (inventory_log['지점명'] == store_name)]['재고평가액']
    end_inv_series = inventory_log[(inventory_log['평가년월'] == selected_month_pl) & (inventory_log['지점명'] == store_name)]['재고평가액']

    begin_inv = begin_inv_series.iloc[0] if not begin_inv_series.empty else 0
    end_inv = end_inv_series.iloc[0] if not end_inv_series.empty else 0
    
    cogs = begin_inv + food_purchase - end_inv
    gross_profit = total_sales - cogs
    operating_profit = gross_profit - sga_expenses
    
    summary_data = {
        '항목': ['I. 총매출', 'II. 식자재 원가 (COGS)', 'III. 매출 총이익', 'IV. 판매비와 관리비', 'V. 영업이익'],
        '금액 (원)': [total_sales, cogs, gross_profit, sga_expenses, operating_profit]
    }
    summary_df = pd.DataFrame(summary_data)
    st.table(summary_df.style.format({'금액 (원)': '{:,.0f}'}))

def render_store_employee_info(user_info):
    st.subheader("👥 직원 정보")
    store_name = user_info['지점명']
    
    employees_df = load_data("직원마스터")
    store_employees_df = employees_df[(employees_df['소속지점'] == store_name) & (employees_df['재직상태'] == '재직중')]

    if store_employees_df.empty:
        st.info("등록된 직원 정보가 없습니다.")
        return

    store_employees_df['보건증만료일'] = pd.to_datetime(store_employees_df['보건증만료일'], errors='coerce')
    today = datetime.now()
    
    expiring_soon_list = []
    for _, row in store_employees_df.iterrows():
        if pd.notna(row['보건증만료일']) and today <= row['보건증만료일'] < (today + timedelta(days=30)):
             expiring_soon_list.append(f"- **{row['이름']}**: {row['보건증만료일'].strftime('%Y-%m-%d')} 만료 예정")

    if expiring_soon_list:
        st.warning("🚨 보건증 만료 임박 직원\n" + "\n".join(expiring_soon_list))

    st.markdown("---")
    st.markdown("##### 우리 지점 직원 목록")
    display_cols = ['이름', '직책', '입사일', '연락처', '보건증만료일']
    st.dataframe(store_employees_df[display_cols].astype(str).replace('NaT',''), use_container_width=True, hide_index=True)

# =============================================================================
# 3. 관리자 (Admin) 페이지 기능
# =============================================================================

def render_admin_dashboard():
    st.subheader("📊 통합 대시보드")
    st.info("전체 지점 데이터 종합 대시보드 기능이 여기에 구현될 예정입니다.")

def render_admin_settlement_input():
    st.subheader("✍️ 월별 정산 입력")
    
    stores_df = load_data("지점마스터")
    store_list = stores_df[stores_df['역할'] == 'store']['지점명'].tolist()
    
    today = date.today()
    options = [(today - relativedelta(months=i)).strftime('%Y-%m') for i in range(12)]
    
    col1, col2 = st.columns(2)
    selected_month_str = col1.selectbox("정산 년/월", options=options)
    selected_store = col2.selectbox("대상 지점", store_list)
    
    st.markdown("---")
    
    categories = ['식자재', '인건비', '판매/마케팅비', '고정비', '공과금', '소모품비', '기타비용']
    col_config = {
        "대분류": st.column_config.SelectboxColumn("대분류", options=categories, required=True),
        "중분류": st.column_config.TextColumn("중분류", required=True), "상세내용": st.column_config.TextColumn("상세내용"),
        "금액": st.column_config.NumberColumn("금액 (원)", format="%d", required=True),
    }

    if 'settlement_df' not in st.session_state:
        st.session_state.settlement_df = pd.DataFrame(columns=col_config.keys())

    edited_df = st.data_editor(st.session_state.settlement_df, num_rows="dynamic", use_container_width=True, column_config=col_config)
    
    if st.button("💾 정산 내역 저장", use_container_width=True, type="primary"):
        df_to_save = edited_df.dropna(subset=['대분류','중분류','금액'])
        if not df_to_save.empty:
            df_to_save['입력일시'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df_to_save['정산일자'] = (selected_month_str + "-01")
            df_to_save['지점명'] = selected_store
            df_to_save['입력자'] = st.session_state['user_info']['지점ID']
            
            final_cols = ['입력일시', '정산일자', '지점명', '대분류', '중분류', '상세내용', '금액', '입력자']
            if append_rows("일일정산_로그", df_to_save[final_cols]):
                st.success(f"{selected_store}의 {selected_month_str} 정산 내역이 저장되었습니다.")
                st.session_state.settlement_df = pd.DataFrame(columns=col_config.keys())
                st.rerun()
        else:
            st.warning("입력된 정산 내역이 없습니다.")

def render_admin_employee_management():
    st.subheader("🗂️ 전 직원 관리")
    
    emp_tabs = st.tabs(["직원 정보 관리", "전체 출근부 조회", "보건증 현황"])
    
    with emp_tabs[0]:
        st.markdown("##### 직원 정보 관리 (추가/수정/퇴사처리)")
        employees_df = load_data("직원마스터")
        edited_df = st.data_editor(employees_df, num_rows="dynamic", use_container_width=True)
        if st.button("💾 직원 정보 전체 저장"):
            if update_sheet("직원마스터", pd.DataFrame(edited_df)):
                st.success("직원 정보가 업데이트되었습니다.")

    with emp_tabs[1]:
        st.markdown("##### 전체 출근부 조회")
        attendance_log_df = load_data("출근부_로그")
        st.dataframe(attendance_log_df, use_container_width=True, hide_index=True)
        if not attendance_log_df.empty:
            excel_data = to_excel(attendance_log_df)
            st.download_button("📥 전체 출근부 다운로드", data=excel_data, file_name="전체_출근부.xlsx")

    with emp_tabs[2]:
        st.markdown("##### 보건증 현황")
        st.info("전체 직원의 보건증 현황 기능이 여기에 구현될 예정입니다.")

def render_admin_settings():
    st.subheader("⚙️ 데이터 및 설정")

    with st.expander("📂 OKPOS 데이터 업로드"):
        uploaded_file = st.file_uploader("OKPOS 엑셀 파일을 업로드하세요", type=['xlsx', 'xls'])
        if uploaded_file is not None:
            new_sales_df = extract_okpos_data(uploaded_file)
            
            if not new_sales_df.empty:
                st.markdown("##### 미리보기")
                st.dataframe(new_sales_df.head())
                if st.button("매출 데이터 저장", type="primary"):
                    if append_rows("매출_로그", new_sales_df):
                        st.success("OKPOS 매출 데이터가 성공적으로 추가되었습니다.")
            else:
                st.error("파일에서 데이터를 추출하지 못했습니다.")

    with st.expander("🏢 지점 계정 관리"):
        stores_df = load_data("지점마스터")
        edited_stores_df = st.data_editor(stores_df, num_rows="dynamic", use_container_width=True)
        if st.button("💾 지점 정보 전체 저장"):
            if update_sheet("지점마스터", pd.DataFrame(edited_stores_df)):
                st.success("지점 정보가 업데이트되었습니다.")

# =============================================================================
# 4. 메인 실행 로직
# =============================================================================

if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

if not st.session_state['logged_in']:
    login_screen()
else:
    user_info = st.session_state['user_info']
    role = user_info.get('역할', 'store')
    name = user_info.get('지점명', '사용자')
    
    st.sidebar.success(f"**{name}** ({role})님")
    st.sidebar.markdown("---")
    if st.sidebar.button("로그아웃"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    if role == 'admin':
        st.title("관리자 페이지")
        admin_tabs = st.tabs(["📊 통합 대시보드", "✍️ 월별 정산 입력", "🗂️ 전 직원 관리", "⚙️ 데이터 및 설정"])
        
        with admin_tabs[0]: render_admin_dashboard()
        with admin_tabs[1]: render_admin_settlement_input()
        with admin_tabs[2]: render_admin_employee_management()
        with admin_tabs[3]: render_admin_settings()

    else:
        st.title(f"{name} 지점 페이지")
        store_tabs = st.tabs(["⏰ 월별 근무기록", "💰 정산 및 재고", "👥 직원 정보"])
        
        with store_tabs[0]: render_store_attendance(user_info)
        with store_tabs[1]: render_store_settlement(user_info)
        with store_tabs[2]: render_store_employee_info(user_info)


