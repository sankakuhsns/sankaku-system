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

st.set_page_config(page_title="산카쿠 통합 관리 시스템", page_icon="🏢", layout="wide")

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

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def extract_okpos_data(uploaded_file):
    st.warning("OKPOS 파일 파싱 로직이 구현되지 않았습니다. (현재는 예시 데이터로 동작)")
    try:
        data = {'매출일자': [date(2025, 8, 1)], '지점명': ['강남점'], '매출유형': ['홀매출'], '금액': [500000], '요일': ['금요일']}
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"파일 파싱 중 오류: {e}")
        return pd.DataFrame()

# =============================================================================
# 1. 로그인 화면
# =============================================================================

def login_screen():
    st.title("🏢 산카쿠 통합 관리 시스템")
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
    st.subheader("⏰ 월별 근무기록 관리")
    store_name = user_info['지점명']

    employees_df = load_data("직원마스터")
    store_employees_df = employees_df[(employees_df['소속지점'] == store_name) & (employees_df['재직상태'] == '재직중')]

    if store_employees_df.empty:
        st.warning("먼저 '직원마스터' 시트에 해당 지점의 재직중인 직원을 등록해주세요.")
        return

    if '근무요일' not in store_employees_df.columns:
        st.error("'직원마스터' 시트에 '근무요일', '기본출근', '기본퇴근' 컬럼을 추가해야 합니다.")
        return

    today = date.today()
    options = [(today - relativedelta(months=i)).strftime('%Y년 / %m월') for i in range(12)]
    selected_month_str_display = st.selectbox("관리할 년/월 선택", options=options)
    selected_month = datetime.strptime(selected_month_str_display, '%Y년 / %m월')

    st.markdown("---")

    # --- 1. 월별 근무 시간표 자동 생성 ---
    @st.cache_data(ttl=3600)
    def generate_timesheet(year, month, employees):
        start_date = date(year, month, 1)
        end_date = start_date + relativedelta(months=1) - timedelta(days=1)
        days_in_month = pd.date_range(start_date, end_date)
        
        timesheet = pd.DataFrame(index=employees['이름'].tolist(), columns=[d.day for d in days_in_month])
        day_map = {'월': 0, '화': 1, '수': 2, '목': 3, '금': 4, '토': 5, '일': 6}

        for emp_idx, emp_row in employees.iterrows():
            work_days_map = [day_map.get(d.strip()) for d in emp_row.get('근무요일', '').split(',')]
            try:
                start_time = datetime.strptime(emp_row.get('기본출근', '09:00'), '%H:%M')
                end_time = datetime.strptime(emp_row.get('기본퇴근', '18:00'), '%H:%M')
                duration = (end_time - start_time).total_seconds() / 3600
                if duration < 0: duration += 24 # 야간 근무 처리
            except:
                duration = 8 # 기본값

            for day in days_in_month:
                if day.weekday() in work_days_map:
                    timesheet.loc[emp_row['이름'], day.day] = f"{duration:.1f}"
        
        return timesheet.fillna("")

    schedule_key = f"timesheet_{selected_month.strftime('%Y-%m')}"
    if schedule_key not in st.session_state:
        st.session_state[schedule_key] = generate_schedule(selected_month.year, selected_month.month, store_employees_df)

    # --- 2. 수정 가능한 근무 시간표 UI ---
    st.markdown("##### 🗓️ **월별 근무 현황표 (시간 직접 수정)**")
    st.info("자동 생성된 근무 시간을 확인하고, 휴가/연장근무 등 변경된 시간을 직접 수정하세요. (휴가/결근 시 칸을 비워주세요)")

    edited_timesheet = st.data_editor(
        st.session_state[schedule_key],
        use_container_width=True,
        key=f"editor_{schedule_key}"
    )

    # --- 3. 총 근무시간 계산 및 표시 ---
    total_hours = 0
    for col in edited_timesheet.columns:
        total_hours += pd.to_numeric(edited_timesheet[col], errors='coerce').sum()
    
    st.metric(label=f"**{selected_month_str_display} 예상 총 근무시간**", value=f"{total_hours:.2f} 시간")

    if st.button("✅ 이달 근무기록 최종 확정", use_container_width=True, type="primary"):
        # (향후 저장 로직: 이 편집된 표를 바탕으로 출퇴근 로그를 역산하여 저장)
        st.success("근무기록이 성공적으로 확정되었습니다. (현재는 저장 로직 구현 전)")
        # 실제 저장 로직 구현 시 아래 주석 해제
        # del st.session_state[schedule_key]
        # st.rerun()

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
                new_row = pd.DataFrame([{'평가년월': selected_month_inv, '지점명': store_name, '재고평가액': inventory_value, '입력일시': datetime.now(), '입력자': user_info['지점ID']}])
                inventory_log_df = pd.concat([inventory_log_df, new_row], ignore_index=True)
            
            if update_sheet("월말재고_로그", inventory_log_df):
                st.success(f"{selected_month_inv}의 재고 평가액이 {inventory_value:,.0f}원으로 저장되었습니다.")

    st.markdown("---")
    st.markdown("##### 🧾 최종 정산표 확인")
    selected_month_pl = st.selectbox("정산표 조회 년/월 선택", options=options, key="pl_month")
    
    sales_log = load_data("매출_로그"); settlement_log = load_data("일일정산_로그"); inventory_log = load_data("월말재고_로그")

    if sales_log.empty or settlement_log.empty or inventory_log.empty:
        st.warning("정산표를 생성하기 위한 데이터(매출, 지출, 재고)가 부족합니다."); return

    selected_dt = datetime.strptime(selected_month_pl, '%Y-%m'); prev_month_str = (selected_dt - relativedelta(months=1)).strftime('%Y-%m')

    sales_log['매출일자'] = pd.to_datetime(sales_log['매출일자'], errors='coerce').dt.strftime('%Y-%m')
    settlement_log['정산일자'] = pd.to_datetime(settlement_log['정산일자'], errors='coerce').dt.strftime('%Y-%m')
    inventory_log['평가년월'] = pd.to_datetime(inventory_log['평가년월'], errors='coerce').dt.strftime('%Y-%m')
    
    total_sales = sales_log[(sales_log['매출일자'] == selected_month_pl) & (sales_log['지점명'] == store_name)]['금액'].sum()
    store_settlement = settlement_log[(settlement_log['정산일자'] == selected_month_pl) & (settlement_log['지점명'] == store_name)]
    food_purchase = store_settlement[store_settlement['대분류'] == '식자재']['금액'].sum()
    sga_expenses = store_settlement[store_settlement['대분류'] != '식자재']['금액'].sum()
    
    begin_inv_series = inventory_log[(inventory_log['평가년월'] == prev_month_str) & (inventory_log['지점명'] == store_name)]['재고평가액']
    end_inv_series = inventory_log[(inventory_log['평가년월'] == selected_month_pl) & (inventory_log['지점명'] == store_name)]['재고평가액']

    begin_inv = begin_inv_series.iloc[0] if not begin_inv_series.empty else 0
    end_inv = end_inv_series.iloc[0] if not end_inv_series.empty else 0
    
    cogs = begin_inv + food_purchase - end_inv; gross_profit = total_sales - cogs; operating_profit = gross_profit - sga_expenses
    
    summary_df = pd.DataFrame({'항목': ['I. 총매출', 'II. 식자재 원가 (COGS)', 'III. 매출 총이익', 'IV. 판매비와 관리비', 'V. 영업이익'],
                               '금액 (원)': [total_sales, cogs, gross_profit, sga_expenses, operating_profit]})
    st.table(summary_df.style.format({'금액 (원)': '{:,.0f}'}))

# streamlit_app.py 파일에서 이 함수를 찾아 아래 코드로 교체하세요.

# streamlit_app.py 파일에서 이 함수를 찾아 아래 코드로 교체하세요.

def render_store_employee_info(user_info):
    st.subheader("👥 직원 정보 관리")
    store_name = user_info['지점명']
    
    # 1. 신규 직원 등록 UI (기존과 동일)
    with st.expander("➕ 신규 직원 등록하기"):
        with st.form("new_employee_form", clear_on_submit=True):
            # ... (이전과 동일한 신규 직원 등록 폼 코드) ...
            st.write("새로운 직원의 정보를 입력하세요.")
            col1, col2 = st.columns(2)
            with col1:
                emp_name = st.text_input("이름")
                emp_position = st.text_input("직책", "직원")
                emp_contact = st.text_input("연락처")
                emp_status = st.selectbox("재직상태", ["재직중", "퇴사"])
            with col2:
                emp_start_date = st.date_input("입사일", date.today())
                emp_health_cert_date = st.date_input("보건증만료일", date.today() + timedelta(days=365))
                emp_work_days = st.text_input("근무요일 (예: 월,화,수,목,금)")
            col3, col4 = st.columns(2)
            with col3: emp_start_time = st.text_input("기본출근 (HH:MM)", "09:00")
            with col4: emp_end_time = st.text_input("기본퇴근 (HH:MM)", "18:00")

            submitted = st.form_submit_button("💾 신규 직원 저장")
            if submitted:
                if not emp_name: st.error("직원 이름은 반드시 입력해야 합니다.")
                else:
                    emp_id = f"{store_name.replace('점','')}_{emp_name}_{emp_start_date.strftime('%y%m%d')}"
                    new_employee_data = pd.DataFrame([{"직원ID": emp_id, "이름": emp_name, "소속지점": store_name,
                                                     "직책": emp_position, "입사일": emp_start_date.strftime('%Y-%m-%d'),
                                                     "연락처": emp_contact, "보건증만료일": emp_health_cert_date.strftime('%Y-%m-%d'),
                                                     "재직상태": emp_status, "근무요일": emp_work_days,
                                                     "기본출근": emp_start_time, "기본퇴근": emp_end_time}])
                    if append_rows("직원마스터", new_employee_data):
                        st.success(f"'{emp_name}' 직원의 정보가 성공적으로 등록되었습니다.")
    
    st.markdown("---")
    
    # 2. 직원 정보 수정 및 관리 (st.data_editor로 변경)
    st.markdown("##### 우리 지점 직원 목록 (정보 수정/퇴사 처리)")
    all_employees_df = load_data("직원마스터")
    store_employees_df = all_employees_df[all_employees_df['소속지점'] == store_name].copy()

    if store_employees_df.empty:
        st.info("등록된 직원이 없습니다. 위에서 신규 직원을 등록해주세요."); return

    # 수정 가능한 컬럼 목록
    editable_cols = ['이름', '직책', '연락처', '재직상태', '근무요일', '기본출근', '기본퇴근', '보건증만료일']
    
    # st.data_editor는 키 값으로 인덱스를 사용하므로, 고유 ID를 인덱스로 설정
    store_employees_df.set_index('직원ID', inplace=True)

    edited_df = st.data_editor(
        store_employees_df[editable_cols],
        key="employee_editor",
        use_container_width=True
    )

    if st.button("💾 변경사항 저장", type="primary", use_container_width=True):
        # 원본 데이터프레임에서 수정된 부분만 업데이트
        all_employees_df.set_index('직원ID', inplace=True)
        all_employees_df.update(edited_df)
        all_employees_df.reset_index(inplace=True)
        
        if update_sheet("직원마스터", all_employees_df):
            st.success("직원 정보가 성공적으로 업데이트되었습니다.")
            st.rerun()

    # 3. 보건증 알림 기능 (유지)
    active_employees_df = store_employees_df[store_employees_df['재직상태'] == '재직중']
    active_employees_df['보건증만료일'] = pd.to_datetime(active_employees_df['보건증만료일'], errors='coerce')
    today = datetime.now()
    
    expiring_soon_list = []
    for _, row in active_employees_df.iterrows():
        if pd.notna(row['보건증만료일']) and today <= row['보건증만료일'] < (today + timedelta(days=30)):
             expiring_soon_list.append(f"- **{row['이름']}**: {row['보건증만료일'].strftime('%Y-%m-%d')} 만료 예정")

    if expiring_soon_list:
        st.warning("🚨 보건증 만료 임박 직원\n" + "\n".join(expiring_soon_list))

# =============================================================================
# 3. 관리자 (Admin) 페이지 기능
# =============================================================================

def render_admin_dashboard():
    st.subheader("📊 통합 대시보드")
    st.info("전체 지점 데이터 종합 대시보드 기능이 여기에 구현될 예정입니다.")

def render_admin_settlement_input():
    st.subheader("✍️ 월별 정산 입력")
    # (이전 코드와 동일)
    st.info("월별/지점별 지출 내역 입력 기능이 여기에 구현될 예정입니다.")

def render_admin_employee_management():
    st.subheader("🗂️ 전 직원 관리")
    # (이전 코드와 동일)
    st.info("전체 직원 정보, 출근부, 보건증 현황 관리 기능이 여기에 구현될 예정입니다.")

def render_admin_settings():
    st.subheader("⚙️ 데이터 및 설정")
    # (이전 코드와 동일)
    st.info("OKPOS 파일 업로드, 지점 계정 관리 기능이 여기에 구현될 예정입니다.")


# =============================================================================
# 4. 메인 실행 로직
# =============================================================================

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False

if not st.session_state['logged_in']:
    login_screen()
else:
    user_info = st.session_state['user_info']
    role = user_info.get('역할', 'store'); name = user_info.get('지점명', '사용자')
    
    st.sidebar.success(f"**{name}** ({role})님")
    st.sidebar.markdown("---")
    if st.sidebar.button("로그아웃"):
        for key in list(st.session_state.keys()): del st.session_state[key]
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






