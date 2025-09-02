# streamlit_app.py

import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import io
import plotly.express as px
import holidays

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
            if '금액' in col or '평가액' in col or '총시간' in col:
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
        st.error(f"'{sheet_name}' 시트 업데이트 오류: {e}"); return False

def append_rows(sheet_name, rows_df):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        rows_df_str = rows_df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.append_rows(rows_df_str.values.tolist())
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 행 추가 오류: {e}"); return False

# =============================================================================
# 0-1. 헬퍼 함수
# =============================================================================

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
        st.error(f"파일 파싱 중 오류: {e}"); return pd.DataFrame()

# =============================================================================
# 1. 로그인 화면
# =============================================================================

def login_screen():
    st.title("🏢 산카쿠 통합 관리 시스템")
    users_df = load_data("지점마스터")
    if users_df.empty:
        st.error("'지점마스터' 시트를 확인해주세요."); st.stop()

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

def render_store_attendance(user_info):
    st.subheader("⏰ 월별 근무기록 관리")
    store_name = user_info['지점명']

    employees_df = load_data("직원마스터")
    store_employees_df = employees_df[(employees_df['소속지점'] == store_name) & (employees_df['재직상태'] == '재직중')]
    if store_employees_df.empty:
        st.warning("먼저 '직원마스터' 시트에 해당 지점의 재직중인 직원을 등록해주세요."); return
    if '근무요일' not in store_employees_df.columns:
        st.error("'직원마스터' 시트에 '근무요일', '기본출근', '기본퇴근' 컬럼을 추가해야 합니다."); return

    today = date.today()
    options = [(today - relativedelta(months=i)).strftime('%Y년 / %m월') for i in range(12)]
    selected_month_str_display = st.selectbox("관리할 년/월 선택", options=options)
    selected_month = datetime.strptime(selected_month_str_display, '%Y년 / %m월')
    kr_holidays = holidays.KR(years=selected_month.year)

    st.markdown("##### 🗓️ **월별 근무 현황표**")
    
    # (1) 기본 스케줄 생성
    default_records = []
    start_date = date(selected_month.year, selected_month.month, 1)
    end_date = start_date + relativedelta(months=1) - timedelta(days=1)
    day_map = {'월': 0, '화': 1, '수': 2, '목': 3, '금': 4, '토': 5, '일': 6}
    for single_date in pd.date_range(start_date, end_date):
        for _, emp in store_employees_df.iterrows():
            work_days = [d.strip() for d in emp.get('근무요일', '').split(',')]
            if single_date.weekday() in [day_map.get(d) for d in work_days]:
                try:
                    start_time = datetime.strptime(emp.get('기본출근', '09:00'), '%H:%M')
                    end_time = datetime.strptime(emp.get('기본퇴근', '18:00'), '%H:%M')
                    duration = (end_time - start_time).total_seconds() / 3600
                    if duration < 0: duration += 24
                    default_records.append({"기록ID": f"{single_date.strftime('%y%m%d')}_{store_name}_{emp['이름']}", "지점명": store_name, "근무일자": single_date.strftime('%Y-%m-%d'), "직원이름": emp['이름'], "구분": "정상근무", "출근시간": emp.get('기본출근', '09:00'), "퇴근시간": emp.get('기본퇴근', '18:00'), "총시간": duration, "비고": "자동 생성"})
                except: continue
    default_df = pd.DataFrame(default_records)

    # (2) 저장된 상세 기록 불러오기
    attendance_detail_df = load_data("근무기록_상세")
    if '근무일자' in attendance_detail_df.columns and not attendance_detail_df.empty:
        attendance_detail_df['근무일자'] = pd.to_datetime(attendance_detail_df['근무일자'], errors='coerce').dt.strftime('%Y-%m-%d')
        month_attendance_df = attendance_detail_df[(attendance_detail_df['근무일자'].str.startswith(selected_month.strftime('%Y-%m'))) & (attendance_detail_df['지점명'] == store_name)]
    else: month_attendance_df = pd.DataFrame()

    # (3) 기본 스케줄 위에 저장된 기록 덮어쓰기
    final_df = pd.concat([default_df, month_attendance_df]).drop_duplicates(subset=['기록ID'], keep='last').sort_values(by=['근무일자', '직원이름'])
    
    # (4) 근무 현황표(Pivot Table) 생성 및 표시
    if not final_df.empty:
        timesheet = final_df.pivot_table(index='직원이름', columns=pd.to_datetime(final_df['근무일자']).dt.day, values='총시간', aggfunc='sum')
        timesheet.columns = [f"{col}일" for col in timesheet.columns]
        def style_day_columns(df):
            style = pd.DataFrame('', index=df.index, columns=df.columns)
            last_day = (selected_month.replace(day=1) + relativedelta(months=1) - timedelta(days=1)).day
            for day in range(1, last_day + 1):
                try:
                    current_date = date(selected_month.year, selected_month.month, day); col_name = f"{day}일"
                    if col_name in df.columns:
                        if current_date in kr_holidays: style[col_name] = 'background-color: #ffe0e0'
                        elif current_date.weekday() == 6: style[col_name] = 'background-color: #ffefef'
                        elif current_date.weekday() == 5: style[col_name] = 'background-color: #f0f5ff'
                except ValueError: continue
            return style
        st.dataframe(timesheet.style.apply(style_day_columns, axis=None).format("{:.1f}", na_rep=""), use_container_width=True)
    else: st.info(f"{selected_month_str_display}에 대한 근무 스케줄 정보가 없습니다.")
    
    st.markdown("---")
    with st.expander("✍️ **상세 근무 기록 추가 및 수정**"):
        with st.form("attendance_detail_form"):
            col1, col2, col3 = st.columns(3)
            emp_name = col1.selectbox("직원 선택", store_employees_df['이름'].tolist())
            work_date = col2.date_input("날짜 선택", selected_month.date())
            work_type = col3.selectbox("근무 유형", ["정상근무", "연장근무", "유급휴가", "무급휴가", "결근"])
            emp_info = store_employees_df[store_employees_df['이름'] == emp_name].iloc[0]
            default_start = emp_info.get('기본출근', '09:00'); default_end = emp_info.get('기본퇴근', '18:00')
            col4, col5 = st.columns(2)
            start_time_str = col4.text_input("출근 시간 (HH:MM)", default_start)
            end_time_str = col5.text_input("퇴근 시간 (HH:MM)", default_end)
            notes = st.text_input("비고 (선택 사항)")
            submitted = st.form_submit_button("💾 기록 추가/수정하기", use_container_width=True, type="primary")
            if submitted:
                try:
                    start_dt = datetime.strptime(start_time_str, "%H:%M"); end_dt = datetime.strptime(end_time_str, "%H:%M")
                    duration = (end_dt - start_dt).total_seconds() / 3600
                    if duration < 0: duration += 24
                    new_record = pd.DataFrame([{"기록ID": f"{work_date.strftime('%y%m%d')}_{store_name}_{emp_name}", "지점명": store_name, "근무일자": work_date.strftime('%Y-%m-%d'), "직원이름": emp_name, "구분": work_type, "출근시간": start_time_str, "퇴근시간": end_time_str, "총시간": duration, "비고": notes}])
                    if not attendance_detail_df.empty: attendance_detail_df = attendance_detail_df[attendance_detail_df['기록ID'] != new_record['기록ID'].iloc[0]]
                    final_df_to_save = pd.concat([attendance_detail_df, new_record], ignore_index=True)
                    if update_sheet("근무기록_상세", final_df_to_save):
                        st.success(f"{emp_name} 직원의 {work_date.strftime('%Y-%m-%d')} 근무기록이 저장되었습니다."); st.rerun()
                except Exception as e: st.error(f"저장 중 오류 발생: {e}. 시간 형식을(HH:MM) 확인해주세요.")
    st.markdown("---")
    st.markdown("##### 📊 **직원별 근무 시간 집계**")
    if not final_df.empty:
        summary = final_df.pivot_table(index='직원이름', columns='구분', values='총시간', aggfunc='sum', fill_value=0)
        st.dataframe(summary.style.format("{:.1f} 시간"), use_container_width=True)
    else: st.info("집계할 근무기록이 없습니다.")


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
            if '평가년월' in inventory_log_df.columns: inventory_log_df['평가년월'] = pd.to_datetime(inventory_log_df['평가년월']).dt.strftime('%Y-%m')
            existing_indices = inventory_log_df[(inventory_log_df['평가년월'] == selected_month_inv) & (inventory_log_df['지점명'] == store_name)].index
            if not existing_indices.empty:
                inventory_log_df.loc[existing_indices, ['재고평가액', '입력일시']] = [inventory_value, datetime.now()]
            else:
                new_row = pd.DataFrame([{'평가년월': selected_month_inv, '지점명': store_name, '재고평가액': inventory_value, '입력일시': datetime.now(), '입력자': user_info['지점ID']}])
                inventory_log_df = pd.concat([inventory_log_df, new_row], ignore_index=True)
            if update_sheet("월말재고_로그", inventory_log_df): st.success(f"{selected_month_inv}의 재고 평가액이 {inventory_value:,.0f}원으로 저장되었습니다.")

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
    
    summary_df = pd.DataFrame({'항목': ['I. 총매출', 'II. 식자재 원가 (COGS)', 'III. 매출 총이익', 'IV. 판매비와 관리비', 'V. 영업이익'], '금액 (원)': [total_sales, cogs, gross_profit, sga_expenses, operating_profit]})
    st.table(summary_df.style.format({'금액 (원)': '{:,.0f}'}))


def render_store_employee_info(user_info):
    st.subheader("👥 직원 정보 관리")
    store_name = user_info['지점명']
    
    with st.expander("➕ 신규 직원 등록하기"):
        with st.form("new_employee_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1: emp_name = st.text_input("이름"); emp_position = st.text_input("직책", "직원"); emp_contact = st.text_input("연락처"); emp_status = st.selectbox("재직상태", ["재직중", "퇴사"])
            with col2: emp_start_date = st.date_input("입사일", date.today()); emp_health_cert_date = st.date_input("보건증만료일", date.today() + timedelta(days=365)); emp_work_days = st.text_input("근무요일 (예: 월,화,수)")
            col3, col4 = st.columns(2)
            with col3: emp_start_time = st.text_input("기본출근 (HH:MM)", "09:00")
            with col4: emp_end_time = st.text_input("기본퇴근 (HH:MM)", "18:00")

            submitted = st.form_submit_button("💾 신규 직원 저장")
            if submitted:
                if not emp_name: st.error("직원 이름은 반드시 입력해야 합니다.")
                else:
                    emp_id = f"{store_name.replace('점','')}_{emp_name}_{emp_start_date.strftime('%y%m%d')}"
                    new_employee_data = pd.DataFrame([{"직원ID": emp_id, "이름": emp_name, "소속지점": store_name, "직책": emp_position, "입사일": emp_start_date.strftime('%Y-%m-%d'), "연락처": emp_contact, "보건증만료일": emp_health_cert_date.strftime('%Y-%m-%d'), "재직상태": emp_status, "근무요일": emp_work_days, "기본출근": emp_start_time, "기본퇴근": emp_end_time}])
                    if append_rows("직원마스터", new_employee_data): st.success(f"'{emp_name}' 직원의 정보가 성공적으로 등록되었습니다.")
    
    st.markdown("---")
    st.markdown("##### 우리 지점 직원 목록 (정보 수정/퇴사 처리)")
    all_employees_df = load_data("직원마스터")
    store_employees_df = all_employees_df[all_employees_df['소속지점'] == store_name].copy()

    if store_employees_df.empty: st.info("등록된 직원이 없습니다."); return

    store_employees_df.set_index('직원ID', inplace=True)
    edited_df = st.data_editor(store_employees_df, key="employee_editor", use_container_width=True)

    if st.button("💾 변경사항 저장", type="primary", use_container_width=True):
        all_employees_df.set_index('직원ID', inplace=True)
        all_employees_df.update(edited_df)
        all_employees_df.reset_index(inplace=True)
        if update_sheet("직원마스터", all_employees_df): st.success("직원 정보가 성공적으로 업데이트되었습니다."); st.rerun()

    active_employees_df = store_employees_df[store_employees_df['재직상태'] == '재직중']
    active_employees_df['보건증만료일'] = pd.to_datetime(active_employees_df['보건증만료일'], errors='coerce')
    today = datetime.now()
    expiring_soon_list = []
    for _, row in active_employees_df.iterrows():
        if pd.notna(row['보건증만료일']) and today <= row['보건증만료일'] < (today + timedelta(days=30)):
             expiring_soon_list.append(f"- **{row['이름']}**: {row['보건증만료일'].strftime('%Y-%m-%d')} 만료 예정")
    if expiring_soon_list: st.warning("🚨 보건증 만료 임박 직원\n" + "\n".join(expiring_soon_list))

# =============================================================================
# 3. 관리자 (Admin) 페이지 기능
# =============================================================================

def render_admin_dashboard():
    st.subheader("📊 통합 대시보드")
    st.info("전체 지점 데이터 종합 대시보드 기능이 여기에 구현될 예정입니다.")

def render_admin_settlement_input():
    st.subheader("✍️ 월별 정산 입력")
    st.info("월별/지점별 지출 내역 입력 기능이 여기에 구현될 예정입니다.")

def render_admin_employee_management():
    st.subheader("🗂️ 전 직원 관리")
    st.info("전체 직원 정보, 출근부, 보건증 현황 관리 기능이 여기에 구현될 예정입니다.")

def render_admin_settings():
    st.subheader("⚙️ 데이터 및 설정")
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
        with store_tabs[1]: render_store
