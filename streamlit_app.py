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
# 0-1. 헬퍼 함수 (Helper Functions)
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

@st.cache_data(ttl=3600)
def generate_schedule(year, month, employees):
    start_date = date(year, month, 1)
    end_date = start_date + relativedelta(months=1) - timedelta(days=1)
    days_in_month = pd.date_range(start_date, end_date)
    
    timesheet = pd.DataFrame(index=employees['이름'].tolist(), columns=[d.day for d in days_in_month])
    day_map = {'월': 0, '화': 1, '수': 2, '목': 3, '금': 4, '토': 5, '일': 6}

    for _, emp_row in employees.iterrows():
        work_days_map = [day_map.get(d.strip()) for d in emp_row.get('근무요일', '').split(',')]
        try:
            start_time = datetime.strptime(emp_row.get('기본출근', '09:00'), '%H:%M')
            end_time = datetime.strptime(emp_row.get('기본퇴근', '18:00'), '%H:%M')
            duration = (end_time - start_time).total_seconds() / 3600
            if duration < 0: duration += 24
        except:
            duration = 8.0

        for day in days_in_month:
            if day.weekday() in work_days_map:
                timesheet.loc[emp_row['이름'], day.day] = f"{duration:.1f}"
    
    return timesheet.fillna("")

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

# streamlit_app.py 파일에서 이 함수를 찾아 아래 코드로 교체하세요.
# 맨 위에 'import holidays'를 추가해야 합니다.
import holidays

def render_store_attendance(user_info):
    st.subheader("⏰ 월별 근무기록 관리")
    store_name = user_info['지점명']

    # --- 데이터 로딩 ---
    employees_df = load_data("직원마스터")
    store_employees_df = employees_df[(employees_df['소속지점'] == store_name) & (employees_df['재직상태'] == '재직중')]
    if store_employees_df.empty:
        st.warning("먼저 '직원마스터' 시트에 해당 지점의 재직중인 직원을 등록해주세요."); return
    if '근무요일' not in store_employees_df.columns:
        st.error("'직원마스터' 시트에 '근무요일', '기본출근', '기본퇴근' 컬럼을 추가해야 합니다."); return

    # --- 날짜 및 공휴일 설정 ---
    today = date.today()
    options = [(today - relativedelta(months=i)).strftime('%Y년 / %m월') for i in range(12)]
    selected_month_str_display = st.selectbox("관리할 년/월 선택", options=options)
    selected_month = datetime.strptime(selected_month_str_display, '%Y년 / %m월')
    kr_holidays = holidays.KR(years=selected_month.year)

    # --- 1. 월별 근무 현황표 (숫자 달력) ---
    st.markdown("##### 🗓️ **월별 근무 현황표**")
    
    # 상세 근무기록 불러오기
    attendance_detail_df = load_data("근무기록_상세")
    if '근무일자' in attendance_detail_df.columns:
        attendance_detail_df['근무일자'] = pd.to_datetime(attendance_detail_df['근무일자'])
        month_attendance_df = attendance_detail_df[
            (attendance_detail_df['근무일자'].dt.strftime('%Y-%m') == selected_month.strftime('%Y-%m')) &
            (attendance_detail_df['지점명'] == store_name)
        ]
    else:
        month_attendance_df = pd.DataFrame()

    # 근무 현황표(Pivot Table) 생성
    if not month_attendance_df.empty:
        timesheet = month_attendance_df.pivot_table(index='직원이름', columns=month_attendance_df['근무일자'].dt.day, values='총시간', aggfunc='sum')
        timesheet.columns = [f"{col}일" for col in timesheet.columns]
        
        # 주말 및 공휴일 스타일링
        def style_day_columns(df):
            style = pd.DataFrame('', index=df.index, columns=df.columns)
            for day in range(1, selected_month.replace(month=selected_month.month % 12 + 1, day=1).day if selected_month.month != 12 else 32):
                try:
                    current_date = date(selected_month.year, selected_month.month, day)
                    col_name = f"{day}일"
                    if col_name in df.columns:
                        if current_date in kr_holidays:
                            style[col_name] = 'background-color: #ffe0e0' # 공휴일
                        elif current_date.weekday() == 6: # 일요일
                            style[col_name] = 'background-color: #ffefef'
                        elif current_date.weekday() == 5: # 토요일
                            style[col_name] = 'background-color: #f0f5ff'
                except ValueError:
                    continue
            return style
        
        st.dataframe(timesheet.style.apply(style_day_columns, axis=None).format("{:.1f}", na_rep=""), use_container_width=True)
    else:
        st.info(f"{selected_month_str_display}에 등록된 근무기록이 없습니다. 아래에서 상세 기록을 추가해주세요.")

    st.markdown("---")

    # --- 2. 상세 근무 기록 추가/수정 ---
    st.markdown("##### ✍️ **상세 근무 기록 추가 및 수정**")
    st.info("직원의 기본 스케줄을 바탕으로 근무를 추가하거나, 특정 날짜의 근무 유형을 변경할 수 있습니다.")

    with st.form("attendance_detail_form"):
        col1, col2, col3 = st.columns(3)
        emp_name = col1.selectbox("직원 선택", store_employees_df['이름'].tolist())
        work_date = col2.date_input("날짜 선택", selected_month.date())
        work_type = col3.selectbox("근무 유형", ["정상근무", "연장근무", "유급휴가", "무급휴가", "결근"])

        # 직원의 기본 근무시간 자동 제안
        emp_info = store_employees_df[store_employees_df['이름'] == emp_name].iloc[0]
        default_start = emp_info.get('기본출근', '09:00')
        default_end = emp_info.get('기본퇴근', '18:00')

        col4, col5 = st.columns(2)
        start_time_str = col4.text_input("출근 시간 (HH:MM)", default_start)
        end_time_str = col5.text_input("퇴근 시간 (HH:MM)", default_end)
        
        notes = st.text_input("비고 (선택 사항)")
        
        submitted = st.form_submit_button("💾 기록 추가/수정하기", use_container_width=True, type="primary")
        if submitted:
            try:
                start_dt = datetime.strptime(start_time_str, "%H:%M")
                end_dt = datetime.strptime(end_time_str, "%H:%M")
                duration = (end_dt - start_dt).total_seconds() / 3600
                if duration < 0: duration += 24
                
                new_record = pd.DataFrame([{
                    "기록ID": f"{work_date.strftime('%y%m%d')}_{store_name}_{emp_name}",
                    "지점명": store_name, "근무일자": work_date.strftime('%Y-%m-%d'),
                    "직원이름": emp_name, "구분": work_type,
                    "출근시간": start_time_str, "퇴근시간": end_time_str,
                    "총시간": duration, "비고": notes
                }])
                
                # 기존 기록이 있으면 덮어쓰기(수정), 없으면 추가
                if not attendance_detail_df.empty:
                    attendance_detail_df = attendance_detail_df[attendance_detail_df['기록ID'] != new_record['기록ID'].iloc[0]]
                
                final_df = pd.concat([attendance_detail_df, new_record], ignore_index=True)
                
                if update_sheet("근무기록_상세", final_df):
                    st.success(f"{emp_name} 직원의 {work_date.strftime('%Y-%m-%d')} 근무기록이 저장되었습니다.")
                    st.rerun()

            except Exception as e:
                st.error(f"저장 중 오류 발생: {e}. 시간 형식을(HH:MM) 확인해주세요.")
                
    st.markdown("---")
    
    # --- 3. 직원별 근무 시간 집계 ---
    st.markdown("##### 📊 **직원별 근무 시간 집계**")
    if not month_attendance_df.empty:
        summary = month_attendance_df.pivot_table(index='직원이름', columns='구분', values='총시간', aggfunc='sum', fill_value=0)
        st.dataframe(summary.style.format("{:.1f} 시간"), use_container_width=True)
    else:
        st.info("집계할 근무기록이 없습니다.")


def render_store_settlement(user_info):
    st.subheader("💰 정산 및 재고")
    # (이전 코드와 동일)
    st.info("월말 재고 입력 및 정산표 확인 기능이 여기에 구현될 예정입니다.")


def render_store_employee_info(user_info):
    st.subheader("👥 직원 정보 관리")
    # (이전 코드와 동일)
    st.info("신규 직원 등록, 정보 수정/퇴사 처리 기능이 여기에 구현될 예정입니다.")


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
        with store_tabs[1]: render_store_settlement(user_info)
        with store_tabs[2]: render_store_employee_info(user_info)

