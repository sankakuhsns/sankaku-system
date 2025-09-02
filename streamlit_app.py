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
    st.subheader("⏰ 월별 근무기록")
    store_name = user_info['지점명']

    employees_df = load_data("직원마스터")
    store_employees_df = employees_df[(employees_df['소속지점'] == store_name) & (employees_df['재직상태'] == '재직중')]

    if store_employees_df.empty:
        st.warning("먼저 '직원마스터' 시트에 해당 지점의 재직중인 직원을 등록해주세요.")
        return

    # --- 1. 스케줄 자동 생성을 위한 준비 ---
    if '근무요일' not in store_employees_df.columns:
        st.error("'직원마스터' 시트에 '근무요일', '기본출근', '기본퇴근' 컬럼을 추가해야 합니다.")
        return

    today = date.today()
    options = [(today - relativedelta(months=i)).strftime('%Y년 / %m월') for i in range(12)]
    selected_month_str_display = st.selectbox("근무 기록 년/월 선택", options=options)
    selected_month = datetime.strptime(selected_month_str_display, '%Y년 / %m월')

    st.markdown("---")
    st.markdown("##### 🗓️ 근무 스케줄 관리")
    st.info("직원의 고정 스케줄을 바탕으로 기본 근무표가 자동 생성됩니다. 휴가, 연장근무 등 변경된 내용만 수정하세요.")

    # --- 2. 월별 기본 근무표 자동 생성 로직 ---
    @st.cache_data(ttl=3600) # 스케줄은 1시간 동안 캐시
    def generate_schedule(year, month, employees):
        schedule_entries = []
        start_date = date(year, month, 1)
        end_date = start_date + relativedelta(months=1) - timedelta(days=1)
        
        day_map = {'월': 0, '화': 1, '수': 2, '목': 3, '금': 4, '토': 5, '일': 6}

        for single_date in pd.date_range(start_date, end_date):
            for _, emp in employees.iterrows():
                work_days = [d.strip() for d in emp.get('근무요일', '').split(',')]
                if single_date.weekday() in [day_map.get(d) for d in work_days]:
                    schedule_entries.append({
                        "일": single_date.day,
                        "직원 이름": emp['이름'],
                        "출근 시간": emp.get('기본출근', '09:00').replace(':', ''),
                        "퇴근 시간": emp.get('기본퇴근', '18:00').replace(':', ''),
                        "비고": ""
                    })
        return pd.DataFrame(schedule_entries)

    # st.data_editor는 상태 유지가 중요하므로 세션 상태 활용
    schedule_key = f"schedule_{selected_month.strftime('%Y-%m')}"
    if schedule_key not in st.session_state:
        st.session_state[schedule_key] = generate_schedule(selected_month.year, selected_month.month, store_employees_df)

    col_config = {
        "일": st.column_config.TextColumn("일 (DD)", max_chars=2, required=True),
        "직원 이름": st.column_config.SelectboxColumn("직원 이름", options=store_employees_df['이름'].tolist(), required=True),
        "출근 시간": st.column_config.TextColumn("출근 시간 (HHMM)", max_chars=4, required=True),
        "퇴근 시간": st.column_config.TextColumn("퇴근 시간 (HHMM)", max_chars=4, required=True),
        "비고": st.column_config.TextColumn("비고"),
    }
    
    final_schedule_df = st.data_editor(
        st.session_state[schedule_key],
        num_rows="dynamic",
        use_container_width=True,
        column_config=col_config,
        key=f"editor_{schedule_key}"
    )

    if st.button("✅ 이달 근무기록 최종 확정", use_container_width=True, type="primary"):
        df_to_save = final_schedule_df.dropna(subset=['일', '직원 이름', '출근 시간', '퇴근 시간']).reset_index(drop=True)
        if not df_to_save.empty:
            log_entries, is_valid = [], True
            for index, row in df_to_save.iterrows():
                try:
                    full_date_str = f"{selected_month.year}-{selected_month.month:02d}-{int(row['일']):02d}"
                    datetime.strptime(full_date_str, '%Y-%m-%d')
                    in_time = f"{str(row['출근 시간'])[:2]}:{str(row['출근 시간'])[2:]}"
                    out_time = f"{str(row['퇴근 시간'])[:2]}:{str(row['퇴근 시간'])[2:]}"
                    datetime.strptime(in_time, '%H:%M'); datetime.strptime(out_time, '%H:%M')
                    
                    log_entries.append([datetime.now(), store_name, row['직원 이름'], '출근', f"{full_date_str} {in_time}:00"])
                    log_entries.append([datetime.now(), store_name, row['직원 이름'], '퇴근', f"{full_date_str} {out_time}:00"])
                except Exception:
                    st.error(f"{index + 1}번째 행의 날짜 또는 시간 형식이 올바르지 않습니다.")
                    is_valid = False; break
            
            if is_valid:
                # !중요! 저장 전, 해당 월의 기존 기록을 삭제하여 중복 방지 (선택적 기능)
                # 이 기능을 구현하려면 gspread의 delete_rows() 등을 사용한 복잡한 로직이 필요.
                # 우선은 append만으로 구현하고, 필요시 고도화.
                log_df = pd.DataFrame(log_entries, columns=['기록일시', '지점명', '직원이름', '출/퇴근', '근무시각'])
                if append_rows("출근부_로그", log_df):
                    st.success(f"{selected_month_str_display} 근무기록이 성공적으로 저장(확정)되었습니다.")
                    del st.session_state[schedule_key] # 저장 후 캐시 삭제
                    st.rerun()
        else:
            st.warning("확정할 근무기록이 없습니다.")

def render_store_settlement(user_info):
    # (이전 코드와 동일)
    st.subheader("💰 정산 및 재고")
    st.info("월말 재고 입력 및 정산표 확인 기능이 여기에 구현될 예정입니다.")

def render_store_employee_info(user_info):
    # (이전 코드와 동일)
    st.subheader("👥 직원 정보")
    st.info("직원 정보 및 보건증 만료일 확인 기능이 여기에 구현될 예정입니다.")


# =============================================================================
# 3. 관리자 (Admin) 페이지 기능
# =============================================================================

def render_admin_dashboard():
    # (이전 코드와 동일)
    st.subheader("📊 통합 대시보드")
    st.info("전체 지점 데이터 종합 대시보드 기능이 여기에 구현될 예정입니다.")

def render_admin_settlement_input():
    # (이전 코드와 동일)
    st.subheader("✍️ 월별 정산 입력")
    st.info("월별/지점별 지출 내역 입력 기능이 여기에 구현될 예정입니다.")

def render_admin_employee_management():
    # (이전 코드와 동일)
    st.subheader("🗂️ 전 직원 관리")
    st.info("전체 직원 정보, 출근부, 보건증 현황 관리 기능이 여기에 구현될 예정입니다.")

def render_admin_settings():
    # (이전 코드와 동일)
    st.subheader("⚙️ 데이터 및 설정")
    st.info("OKPOS 파일 업로드, 지점 계정 관리 기능이 여기에 구현될 예정입니다.")


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


