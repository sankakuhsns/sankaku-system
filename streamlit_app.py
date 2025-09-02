# streamlit_app.py

import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
import io

# =============================================================================
# 0. 기본 설정 및 구글 시트 연결
# =============================================================================

# --- Streamlit 페이지 설정 ---
st.set_page_config(page_title="산카쿠 통합 관리 시스템", page_icon="🏢", layout="wide")

# --- 구글 시트 연결 ---
@st.cache_resource
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    return gspread.authorize(creds)

@st.cache_data(ttl=300) # 5분마다 데이터 새로고침
def load_data(sheet_name):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        # 숫자나 날짜 컬럼 형식 변환 (필요 시)
        for col in df.columns:
            if '금액' in col or '평가액' in col:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            if '일자' in col or '일시' in col or '년월' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 로딩 오류: {e}")
        return pd.DataFrame()

def update_sheet(sheet_name, df):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        # nan 값을 빈 문자열로 변환
        df_str = df.astype(str).replace('nan', '')
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
        # nan 값을 빈 문자열로 변환
        rows_df_str = rows_df.astype(str).replace('nan', '')
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
    # 이 함수는 제공해주신 OKPOS 분석 코드의 파싱 로직을 그대로 가져와 사용하시면 됩니다.
    # 아래는 예시 데이터프레임을 반환하는 코드입니다.
    st.warning("OKPOS 파일 파싱 로직이 구현되지 않았습니다. (현재는 예시 데이터로 동작)")
    data = {
        '매출일자': [date(2025, 8, 1), date(2025, 8, 1)],
        '지점명': ['강남점', '강남점'], # 파일명이나 내용에서 지점명 추출 필요
        '매출유형': ['홀매출', '포장매출'],
        '금액': [500000, 150000],
        '요일': ['금요일', '금요일']
    }
    return pd.DataFrame(data)


# =============================================================================
# 1. 로그인 화면
# =============================================================================

def login_screen():
    st.title("🏢 산카쿠 통합 관리 시스템")
    st.markdown("---")
    
    users_df = load_data("지점마스터")
    if users_df.empty:
        st.error("'지점마스터' 시트 또는 시트 내용을 확인해주세요.")
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

def render_store_attendance(user_info):
    st.subheader("⏰ 월별 근무기록")
    store_name = user_info['지점명']

    employees_df = load_data("직원마스터")
    store_employees = employees_df[employees_df['소속지점'] == store_name]['이름'].tolist()

    # 년/월 선택
    today = date.today()
    selected_month_str = st.selectbox("근무 기록 년/월 선택", 
                                      options=[f"{today.year}-{m:02d}" for m in range(1, 13)],
                                      index=today.month - 1)
    
    st.markdown("---")
    st.markdown("##### 근무 기록 입력")

    # 입력용 데이터프레임 생성
    if 'attendance_df' not in st.session_state:
        st.session_state.attendance_df = pd.DataFrame(columns=["근무일자", "직원 이름", "출근 시간", "퇴근 시간"])

    edited_df = st.data_editor(
        st.session_state.attendance_df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "근무일자": st.column_config.DateColumn("근무일자", format="YYYY-MM-DD", required=True),
            "직원 이름": st.column_config.SelectboxColumn("직원 이름", options=store_employees, required=True),
            "출근 시간": st.column_config.TextColumn("출근 시간 (HH:MM)", required=True),
            "퇴근 시간": st.column_config.TextColumn("퇴근 시간 (HH:MM)", required=True),
        }
    )

    col1, col2 = st.columns(2)
    if col1.button("💾 근무기록 저장", use_container_width=True, type="primary"):
        if not edited_df.dropna().empty:
            log_entries = []
            for _, row in edited_df.dropna().iterrows():
                try:
                    # 출근 기록
                    clock_in_time = datetime.strptime(f"{row['근무일자'].strftime('%Y-%m-%d')} {row['출근 시간']}", "%Y-%m-%d %H:%M")
                    log_entries.append([datetime.now(), store_name, row['직원 이름'], '출근', clock_in_time])
                    # 퇴근 기록
                    clock_out_time = datetime.strptime(f"{row['근무일자'].strftime('%Y-%m-%d')} {row['퇴근 시간']}", "%Y-%m-%d %H:%M")
                    log_entries.append([datetime.now(), store_name, row['직원 이름'], '퇴근', clock_out_time])
                except ValueError:
                    st.error("시간 형식이 올바르지 않습니다 (HH:MM 형식으로 입력).")
                    log_entries = []
                    break
            
            if log_entries:
                log_df = pd.DataFrame(log_entries, columns=['기록일시', '지점명', '직원이름', '출/퇴근', '근무시각'])
                if append_rows("출근부_로그", log_df):
                    st.success("근무기록이 성공적으로 저장되었습니다.")
                    st.session_state.attendance_df = pd.DataFrame(columns=["근무일자", "직원 이름", "출근 시간", "퇴근 시간"]) # 입력창 초기화
                    st.rerun()
        else:
            st.warning("입력된 근무기록이 없습니다.")

    if col2.button("🔄 새로고침", use_container_width=True):
        st.rerun()

def render_store_settlement(user_info):
    st.subheader("💰 정산 및 재고")
    store_name = user_info['지점명']

    with st.expander("월말 재고 자산 평가액 입력", expanded=True):
        today = date.today()
        selected_month_str = st.selectbox("재고 평가 년/월 선택", 
                                          options=[f"{today.year}-{m:02d}" for m in range(1, 13)],
                                          index=today.month - 1)
        
        inventory_value = st.number_input("해당 월의 최종 재고 평가액(원)을 입력하세요.", min_value=0, step=10000)

        if st.button("💾 재고액 저장", type="primary"):
            inventory_log_df = load_data("월말재고_로그")
            
            # 기존 데이터가 있으면 업데이트, 없으면 추가
            existing_entry = inventory_log_df[(inventory_log_df['평가년월'].dt.strftime('%Y-%m') == selected_month_str) & 
                                              (inventory_log_df['지점명'] == store_name)]
            
            if not existing_entry.empty:
                inventory_log_df.loc[existing_entry.index, '재고평가액'] = inventory_value
                inventory_log_df.loc[existing_entry.index, '입력일시'] = datetime.now()
            else:
                new_row = pd.DataFrame([{
                    '평가년월': pd.to_datetime(selected_month_str + "-01"),
                    '지점명': store_name,
                    '재고평가액': inventory_value,
                    '입력일시': datetime.now(),
                    '입력자': user_info['지점ID']
                }])
                inventory_log_df = pd.concat([inventory_log_df, new_row], ignore_index=True)

            if update_sheet("월말재고_로그", inventory_log_df):
                st.success(f"{selected_month_str}의 재고 평가액이 {inventory_value:,.0f}원으로 저장되었습니다.")

    st.markdown("---")
    st.markdown("##### 최종 정산표 확인")
    st.info("관리자가 정산 입력을 완료하면, 이곳에서 최종 손익계산서를 확인할 수 있습니다.")


def render_store_employee_info(user_info):
    st.subheader("👥 직원 정보")
    store_name = user_info['지점명']
    
    employees_df = load_data("직원마스터")
    store_employees_df = employees_df[(employees_df['소속지점'] == store_name) & (employees_df['재직상태'] == '재직중')]

    if store_employees_df.empty:
        st.info("등록된 직원 정보가 없습니다.")
        return

    # 보건증 만료일 체크
    today = datetime.now()
    store_employees_df['보건증만료일'] = pd.to_datetime(store_employees_df['보건증만료일'], errors='coerce')
    
    expiring_soon = store_employees_df[
        store_employees_df['보건증만료일'].between(today, today + timedelta(days=30))
    ]

    if not expiring_soon.empty:
        st.warning("🚨 보건증 만료 임박 직원")
        for _, row in expiring_soon.iterrows():
            st.write(f"- **{row['이름']}**: {row['보건증만료일'].strftime('%Y-%m-%d')} 만료 예정")

    st.markdown("---")
    st.markdown("##### 우리 지점 직원 목록")
    st.dataframe(store_employees_df[['이름', '직책', '입사일', '연락처', '보건증만료일']], use_container_width=True, hide_index=True)


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
    
    emp_tabs = st.tabs(["직원 정보 관리", "전체 출근부 조회", "보건증 현황"])
    
    with emp_tabs[0]:
        st.markdown("##### 직원 정보 관리")
        employees_df = load_data("직원마스터")
        edited_df = st.data_editor(employees_df, num_rows="dynamic", use_container_width=True)
        if st.button("💾 직원 정보 전체 저장", type="primary"):
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
            
            if st.button("매출 데이터 저장"):
                if not new_sales_df.empty:
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
