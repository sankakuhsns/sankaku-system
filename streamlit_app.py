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
        "https://www.googleapis.com/auth/drive"  # <--- 권한 추가된 부분
    ]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    return gspread.authorize(creds)

@st.cache_data(ttl=600)
def load_data(sheet_name):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        return pd.DataFrame(worksheet.get_all_records())
    except gspread.exceptions.APIError as e:
        st.error(f"구글 시트 API 오류가 발생했습니다. (오류: {e})")
        st.error("1. Secrets에 SPREADSHEET_KEY가 올바르게 입력되었는지 확인하세요.")
        st.error("2. 해당 서비스 계정 이메일이 구글 시트 파일에 '편집자'로 공유되었는지 확인하세요.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"'{sheet_name}' 시트를 불러오는 중 오류 발생: {e}")
        return pd.DataFrame()

def update_sheet(sheet_name, df):
    """데이터프레임으로 시트 전체를 업데이트하는 함수"""
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 업데이트 중 오류 발생: {e}")
        return False

def append_rows(sheet_name, rows_df):
    """데이터프레임의 행들을 시트에 추가하는 함수"""
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.append_rows(rows_df.values.tolist())
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트에 행 추가 중 오류 발생: {e}")
        return False

# =============================================================================
# 1. 로그인 화면 및 로직
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

def render_store_attendance(user_info):
    """월별 근무기록 입력 및 조회"""
    st.subheader("⏰ 월별 근무기록")
    
    st.info("지점별 월별 근무기록 입력 기능이 여기에 구현될 예정입니다.")


def render_store_settlement(user_info):
    """월말 재고 입력 및 정산표 확인"""
    st.subheader("💰 정산 및 재고")
    
    st.info("월말 재고 입력 및 정산표 확인 기능이 여기에 구현될 예정입니다.")

def render_store_employee_info(user_info):
    """직원 정보 및 보건증 관리"""
    st.subheader("👥 직원 정보")
    
    st.info("직원 정보 및 보건증 만료일 확인 기능이 여기에 구현될 예정입니다.")

# =============================================================================
# 3. 관리자 (Admin) 페이지 기능
# =============================================================================

def render_admin_dashboard():
    """통합 대시보드"""
    st.subheader("📊 통합 대시보드")
    
    st.info("전체 지점 데이터 종합 대시보드 기능이 여기에 구현될 예정입니다.")

def render_admin_settlement_input():
    """월별 정산 내역 입력"""
    st.subheader("✍️ 월별 정산 입력")

    st.info("월별/지점별 지출 내역 입력 기능이 여기에 구현될 예정입니다.")


def render_admin_employee_management():
    """전 직원 관리"""
    st.subheader("🗂️ 전 직원 관리")

    st.info("전체 직원 정보, 출근부, 보건증 현황 관리 기능이 여기에 구현될 예정입니다.")


def render_admin_settings():
    """OKPOS 업로드 및 시스템 설정"""
    st.subheader("⚙️ 데이터 및 설정")

    st.info("OKPOS 파일 업로드, 지점 계정 관리 기능이 여기에 구현될 예정입니다.")

# =============================================================================
# 4. 메인 실행 로직
# =============================================================================

# 세션 상태 초기화
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

# 로그인 페이지 또는 메인 페이지 표시
if not st.session_state['logged_in']:
    login_screen()
else:
    # --- 사이드바 ---
    user_info = st.session_state['user_info']
    role = user_info.get('역할', 'store')
    name = user_info.get('지점명', '사용자')
    
    st.sidebar.success(f"**{name}** ({role})님")
    st.sidebar.markdown("---")
    if st.sidebar.button("로그아웃"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    # --- 메인 콘텐츠 ---
    if role == 'admin':
        st.title("관리자 페이지")
        admin_tabs = st.tabs(["📊 통합 대시보드", "✍️ 월별 정산 입력", "🗂️ 전 직원 관리", "⚙️ 데이터 및 설정"])
        
        with admin_tabs[0]:
            render_admin_dashboard()
        with admin_tabs[1]:
            render_admin_settlement_input()
        with admin_tabs[2]:
            render_admin_employee_management()
        with admin_tabs[3]:
            render_admin_settings()

    else: # 'store' 역할
        st.title(f"{name} 지점 페이지")
        store_tabs = st.tabs(["⏰ 월별 근무기록", "💰 정산 및 재고", "👥 직원 정보"])
        
        with store_tabs[0]:
            render_store_attendance(user_info)
        with store_tabs[1]:
            render_store_settlement(user_info)
        with store_tabs[2]:
            render_store_employee_info(user_info)
