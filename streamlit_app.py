# app.py

import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

# =============================================================================
# 1. 기본 설정
# =============================================================================

# Streamlit 페이지 설정
st.set_page_config(page_title="산카쿠 통합 관리 시스템", page_icon="📈", layout="wide")

# Google Sheets API와 연결 설정 (이 부분은 한번만 설정하면 됩니다)
@st.cache_resource
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    return gspread.authorize(creds)

# 스프레드시트 열기
@st.cache_resource
def open_spreadsheet(sheet_name):
    # 여기에 본인의 구글 시트 파일 이름을 넣으세요.
    SPREADSHEET_NAME = "산카쿠 통합 정산 시스템" 
    try:
        spreadsheet = get_gspread_client().open(SPREADSHEET_NAME)
        return spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        st.error("스프레드시트를 찾을 수 없습니다. 파일 이름을 확인하세요.")
        st.stop()
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{sheet_name}' 시트를 찾을 수 없습니다. 시트 이름을 확인하세요.")
        st.stop()

# =============================================================================
# 2. 로그인 기능
# =============================================================================

def login():
    st.title("산카쿠 통합 관리 시스템")
    st.markdown("---")

    # 지점마스터 시트에서 사용자 정보 불러오기
    users_sheet = open_spreadsheet("지점마스터")
    users_df = pd.DataFrame(users_sheet.get_all_records())

    with st.form("login_form"):
        username = st.text_input("아이디 (지점ID)")
        password = st.text_input("비밀번호", type="password")
        submitted = st.form_submit_button("로그인")

        if submitted:
            user_info = users_df[(users_df['지점ID'] == username) & (users_df['지점PW'] == password)] # 실제로는 비밀번호 해싱 필요
            if not user_info.empty:
                st.session_state['logged_in'] = True
                st.session_state['user_info'] = user_info.iloc[0].to_dict()
                st.rerun()
            else:
                st.error("아이디 또는 비밀번호가 올바르지 않습니다.")

# =============================================================================
# 3. 메인 애플리케이션 실행 로직
# =============================================================================

# 로그인 상태가 아니면 로그인 화면 표시
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

if not st.session_state['logged_in']:
    login()
else:
    # 로그인 성공 시, 사용자 정보 가져오기
    user_info = st.session_state['user_info']
    role = user_info['역할']
    name = user_info['지점명']

    st.sidebar.success(f"{name} ({role})님, 환영합니다.")
    if st.sidebar.button("로그아웃"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    st.title("📈 산카쿠 통합 관리 시스템")
    st.markdown("---")

    # 역할에 따라 다른 탭 메뉴를 보여줌
    if role == 'admin':
        st.header("관리자 페이지")
        tab1, tab2, tab3, tab4 = st.tabs(["📊 통합 대시보드", "✍️ 월별 정산 입력", "🗂️ 전 직원 관리", "⚙️ 데이터 및 설정"])

        with tab1:
            st.write("여기에 전체 지점 데이터를 종합한 대시보드를 만듭니다.")
        with tab2:
            st.write("여기에 월별/지점별 지출 내역을 입력하는 기능을 만듭니다.")
        with tab3:
            st.write("여기에 전체 직원 정보, 출근부, 보건증 현황을 관리하는 기능을 만듭니다.")
        with tab4:
            st.write("여기에 OKPOS 파일 업로드, 지점 계정 관리 기능을 만듭니다.")

    elif role == 'store':
        st.header(f"{name} 지점 페이지")
        tab1, tab2, tab3 = st.tabs(["⏰ 월별 근무기록", "💰 정산 및 재고", "👥 직원 정보"])

        with tab1:
            st.write("여기에 월별 출근부를 한번에 입력하고 엑셀로 다운로드하는 기능을 만듭니다.")
        with tab2:
            st.write("여기에 월말 재고 자산 평가액을 입력하고, 최종 정산표를 확인하는 기능을 만듭니다.")
        with tab3:
            st.write("여기에 우리 지점 직원 정보와 보건증 만료일을 확인하는 기능을 만듭니다.")
