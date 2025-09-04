import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime, date, time, timedelta
from dateutil.relativedelta import relativedelta
import re
import holidays
import io

# =============================================================================
# 0. 기본 설정 및 상수 정의
# =============================================================================
st.set_page_config(page_title="산카쿠 통합 관리 시스템", page_icon="🏢", layout="wide")

# -- 시트 이름 상수 --
SHEET_NAMES = {
    "STORE_MASTER": "지점마스터", "EMPLOYEE_MASTER": "직원마스터",
    "ATTENDANCE_DETAIL": "근무기록_상세", "INVENTORY_LOG": "월말재고_로그",
    "INVENTORY_MASTER": "재고마스터", "INVENTORY_DETAIL_LOG": "월말재고_상세로그",
    "SALES_LOG": "매출_로그", "SETTLEMENT_LOG": "일일정산_로그",
    "PERSONNEL_TRANSFER_LOG": "인사이동_로그", "SETTLEMENT_LOCK_LOG": "정산_마감_로그",
    "DISPATCH_LOG": "파견_로그", "PERSONNEL_REQUEST_LOG": "인사요청_로그"
}

# -- UI 테마 상수 --
THEME = { "BORDER": "#e8e8ee", "PRIMARY": "#1C6758", "BG": "#f7f8fa", "TEXT": "#222" }

# -- 상태 및 유형 상수 --
STATUS = {
    "EMPLOYEE_ACTIVE": "재직중", "EMPLOYEE_INACTIVE": "퇴사",
    "ATTENDANCE_NORMAL": "정상근무", "ATTENDANCE_OVERTIME": "연장근무",
    "LOCK_REQUESTED": "요청", "LOCK_APPROVED": "승인", "LOCK_REJECTED": "반려",
    "REQUEST_TYPE_TRANSFER": "지점 이동", "REQUEST_TYPE_DISPATCH": "파견"
}

# =============================================================================
# 1. 구글 시트 연결 및 데이터 처리 함수 (안정성 강화)
# =============================================================================
@st.cache_resource
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)

def _get_sheet_key():
    try: return st.secrets["gcp_service_account"]["SPREADSHEET_KEY"]
    except KeyError:
        try: return st.secrets["SPREADSHEET_KEY"]
        except KeyError: raise RuntimeError("SPREADSHEET_KEY가 secrets에 없습니다.")

@st.cache_data(ttl=60)
def load_data(sheet_name):
    try:
        spreadsheet = get_gspread_client().open_by_key(_get_sheet_key())
        worksheet = spreadsheet.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
            
        numeric_cols = ['금액', '평가액', '총시간', '단가', '수량', '소계', '재고평가액']
        for col in df.columns:
            if col in numeric_cols:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{sheet_name}' 시트를 찾을 수 없습니다.")
        return pd.DataFrame()
    except Exception as e:
        if "Quota exceeded" in str(e): st.error("🔌 API 요청 한도를 초과했습니다. 1분 후 새로고침 해주세요.")
        else: st.error(f"'{sheet_name}' 로딩 중 오류: {e}")
        return pd.DataFrame()

def update_sheet(sheet_name, df):
    try:
        spreadsheet = get_gspread_client().open_by_key(_get_sheet_key())
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        df_str = df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.update([df_str.columns.values.tolist()] + df_str.values.tolist(), value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 업데이트 오류: {e}"); return False

def append_rows(sheet_name, rows_df):
    try:
        spreadsheet = get_gspread_client().open_by_key(_get_sheet_key())
        worksheet = spreadsheet.worksheet(sheet_name)
        rows_df_str = rows_df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.append_rows(rows_df_str.values.tolist(), value_input_option='USER_ENTERED', insert_data_option='INSERT_ROWS', table_range='A1')
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 행 추가 오류: {e}"); return False

def update_sheet_and_clear_cache(sheet_name, df):
    if update_sheet(sheet_name, df):
        st.cache_data.clear()
        st.session_state.pop('data_cache', None)
        return True
    return False

def append_rows_and_clear_cache(sheet_name, rows_df):
    if append_rows(sheet_name, rows_df):
        st.cache_data.clear()
        st.session_state.pop('data_cache', None)
        return True
    return False

# =============================================================================
# 2. 데이터 전처리 및 헬퍼 함수
# =============================================================================
def preprocess_dataframes(data_cache):
    """데이터 로드 후 공통 전처리 (성능 최적화)"""
    date_cols_map = {
        "ATTENDANCE_DETAIL": "근무일자",
        "SALES_LOG": "매출일자",
        "SETTLEMENT_LOG": "정산일자",
        "EMPLOYEE_MASTER": "보건증만료일"
    }
    for name, col in date_cols_map.items():
        if name in data_cache and not data_cache[name].empty and col in data_cache[name].columns:
            df = data_cache[name]
            df[f'{col}_dt'] = pd.to_datetime(df[col], errors='coerce')
            if name != "EMPLOYEE_MASTER":
                df['년월'] = df[f'{col}_dt'].dt.strftime('%Y-%m')
    return data_cache

def _format_time_input(time_input):
    s = str(time_input).strip().replace('.', ':')
    if s.isdigit():
        if len(s) <= 2: s = f"{s.zfill(2)}:00"
        elif len(s) == 3: s = f"0{s[0]}:{s[1:]}"
        elif len(s) == 4: s = f"{s[:2]}:{s[2:]}"
    elif ':' in s:
        hh, mm = (s.split(':') + ["0"])[:2]
        s = f"{hh.zfill(2)}:{mm.zfill(2)}"
    return s if re.match(r'^([01]\d|2[0-3]):([0-5]\d)$', s) else None

def _has_overlap(group):
    grp = group.sort_values('start_dt').reset_index(drop=True)
    return any(grp.loc[i, 'end_dt'] > grp.loc[i+1, 'start_dt'] for i in range(len(grp)-1))

def _validate_phone_number(phone):
    return re.match(r'^\d{3}-\d{4}-\d{4}$', str(phone))

def create_excel_report(summary_pivot, display_summary, month_records_df, selected_month_str, store_name):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        summary_pivot.to_excel(writer, sheet_name='월별 근무 현황', startrow=1)
        display_summary.to_excel(writer, sheet_name='근무 시간 집계', index=False, startrow=1)
        if not month_records_df.empty:
            attendance_log = month_records_df[['근무일자', '직원이름', '구분', '출근시간', '퇴근시간', '총시간']].rename(
                columns={'근무일자': '날짜', '직원이름': '이름', '총시간': '근무시간(h)'}
            ).sort_values(by=['날짜', '이름'])
            attendance_log.to_excel(writer, sheet_name='출근부', index=False, startrow=1)
            
        workbook = writer.book
        title_format = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'left'})
        header_format = workbook.add_format({'bold': True, 'valign': 'top', 'fg_color': '#DDEBF7', 'border': 1, 'align': 'center'})
        
        sheets_config = {
            '월별 근무 현황': {'df': summary_pivot, 'cols': [('A:A', 12), ('B:AF', 5)], 'index': True},
            '근무 시간 집계': {'df': display_summary, 'cols': [('A:D', 15)], 'index': False},
            '출근부': {'df': attendance_log, 'cols': [('A:A', 12), ('B:B', 12), ('C:F', 10)], 'index': False}
        }

        for sheet_name, config in sheets_config.items():
            if sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                title = f"{selected_month_str.replace('-', '.')} {sheet_name}"
                worksheet.write('A1', title, title_format)
                for col_range, width in config['cols']:
                    worksheet.set_column(col_range, width)
                
                header_offset = 1 if config['index'] else 0
                if config['index']:
                    worksheet.write(1, 0, '직원이름', header_format)

                for col_num, value in enumerate(config['df'].columns.values):
                     worksheet.write(1, col_num + header_offset, value, header_format)

    return output.getvalue()

def check_health_cert_expiration(user_info, all_employees_df):
    if all_employees_df.empty or '보건증만료일_dt' not in all_employees_df.columns: return

    store_name = user_info['지점명']
    store_employees_df = all_employees_df[
        (all_employees_df['소속지점'] == store_name) & 
        (all_employees_df['재직상태'] == STATUS["EMPLOYEE_ACTIVE"])
    ]

    if store_employees_df.empty: return
    
    today = pd.to_datetime(date.today())
    expiring_soon_list = [
        f"- **{row['이름']}**: {row['보건증만료일_dt'].strftime('%Y-%m-%d')} 만료"
        for _, row in store_employees_df.iterrows()
        if pd.notna(row['보건증만료일_dt']) and today <= row['보건증만료일_dt'] < (today + timedelta(days=30))
    ]
    if expiring_soon_list:
        st.sidebar.warning("🚨 보건증 만료 임박\n" + "\n".join(expiring_soon_list))

# =============================================================================
# 3. 로그인 화면
# =============================================================================
def login_screen():
    st.markdown("""
        <style>
            .main .st-emotion-cache-1jicfl2 { justify-content: center; }
            h1 { text-align: center; }
        </style>
    """, unsafe_allow_html=True)
    _, center_col, _ = st.columns([1, 1, 1])
    with center_col:
        with st.container(border=True):
            st.title("🏢 산카쿠 통합 관리 시스템")
            st.markdown("<br>", unsafe_allow_html=True)
            with st.form("login_form"):
                username = st.text_input("아이디 (지점ID)", placeholder="지점 아이디를 입력하세요")
                password = st.text_input("비밀번호", type="password", placeholder="비밀번호를 입력하세요")
                submitted = st.form_submit_button("로그인", use_container_width=True, type="primary")
                if submitted:
                    users_df = load_data(SHEET_NAMES["STORE_MASTER"])
                    if not users_df.empty:
                        user_info_df = users_df[(users_df['지점ID'] == username.strip()) & (users_df['지점PW'] == password)]
                        if not user_info_df.empty:
                            st.session_state['logged_in'] = True
                            st.session_state['user_info'] = user_info_df.iloc[0].to_dict()
                            st.session_state['data_cache'] = {}
                            st.rerun()
                        else:
                            st.error("아이디 또는 비밀번호가 올바르지 않습니다.")

# =============================================================================
# 4. [지점] 페이지 렌더링 함수
# =============================================================================
def render_store_attendance(user_info, employees_df, attendance_detail_df, lock_log_df):
    st.subheader("⏰ 월별 근무기록 관리")
    store_name = user_info['지점명']
    
    store_employees_df = employees_df[
        (employees_df['소속지점'] == store_name) & (employees_df['재직상태'] == STATUS["EMPLOYEE_ACTIVE"])
    ]
    if store_employees_df.empty:
        st.warning("등록된 재직중인 직원이 없습니다."); return

    month_options = [(date.today() - relativedelta(months=i)).replace(day=1) for i in range(4)]
    selected_month_date = st.selectbox("관리할 년/월 선택", options=month_options, format_func=lambda d: d.strftime('%Y년 / %m월'))
    selected_month_str = selected_month_date.strftime('%Y-%m')

    # 마감 상태 확인
    lock_status, is_locked = "미요청", False
    required_lock_cols = ['지점명', '마감유형', '상태', '마감년월']
    current_lock_request = pd.DataFrame()
    if not lock_log_df.empty and all(col in lock_log_df.columns for col in required_lock_cols):
        current_lock_request = lock_log_df[
            (lock_log_df['지점명'] == store_name) & (lock_log_df['마감유형'] == '근무') & (lock_log_df['마감년월'] == selected_month_str)
        ]
        if not current_lock_request.empty:
            lock_status = current_lock_request.iloc[0]['상태']
    is_locked = lock_status in [STATUS["LOCK_APPROVED"], STATUS["LOCK_REQUESTED"]]
    
    month_records_df = pd.DataFrame()
    if not attendance_detail_df.empty and '년월' in attendance_detail_df.columns:
        month_records_df = attendance_detail_df[
            (attendance_detail_df['년월'] == selected_month_str) & (attendance_detail_df['지점명'] == store_name)
        ].copy()

    if month_records_df.empty:
        st.markdown("---"); st.markdown("##### ✍️ 기본 스케줄 생성")
        st.info(f"**{selected_month_str}**에 대한 근무 기록이 없습니다. 아래 직원 정보를 확인 후 기본 스케줄을 생성해주세요.")
        st.dataframe(store_employees_df[['이름', '직책', '근무요일', '기본출근', '기본퇴근']], use_container_width=True, hide_index=True)
        
        if st.button(f"🗓️ {selected_month_str} 기본 스케줄 생성하기", type="primary", use_container_width=True):
            new_records = []
            day_map = {'월': 0, '화': 1, '수': 2, '목': 3, '금': 4, '토': 5, '일': 6}
            start_date = selected_month_date
            end_date = (start_date + relativedelta(months=1)) - timedelta(days=1)

            for _, emp in store_employees_df.iterrows():
                work_days = re.sub(r'[,\s]+', ' ', emp.get('근무요일', '')).split()
                work_day_indices = {day_map[d] for d in work_days if d in day_map}
                
                for dt in pd.date_range(start_date, end_date):
                    if dt.weekday() in work_day_indices:
                        uid = f"{dt.strftime('%y%m%d')}_{emp['이름']}_{int(datetime.now().timestamp())}_{len(new_records)}"
                        new_records.append({
                            "기록ID": f"manual_{uid}", 
                            "지점명": store_name, 
                            "근무일자": dt.strftime('%Y-%m-%d'), 
                            "직원이름": emp['이름'], 
                            "구분": STATUS["ATTENDANCE_NORMAL"], 
                            "출근시간": emp.get('기본출근', '09:00'), 
                            "퇴근시간": emp.get('기본퇴근', '18:00'), 
                            "비고": "기본 스케줄 생성"
                        })
            
            # --- [핵심 변경] ---
            # 전체 데이터를 읽고 덮어쓰는 대신, 새로 생성된 데이터만 '추가'합니다.
            if new_records and append_rows_and_clear_cache(SHEET_NAMES["ATTENDANCE_DETAIL"], pd.DataFrame(new_records)):
                st.toast(f"✅ {selected_month_str}의 기본 스케줄이 성공적으로 생성되었습니다."); st.rerun()
            elif not new_records:
                st.warning("스케줄을 생성할 직원이 없습니다.")

    else:
        def calculate_duration(row):
            try:
                start_t = datetime.strptime(str(row['출근시간']), '%H:%M')
                end_t = datetime.strptime(str(row['퇴근시간']), '%H:%M')
                duration = (end_t - start_t).total_seconds() / 3600
                return duration + 24 if duration < 0 else duration
            except (TypeError, ValueError): return 0
        month_records_df['총시간'] = month_records_df.apply(calculate_duration, axis=1)

        summary_pivot, display_summary = display_attendance_summary(month_records_df, selected_month_date)
        
        with st.expander("📊 엑셀 리포트 다운로드"):
            excel_data = create_excel_report(summary_pivot, display_summary, month_records_df, selected_month_str, store_name)
            st.download_button(label="📥 **월별 리포트 엑셀 다운로드**", data=excel_data,
                file_name=f"{store_name}_{selected_month_str}_월별근무보고서.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        
        st.markdown("---"); st.markdown("##### ✍️ 근무 기록 관리")
        render_daily_attendance_editor(month_records_df, store_employees_df, attendance_detail_df, selected_month_date, store_name, is_locked)
    
    st.markdown("---")
    if lock_status == STATUS["LOCK_APPROVED"]:
        st.success(f"✅ {selected_month_str}의 근무 정산이 마감되었습니다.")
    elif lock_status == STATUS["LOCK_REQUESTED"]:
        st.warning("🔒 관리자에게 마감 요청 중입니다.")
    elif lock_status == STATUS["LOCK_REJECTED"]:
        st.error(f"❌ 마감 요청이 반려되었습니다. 기록 수정 후 다시 요청해주세요.")
        if st.button(f"🔒 {selected_month_str} 근무기록 재요청", use_container_width=True, type="primary"):
            lock_log_df.loc[current_lock_request.index, '상태'] = STATUS["LOCK_REQUESTED"]
            lock_log_df.loc[current_lock_request.index, '요청일시'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if update_sheet_and_clear_cache(SHEET_NAMES["SETTLEMENT_LOCK_LOG"], lock_log_df):
                 st.toast("✅ 마감 재요청을 보냈습니다."); st.rerun()
    else:
        if st.button(f"🔒 {selected_month_str} 근무기록 마감 요청", use_container_width=True, type="primary"):
            new_lock_request = pd.DataFrame([{"마감년월": selected_month_str, "지점명": store_name, "마감유형": "근무", "상태": STATUS["LOCK_REQUESTED"], "요청일시": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "처리일시": "", "실행관리자": ""}])
            if append_rows_and_clear_cache(SHEET_NAMES["SETTLEMENT_LOCK_LOG"], new_lock_request):
                st.toast("✅ 마감 요청을 보냈습니다."); st.rerun()

def render_store_inventory_check(user_info, inventory_master_df, inventory_log_df, inventory_detail_log_df):
    st.subheader("📦 월말 재고확인")
    store_name = user_info['지점명']
    
    if inventory_master_df.empty:
        st.error("'재고마스터' 시트에 품목을 먼저 등록해주세요."); return

    month_options = [(date.today() - relativedelta(months=i)).replace(day=1) for i in range(4)]
    selected_month_date = st.selectbox("재고를 확인할 년/월 선택", options=month_options, format_func=lambda d: d.strftime('%Y년 / %m월'))
    selected_month_str = selected_month_date.strftime('%Y-%m')
    
    is_submitted = not inventory_log_df[(inventory_log_df['지점명'] == store_name) & (inventory_log_df['평가년월'] == selected_month_str)].empty
    
    st.markdown("---")
    
    if is_submitted:
        st.success(f"**{selected_month_str}**의 재고가 이미 제출되었습니다.")
        submitted_items = inventory_detail_log_df[(inventory_detail_log_df['지점명'] == store_name) & (inventory_detail_log_df['평가년월'] == selected_month_str)]
        st.dataframe(submitted_items[['품목명', '수량', '단위', '소계']].style.format({"소계": "₩{:,}"}), use_container_width=True, hide_index=True)
        total_value = submitted_items['소계'].sum()
        st.metric("**제출된 재고 총액**", f"₩ {total_value:,.0f}")

    else:
        cart_key = f"inventory_cart_{selected_month_str}"
        if cart_key not in st.session_state:
            st.session_state[cart_key] = {}
            
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("##### 🛒 품목 선택")
            search_term = st.text_input("품목 검색", placeholder="품목명으로 검색...")
            
            display_df = inventory_master_df.copy()
            if search_term:
                display_df = display_df[display_df['품목명'].str.contains(search_term, case=False, na=False)]
            display_df['수량'] = 0
            
            edited_items = st.data_editor(display_df[['품목명', '종류', '단위', '단가', '수량']],
                key=f"inventory_adder_{selected_month_str}", use_container_width=True,
                column_config={ "품목명": st.column_config.TextColumn(disabled=True), "종류": st.column_config.TextColumn(disabled=True), "단위": st.column_config.TextColumn(disabled=True), "단가": st.column_config.NumberColumn(disabled=True, format="%,d 원"), "수량": st.column_config.NumberColumn(min_value=0, step=1)},
                hide_index=True)

            if st.button("➕ 장바구니에 담기", use_container_width=True):
                for _, row in edited_items[edited_items['수량'] > 0].iterrows():
                    st.session_state[cart_key][row['품목명']] = row.to_dict()
                st.toast("🛒 장바구니에 품목을 담았습니다."); st.rerun()

        with col2:
            st.markdown("##### 📋 담은 재고 목록")
            if not st.session_state[cart_key]:
                st.info("아직 담은 품목이 없습니다.")
            else:
                cart_df = pd.DataFrame(list(st.session_state[cart_key].values()))
                cart_df['소계'] = cart_df['단가'].astype(float) * cart_df['수량'].astype(float)
                st.dataframe(cart_df[['품목명', '수량', '단위', '소계']].style.format({"소계": "₩{:,}"}), use_container_width=True, hide_index=True)
                total_value = cart_df['소계'].sum()
                st.metric("**재고 총액**", f"₩ {total_value:,.0f}")

                if st.button("🗑️ 장바구니 비우기", use_container_width=True):
                    st.session_state[cart_key] = {}; st.rerun()
                
                if st.button(f"🚀 {selected_month_str} 재고 제출하기", type="primary", use_container_width=True):
                    new_log_row = pd.DataFrame([{'평가년월': selected_month_str, '지점명': store_name, '재고평가액': total_value, '입력일시': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '입력자': user_info['지점ID']}])
                    log_success = append_rows_and_clear_cache(SHEET_NAMES["INVENTORY_LOG"], new_log_row)

                    cart_df_final = cart_df.copy()
                    cart_df_final['평가년월'] = selected_month_str; cart_df_final['지점명'] = store_name
                    detail_success = append_rows_and_clear_cache(SHEET_NAMES["INVENTORY_DETAIL_LOG"], cart_df_final[['평가년월', '지점명', '품목명', '종류', '단위', '단가', '수량', '소계']])
                    
                    if log_success and detail_success:
                        st.session_state[cart_key] = {}
                        st.toast(f"✅ {selected_month_str} 재고({total_value:,.0f}원)가 제출되었습니다."); st.rerun()

def render_store_employee_info(user_info, employees_df, personnel_request_log_df, stores_df):
    st.subheader("👥 직원 정보 관리")
    with st.expander("💡 도움말"):
        st.info("""
            - **신규 직원 등록**: 새로운 직원의 정보를 입력합니다. 연락처는 `010-1234-5678` 형식으로, 근무요일은 목록에서 중복 선택해야 합니다.
            - **우리 지점 직원 목록**: 현재 지점에 소속된 직원들의 정보를 확인하고 수정할 수 있습니다. 
            - **인사 이동/파견 요청**: 직원의 소속 지점 변경(이동)이나 특정 기간동안 다른 지점 근무(파견)를 관리자에게 요청합니다.
        """)
    store_name = user_info['지점명']
    with st.expander("➕ **신규 직원 등록하기**", expanded=True):
        with st.form("new_employee_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                emp_name = st.text_input("이름")
                emp_contact = st.text_input("연락처", placeholder="010-1234-5678")
                emp_status = st.selectbox("재직상태", [STATUS["EMPLOYEE_ACTIVE"], STATUS["EMPLOYEE_INACTIVE"]])
            with col2:
                emp_start_date = st.date_input("입사일", date.today())
                days_of_week = ["월", "화", "수", "목", "금", "토", "일"]
                emp_work_days_list = st.multiselect("근무요일", options=days_of_week)
            col3, col4 = st.columns(2)
            with col3: emp_start_time = st.time_input("기본출근", time(9, 0))
            with col4: emp_end_time = st.time_input("기본퇴근", time(18, 0))

            if st.form_submit_button("💾 신규 직원 저장", type="primary", use_container_width=True):
                if not emp_name: st.error("직원 이름은 반드시 입력해야 합니다.")
                elif not _validate_phone_number(emp_contact): st.error("연락처 형식이 올바르지 않습니다. (예: 010-1234-5678)")
                elif not emp_work_days_list: st.error("근무요일을 한 개 이상 선택해주세요.")
                else:
                    emp_work_days_str = ",".join(emp_work_days_list)
                    emp_id = f"{store_name.replace('점','')}_{emp_name}_{emp_start_date.strftime('%y%m%d')}"
                    new_data = {"직원ID": emp_id, "이름": emp_name, "소속지점": store_name, "직책": "직원", "입사일": emp_start_date.strftime('%Y-%m-%d'), "연락처": emp_contact, "보건증만료일": date.today().strftime('%Y-%m-%d'), "재직상태": emp_status, "근무요일": emp_work_days_str, "기본출근": emp_start_time.strftime('%H:%M'), "기본퇴근": emp_end_time.strftime('%H:%M')}
                    if append_rows_and_clear_cache(SHEET_NAMES["EMPLOYEE_MASTER"], pd.DataFrame([new_data])):
                        st.toast(f"✅ '{emp_name}' 직원의 정보가 성공적으로 등록되었습니다."); st.rerun()

    st.markdown("---"); st.markdown("##### **우리 지점 직원 목록**")
    store_employees_df = employees_df[employees_df['소속지점'] == store_name].copy()
    if not store_employees_df.empty:
        edited_df = st.data_editor(store_employees_df, key="employee_editor", use_container_width=True,
            column_config={
                "직원ID": st.column_config.TextColumn("직원ID", disabled=True),
                "소속지점": st.column_config.TextColumn("소속지점", disabled=True),
                "재직상태": st.column_config.SelectboxColumn("재직상태", options=[STATUS["EMPLOYEE_ACTIVE"], STATUS["EMPLOYEE_INACTIVE"]], required=True),
            })
        if st.button("💾 변경사항 저장", type="primary", use_container_width=True):
            error_found = False
            for index, row in edited_df.iterrows():
                if not _validate_phone_number(row['연락처']):
                    st.error(f"'{row['이름']}' 직원의 연락처 형식이 올바르지 않습니다. (010-1234-5678)"); error_found = True
                if not _validate_work_days(row['근무요일']):
                    st.error(f"'{row['이름']}' 직원의 근무요일 형식이 올바르지 않습니다. (쉼표로 구분된 요일: 월,수,금)"); error_found = True
            if not error_found:
                other_stores_df = employees_df[employees_df['소속지점'] != store_name]
                updated_full_df = pd.concat([other_stores_df, edited_df], ignore_index=True)
                if update_sheet_and_clear_cache(SHEET_NAMES["EMPLOYEE_MASTER"], updated_full_df):
                    st.toast("✅ 직원 정보가 성공적으로 업데이트되었습니다."); st.rerun()

    with st.expander("✈️ **인사 이동 / 파견 요청**"):
        with st.form("personnel_request_form", clear_on_submit=True):
            req_emp_name = st.selectbox("요청 직원", options=store_employees_df['이름'].unique())
            req_type = st.radio("요청 유형", [STATUS["REQUEST_TYPE_TRANSFER"], STATUS["REQUEST_TYPE_DISPATCH"]], horizontal=True)
            other_stores = stores_df[stores_df['지점명'] != store_name]['지점명'].unique().tolist()
            req_target_store = st.selectbox("요청 지점", options=other_stores)
            
            detail_text = ""
            if req_type == STATUS["REQUEST_TYPE_DISPATCH"]:
                c1, c2 = st.columns(2)
                start_date_req = c1.date_input("파견 시작일")
                end_date_req = c2.date_input("파견 종료일")
                detail_text = f"{req_target_store}으로 {start_date_req}부터 {end_date_req}까지 파견 요청"
            else:
                detail_text = f"{req_target_store}으로 소속 이동 요청"
            
            if st.form_submit_button("관리자에게 요청 보내기", type="primary"):
                new_request = pd.DataFrame([{"요청일시": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "요청지점": store_name, "요청직원": req_emp_name, "요청유형": req_type, "상세내용": detail_text, "상태": STATUS["LOCK_REQUESTED"], "처리일시": "", "처리관리자": ""}])
                if append_rows_and_clear_cache(SHEET_NAMES["PERSONNEL_REQUEST_LOG"], new_request):
                    st.toast("✅ 관리자에게 인사 요청을 보냈습니다."); st.rerun()

# =============================================================================
# 5. [관리자] 페이지 렌더링 함수
# =============================================================================
def render_admin_dashboard(cache):
    st.subheader("📊 통합 대시보드")

    # --- 할 일 목록 ---
    st.markdown("##### 📥 할 일 목록")
    cols = st.columns(3)
    
    lock_log_df = cache["SETTLEMENT_LOCK_LOG"]
    personnel_request_log_df = cache["PERSONNEL_REQUEST_LOG"]
    employees_df = cache["EMPLOYEE_MASTER"]

    pending_locks = 0
    if not lock_log_df.empty and '상태' in lock_log_df.columns:
        pending_locks = len(lock_log_df[lock_log_df['상태'] == STATUS["LOCK_REQUESTED"]])
    cols[0].metric("정산 마감 요청", f"{pending_locks} 건")

    pending_personnel = 0
    if not personnel_request_log_df.empty and '상태' in personnel_request_log_df.columns:
        pending_personnel = len(personnel_request_log_df[personnel_request_log_df['상태'] == STATUS["LOCK_REQUESTED"]])
    cols[1].metric("인사 요청", f"{pending_personnel} 건")

    expiring_certs = 0
    if not employees_df.empty and '보건증만료일_dt' in employees_df.columns:
        today = pd.to_datetime(date.today())
        expiring_df = employees_df[
            (employees_df['재직상태'] == STATUS["EMPLOYEE_ACTIVE"]) &
            (employees_df['보건증만료일_dt'].notna()) &
            (employees_df['보건증만료일_dt'] >= today) &
            (employees_df['보건증만료일_dt'] < today + timedelta(days=30))
        ]
        expiring_certs = len(expiring_df)
    cols[2].metric("보건증 만료 임박", f"{expiring_certs} 건")
    st.info("각 항목의 처리 및 관리는 해당 관리 탭에서 진행할 수 있습니다.")

    st.markdown("---")
    # --- 핵심 지표 ---
    st.markdown("##### 📈 핵심 지표")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("👨‍👩‍👧‍👦 전체 직원 수", f"{len(employees_df[employees_df['재직상태'] == STATUS['EMPLOYEE_ACTIVE']]):,} 명")
    
    if not cache["INVENTORY_LOG"].empty:
        latest_month = cache["INVENTORY_LOG"]['평가년월'].max()
        latest_inv_total = cache["INVENTORY_LOG"][cache["INVENTORY_LOG"]['평가년월'] == latest_month]['재고평가액'].sum()
        c2.metric(f"📦 전 지점 재고 자산 ({latest_month})", f"₩ {int(latest_inv_total):,}")

    if not cache["SALES_LOG"].empty:
        this_month_str = datetime.now().strftime('%Y-%m')
        this_month_sales = cache["SALES_LOG"][cache["SALES_LOG"]['년월'] == this_month_str]['금액'].sum()
        c3.metric(f"💰 금월 전체 매출 ({this_month_str})", f"₩ {int(this_month_sales):,}")
        
        this_month_df = cache["SALES_LOG"][cache["SALES_LOG"]['년월'] == this_month_str]
        if not this_month_df.empty:
            best_store = this_month_df.groupby('지점명')['금액'].sum().idxmax()
            c4.metric("🏆 금월 최고 매출 지점", best_store)
        else:
            c4.metric("🏆 금월 최고 매출 지점", "N/A")

def render_admin_settlement_management(cache):
    st.subheader("🧾 정산 관리")
    
    lock_log_df = cache["SETTLEMENT_LOCK_LOG"]
    inventory_detail_log_df = cache["INVENTORY_DETAIL_LOG"]

    tab1, tab2, tab3 = st.tabs(["📂 매출 정보", "✍️ 지출 정보", "📦 월말 재고"])

    with tab1: # 매출 정보
        st.markdown("###### 매출 정보 업로드")
        template_df = pd.DataFrame([{"매출일자": "2025-09-01", "지점명": "전대점", "매출유형": "카드매출", "금액": 100000}])
        output = io.BytesIO()
        template_df.to_excel(output, index=False, sheet_name='매출 업로드 양식')
        st.download_button("📥 매출 엑셀 양식 다운로드", data=output.getvalue(), file_name="매출_업로드_양식.xlsx")
        
        uploaded_file = st.file_uploader("매출 엑셀 파일 업로드", type=["xlsx"], key="sales_uploader")
        if uploaded_file:
            try:
                upload_df = pd.read_excel(uploaded_file)
                upload_df['매출일자'] = pd.to_datetime(upload_df['매출일자']).dt.strftime('%Y-%m-%d')
                st.dataframe(upload_df, use_container_width=True)
                if st.button("⬆️ 매출 데이터 저장하기"):
                    if append_rows_and_clear_cache(SHEET_NAMES["SALES_LOG"], upload_df):
                        st.toast(f"✅ 매출 데이터 {len(upload_df)}건이 저장되었습니다."); st.rerun()
            except Exception as e:
                st.error(f"파일 처리 중 오류: {e}")

    with tab2: # 지출 정보
        st.markdown("###### 지출 정보 업로드")
        template_df_exp = pd.DataFrame([{"정산일자": "2025-09-01", "지점명": "전대점", "대분류": "식자재", "상세내용": "삼겹살 10kg", "금액": 150000}])
        output_exp = io.BytesIO()
        template_df_exp.to_excel(output_exp, index=False, sheet_name='지출 업로드 양식')
        st.download_button("📥 지출 엑셀 양식 다운로드", data=output_exp.getvalue(), file_name="지출_업로드_양식.xlsx", key="exp_template_downloader")
        
        uploaded_file_exp = st.file_uploader("지출 엑셀 파일 업로드", type=["xlsx"], key="settlement_uploader")
        if uploaded_file_exp:
            try:
                upload_df_exp = pd.read_excel(uploaded_file_exp)
                upload_df_exp['정산일자'] = pd.to_datetime(upload_df_exp['정산일자']).dt.strftime('%Y-%m-%d')
                upload_df_exp['입력일시'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                upload_df_exp['입력자'] = st.session_state['user_info']['지점ID']
                st.dataframe(upload_df_exp, use_container_width=True)
                if st.button("⬆️ 지출 데이터 저장하기"):
                    if append_rows_and_clear_cache(SHEET_NAMES["SETTLEMENT_LOG"], upload_df_exp):
                        st.toast(f"✅ 지출 데이터 {len(upload_df_exp)}건이 저장되었습니다."); st.rerun()
            except Exception as e:
                st.error(f"파일 처리 중 오류: {e}")
    
    with tab3: # 월말 재고
        st.markdown("###### 지점별 월말 재고 상세 조회")
        if inventory_detail_log_df.empty:
            st.info("조회할 재고 로그 데이터가 없습니다.")
        else:
            c1, c2 = st.columns(2)
            store_options = ["전체"] + sorted(inventory_detail_log_df['지점명'].unique().tolist())
            month_options = ["전체"] + sorted(inventory_detail_log_df['평가년월'].unique().tolist(), reverse=True)
            selected_store = c1.selectbox("지점 선택", options=store_options, key="inv_log_store")
            selected_month = c2.selectbox("년/월 선택", options=month_options, key="inv_log_month")
            
            filtered_log = inventory_detail_log_df.copy()
            if selected_store != "전체": filtered_log = filtered_log[filtered_log['지점명'] == selected_store]
            if selected_month != "전체": filtered_log = filtered_log[filtered_log['평가년월'] == selected_month]
            st.dataframe(filtered_log, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("###### 📦 재고 정산 마감 관리")

        # --- [오류 수정] ---
        # lock_log_df가 비어있지 않고, 필요한 컬럼이 모두 있는지 먼저 확인합니다.
        pending_locks = pd.DataFrame()
        required_cols = ['상태', '마감유형', '마감년월', '지점명']
        if not lock_log_df.empty and all(col in lock_log_df.columns for col in required_cols):
            pending_locks = lock_log_df[
                (lock_log_df['상태'] == STATUS["LOCK_REQUESTED"]) &
                (lock_log_df['마감유형'] == '재고')
            ]
        
        if pending_locks.empty:
            st.info("처리 대기 중인 재고 정산 마감 요청이 없습니다.")
        else:
            st.warning("아래 재고 정산 마감 요청이 대기 중입니다.")
            st.dataframe(pending_locks, use_container_width=True, hide_index=True)
            
            def format_lock_req(index):
                if index == "": return "처리할 요청 선택..."
                try: return f"{pending_locks.loc[index, '마감년월']} / {pending_locks.loc[index, '지점명']}"
                except KeyError: return "만료된 요청"
            
            options = [""] + pending_locks.index.tolist()
            selected_req_index = st.selectbox("처리할 요청 선택", options, format_func=format_lock_req, key="inv_lock_selector")

            if selected_req_index != "" and selected_req_index in pending_locks.index:
                c1, c2 = st.columns(2)
                admin_id = st.session_state['user_info']['지점ID']
                if c1.button("✅ 재고 마감 승인", use_container_width=True, type="primary"):
                    lock_log_df.loc[selected_req_index, '상태'] = STATUS["LOCK_APPROVED"]
                    lock_log_df.loc[selected_req_index, '처리일시'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    lock_log_df.loc[selected_req_index, '실행관리자'] = admin_id
                    if update_sheet_and_clear_cache(SHEET_NAMES["SETTLEMENT_LOCK_LOG"], lock_log_df):
                        st.toast("재고 정산 마감 요청이 승인되었습니다."); st.rerun()
                if c2.button("❌ 재고 마감 반려", use_container_width=True):
                    lock_log_df.loc[selected_req_index, '상태'] = STATUS["LOCK_REJECTED"]
                    lock_log_df.loc[selected_req_index, '처리일시'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    lock_log_df.loc[selected_req_index, '실행관리자'] = admin_id
                    if update_sheet_and_clear_cache(SHEET_NAMES["SETTLEMENT_LOCK_LOG"], lock_log_df):
                        st.toast("재고 정산 마감 요청이 반려되었습니다."); st.rerun()

def render_admin_employee_management(cache):
    st.subheader("👨‍💼 전직원 관리")
    
    employees_df = cache["EMPLOYEE_MASTER"]
    personnel_request_log_df = cache["PERSONNEL_REQUEST_LOG"]
    attendance_df = cache["ATTENDANCE_DETAIL"]

    tab1, tab2 = st.tabs(["👥 전체 직원 현황", "⏰ 전체 근무 현황"])

    with tab1: # 전체 직원 현황
        st.markdown("###### ✈️ 인사 이동/파견 요청 처리")

        # --- [오류 수정] ---
        # personnel_request_log_df가 비어있지 않고, 필요한 컬럼이 모두 있는지 먼저 확인합니다.
        pending_personnel = pd.DataFrame()
        required_cols = ['상태', '요청일시', '요청직원', '요청유형', '상세내용']
        if not personnel_request_log_df.empty and all(col in personnel_request_log_df.columns for col in required_cols):
            pending_personnel = personnel_request_log_df[personnel_request_log_df['상태'] == STATUS["LOCK_REQUESTED"]]
        
        if pending_personnel.empty:
            st.info("처리 대기 중인 인사 요청이 없습니다.")
        else:
            st.warning("아래 인사 요청이 대기 중입니다.")
            st.dataframe(pending_personnel, use_container_width=True, hide_index=True)
            
            def format_personnel_req(index):
                if index == "": return "처리할 요청 선택..."
                try: return f"{pending_personnel.loc[index, '요청일시']} / {pending_personnel.loc[index, '요청직원']}"
                except KeyError: return "만료된 요청"
            
            options_p = [""] + pending_personnel.index.tolist()
            selected_req_index_p = st.selectbox("처리할 요청 선택", options_p, format_func=format_personnel_req, key="personnel_req_selector")

            if selected_req_index_p != "" and selected_req_index_p in pending_personnel.index:
                c1, c2 = st.columns(2)
                request_details = pending_personnel.loc[selected_req_index_p]
                admin_id = st.session_state['user_info']['지점ID']
                
                if c1.button("✅ 인사 요청 승인", key=f"approve_personnel_{selected_req_index_p}", use_container_width=True, type="primary"):
                    success = False
                    req_type = request_details['요청유형']
                    emp_name = request_details['요청직원']
                    detail_text = request_details['상세내용']
                    
                    emp_info = employees_df[employees_df['이름'] == emp_name]
                    if emp_info.empty:
                        st.error(f"직원 '{emp_name}'의 정보를 찾을 수 없습니다.")
                    else:
                        emp_id = emp_info.iloc[0]['직원ID']
                        current_store = emp_info.iloc[0]['소속지점']

                        if req_type == STATUS["REQUEST_TYPE_TRANSFER"]:
                            target_store = detail_text.split('으로')[0]
                            updated_employees = employees_df.copy()
                            updated_employees.loc[updated_employees['이름'] == emp_name, '소속지점'] = target_store
                            new_log = pd.DataFrame([{"이동일시": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "직원ID": emp_id, "이름": emp_name, "이전지점": current_store, "새지점": target_store, "실행관리자": admin_id}])
                            if update_sheet_and_clear_cache(SHEET_NAMES["EMPLOYEE_MASTER"], updated_employees):
                                append_rows_and_clear_cache(SHEET_NAMES["PERSONNEL_TRANSFER_LOG"], new_log)
                                success = True

                        elif req_type == STATUS["REQUEST_TYPE_DISPATCH"]:
                            parts = detail_text.split(' ')
                            target_store = parts[0].replace('으로', '')
                            start_date = parts[1]
                            end_date = parts[3]
                            new_dispatch = pd.DataFrame([{"직원ID": emp_id, "이름": emp_name, "원소속": current_store, "파견지점": target_store, "파견시작일": start_date, "파견종료일": end_date, "실행관리자": admin_id}])
                            if append_rows_and_clear_cache(SHEET_NAMES["DISPATCH_LOG"], new_dispatch):
                                success = True
                        
                        if success:
                            personnel_request_log_df.loc[selected_req_index_p, '상태'] = STATUS["LOCK_APPROVED"]
                            personnel_request_log_df.loc[selected_req_index_p, '처리일시'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            personnel_request_log_df.loc[selected_req_index_p, '처리관리자'] = admin_id
                            if update_sheet_and_clear_cache(SHEET_NAMES["PERSONNEL_REQUEST_LOG"], personnel_request_log_df):
                                st.toast(f"✅ {emp_name} 직원의 {req_type} 요청이 승인되었습니다."); st.rerun()

                if c2.button("❌ 인사 요청 반려", key=f"reject_personnel_{selected_req_index_p}", use_container_width=True):
                    personnel_request_log_df.loc[selected_req_index_p, '상태'] = STATUS["LOCK_REJECTED"]
                    personnel_request_log_df.loc[selected_req_index_p, '처리일시'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    personnel_request_log_df.loc[selected_req_index_p, '처리관리자'] = admin_id
                    if update_sheet_and_clear_cache(SHEET_NAMES["PERSONNEL_REQUEST_LOG"], personnel_request_log_df):
                        st.toast("인사 요청이 반려되었습니다."); st.rerun()

        st.markdown("---")
        st.markdown("###### 📝 전체 직원 목록")
        if employees_df.empty:
            st.warning("등록된 직원이 없습니다.")
        else:
            stores = ['전체 지점'] + sorted(employees_df['소속지점'].unique().tolist())
            selected_store = st.selectbox("지점 필터", stores)
            display_df = employees_df if selected_store == '전체 지점' else employees_df[employees_df['소속지점'] == selected_store]
            edited_df = st.data_editor(display_df, hide_index=True, use_container_width=True, key="admin_emp_editor", 
                column_config={"직원ID": st.column_config.TextColumn(disabled=True)})
            if st.button("💾 직원 정보 저장", use_container_width=True):
                final_df = edited_df if selected_store == '전체 지점' else pd.concat([employees_df[employees_df['소속지점'] != selected_store], edited_df], ignore_index=True)
                if update_sheet_and_clear_cache(SHEET_NAMES["EMPLOYEE_MASTER"], final_df):
                    st.toast("✅ 전체 직원 정보가 업데이트되었습니다."); st.rerun()
    
    with tab2:
        st.markdown("###### 📊 지점별 근무 시간 분석")
        if not attendance_df.empty:
            if '총시간' not in attendance_df.columns or attendance_df['총시간'].isnull().all():
                def calculate_duration(row):
                    try:
                        start_t = datetime.strptime(str(row['출근시간']), '%H:%M')
                        end_t = datetime.strptime(str(row['퇴근시간']), '%H:%M')
                        duration = (end_t - start_t).total_seconds() / 3600
                        return duration + 24 if duration < 0 else duration
                    except: return 0
                attendance_df['총시간'] = attendance_df.apply(calculate_duration, axis=1)
            
            pivot_df = attendance_df.pivot_table(index='지점명', columns='구분', values='총시간', aggfunc='sum', fill_value=0)
            if STATUS["ATTENDANCE_NORMAL"] not in pivot_df.columns: pivot_df[STATUS["ATTENDANCE_NORMAL"]] = 0
            if STATUS["ATTENDANCE_OVERTIME"] not in pivot_df.columns: pivot_df[STATUS["ATTENDANCE_OVERTIME"]] = 0

            st.bar_chart(pivot_df[[STATUS["ATTENDANCE_NORMAL"], STATUS["ATTENDANCE_OVERTIME"]]])
        else:
            st.info("분석할 근무 기록 데이터가 없습니다.")

        st.markdown("---")
        st.markdown("###### 📋 전체 근무 기록 조회")
        st.dataframe(attendance_df.drop(columns=['총시간'], errors='ignore'), use_container_width=True, hide_index=True)

        with st.expander("🔒 근무 기록 수동 마감"):
            st.warning("이 기능은 요청/승인 절차 없이 즉시 데이터를 마감 처리합니다.")
            c1, c2, c3 = st.columns(3)
            lock_store = c1.selectbox("마감할 지점", cache['STORE_MASTER'][cache['STORE_MASTER']['역할'] != 'admin']['지점명'].unique())
            lock_month = c2.selectbox("마감할 년/월", [(date.today() - relativedelta(months=i)).strftime('%Y-%m') for i in range(12)])
            if c3.button("🚀 근무 기록 즉시 마감", type="primary"):
                new_lock = pd.DataFrame([{"마감년월": lock_month, "지점명": lock_store, "마감유형": "근무", "상태": "승인", "요청일시": "수동 마감", "처리일시": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "실행관리자": st.session_state['user_info']['지점ID']}])
                if append_rows_and_clear_cache(SHEET_NAMES["SETTLEMENT_LOCK_LOG"], new_lock):
                    st.toast(f"✅ {lock_store} {lock_month} 근무 기록이 마감 처리되었습니다."); st.rerun()

def render_admin_pnl_analysis(cache):
    st.subheader("📈 종합 손익 분석")
    sales_df = cache["SALES_LOG"]
    settlement_df = cache["SETTLEMENT_LOG"]
    inventory_log_df = cache["INVENTORY_LOG"]

    if sales_df.empty or settlement_df.empty or inventory_log_df.empty:
        st.warning("손익 분석을 위한 매출, 지출, 재고 데이터가 모두 필요합니다."); return

    all_stores = sales_df['지점명'].unique().tolist()
    selected_store = st.selectbox("분석할 지점 선택", options=["전체"] + all_stores)
    
    if selected_store != "전체":
        sales_df = sales_df[sales_df['지점명'] == selected_store]
        settlement_df = settlement_df[settlement_df['지점명'] == selected_store]
        inventory_log_df = inventory_log_df[inventory_log_df['지점명'] == selected_store]
        
    sales_df['월'] = pd.to_datetime(sales_df['매출일자']).dt.to_period('M')
    settlement_df['월'] = pd.to_datetime(settlement_df['정산일자']).dt.to_period('M')
    inventory_log_df['월'] = pd.to_datetime(inventory_log_df['평가년월']).dt.to_period('M')

    monthly_sales = sales_df.groupby('월')['금액'].sum()
    monthly_expenses = settlement_df.pivot_table(index='월', columns='대분류', values='금액', aggfunc='sum').fillna(0)
    monthly_inventory = inventory_log_df.set_index('월')['재고평가액']
    
    analysis_df = pd.DataFrame(monthly_sales).rename(columns={'금액': '매출'})
    analysis_df = analysis_df.join(monthly_expenses)
    analysis_df['기말재고'] = monthly_inventory
    analysis_df['기초재고'] = monthly_inventory.shift(1).fillna(0)
    
    for col in ['식자재', '판관비', '기타']:
        if col not in analysis_df.columns:
            analysis_df[col] = 0

    analysis_df['매출원가'] = analysis_df['기초재고'] + analysis_df['식자재'] - analysis_df['기말재고']
    analysis_df['매출총이익'] = analysis_df['매출'] - analysis_df['매출원가']
    analysis_df['영업이익'] = analysis_df['매출총이익'] - analysis_df['판관비'] - analysis_df['기타']
    
    st.markdown("#### **📊 월별 손익(P&L) 추이**")
    st.line_chart(analysis_df[['매출', '매출총이익', '영업이익']])
    st.dataframe(analysis_df.style.format("{:,.0f}"))

def render_admin_system_settings(cache):
    st.subheader("⚙️ 시스템 관리")
    store_master_df = cache["STORE_MASTER"]
    inventory_master_df = cache["INVENTORY_MASTER"]

    tab1, tab2 = st.tabs(["🏢 지점 계정 관리", "📋 재고 마스터 관리"])
    with tab1:
        st.markdown("###### 지점 계정 정보")
        st.info("지점 정보를 수정하거나 새 지점을 추가한 후 '계정 정보 저장' 버튼을 누르세요.")
        edited_stores_df = st.data_editor(store_master_df, num_rows="dynamic", use_container_width=True, key="admin_settings_editor")
        if st.button("💾 계정 정보 저장", use_container_width=True):
            if update_sheet_and_clear_cache(SHEET_NAMES["STORE_MASTER"], edited_stores_df):
                st.toast("✅ 지점 계정 정보가 저장되었습니다."); st.rerun()

    with tab2:
        st.markdown("###### 재고 품목 정보")
        st.info("이곳에서 품목을 추가, 수정, 삭제하면 모든 지점의 '월말 재고확인' 화면에 즉시 반영됩니다.")
        edited_master = st.data_editor(inventory_master_df, num_rows="dynamic", use_container_width=True, key="master_inv_editor")
        if st.button("💾 재고마스터 저장", type="primary", use_container_width=True):
            if update_sheet_and_clear_cache(SHEET_NAMES["INVENTORY_MASTER"], edited_master):
                st.toast("✅ 재고마스터가 성공적으로 업데이트되었습니다."); st.rerun()

# =============================================================================
# 6. 메인 실행 로직
# =============================================================================
def main():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    
    if not st.session_state['logged_in']:
        login_screen()
    else:
        if 'data_cache' not in st.session_state or not st.session_state['data_cache']:
            with st.spinner("데이터를 불러오는 중입니다..."):
                raw_data = {name: load_data(sheet) for name, sheet in SHEET_NAMES.items()}
                st.session_state['data_cache'] = preprocess_dataframes(raw_data)
        
        cache = st.session_state['data_cache']
        user_info = st.session_state['user_info']
        role, name = user_info.get('역할', 'store'), user_info.get('지점명', '사용자')
        
        st.sidebar.success(f"**{name}** ({role})님, 환영합니다.")
        st.sidebar.markdown("---")
        
        if role == 'store':
            check_health_cert_expiration(user_info, cache['EMPLOYEE_MASTER'])
        
        if st.sidebar.button("로그아웃"):
            st.session_state.clear(); st.rerun()
        
        st.markdown(f"""<style>
            .stTabs [data-baseweb="tab-list"] {{ gap: 12px; }}
            .stTabs [data-baseweb="tab"] {{ height: 42px; border: 1px solid {THEME['BORDER']}; border-radius: 12px; background-color: #fff; padding: 10px 14px; box-shadow: 0 1px 6px rgba(0,0,0,0.04); }}
            .stTabs [aria-selected="true"] {{ border-color: {THEME['PRIMARY']}; color: {THEME['PRIMARY']}; box-shadow: 0 6px 16px rgba(28,103,88,0.18); font-weight: 700; }}
            html, body, [data-testid="stAppViewContainer"] {{ background: {THEME['BG']}; }}
            [data-testid="stAppViewContainer"] .main .block-container {{ max-width: 1050px; margin: 0 auto; }}
            .stTabs [data-baseweb="tab-highlight"], .stTabs [data-baseweb="tab-border"] {{ display: none; }}
        </style>""", unsafe_allow_html=True)
        
        if role == 'admin':
            st.title("👑 관리자 페이지")
            admin_tabs = st.tabs(["📊 통합 대시보드", "🧾 정산 관리", "👨‍💼 전직원 관리", "📈 종합 손익 분석", "⚙️ 시스템 관리"])
            
            with admin_tabs[0]: render_admin_dashboard(cache)
            with admin_tabs[1]: render_admin_settlement_management(cache)
            with admin_tabs[2]: render_admin_employee_management(cache)
            with admin_tabs[3]: render_admin_pnl_analysis(cache)
            with admin_tabs[4]: render_admin_system_settings(cache)

        else: # role == 'store'
            st.title(f"🏢 {name} 지점 관리 시스템")
            store_tabs = st.tabs(["⏰ 월별 근무기록", "📦 월말 재고확인", "👥 직원 정보"])
            with store_tabs[0]:
                render_store_attendance(user_info, cache['EMPLOYEE_MASTER'], cache['ATTENDANCE_DETAIL'], cache['SETTLEMENT_LOCK_LOG'])
            with store_tabs[1]:
                render_store_inventory_check(user_info, cache['INVENTORY_MASTER'], cache['INVENTORY_LOG'], cache['INVENTORY_DETAIL_LOG'])
            with store_tabs[2]:
                render_store_employee_info(user_info, cache['EMPLOYEE_MASTER'], cache['PERSONNEL_REQUEST_LOG'], cache['STORE_MASTER'])

if __name__ == "__main__":
    main()



