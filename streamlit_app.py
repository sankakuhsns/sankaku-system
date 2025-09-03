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

SHEET_NAMES = {
    "STORE_MASTER": "지점마스터", "EMPLOYEE_MASTER": "직원마스터",
    "ATTENDANCE_DETAIL": "근무기록_상세", "INVENTORY_LOG": "월말재고_로그",
    "INVENTORY_MASTER": "재고마스터", "INVENTORY_DETAIL_LOG": "월말재고_상세로그",
    "SALES_LOG": "매출_로그", "SETTLEMENT_LOG": "일일정산_로그" # 관리자 대시보드용
}

# =============================================================================
# 1. 구글 시트 연결 및 데이터 처리 함수
# =============================================================================
@st.cache_resource
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)

def load_data(sheet_name):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
        numeric_cols = ['금액', '평가액', '총시간', '단가', '수량', '소계']
        for col in df.columns:
            if col in numeric_cols:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{sheet_name}' 시트를 찾을 수 없습니다. 구글 시트를 확인해주세요.")
        return pd.DataFrame()
    except Exception as e:
        if "Quota exceeded" in str(e): st.error("🔌 구글 시트 API 요청 한도를 초과했습니다. 1분 후에 페이지를 새로고침 해주세요.")
        else: st.error(f"'{sheet_name}' 시트 로딩 중 오류: {e}")
        return pd.DataFrame()

def update_sheet(sheet_name, df):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        df_str = df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.update([df_str.columns.values.tolist()] + df_str.values.tolist(), value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 업데이트 오류: {e}"); return False

def append_rows(sheet_name, rows_df):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        rows_df_str = rows_df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.append_rows(rows_df_str.values.tolist(), value_input_option='USER_ENTERED', insert_data_option='INSERT_ROWS', table_range='A1')
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 행 추가 오류: {e}"); return False

def update_sheet_and_clear_cache(sheet_name, df):
    if update_sheet(sheet_name, df):
        if 'data_cache' in st.session_state: del st.session_state['data_cache']
        return True
    return False

def append_rows_and_clear_cache(sheet_name, rows_df):
    if append_rows(sheet_name, rows_df):
        if 'data_cache' in st.session_state: del st.session_state['data_cache']
        return True
    return False

# =============================================================================
# 2. 헬퍼 함수 및 기능별 로직
# =============================================================================
def _format_time_input(time_input):
    s = str(time_input).strip()
    if ':' in s:
        parts = s.split(':')
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            s = f"{parts[0].zfill(2)}:{parts[1].zfill(2)}"
    elif s.isdigit():
        if len(s) == 3: s = f"0{s[0]}:{s[1:]}"
        elif len(s) == 4: s = f"{s[:2]}:{s[2:]}"
    return s if re.match(r'^([01]\d|2[0-3]):([0-5]\d)$', s) else None

def _validate_phone_number(phone):
    pattern = re.compile(r'^\d{3}-\d{4}-\d{4}$')
    return pattern.match(str(phone))

def _validate_work_days(days_str):
    valid_days = ["월", "화", "수", "목", "금", "토", "일"]
    parts = str(days_str).strip().split(',')
    return all(day.strip() in valid_days for day in parts)

def create_excel_report(summary_pivot, display_summary, selected_month_str, store_name):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        summary_pivot.to_excel(writer, sheet_name='월별 근무 현황', startrow=1)
        display_summary.to_excel(writer, sheet_name='근무 시간 집계', index=False, startrow=1)
        workbook = writer.book
        title_format = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'left'})
        header_format = workbook.add_format({'bold': True, 'valign': 'top', 'fg_color': '#DDEBF7', 'border': 1, 'align': 'center'})
        worksheet1 = writer.sheets['월별 근무 현황']
        worksheet1.write('A1', f"{selected_month_str.replace(' / ', '.')} 근무 현황", title_format)
        worksheet1.set_column('A:A', 12); worksheet1.set_column('B:AF', 5)
        worksheet1.write('A2', '직원이름', header_format)
        for col_num, value in enumerate(summary_pivot.columns.values):
            worksheet1.write(1, col_num + 1, value, header_format)
        worksheet2 = writer.sheets['근무 시간 집계']
        worksheet2.write('A1', f"{selected_month_str.replace(' / ', '.')} 근무 시간 집계", title_format)
        worksheet2.set_column('A:D', 15)
        for col_num, value in enumerate(display_summary.columns.values):
            worksheet2.write(1, col_num, value, header_format)
    return output.getvalue()

def check_health_cert_expiration(user_info, all_employees_df):
    if all_employees_df.empty: return
    store_name = user_info['지점명']
    store_employees_df = all_employees_df[(all_employees_df['소속지점'] == store_name) & (all_employees_df['재직상태'] == '재직중')]
    if store_employees_df.empty: return
    store_employees_df['보건증만료일'] = pd.to_datetime(store_employees_df['보건증만료일'], errors='coerce')
    today = datetime.now()
    expiring_soon_list = [f"- **{row['이름']}**: {row['보건증만료일'].strftime('%Y-%m-%d')} 만료" for _, row in store_employees_df.iterrows() if pd.notna(row['보건증만료일']) and today <= row['보건증만료일'] < (today + timedelta(days=30))]
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
# 4. 기능별 페이지 렌더링 함수
# =============================================================================
def render_store_attendance(user_info, employees_df, attendance_detail_df):
    st.subheader("⏰ 월별 근무기록 관리")
    store_name = user_info['지점명']
    store_employees_df = employees_df[(employees_df['소속지점'] == store_name) & (employees_df['재직상태'] == '재직중')]
    if store_employees_df.empty:
        st.warning("먼저 '직원 정보' 탭에서 '재직중' 상태의 직원을 한 명 이상 등록해주세요."); return

    selected_month_str = st.selectbox("관리할 년/월 선택", options=[(date.today() - relativedelta(months=i)).strftime('%Y년 / %m월') for i in range(12)])
    selected_month = datetime.strptime(selected_month_str, '%Y년 / %m월')
    start_date, end_date = selected_month.date(), (selected_month.date() + relativedelta(months=1)) - timedelta(days=1)
    
    month_records_df = pd.DataFrame()
    if not attendance_detail_df.empty and '근무일자' in attendance_detail_df.columns:
        month_records_df = attendance_detail_df[(pd.to_datetime(attendance_detail_df['근무일자'], errors='coerce').dt.strftime('%Y-%m') == selected_month.strftime('%Y-%m')) & (attendance_detail_df['지점명'] == store_name)].copy()

    if month_records_df.empty:
        st.markdown("---"); st.markdown("##### ✍️ 기본 스케줄 생성")
        st.info(f"**{selected_month_str}**에 대한 근무 기록이 없습니다. 아래 직원 정보를 확인 후 기본 스케줄을 생성해주세요.")
        st.dataframe(store_employees_df[['이름', '직책', '근무요일', '기본출근', '기본퇴근']], use_container_width=True, hide_index=True)
        if st.button(f"🗓️ {selected_month_str} 기본 스케줄 생성하기", type="primary", use_container_width=True):
            new_records = []
            day_map = {'월': 0, '화': 1, '수': 2, '목': 3, '금': 4, '토': 5, '일': 6}
            for _, emp in store_employees_df.iterrows():
                work_days = re.sub(r'요일|[,\s/]+', ' ', emp.get('근무요일', '')).split()
                work_day_indices = {day_map[d[0]] for d in work_days if d and d[0] in day_map}
                for dt in pd.date_range(start_date, end_date):
                    if dt.weekday() in work_day_indices:
                        uid = f"{dt.strftime('%y%m%d')}_{emp['이름']}_{int(datetime.now().timestamp())}_{len(new_records)}"
                        new_records.append({"기록ID": f"manual_{uid}", "지점명": store_name, "근무일자": dt.strftime('%Y-%m-%d'), "직원이름": emp['이름'], "구분": "정상근무", "출근시간": emp.get('기본출근', '09:00'), "퇴근시간": emp.get('기본퇴근', '18:00'), "비고": ""})
            if new_records and update_sheet_and_clear_cache(SHEET_NAMES["ATTENDANCE_DETAIL"], pd.concat([attendance_detail_df, pd.DataFrame(new_records)], ignore_index=True)):
                st.toast(f"✅ {selected_month_str}의 기본 스케줄이 성공적으로 생성되었습니다."); st.rerun()
    else:
        if '총시간' not in month_records_df.columns: month_records_df['총시간'] = 0
        def calculate_duration(row):
            try:
                start_t, end_t = datetime.strptime(str(row['출근시간']), '%H:%M'), datetime.strptime(str(row['퇴근시간']), '%H:%M')
                duration = (end_t - start_t).total_seconds() / 3600
                return duration + 24 if duration < 0 else duration
            except (TypeError, ValueError): return 0
        month_records_df['총시간'] = month_records_df.apply(calculate_duration, axis=1)
        
        st.markdown("---"); st.markdown("##### 🗓️ 근무 현황 요약")
        summary_pivot = month_records_df.pivot_table(index='직원이름', columns=pd.to_datetime(month_records_df['근무일자']).dt.day, values='총시간', aggfunc='sum').reindex(columns=range(1, end_date.day + 1))
        summary_pivot.columns = [f"{day}" for day in range(1, end_date.day + 1)]
        st.dataframe(summary_pivot.style.format(lambda val: f"{val:.1f}" if pd.notna(val) else ""), use_container_width=True)
        
        summary = month_records_df.pivot_table(index='직원이름', columns='구분', values='총시간', aggfunc='sum', fill_value=0)
        required_cols = ['정상근무', '연장근무']
        for col in required_cols:
            if col not in summary.columns: summary[col] = 0
        summary['총합'] = summary[required_cols].sum(axis=1)
        display_summary = summary[required_cols + ['총합']].reset_index().rename(columns={'직원이름':'이름'})
        st.dataframe(display_summary.style.format({'정상근무': '{:.1f} 시간', '연장근무': '{:.1f} 시간', '총합': '{:.1f} 시간'}), use_container_width=True, hide_index=True)

        with st.expander("📊 엑셀 리포트 다운로드"):
            st.info("현재 조회중인 월의 근무 현황 전체를 서식이 적용된 엑셀 파일로 다운로드합니다.")
            excel_data = create_excel_report(summary_pivot, display_summary, selected_month_str, store_name)
            st.download_button(label="📥 **월별 리포트 엑셀 다운로드**", data=excel_data, file_name=f"{store_name}_{selected_month_str.replace(' / ', '_')}_월별근무보고서.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        
        st.markdown("---"); st.markdown("##### ✍️ 근무 기록 관리")
        with st.expander("🗂️ 근무기록 일괄관리"):
            bulk_emp_name = st.selectbox("관리 대상 직원", options=store_employees_df['이름'].unique(), key="bulk_emp")
            emp_info = store_employees_df[store_employees_df['이름'] == bulk_emp_name].iloc[0]
            bulk_action = st.selectbox("관리 유형", ["입사/지점이동 (기록 추가)", "퇴사/지점이동 (기록 삭제)"], key="bulk_action")
            
            if bulk_action == "입사/지점이동 (기록 추가)":
                st.info(f"선택한 직원의 기본 설정에 따라 기록을 추가합니다. (근무 요일: **{emp_info.get('근무요일', '미지정')}**, 근무 시간: **{emp_info.get('기본출근', '미지정')} ~ {emp_info.get('기본퇴근', '미지정')}**)")
            else:
                st.info("선택한 직원의 모든 기록이 삭제되며, **수동으로 시간을 변경한 기록도 포함**됩니다. 주의해주세요.")

            c1, c2 = st.columns(2)
            bulk_start_date = c1.date_input("시작일", value=start_date, min_value=start_date, max_value=end_date, key="bulk_start")
            bulk_end_date = c2.date_input("종료일", value=end_date, min_value=start_date, max_value=end_date, key="bulk_end")
            
            if bulk_action == "입사/지점이동 (기록 추가)":
                day_map = {'월': 0, '화': 1, '수': 2, '목': 3, '금': 4, '토': 5, '일': 6}
                work_days = re.sub(r'요일|[,\s/]+', ' ', emp_info.get('근무요일', '')).split()
                work_day_indices = {day_map[d[0]] for d in work_days if d and d[0] in day_map}
                existing_dates = set(pd.to_datetime(attendance_detail_df[attendance_detail_df['직원이름'] == bulk_emp_name]['근무일자']).dt.date) if not attendance_detail_df.empty else set()
                potential_dates = [dt for dt in pd.date_range(bulk_start_date, bulk_end_date) if dt.weekday() in work_day_indices]
                dates_to_add = [dt for dt in potential_dates if dt.date() not in existing_dates]
                st.warning(f"총 **{len(dates_to_add)}** 건의 근무 기록이 새로 추가됩니다. (이미 기록이 있는 날짜는 제외)")
            elif bulk_action == "퇴사/지점이동 (기록 삭제)":
                df_to_delete = attendance_detail_df.copy()
                df_to_delete['근무일자_dt'] = pd.to_datetime(df_to_delete['근무일자']).dt.date
                records_to_delete_count = len(df_to_delete[(df_to_delete['직원이름'] == bulk_emp_name) & (df_to_delete['근무일자_dt'] >= bulk_start_date) & (df_to_delete['근무일자_dt'] <= bulk_end_date)])
                st.warning(f"총 **{records_to_delete_count}** 건의 근무 기록이 삭제됩니다.")

            confirm = st.checkbox(f"**주의:** '{bulk_emp_name}' 직원의 {bulk_start_date} ~ {bulk_end_date} 기록을 일괄 변경합니다.")
            if st.button("🚀 일괄 적용하기", key="bulk_apply", disabled=not confirm):
                if bulk_action == "입사/지점이동 (기록 추가)":
                    new_records = [{"기록ID": f"manual_{dt.strftime('%y%m%d')}_{emp_info['이름']}_{int(datetime.now().timestamp())}_{i}", "지점명": store_name, "근무일자": dt.strftime('%Y-%m-%d'), "직원이름": emp_info['이름'], "구분": "정상근무", "출근시간": emp_info.get('기본출근', '09:00'), "퇴근시간": emp_info.get('기본퇴근', '18:00'), "비고": "일괄 추가"} for i, dt in enumerate(dates_to_add)]
                    if new_records and update_sheet_and_clear_cache(SHEET_NAMES["ATTENDANCE_DETAIL"], pd.concat([attendance_detail_df, pd.DataFrame(new_records)], ignore_index=True)):
                        st.toast(f"✅ '{bulk_emp_name}' 직원의 근무 기록 {len(new_records)}건이 추가되었습니다."); st.rerun()
                    else: st.info("추가할 새로운 근무 기록이 없습니다.")
                elif bulk_action == "퇴사/지점이동 (기록 삭제)":
                    original_count = len(attendance_detail_df)
                    df_to_delete['근무일자_dt'] = pd.to_datetime(df_to_delete['근무일자']).dt.date
                    final_df = df_to_delete[~((df_to_delete['직원이름'] == bulk_emp_name) & (df_to_delete['근무일자_dt'] >= bulk_start_date) & (df_to_delete['근무일자_dt'] <= bulk_end_date))].drop(columns=['근무일자_dt'])
                    if update_sheet_and_clear_cache(SHEET_NAMES["ATTENDANCE_DETAIL"], final_df):
                        st.toast(f"🗑️ '{bulk_emp_name}' 직원의 근무 기록 {original_count - len(final_df)}건이 삭제되었습니다."); st.rerun()
        
        default_date = date.today() if start_date <= date.today() <= end_date else start_date
        selected_date = st.date_input("관리할 날짜 선택", value=default_date, min_value=start_date, max_value=end_date, key="date_selector", help="표를 수정하려면 먼저 날짜를 선택하세요.")
        st.info(f"**{selected_date.strftime('%Y년 %m월 %d일')}**의 기록을 아래 표에서 직접 수정, 추가, 삭제할 수 있습니다.")
        daily_records_df = month_records_df[month_records_df['근무일자'] == selected_date.strftime('%Y-%m-%d')].copy()
        daily_records_df.drop(columns=['총시간', '지점명'], inplace=True, errors='ignore'); daily_records_df.reset_index(drop=True, inplace=True)
        edited_df = st.data_editor(daily_records_df, key=f"editor_{selected_date}", num_rows="dynamic", use_container_width=True,
            column_config={"기록ID": None, "근무일자": None, "직원이름": st.column_config.SelectboxColumn("이름", options=list(store_employees_df['이름'].unique()), required=True), "구분": st.column_config.SelectboxColumn("구분", options=["정상근무", "연장근무", "유급휴가", "무급휴가", "결근"], required=True), "출근시간": st.column_config.TextColumn("출근(HH:MM)", help="`9:00`, `900` 형식 모두 가능", default="09:00", required=True), "퇴근시간": st.column_config.TextColumn("퇴근(HH:MM)", help="`18:30`, `1830` 형식 모두 가능", default="18:00", required=True), "비고": st.column_config.TextColumn("비고")},
            hide_index=True, column_order=["직원이름", "구분", "출근시간", "퇴근시간", "비고"])
        if st.button(f"💾 {selected_date.strftime('%m월 %d일')} 기록 저장", type="primary", use_container_width=True):
            error_found = False; processed_df = edited_df.copy()
            if processed_df[["직원이름", "구분", "출근시간", "퇴근시간"]].isnull().values.any():
                st.error("필수 항목이 비어있습니다."); error_found = True
            else:
                processed_df['출근시간'] = processed_df['출근시간'].apply(_format_time_input)
                processed_df['퇴근시간'] = processed_df['퇴근시간'].apply(_format_time_input)
                invalid_rows = edited_df.loc[processed_df['출근시간'].isnull() | processed_df['퇴근시간'].isnull(), '직원이름']
                if not invalid_rows.empty:
                    st.error(f"시간 형식이 잘못되었습니다. 직원: {', '.join(set(invalid_rows))}"); error_found = True
            if not error_found:
                df_check = processed_df.copy()
                df_check['start_dt'] = pd.to_datetime(selected_date.strftime('%Y-%m-%d') + ' ' + df_check['출근시간'], errors='coerce')
                df_check['end_dt'] = pd.to_datetime(selected_date.strftime('%Y-%m-%d') + ' ' + df_check['퇴근시간'], errors='coerce')
                df_check.loc[df_check['end_dt'] <= df_check['start_dt'], 'end_dt'] += timedelta(days=1)
                overlap_employees = [name for name, group in df_check.groupby('직원이름') if any(group.sort_values('start_dt').reset_index().loc[i, 'end_dt'] > group.sort_values('start_dt').reset_index().loc[i+1, 'start_dt'] for i in range(len(group) - 1))]
                if overlap_employees:
                    st.error(f"근무 시간이 겹칩니다. 직원: {', '.join(set(overlap_employees))}"); error_found = True
            if not error_found:
                other_records = attendance_detail_df[attendance_detail_df['근무일자'] != selected_date.strftime('%Y-%m-%d')]
                new_details = processed_df.copy()
                for i, row in new_details.iterrows():
                    if pd.isna(row.get('기록ID')) or row.get('기록ID') == '':
                        uid = f"{selected_date.strftime('%y%m%d')}_{row['직원이름']}_{int(datetime.now().timestamp()) + i}"
                        new_details.at[i, '기록ID'] = f"manual_{uid}"
                    new_details.at[i, '지점명'] = store_name; new_details.at[i, '근무일자'] = selected_date.strftime('%Y-%m-%d')
                final_df = pd.concat([other_records, new_details], ignore_index=True)
                if update_sheet_and_clear_cache(SHEET_NAMES["ATTENDANCE_DETAIL"], final_df):
                    st.toast(f"✅ {selected_date.strftime('%m월 %d일')}의 근무 기록이 성공적으로 저장되었습니다."); st.rerun()

def render_store_inventory_check(user_info, inventory_master_df, inventory_log_df, inventory_detail_log_df):
    st.subheader("📦 월말 재고확인")
    store_name = user_info['지점명']
    
    if inventory_master_df.empty:
        st.error("'재고마스터' 시트에 품목을 먼저 등록해주세요."); return

    options = [(date.today() - relativedelta(months=i)) for i in range(12)]
    selected_month = st.selectbox("재고를 확인할 년/월 선택", options=options, format_func=lambda d: d.strftime('%Y년 / %m월'))
    selected_month_str = selected_month.strftime('%Y-%m')
    
    st.markdown("---")
    st.info("각 품목의 현재 수량을 입력하면 총액이 자동 계산됩니다.")
    
    editable_df = inventory_master_df.copy()
    if '수량' not in editable_df.columns: editable_df['수량'] = 0
    editable_df['소계'] = 0
    
    edited_df = st.data_editor(editable_df, key=f"inventory_editor_{selected_month_str}", use_container_width=True,
        column_config={ "품목명": st.column_config.TextColumn("품목명", disabled=True), "단위": st.column_config.TextColumn("단위", disabled=True), "단가": st.column_config.NumberColumn("단가", disabled=True, format="%,d 원"), "수량": st.column_config.NumberColumn("수량", min_value=0, step=1), "소계": st.column_config.NumberColumn("소계", disabled=True, format="%,d 원") },
        hide_index=True)
    
    total_inventory_value = (edited_df['단가'] * edited_df['수량']).sum() if not edited_df.empty else 0
    st.markdown("---"); st.metric("**월말 재고 총 합계액**", f"₩ {total_inventory_value:,.0f}")

    if st.button(f"💾 {selected_month.strftime('%Y년 %m월')} 재고 제출하기", type="primary", use_container_width=True):
        if '평가년월' in inventory_log_df.columns:
            inventory_log_df['평가년월'] = pd.to_datetime(inventory_log_df['평가년월'], errors='coerce').dt.strftime('%Y-%m')
        existing_indices = inventory_log_df[(inventory_log_df['평가년월'] == selected_month_str) & (inventory_log_df['지점명'] == store_name)].index
        if not existing_indices.empty:
            inventory_log_df.loc[existing_indices, ['재고평가액', '입력일시']] = [total_inventory_value, datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        else:
            new_row = pd.DataFrame([{'평가년월': selected_month_str, '지점명': store_name, '재고평가액': total_inventory_value, '입력일시': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '입력자': user_info['지점ID']}])
            inventory_log_df = pd.concat([inventory_log_df, new_row], ignore_index=True)
        update_success = update_sheet_and_clear_cache(SHEET_NAMES["INVENTORY_LOG"], inventory_log_df)

        detail_log_df = edited_df[edited_df['수량'] > 0].copy()
        if not detail_log_df.empty:
            detail_log_df['소계'] = detail_log_df['단가'] * detail_log_df['수량']
            detail_log_df['평가년월'] = selected_month_str; detail_log_df['지점명'] = store_name
            detail_log_df = detail_log_df[['평가년월', '지점명', '품목명', '단위', '단가', '수량', '소계']]
            append_success = append_rows_and_clear_cache(SHEET_NAMES["INVENTORY_DETAIL_LOG"], detail_log_df)
        else: append_success = True
        
        if update_success and append_success:
            st.toast(f"✅ {selected_month_str}의 재고({total_inventory_value:,.0f}원)가 성공적으로 제출되었습니다."); st.rerun()

def render_store_employee_info(user_info, employees_df):
    st.subheader("👥 직원 정보 관리")
    store_name = user_info['지점명']
    with st.expander("➕ **신규 직원 등록하기**", expanded=True):
        with st.form("new_employee_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                emp_name = st.text_input("이름")
                emp_contact = st.text_input("연락처", placeholder="010-1234-5678")
                emp_status = st.selectbox("재직상태", ["재직중", "퇴사"])
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
        edited_df = st.data_editor(store_employees_df, key="employee_editor", use_container_width=True, disabled=["직원ID", "소속지점"],
            column_config={"재직상태": st.column_config.SelectboxColumn("재직상태", options=["재직중", "퇴사"], required=True)})
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

def render_admin_dashboard(sales_df, settlement_df):
    st.subheader("📊 통합 대시보드")
    if sales_df.empty:
        st.warning("분석할 매출 데이터가 없습니다."); return
    sales_df['월'] = pd.to_datetime(sales_df['매출일자']).dt.strftime('%Y-%m')
    settlement_df['월'] = pd.to_datetime(settlement_df['정산일자']).dt.strftime('%Y-%m')
    monthly_sales, monthly_expenses = sales_df.groupby('월')['금액'].sum().rename('전체 매출'), settlement_df.groupby('월')['금액'].sum().rename('총 지출')
    summary_df = pd.concat([monthly_sales, monthly_expenses], axis=1).fillna(0).sort_index()
    summary_df['순이익'] = summary_df['전체 매출'] - summary_df['총 지출']
    if not summary_df.empty:
        latest = summary_df.iloc[-1]
        c1, c2, c3 = st.columns(3)
        c1.metric(f"💰 전체 매출 ({latest.name})", f"₩ {latest['전체 매출']:,.0f}")
        c2.metric(f"💸 총 지출 ({latest.name})", f"₩ {latest['총 지출']:,.0f}")
        c3.metric(f"📈 순이익 ({latest.name})", f"₩ {latest['순이익']:,.0f}")
        st.markdown("---"); st.write("📈 **월별 손익 추이**"); st.line_chart(summary_df)
    else:
        st.info("요약할 데이터가 없습니다.")

def render_admin_employee_management(employees_df):
    st.subheader("🗂️ 전 직원 관리")
    if employees_df.empty:
        st.warning("등록된 직원이 없습니다."); return
    stores = ['전체 지점'] + sorted(employees_df['소속지점'].unique().tolist())
    selected_store = st.selectbox("지점 선택", stores)
    display_df = employees_df if selected_store == '전체 지점' else employees_df[employees_df['소속지점'] == selected_store]
    st.markdown(f"**{selected_store}** 직원 목록")
    edited_subset_df = st.data_editor(display_df, hide_index=True, use_container_width=True, key="admin_emp_editor", disabled=["직원ID"])
    if st.button("직원 정보 저장", use_container_width=True, type="primary"):
        final_df = edited_subset_df if selected_store == '전체 지점' else pd.concat([employees_df[employees_df['소속지점'] != selected_store], edited_subset_df], ignore_index=True)
        if update_sheet_and_clear_cache(SHEET_NAMES["EMPLOYEE_MASTER"], final_df):
            st.success("전체 직원 정보가 업데이트되었습니다."); st.rerun()

def render_admin_settings(store_master_df):
    st.subheader("⚙️ 데이터 및 설정")
    st.write("👥 **지점 계정 관리**")
    if store_master_df.empty:
        st.error("지점 마스터 시트를 불러올 수 없습니다."); return
    st.info("지점 정보를 수정하거나 새 지점을 추가한 후 '계정 정보 저장' 버튼을 누르세요.")
    edited_stores_df = st.data_editor(store_master_df, num_rows="dynamic", use_container_width=True, key="admin_settings_editor")
    if st.button("지점 계정 정보 저장", use_container_width=True):
        if update_sheet_and_clear_cache(SHEET_NAMES["STORE_MASTER"], edited_stores_df):
            st.success("지점 계정 정보가 저장되었습니다."); st.rerun()
            
# =============================================================================
# 5. 메인 실행 로직
# =============================================================================
def main():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if not st.session_state['logged_in']:
        login_screen()
    else:
        if 'data_cache' not in st.session_state or not st.session_state['data_cache']:
            with st.spinner("데이터를 불러오는 중입니다..."):
                st.session_state['data_cache'] = {
                    "employees": load_data(SHEET_NAMES["EMPLOYEE_MASTER"]),
                    "attendance": load_data(SHEET_NAMES["ATTENDANCE_DETAIL"]),
                    "inventory": load_data(SHEET_NAMES["INVENTORY_LOG"]),
                    "inventory_master": load_data(SHEET_NAMES["INVENTORY_MASTER"]),
                    "inventory_detail_log": load_data(SHEET_NAMES["INVENTORY_DETAIL_LOG"]),
                    "sales": load_data(SHEET_NAMES["SALES_LOG"]),
                    "settlement": load_data(SHEET_NAMES["SETTLEMENT_LOG"]),
                    "stores": load_data(SHEET_NAMES["STORE_MASTER"]),
                }
        
        cache = st.session_state['data_cache']
        employees_df, attendance_df = cache['employees'], cache['attendance']
        inventory_df, inventory_master_df, inventory_detail_log_df = cache['inventory'], cache['inventory_master'], cache['inventory_detail_log']
        sales_df, settlement_df, stores_df = cache['sales'], cache['settlement'], cache['stores']
        
        user_info = st.session_state['user_info']
        role, name = user_info.get('역할', 'store'), user_info.get('지점명', '사용자')
        st.sidebar.success(f"**{name}** ({role})님, 환영합니다.")
        st.sidebar.markdown("---")
        if role != 'admin':
            check_health_cert_expiration(user_info, employees_df)
        if st.sidebar.button("로그아웃"):
            st.session_state.clear(); st.rerun()
        
        if role == 'admin':
            st.title("👑 관리자 페이지")
            admin_tabs = st.tabs(["📊 통합 대시보드", "🗂️ 전 직원 관리", "⚙️ 데이터 및 설정"])
            with admin_tabs[0]: render_admin_dashboard(sales_df, settlement_df)
            with admin_tabs[1]: render_admin_employee_management(employees_df)
            with admin_tabs[2]: render_admin_settings(stores_df)
        else:
            st.title(f"🏢 {name} 지점 관리 시스템")
            store_tabs = st.tabs(["⏰ 월별 근무기록", "📦 월말 재고확인", "👥 직원 정보"])
            with store_tabs[0]:
                render_store_attendance(user_info, employees_df, attendance_df)
            with store_tabs[1]:
                render_store_inventory_check(user_info, inventory_master_df, inventory_df, inventory_detail_log_df)
            with store_tabs[2]:
                render_store_employee_info(user_info, employees_df)

if __name__ == "__main__":
    main()
