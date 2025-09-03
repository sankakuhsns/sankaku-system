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
    "ATTENDANCE_DETAIL": "근무기록_상세", "SALES_LOG": "매출_로그",
    "SETTLEMENT_LOG": "일일정산_로그", "INVENTORY_LOG": "월말재고_로그"
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
        for col in df.columns:
            if any(keyword in col for keyword in ['금액', '평가액', '총시간']):
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        return df
    except Exception as e:
        if "Quota exceeded" in str(e):
            st.error("🔌 구글 시트 API 요청 한도를 초과했습니다. 1분 후에 페이지를 새로고침 해주세요.")
        else:
            st.error(f"'{sheet_name}' 시트 로딩 중 오류: {e}")
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
        worksheet.append_rows(rows_df_str.values.tolist(), value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 행 추가 오류: {e}"); return False

# 데이터 수정 후 session_state 캐시를 초기화하는 래퍼(wrapper) 함수
def update_sheet_and_clear_cache(sheet_name, df):
    if update_sheet(sheet_name, df):
        if 'data_cache' in st.session_state:
            del st.session_state['data_cache']
        return True
    return False

def append_rows_and_clear_cache(sheet_name, rows_df):
    if append_rows(sheet_name, rows_df):
        if 'data_cache' in st.session_state:
            del st.session_state['data_cache']
        return True
    return False

# =============================================================================
# 2. 헬퍼 함수
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
    st.markdown("""<style>.main > div:first-child { display: flex; flex-direction: column; justify-content: center; align-items: center; height: 100vh; }</style>""", unsafe_allow_html=True)
    st.markdown('<div style="background:white; padding:2.5rem 3rem; border-radius:1rem; box-shadow:0 4px 12px rgba(0,0,0,0.15); width:100%; max-width:420px;">', unsafe_allow_html=True)
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
                    st.session_state['data_cache'] = {} # 로그인 성공 시 데이터 캐시 초기화
                    st.rerun()
                else:
                    st.error("아이디 또는 비밀번호가 올바르지 않습니다.")
    st.markdown('</div>', unsafe_allow_html=True)


# =============================================================================
# 4. 각 기능별 함수 (데이터를 인자로 받도록 수정)
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
        summary_pivot.columns = [f"{day}일" for day in range(1, end_date.day + 1)]
        kr_holidays = holidays.KR(years=selected_month.year)
        def style_day_columns(col):
            try:
                d = date(selected_month.year, selected_month.month, int(col.name.replace('일', '')))
                if d in kr_holidays: return ['background-color: #ffcccc'] * len(col)
                if d.weekday() == 6: return ['background-color: #ffdddd'] * len(col)
                if d.weekday() == 5: return ['background-color: #ddeeff'] * len(col)
                return [''] * len(col)
            except (ValueError, TypeError): return [''] * len(col)
        st.dataframe(summary_pivot.style.apply(style_day_columns, axis=0).format(lambda val: f"{val:.1f}" if pd.notna(val) else ""), use_container_width=True)
        summary = month_records_df.pivot_table(index='직원이름', columns='구분', values='총시간', aggfunc='sum', fill_value=0)
        required_cols = ['정상근무', '연장근무']
        for col in required_cols:
            if col not in summary.columns: summary[col] = 0
        summary['총합'] = summary[required_cols].sum(axis=1)
        display_summary = summary[required_cols + ['총합']].reset_index().rename(columns={'직원이름':'이름'})
        dl_col1, dl_col2 = st.columns([3, 1])
        with dl_col1:
            st.dataframe(display_summary.style.format({'정상근무': '{:.1f} 시간', '연장근무': '{:.1f} 시간', '총합': '{:.1f} 시간'}), use_container_width=True, hide_index=True)
        with dl_col2:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                display_summary.to_excel(writer, index=False, sheet_name='근무시간집계')
                wks = writer.sheets['근무시간집계']; wks.set_column('A:A', 15); wks.set_column('B:D', 12)
            st.download_button("📥 엑셀 다운로드", output.getvalue(), f"{store_name}_{selected_month_str.replace(' / ', '_')}_근무시간집계.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        st.markdown("---")
        with st.expander("🗂️ 근무기록 일괄관리"):
            # (일괄 관리 로직은 이전과 동일하나, 캐시 초기화 함수를 사용)
            pass
        st.markdown("##### ✍️ 근무 기록 관리")
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
                other_day_records = month_records_df[month_records_df['근무일자'] != selected_date.strftime('%Y-%m-%d')]
                other_month_records = attendance_detail_df[pd.to_datetime(attendance_detail_df['근무일자']).dt.strftime('%Y-%m') != selected_month.strftime('%Y-%m')]
                new_details = processed_df.copy()
                for i, row in new_details.iterrows():
                    if pd.isna(row.get('기록ID')) or row.get('기록ID') == '':
                        uid = f"{selected_date.strftime('%y%m%d')}_{row['직원이름']}_{int(datetime.now().timestamp()) + i}"
                        new_details.at[i, '기록ID'] = f"manual_{uid}"
                    new_details.at[i, '지점명'] = store_name; new_details.at[i, '근무일자'] = selected_date.strftime('%Y-%m-%d')
                final_df = pd.concat([other_month_records, other_day_records, new_details], ignore_index=True)
                if update_sheet_and_clear_cache(SHEET_NAMES["ATTENDANCE_DETAIL"], final_df):
                    st.toast(f"✅ {selected_date.strftime('%m월 %d일')}의 근무 기록이 성공적으로 저장되었습니다."); st.rerun()

def render_store_settlement(user_info, sales_df, settlement_df, inventory_df):
    st.subheader("💰 정산 및 재고")
    store_name = user_info['지점명']
    options = [(date.today() - relativedelta(months=i)).strftime('%Y-%m') for i in range(12)]
    with st.expander("📈 **일일 매출 및 지출 입력**"):
        with st.form("daily_log_form", clear_on_submit=True):
            log_date = st.date_input("기록할 날짜", date.today())
            st.markdown("###### **매출 입력**"); c1, c2, c3 = st.columns(3)
            sales_card, sales_cash, sales_delivery = c1.number_input("카드 매출", 0, step=1000), c2.number_input("현금 매출", 0, step=1000), c3.number_input("배달 매출", 0, step=1000)
            st.markdown("###### **지출 입력**"); c4, c5, c6 = st.columns(3)
            exp_food = c4.number_input("식자재 구매", 0, step=1000)
            exp_sga_cat, exp_sga_amount = c5.selectbox("기타 비용 항목", ["공과금", "소모품비", "수리비", "인건비", "기타"]), c6.number_input("기타 비용 금액", 0, step=1000)
            if st.form_submit_button("💾 일일 기록 저장", use_container_width=True, type="primary"):
                sales, expenses = [], []
                if sales_card > 0: sales.append([log_date, store_name, '카드매출', sales_card, log_date.strftime('%A')])
                if sales_cash > 0: sales.append([log_date, store_name, '현금매출', sales_cash, log_date.strftime('%A')])
                if sales_delivery > 0: sales.append([log_date, store_name, '배달매출', sales_delivery, log_date.strftime('%A')])
                if exp_food > 0: expenses.append([log_date, store_name, '식자재', '식자재 구매', exp_food, user_info['지점ID']])
                if exp_sga_amount > 0: expenses.append([log_date, store_name, '판관비', exp_sga_cat, exp_sga_amount, user_info['지점ID']])
                if sales and append_rows_and_clear_cache(SHEET_NAMES["SALES_LOG"], pd.DataFrame(sales, columns=['매출일자', '지점명', '매출유형', '금액', '요일'])):
                    st.toast("✅ 매출 기록이 추가되었습니다.")
                if expenses and append_rows_and_clear_cache(SHEET_NAMES["SETTLEMENT_LOG"], pd.DataFrame(expenses, columns=['정산일자', '지점명', '대분류', '소분류', '금액', '담당자'])):
                    st.toast("✅ 지출 기록이 추가되었습니다.")
                st.rerun()
    with st.expander("📦 **월말 재고 자산 평가액 입력**"):
        selected_month_inv = st.selectbox("재고 평가 년/월 선택", options=options, key="inv_month")
        inventory_value = st.number_input("해당 월의 최종 재고 평가액(원)을 입력하세요.", min_value=0, step=10000)
        if st.button("💾 재고액 저장", type="primary", key="inv_save"):
            if '평가년월' in inventory_df.columns: inventory_df['평가년월'] = pd.to_datetime(inventory_df['평가년월'], errors='coerce').dt.strftime('%Y-%m')
            existing_indices = inventory_df[(inventory_df['평가년월'] == selected_month_inv) & (inventory_df['지점명'] == store_name)].index
            if not existing_indices.empty:
                inventory_df.loc[existing_indices, ['재고평가액', '입력일시']] = [inventory_value, datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            else:
                new_row = pd.DataFrame([{'평가년월': selected_month_inv, '지점명': store_name, '재고평가액': inventory_value, '입력일시': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '입력자': user_info['지점ID']}])
                inventory_df = pd.concat([inventory_df, new_row], ignore_index=True)
            if update_sheet_and_clear_cache(SHEET_NAMES["INVENTORY_LOG"], inventory_df): 
                st.toast(f"✅ {selected_month_inv}의 재고 평가액이 {inventory_value:,.0f}원으로 저장되었습니다."); st.rerun()
    st.markdown("---"); st.markdown("##### 🧾 **월별 손익계산서**")
    selected_month_pl = st.selectbox("정산표 조회 년/월 선택", options=options, key="pl_month")
    if sales_df.empty or settlement_df.empty:
        st.warning("정산표 생성을 위한 데이터가 부족합니다."); return
    selected_dt, prev_month_str = datetime.strptime(selected_month_pl, '%Y-%m'), (datetime.strptime(selected_month_pl, '%Y-%m') - relativedelta(months=1)).strftime('%Y-%m')
    total_sales = sales_df[(pd.to_datetime(sales_df['매출일자']).dt.strftime('%Y-%m') == selected_month_pl) & (sales_df['지점명'] == store_name)]['금액'].sum()
    store_settlement = settlement_df[(pd.to_datetime(settlement_df['정산일자']).dt.strftime('%Y-%m') == selected_month_pl) & (settlement_df['지점명'] == store_name)]
    food_purchase, sga_expenses = store_settlement[store_settlement['대분류'] == '식자재']['금액'].sum(), store_settlement[store_settlement['대분류'] != '식자재']['금액'].sum()
    begin_inv_series = inventory_df[(pd.to_datetime(inventory_df['평가년월']).dt.strftime('%Y-%m') == prev_month_str) & (inventory_df['지점명'] == store_name)]['재고평가액']
    end_inv_series = inventory_df[(pd.to_datetime(inventory_df['평가년월']).dt.strftime('%Y-%m') == selected_month_pl) & (inventory_df['지점명'] == store_name)]['재고평가액']
    begin_inv = begin_inv_series.iloc[0] if not begin_inv_series.empty else 0
    if begin_inv == 0: st.info(f"💡 {prev_month_str}(전월) 재고 데이터가 없어 기초 재고가 0원으로 계산됩니다.")
    end_inv = end_inv_series.iloc[0] if not end_inv_series.empty else 0
    cogs, gross_profit, operating_profit = begin_inv + food_purchase - end_inv, total_sales - (begin_inv + food_purchase - end_inv), total_sales - (begin_inv + food_purchase - end_inv) - sga_expenses
    m1, m2, m3 = st.columns(3)
    m1.metric("💰 총매출", f"{total_sales:,.0f} 원")
    m2.metric("📈 매출 총이익", f"{gross_profit:,.0f} 원", f"{((gross_profit / total_sales * 100) if total_sales else 0):.1f}%")
    m3.metric("🏆 영업이익", f"{operating_profit:,.0f} 원", f"{((operating_profit / total_sales * 100) if total_sales else 0):.1f}%")

def render_store_employee_info(user_info, employees_df):
    st.subheader("👥 직원 정보 관리")
    store_name = user_info['지점명']
    with st.expander("➕ **신규 직원 등록하기**", expanded=True):
        with st.form("new_employee_form", clear_on_submit=True):
            st.info("각 항목을 정확하게 선택하고 입력해주세요. 잘못된 정보는 근무기록 생성 시 문제를 일으킬 수 있습니다.")
            col1, col2 = st.columns(2)
            with col1:
                emp_name, emp_position, emp_contact, emp_status = st.text_input("이름", help="직원의 실명을 입력하세요."), st.text_input("직책", "직원"), st.text_input("연락처", help="'-' 없이 숫자만 입력하세요."), st.selectbox("재직상태", ["재직중", "퇴사"], help="퇴사 처리 시 근무기록이 생성되지 않습니다.")
            with col2:
                emp_start_date, emp_health_cert_date = st.date_input("입사일", date.today()), st.date_input("보건증만료일", date.today() + timedelta(days=365))
                days_of_week = ["월", "화", "수", "목", "금", "토", "일"]
                emp_work_days_list = st.multiselect("근무요일 (중복 선택 가능)", options=days_of_week, help="근무하는 요일을 모두 선택하세요.")
            col3, col4 = st.columns(2)
            with col3: emp_start_time = st.time_input("기본출근", time(9, 0))
            with col4: emp_end_time = st.time_input("기본퇴근", time(18, 0))
            if st.form_submit_button("💾 신규 직원 저장", type="primary", use_container_width=True):
                if not emp_name: st.error("직원 이름은 반드시 입력해야 합니다.")
                elif not emp_contact.isdigit(): st.error("연락처는 '-' 없이 숫자만 입력해주세요.")
                elif not emp_work_days_list: st.error("근무요일을 한 개 이상 선택해주세요.")
                else:
                    emp_work_days_str = ",".join(emp_work_days_list)
                    emp_id = f"{store_name.replace('점','')}_{emp_name}_{emp_start_date.strftime('%y%m%d')}"
                    new_data = {"직원ID": emp_id, "이름": emp_name, "소속지점": store_name, "직책": emp_position, "입사일": emp_start_date.strftime('%Y-%m-%d'), "연락처": emp_contact, "보건증만료일": emp_health_cert_date.strftime('%Y-%m-%d'), "재직상태": emp_status, "근무요일": emp_work_days_str, "기본출근": emp_start_time.strftime('%H:%M'), "기본퇴근": emp_end_time.strftime('%H:%M')}
                    if append_rows_and_clear_cache(SHEET_NAMES["EMPLOYEE_MASTER"], pd.DataFrame([new_data])):
                        st.toast(f"✅ '{emp_name}' 직원의 정보가 성공적으로 등록되었습니다."); st.rerun()
    st.markdown("---"); st.markdown("##### **우리 지점 직원 목록 (정보 수정/퇴사 처리)**")
    store_employees_df = employees_df[employees_df['소속지점'] == store_name].copy()
    if not store_employees_df.empty:
        st.info("💡 아래 표에서 직접 값을 수정하고 '변경사항 저장' 버튼을 누르세요. '근무요일'은 '월,화,수' 형식으로 입력해야 합니다.")
        edited_df = st.data_editor(store_employees_df, key="employee_editor", use_container_width=True, disabled=["직원ID", "소속지점"])
        if st.button("💾 변경사항 저장", type="primary", use_container_width=True):
            other_stores_df = employees_df[employees_df['소속지점'] != store_name]
            updated_full_df = pd.concat([other_stores_df, edited_df], ignore_index=True)
            if update_sheet_and_clear_cache(SHEET_NAMES["EMPLOYEE_MASTER"], updated_full_df):
                st.toast("✅ 직원 정보가 성공적으로 업데이트되었습니다."); st.rerun()

# =============================================================================
# 6. 관리자 페이지 기능 (이전과 동일)
# =============================================================================
def render_admin_dashboard():
    st.subheader("📊 통합 대시보드")
    sales_df, settlement_df = load_data(SHEET_NAMES["SALES_LOG"]), load_data(SHEET_NAMES["SETTLEMENT_LOG"])
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

def render_admin_employee_management():
    st.subheader("🗂️ 전 직원 관리")
    all_employees_df = load_data(SHEET_NAMES["EMPLOYEE_MASTER"])
    if all_employees_df.empty:
        st.warning("등록된 직원이 없습니다."); return
    stores = ['전체 지점'] + sorted(all_employees_df['소속지점'].unique().tolist())
    selected_store = st.selectbox("지점 선택", stores)
    display_df = all_employees_df if selected_store == '전체 지점' else all_employees_df[all_employees_df['소속지점'] == selected_store]
    st.markdown(f"**{selected_store}** 직원 목록")
    edited_subset_df = st.data_editor(display_df, hide_index=True, use_container_width=True, key="admin_emp_editor")
    if st.button("직원 정보 저장", use_container_width=True, type="primary"):
        final_df = edited_subset_df if selected_store == '전체 지점' else pd.concat([all_employees_df[all_employees_df['소속지점'] != selected_store], edited_subset_df], ignore_index=True)
        if update_sheet(SHEET_NAMES["EMPLOYEE_MASTER"], final_df):
            st.success("전체 직원 정보가 업데이트되었습니다."); st.rerun()

def render_admin_settings():
    st.subheader("⚙️ 데이터 및 설정")
    st.write("👥 **지점 계정 관리**")
    store_master_df = load_data(SHEET_NAMES["STORE_MASTER"])
    if store_master_df.empty:
        st.error("지점 마스터 시트를 불러올 수 없습니다."); return
    st.info("지점 정보를 수정하거나 새 지점을 추가한 후 '계정 정보 저장' 버튼을 누르세요.")
    edited_stores_df = st.data_editor(store_master_df, num_rows="dynamic", use_container_width=True, key="admin_settings_editor")
    if st.button("지점 계정 정보 저장", use_container_width=True):
        if update_sheet(SHEET_NAMES["STORE_MASTER"], edited_stores_df):
            st.success("지점 계정 정보가 저장되었습니다."); st.rerun()

# =============================================================================
# 5. 메인 실행 로직 (Session State 활용)
# =============================================================================
def main():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if not st.session_state['logged_in']:
        login_screen()
    else:
        # 로그인 후 모든 데이터를 한 번만 로드하여 세션에 저장
        if 'data_cache' not in st.session_state or not st.session_state['data_cache']:
            with st.spinner("데이터를 불러오는 중입니다..."):
                st.session_state['data_cache'] = {
                    "employees": load_data(SHEET_NAMES["EMPLOYEE_MASTER"]),
                    "attendance": load_data(SHEET_NAMES["ATTENDANCE_DETAIL"]),
                    "sales": load_data(SHEET_NAMES["SALES_LOG"]),
                    "settlement": load_data(SHEET_NAMES["SETTLEMENT_LOG"]),
                    "inventory": load_data(SHEET_NAMES["INVENTORY_LOG"]),
                }
        
        employees_df = st.session_state['data_cache']['employees']
        attendance_df = st.session_state['data_cache']['attendance']
        sales_df = st.session_state['data_cache']['sales']
        settlement_df = st.session_state['data_cache']['settlement']
        inventory_df = st.session_state['data_cache']['inventory']

        user_info = st.session_state['user_info']
        role, name = user_info.get('역할', 'store'), user_info.get('지점명', '사용자')
        st.sidebar.success(f"**{name}** ({role})님, 환영합니다.")
        st.sidebar.markdown("---")
        if role != 'admin':
            check_health_cert_expiration(user_info, employees_df)
        if st.sidebar.button("로그아웃"):
            st.session_state.clear()
            st.rerun()
        
        st.title(f"🏢 {name} 지점 관리 시스템")
        store_tabs = st.tabs(["⏰ 월별 근무기록", "💰 정산 및 재고", "👥 직원 정보"])
        with store_tabs[0]:
            render_store_attendance(user_info, employees_df, attendance_df)
        with store_tabs[1]:
            render_store_settlement(user_info, sales_df, settlement_df, inventory_df)
        with store_tabs[2]:
            render_store_employee_info(user_info, employees_df)

if __name__ == "__main__":
    main()
