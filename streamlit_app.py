import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime, date, time, timedelta
from dateutil.relativedelta import relativedelta
import re
import holidays
import io
import hashlib

# =============================================================================
# 0. 기본 설정 및 상수 정의
# =============================================================================

# 페이지 기본 설정
st.set_page_config(page_title="산카쿠 통합 관리 시스템", page_icon="🏢", layout="wide")

# 시트 이름을 상수로 관리
SHEET_NAMES = {
    "STORE_MASTER": "지점마스터",
    "EMPLOYEE_MASTER": "직원마스터",
    "ATTENDANCE_DETAIL": "근무기록_상세",
    "SALES_LOG": "매출_로그",
    "SETTLEMENT_LOG": "일일정산_로그",
    "INVENTORY_LOG": "월말재고_로그"
}

# =============================================================================
# 1. 구글 시트 연결 및 데이터 처리 함수
# =============================================================================

@st.cache_resource
def get_gspread_client():
    """Google Sheets API 클라이언트를 생성하고 캐시합니다."""
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)

@st.cache_data(ttl=300)
def load_data(sheet_name):
    """지정된 시트에서 데이터를 불러오고 전처리 후 캐시합니다."""
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
        st.error(f"'{sheet_name}' 시트 로딩 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

def update_sheet(sheet_name, df):
    """시트 전체를 새로운 데이터프레임으로 업데이트합니다."""
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        df_str = df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.update([df_str.columns.values.tolist()] + df_str.values.tolist(), value_input_option='USER_ENTERED')
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 업데이트 중 오류가 발생했습니다: {e}")
        return False

def append_rows(sheet_name, rows_df):
    """시트에 새로운 행들을 추가합니다."""
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        rows_df_str = rows_df.astype(str).replace('nan', '').replace('NaT', '')
        worksheet.append_rows(rows_df_str.values.tolist(), value_input_option='USER_ENTERED')
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트에 행을 추가하는 중 오류가 발생했습니다: {e}")
        return False

# =============================================================================
# 2. 헬퍼 함수
# =============================================================================

def check_health_cert_expiration(user_info):
    """사이드바에 보건증 만료 임박 직원 목록을 표시합니다."""
    store_name = user_info['지점명']
    all_employees_df = load_data(SHEET_NAMES["EMPLOYEE_MASTER"])
    store_employees_df = all_employees_df[(all_employees_df['소속지점'] == store_name) & (all_employees_df['재직상태'] == '재직중')]
    if store_employees_df.empty: return

    store_employees_df['보건증만료일'] = pd.to_datetime(store_employees_df['보건증만료일'], errors='coerce')
    today = datetime.now()
    expiring_soon_list = []
    for _, row in store_employees_df.iterrows():
        if pd.notna(row['보건증만료일']) and today <= row['보건증만료일'] < (today + timedelta(days=30)):
            expiring_soon_list.append(f"- **{row['이름']}**: {row['보건증만료일'].strftime('%Y-%m-%d')} 만료")
    
    if expiring_soon_list:
        st.sidebar.warning("🚨 보건증 만료 임박\n" + "\n".join(expiring_soon_list))

# =============================================================================
# 3. 로그인 화면
# =============================================================================

def login_screen():
    # CSS를 사용하여 로그인 폼을 화면 중앙에 배치하고 스타일을 적용합니다.
    st.markdown("""
        <style>
            .main > div:first-child {
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
                height: 100vh;
            }
            .login-box {
                background: white;
                padding: 2.5rem 3rem;
                border-radius: 1rem;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
                width: 100%;
                max-width: 420px;
            }
            .login-box h1 {
                text-align: center;
                margin-bottom: 2rem;
            }
        </style>
    """, unsafe_allow_html=True)

    users_df = load_data(SHEET_NAMES["STORE_MASTER"])
    if users_df.empty:
        st.error(f"'{SHEET_NAMES['STORE_MASTER']}' 시트를 불러올 수 없습니다.")
        st.stop()
    
    st.markdown('<div class="login-box">', unsafe_allow_html=True)
    
    st.title("🏢 산카쿠 통합 관리 시스템")
    st.markdown("<br>", unsafe_allow_html=True)

    with st.form("login_form"):
        username = st.text_input("아이디 (지점ID)", placeholder="지점 아이디를 입력하세요")
        password = st.text_input("비밀번호", type="password", placeholder="비밀번호를 입력하세요")
        submitted = st.form_submit_button("로그인", use_container_width=True, type="primary")
        
        if submitted:
            user_info_df = users_df[(users_df['지점ID'] == username.strip()) & (users_df['지점PW'] == password)]
            if not user_info_df.empty:
                st.session_state['logged_in'] = True
                st.session_state['user_info'] = user_info_df.iloc[0].to_dict()
                st.rerun()
            else:
                st.error("아이디 또는 비밀번호가 올바르지 않습니다.")

    st.markdown('</div>', unsafe_allow_html=True)

# =============================================================================
# 4. 지점 (Store) 페이지 기능 - 월별 근무기록 관리 (최종 UI)
# =============================================================================
def render_store_attendance(user_info):
    st.subheader("⏰ 월별 근무기록 관리")
    store_name = user_info['지점명']

    employees_df = load_data(SHEET_NAMES["EMPLOYEE_MASTER"])
    store_employees_df = employees_df[(employees_df['소속지점'] == store_name) & (employees_df['재직상태'] == '재직중')]

    if store_employees_df.empty:
        st.warning("먼저 '직원 정보' 탭에서 '재직중' 상태의 직원을 한 명 이상 등록해주세요.")
        return

    # --- UI 개선: 컨트롤 위젯을 메인 화면 상단에 재배치 ---
    col1, col2 = st.columns(2)
    with col1:
        selected_month_str = st.selectbox("관리할 년/월 선택",
            options=[(date.today() - relativedelta(months=i)).strftime('%Y년 / %m월') for i in range(12)])
    
    selected_month = datetime.strptime(selected_month_str, '%Y년 / %m월')
    start_date = selected_month.date()
    end_date = (start_date + relativedelta(months=1)) - timedelta(days=1)

    with col2:
        selected_date = st.date_input("관리할 날짜 선택", value=start_date, min_value=start_date, max_value=end_date)
    # ----------------------------------------------------

    attendance_detail_df = load_data(SHEET_NAMES["ATTENDANCE_DETAIL"])
    month_records_df = pd.DataFrame()

    if not attendance_detail_df.empty and '근무일자' in attendance_detail_df.columns:
        month_records_df = attendance_detail_df[
            (pd.to_datetime(attendance_detail_df['근무일자'], errors='coerce').dt.strftime('%Y-%m') == selected_month.strftime('%Y-%m')) &
            (attendance_detail_df['지점명'] == store_name)
        ].copy()

    def calculate_duration(row):
        try:
            start_t = datetime.strptime(str(row['출근시간']), '%H:%M')
            end_t = datetime.strptime(str(row['퇴근시간']), '%H:%M')
            duration = (end_t - start_t).total_seconds() / 3600
            return duration + 24 if duration < 0 else duration
        except (TypeError, ValueError):
            return 0
    
    if not month_records_df.empty:
        month_records_df['총시간'] = month_records_df.apply(calculate_duration, axis=1)
        
    st.markdown("---")

    # 1. 월별 현황 요약
    st.markdown("##### 🗓️ **월별 근무 현황 요약**")
    if not month_records_df.empty:
        summary_pivot = month_records_df.pivot_table(index='직원이름', columns=pd.to_datetime(month_records_df['근무일자']).dt.day, values='총시간', aggfunc='sum')
        all_days_cols = range(1, end_date.day + 1)
        summary_pivot = summary_pivot.reindex(columns=all_days_cols)
        summary_pivot.columns = [f"{day}일" for day in all_days_cols]

        kr_holidays = holidays.KR(years=selected_month.year)
        def style_day_columns(col):
            try:
                d = date(selected_month.year, selected_month.month, int(col.name.replace('일', '')))
                if d in kr_holidays: return ['background-color: #ffcccc'] * len(col)
                if d.weekday() == 6: return ['background-color: #ffdddd'] * len(col)
                if d.weekday() == 5: return ['background-color: #ddeeff'] * len(col)
                return [''] * len(col)
            except (ValueError, TypeError):
                return [''] * len(col)
        
        st.dataframe(summary_pivot.style.apply(style_day_columns, axis=0).format(lambda val: f"{val:.1f}" if pd.notna(val) else ""), use_container_width=True)

        st.markdown("##### 📊 **직원별 근무 시간 집계**")
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

    # 2. 일일 기록 상세 관리
    st.markdown("##### ✍️ **일일 근무 기록 상세 관리**")
    if month_records_df.empty:
        st.info(f"**{selected_month_str}**에 대한 근무 기록이 없습니다. 아래에서 기본 스케줄을 생성해주세요.")
        st.dataframe(store_employees_df[['이름', '직책', '근무요일', '기본출근', '기본퇴근']], use_container_width=True, hide_index=True)
        if st.button(f"🗓️ {selected_month_str} 기본 스케줄 생성하기", type="primary"):
            new_records = []
            day_map = {'월': 0, '화': 1, '수': 2, '목': 3, '금': 4, '토': 5, '일': 6}
            for _, emp in store_employees_df.iterrows():
                work_days_str = emp.get('근무요일', '')
                cleaned_days = re.sub(r'요일|[,\s/]+', ' ', work_days_str).split()
                work_day_indices = {day_map[day[0]] for day in cleaned_days if day and day[0] in day_map}
                for dt in pd.date_range(start_date, end_date):
                    if dt.weekday() in work_day_indices:
                        record_id = f"manual_{dt.strftime('%y%m%d')}_{emp['이름']}_{int(datetime.now().timestamp())}"
                        new_records.append({"기록ID": record_id, "지점명": store_name, "근무일자": dt.strftime('%Y-%m-%d'), "직원이름": emp['이름'], "구분": "정상근무", "출근시간": emp.get('기본출근', '09:00'), "퇴근시간": emp.get('기본퇴근', '18:00'), "비고": ""})
            if new_records:
                new_df = pd.DataFrame(new_records)
                if update_sheet(SHEET_NAMES["ATTENDANCE_DETAIL"], pd.concat([attendance_detail_df, new_df], ignore_index=True)):
                    st.success("기본 스케줄이 생성되었습니다."); st.rerun()
        return

    st.info(f"**{selected_date.strftime('%Y년 %m월 %d일')}** 기록을 아래 표에서 직접 수정, 추가, 삭제하세요.")
    daily_records_df = month_records_df[month_records_df['근무일자'] == selected_date.strftime('%Y-%m-%d')].copy()
    daily_records_df.drop(columns=['총시간', '지점명'], inplace=True, errors='ignore'); daily_records_df.reset_index(drop=True, inplace=True)

    edited_df = st.data_editor(daily_records_df, key=f"editor_{selected_date}", num_rows="dynamic", use_container_width=True,
        column_config={"기록ID": None, "근무일자": None,
            "직원이름": st.column_config.SelectboxColumn("이름", options=list(store_employees_df['이름'].unique()), required=True),
            "구분": st.column_config.SelectboxColumn("구분", options=["정상근무", "연장근무", "유급휴가", "무급휴가", "결근"], required=True),
            "출근시간": st.column_config.TextColumn("출근(HH:MM)", default="09:00", required=True),
            "퇴근시간": st.column_config.TextColumn("퇴근(HH:MM)", default="18:00", required=True),
            "비고": st.column_config.TextColumn("비고")},
        hide_index=True, column_order=["직원이름", "구분", "출근시간", "퇴근시간", "비고"])

    if st.button(f"💾 {selected_date.strftime('%m월 %d일')} 기록 저장", type="primary", use_container_width=True):
        error_found = False
        if edited_df[["직원이름", "구분", "출근시간", "퇴근시간"]].isnull().values.any():
            st.error("필수 항목(이름, 구분, 출/퇴근 시간)이 비어있습니다."); error_found = True
        time_pattern = re.compile(r'^([01]\d|2[0-3]):([0-5]\d)$')
        invalid_rows = [r['직원이름'] for _, r in edited_df.iterrows() if not time_pattern.match(str(r['출근시간'])) or not time_pattern.match(str(r['퇴근시간']))]
        if invalid_rows:
            st.error(f"시간 형식이 잘못되었습니다 (HH:MM). 직원: {', '.join(set(invalid_rows))}"); error_found = True
        if not error_found:
            df_check = edited_df.copy()
            df_check['start_dt'] = pd.to_datetime(selected_date.strftime('%Y-%m-%d') + ' ' + df_check['출근시간'], errors='coerce')
            df_check['end_dt'] = pd.to_datetime(selected_date.strftime('%Y-%m-%d') + ' ' + df_check['퇴근시간'], errors='coerce')
            df_check.loc[df_check['end_dt'] <= df_check['start_dt'], 'end_dt'] += timedelta(days=1)
            overlap_employees = []
            for name, group in df_check.groupby('직원이름'):
                group = group.sort_values('start_dt').reset_index()
                if any(group.loc[i, 'end_dt'] > group.loc[i+1, 'start_dt'] for i in range(len(group) - 1)):
                    overlap_employees.append(name)
            if overlap_employees:
                st.error(f"근무 시간이 겹칩니다. 직원: {', '.join(set(overlap_employees))}"); error_found = True
        if not error_found:
            other_records = attendance_detail_df[attendance_detail_df['근무일자'] != selected_date.strftime('%Y-%m-%d')]
            new_details = edited_df.copy()
            for i, row in new_details.iterrows():
                if pd.isna(row.get('기록ID')) or row.get('기록ID') == '':
                    new_details.at[i, '기록ID'] = f"manual_{selected_date.strftime('%y%m%d')}_{row['직원이름']}_{int(datetime.now().timestamp()) + i}"
                new_details.at[i, '지점명'] = store_name; new_details.at[i, '근무일자'] = selected_date.strftime('%Y-%m-%d')
            if update_sheet(SHEET_NAMES["ATTENDANCE_DETAIL"], pd.concat([other_records, new_details], ignore_index=True)):
                st.success("변경사항이 성공적으로 저장되었습니다."); st.rerun()
                
def render_store_settlement(user_info):
    st.subheader("💰 정산 및 재고")
    store_name = user_info['지점명']
    today = date.today()
    options = [(today - relativedelta(months=i)).strftime('%Y-%m') for i in range(12)]
    
    with st.expander("📈 **일일 매출 및 지출 입력**"):
        with st.form("daily_log_form", clear_on_submit=True):
            log_date = st.date_input("기록할 날짜", date.today())
            st.markdown("###### **매출 입력**")
            c1, c2, c3 = st.columns(3)
            sales_card = c1.number_input("카드 매출", min_value=0, step=1000)
            sales_cash = c2.number_input("현금 매출", min_value=0, step=1000)
            sales_delivery = c3.number_input("배달 매출", min_value=0, step=1000)
            st.markdown("###### **지출 입력**")
            c4, c5, c6 = st.columns(3)
            exp_food = c4.number_input("식자재 구매", min_value=0, step=1000)
            exp_sga_cat = c5.selectbox("기타 비용 항목", ["공과금", "소모품비", "수리비", "인건비", "기타"])
            exp_sga_amount = c6.number_input("기타 비용 금액", min_value=0, step=1000)
            
            if st.form_submit_button("💾 일일 기록 저장", use_container_width=True, type="primary"):
                sales_data, expense_data = [], []
                if sales_card > 0: sales_data.append([log_date, store_name, '카드매출', sales_card, log_date.strftime('%A')])
                if sales_cash > 0: sales_data.append([log_date, store_name, '현금매출', sales_cash, log_date.strftime('%A')])
                if sales_delivery > 0: sales_data.append([log_date, store_name, '배달매출', sales_delivery, log_date.strftime('%A')])
                if exp_food > 0: expense_data.append([log_date, store_name, '식자재', '식자재 구매', exp_food, user_info['지점ID']])
                if exp_sga_amount > 0: expense_data.append([log_date, store_name, '판관비', exp_sga_cat, exp_sga_amount, user_info['지점ID']])

                if sales_data: append_rows(SHEET_NAMES["SALES_LOG"], pd.DataFrame(sales_data, columns=['매출일자', '지점명', '매출유형', '금액', '요일']))
                if expense_data: append_rows(SHEET_NAMES["SETTLEMENT_LOG"], pd.DataFrame(expense_data, columns=['정산일자', '지점명', '대분류', '소분류', '금액', '담당자']))
                st.success(f"{log_date.strftime('%Y-%m-%d')}의 기록이 저장되었습니다.")

    with st.expander("📦 **월말 재고 자산 평가액 입력**"):
        selected_month_inv = st.selectbox("재고 평가 년/월 선택", options=options, key="inv_month")
        inventory_value = st.number_input("해당 월의 최종 재고 평가액(원)을 입력하세요.", min_value=0, step=10000)
        if st.button("💾 재고액 저장", type="primary", key="inv_save"):
            inventory_log_df = load_data(SHEET_NAMES["INVENTORY_LOG"])
            if '평가년월' in inventory_log_df.columns: inventory_log_df['평가년월'] = pd.to_datetime(inventory_log_df['평가년월'], errors='coerce').dt.strftime('%Y-%m')
            
            existing_indices = inventory_log_df[(inventory_log_df['평가년월'] == selected_month_inv) & (inventory_log_df['지점명'] == store_name)].index
            if not existing_indices.empty:
                inventory_log_df.loc[existing_indices, ['재고평가액', '입력일시']] = [inventory_value, datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            else:
                new_row = pd.DataFrame([{'평가년월': selected_month_inv, '지점명': store_name, '재고평가액': inventory_value, '입력일시': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '입력자': user_info['지점ID']}])
                inventory_log_df = pd.concat([inventory_log_df, new_row], ignore_index=True)
            if update_sheet(SHEET_NAMES["INVENTORY_LOG"], inventory_log_df): st.success(f"{selected_month_inv}의 재고 평가액이 {inventory_value:,.0f}원으로 저장되었습니다.")

    st.markdown("---")
    st.markdown("##### 🧾 **월별 손익계산서**")
    selected_month_pl = st.selectbox("정산표 조회 년/월 선택", options=options, key="pl_month")
    
    sales_log = load_data(SHEET_NAMES["SALES_LOG"]); settlement_log = load_data(SHEET_NAMES["SETTLEMENT_LOG"]); inventory_log = load_data(SHEET_NAMES["INVENTORY_LOG"])
    if sales_log.empty or settlement_log.empty:
        st.warning("정산표 생성을 위한 매출 또는 지출 데이터가 부족합니다."); return

    selected_dt = datetime.strptime(selected_month_pl, '%Y-%m'); prev_month_str = (selected_dt - relativedelta(months=1)).strftime('%Y-%m')
    total_sales = sales_log[(pd.to_datetime(sales_log['매출일자'], errors='coerce').dt.strftime('%Y-%m') == selected_month_pl) & (sales_log['지점명'] == store_name)]['금액'].sum()
    store_settlement = settlement_log[(pd.to_datetime(settlement_log['정산일자'], errors='coerce').dt.strftime('%Y-%m') == selected_month_pl) & (settlement_log['지점명'] == store_name)]
    food_purchase = store_settlement[store_settlement['대분류'] == '식자재']['금액'].sum()
    sga_expenses = store_settlement[store_settlement['대분류'] != '식자재']['금액'].sum()
    
    begin_inv_series = inventory_log[(pd.to_datetime(inventory_log['평가년월'], errors='coerce').dt.strftime('%Y-%m') == prev_month_str) & (inventory_log['지점명'] == store_name)]['재고평가액']
    end_inv_series = inventory_log[(pd.to_datetime(inventory_log['평가년월'], errors='coerce').dt.strftime('%Y-%m') == selected_month_pl) & (inventory_log['지점명'] == store_name)]['재고평가액']
    
    begin_inv = begin_inv_series.iloc[0] if not begin_inv_series.empty else 0
    if begin_inv == 0: st.info(f"💡 {prev_month_str}(전월) 재고 데이터가 없어 기초 재고가 0원으로 계산됩니다.")
    end_inv = end_inv_series.iloc[0] if not end_inv_series.empty else 0
    
    cogs = begin_inv + food_purchase - end_inv
    gross_profit = total_sales - cogs
    operating_profit = gross_profit - sga_expenses
    
    m1, m2, m3 = st.columns(3)
    m1.metric("💰 총매출", f"{total_sales:,.0f} 원")
    m2.metric("📈 매출 총이익", f"{gross_profit:,.0f} 원", f"{((gross_profit / total_sales * 100) if total_sales else 0):.1f}%")
    m3.metric("🏆 영업이익", f"{operating_profit:,.0f} 원", f"{((operating_profit / total_sales * 100) if total_sales else 0):.1f}%")


def render_store_employee_info(user_info):
    st.subheader("👥 직원 정보 관리")
    store_name = user_info['지점명']
    
    with st.expander("➕ **신규 직원 등록하기**"):
        with st.form("new_employee_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                emp_name = st.text_input("이름")
                emp_position = st.text_input("직책", "직원")
                emp_contact = st.text_input("연락처 (숫자만 입력)")
                emp_status = st.selectbox("재직상태", ["재직중", "퇴사"])
            with col2:
                emp_start_date = st.date_input("입사일", date.today())
                emp_health_cert_date = st.date_input("보건증만료일", date.today() + timedelta(days=365))
                emp_work_days = st.text_input("근무요일 (예: 월,화,수,목,금)")
            
            col3, col4 = st.columns(2)
            with col3: emp_start_time = st.time_input("기본출근", time(9, 0))
            with col4: emp_end_time = st.time_input("기본퇴근", time(18, 0))

            if st.form_submit_button("💾 신규 직원 저장", type="primary"):
                if not emp_name: st.error("직원 이름은 반드시 입력해야 합니다.")
                elif not emp_contact.isdigit(): st.error("연락처는 숫자만 입력해주세요.")
                else:
                    emp_id = f"{store_name.replace('점','')}_{emp_name}_{emp_start_date.strftime('%y%m%d')}"
                    new_employee_data = pd.DataFrame([{"직원ID": emp_id, "이름": emp_name, "소속지점": store_name, "직책": emp_position, "입사일": emp_start_date.strftime('%Y-%m-%d'), "연락처": emp_contact, "보건증만료일": emp_health_cert_date.strftime('%Y-%m-%d'), "재직상태": emp_status, "근무요일": emp_work_days, "기본출근": emp_start_time.strftime('%H:%M'), "기본퇴근": emp_end_time.strftime('%H:%M')}])
                    if append_rows(SHEET_NAMES["EMPLOYEE_MASTER"], new_employee_data):
                        st.success(f"'{emp_name}' 직원의 정보가 등록되었습니다."); st.rerun()

    st.markdown("---")
    st.markdown("##### **우리 지점 직원 목록 (정보 수정/퇴사 처리)**")
    
    all_employees_df = load_data(SHEET_NAMES["EMPLOYEE_MASTER"])
    store_employees_df = all_employees_df[all_employees_df['소속지점'] == store_name].copy()

    if not store_employees_df.empty:
        st.info("💡 아래 표에서 직접 값을 수정하고 '변경사항 저장' 버튼을 누르세요.")
        
        edited_df = st.data_editor(
            store_employees_df, key="employee_editor", use_container_width=True, disabled=["직원ID", "소속지점"]
        )
        
        if st.button("💾 변경사항 저장", type="primary", use_container_width=True):
            other_stores_df = all_employees_df[all_employees_df['소속지점'] != store_name]
            updated_full_df = pd.concat([other_stores_df, edited_df], ignore_index=True)
            
            if update_sheet(SHEET_NAMES["EMPLOYEE_MASTER"], updated_full_df):
                st.success("직원 정보가 성공적으로 업데이트되었습니다."); st.rerun()

# =============================================================================
# 5. 관리자 (Admin) 페이지 기능
# =============================================================================

def render_admin_dashboard():
    st.subheader("📊 통합 대시보드")
    
    sales_df = load_data(SHEET_NAMES["SALES_LOG"])
    settlement_df = load_data(SHEET_NAMES["SETTLEMENT_LOG"])

    if sales_df.empty:
        st.warning("분석할 매출 데이터가 없습니다.")
        return

    sales_df['매출일자'] = pd.to_datetime(sales_df['매출일자'])
    sales_df['월'] = sales_df['매출일자'].dt.strftime('%Y-%m')
    
    settlement_df['정산일자'] = pd.to_datetime(settlement_df['정산일자'])
    settlement_df['월'] = settlement_df['정산일자'].dt.strftime('%Y-%m')

    monthly_sales = sales_df.groupby('월')['금액'].sum().rename('전체 매출')
    monthly_expenses = settlement_df.groupby('월')['금액'].sum().rename('총 지출')
    
    summary_df = pd.concat([monthly_sales, monthly_expenses], axis=1).fillna(0).sort_index()
    summary_df['순이익'] = summary_df['전체 매출'] - summary_df['총 지출']
    
    if not summary_df.empty:
        latest_month_data = summary_df.iloc[-1]
        col1, col2, col3 = st.columns(3)
        col1.metric(f"💰 전체 매출 ({latest_month_data.name})", f"₩ {latest_month_data['전체 매출']:,.0f}")
        col2.metric(f"💸 총 지출 ({latest_month_data.name})", f"₩ {latest_month_data['총 지출']:,.0f}")
        col3.metric(f"📈 순이익 ({latest_month_data.name})", f"₩ {latest_month_data['순이익']:,.0f}")

        st.markdown("---")
        st.write("📈 **월별 손익 추이**")
        st.line_chart(summary_df)
    else:
        st.info("요약할 데이터가 없습니다.")

def render_admin_employee_management():
    st.subheader("🗂️ 전 직원 관리")
    
    all_employees_df = load_data(SHEET_NAMES["EMPLOYEE_MASTER"])
    if all_employees_df.empty:
        st.warning("등록된 직원이 없습니다."); return

    stores = ['전체 지점'] + sorted(all_employees_df['소속지점'].unique().tolist())
    selected_store = st.selectbox("지점 선택", stores)
    
    display_df = all_employees_df.copy()
    if selected_store != '전체 지점':
        display_df = display_df[display_df['소속지점'] == selected_store]
        
    st.markdown(f"**{selected_store}** 직원 목록")
    edited_subset_df = st.data_editor(display_df, hide_index=True, use_container_width=True, key="admin_emp_editor")

    if st.button("직원 정보 저장", use_container_width=True, type="primary"):
        if selected_store == '전체 지점':
            final_df = edited_subset_df
        else:
            other_stores_df = all_employees_df[all_employees_df['소속지점'] != selected_store]
            final_df = pd.concat([other_stores_df, edited_subset_df], ignore_index=True)

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
# 6. 메인 실행 로직
# =============================================================================

def main():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False

    if not st.session_state['logged_in']:
        login_screen()
    else:
        # 로그인 후에는 사이드바와 메인 컨텐츠를 정상적으로 표시
        st.markdown("""<style>.main > div:first-child {height: auto;}</style>""", unsafe_allow_html=True)
        
        user_info = st.session_state['user_info']
        role = user_info.get('역할', 'store')
        name = user_info.get('지점명', '사용자')
        
        st.sidebar.success(f"**{name}** ({role})님, 환영합니다.")
        st.sidebar.markdown("---")

        if role != 'admin':
            check_health_cert_expiration(user_info)

        if st.sidebar.button("로그아웃"):
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.rerun()

        if role == 'admin':
            st.title("👑 관리자 페이지")
            admin_tabs = st.tabs(["📊 통합 대시보드", "🗂️ 전 직원 관리", "⚙️ 데이터 및 설정"])
            with admin_tabs[0]: render_admin_dashboard()
            with admin_tabs[1]: render_admin_employee_management()
            with admin_tabs[2]: render_admin_settings()
        else:
            st.title(f"🏢 {name} 지점 관리 시스템")
            store_tabs = st.tabs(["⏰ 월별 근무기록", "💰 정산 및 재고", "👥 직원 정보"])
            with store_tabs[0]: render_store_attendance(user_info)
            with store_tabs[1]: render_store_settlement(user_info)
            with store_tabs[2]: render_store_employee_info(user_info)

if __name__ == "__main__":
    main()



