# streamlit_app.py

import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime, date, time, timedelta
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
        # 숫자 변환이 필요한 컬럼들에 대해 일괄 처리
        for col in df.columns:
            if any(keyword in col for keyword in ['금액', '평가액', '총시간']):
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        return df
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 로딩 오류: {e}")
        return pd.DataFrame()

def update_sheet(sheet_name, df):
    try:
        spreadsheet = get_gspread_client().open_by_key(st.secrets["gcp_service_account"]["SPREADSHEET_KEY"])
        worksheet = spreadsheet.worksheet(sheet_name)
        # datetime 객체를 문자열로 변환
        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
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
        worksheet.append_rows(rows_df_str.values.tolist(), value_input_option='USER_ENTERED')
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트 행 추가 오류: {e}"); return False

# =============================================================================
# 0-1. 헬퍼 함수
# =============================================================================

def check_health_cert_expiration(user_info):
    store_name = user_info['지점명']
    all_employees_df = load_data("직원마스터")
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
                st.error("아이디 또는 비밀번호가 올바지 않습니다.")

# =============================================================================
# 2. 지점 (Store) 페이지 기능
# =============================================================================

def render_store_attendance(user_info):
    st.subheader("⏰ 월별 근무기록 관리")
    store_name = user_info['지점명']

    # --- UI 스타일 ---
    st.markdown("""
    <style>
        .stDataFrame th, .stDataFrame td { text-align: center !important; }
        #summary-table .stDataFrame th[aria-colindex="1"], 
        #summary-table .stDataFrame td[aria-colindex="1"] { text-align: left !important; }
    </style>
    """, unsafe_allow_html=True)

    # --- 1. 데이터 준비 ---
    employees_df = load_data("직원마스터")
    store_employees_df = employees_df[(employees_df['소속지점'] == store_name) & (employees_df['재직상태'] == '재직중')]
    attendance_detail_df = load_data("근무기록_상세")

    if store_employees_df.empty:
        st.warning("먼저 '직원마스터' 시트에 해당 지점의 재직중인 직원을 등록해주세요."); return

    # --- 2. 월 선택 및 전체 근무 데이터 생성 ---
    selected_month_str = st.selectbox("관리할 년/월 선택", 
        options=[(date.today() - relativedelta(months=i)).strftime('%Y년 / %m월') for i in range(12)])
    selected_month = datetime.strptime(selected_month_str, '%Y년 / %m월')
    start_date = selected_month.date()
    end_date = (start_date + relativedelta(months=1)) - timedelta(days=1)

    # 기본 근무 스케줄 생성
    default_records = []
    day_map = {'월': 0, '화': 1, '수': 2, '목': 3, '금': 4, '토': 5, '일': 6}
    for dt in pd.date_range(start_date, end_date):
        for _, emp in store_employees_df.iterrows():
            if dt.weekday() in [day_map.get(d.strip()) for d in emp.get('근무요일', '').split(',')]:
                default_records.append({
                    "기록ID": f"default_{dt.strftime('%y%m%d')}_{emp['이름']}", "근무일자": dt.strftime('%Y-%m-%d'),
                    "직원이름": emp['이름'], "구분": "정상근무", "출근시간": emp.get('기본출근', '09:00'),
                    "퇴근시간": emp.get('기본퇴근', '18:00'), "비고": "자동 생성"
                })
    default_df = pd.DataFrame(default_records)

    # 상세 기록과 기본 스케줄 병합
    if not attendance_detail_df.empty:
        attendance_detail_df['근무일자'] = pd.to_datetime(attendance_detail_df['근무일자']).dt.strftime('%Y-%m-%d')
        month_details = attendance_detail_df[
            (pd.to_datetime(attendance_detail_df['근무일자']).dt.strftime('%Y-%m') == selected_month.strftime('%Y-%m')) &
            (attendance_detail_df['지점명'] == store_name)
        ]
        # 기본 근무 기록 중, 상세 기록이 있는 직원의 기록은 제외
        for _, row in month_details.iterrows():
            default_df = default_df[~((default_df['근무일자'] == row['근무일자']) & (default_df['직원이름'] == row['직원이름']))]
        final_df = pd.concat([default_df, month_details]).sort_values(by=['근무일자', '직원이름']).reset_index(drop=True)
    else:
        final_df = default_df

    # 총 근무시간 계산
    final_df['총시간'] = final_df.apply(lambda row: 
        (datetime.strptime(row['퇴근시간'], '%H:%M') - datetime.strptime(row['출근시간'], '%H:%M')).total_seconds() / 3600
        if row['출근시간'] and row['퇴근시간'] else 0, axis=1)
    final_df['총시간'] = final_df['총시간'].apply(lambda h: h + 24 if h < 0 else h)


    # --- 3. 월별 근무 현황 요약표 (읽기 전용) ---
    st.markdown("##### 🗓️ **월별 근무 현황 요약**")
    summary_pivot = final_df.pivot_table(index='직원이름', columns=pd.to_datetime(final_df['근무일자']).dt.day, values='총시간', aggfunc='sum')
    st.dataframe(summary_pivot.style.format("{:.1f}", na_rep=""), use_container_width=True)


    # --- 4. 일일 근무 기록 상세 관리 (핵심 기능) ---
    st.markdown("---")
    st.markdown("##### ✍️ **일일 근무 기록 상세 관리**")

    # 날짜 선택
    selected_date = st.date_input("관리할 날짜 선택", value=start_date, min_value=start_date, max_value=end_date)
    
    # 선택된 날짜의 기록 필터링
    daily_records_df = final_df[final_df['근무일자'] == selected_date.strftime('%Y-%m-%d')].copy()
    daily_records_df.drop(columns=['총시간'], inplace=True, errors='ignore') # 총시간은 자동계산되므로 편집에서 제외

    st.info(f"**{selected_date.strftime('%Y년 %m월 %d일')}**의 근무 기록을 아래 표에서 직접 수정, 추가, 삭제할 수 있습니다.")

    # 데이터 에디터 UI
    edited_df = st.data_editor(
        daily_records_df,
        key=f"editor_{selected_date}",
        num_rows="dynamic", # 행 추가/삭제 활성화
        use_container_width=True,
        column_config={
            "직원이름": st.column_config.SelectboxColumn("이름", options=store_employees_df['이름'].unique(), required=True),
            "구분": st.column_config.SelectboxColumn("구분", options=["정상근무", "연장근무", "유급휴가", "무급휴가", "결근"], required=True),
            "출근시간": st.column_config.TextColumn("출근(HH:MM)", required=True),
            "퇴근시간": st.column_config.TextColumn("퇴근(HH:MM)", required=True),
        },
        disabled=["기록ID", "근무일자"], # 수정 불가 컬럼
        hide_index=True
    )

    # 저장 버튼
    if st.button(f"💾 {selected_date.strftime('%m월 %d일')} 기록 저장", type="primary", use_container_width=True):
        # 변경된 내용을 실제 '근무기록_상세' 시트에 반영
        
        # 1. 원본 상세 기록에서 오늘 날짜 기록은 모두 제거 (덮어쓰기 준비)
        other_days_details = attendance_detail_df[
            attendance_detail_df['근무일자'] != selected_date.strftime('%Y-%m-%d')
        ]

        # 2. 에디터에서 수정된 내용(edited_df)을 새롭게 추가할 상세 기록으로 준비
        new_details_to_add = edited_df.copy()
        new_details_to_add['지점명'] = store_name
        
        # '자동 생성'된 기록은 상세 기록으로 전환 (고유 ID 부여)
        for index, row in new_details_to_add.iterrows():
            if 'default_' in str(row['기록ID']):
                new_details_to_add.at[index, '기록ID'] = f"manual_{row['근무일자'].replace('-', '')}_{row['직원이름']}_{int(datetime.now().timestamp()) + index}"
        
        # 3. 다른 날짜 기록과 오늘 수정된 기록을 합쳐서 최종본 생성
        final_sheet_df = pd.concat([other_days_details, new_details_to_add], ignore_index=True)

        # 4. 구글 시트 업데이트
        if update_sheet("근무기록_상세", final_sheet_df):
            st.success("변경사항이 성공적으로 저장되었습니다.")
            st.rerun()
        else:
            st.error("저장 중 오류가 발생했습니다.")


    # --- 5. 월별 근무 시간 집계 ---
    st.markdown("---")
    st.markdown("##### 📊 **직원별 근무 시간 집계**")
    if not final_df.empty:
        summary = final_df.pivot_table(index='직원이름', columns='구분', values='총시간', aggfunc='sum', fill_value=0)
        required_cols = ['정상근무', '연장근무']
        for col in required_cols:
            if col not in summary.columns: summary[col] = 0
        summary['총합'] = summary[required_cols].sum(axis=1)
        display_summary = summary[required_cols + ['총합']].reset_index().rename(columns={'직원이름':'이름'})
        
        formatter = {'정상근무': '{:.1f} 시간', '연장근무': '{:.1f} 시간', '총합': '{:.1f} 시간'}
        st.markdown('<div id="summary-table">', unsafe_allow_html=True)
        st.dataframe(display_summary.style.format(formatter), use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

def render_store_settlement(user_info):
    st.subheader("💰 정산 및 재고")
    store_name = user_info['지점명']
    today = date.today()
    options = [(today - relativedelta(months=i)).strftime('%Y-%m') for i in range(12)]
    
    # --- 기능 개선: 일일 매출 및 지출 입력 ---
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
            
            log_submitted = st.form_submit_button("💾 일일 기록 저장", use_container_width=True, type="primary")
            if log_submitted:
                sales_data, expense_data = [], []
                if sales_card > 0: sales_data.append([log_date, store_name, '카드매출', sales_card, log_date.strftime('%A')])
                if sales_cash > 0: sales_data.append([log_date, store_name, '현금매출', sales_cash, log_date.strftime('%A')])
                if sales_delivery > 0: sales_data.append([log_date, store_name, '배달매출', sales_delivery, log_date.strftime('%A')])
                
                if exp_food > 0: expense_data.append([log_date, store_name, '식자재', '식자재 구매', exp_food, user_info['지점ID']])
                if exp_sga_amount > 0: expense_data.append([log_date, store_name, '판관비', exp_sga_cat, exp_sga_amount, user_info['지점ID']])

                success_sales, success_expenses = True, True
                if sales_data:
                    sales_df = pd.DataFrame(sales_data, columns=['매출일자', '지점명', '매출유형', '금액', '요일'])
                    if not append_rows("매출_로그", sales_df): success_sales = False
                if expense_data:
                    expenses_df = pd.DataFrame(expense_data, columns=['정산일자', '지점명', '대분류', '소분류', '금액', '담당자'])
                    if not append_rows("일일정산_로그", expenses_df): success_expenses = False
                
                if success_sales and success_expenses: st.success(f"{log_date.strftime('%Y-%m-%d')}의 매출/지출 내역이 성공적으로 저장되었습니다.")
                else: st.error("저장 중 오류가 발생했습니다.")


    with st.expander("📦 **월말 재고 자산 평가액 입력**"):
        selected_month_inv = st.selectbox("재고 평가 년/월 선택", options=options, key="inv_month")
        inventory_value = st.number_input("해당 월의 최종 재고 평가액(원)을 입력하세요.", min_value=0, step=10000)
        if st.button("💾 재고액 저장", type="primary", key="inv_save"):
            inventory_log_df = load_data("월말재고_로그")
            if '평가년월' in inventory_log_df.columns: inventory_log_df['평가년월'] = pd.to_datetime(inventory_log_df['평가년월']).dt.strftime('%Y-%m')
            
            existing_indices = inventory_log_df[(inventory_log_df['평가년월'] == selected_month_inv) & (inventory_log_df['지점명'] == store_name)].index
            if not existing_indices.empty:
                inventory_log_df.loc[existing_indices, ['재고평가액', '입력일시']] = [inventory_value, datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            else:
                new_row = pd.DataFrame([{'평가년월': selected_month_inv, '지점명': store_name, '재고평가액': inventory_value, '입력일시': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '입력자': user_info['지점ID']}])
                inventory_log_df = pd.concat([inventory_log_df, new_row], ignore_index=True)
            
            if update_sheet("월말재고_로그", inventory_log_df): st.success(f"{selected_month_inv}의 재고 평가액이 {inventory_value:,.0f}원으로 저장되었습니다.")

    st.markdown("---")
    st.markdown("##### 🧾 **월별 손익계산서**")
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
    sga_expenses_df = store_settlement[store_settlement['대분류'] != '식자재']
    sga_expenses = sga_expenses_df['금액'].sum()
    
    begin_inv_series = inventory_log[(inventory_log['평가년월'] == prev_month_str) & (inventory_log['지점명'] == store_name)]['재고평가액']
    end_inv_series = inventory_log[(inventory_log['평가년월'] == selected_month_pl) & (inventory_log['지점명'] == store_name)]['재고평가액']
    
    begin_inv = begin_inv_series.iloc[0] if not begin_inv_series.empty else 0
    if begin_inv == 0:
        st.info(f"💡 {prev_month_str} (전월) 재고 데이터가 없어 기초 재고가 0원으로 계산됩니다.")
    end_inv = end_inv_series.iloc[0] if not end_inv_series.empty else 0
    
    cogs = begin_inv + food_purchase - end_inv; gross_profit = total_sales - cogs; operating_profit = gross_profit - sga_expenses
    
    # --- UI 개선: st.metric 및 시각화 ---
    st.markdown("###### **요약**")
    m1, m2, m3 = st.columns(3)
    m1.metric("💰 총매출", f"{total_sales:,.0f} 원")
    m2.metric("📈 매출 총이익", f"{gross_profit:,.0f} 원", f"{((gross_profit / total_sales * 100) if total_sales else 0):.1f}%")
    m3.metric("🏆 영업이익", f"{operating_profit:,.0f} 원", f"{((operating_profit / total_sales * 100) if total_sales else 0):.1f}%")

    st.markdown("###### **상세 내역**")
    summary_data = {
        '항목': ['I. 총매출', '(-) II. 식자재 원가 (COGS)', '(=) III. 매출 총이익', '(-) IV. 판매비와 관리비', '(=) V. 영업이익'],
        '금액 (원)': [total_sales, cogs, gross_profit, sga_expenses, operating_profit]
    }
    summary_df = pd.DataFrame(summary_data)
    st.table(summary_df.style.format({'금액 (원)': '{:,.0f}'}))

    v1, v2 = st.columns(2)
    with v1:
        st.markdown("###### **수익 구조 (폭포 차트)**")
        if total_sales > 0:
            fig_waterfall = px.bar(summary_df, x='항목', y='금액 (원)', 
                                   title='매출부터 영업이익까지 흐름',
                                   text=[f"{v:,.0f}" for v in summary_df['금액 (원)']])
            fig_waterfall.update_traces(textposition='outside')
            st.plotly_chart(fig_waterfall, use_container_width=True)

    with v2:
        st.markdown("###### **판매비와 관리비 구성 (파이 차트)**")
        if not sga_expenses_df.empty:
            sga_by_cat = sga_expenses_df.groupby('소분류')['금액'].sum().reset_index()
            fig_pie = px.pie(sga_by_cat, values='금액', names='소분류', title='비용 항목별 비중')
            st.plotly_chart(fig_pie, use_container_width=True)


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

            submitted = st.form_submit_button("💾 신규 직원 저장", type="primary")
            if submitted:
                if not emp_name: st.error("직원 이름은 반드시 입력해야 합니다.")
                elif not emp_contact.isdigit(): st.error("연락처는 숫자만 입력해주세요.")
                else:
                    emp_id = f"{store_name.replace('점','')}_{emp_name}_{emp_start_date.strftime('%y%m%d')}"
                    new_employee_data = pd.DataFrame([{
                        "직원ID": emp_id, "이름": emp_name, "소속지점": store_name, 
                        "직책": emp_position, "입사일": emp_start_date.strftime('%Y-%m-%d'), 
                        "연락처": emp_contact, "보건증만료일": emp_health_cert_date.strftime('%Y-%m-%d'),
                        "재직상태": emp_status, "근무요일": emp_work_days, 
                        "기본출근": emp_start_time.strftime('%H:%M'), "기본퇴근": emp_end_time.strftime('%H:%M')
                    }])
                    if append_rows("직원마스터", new_employee_data):
                        st.success(f"'{emp_name}' 직원의 정보가 성공적으로 등록되었습니다.")

    st.markdown("---")
    st.markdown("##### **우리 지점 직원 목록 (정보 수정/퇴사 처리)**")
    all_employees_df = load_data("직원마스터")
    store_employees_df = all_employees_df[all_employees_df['소속지점'] == store_name].copy()

    if store_employees_df.empty:
        st.info("등록된 직원이 없습니다."); return

    # st.data_editor를 통한 간편 수정
    st.info("💡 아래 표에서 직접 값을 수정하고 '변경사항 저장' 버튼을 누르세요.")
    edited_df = st.data_editor(
        store_employees_df, 
        key="employee_editor", 
        use_container_width=True,
        # 수정 불가능하게 할 컬럼 설정
        disabled=["직원ID", "소속지점"],
        num_rows="dynamic" # 행 추가/삭제 기능 (필요 시 사용)
    )

    if st.button("💾 변경사항 저장", type="primary", use_container_width=True):
        if update_sheet("직원마스터", edited_df):
            st.success("직원 정보가 성공적으로 업데이트되었습니다."); st.rerun()

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

    # --- 개선: 중요 알림(보건증)을 사이드바로 이동 ---
    if role != 'admin':
        check_health_cert_expiration(user_info)

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
        st.title(f"🏢 {name} 지점 관리 시스템")
        store_tabs = st.tabs(["⏰ 월별 근무기록", "💰 정산 및 재고", "👥 직원 정보"])
        with store_tabs[0]: render_store_attendance(user_info)
        with store_tabs[1]: render_store_settlement(user_info)
        with store_tabs[2]: render_store_employee_info(user_info)










