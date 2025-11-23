import warnings
from datetime import timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

DEFAULT_PLOTLY_CONFIG = {'displayModeBar': True, 'displaylogo': False}

warnings.filterwarnings('ignore')


@st.cache_data(ttl=18000)  # 缓存5小时 (5 * 60 * 60 = 18000秒)
def get_stock_data():
    """从数据库获取股票数据"""
    try:
        engine = create_engine(
            # f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"
            f"mysql+pymysql://{st.secrets.ops_db.username}:{st.secrets.ops_db.password}@{st.secrets.ops_db.host}/{st.secrets.ops_db.database}?charset=utf8mb4"
        )
        
        query = """
        SELECT 
            date,
            name,
            code,
            price,
            first_price,
            last_price,
            volume,
            turnover_rate,
            real_turnover_rate,
            limit_up_days,
            limit_up_statistics,
            first_seal_time,
            last_seal_time,
            dc_first_seal_time,
            dc_last_seal_time,
            first_break_time,
            last_break_time,
            dc_break_count as break_count,
            amplitude,
            industry
        FROM stock_model 
        WHERE date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
        ORDER BY date DESC, limit_up_days DESC
        """
        
        df = pd.read_sql(query, engine)
        engine.dispose()
        
        # 确保日期格式一致
        df['date'] = pd.to_datetime(df['date']).dt.date
        return df
    except Exception as e:
        st.error(f"数据库连接失败: {str(e)}")
        return pd.DataFrame()

def filter_data_by_date_range(df, date_range):
    """根据日期范围筛选数据"""
    if len(date_range) == 2:
        start_date = pd.to_datetime(date_range[0]).date()
        end_date = pd.to_datetime(date_range[1]).date()
        return df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()
    return df.copy()

def calculate_premium_rates(df):
    """计算涨停股票第二天的溢价率"""
    premium_data = []
    
    # 按股票代码和日期排序
    df_sorted = df.sort_values(['code', 'date']).reset_index(drop=True)
    
    for i in range(len(df_sorted) - 1):
        current_row = df_sorted.iloc[i]
        next_row = df_sorted.iloc[i + 1]
        
        # 确保是同一只股票且日期连续
        if (current_row['code'] == next_row['code'] and 
            pd.to_datetime(next_row['date']) - pd.to_datetime(current_row['date']) == pd.Timedelta(days=1) and
            pd.notna(current_row['limit_up_days'])):  # 确保当天是涨停股票
            
            # 计算第二天开盘价溢价率 (这里用当日收盘价作为开盘价近似)
            next_day_open_price = next_row['first_price']
            limit_up_price = current_row['price']
            
            # 第二天开盘价溢价率 = (次日开盘价 - 涨停价) / 涨停价 * 100%
            opening_premium_rate = ((next_day_open_price - limit_up_price) / limit_up_price) * 100
            
            # 第二天收盘价较前一天涨停价溢价率
            next_day_close_price = next_row['last_price']
            closing_premium_rate = ((next_day_close_price - limit_up_price) / limit_up_price) * 100
            
            premium_data.append({
                'date': pd.to_datetime(current_row['date']).date(),
                'code': current_row['code'],
                'name': current_row['name'],
                'limit_up_price': limit_up_price,
                'limit_up_statistics': current_row['limit_up_statistics'] if pd.notna(current_row['limit_up_statistics']) else '',
                'next_day_open_price': next_day_open_price,
                'next_day_close_price': next_day_close_price,
                'opening_premium_rate': opening_premium_rate,
                'closing_premium_rate': closing_premium_rate,
                'limit_up_days': current_row['limit_up_days'],
                'industry': current_row['industry']
            })
    
    return pd.DataFrame(premium_data)

def get_daily_premium_stats(premium_df):
    """获取每日溢价率统计数据"""
    if premium_df.empty:
        return pd.DataFrame()
    
    daily_stats = premium_df.groupby('date').agg({
        'opening_premium_rate': ['mean', 'median', 'count'],
        'closing_premium_rate': ['mean', 'median'],
        'limit_up_days': 'mean'
    }).round(2)
    
    # 展平多重索引列名
    daily_stats.columns = ['_'.join(col).strip() for col in daily_stats.columns.values]
    daily_stats = daily_stats.reset_index()
    
    # 重命名列
    daily_stats.rename(columns={
        'opening_premium_rate_mean': 'avg_opening_premium',
        'opening_premium_rate_median': 'median_opening_premium',
        'opening_premium_rate_count': 'stock_count',
        'closing_premium_rate_mean': 'avg_closing_premium',
        'closing_premium_rate_median': 'median_closing_premium',
        'limit_up_days_mean': 'avg_limit_up_days'
    }, inplace=True)
    
    return daily_stats

def calculate_sentiment_value(df):
    """计算每日情绪值"""
    daily_stats = []
    
    for date in df['date'].unique():
        day_data = df[df['date'] == date].copy()
        
        # 涨停股票数量 (归一化到0-100)
        limit_up_count = len(day_data[day_data['limit_up_days'].notna()])
        max_count = df.groupby('date').size().max()
        limit_up_score = min(100, (limit_up_count / max_count) * 100) * 0.3
        
        # 连板高度得分
        max_continuous = day_data['limit_up_days'].max()
        if pd.isna(max_continuous):
            continuous_score = 0
        else:
            continuous_score = min(100, (max_continuous / 10) * 100) * 0.25
        
        # 封板成功率
        total_stocks = len(day_data[day_data['limit_up_days'].notna()])
        if total_stocks > 0:
            success_count = len(day_data[(day_data['limit_up_days'].notna()) & (day_data['break_count'] == 0)])
            success_rate = (success_count / total_stocks) * 100
        else:
            success_rate = 0
        success_score = success_rate * 0.25
        
        # 换手率活跃度得分
        avg_turnover = day_data['real_turnover_rate'].mean()
        if pd.isna(avg_turnover):
            turnover_score = 0
        else:
            turnover_score = min(100, avg_turnover * 2) * 0.2
        
        sentiment_value = limit_up_score + continuous_score + success_score + turnover_score
        
        daily_stats.append({
            'date': pd.to_datetime(date).date(),
            'sentiment_value': sentiment_value,
            'limit_up_count': limit_up_count,
            'max_continuous': max_continuous if pd.notna(max_continuous) else 0,
            'success_rate': success_rate,
            'avg_turnover': avg_turnover if pd.notna(avg_turnover) else 0
        })
    
    return pd.DataFrame(daily_stats)

def create_chart_with_date_filter(title, df, chart_func, default_days=30):
    """创建带日期筛选的图表"""
    st.subheader(f"📊 {title}")
    
    # 日期范围选择器
    col1, col2, col3 = st.columns([2, 2, 6])
    with col1:
        if not df.empty:
            default_start = df['date'].max() - timedelta(days=default_days)
            start_date = st.date_input(
                "开始日期",
                value=default_start,
                min_value=df['date'].min(),
                max_value=df['date'].max(),
                key=f"start_{title}"
            )
    with col2:
        if not df.empty:
            end_date = st.date_input(
                "结束日期",
                value=df['date'].max(),
                min_value=start_date,
                max_value=df['date'].max(),
                key=f"end_{title}"
            )
    
    # 筛选数据
    if not df.empty and len(str(start_date)) > 0 and len(str(end_date)) > 0:
        filtered_df = filter_data_by_date_range(df, [start_date, end_date])
        
        # 显示数据量信息
        with col3:
            st.write(f"📈 数据范围: {start_date} 至 {end_date} (共 {len(filtered_df)} 条记录)")
        
        # 生成图表
        chart_func(filtered_df)
    else:
        st.info("暂无数据或日期范围无效")

def main():
    st.set_page_config(
        page_title="数据分析看板",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="collapsed"  # 隐藏侧边栏
    )
    
    st.title("📈 数据分析看板")
    st.markdown("---")
    
    # 获取数据
    with st.spinner("正在加载数据..."):
        df = get_stock_data()
    
    if df.empty:
        st.error("无法获取数据，请检查数据库连接")
        return
    
    # 计算统计数据
    sentiment_df = calculate_sentiment_value(df)
    premium_df = calculate_premium_rates(df)
    
    # 1. 每日涨停连板梯队表
    st.header("📊 每日连板梯队表")
    
    # 为连板梯队表添加日期选择
    col1, col2, col3 = st.columns([2, 2, 6])
    with col1:
        latest_date = df['date'].max()
        default_date = latest_date
        selected_date = st.date_input(
            "选择日期",
            value=default_date,
            min_value=df['date'].min(),
            max_value=df['date'].max(),
            key="ranking_date"
        )
    
    # 获取选定日期的涨停股票
    selected_stocks = df[df['date'] == pd.to_datetime(selected_date).date()].copy()
    
    # 按连板天数分组 - 包含连板天数为0但limit_up_statistics不等于"0/0"的股票
    ranking_data = []
    
    # 处理正常涨停股票（limit_up_days不为空）
    for days in sorted(selected_stocks['limit_up_days'].dropna().unique(), reverse=True):
        group = selected_stocks[selected_stocks['limit_up_days'] == days].copy()
        for _, stock in group.iterrows():
            # 格式化首次触板时间 (dc_first_seal_time)
            first_touch_time = ''
            if pd.notna(stock['dc_first_seal_time']):
                try:
                    # 处理timedelta格式的时间数据
                    if hasattr(stock['dc_first_seal_time'], 'total_seconds'):
                        # 如果是timedelta，转换为时间字符串
                        total_seconds = int(stock['dc_first_seal_time'].total_seconds())
                        hours = total_seconds // 3600
                        minutes = (total_seconds % 3600) // 60
                        seconds = total_seconds % 60
                        first_touch_time = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                    else:
                        # 如果已经是时间格式，直接格式化
                        first_touch_time = stock['dc_first_seal_time'].strftime('%H:%M:%S')
                except Exception as e:
                    # 如果是字符串或其他格式，直接显示
                    first_touch_time = str(stock['dc_first_seal_time']).split('.')[0]  # 去掉微秒部分
            
            # 格式化最后封板时间 (dc_last_seal_time)
            last_seal_time = ''
            if pd.notna(stock['dc_last_seal_time']):
                try:
                    # 处理timedelta格式的时间数据
                    if hasattr(stock['dc_last_seal_time'], 'total_seconds'):
                        # 如果是timedelta，转换为时间字符串
                        total_seconds = int(stock['dc_last_seal_time'].total_seconds())
                        hours = total_seconds // 3600
                        minutes = (total_seconds % 3600) // 60
                        seconds = total_seconds % 60
                        last_seal_time = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                    else:
                        # 如果已经是时间格式，直接格式化
                        last_seal_time = stock['dc_last_seal_time'].strftime('%H:%M:%S')
                except Exception as e:
                    # 如果是字符串或其他格式，直接显示
                    last_seal_time = str(stock['dc_last_seal_time']).split('.')[0]  # 去掉微秒部分
            
            ranking_data.append({
                '连板天数': int(days),
                '股票代码': stock['code'],
                '股票名称': stock['name'],
                '当前价格': f"{stock['price']:.2f}",
                '涨停统计': stock['limit_up_statistics'] if pd.notna(stock['limit_up_statistics']) else '',
                '换手率': f"{stock['turnover_rate']:.2f}%",  # 普通换手率
                '真实换手率': f"{stock['real_turnover_rate']:.2f}%",  # 真实换手率
                '首次触板时间': first_touch_time,  # 新的首次触板时间格式
                '最后封板时间': last_seal_time,   # 新的最后封板时间
                '炸板次数': int(stock['break_count']) if pd.notna(stock['break_count']) else 0,
                '行业': stock['industry'] if pd.notna(stock['industry']) else '未知'
            })
    
    # 处理特殊股票：连板天数为0但limit_up_statistics不等于"0/0"的股票
    special_stocks = selected_stocks[
        (selected_stocks['limit_up_days'].isna()) & 
        (selected_stocks['limit_up_statistics'].notna()) & 
        (selected_stocks['limit_up_statistics'] != '0/0')
    ].copy()
    
    for _, stock in special_stocks.iterrows():
        # 格式化首次触板时间 (dc_first_seal_time)
        first_touch_time = ''
        if pd.notna(stock['dc_first_seal_time']):
            try:
                # 如果已经是时间格式，直接格式化
                first_touch_time = stock['dc_first_seal_time'].strftime('%H:%M:%S')
            except:
                # 如果是字符串，直接显示
                first_touch_time = str(stock['dc_first_seal_time'])
        
        # 格式化最后封板时间 (dc_last_seal_time)
        last_seal_time = ''
        if pd.notna(stock['dc_last_seal_time']):
            try:
                # 如果已经是时间格式，直接格式化
                last_seal_time = stock['dc_last_seal_time'].strftime('%H:%M:%S')
            except:
                # 如果是字符串，直接显示
                last_seal_time = str(stock['dc_last_seal_time'])
        
        ranking_data.append({
            '连板天数': 0,  # 连板天数记为0
            '股票代码': stock['code'],
            '股票名称': stock['name'],
            '当前价格': f"{stock['price']:.2f}",
            '涨停统计': stock['limit_up_statistics'] if pd.notna(stock['limit_up_statistics']) else '',
            '换手率': f"{stock['turnover_rate']:.2f}%",  # 普通换手率
            '真实换手率': f"{stock['real_turnover_rate']:.2f}%",  # 真实换手率
            '首次触板时间': first_touch_time,  # 新的首次触板时间格式
            '最后封板时间': last_seal_time,   # 新的最后封板时间
            '炸板次数': int(stock['break_count']) if pd.notna(stock['break_count']) else 0,
            '行业': stock['industry'] if pd.notna(stock['industry']) else '未知'
        })
    
    if ranking_data:
        ranking_df = pd.DataFrame(ranking_data)
        st.dataframe(ranking_df, width='stretch', hide_index=True)
    else:
        st.info(f"{selected_date} 暂无涨停股票数据")
    
    # 连板高度趋势
    def create_continuous_height_chart(filtered_df):
        # 获取每日最高连板高度
        daily_max_continuous = filtered_df.groupby('date')['limit_up_days'].max().reset_index()
        daily_max_continuous['limit_up_days'] = daily_max_continuous['limit_up_days'].fillna(0)
        
        # 为每一天找到达到最高连板的股票信息
        stock_info_list = []
        for _, row in daily_max_continuous.iterrows():
            date = row['date']
            max_days = row['limit_up_days']
            
            # 找到当天达到最高连板的股票
            max_stocks = filtered_df[
                (filtered_df['date'] == date) & 
                (filtered_df['limit_up_days'] == max_days)
            ][['code', 'name']].drop_duplicates()
            
            # 格式化股票信息
            if not max_stocks.empty:
                stock_codes = max_stocks['code'].tolist()
                stock_names = max_stocks['name'].tolist()
                stock_info = '<br>'.join([f"{code} {name}" for code, name in zip(stock_codes, stock_names)])
                stock_info_list.append(stock_info)
            else:
                stock_info_list.append('')
        
        daily_max_continuous['stock_info'] = stock_info_list
        
        # 创建图表，添加悬停信息
        fig = px.line(
            daily_max_continuous,
            x='date',
            y='limit_up_days',
            title='每日最高连板高度趋势',
            labels={'date': '日期', 'limit_up_days': '连板高度'},
            hover_data={'stock_info': True}
        )
        daily_max_continuous['date_str'] = daily_max_continuous['date'].astype(str)
        _ticks = daily_max_continuous['date_str'].tolist()
        _tickvals_5 = [_ticks[i] for i in range(0, len(_ticks), 5)]
        if len(_ticks) > 0 and _ticks[-1] not in _tickvals_5:
            _tickvals_5.append(_ticks[-1])
        fig.update_xaxes(
            type='category',
            categoryorder='array',
            categoryarray=daily_max_continuous['date_str'],
            tickmode='array',
            tickvals=_tickvals_5,
            ticktext=_tickvals_5
        )
        # 自定义悬停模板
        fig.update_traces(
            hovertemplate='<b>日期:</b> %{x}<br>' +
                         '<b>连板高度:</b> %{y}天<br>' +
                         '<b>股票信息:</b><br>%{customdata[0]}<br>' +
                         '<extra></extra>',
            customdata=daily_max_continuous[['stock_info']]
        )
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True, config=DEFAULT_PLOTLY_CONFIG, key="continuous_height_chart")
    
    create_chart_with_date_filter("连板高度趋势", df, create_continuous_height_chart)
    
    # 涨停数量趋势
    def create_limit_up_counts_chart(filtered_df):
        if filtered_df.empty:
            st.info("暂无数据")
            return
        dates = sorted(filtered_df['date'].unique())
        rows = []
        for date in dates:
            d = filtered_df[filtered_df['date'] == date]['limit_up_days']
            rows.append({
                'date': pd.to_datetime(date).date(),
                '1板': int((d == 1).sum()),
                '2板': int((d == 2).sum()),
                '3板': int((d == 3).sum()),
                '4板': int((d == 4).sum()),
                '4板以上': int((d > 4).sum()),
                '总涨停': int(d.notna().sum())
            })
        counts_df = pd.DataFrame(rows)
        counts_df['date_str'] = counts_df['date'].astype(str)
        _ticks = counts_df['date_str'].tolist()
        _tickvals_5 = [_ticks[i] for i in range(0, len(_ticks), 5)]
        if len(_ticks) > 0 and _ticks[-1] not in _tickvals_5:
            _tickvals_5.append(_ticks[-1])
        left_col, right_col = st.columns(2)
        with left_col:
            fig_left = px.line(
                counts_df,
                x='date_str',
                y=['1板', '总涨停'],
                title='1板与总涨停数量趋势',
                labels={'date_str': '日期', 'value': '数量', 'variable': '类别'}
            )
            fig_left.update_xaxes(
                type='category',
                categoryorder='array',
                categoryarray=counts_df['date_str'],
                tickmode='array',
                tickvals=_tickvals_5,
                ticktext=_tickvals_5
            )
            fig_left.update_layout(height=400)
            st.plotly_chart(fig_left, use_container_width=True, config=DEFAULT_PLOTLY_CONFIG, key="limit_up_counts_left")
        with right_col:
            fig_right = px.line(
                counts_df,
                x='date_str',
                y=['2板', '3板', '4板', '4板以上'],
                title='2/3/4及以上涨停数量趋势',
                labels={'date_str': '日期', 'value': '数量', 'variable': '梯队'}
            )
            fig_right.update_xaxes(
                type='category',
                categoryorder='array',
                categoryarray=counts_df['date_str'],
                tickmode='array',
                tickvals=_tickvals_5,
                ticktext=_tickvals_5
            )
            fig_right.update_layout(height=400)
            st.plotly_chart(fig_right, use_container_width=True, config=DEFAULT_PLOTLY_CONFIG, key="limit_up_counts_right")

    create_chart_with_date_filter("涨停数量趋势", df, create_limit_up_counts_chart)

    # 晋级率趋势
    def create_advancement_rate_chart(filtered_df):
        dates = sorted(filtered_df['date'].unique())
        rows = []
        for i in range(1, len(dates)):
            date = dates[i]
            prev_date = dates[i-1]
            today = filtered_df[filtered_df['date'] == date]
            prev_day = filtered_df[filtered_df['date'] == prev_date]
            d1 = (prev_day['limit_up_days'] == 1).sum()
            n2 = (today['limit_up_days'] == 2).sum()
            r12 = (n2 / d1 * 100) if d1 > 0 else 0
            d2 = (prev_day['limit_up_days'] == 2).sum()
            n3 = (today['limit_up_days'] == 3).sum()
            r23 = (n3 / d2 * 100) if d2 > 0 else 0
            d3 = (prev_day['limit_up_days'] == 3).sum()
            n4 = (today['limit_up_days'] == 4).sum()
            r34 = (n4 / d3 * 100) if d3 > 0 else 0
            d4 = (prev_day['limit_up_days'] == 4).sum()
            n5 = (today['limit_up_days'] == 5).sum()
            r45 = (n5 / d4 * 100) if d4 > 0 else 0
            d5p = (prev_day['limit_up_days'] >= 5).sum()
            n6p = (today['limit_up_days'] >= 6).sum()
            r5p6p = (n6p / d5p * 100) if d5p > 0 else 0
            rows.append({
                'date': pd.to_datetime(date).date(),
                'rate_1_to_2': r12,
                'rate_2_to_3': r23,
                'rate_3_to_4': r34,
                'rate_4_to_5': r45,
                'rate_5_plus': r5p6p,
                'n_1_to_2': n2,
                'd_1_to_2': d1,
                'n_2_to_3': n3,
                'd_2_to_3': d2,
                'n_3_to_4': n4,
                'd_3_to_4': d3,
                'n_4_to_5': n5,
                'd_4_to_5': d4,
                'n_5_plus': n6p,
                'd_5_plus': d5p,
                'd_total': d1 + d2 + d3 + d4 + d5p,
                'n_total': n2 + n3 + n4 + n5 + n6p
            })
        if len(rows) == 0:
            st.info("暂无晋级率数据")
            return
        df_rates = pd.DataFrame(rows)
        df_rates['overall_rate'] = np.where(df_rates['d_total'] > 0, df_rates['n_total'] / df_rates['d_total'] * 100, 0)
        df_rates['ma3_1_to_2'] = df_rates['rate_1_to_2'].rolling(window=3).mean()
        df_rates['ma3_2_to_3'] = df_rates['rate_2_to_3'].rolling(window=3).mean()
        df_rates['ma3_3_to_4'] = df_rates['rate_3_to_4'].rolling(window=3).mean()
        df_rates['ma3_4_to_5'] = df_rates['rate_4_to_5'].rolling(window=3).mean()
        df_rates['ma3_5_plus'] = df_rates['rate_5_plus'].rolling(window=3).mean()
        df_rates['ma3_overall'] = df_rates['overall_rate'].rolling(window=3).mean()
        df_rates['ma5_1_to_2'] = df_rates['rate_1_to_2'].rolling(window=5).mean()
        df_rates['ma5_2_to_3'] = df_rates['rate_2_to_3'].rolling(window=5).mean()
        df_rates['ma5_3_to_4'] = df_rates['rate_3_to_4'].rolling(window=5).mean()
        df_rates['ma5_4_to_5'] = df_rates['rate_4_to_5'].rolling(window=5).mean()
        df_rates['ma5_5_plus'] = df_rates['rate_5_plus'].rolling(window=5).mean()
        df_rates['ma5_overall'] = df_rates['overall_rate'].rolling(window=5).mean()
        df_rates['date_str'] = df_rates['date'].astype(str)
        _ticks = df_rates['date_str'].tolist()
        _tickvals_5 = [
            _ticks[i] for i in range(0, len(_ticks), 5)
        ]
        if len(_ticks) > 0 and _ticks[-1] not in _tickvals_5:
            _tickvals_5.append(_ticks[-1])
        chart_specs = [
            ('总体晋级率', 'overall_rate', 'ma3_overall', 'ma5_overall', 'adv_rate_overall', 'n_total', 'd_total'),
            ('首板晋级二板率', 'rate_1_to_2', 'ma3_1_to_2', 'ma5_1_to_2', 'adv_rate_1_2', 'n_1_to_2', 'd_1_to_2'),
            ('二板晋级三板率', 'rate_2_to_3', 'ma3_2_to_3', 'ma5_2_to_3', 'adv_rate_2_3', 'n_2_to_3', 'd_2_to_3'),
            ('三板晋级四板率', 'rate_3_to_4', 'ma3_3_to_4', 'ma5_3_to_4', 'adv_rate_3_4', 'n_3_to_4', 'd_3_to_4'),
            ('四板晋级五板率', 'rate_4_to_5', 'ma3_4_to_5', 'ma5_4_to_5', 'adv_rate_4_5', 'n_4_to_5', 'd_4_to_5'),
            ('五板及以上晋级率', 'rate_5_plus', 'ma3_5_plus', 'ma5_5_plus', 'adv_rate_5_plus', 'n_5_plus', 'd_5_plus')
        ]
        for i, (title, col, ma3_col, ma5_col, key, num_col, denom_col) in enumerate(chart_specs):
            if i % 2 == 0:
                cols = st.columns(2)
            c = cols[i % 2]
            with c:
                fig = px.line(
                    df_rates,
                    x='date_str',
                    y=col,
                    title=title,
                    labels={'date_str': '日期', col: '晋级率(%)'}
                )
                fig.update_xaxes(
                    type='category',
                    categoryorder='array',
                    categoryarray=df_rates['date_str'],
                    tickmode='array',
                    tickvals=_tickvals_5,
                    ticktext=_tickvals_5
                )
                labels_text = df_rates[num_col].astype(int).astype(str) + '/' + df_rates[denom_col].astype(int).astype(str)
                fig.add_scatter(
                    x=df_rates['date_str'],
                    y=df_rates[col],
                    mode='text',
                    text=labels_text,
                    textposition='top center',
                    textfont=dict(size=10, color='gray'),
                    name='分子/分母',
                    hoverinfo='skip',
                    showlegend=False
                )
                fig.update_traces(
                    hovertemplate='<b>日期:</b> %{x}<br>' +
                                  '<b>晋级率:</b> %{y:.2f}%<br>' +
                                  '<b>分子/分母:</b> %{customdata[0]}/%{customdata[1]}<br>' +
                                  '<extra></extra>',
                    customdata=df_rates[[num_col, denom_col]]
                )
                fig.add_scatter(
                    x=df_rates['date_str'],
                    y=df_rates[ma3_col],
                    name='3日均值',
                    line=dict(dash='dash'),
                    hovertemplate='<b>日期:</b> %{x}<br>' +
                                  '<b>3日均值:</b> %{y:.2f}%<br>' +
                                  '<extra></extra>'
                )
                fig.add_scatter(
                    x=df_rates['date_str'],
                    y=df_rates[ma5_col],
                    name='5日均值',
                    line=dict(dash='dash'),
                    hovertemplate='<b>日期:</b> %{x}<br>' +
                                  '<b>5日均值:</b> %{y:.2f}%<br>' +
                                  '<extra></extra>'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True, config=DEFAULT_PLOTLY_CONFIG, key=key)
                table_df = df_rates[['date', col, num_col, denom_col]].copy()
                table_df.columns = ['日期', '晋级率(%)', '分子', '分母']
                table_df['晋级率(%)'] = table_df['晋级率(%)'].round(2)
                st.dataframe(table_df, width='stretch', hide_index=True)
    
    create_chart_with_date_filter("晋级率趋势", df, create_advancement_rate_chart)
    
    # 涨停池，涨停率趋势
    def create_success_rate_chart(filtered_df):
        daily_success_rate = []
        
        for date in filtered_df['date'].unique():
            day_data = filtered_df[filtered_df['date'] == date]
            touched_limit = len(day_data[day_data['limit_up_days'].notna()])
            
            if touched_limit > 0:
                success_count = len(day_data)
                success_rate = (touched_limit / success_count) * 100
            else:
                success_rate = 0
            
            daily_success_rate.append({
                'date': pd.to_datetime(date).date(),
                'success_rate': success_rate
            })
        
        success_rate_df = pd.DataFrame(daily_success_rate)
        success_rate_df = success_rate_df.sort_values('date', ascending=True).reset_index(drop=True)
        
        success_rate_df['date_str'] = success_rate_df['date'].astype(str)
        _ticks = success_rate_df['date_str'].tolist()
        _tickvals_5 = [_ticks[i] for i in range(0, len(_ticks), 5)]
        if len(_ticks) > 0 and _ticks[-1] not in _tickvals_5:
            _tickvals_5.append(_ticks[-1])
        fig = px.line(
            success_rate_df,
            x='date_str',
            y='success_rate',
            title='涨停率趋势',
            labels={'date_str': '日期', 'success_rate': '成功率(%)'}
        )
        fig.update_xaxes(
            type='category',
            categoryorder='array',
            categoryarray=success_rate_df['date_str'],
            tickmode='array',
            tickvals=_tickvals_5,
            ticktext=_tickvals_5
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True, config=DEFAULT_PLOTLY_CONFIG, key="success_rate_chart")
    
    create_chart_with_date_filter("涨停率趋势", df, create_success_rate_chart)
    
    # 5. 市场情绪指数
    def create_sentiment_chart(filtered_df):
        sentiment_filtered = calculate_sentiment_value(filtered_df)
        
        if not sentiment_filtered.empty:
            sentiment_filtered['date_str'] = sentiment_filtered['date'].astype(str)
            _ticks = sentiment_filtered['date_str'].tolist()
            _tickvals_5 = [_ticks[i] for i in range(0, len(_ticks), 5)]
            if len(_ticks) > 0 and _ticks[-1] not in _tickvals_5:
                _tickvals_5.append(_ticks[-1])
            fig = px.line(
                sentiment_filtered,
                x='date_str',
                y='sentiment_value',
                title='每日市场情绪指数',
                labels={'date_str': '日期', 'sentiment_value': '情绪值'}
            )
            fig.update_xaxes(
                type='category',
                categoryorder='array',
                categoryarray=sentiment_filtered['date_str'],
                tickmode='array',
                tickvals=_tickvals_5,
                ticktext=_tickvals_5
            )
            
            # 添加情绪值区间标注
            fig.add_hline(y=80, line_dash="dash", line_color="green", 
                         annotation_text="乐观区间")
            fig.add_hline(y=50, line_dash="dash", line_color="yellow", 
                         annotation_text="中性区间")
            fig.add_hline(y=20, line_dash="dash", line_color="red", 
                         annotation_text="悲观区间")
            
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True, config=DEFAULT_PLOTLY_CONFIG, key="sentiment_chart")
        else:
            st.info("暂无情绪指数数据")
    
    create_chart_with_date_filter("市场情绪指数", df, create_sentiment_chart)
    
    # 6. 溢价率分析
    if not premium_df.empty:
        st.header("💰 涨停板第二天溢价率分析")
        
        def create_premium_analysis_chart(filtered_df):
            premium_filtered = calculate_premium_rates(filtered_df)
            
            if not premium_filtered.empty:
                daily_premium_stats = get_daily_premium_stats(premium_filtered)
                
                if not daily_premium_stats.empty:
                    daily_premium_stats['date_str'] = daily_premium_stats['date'].astype(str)
                    _ticks = daily_premium_stats['date_str'].tolist()
                    _tickvals_5 = [_ticks[i] for i in range(0, len(_ticks), 5)]
                    if len(_ticks) > 0 and _ticks[-1] not in _tickvals_5:
                        _tickvals_5.append(_ticks[-1])
                    long_df = daily_premium_stats.melt(
                        id_vars=['date_str'],
                        value_vars=['avg_opening_premium', 'avg_closing_premium'],
                        var_name='类型',
                        value_name='平均溢价率(%)'
                    )
                    fig_premium = px.line(
                        long_df,
                        x='date_str',
                        y='平均溢价率(%)',
                        color='类型',
                        title='涨停股票第二天溢价率趋势',
                        labels={'date_str': '涨停日期', '平均溢价率(%)': '平均溢价率(%)', '类型': '类型'}
                    )
                    fig_premium.update_xaxes(
                        type='category',
                        categoryorder='array',
                        categoryarray=daily_premium_stats['date_str'],
                        tickmode='array',
                        tickvals=_tickvals_5,
                        ticktext=_tickvals_5
                    )
                    fig_premium.update_layout(height=400)
                    st.plotly_chart(fig_premium, use_container_width=True, config=DEFAULT_PLOTLY_CONFIG, key="premium_combined_chart")
                    
                    # 显示统计摘要
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        avg_opening = premium_filtered['opening_premium_rate'].mean()
                        st.metric("平均开盘价溢价率", f"{avg_opening:.2f}%")
                    with col2:
                        avg_closing = premium_filtered['closing_premium_rate'].mean()
                        st.metric("平均收盘价溢价率", f"{avg_closing:.2f}%")
                    with col3:
                        median_opening = premium_filtered['opening_premium_rate'].median()
                        st.metric("中位数开盘价溢价率", f"{median_opening:.2f}%")
                    with col4:
                        median_closing = premium_filtered['closing_premium_rate'].median()
                        st.metric("中位数收盘价溢价率", f"{median_closing:.2f}%")
                
                else:
                    st.info("暂无溢价率统计数据")
            else:
                st.info("暂无符合条件的溢价率数据")
        
        create_chart_with_date_filter("溢价率分析", df, create_premium_analysis_chart)
    
    # 数据概览
    st.header("📊 数据概览")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_stocks = len(df)
        st.metric("总记录数", f"{total_stocks:,}")
    
    with col2:
        limit_up_stocks = len(df[df['limit_up_days'].notna()])
        st.metric("涨停记录数", f"{limit_up_stocks:,}")
    
    with col3:
        avg_sentiment = sentiment_df['sentiment_value'].mean()
        st.metric("平均情绪值", f"{avg_sentiment:.1f}")
    
    with col4:
        max_continuous = df['limit_up_days'].max()
        st.metric("最高连板", f"{max_continuous:.0f}" if pd.notna(max_continuous) else "0")

if __name__ == "__main__":
    main()