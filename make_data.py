import time

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import pymysql
import json as json
import random

from pandas.core.interchange.dataframe_protocol import DataFrame


def make_stock_data(row, date):

    print(row)

    name = safe_get_row_value(row, '名称') or safe_get_row_value(row, 'name')  # 名称
    code = safe_get_row_value(row, '代码') or safe_get_row_value(row, 'code')  # 代码

    print(f"开始处理: {date} {code} {name}")
    # 通过 date 和 code 查询数据库是否有数据，有的话直接return
    if check_data_exists(date, code):
        print(f"数据已存在: {date} {code} {name}")
        return

    if not (code.startswith('60') or code.startswith('30') or code.startswith('00')):
        return

    if code.startswith('6'):
        stock_type = 'sh'
    else:
        stock_type = 'sz'


    # 获取股票信息 总股本，流通股本，行业
    print('--'*8+'获取股票信息 总股本，流通股本，行业')
    random_range = random.randrange(30, 60)
    time.sleep(random_range)
    stock_individual_info_em_df = ak.stock_individual_info_em(symbol=code)

    df_indexed = stock_individual_info_em_df.set_index('item')
    capitalization = df_indexed.loc['总股本', 'value']  # 总股本
    circulating_cap = df_indexed.loc['流通股', 'value']  # 流通股本

    print('--'*8+'获取10大流通股东')
    random_range = random.randrange(30, 60)
    time.sleep(random_range)
    try:
        stock_gdfx_free_top_10_em_df = ak.stock_gdfx_free_top_10_em(symbol=stock_type + code, date="20250630")
        shareholding_numbers = stock_gdfx_free_top_10_em_df.query(
            '`占总流通股本持股比例` > 5.0'
        )['持股数'].tolist()
        circulating_cap_real = circulating_cap - sum(shareholding_numbers)  # 真实流通股本
    except Exception as e:
        circulating_cap_real = circulating_cap



    print('--'*8+'获取实时数据')
    random_range = random.randrange(30, 60)
    time.sleep(random_range)
    stock_bid_ask_em_df = ak.stock_bid_ask_em(symbol=code)
    ask_index = stock_bid_ask_em_df.set_index('item')
    volume = ask_index.loc['总手', 'value']  # 成交量 手
    if volume == '-': #停牌
        return
    turnover_rate = ask_index.loc['换手', 'value']  # 换手率 %
    turnover_rate_real = round((volume * 10000 / circulating_cap_real), 2)  # 真实换手率 %
    buying_at_ask = ask_index.loc['外盘', 'value']  # 外盘
    selling_at_bid = ask_index.loc['内盘', 'value']  # 内盘
    buy_1_vol = ask_index.loc['buy_1_vol', 'value'] # 买一量

    price = safe_get_row_value(row, '最新价') or ask_index.loc['最新', 'value'] # 最新价
    limit_price =safe_get_row_value(row, '涨停价') or ask_index.loc['涨停', 'value'] # 涨停价

    price_max = ask_index.loc['最高', 'value']
    price_min = ask_index.loc['最低', 'value']
    pre_price = ask_index.loc['昨收', 'value']
    amplitude = round((price_max - price_min) / pre_price * 100, 2) # 振幅 %  （最高-最低）/ 昨收

    print('--' * 8 + '分笔数据')

    stock_zh_a_tick_tx_js_df = ak.stock_zh_a_tick_tx_js(symbol=stock_type + code)
    if stock_zh_a_tick_tx_js_df.empty:
        print(f'没有数据{code},{name}')
        # return

    df = stock_zh_a_tick_tx_js_df

    # 竞价数据
    first_tick = df.iloc[0]
    first_volume = first_tick['成交量']  # 第一笔成交量
    first_price = first_tick['成交价格']  # 第一笔成交价格

    # 最后一笔 数据
    last_tick = df.iloc[-1]
    last_volume = last_tick['成交量']
    last_price = last_tick['成交价格']


    # 将时间字符串转换为时间对象以便比较
    df['时间对象'] = pd.to_datetime(df['成交时间'], format='%H:%M:%S')

    # 合并所有方法的结果
    print("检测涨停事件:")
    all_events = merge_events(df, limit_price)



    # 获取首次封板时间，最后封板时间，首次炸板时间，最后炸板时间，炸板次数
    first_seal_time = None
    last_seal_time = None
    first_break_time = None
    last_break_time = None

    # 分析事件
    seal_events = [event for event in all_events if event['类型'] in ['封板', '回封']]
    break_events = [event for event in all_events if event['类型'] == '炸板']

    # 获取首次和最后封板时间
    if seal_events:
        first_seal_time = min(seal_events, key=lambda x: x['时间'])['时间']
        last_seal_time = max(seal_events, key=lambda x: x['时间'])['时间']

    # 获取首次和最后炸板时间
    if break_events:
        first_break_time = min(break_events, key=lambda x: x['时间'])['时间']
        last_break_time = max(break_events, key=lambda x: x['时间'])['时间']

    # 计算炸板次数
    break_count = len(break_events)

    dc_first_seal_time = safe_get_row_value(row, '首次封板时间')
    dc_last_seal_time = safe_get_row_value(row, '最后封板时间')
    dc_break_count = safe_get_row_value(row, '炸板次数') or 0
    limit_up_statistics =  safe_get_row_value(row, '涨停统计') or increment_first_number(safe_get_row_value(row, 'limit_up_statistics'))
    limit_up_days = safe_get_row_value(row, '连板数')
    amplitude = safe_get_row_value(row, '振幅') or amplitude
    industry = safe_get_row_value(row, '所属行业') or safe_get_row_value(row, 'industry')
    for event in all_events:
        if '时间' in event and hasattr(event['时间'], 'strftime'):
            event['时间'] = event['时间'].strftime('%H:%M:%S')
    events = json.dumps(all_events, ensure_ascii=False)

    data_to_save = (
        date, # 当前日期 yyyy-MM-dd
        name, # 名称
        code, # 代码
        capitalization, # 总股本 float
        circulating_cap, # 流通股本 float
        circulating_cap_real, # 真实流通股本 float
        price, # 最新价 float
        volume, # 成交量 手 int
        turnover_rate, # 换手率 % float
        turnover_rate_real, # 真实换手率 % float
        buying_at_ask, # 外盘 float
        selling_at_bid, # 内盘 float
        first_volume, # 首笔成交量 手 int
        first_price, # 首笔成交价格 float
        last_volume, # 最后一笔成交量 手 int
        last_price, # 最后一笔成交价格  float
        first_seal_time, # 首次封板时间  %H:%M:%S
        last_seal_time, # 最后封板时间  %H:%M:%S
        first_break_time, # 首次炸板时间  %H:%M:%S
        last_break_time, # 最后炸板时间  %H:%M:%S
        break_count, # 炸板次数 int
        events, # 炸板事件 json
        dc_first_seal_time, # 东财首次封板时间  %H:%M:%S
        dc_last_seal_time, # 东财最后封板时间  %H:%M:%S
        dc_break_count, # 东财炸板次数 int
        limit_up_statistics, # 涨停统计 str
        amplitude, # 振幅  float
        industry, # 所属行业 str
        buy_1_vol, # 买一量
        limit_up_days # 连板数 int

    )

    # 保存数据到数据库
    print("保存数据到数据库")
    save_date(data_to_save)


def increment_first_number(pattern_str):
    """
    将 '3/2' 这种模式的字符串第一个数字加1，返回 '4/2'

    参数:
    pattern_str: 形如 '3/2' 的字符串

    返回:
    str: 第一个数字加1后的字符串，如 '4/2'
    """
    parts = pattern_str.split('/')
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        first_num = int(parts[0]) + 1
        return f"{first_num}/{parts[1]}"
    else:
        return None

# 安全地获取字段值，如果不存在则使用默认值
def safe_get_row_value(row, column_name, default_value=None):
    try:
        return row[column_name]
    except KeyError:
        return default_value

def identify_limit_events(tick_df, limit_price):
    """
    基于分笔数据识别涨停事件

    参数:
    tick_df: 分笔数据DataFrame
    limit_price: 涨停价

    返回:
    dict: 包含封板、炸板和回封事件的字典
    """
    # 初始化状态变量
    state = "未封板"  # 可能状态: "未封板", "封板", "炸板"
    events = []
    current_seal_start = None
    current_break_start = None

    # 存储临时状态
    seal_candidate = False
    seal_candidate_time = None
    break_candidate = False
    break_candidate_time = None

    # 遍历每一笔成交
    for i, row in tick_df.iterrows():
        current_time = row['时间对象']
        current_price = row['成交价格']
        volume = row['成交量']

        # 状态: 未封板
        if state == "未封板":
            if current_price >= limit_price:
                # 首次达到涨停价，标记为封板候选
                if not seal_candidate:
                    seal_candidate = True
                    seal_candidate_time = current_time
                    # 检查后续几笔确认
                    confirm_count = 0
                    for j in range(i + 1, min(i + 4, len(tick_df))):  # 查看后续3笔
                        if tick_df.iloc[j]['成交价格'] >= limit_price:
                            confirm_count += 1

                    if confirm_count >= 2:  # 后续至少有2笔确认
                        state = "封板"
                        current_seal_start = seal_candidate_time
                        events.append({
                            '时间': seal_candidate_time,
                            '类型': '封板',
                            '价格': current_price,
                            '成交量': volume
                        })
                        seal_candidate = False

        # 状态: 封板
        elif state == "封板":
            if current_price < limit_price:
                # 价格跌破涨停价，标记为炸板候选
                if not break_candidate:
                    break_candidate = True
                    break_candidate_time = current_time
                    # 检查后续几笔确认
                    confirm_count = 0
                    for j in range(i + 1, min(i + 3, len(tick_df))):  # 查看后续2笔
                        if tick_df.iloc[j]['成交价格'] < limit_price:
                            confirm_count += 1

                    if confirm_count >= 1:  # 后续至少有1笔确认
                        state = "炸板"
                        events.append({
                            '时间': break_candidate_time,
                            '类型': '炸板',
                            '价格': current_price,
                            '成交量': volume
                        })
                        break_candidate = False
                        current_break_start = break_candidate_time

        # 状态: 炸板
        elif state == "炸板":
            if current_price >= limit_price:
                # 价格再次达到涨停价，标记为回封候选
                if not seal_candidate:
                    seal_candidate = True
                    seal_candidate_time = current_time
                    # 检查后续几笔确认
                    confirm_count = 0
                    for j in range(i + 1, min(i + 4, len(tick_df))):  # 查看后续3笔
                        if tick_df.iloc[j]['成交价格'] >= limit_price:
                            confirm_count += 1

                    if confirm_count >= 2:  # 后续至少有2笔确认
                        state = "封板"
                        events.append({
                            '时间': seal_candidate_time,
                            '类型': '回封',
                            '价格': current_price,
                            '成交量': volume
                        })
                        seal_candidate = False
                        current_seal_start = seal_candidate_time

    return events


def identify_with_time_window(tick_df, limit_price, time_window_seconds=30):
    """
    使用时间窗口确认涨停事件

    参数:
    tick_df: 分笔数据DataFrame
    limit_price: 涨停价
    time_window_seconds: 确认时间窗口(秒)

    返回:
    list: 事件列表
    """
    events = []
    state = "未封板"
    last_event_time = None

    for i, row in tick_df.iterrows():
        current_time = row['时间对象']
        current_price = row['成交价格']

        # 封板检测
        if state in ["未封板", "炸板"] and current_price >= limit_price:
            # 查找时间窗口内的后续交易
            window_end = current_time + timedelta(seconds=time_window_seconds)
            window_data = tick_df[(tick_df['时间对象'] >= current_time) &
                                  (tick_df['时间对象'] <= window_end)]

            # 计算窗口中涨停价交易的比例
            limit_trades = len(window_data[window_data['成交价格'] >= limit_price])
            total_trades = len(window_data)

            if total_trades > 0 and limit_trades / total_trades > 0.7:  # 70%以上在涨停价
                event_type = "回封" if state == "炸板" else "封板"
                events.append({
                    '时间': current_time,
                    '类型': event_type,
                    '价格': current_price
                })
                state = "封板"
                last_event_time = current_time

        # 炸板检测
        elif state == "封板" and current_price < limit_price:
            # 查找时间窗口内的后续交易
            window_end = current_time + timedelta(seconds=time_window_seconds)
            window_data = tick_df[(tick_df['时间对象'] >= current_time) &
                                  (tick_df['时间对象'] <= window_end)]

            # 计算窗口中低于涨停价交易的比例
            below_limit_trades = len(window_data[window_data['成交价格'] < limit_price])
            total_trades = len(window_data)

            if total_trades > 0 and below_limit_trades / total_trades > 0.6:  # 60%以上低于涨停价
                events.append({
                    '时间': current_time,
                    '类型': '炸板',
                    '价格': current_price
                })
                state = "炸板"
                last_event_time = current_time

    return events


def advanced_limit_detection(tick_df, limit_price):
    """
    高级涨停事件检测，考虑成交量与价格变动
    """
    events = []
    state = "未封板"
    seal_streak = 0  # 连续涨停计数
    break_streak = 0  # 连续非涨停计数

    # 计算平均成交量
    avg_volume = tick_df['成交量'].mean()

    for i, row in tick_df.iterrows():
        current_time = row['时间对象']
        current_price = row['成交价格']
        current_volume = row['成交量']
        price_change = row['价格变动']

        # 封板检测
        if state != "封板" and current_price >= limit_price:
            # 大成交量涨停更可能是真封板
            volume_ratio = current_volume / avg_volume
            if volume_ratio > 0.8:  # 成交量大于平均值的80%
                seal_streak += 1
                break_streak = 0

                if seal_streak >= 2:  # 连续2笔涨停价成交
                    event_type = "回封" if state == "炸板" else "封板"
                    events.append({
                        '时间': current_time,
                        '类型': event_type,
                        '价格': current_price,
                        '成交量': current_volume
                    })
                    state = "封板"
                    seal_streak = 0
            else:
                seal_streak = 0

        # 炸板检测
        elif state == "封板" and current_price < limit_price:
            # 大成交量下跌更可能是真炸板
            volume_ratio = current_volume / avg_volume
            if volume_ratio > 0.5:  # 成交量大于平均值的50%
                break_streak += 1
                seal_streak = 0

                if break_streak >= 2:  # 连续2笔非涨停价成交
                    events.append({
                        '时间': current_time,
                        '类型': '炸板',
                        '价格': current_price,
                        '成交量': current_volume
                    })
                    state = "炸板"
                    break_streak = 0
            else:
                break_streak = 0

    return events


# 合并结果并进行投票
def merge_events(df, limit_price):
    # 方法1: 基于状态机的方法
    events1 = identify_limit_events(df, limit_price)

    # 方法2: 基于时间窗口的方法
    events2 = identify_with_time_window(df, limit_price, time_window_seconds=30)

    # 方法3: 高级检测方法
    events3 = advanced_limit_detection(df, limit_price)

    events_list = [events1, events2, events3]

    """合并多个方法的结果"""
    from collections import defaultdict

    # 按时间分组事件
    time_groups = defaultdict(list)
    for events in events_list:
        for event in events:
            time_key = event['时间'].strftime('%H:%M:%S')
            time_groups[time_key].append(event['类型'])

    # 对每个时间点进行投票
    merged_events = []
    for time_key, types in time_groups.items():
        # 计算每种类型的票数
        from collections import Counter
        type_counts = Counter(types)
        most_common_type, count = type_counts.most_common(1)[0]

        # 如果至少有两种方法同意，则采纳
        if count >= 2:
            # 找到原始事件获取详细信息
            for events in events_list:
                for event in events:
                    if event['时间'].strftime('%H:%M:%S') == time_key and event['类型'] == most_common_type:
                        merged_events.append(event)
                        break
                if any(event['时间'].strftime('%H:%M:%S') == time_key for event in merged_events):
                    break

    return merged_events


def check_data_exists(date, code):
    """
    检查指定日期和代码的数据是否已存在于数据库中

    参数:
    date: 日期 (yyyy-mm-dd格式)
    code: 股票代码

    返回:
    bool: 如果数据存在返回True，否则返回False
    """
    db_config = {
        'host': 'mysql-tt.dingteam.com',
        'user': 'dingteam_ops',
        'password': 'GdTtsy1qNJ0RTfgceblzUFNLS2AH5qQi',
        'database': 'dingteam_ops'
    }

    # 连接数据库
    connection = pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:

        with connection.cursor() as cursor:
            # 查询是否存在相同日期和代码的数据
            query = """
                SELECT COUNT(*) as count 
                FROM stock_model 
                WHERE date = %s AND code = %s
            """
            cursor.execute(query, (date, code))
            result = cursor.fetchone()

            # 如果计数大于0，说明数据已存在
            return result['count'] > 0

    except Exception as e:
        print(f"检查数据是否存在时出错: {e}")
        return False  # 出错时默认返回False，继续处理数据

    finally:
        if connection:
            connection.close()

def save_date(data):
    db_config = {
        'host': 'mysql-tt.dingteam.com',
        'user': 'dingteam_ops',
        'password': 'GdTtsy1qNJ0RTfgceblzUFNLS2AH5qQi',
        'database': 'dingteam_ops'
    }

    # 连接数据库
    connection = pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            # 准备插入语句
            # Python中使用参数化查询的示例
            insert_query = """
            INSERT INTO stock_model (
                date, name, code, market_capitalization, circulating_market_capitalization,
                real_circulating_capitalization, price, volume, turnover_rate, real_turnover_rate,
                outside_volume, inside_volume, first_volume, first_price, last_volume, last_price,
                first_seal_time, last_seal_time, first_break_time, last_break_time, break_count,
                events, dc_first_seal_time, dc_last_seal_time, dc_break_count, limit_up_statistics,
                amplitude, industry, buy_1_vol, limit_up_days
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # 执行插入操作
            cursor.execute(insert_query, data)

        connection.commit()
    except Exception as e:
        # 发生错误时回滚
        connection.rollback()
        print(f"数据保存失败: {e}")

    finally:
        # 关闭数据库连接
        connection.close()


def get_previous_limit_up_stocks(date):
    """
    从数据库获取昨日涨停的股票
    条件为: limit_up_days is not null and date = {date}

    参数:
    date: 日期 (yyyy-mm-dd格式)

    返回:
    list: 符合条件的股票数据列表
    """

    db_config = {
        'host': 'mysql-tt.dingteam.com',
        'user': 'dingteam_ops',
        'password': 'GdTtsy1qNJ0RTfgceblzUFNLS2AH5qQi',
        'database': 'dingteam_ops'
    }

    # 连接数据库
    connection = pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            # 查询昨日涨停的股票数据
            query = """
                SELECT * 
                FROM stock_model 
                WHERE limit_up_days IS NOT NULL 
                AND date = %s
            """
            cursor.execute(query, (date,))
            result = cursor.fetchall()
            return result

    except Exception as e:
        print(f"查询昨日涨停股票时出错: {e}")
        return []

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    raw_date = '20251124'  # 原始日期格式
    target_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"  # '2025-09-19'
    # # 炸板池
    stock_zt_pool_zbgc_em_df = ak.stock_zt_pool_zbgc_em(date=raw_date)
    print(stock_zt_pool_zbgc_em_df.shape)
    stock_zt_pool_zbgc_em_df.apply(lambda row:make_stock_data(row, target_date), axis=1)
    # 涨停池
    stock_zt_pool_em_df = ak.stock_zt_pool_em(date=raw_date)
    print(stock_zt_pool_em_df.shape)
    stock_zt_pool_em_df.apply(lambda row:make_stock_data(row, target_date), axis=1)

    # 昨日涨停池数据补充
    # 获取昨日涨停股票数据 时间需要手动调整
    pre_date = "2025-11-21"
    previous_limit_up_stocks = get_previous_limit_up_stocks(pre_date)
    previous_limit_up_df = pd.DataFrame(previous_limit_up_stocks)
    print(previous_limit_up_df.shape)
    previous_limit_up_df.apply(lambda row:make_stock_data(row, target_date), axis=1)
