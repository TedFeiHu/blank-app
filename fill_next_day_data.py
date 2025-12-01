import os
import argparse
import logging
import random
import time
import pandas as pd
from sqlalchemy import create_engine, text
import akshare as ak

def dsn_from_env_or_args(args):
    host = os.getenv("OPS_DB_HOST") or args.host
    user = os.getenv("OPS_DB_USER") or args.user
    password = os.getenv("OPS_DB_PASSWORD") or args.password
    database = os.getenv("OPS_DB_DATABASE") or args.database
    charset = os.getenv("OPS_DB_CHARSET") or args.charset
    return f"mysql+pymysql://{user}:{password}@{host}/{database}?charset={charset}"

def increment_first_number(pattern_str):
    if not pattern_str or not isinstance(pattern_str, str):
        return None
    parts = pattern_str.split('/')
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        first_num = int(parts[0]) + 1
        return f"{first_num}/{parts[1]}"
    return None

def get_limit_up_rows(engine, date):
    # 1. 获取涨停板数据 条件为 limit_up_days is not null
    q = text(
        """
        SELECT id, code, date, name, industry,
               market_capitalization, circulating_market_capitalization, real_circulating_capitalization,
               limit_up_statistics
        FROM stock_model
        WHERE limit_up_days IS NOT NULL and date != :date
        ORDER BY code, date
        """
    )
    with engine.begin() as conn:
        df = pd.read_sql(q, conn, params={"date": date})
    logging.info("found limit-up rows: %d", len(df))
    return df

def check_next_day_exists_batch(engine, rows):
    # 2. 检查是否 存在 T+1 的数据 （date）
    # 批量检查：构造 (code, next_possible_date) 列表并在库中查询
    # 为简单起见，先假设T+1就是日历日+1，然后在库里查是否存在 >= T+1 的记录作为近似
    # 但最准确的是逐个检查，或按代码批量查出所有日期集合
    # 这里采用按代码分组查询该代码所有日期，然后在内存比对
    codes = rows['code'].unique()
    logging.info("checking existing dates for %d codes", len(codes))
    
    existing_dates_map = {}
    
    # 分批查询以避免SQL过长
    batch_size = 50
    for i in range(0, len(codes), batch_size):
        batch_codes = codes[i:i+batch_size]
        q = text("SELECT code, date FROM stock_model WHERE code IN :codes")
        with engine.begin() as conn:
            res = pd.read_sql(q, conn, params={"codes": tuple(batch_codes)})
        for code, group in res.groupby('code'):
            existing_dates_map[code] = set(group['date'].tolist())
            
    missing_rows = []
    for _, row in rows.iterrows():
        code = row['code']
        curr_date = row['date']
        # 严格检查：只看 T+1 (日历日) 是否存在
        # 如果 T+1 存在，说明肯定有下一个交易日数据
        # 如果 T+1 不存在（可能是周五、假期、或真缺数据），都加入待补充列表，由后续步骤通过 API 确认
        next_cal_day = curr_date + pd.Timedelta(days=1)
        dates = existing_dates_map.get(code, set())
        
        # 检查是否为周五
        is_friday = pd.Timestamp(curr_date).weekday() == 4  # 0=周一, 4=周五
        
        # 如果是周五，检查下周一数据是否存在
        if is_friday:
            next_monday = curr_date + pd.Timedelta(days=3)  # 周五+3天=周一
            # 如果下周一数据也不存在，才加入待补充列表
            if next_cal_day not in dates and next_monday not in dates:
                missing_rows.append(row)
        else:
            # 非周五情况，保持原逻辑
            if next_cal_day not in dates:
                missing_rows.append(row)
            
    logging.info("rows missing next trading day data: %d", len(missing_rows))
    return pd.DataFrame(missing_rows)

def fetch_hist_data(code, start_date, end_date):
    symbol = code[-6:]
    wait = random.randrange(60, 90)
    logging.info("fetching %s [%s-%s], sleep %ds", symbol, start_date, end_date, wait)
    time.sleep(wait)
    df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    return df


def process_missing_rows(engine, missing_df):
    # 3. 不存在 ，则加入待补充列表（同时依据code分组）
    if missing_df.empty:
        return 0
        
    grouped = missing_df.groupby('code')
    total_inserted = 0
    
    for code, group in grouped:
        # 对每个代码，找到最早的缺数日期，往后拉取一段
        min_date = group['date'].min()
        max_date = group['date'].max()
        # 4. 通过 ak.stock_zh_a_hist 获取历史交易日信息
        # 往后拉取30天以覆盖可能的假期，使用 max_date 确保覆盖该组所有日期
        # 限制 end_date 不超过今天
        today = pd.Timestamp.now().floor('D')
        min_ts = pd.to_datetime(min_date)
        max_ts = pd.to_datetime(max_date)

        start_ts = min_ts + pd.Timedelta(days=1)
        end_ts = max_ts + pd.Timedelta(days=30)

        if end_ts > today:
            end_ts = today

        if start_ts > end_ts:
            logging.debug("start %s > end %s for %s, skipping", start_ts.date(), end_ts.date(), code)
            continue

        start_str = start_ts.strftime("%Y%m%d")
        end_str = end_ts.strftime("%Y%m%d")
        
        hist_df = fetch_hist_data(code, start_str, end_str)
        
        if hist_df is None or hist_df.empty or "日期" not in hist_df.columns:
            logging.warning("no hist data for %s after %s", code, min_date)
            continue
            
        hist_df['date_obj'] = pd.to_datetime(hist_df['日期']).dt.date
        
        # 对该代码下的每一条缺失记录进行处理
        for _, row in group.iterrows():
            curr_date = row['date']
            # 找到大于 curr_date 的第一个交易日
            next_days = hist_df[hist_df['date_obj'] > curr_date].sort_values('date_obj')
            
            if next_days.empty:
                logging.warning("no next trading day found in fetched window for %s after %s", code, curr_date)
                continue
                
            target_day = next_days.iloc[0]
            target_date = target_day['date_obj']
            
            # 5. 再次检查第二个交易日是否存在于数据库
            with engine.begin() as conn:
                exists = conn.execute(
                    text("SELECT COUNT(1) FROM stock_model WHERE code=:code AND date=:date"),
                    {"code": code, "date": target_date}
                ).scalar()
                
            if exists > 0:
                logging.info("%s next day %s already exists for %s, skipping", curr_date, target_date, code)
                continue
                
            # 6. 不存在，则构建数据，插入数据库
            # 字段映射
            # real_turnover_rate: volume/real_circulating_capitalization % (注意单位换算，成交量是手，股本是股？需确认)
            # 通常 real_circulating_capitalization 是股数，volume 是手（100股）
            # 换手率 = (volume * 100) / real_circulating_capitalization * 100%
            
            vol = int(target_day['成交量'])
            real_cap = float(row['real_circulating_capitalization']) if row['real_circulating_capitalization'] else 0
            real_to = 0.0
            if real_cap > 0:
                real_to = (vol * 100 / real_cap) * 100
                
            insert_row = {
                "date": target_date,
                "code": code,
                "name": row['name'],
                "industry": row['industry'],
                "market_capitalization": row['market_capitalization'],
                "circulating_market_capitalization": row['circulating_market_capitalization'],
                "real_circulating_capitalization": row['real_circulating_capitalization'],
                
                "price": float(target_day['收盘']),
                "volume": vol,
                "turnover_rate": float(target_day['换手率']),
                "real_turnover_rate": round(real_to, 2),
                
                "first_price": float(target_day['开盘']),
                "last_price": float(target_day['收盘']),
                
                "break_count": 0,
                "dc_break_count": 0,
                "amplitude": float(target_day['振幅']),
                "limit_up_statistics": increment_first_number(row['limit_up_statistics']),
                
                # 默认填充字段以满足非空约束
                "outside_volume": 0,
                "inside_volume": 0,
                "first_volume": 0,
                "last_volume": 0,
                "buy_1_vol": None,
                "limit_up_days": None, # 明确置空，因为是次日数据
                "events": "[]"
            }
            
            stmt = text(
                """
                INSERT INTO stock_model (
                    date, code, name, industry,
                    market_capitalization, circulating_market_capitalization, real_circulating_capitalization,
                    price, volume, turnover_rate, real_turnover_rate,
                    first_price, last_price,
                    break_count, dc_break_count, amplitude, limit_up_statistics,
                    outside_volume, inside_volume, first_volume, last_volume, buy_1_vol, limit_up_days, events
                ) VALUES (
                    :date, :code, :name, :industry,
                    :market_capitalization, :circulating_market_capitalization, :real_circulating_capitalization,
                    :price, :volume, :turnover_rate, :real_turnover_rate,
                    :first_price, :last_price,
                    :break_count, :dc_break_count, :amplitude, :limit_up_statistics,
                    :outside_volume, :inside_volume, :first_volume, :last_volume, :buy_1_vol, :limit_up_days, :events
                )
                """
            )
            
            try:
                with engine.begin() as conn:
                    conn.execute(stmt, insert_row)
                logging.info("inserted next day %s for %s", target_date, code)
                total_inserted += 1
            except Exception as e:
                logging.error("insert failed for %s %s: %s", code, target_date, e)
                
    return total_inserted

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    # p = argparse.ArgumentParser()
    # p.add_argument("--host", required=False)
    # p.add_argument("--user", required=False)
    # p.add_argument("--password", required=False)
    # p.add_argument("--database", required=False)
    # p.add_argument("--charset", default="utf8mb4")
    # args = p.parse_args()
    
    logging.info("start fill_next_day_data")
    # dsn = dsn_from_env_or_args(args)
    dsn = "mysql+pymysql://dingteam_ops:GdTtsy1qNJ0RTfgceblzUFNLS2AH5qQi@mysql-tt.dingteam.com/dingteam_ops?charset=utf8mb4"
    engine = create_engine(dsn)
    
    # 1. 获取涨停板数据, 排除今天的数据
    lu_rows = get_limit_up_rows(engine, "2025-11-28")
    if lu_rows.empty:
        logging.info("no limit up rows found")
        return
        
    # 2. 检查是否存在 T+1 数据
    missing_df = check_next_day_exists_batch(engine, lu_rows)
    if missing_df.empty:
        logging.info("all limit up rows have next day data")
        return
        
    # 3-6. 补充缺失数据
    inserted_count = process_missing_rows(engine, missing_df)
    logging.info("completed. total inserted rows: %d", inserted_count)

if __name__ == "__main__":
    main()
