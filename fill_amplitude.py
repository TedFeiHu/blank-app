import os
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
import random
import time
import logging

import akshare as ak

def dsn_from_env_or_args(args):
    host = os.getenv("OPS_DB_HOST") or args.host
    user = os.getenv("OPS_DB_USER") or args.user
    password = os.getenv("OPS_DB_PASSWORD") or args.password
    database = os.getenv("OPS_DB_DATABASE") or args.database
    charset = os.getenv("OPS_DB_CHARSET") or args.charset
    return f"mysql+pymysql://{user}:{password}@{host}/{database}?charset={charset}"

def get_missing_ranges(engine):
    q = text(
        """
        SELECT code, MIN(date) AS min_date, MAX(date) AS max_date
        FROM stock_model
        WHERE amplitude IS NULL
        GROUP BY code
        """
    )
    with engine.begin() as conn:
        df = pd.read_sql(q, conn)
    logging.info("missing amplitude codes: %d", len(df))
    return df

def fetch_hist_df(code, start_date, end_date):
    symbol = code[-6:]
    random_range = random.randrange(40, 60)
    logging.info("fetch %s [%s-%s], sleep %ds", symbol, start_date, end_date, random_range)
    time.sleep(random_range)
    df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    logging.info("received rows for %s: %d", symbol, len(df) if df is not None else 0)
    if "日期" in df.columns and "振幅" in df.columns:
        df = df[["日期", "振幅"]].copy()
        df["日期"] = pd.to_datetime(df["日期"]).dt.date
        return df
    logging.warning("hist missing required columns for %s", symbol)
    return pd.DataFrame(columns=["日期", "振幅"]) 

def build_updates_for_code(engine, code, min_date, max_date):
    logging.info("processing code %s range %s -> %s", code, str(min_date), str(max_date))
    start = pd.to_datetime(min_date).strftime("%Y%m%d")
    end = pd.to_datetime(max_date).strftime("%Y%m%d")
    hist = fetch_hist_df(code, start, end)
    if hist.empty:
        logging.warning("no hist rows for %s", code)
        return []
    with engine.begin() as conn:
        q = text(
            """
            SELECT date FROM stock_model
            WHERE code = :code AND amplitude IS NULL AND date BETWEEN :start AND :end
            """
        )
        missing_dates = pd.read_sql(q, conn, params={"code": code, "start": min_date, "end": max_date})
    logging.info("missing dates for %s: %d", code, len(missing_dates))
    if missing_dates.empty:
        return []
    m = {row["日期"]: float(row["振幅"]) for _, row in hist.iterrows()}
    updates = []
    for d in missing_dates["date"].tolist():
        val = m.get(pd.to_datetime(d).date())
        if val is not None:
            updates.append({"code": code, "date": d, "amplitude": round(val, 2)})
    logging.info("prepared updates for %s: %d", code, len(updates))
    return updates

def apply_updates(engine, updates):
    if not updates:
        return 0
    stmt = text("UPDATE stock_model SET amplitude = :amplitude WHERE code = :code AND date = :date AND amplitude IS NULL")
    with engine.begin() as conn:
        conn.execute(stmt, updates)
    logging.info("applied updates: %d", len(updates))
    return len(updates)

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO), format="% (asctime)s % (levelname)s % (message)s".replace(" ", ""))
    logging.info("start amplitude fill")
    dsn = "mysql+pymysql://dingteam_ops:GdTtsy1qNJ0RTfgceblzUFNLS2AH5qQi@mysql-tt.dingteam.com/dingteam_ops?charset=utf8mb4"
    engine = create_engine(dsn)
    logging.info("connected to database")
    ranges = get_missing_ranges(engine)
    if ranges.empty:
        logging.info("no missing amplitude")
        return
    total = 0
    count_codes = len(ranges)
    for idx, r in enumerate(ranges.itertuples(index=False), start=1):
        code = str(getattr(r, "code"))
        logging.info("progress %d/%d: %s", idx, count_codes, code)
        updates = build_updates_for_code(engine, code, getattr(r, "min_date"), getattr(r, "max_date")) 
        applied = apply_updates(engine, updates)
        total += applied
        logging.info("code %s applied %d rows, total %d", code, applied, total)
    logging.info("updated total rows: %d", total)

if __name__ == "__main__":
    main()
