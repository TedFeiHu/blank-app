import pymysql
import pandas as pd

def get_count_stocks(date):
    """
    从数据库获取昨日涨停的股票数量
    条件为: limit_up_days is not null and date = {date}

    参数:
    date: 日期 (yyyy-mm-dd格式)

    返回:
    int: 符合条件的股票数量
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
                where date = %s 
            """
            cursor.execute(query, (date,))
            result = cursor.fetchall()
            return result

    except Exception as e:
        print(f"查询昨日涨停股票时出错: {e}")
        return 0

    finally:
        if connection:
            connection.close()


def get_count(date):
    """
    从数据库获取昨日涨停的股票数量
    条件为: limit_up_days is not null and date = {date}

    参数:
    date: 日期 (yyyy-mm-dd格式)

    返回:
    int: 符合条件的股票数量
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
                SELECT count(*) as count
                FROM stock_model
                where date = %s 
                and limit_up_days is not null
                
            """
            cursor.execute(query, (date,))
            result = cursor.fetchall()
            return result[0]['count']

    except Exception as e:
        print(f"查询昨日涨停股票时出错: {e}")
        return 0

    finally:
        if connection:
            connection.close()


def get_stock(date,code):
    """
    从数据库获取昨日涨停的股票数量
    条件为: limit_up_days is not null and date = {date}

    参数:
    date: 日期 (yyyy-mm-dd格式)

    返回:
    int: 符合条件的股票数量
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
                where date = %s 
                and code = %s
            """
            cursor.execute(query, (date,code))
            return cursor.fetchall()

    except Exception as e:
        print(f"查询昨日涨停股票时出错: {e}")
        return 0

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    # 设置默认日期，实际使用时可以修改
    date = "2025-11-26"  # 示例日期，实际使用时请提供有效日期
    
    limit_up_stocks = get_count_stocks(date)
    limit_up_stocks_df = pd.DataFrame(limit_up_stocks)
    
    print('处理中')
    print('炸板', limit_up_stocks_df[limit_up_stocks_df['limit_up_days'].isnull() & (limit_up_stocks_df['dc_first_seal_time'].notnull())].shape)
    print('涨停', limit_up_stocks_df[limit_up_stocks_df['limit_up_days'].notnull()].shape)
    print('昨日涨停', limit_up_stocks_df[limit_up_stocks_df['limit_up_days'].isnull() & (limit_up_stocks_df['dc_first_seal_time'].isnull())].shape)

    date = "2025-11-25"
    count = get_count(date)
    print('昨日涨停股票数量:', count)

    df = get_stock(date,'605188')
    print(df)

    
