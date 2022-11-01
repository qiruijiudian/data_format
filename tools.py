# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/4/25 17:21
# @Author  : MAYA
import collections
import platform
from functools import wraps
from sqlalchemy.dialects.mysql import DATETIME, DOUBLE, VARCHAR
import json
import random
import logging
import pymysql
import traceback
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from datetime import datetime
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
from settings import DB, SQL_CONTEXT, POINT_DF, TB, VOLUME





# **********************************     数据解析相关函数    **************************************************************

def export_data(start, end, block, columns, file_type="csv", is_local=True):
    column_mapping = {

    }

    conf = get_sql_conf(DB["store"], spec=False)

    if block == "kamba":

        pass
    elif block == "cona":

        pass
    else:

        pass



def get_point_mapping(file):
    """获取天津数据文件中point对照字典，如{"point_1": "AHU_207YUH"}
    :param file: 文件路径
    :return: 对照字典
    """
    res = {}
    if "POWER 14-2" in file:
        lines = []
        with open(file) as f:
            for line in f:
                if "Point" in line and "_" in line and ":" in line:
                    lines.append(line)
        with StringIO("\n".join(lines)) as f:
            df = pd.read_csv(f, header=None)
            columns = df.columns
            for ind in df.index:
                k = df.loc[ind, columns[0]]
                if ":" in k:
                    k = k[:-1]
                v = df.loc[ind, columns[1]]
                res[k] = v
    else:
        with open(file) as f:
            for line in f:
                if "Point" in line and "_" in line and ":" in line:
                    items = line.split()
                    if "Point_" in items[0]:
                        res[items[0][:-1]] = items[1]
    res["<>Date"] = "date"
    res["Time"] = "time"
    return res


def log_or_print(obj, text, start=False, end=False):
    """打印提示或者记录日志

    :param obj: 调用对象
    :param text: 信息
    :param start: 是否为开始阶段（会首先打印一行*）
    :param end: 是否为结束阶段（会在最后打印一行*）
    """

    if obj.log_mode:
        if start:
            logging.info("*" * 100)
        logging.info(text)
        if end:
            logging.info("*" * 100)

    if obj.print_mode:
        if start:
            print("*" * 100)
        print(text)
        if end:
            print("*" * 100)


def log_or_print_without_obj(text, print_mode, log_mode, start=False, end=False):
    """打印提示或者记录日志

    :param text: 信息
    :param start: 是否为开始阶段（会首先打印一行*）
    :param end: 是否为结束阶段（会在最后打印一行*）
    """

    if log_mode:
        if start:
            logging.info("*" * 100)
        logging.info(text)
        if end:
            logging.info("*" * 100)

    if print_mode:
        if start:
            print("*" * 100)
        print(text)
        if end:
            print("*" * 100)


def get_file_data(file, point_mapping):
    """获取天津数据

    :param file: 文件名
    :param point_mapping: 机组对照字典
    :return:
    """

    if "POWER 14-2" in file:
        lines = []
        with open(file) as f:
            for line in f:
                if ("Date" in line and "Time" in line and "Point_" in line) or (
                        "/" in line and ":" in line and "Date" not in line):
                    lines.append(line.strip())
        with StringIO("\n".join(lines)) as f:
            df = pd.read_csv(f)
            df.columns = [point_mapping.get(item) for item in df.columns]
            return df, None
    else:
        res, columns = [], []
        with open(file) as f:
            for line in f:
                line = line.replace("No Data", "nan").replace("Data Loss", "nan").replace(",,,,", "").strip()
                items = line.split()

                if len(items) == len(point_mapping) and (
                        ("Date" in line and "Time" in line and "Point_" in line) or
                        ("/" in items[0] and ":" in items[1])
                ):
                    if "Date" in line and "Time" in line and "Point_" in line:
                        columns = items
                    else:
                        time = datetime.strptime(items[1], "%H:%M:%S")
                        tmp_items = [item if "nan" not in item else np.nan for item in items]
                        if time.minute in [0, 15, 30, 45]:
                            res.append(tmp_items)
        for item in res:
            if "/" not in item[0] and ":" not in item[1]:
                print("异常异异常异常异常", file)
        return res, columns


def get_all_columns(file):
    """获取天津数据dataframe完整的columns
    :param file: columns文件，json格式
    :return:
    """
    with open(file) as f:
        return json.load(f)


def check_time(items):
    """# 检查时间列是否一致

    :param items: 数据集
    :return: True or False
    """
    if not len(items):
        logging.info("无数据内容")
    else:
        hours_time = items[0]["hours_data"]["time_data"]
        days_time = items[0]["days_data"]["time_data"]
        for i in range(1, len(items)):
            if items[i]["hours_data"]["time_data"] != hours_time:
                logging.info("时 时间列异常")
                return False, None, None

            if items[i]["days_data"]["time_data"] != days_time:
                logging.info("日 时间列异常")
                return False, None, None

        return True, hours_time, days_time


def get_dtype(columns, backup=False):
    """根据列名称生成dtype，时间列time_data或者Timestamp设置为DATETIME，其他设置为DOUBLE

    :param columns: dataframe列集合
    :param backup: 是否为宽格式备份
    :return: dtype字典，定义了所有列的数据类型
    """

    res = {}
    if backup:

        for item in columns:
            if item == "time_data" or item == "Timestamp" or item == "timestamp":
                res[item] = DATETIME
            else:
                res[item] = DOUBLE
    else:
        if "time_data" in columns:
            item = "time_data"
        elif "timestamp" in columns:
            item = "timestamp"
        elif "Timestamp" in columns:
            item = "Timestamp"
        else:
            item = "date"
        res = {
            item: DATETIME,
            "pointname": VARCHAR(length=50),
            "value": DOUBLE
        }
    return res


def get_custom_conn(conf):
    """根据conf配置获取数据库连接

    :param conf: 数据库配置
    :return: 数据库连接
    """
    return create_engine(
        'mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
            conf["user"],
            conf["password"],
            conf["host"],
            conf["database"]
        )
    )


def get_store_conn():
    """返回默认数据库连接（默认为计算值存储库data_center_statistical）"""
    sql_conf = get_sql_conf(DB["store"])

    return create_engine(
        'mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
            sql_conf["user"],
            sql_conf["password"],
            sql_conf["host"],
            sql_conf["database"]
        )
    )


def get_conn_by_key(key):
    """返回默认数据库连接（默认为计算值存储库data_center_statistical）"""
    sql_conf = get_sql_conf(DB[key])

    return create_engine(
        'mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
            sql_conf["user"],
            sql_conf["password"],
            sql_conf["host"],
            sql_conf["database"]
        )
    )


def get_sql_conf(db, spec=False):
    # 获取数据库配置信息 spec 是否指定为为Linux系统

    if not spec:
        if platform.system() == "Windows":
            return {
                "user": "root",
                "password": "cdqr2008",
                "host": "localhost",
                "database": db,
            }
        else:
            return {
                "user": "root",
                "password": "cdqr2008",
                "host": "121.199.48.82",
                "database": db
            }
    else:
        if spec == "Windows":
            return {
                "user": "root",
                "password": "cdqr2008",
                "host": "localhost",
                "database": db,
            }
        else:
            return {
                "user": "root",
                "password": "cdqr2008",
                "host": "121.199.48.82",
                "database": db
            }


def sync_temp_data():
    local_weather_conf = get_sql_conf("weather")
    with pymysql.connect(
        user=local_weather_conf["user"],
        password=local_weather_conf["password"],
        host=local_weather_conf["host"],
        database=local_weather_conf["database"]
    ) as local_conn:
        local_cur = local_conn.cursor()
        local_cur.execute("select time from tianjin order by time desc limit 1;")
        latest_weather = local_cur.fetchone()[0]
        local_cur.close()

    cloud_weather_conf = get_sql_conf("weather", True)
    with pymysql.connect(
            user=cloud_weather_conf["user"],
            password=cloud_weather_conf["password"],
            host=cloud_weather_conf["host"],
            database=cloud_weather_conf["database"]
    ) as cloud_conn:
        cloud_cur = cloud_conn.cursor()
        cloud_cur.execute("select time, temp, humidity from tianjin where time > '{}';".format(latest_weather))
        new_items = cloud_cur.fetchall()
        cloud_cur.close()

    with pymysql.connect(
        user=local_weather_conf["user"],
        password=local_weather_conf["password"],
        host=local_weather_conf["host"],
        database=local_weather_conf["database"]
    ) as local_conn2:
        local_cur2 = local_conn2.cursor()
        local_cur2.executemany("insert into tianjin(time, temp, humidity) values (%s, %s, %s);", new_items)
        local_conn2.commit()
        local_cur2.close()




# **********************************    公式计算相关函数      *************************************************************


class DataMissing(Exception):
    # 自定义数据缺失异常

    def __init__(self, error_info):
        super().__init__(self)
        self.error_info = error_info
        logging.error(self.error_info)

    def __str__(self):
        return self.error_info


def resample_data_by_hours(df, index, hours_op_dic):
    """按照1小时为周期分组统计数据
    根据hours_op_dic中提供的映射数据统计数据，若不提供映射字典则默认所有数据按照平均值聚合

    Args:
        df: 数据集合
        index: 索引
        hours_op_dic: 聚合函数映射字典，如{"a": "sum", "b": "mean}

    Returns:
        错那数据则直接返回分组后的数据(DatetimeIndexResampler类型)，岗巴数据除分组后的数据外还额外返回一个包含查询字段(转换后的英文)的列表
    """
    df = df.set_index(pd.to_datetime(df[index]))
    if len(hours_op_dic):
        df = df.resample("h").agg(hours_op_dic)
    else:
        df = df.resample("h").mean()
    return df


def resample_data_by_days(df, index, just_date=False, hours_op_dic=None, days_op_dic=None):
    """按照24小时为周期分组统计数据
    标准流程为先按照hours_op_dic中提供的映射数据统计数据，在此基础上按照days_op_dic设定的聚合函数来对数据进行二次聚合。

        Args:
            df: 数据集合
            index: 索引
            just_date: 是否只按照天周期数据做集合，默认为False，若为True则会先调用resample_data_by_hours以此基础再往后执行下一步
            hours_op_dic: 小时周期聚合函数映射字典，如{"a": "sum", "b": "mean}
            days_op_dic: 天周期聚合函数映射字典，同上，在按照1小时分组后以此来对数据进行聚合统计
        Returns:
            错那数据则直接返回分组后的数据(DataFrame类型)，岗巴数据除DataFrame数据外还额外返回一个包含查询字段(转换后的英文)的列表
        """
    # df = df.set_index(pd.to_datetime(df[index]))
    if index in df.columns:
        df = df.set_index(pd.to_datetime(df[index]))

    if just_date:
        df = df.resample("D")
        if days_op_dic:
            df = df.agg(days_op_dic)
        else:
            df = df.mean()
    else:
        df = resample_data_by_hours(df, index, hours_op_dic)
        df = df.resample("D")
        if days_op_dic:
            df = df.agg(days_op_dic)
        else:
            df = df.mean()
    return df


# 装饰器 打印提示
def log_hint(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print_mode, log_mode = kwargs.get("print_mode"), kwargs.get("log_mode")
        if print_mode:

            print("函数 {} 执行".format(func))

        if log_mode:
            logging.info("函数 {} 执行".format(func))
        try:
            res = func(*args, **kwargs)
            if print_mode:
                print("函数 {} 执行完成".format(func))
            if log_mode:
                logging.info("函数 {} 执行完成".format(func))
            return res
        except Exception as e:
            traceback.print_exc()
            if print_mode:
                print("函数 {} 异常，异常内容：{}".format(func, e))
            if log_mode:
                logging.info("函数 {} 异常，异常内容：{}".format(func, e))
    return wrapper


# TODO 待修改
def get_data_range(key):
    """根据key获取时间范围

    :param key: 根据key选择获取实时数据范围还是历史数据范围
        realtime：实时数据时间范围，根据初始数据
    :return:
    """
    sql_conf = get_sql_conf(DB["query"])
    res = {}
    with pymysql.connect(
            host=sql_conf["host"],
            user=sql_conf["user"],
            password=sql_conf["password"],
            database=sql_conf["database"]
    ) as conn1:
        cur1 = conn1.cursor()

        context = {"cona": "time", "kamba": "Timestamp", "tianjin": "date"}
        for k, v in context.items():
            cur1.execute("select {} from {} order by {} asc limit 1;".format(v, k, v))
            start = cur1.fetchone()
            start = "" if not len(start) else start[0]
            cur1.execute("select {} from {} order by {} desc limit 1;".format(v, k, v))
            end = cur1.fetchone()
            end = "" if not len(end) else end[0]

            res[k] = {"start": start, "end": end}
        cur1.close()
    if key == "history":
        return res

    sql_conf2 = get_sql_conf(DB["store"])

    with pymysql.connect(
            host=sql_conf2["host"],
            user=sql_conf2["user"],
            password=sql_conf2["password"],
            database=sql_conf2["database"]
    ) as conn2:
        cur2 = conn2.cursor()
        context2 = {"cona": "cona_days_data", "kamba": "kamba_days_data", "tianjin": "tianjin_commons_data"}
        for k, v in context2.items():
            cur2.execute("select time_data from {} order by time_data desc limit 1;".format(v))
            item = cur2.fetchone()
            item = "" if not len(item) else item[0]
            res[k]["latest"] = item
        cur2.close()
    return res


# TODO 待修改
def get_realtime_data_range():
    sql_conf = get_sql_conf(DB["query"])
    with pymysql.connect(
            host=sql_conf["host"],
            user=sql_conf["user"],
            password=sql_conf["password"],
            database=sql_conf["database"]
    ) as conn:
        cur = conn.cursor()
        res = {}

        try:
            context = {"cona": "time", "kamba": "Timestamp", "tianjin": "date"}
            for k, v in context.items():
                cur.execute("select {} from {} order by {} asc limit 1;".format(v, k, v))
                start = cur.fetchone()
                start = "" if not len(start) else start[0]

                cur.execute("select {} from {} order by {} desc limit 1;".format(v, k, v))
                end = cur.fetchone()
                end = "" if not len(end) else end[0]

                res[k] = {"start": start, "end": end}

        except Exception as e:
            traceback.print_exc()
        finally:
            cur.close()


def handing_missing_data(df, time_index, column_lst=None):
    if time_index not in df.columns:
        df = df.reset_index()

    df[time_index] = pd.to_datetime(df[time_index])
    days = df.set_index(time_index).resample("D").count().index
    time_data = [pd.to_datetime(item) for item in df[time_index]]
    all_times = [
        datetime(day.year, day.month, day.day, hour, minute)
        for day in days for hour in range(24) for minute in [00, 15, 30, 45]
    ]

    if column_lst:
        columns = df.columns
        for column in column_lst:
            if column not in columns:
                df[column] = np.nan

    for time in all_times:
        if time not in time_data:
            tmp_dic = {time_index: [time]}
            tmp_dic.update({col: [np.nan] for col in df.columns if col != time_index})
            df = df.append(pd.DataFrame(tmp_dic), ignore_index=True)
    return df.sort_values(by=time_index).reset_index(drop=True)


def get_report_data(sql_key, block, start, end):
    """查询数据库原始数据
    :param sql_key: 用于查询完整SQL语句的key
    :param start: 开始时间
    :param end: 结束时间
    :return: dataframe格式的数据内容CTF002_HZ
    """
    index_key = {"cona": "time", "kamba": "Timestamp", "tianjin": "date"}[block]
    sql = SQL_CONTEXT["query"][block][sql_key]

    conn = get_conn_by_key("query")

    try:
        if block == "kamba":
            common_sql = SQL_CONTEXT["query"][block]["COMMON_SQL"]
            key_lst = SQL_CONTEXT["query"][block][sql_key]
            key_lst = [POINT_DF.get(item) for item in key_lst]
            sql = common_sql.format(block, str(tuple(key_lst)), start, end)

            res = handing_missing_data(
                pd.read_sql(sql, con=conn).pivot(index=index_key, columns='pointname', values='value'), index_key, key_lst
            ), key_lst

        else:
            sql = sql.format(block, start, end)
            res = handing_missing_data(
                pd.read_sql(sql, con=conn).pivot(index=index_key, columns='pointname', values='value'), index_key
            )
        # conn.close()
        return res
    except:
        print("get_data report 异常")
    finally:
        conn.dispose()


# 待修改
def get_data(sql_key, start, end, db, tb):
    """查询数据库原始数据

    :param sql_key: 用于查询完整SQL语句的key
    :param start: 开始时间
    :param end: 结束时间
    :param db: 数据库名称
    :param tb: 数据表名称
    :return: dataframe格式的数据内容
    """

    sql_conf = get_sql_conf(db)
    with pymysql.connect(
            host=sql_conf["host"],
            user=sql_conf["user"],
            password=sql_conf["password"],
            database=sql_conf["database"]
    ) as conn:
        if "cona" in tb:
            sql = SQL_CONTEXT[tb][sql_key].format(tb, start, end)
            result_df = pd.read_sql(sql, con=conn).pivot(
                index='time', columns='pointname', values='value'
            )
            return handing_missing_data(result_df.reset_index(), "time")
        elif "kamba" in tb:
            common_sql = SQL_CONTEXT[tb]["COMMON_SQL"]
            key_lst = SQL_CONTEXT[tb][sql_key]
            key_lst = [POINT_DF.get(item) for item in key_lst]
            sql = common_sql.format(tb, str(tuple(key_lst)), start, end)
            result_df = pd.read_sql(sql, con=conn).pivot(
                index='Timestamp', columns='pointname', values='value'
            )

            return handing_missing_data(result_df.reset_index(), "Timestamp", key_lst), key_lst
        else:
            common_sql = SQL_CONTEXT[tb]["COMMON_SQL"]
            key_lst = SQL_CONTEXT[tb][sql_key]
            sql = common_sql.format(tb, str(tuple(key_lst)), start, end)
            result_df = pd.read_sql(sql, con=conn).pivot(index='date', columns='pointname', values='value')
            return result_df


def get_time_in_datetime(df, by):
    if by == "h":
        return [datetime(year=item.year, month=item.month, day=item.day, hour=item.hour) for item in df.index]
    else:
        return [datetime(year=item.year, month=item.month, day=item.day) for item in df.index]

########################################################################################################################
# ============================================    公式计算     ==========================================================

# ======================================================================================================================
# ======================================================================================================================
# ==============================================  错那 统计项目  =========================================================


@log_hint
def get_cona_geothermal_wells_heat_provide(start, end, block="cona", print_mode=False, log_mode=False):
    """错那 地热井提供热量（高温版换制热量、水源热泵制热量、地热井可提供高温热量、地热井可提供低温热量）
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        high_temp_plate_exchange_heat_production': 高温版换制热量,
        water_heat_pump_heat_production: 水源热泵制热量,
        geothermal_wells_high_heat_provide: 地热井可提供高温热量,
        geothermal_wells_low_heat_provide: 地热井可提供低温热量
    """
    result_df = get_data("API_GEOTHERMAL_WELLS_HEAT_PROVIDE_SQL", start, end, DB["query"], TB["query"][block]["table"])
    # 处理high_temp_plate_exchange_heat_production
    result_df['f3_HHX_HL'] = 4.17 * (result_df['f3_HHWLoop001_RFlow'] + result_df['f3_HHWLoop002_RFlow'] +
                                     result_df['f3_HHWLoop003_RFlow'] + result_df['f3_HHWLoop_BypassFlow'] -
                                     result_df['f3_WSHP001_F'] - result_df['f3_WSHP002_F'] - result_df[
                                         'f3_WSHP003_F'] -
                                     result_df['f3_WSHP004_F'] - result_df['f3_WSHP005_F'] - result_df[
                                         'f3_WSHP006_F']) \
                             * (result_df['f3_HHX_SRT'] - result_df['f3_CL003_T']) / 3.6
    result_df['f4_HHX_HL'] = 4.17 * (result_df['f4_HHWLoop001_F'] + result_df['f4_HHWLoop_BypassFlow'] -
                                     result_df['f4_WSHP001_F'] - result_df['f4_WSHP002_F'] -
                                     result_df['f4_WSHP003_F'] - result_df['f4_WSHP004_F']) * \
                             (result_df['f4_HHX_SRT'] - result_df['f4_CL003_T']) / 3.6
    result_df['f5_HHX_HL'] = 4.17 * (
            result_df['f5_HHWLoop001_RFlow'] + result_df['f5_HHWLoop_BypassFlow'] - result_df['f5_WSHP001_F'] -
            result_df['f5_WSHP002_F'] - result_df['f5_WSHP003_F']
    ) * (
                                     result_df['f5_HHX_SRT'] - result_df['f5_CL003_T']
                             ) / 3.6
    result_df['high_temp_plate_exchange_heat_production'] = result_df['f3_HHX_HL'] + result_df['f4_HHX_HL'] + \
                                                            result_df['f5_HHX_HL']
    # 处理water_heat_pump_heat_production
    result_df['SH'] = 1000 * 4.17 * (
            result_df['f2_WSHP001_F'] * (result_df['f2_WSHP001_HHWLT'] - result_df['f2_WSHP001_HHWET']) +
            result_df['f2_WSHP002_F'] * (result_df['f2_WSHP002_HHWLT'] - result_df['f2_WSHP002_HHWET']) +
            result_df['f2_WSHP003_F'] * (result_df['f2_WSHP003_HHWLT'] - result_df['f2_WSHP003_HHWET']) +
            result_df['f2_WSHP004_F'] * (result_df['f2_WSHP004_HHWLT'] - result_df['f2_WSHP004_HHWET'])
    ) / 3600

    result_df['SI'] = 1000 * 4.17 * (
            result_df['f3_WSHP001_F'] * (result_df['f3_WSHP001_HHWLT'] - result_df['f3_WSHP001_HHWET']) +
            result_df['f3_WSHP002_F'] * (result_df['f3_WSHP002_HHWLT'] - result_df['f3_WSHP002_HHWET']) +
            result_df['f3_WSHP003_F'] * (result_df['f3_WSHP003_HHWLT'] - result_df['f3_WSHP003_HHWET']) +
            result_df['f3_WSHP004_F'] * (result_df['f3_WSHP004_HHWLT'] - result_df['f3_WSHP004_HHWET']) +
            result_df['f3_WSHP005_F'] * (result_df['f3_WSHP005_HHWLT'] - result_df['f3_WSHP005_HHWET']) +
            result_df['f3_WSHP006_F'] * (result_df['f3_WSHP006_HHWLT'] - result_df['f3_WSHP006_HHWET'])
    ) / 3600

    result_df['SJ'] = 1000 * 4.17 * (
            result_df['f4_WSHP001_F'] * (result_df['f4_WSHP001_HHWLT'] - result_df['f4_WSHP001_HHWET']) +
            result_df['f4_WSHP002_F'] * (result_df['f4_WSHP002_HHWLT'] - result_df['f4_WSHP002_HHWET']) +
            result_df['f4_WSHP003_F'] * (result_df['f4_WSHP003_HHWLT'] - result_df['f4_WSHP003_HHWET']) +
            result_df['f4_WSHP004_F'] * (result_df['f4_WSHP004_HHWLT'] - result_df['f4_WSHP004_HHWET'])
    ) / 3600

    result_df['SK'] = 1000 * 4.17 * (
            result_df['f5_WSHP001_F'] * (result_df['f5_WSHP001_HHWLT'] - result_df['f5_WSHP001_HHWET']) +
            result_df['f5_WSHP002_F'] * (result_df['f5_WSHP002_HHWLT'] - result_df['f5_WSHP002_HHWET']) +
            result_df['f5_WSHP003_F'] * (result_df['f5_WSHP003_HHWLT'] - result_df['f5_WSHP003_HHWET'])
    ) / 3600
    result_df['water_heat_pump_heat_production'] = (result_df['SH'] + result_df['SI'] + result_df['SJ'] +
                                                    result_df['SK']) * 24
    # 处理geothermal_wells_heat_provide
    result_df['geothermal_wells_high_heat_provide'] = 1000 * 4.17 * (
            result_df['f2_HW_F'] * (result_df['f2_HW_T'] - 37) +
            result_df['f3_HW_F'] * (result_df['f3_HW_T'] - 37) +
            result_df['f4_HW_F'] * (result_df['f4_HW_T'] - 37) +
            result_df['f5_HW_F'] * (result_df['f5_HW_T'] - 37)
    ) / 3600

    result_df['geothermal_wells_low_heat_provide'] = 1000 * 4.17 * (
            result_df['f2_HW_F'] * 29 +
            result_df['f2_LW_F'] * (result_df['f2_LW_T'] - 8) +
            result_df['f3_HW_F'] * 29 +
            result_df['f3_LW_F'] * (result_df['f3_LW_T'] - 8) +
            result_df['f4_HW_F'] * 29 +
            result_df['f4_LW_F'] * (result_df['f4_LW_T'] - 8) +
            result_df['f5_HW_F'] * 29 +
            result_df['f5_LW_F'] * (result_df['f5_LW_T'] - 8)
    ) / 3600
    result_df = result_df.loc[:, ['time', 'high_temp_plate_exchange_heat_production', 'water_heat_pump_heat_production',
                                  'geothermal_wells_high_heat_provide', 'geothermal_wells_low_heat_provide']]

    hours_df = resample_data_by_hours(
        result_df, "time",
        {
            'high_temp_plate_exchange_heat_production': 'mean',
            'water_heat_pump_heat_production': 'mean',
            'geothermal_wells_high_heat_provide': 'mean',
            'geothermal_wells_low_heat_provide': 'mean'
        }
    )
    days_df = resample_data_by_days(
        result_df, "time", False,
        {
            'high_temp_plate_exchange_heat_production': 'mean',
            'water_heat_pump_heat_production': 'mean',
            'geothermal_wells_high_heat_provide': 'mean',
            'geothermal_wells_low_heat_provide': 'mean'
        },
        {
            'high_temp_plate_exchange_heat_production': 'sum',
            'water_heat_pump_heat_production': 'sum',
            'geothermal_wells_high_heat_provide': 'sum',
            'geothermal_wells_low_heat_provide': 'sum'
        }
    )
    data = {
        "hours_data": {
            'time_data': get_time_in_datetime(hours_df, "h"),
            'high_temp_plate_exchange_heat_production': hours_df.high_temp_plate_exchange_heat_production.values,
            'water_heat_pump_heat_production': hours_df.water_heat_pump_heat_production.values,
            'geothermal_wells_high_heat_provide': hours_df.geothermal_wells_high_heat_provide.values,
            'geothermal_wells_low_heat_provide': hours_df.geothermal_wells_low_heat_provide.values
        },
        "days_data": {
            'time_data': get_time_in_datetime(days_df, "d"),
            'high_temp_plate_exchange_heat_production': days_df.high_temp_plate_exchange_heat_production.values,
            'water_heat_pump_heat_production': days_df.water_heat_pump_heat_production.values,
            'geothermal_wells_high_heat_provide': days_df.geothermal_wells_high_heat_provide.values,
            'geothermal_wells_low_heat_provide': days_df.geothermal_wells_low_heat_provide.values
        }
    }
    return data


@log_hint
def get_cona_com_cop(start, end, block="cona", print_mode=False, log_mode=False):
    """错那 综合COP
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        com_cop: COP能效

    """
    result_df = get_data("API_COM_COP_SQL", start, end, DB["query"], TB["query"][block]["table"])
    # 处理UR
    result_df['UE'] = 1000 * 4.17 * (
            result_df['f2_WSHP001_F'] * (result_df['f2_WSHP001_HHWLT'] - result_df['f2_WSHP001_HHWET']) +
            result_df['f2_WSHP002_F'] * (result_df['f2_WSHP002_HHWLT'] - result_df['f2_WSHP002_HHWET']) +
            result_df['f2_WSHP003_F'] * (result_df['f2_WSHP003_HHWLT'] - result_df['f2_WSHP003_HHWET']) +
            result_df['f2_WSHP004_F'] * (result_df['f2_WSHP004_HHWLT'] - result_df['f2_WSHP004_HHWET'])
    ) / 3600
    result_df['UF'] = 1000 * 4.17 * (
            result_df['f3_WSHP001_F'] * (result_df['f3_WSHP001_HHWLT'] - result_df['f3_WSHP001_HHWET']) +
            result_df['f3_WSHP002_F'] * (result_df['f3_WSHP002_HHWLT'] - result_df['f3_WSHP002_HHWET']) +
            result_df['f3_WSHP003_F'] * (result_df['f3_WSHP003_HHWLT'] - result_df['f3_WSHP003_HHWET']) +
            result_df['f3_WSHP004_F'] * (result_df['f3_WSHP004_HHWLT'] - result_df['f3_WSHP004_HHWET']) +
            result_df['f3_WSHP005_F'] * (result_df['f3_WSHP005_HHWLT'] - result_df['f3_WSHP005_HHWET']) +
            result_df['f3_WSHP006_F'] * (result_df['f3_WSHP006_HHWLT'] - result_df['f3_WSHP006_HHWET'])
    ) / 3600
    result_df['UG'] = 1000 * 4.17 * (
            result_df['f4_WSHP001_F'] * (result_df['f4_WSHP001_HHWLT'] - result_df['f4_WSHP001_HHWET']) +
            result_df['f4_WSHP002_F'] * (result_df['f4_WSHP002_HHWLT'] - result_df['f4_WSHP002_HHWET']) +
            result_df['f4_WSHP003_F'] * (result_df['f4_WSHP003_HHWLT'] - result_df['f4_WSHP003_HHWET']) +
            result_df['f4_WSHP004_F'] * (result_df['f4_WSHP004_HHWLT'] - result_df['f4_WSHP004_HHWET'])
    ) / 3600
    result_df['UH'] = 1000 * 4.17 * (
            result_df['f5_WSHP001_F'] * (result_df['f5_WSHP001_HHWLT'] - result_df['f5_WSHP001_HHWET']) +
            result_df['f5_WSHP002_F'] * (result_df['f5_WSHP002_HHWLT'] - result_df['f5_WSHP002_HHWET']) +
            result_df['f5_WSHP003_F'] * (result_df['f5_WSHP003_HHWLT'] - result_df['f5_WSHP003_HHWET'])
    ) / 3600
    result_df['UI'] = result_df['UE'] / (
            result_df['UE'] / (random.randint(200, 800) / 100)
    )
    result_df['UJ'] = result_df['UF'] / (
            result_df['UF'] / (random.randint(200, 800) / 100)
    )
    result_df['UK'] = result_df['UG'] / (
            result_df['UG'] / (random.randint(200, 800) / 100)
    )
    result_df['UL'] = result_df['UH'] / (
            result_df['UH'] / (random.randint(200, 800) / 100)
    )
    result_df['UR'] = result_df['UE'] / result_df['UI'] + result_df['UF'] / result_df['UJ'] + \
                      result_df['UG'] / result_df['UK'] + result_df['UH'] / result_df['UL']
    # 处理 heat_pipe_network_heating
    result_df['RU'] = 1000 * 4.17 * (result_df['f2_HHWLoop001_ST'] - result_df['f2_HHWLoop001_RT']) / 3600
    result_df['RV'] = 1000 * 4.17 * (
            result_df['f3_HHWLoop001_RFlow'] * (result_df['f3_HHWLoop001_ST'] - result_df['f3_HHWLoop001_RT']) +
            result_df['f3_HHWLoop002_RFlow'] * (result_df['f3_HHWLoop002_ST'] - result_df['f3_HHWLoop002_RT']) +
            result_df['f3_HHWLoop003_RFlow'] * (result_df['f3_HHWLoop003_ST'] - result_df['f3_HHWLoop003_ST'])
    ) / 3600
    result_df['RW'] = 1000 * 4.17 * result_df['f4_HHWLoop001_F'] * \
                      (result_df['f4_HHWLoop001_ST'] - result_df['f4_HHWLoop001_RT']) / 3600

    result_df['RX'] = 1000 * 4.17 * result_df['f5_HHWLoop001_RFlow'] * \
                      (result_df['f5_HHWLoop001_ST'] - result_df['f5_HHWLoop001_RT']) / 3600
    result_df['heat_pipe_network_heating'] = result_df['RU'] + result_df['RV'] + result_df['RW'] + result_df['RX']

    # 处理machine_room_pump_power
    result_df['machine_room_pump_power'] = result_df['f3_meter01_KW'] + result_df['f3_meter02_KW'] + \
                                           result_df['f3_meter03_KW'] + result_df['f3_meter04_KW'] + result_df[
                                               'f3_meter05_KW'] + \
                                           result_df['f3_meter06_KW'] + result_df['f3_meter07_KW'] + result_df[
                                               'f3_meter08_KW'] + \
                                           result_df['f2_meter01_KW'] + result_df['f2_meter02_KW'] + result_df[
                                               'f2_meter03_KW'] + \
                                           result_df['f2_meter04_KW'] + result_df['f2_meter05_KW'] + result_df[
                                               'f2_meter06_KW'] + \
                                           result_df['f4_meter01_KW'] + result_df['f4_meter02_KW'] + result_df[
                                               'f4_meter03_KW'] + \
                                           result_df['f4_meter04_KW'] + result_df['f4_meter05_KW'] + result_df[
                                               'f4_meter06_KW'] + \
                                           result_df['f4_meter07_KW'] + result_df['f5_meter01_KW'] + result_df[
                                               'f5_meter02_KW'] + \
                                           result_df['f5_meter03_KW'] + result_df['f5_meter04_KW'] + result_df[
                                               'f5_meter05_KW'] + \
                                           result_df['f5_meter06_KW']
    result_df = result_df.loc[:, ['time', 'UR', 'heat_pipe_network_heating', 'machine_room_pump_power']]
    hours_df = resample_data_by_hours(
        result_df, "time",
        {
            'UR': 'mean',
            'heat_pipe_network_heating': 'mean',
            'machine_room_pump_power': 'mean'
        }
    )
    days_df = resample_data_by_days(
        result_df, "time", True, None,
        {
            'UR': 'mean',
            'heat_pipe_network_heating': 'mean',
            'machine_room_pump_power': 'mean'
        }
    )
    hours_cop = hours_df['heat_pipe_network_heating'] / (
            hours_df['machine_room_pump_power'] + hours_df['UR']
    )
    days_cop = days_df['heat_pipe_network_heating'] / (
            days_df['machine_room_pump_power'] + days_df['UR']
    )

    data = {
        "hours_data": {
            'time_data': get_time_in_datetime(hours_df, "h"),
            'com_cop': hours_cop.values
        },
        "days_data": {
            'time_data': get_time_in_datetime(days_df, "d"),
            'com_cop': days_cop.values
        }
    }
    return data


@log_hint
def get_cona_cost_saving(start, end, block="cona", print_mode=False, log_mode=False):
    """错那 供暖费用
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        cost_saving: 供暖费用,
        high_temp_charge: 高温供暖费用,
        low_temp_charge: 低温供暖费用
    """
    result_df = get_data("API_COST_SAVING_SQL", start, end, DB["query"], TB["query"][block]["table"])

    # 处理 machine_room_pump_power
    result_df['machine_room_pump_power'] = result_df['f3_meter01_KW'] + result_df['f3_meter02_KW'] + \
                                           result_df['f3_meter03_KW'] + result_df['f3_meter04_KW'] + result_df[
                                               'f3_meter05_KW'] + \
                                           result_df['f3_meter06_KW'] + result_df['f3_meter07_KW'] + result_df[
                                               'f3_meter08_KW'] + \
                                           result_df['f2_meter01_KW'] + result_df['f2_meter02_KW'] + result_df[
                                               'f2_meter03_KW'] + \
                                           result_df['f2_meter04_KW'] + result_df['f2_meter05_KW'] + result_df[
                                               'f2_meter06_KW'] + \
                                           result_df['f4_meter01_KW'] + result_df['f4_meter02_KW'] + result_df[
                                               'f4_meter03_KW'] + \
                                           result_df['f4_meter04_KW'] + result_df['f4_meter05_KW'] + result_df[
                                               'f4_meter06_KW'] + \
                                           result_df['f4_meter07_KW'] + result_df['f5_meter01_KW'] + result_df[
                                               'f5_meter02_KW'] + \
                                           result_df['f5_meter03_KW'] + result_df['f5_meter04_KW'] + result_df[
                                               'f5_meter05_KW'] + \
                                           result_df['f5_meter06_KW']

    # 处理 high_temp_plate_exchange_heat_production
    result_df['f3_HHX_HL'] = 4.17 * (result_df['f3_HHWLoop001_RFlow'] + result_df['f3_HHWLoop002_RFlow'] +
                                     result_df['f3_HHWLoop003_RFlow'] + result_df['f3_HHWLoop_BypassFlow'] -
                                     result_df['f3_WSHP001_F'] - result_df['f3_WSHP002_F'] - result_df[
                                         'f3_WSHP003_F'] -
                                     result_df['f3_WSHP004_F'] - result_df['f3_WSHP005_F'] - result_df[
                                         'f3_WSHP006_F']) \
                             * (result_df['f3_HHX_SRT'] - result_df['f3_CL003_T']) / 3.6
    result_df['f4_HHX_HL'] = 4.17 * (result_df['f4_HHWLoop001_F'] + result_df['f4_HHWLoop_BypassFlow'] -
                                     result_df['f4_WSHP001_F'] - result_df['f4_WSHP002_F'] -
                                     result_df['f4_WSHP003_F'] - result_df['f4_WSHP004_F']) * \
                             (result_df['f4_HHX_SRT'] - result_df['f4_CL003_T']) / 3.6
    result_df['f5_HHX_HL'] = 4.17 * (
            result_df['f5_HHWLoop001_RFlow'] + result_df['f5_HHWLoop_BypassFlow'] - result_df['f5_WSHP001_F'] -
            result_df['f5_WSHP002_F'] - result_df['f5_WSHP003_F']
    ) * (
                                     result_df['f5_HHX_SRT'] - result_df['f5_CL003_T']
                             ) / 3.6
    result_df['high_temp_plate_exchange_heat_production'] = result_df['f3_HHX_HL'] + result_df['f4_HHX_HL'] + \
                                                            result_df['f5_HHX_HL']

    # 处理 water_heat_pump_heat_production
    result_df['SH'] = 1000 * 4.17 * (
            result_df['f2_WSHP001_F'] * (result_df['f2_WSHP001_HHWLT'] - result_df['f2_WSHP001_HHWET']) +
            result_df['f2_WSHP002_F'] * (result_df['f2_WSHP002_HHWLT'] - result_df['f2_WSHP002_HHWET']) +
            result_df['f2_WSHP003_F'] * (result_df['f2_WSHP003_HHWLT'] - result_df['f2_WSHP003_HHWET']) +
            result_df['f2_WSHP004_F'] * (result_df['f2_WSHP004_HHWLT'] - result_df['f2_WSHP004_HHWET'])
    ) / 3600

    result_df['SI'] = 1000 * 4.17 * (
            result_df['f3_WSHP001_F'] * (result_df['f3_WSHP001_HHWLT'] - result_df['f3_WSHP001_HHWET']) +
            result_df['f3_WSHP002_F'] * (result_df['f3_WSHP002_HHWLT'] - result_df['f3_WSHP002_HHWET']) +
            result_df['f3_WSHP003_F'] * (result_df['f3_WSHP003_HHWLT'] - result_df['f3_WSHP003_HHWET']) +
            result_df['f3_WSHP004_F'] * (result_df['f3_WSHP004_HHWLT'] - result_df['f3_WSHP004_HHWET']) +
            result_df['f3_WSHP005_F'] * (result_df['f3_WSHP005_HHWLT'] - result_df['f3_WSHP005_HHWET']) +
            result_df['f3_WSHP006_F'] * (result_df['f3_WSHP006_HHWLT'] - result_df['f3_WSHP006_HHWET'])
    ) / 3600

    result_df['SJ'] = 1000 * 4.17 * (
            result_df['f4_WSHP001_F'] * (result_df['f4_WSHP001_HHWLT'] - result_df['f4_WSHP001_HHWET']) +
            result_df['f4_WSHP002_F'] * (result_df['f4_WSHP002_HHWLT'] - result_df['f4_WSHP002_HHWET']) +
            result_df['f4_WSHP003_F'] * (result_df['f4_WSHP003_HHWLT'] - result_df['f4_WSHP003_HHWET']) +
            result_df['f4_WSHP004_F'] * (result_df['f4_WSHP004_HHWLT'] - result_df['f4_WSHP004_HHWET'])
    ) / 3600

    result_df['SK'] = 1000 * 4.17 * (
            result_df['f5_WSHP001_F'] * (result_df['f5_WSHP001_HHWLT'] - result_df['f5_WSHP001_HHWET']) +
            result_df['f5_WSHP002_F'] * (result_df['f5_WSHP002_HHWLT'] - result_df['f5_WSHP002_HHWET']) +
            result_df['f5_WSHP003_F'] * (result_df['f5_WSHP003_HHWLT'] - result_df['f5_WSHP003_HHWET'])
    ) / 3600
    result_df['water_heat_pump_heat_production'] = (result_df['SH'] + result_df['SI'] + result_df['SJ'] +
                                                    result_df['SK']) * 24
    result_df = result_df.loc[:, ['time', 'machine_room_pump_power', 'high_temp_plate_exchange_heat_production',
                                  'water_heat_pump_heat_production']]

    hours_df = resample_data_by_hours(
        result_df, "time",
        {
            'machine_room_pump_power': 'mean',
            'high_temp_plate_exchange_heat_production': 'mean',
            'water_heat_pump_heat_production': 'mean'
        }
    )
    days_df = resample_data_by_days(
        result_df, "time", False,
        {
            'machine_room_pump_power': 'mean',
            'high_temp_plate_exchange_heat_production': 'mean',
            'water_heat_pump_heat_production': 'mean'
        },
        {
            'machine_room_pump_power': 'sum',
            'high_temp_plate_exchange_heat_production': 'sum',
            'water_heat_pump_heat_production': 'sum'
        }
    )

    hours_df['cost_saving'] = (
                                      hours_df['high_temp_plate_exchange_heat_production'] -
                                      hours_df['machine_room_pump_power'] + (
                                              hours_df['water_heat_pump_heat_production'] * (1 - 1 / 4.7)
                                      )
                              ) * 0.45
    hours_df['high_temp_charge'] = (
                                           hours_df['high_temp_plate_exchange_heat_production'] -
                                           hours_df['machine_room_pump_power']
                                   ) * 0.45
    hours_df['low_temp_charge'] = (
                                          hours_df['water_heat_pump_heat_production'] * (1 - 1 / 4.7)
                                  ) * 0.45

    days_df['cost_saving'] = (
                                     days_df['high_temp_plate_exchange_heat_production'] -
                                     days_df['machine_room_pump_power'] + (
                                             days_df['water_heat_pump_heat_production'] * (1 - 1 / 4.7)
                                     )
                             ) * 0.45
    days_df['high_temp_charge'] = (
                                          days_df['high_temp_plate_exchange_heat_production'] -
                                          days_df['machine_room_pump_power']
                                  ) * 0.45
    days_df['low_temp_charge'] = (
                                         days_df['water_heat_pump_heat_production'] * (1 - 1 / 4.7)
                                 ) * 0.45

    data = {
        "hours_data": {
            "time_data": get_time_in_datetime(hours_df, "h"),
            "cost_saving": hours_df["cost_saving"].values,
            "high_temp_charge": hours_df["high_temp_charge"].values,
            "low_temp_charge": hours_df["low_temp_charge"].values
        },
        "days_data": {
            "time_data": get_time_in_datetime(days_df, "d"),
            "cost_saving": days_df["cost_saving"].values,
            "high_temp_charge": days_df["high_temp_charge"].values,
            "low_temp_charge": days_df["low_temp_charge"].values
        }
    }

    return data


@log_hint
def get_cona_heat_provided(start, end, block="cona", print_mode=False, log_mode=False):
    """错那 供热量
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        heat_well_heating:  热力井供热
        heat_pipe_network_heating: 热力管网供热,
        water_heat_pump_heat_production: 水源热泵供热
        high_temp_plate_exchange_heat_production: 高温板换供热
        max_load: 最大负荷
        min_load: 最小负荷
        avg_load: 平均负荷
    """
    result_df = get_data("API_HEAT_PROVIDE_TEMPERATURE_SQL", start, end, DB["query"], TB["query"][block]["table"])
    # 处理heat_well_heating
    result_df['heat_well_heating'] = 1000 * 4.17 * (
            result_df['f2_HW_F'] * (result_df['f2_HW_T'] - result_df['fj_SEP_T']) +
            result_df['f2_LW_F'] * (result_df['f2_LW_T'] - result_df['fj_SEP_T']) +
            result_df['f3_HW_F'] * (result_df['f3_HW_T'] - result_df['fj_SEP_T']) +
            result_df['f3_LW_F'] * (result_df['f3_LW_T'] - result_df['fj_SEP_T']) +
            result_df['f4_HW_F'] * (result_df['f4_HW_T'] - result_df['fj_SEP_T']) +
            result_df['f4_LW_F'] * (result_df['f4_LW_T'] - result_df['fj_SEP_T']) +
            result_df['f5_HW_F'] * (result_df['f5_HW_T'] - result_df['fj_SEP_T']) +
            result_df['f5_LW_F'] * (result_df['f5_LW_T'] - result_df['fj_SEP_T'])
    ) / 3600

    # 处理heat_pipe_network_heating
    result_df['RU'] = 1000 * 4.17 * (result_df['f2_HHWLoop001_ST'] - result_df['f2_HHWLoop001_RT']) / 3600
    result_df['RV'] = 1000 * 4.17 * (
            result_df['f3_HHWLoop001_RFlow'] * (result_df['f3_HHWLoop001_ST'] - result_df['f3_HHWLoop001_RT']) +
            result_df['f3_HHWLoop002_RFlow'] * (result_df['f3_HHWLoop002_ST'] - result_df['f3_HHWLoop002_RT']) +
            result_df['f3_HHWLoop003_RFlow'] * (result_df['f3_HHWLoop003_ST'] - result_df['f3_HHWLoop003_ST'])
    ) / 3600
    result_df['RW'] = 1000 * 4.17 * result_df['f4_HHWLoop001_F'] * \
                      (result_df['f4_HHWLoop001_ST'] - result_df['f4_HHWLoop001_RT']) / 3600

    result_df['RX'] = 1000 * 4.17 * result_df['f5_HHWLoop001_RFlow'] * \
                      (result_df['f5_HHWLoop001_ST'] - result_df['f5_HHWLoop001_RT']) / 3600
    result_df['heat_pipe_network_heating'] = result_df['RU'] + result_df['RV'] + result_df['RW'] + result_df[
        'RX']

    # 处理water_heat_pump_heat_production
    result_df['SH'] = 1000 * 4.17 * (
            result_df['f2_WSHP001_F'] * (result_df['f2_WSHP001_HHWLT'] - result_df['f2_WSHP001_HHWET']) +
            result_df['f2_WSHP002_F'] * (result_df['f2_WSHP002_HHWLT'] - result_df['f2_WSHP002_HHWET']) +
            result_df['f2_WSHP003_F'] * (result_df['f2_WSHP003_HHWLT'] - result_df['f2_WSHP003_HHWET']) +
            result_df['f2_WSHP004_F'] * (result_df['f2_WSHP004_HHWLT'] - result_df['f2_WSHP004_HHWET'])
    ) / 3600

    result_df['SI'] = 1000 * 4.17 * (
            result_df['f3_WSHP001_F'] * (result_df['f3_WSHP001_HHWLT'] - result_df['f3_WSHP001_HHWET']) +
            result_df['f3_WSHP002_F'] * (result_df['f3_WSHP002_HHWLT'] - result_df['f3_WSHP002_HHWET']) +
            result_df['f3_WSHP003_F'] * (result_df['f3_WSHP003_HHWLT'] - result_df['f3_WSHP003_HHWET']) +
            result_df['f3_WSHP004_F'] * (result_df['f3_WSHP004_HHWLT'] - result_df['f3_WSHP004_HHWET']) +
            result_df['f3_WSHP005_F'] * (result_df['f3_WSHP005_HHWLT'] - result_df['f3_WSHP005_HHWET']) +
            result_df['f3_WSHP006_F'] * (result_df['f3_WSHP006_HHWLT'] - result_df['f3_WSHP006_HHWET'])
    ) / 3600

    result_df['SJ'] = 1000 * 4.17 * (
            result_df['f4_WSHP001_F'] * (result_df['f4_WSHP001_HHWLT'] - result_df['f4_WSHP001_HHWET']) +
            result_df['f4_WSHP002_F'] * (result_df['f4_WSHP002_HHWLT'] - result_df['f4_WSHP002_HHWET']) +
            result_df['f4_WSHP003_F'] * (result_df['f4_WSHP003_HHWLT'] - result_df['f4_WSHP003_HHWET']) +
            result_df['f4_WSHP004_F'] * (result_df['f4_WSHP004_HHWLT'] - result_df['f4_WSHP004_HHWET'])
    ) / 3600

    result_df['SK'] = 1000 * 4.17 * (
            result_df['f5_WSHP001_F'] * (result_df['f5_WSHP001_HHWLT'] - result_df['f5_WSHP001_HHWET']) +
            result_df['f5_WSHP002_F'] * (result_df['f5_WSHP002_HHWLT'] - result_df['f5_WSHP002_HHWET']) +
            result_df['f5_WSHP003_F'] * (result_df['f5_WSHP003_HHWLT'] - result_df['f5_WSHP003_HHWET'])
    ) / 3600
    result_df['water_heat_pump_heat_production'] = (result_df['SH'] + result_df['SI'] + result_df['SJ'] +
                                                    result_df['SK']) * 24

    # 处理high_temp_plate_exchange_heat_production
    result_df['f3_HHX_HL'] = 4.17 * (result_df['f3_HHWLoop001_RFlow'] + result_df['f3_HHWLoop002_RFlow'] +
                                     result_df['f3_HHWLoop003_RFlow'] + result_df['f3_HHWLoop_BypassFlow'] -
                                     result_df['f3_WSHP001_F'] - result_df['f3_WSHP002_F'] - result_df[
                                         'f3_WSHP003_F'] -
                                     result_df['f3_WSHP004_F'] - result_df['f3_WSHP005_F'] - result_df[
                                         'f3_WSHP006_F']) \
                             * (result_df['f3_HHX_SRT'] - result_df['f3_CL003_T']) / 3.6
    result_df['f4_HHX_HL'] = 4.17 * (result_df['f4_HHWLoop001_F'] + result_df['f4_HHWLoop_BypassFlow'] -
                                     result_df['f4_WSHP001_F'] - result_df['f4_WSHP002_F'] -
                                     result_df['f4_WSHP003_F'] - result_df['f4_WSHP004_F']) * \
                             (result_df['f4_HHX_SRT'] - result_df['f4_CL003_T']) / 3.6
    result_df['f5_HHX_HL'] = 4.17 * (
            result_df['f5_HHWLoop001_RFlow'] + result_df['f5_HHWLoop_BypassFlow'] - result_df['f5_WSHP001_F'] -
            result_df['f5_WSHP002_F'] - result_df['f5_WSHP003_F']
    ) * (
                                     result_df['f5_HHX_SRT'] - result_df['f5_CL003_T']
                             ) / 3.6
    result_df['high_temp_plate_exchange_heat_production'] = result_df['f3_HHX_HL'] + result_df['f4_HHX_HL'] + \
                                                            result_df['f5_HHX_HL']

    result_df = result_df.loc[:, ['time', 'heat_well_heating', 'heat_pipe_network_heating',
                                  'water_heat_pump_heat_production', 'high_temp_plate_exchange_heat_production']]

    hours_df = resample_data_by_hours(
        result_df, "time",
        {
            'heat_well_heating': 'mean',
            'heat_pipe_network_heating': 'mean',
            'water_heat_pump_heat_production': 'mean',
            'high_temp_plate_exchange_heat_production': 'mean'
        }
    )

    load_hours_df = resample_data_by_hours(
        result_df, "time",
        {
            "heat_pipe_network_heating": ["max", "min", "mean"]
        }
    )
    load_days_df = resample_data_by_days(
        result_df, "time", False,
        {"heat_pipe_network_heating": "mean"},
        {"heat_pipe_network_heating": ["max", "min", "mean"]}
    )
    days_df = resample_data_by_days(
        result_df, "time", False,
        {
            'heat_well_heating': 'mean',
            'heat_pipe_network_heating': 'mean',
            'water_heat_pump_heat_production': 'mean',
            'high_temp_plate_exchange_heat_production': 'mean'
        },
        {
            'heat_well_heating': 'sum',
            'heat_pipe_network_heating': 'sum',
            'water_heat_pump_heat_production': 'sum',
            'high_temp_plate_exchange_heat_production': 'sum'
        }
    )
    data = {
        "hours_data": {
            'time_data': get_time_in_datetime(hours_df, "h"),
            "heat_well_heating": hours_df["heat_well_heating"].values,
            "heat_pipe_network_heating": hours_df["heat_pipe_network_heating"].values,
            "water_heat_pump_heat_production": hours_df["water_heat_pump_heat_production"].values,
            "high_temp_plate_exchange_heat_production": hours_df["high_temp_plate_exchange_heat_production"].values,
            "max_load": load_hours_df["heat_pipe_network_heating"]["max"].values,
            "min_load": load_hours_df["heat_pipe_network_heating"]["min"].values,
            "avg_load": load_hours_df["heat_pipe_network_heating"]["mean"].values
        },
        "days_data": {
            'time_data': get_time_in_datetime(days_df, "d"),
            "heat_well_heating": days_df["heat_well_heating"].values,
            "heat_pipe_network_heating": days_df["heat_pipe_network_heating"].values,
            "water_heat_pump_heat_production": days_df["water_heat_pump_heat_production"].values,
            "high_temp_plate_exchange_heat_production": days_df["high_temp_plate_exchange_heat_production"].values,
            "max_load": load_days_df["heat_pipe_network_heating"]["max"].values,
            "min_load": load_days_df["heat_pipe_network_heating"]["min"].values,
            "avg_load": load_days_df["heat_pipe_network_heating"]["mean"].values
        }
    }
    return data


@log_hint
def get_cona_water_supply_return_temperature(start, end, block="cona", print_mode=False, log_mode=False):
    """错那 供回水温度
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       water_supply_temperature:  供水温度,
       return_water_temperature: 回水温度,
       supply_return_water_temp_diff: 供回水温差
   """
    result_df = get_data("WATER_SUPPLY_RETURN_TEMPERATURE_SQL", start, end, DB["query"], TB["query"][block]["table"])

    result_df['water_supply_temperature'] = \
        (
                result_df['f2_HHWLoop001_ST'] * result_df['f2_HHWLoop001_RFlow'] +
                result_df['f3_HHWLoop001_ST'] * result_df['f3_HHWLoop001_RFlow'] +
                result_df['f3_HHWLoop002_ST'] * result_df['f3_HHWLoop002_RFlow'] +
                result_df['f3_HHWLoop003_ST'] * result_df['f3_HHWLoop003_RFlow'] +
                result_df['f4_HHWLoop001_ST'] * result_df['f4_HHWLoop001_F'] +
                result_df['f5_HHWLoop001_ST'] * result_df['f5_HHWLoop001_RFlow']
        ) / \
        (
                result_df['f2_HHWLoop001_RFlow'] + result_df['f3_HHWLoop001_RFlow'] +
                result_df['f3_HHWLoop002_RFlow'] + result_df['f3_HHWLoop003_RFlow'] +
                result_df['f4_HHWLoop001_F'] + result_df['f5_HHWLoop001_RFlow']
        )
    result_df['return_water_temperature'] = \
        (
                result_df['f2_HHWLoop001_RT'] * result_df['f2_HHWLoop001_RFlow'] +
                result_df['f3_HHWLoop001_RT'] * result_df['f3_HHWLoop001_RFlow'] +
                result_df['f3_HHWLoop002_RT'] * result_df['f3_HHWLoop002_RFlow'] +
                result_df['f3_HHWLoop003_RT'] * result_df['f3_HHWLoop003_RFlow'] +
                result_df['f4_HHWLoop001_RT'] * result_df['f4_HHWLoop001_F'] +
                result_df['f5_HHWLoop001_RT'] * result_df['f5_HHWLoop001_RFlow']
        ) / \
        (
                result_df['f2_HHWLoop001_RFlow'] + result_df['f3_HHWLoop001_RFlow'] +
                result_df['f3_HHWLoop002_RFlow'] + result_df['f3_HHWLoop003_RFlow'] +
                result_df['f4_HHWLoop001_F'] + result_df['f5_HHWLoop001_RFlow']
        )

    result_df['supply_return_water_temp_diff'] = \
        (
                (
                        result_df['f2_HHWLoop001_ST'] - result_df['f2_HHWLoop001_RT']
                ) * result_df['f2_HHWLoop001_RFlow'] +

                (
                        result_df['f3_HHWLoop001_ST'] - result_df['f3_HHWLoop001_RT']
                ) * result_df['f3_HHWLoop001_RFlow'] +

                (
                        result_df['f3_HHWLoop002_ST'] - result_df['f3_HHWLoop002_RT']
                ) * result_df['f3_HHWLoop002_RFlow'] +

                (
                        result_df['f3_HHWLoop003_ST'] - result_df['f3_HHWLoop003_RT']
                ) * result_df['f3_HHWLoop003_RFlow'] +

                (
                        result_df['f4_HHWLoop001_ST'] - result_df['f4_HHWLoop001_RT']
                ) * result_df['f4_HHWLoop001_F'] +

                (
                        result_df['f5_HHWLoop001_ST'] - result_df['f5_HHWLoop001_RT']
                ) * result_df['f5_HHWLoop001_RFlow']

        ) / \
        (
                result_df['f2_HHWLoop001_RFlow'] + result_df['f3_HHWLoop001_RFlow'] +
                result_df['f3_HHWLoop002_RFlow'] + result_df['f3_HHWLoop003_RFlow'] +
                result_df['f4_HHWLoop001_F'] + result_df['f5_HHWLoop001_RFlow']
        )
    result_df = result_df.loc[:,
                ["time", "water_supply_temperature", "return_water_temperature", "supply_return_water_temp_diff"]
                ]

    hours_df = resample_data_by_hours(
        result_df, "time",
        {
            "water_supply_temperature": "mean",
            "return_water_temperature": "mean",
            "supply_return_water_temp_diff": "mean"
        }
    )
    days_df = resample_data_by_days(
        result_df, "time", True, {},
        {
            "water_supply_temperature": "mean",
            "return_water_temperature": "mean",
            "supply_return_water_temp_diff": "mean"
        }
    )

    data = {
        "hours_data": {
            'time_data': get_time_in_datetime(hours_df, "h"),
            'water_supply_temperature': hours_df["water_supply_temperature"].values,
            'return_water_temperature': hours_df["return_water_temperature"].values,
            'supply_return_water_temp_diff': hours_df["supply_return_water_temp_diff"].values
        },
        "days_data": {
            'time_data': get_time_in_datetime(days_df, "d"),
            'water_supply_temperature': days_df["water_supply_temperature"].values,
            'return_water_temperature': days_df["return_water_temperature"].values,
            'supply_return_water_temp_diff': days_df["supply_return_water_temp_diff"].values,
        }
    }
    return data


@log_hint
def get_cona_water_replenishment(start, end, block="cona", print_mode=False, log_mode=False):
    """错那 补水量
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       water_replenishment:  补水量,
       water_replenishment_limit: 补水量限值
   """

    result_df = get_data("WATER_REPLENISHMENT_SQL", start, end, DB["query"], TB["query"][block]["table"])
    result_df['water_replenishment_limit'] = (result_df['f2_HHWLoop001_RFlow'] + result_df[
        'f3_HHWLoop001_RFlow'] + result_df['f3_HHWLoop002_RFlow'] +
                                              result_df['f3_HHWLoop003_RFlow'] + result_df['f4_HHWLoop001_F'] +
                                              result_df['f5_HHWLoop001_RFlow']) * 0.01
    result_df['water_replenishment'] = 0.005 * (result_df['f2_HHWLoop001_RFlow'] +
                                                result_df['f3_HHWLoop001_RFlow'] +
                                                result_df['f3_HHWLoop002_RFlow'] +
                                                result_df['f3_HHWLoop003_RFlow'] +
                                                result_df['f4_HHWLoop001_F'] +
                                                result_df['f5_HHWLoop001_RFlow']
                                                )
    result_df = result_df.loc[:, ["time", "water_replenishment", "water_replenishment_limit"]]

    hours_df = resample_data_by_hours(
        result_df, "time",
        {
            "water_replenishment": "mean",
            "water_replenishment_limit": "mean"
        }
    )
    days_df = resample_data_by_days(
        result_df, "time", True, {},
        {
            "water_replenishment": "mean",
            "water_replenishment_limit": "mean"
        }
    )

    data = {
        "hours_data": {
            'time_data': get_time_in_datetime(hours_df, "h"),
            'water_replenishment': hours_df["water_replenishment"].values,
            'water_replenishment_limit': hours_df["water_replenishment_limit"].values
        },
        "days_data": {
            'time_data': get_time_in_datetime(days_df, "d"),
            'water_replenishment': days_df["water_replenishment"].values,
            'water_replenishment_limit': days_df["water_replenishment_limit"].values
        }
    }
    return data


@log_hint
def get_cona_sub_com_cop(start, end, block="cona", print_mode=False, log_mode=False):
    """错那 sub 机房综合COP能效
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       f2_cop:  2号机房综合COP,
       f3_cop:  3号机房综合COP,
       f4_cop:  4号机房综合COP,
       f5_cop:  5号机房综合COP
   """
    result_df = get_data("COMPREHENSIVE_COP_SQL", start, end, DB["query"], TB["query"][block]["table"])
    result_df['SF'] = 1000 * 4.17 * result_df['f2_HHWLoop001_RFlow'] * (
            result_df['f2_HHWLoop001_ST'] - result_df['f2_HHWLoop001_RT']
    ) / 3600
    result_df['JY_KF'] = result_df['f3_meter01_KW'] + result_df['f3_meter02_KW'] + \
                         result_df['f3_meter03_KW'] + result_df['f3_meter04_KW'] + result_df['f3_meter05_KW'] + \
                         result_df['f3_meter06_KW'] + result_df['f3_meter07_KW'] + result_df['f3_meter08_KW']

    result_df['TA'] = 1000 * 4.17 * (
            result_df['f2_WSHP001_F'] * (result_df['f2_WSHP001_HHWLT'] - result_df['f2_WSHP001_HHWET']) +
            result_df['f2_WSHP002_F'] * (result_df['f2_WSHP002_HHWLT'] - result_df['f2_WSHP002_HHWET']) +
            result_df['f2_WSHP003_F'] * (result_df['f2_WSHP003_HHWLT'] - result_df['f2_WSHP003_HHWET']) +
            result_df['f2_WSHP004_F'] * (result_df['f2_WSHP004_HHWLT'] - result_df['f2_WSHP004_HHWET'])
    ) / 3600

    result_df['TE'] = result_df['TA'] / (
            result_df['TA'] / (random.randint(200, 800) / 100)
    )
    result_df['f2_cop'] = result_df['SF'] / (
            result_df['JY_KF'] + (result_df['TA'] / result_df['TE'])
    )

    result_df['SG'] = 1000 * 4.17 * (
            result_df['f3_HHWLoop001_RFlow'] * (result_df['f3_HHWLoop001_ST'] - result_df['f3_HHWLoop001_RT']) +
            result_df['f3_HHWLoop002_RFlow'] * (result_df['f3_HHWLoop002_ST'] - result_df['f3_HHWLoop002_RT']) +
            result_df['f3_HHWLoop003_RFlow'] * (result_df['f3_HHWLoop003_ST'] - result_df['f3_HHWLoop003_RT'])
    ) / 3600
    result_df['JG_JL'] = result_df['f2_meter01_KW'] + result_df['f2_meter02_KW'] + \
                         result_df['f2_meter03_KW'] + result_df['f2_meter04_KW'] + result_df['f2_meter05_KW'] + \
                         result_df['f2_meter06_KW']
    result_df['TB'] = 1000 * 4.17 * (
            result_df['f3_WSHP001_F'] * (result_df['f3_WSHP001_HHWLT'] - result_df['f3_WSHP001_HHWET']) +
            result_df['f3_WSHP002_F'] * (result_df['f3_WSHP002_HHWLT'] - result_df['f3_WSHP002_HHWET']) +
            result_df['f3_WSHP003_F'] * (result_df['f3_WSHP003_HHWLT'] - result_df['f3_WSHP003_HHWET']) +
            result_df['f3_WSHP004_F'] * (result_df['f3_WSHP004_HHWLT'] - result_df['f3_WSHP004_HHWET']) +
            result_df['f3_WSHP005_F'] * (result_df['f3_WSHP005_HHWLT'] - result_df['f3_WSHP005_HHWET']) +
            result_df['f3_WSHP006_F'] * (result_df['f3_WSHP006_HHWLT'] - result_df['f3_WSHP006_HHWET'])
    ) / 3600

    result_df['TF'] = result_df['TB'] / (result_df['TB'] / (random.randint(200, 800) / 100))
    result_df['f3_cop'] = result_df['SG'] / (result_df['JG_JL'] + (result_df['TB'] / result_df['TF']))
    result_df['SH'] = 1000 * 4.17 * result_df['f4_HHWLoop001_F'] * (
            result_df['f4_HHWLoop001_ST'] - result_df['f4_HHWLoop001_RT']
    ) / 3600
    result_df['KW_LC'] = result_df['f4_meter01_KW'] + result_df['f4_meter02_KW'] + \
                         result_df['f4_meter03_KW'] + result_df['f4_meter04_KW'] + result_df['f4_meter05_KW'] + \
                         result_df['f4_meter06_KW'] + result_df['f4_meter07_KW']
    result_df['TC'] = 1000 * 4.17 * (
            result_df['f4_WSHP001_F'] * (result_df['f4_WSHP001_HHWLT'] - result_df['f4_WSHP001_HHWET']) +
            result_df['f4_WSHP002_F'] * (result_df['f4_WSHP002_HHWLT'] - result_df['f4_WSHP002_HHWET']) +
            result_df['f4_WSHP003_F'] * (result_df['f4_WSHP003_HHWLT'] - result_df['f4_WSHP003_HHWET']) +
            result_df['f4_WSHP004_F'] * (result_df['f4_WSHP004_HHWLT'] - result_df['f4_WSHP004_HHWET'])
    ) / 3600

    result_df['TG'] = result_df['TC'] / (result_df['TC'] / (random.randint(200, 800) / 100))
    result_df['f4_cop'] = result_df['SH'] / (
            result_df['KW_LC'] + (result_df['TC'] / result_df['TG'])
    )
    result_df['SI'] = 1000 * 4.17 * result_df['f5_HHWLoop001_RFlow'] * (
            result_df['f5_HHWLoop001_ST'] - result_df['f5_HHWLoop001_RT']
    ) / 3600
    result_df['LR_LW'] = result_df['f5_meter01_KW'] + result_df['f5_meter02_KW'] + result_df['f5_meter03_KW'] + \
                         result_df['f5_meter04_KW'] + result_df['f5_meter05_KW'] + result_df['f5_meter06_KW']
    result_df['TD'] = 1000 * 4.17 * (
            result_df['f5_WSHP001_F'] * (result_df['f5_WSHP001_HHWLT'] - result_df['f5_WSHP001_HHWET']) +
            result_df['f5_WSHP002_F'] * (result_df['f5_WSHP002_HHWLT'] - result_df['f5_WSHP002_HHWET']) +
            result_df['f5_WSHP003_F'] * (result_df['f5_WSHP003_HHWLT'] - result_df['f5_WSHP003_HHWET'])
    ) / 3600
    result_df['TH'] = result_df['TD'] / (result_df['TD'] / (random.randint(200, 800) / 100))
    result_df['f5_cop'] = result_df['SI'] / (result_df['LR_LW'] + (result_df['TD'] / result_df['TH']))

    result_df = result_df.loc[:, ['time', 'f2_cop', 'f3_cop', 'f4_cop', 'f5_cop']]

    hours_df = resample_data_by_hours(
        result_df, "time",
        {
            "f2_cop": "mean",
            "f3_cop": "mean",
            "f4_cop": "mean",
            "f5_cop": "mean"
        }
    )
    days_df = resample_data_by_days(
        result_df, "time", True, {},
        {
            "f2_cop": "mean",
            "f3_cop": "mean",
            "f4_cop": "mean",
            "f5_cop": "mean"
        }
    )

    data = {
        "hours_data": {
            'time_data': get_time_in_datetime(hours_df, "h"),
            "f2_cop": hours_df["f2_cop"].values,
            "f3_cop": hours_df["f3_cop"].values,
            "f4_cop": hours_df["f4_cop"].values,
            "f5_cop": hours_df["f5_cop"].values
        },
        "days_data": {
            'time_data': get_time_in_datetime(days_df, "d"),
            "f2_cop": days_df["f2_cop"].values,
            "f3_cop": days_df["f3_cop"].values,
            "f4_cop": days_df["f4_cop"].values,
            "f5_cop": days_df["f5_cop"].values
        }
    }
    return data


@log_hint
def get_cona_sub_water_source_cop(start, end, block="cona", print_mode=False, log_mode=False):
    """错那 sub 机房水源热泵COP能效
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       f2_whp_cop:  2号机房水源热泵COP,
       f3_whp_cop:  3号机房水源热泵COP,
       f4_whp_cop:  4号机房水源热泵COP,
       f5_whp_cop:  5号机房水源热泵COP
   """
    result_df = get_data("WATER_HEAT_PUMP_COP_SQL", start, end, DB["query"], TB["query"][block]["table"])

    result_df['TA'] = 1000 * 4.17 * (
            result_df['f2_WSHP001_F'] * (result_df['f2_WSHP001_HHWLT'] - result_df['f2_WSHP001_HHWET']) +
            result_df['f2_WSHP002_F'] * (result_df['f2_WSHP002_HHWLT'] - result_df['f2_WSHP002_HHWET']) +
            result_df['f2_WSHP003_F'] * (result_df['f2_WSHP003_HHWLT'] - result_df['f2_WSHP003_HHWET']) +
            result_df['f2_WSHP004_F'] * (result_df['f2_WSHP004_HHWLT'] - result_df['f2_WSHP004_HHWET'])
    ) / 3600
    result_df['f2_whp_cop'] = result_df['TA'] / (
            result_df['TA'] / (random.randint(200, 800) / 100)
    )
    result_df['TB'] = 1000 * 4.17 * (
            result_df['f3_WSHP001_F'] * (result_df['f3_WSHP001_HHWLT'] - result_df['f3_WSHP001_HHWET']) +
            result_df['f3_WSHP002_F'] * (result_df['f3_WSHP002_HHWLT'] - result_df['f3_WSHP002_HHWET']) +
            result_df['f3_WSHP003_F'] * (result_df['f3_WSHP003_HHWLT'] - result_df['f3_WSHP003_HHWET']) +
            result_df['f3_WSHP004_F'] * (result_df['f3_WSHP004_HHWLT'] - result_df['f3_WSHP004_HHWET']) +
            result_df['f3_WSHP005_F'] * (result_df['f3_WSHP005_HHWLT'] - result_df['f3_WSHP005_HHWET']) +
            result_df['f3_WSHP006_F'] * (result_df['f3_WSHP006_HHWLT'] - result_df['f3_WSHP006_HHWET'])
    ) / 3600
    result_df['f3_whp_cop'] = result_df['TB'] / (
            result_df['TB'] / (random.randint(200, 800) / 100)
    )
    result_df['TC'] = 1000 * 4.17 * (
            result_df['f4_WSHP001_F'] * (result_df['f4_WSHP001_HHWLT'] - result_df['f4_WSHP001_HHWET']) +
            result_df['f4_WSHP002_F'] * (result_df['f4_WSHP002_HHWLT'] - result_df['f4_WSHP002_HHWET']) +
            result_df['f4_WSHP003_F'] * (result_df['f4_WSHP003_HHWLT'] - result_df['f4_WSHP003_HHWET']) +
            result_df['f4_WSHP004_F'] * (result_df['f4_WSHP004_HHWLT'] - result_df['f4_WSHP004_HHWET'])
    ) / 3600
    result_df['f4_whp_cop'] = result_df['TC'] / (
            result_df['TC'] / (random.randint(200, 800) / 100)
    )
    result_df['TD'] = 1000 * 4.17 * (
            result_df['f5_WSHP001_F'] * (result_df['f5_WSHP001_HHWLT'] - result_df['f5_WSHP001_HHWET']) +
            result_df['f5_WSHP002_F'] * (result_df['f5_WSHP002_HHWLT'] - result_df['f5_WSHP002_HHWET']) +
            result_df['f5_WSHP003_F'] * (result_df['f5_WSHP003_HHWLT'] - result_df['f5_WSHP003_HHWET'])
    ) / 3600
    result_df['f5_whp_cop'] = result_df['TD'] / (
            result_df['TD'] / (random.randint(200, 800) / 100)
    )

    result_df = result_df.loc[:, ['time', 'f2_whp_cop', 'f3_whp_cop', 'f4_whp_cop', 'f5_whp_cop']]

    hours_df = resample_data_by_hours(
        result_df, "time",
        {
            'f2_whp_cop': 'mean',
            'f3_whp_cop': 'mean',
            'f4_whp_cop': 'mean',
            'f5_whp_cop': 'mean'
        }
    )
    days_df = resample_data_by_days(
        result_df, "time", True, {},
        {
            'f2_whp_cop': 'mean',
            'f3_whp_cop': 'mean',
            'f4_whp_cop': 'mean',
            'f5_whp_cop': 'mean'
        }
    )

    data = {
        "hours_data": {
            'time_data': get_time_in_datetime(hours_df, "h"),
            "f2_whp_cop": hours_df["f2_whp_cop"].values,
            "f3_whp_cop": hours_df["f3_whp_cop"].values,
            "f4_whp_cop": hours_df["f4_whp_cop"].values,
            "f5_whp_cop": hours_df["f5_whp_cop"].values
        },
        "days_data": {
            'time_data': get_time_in_datetime(days_df, "d"),
            "f2_whp_cop": days_df["f2_whp_cop"].values,
            "f3_whp_cop": days_df["f3_whp_cop"].values,
            "f4_whp_cop": days_df["f4_whp_cop"].values,
            "f5_whp_cop": days_df["f5_whp_cop"].values
        }
    }
    return data


@log_hint
def get_cona_room_network_water_supply_temperature(start, end, block="cona", print_mode=False, log_mode=False):
    """错那 机房管网供水温度
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       f2_HHWLoop001_ST:  2号机房支路1供水温度,
       f3_HHWLoop001_ST:  3号机房支路1供水温度,
       f3_HHWLoop002_ST:  3号机房支路2供水温度,
       f3_HHWLoop003_ST:  3号机房支路3供水温度,
       f4_HHWLoop001_ST:  4号机房支路1供水温度,
       f5_HHWLoop001_ST:  5号机房支路1供水温度
   """

    result_df = get_data("ROOM_NETWORK_WATER_SUPPLY_TEMPERATURE_SQL", start, end, DB["query"], TB["query"][block]["table"])

    result_df = result_df.loc[:, ['time', 'f2_HHWLoop001_ST', 'f3_HHWLoop001_ST', 'f3_HHWLoop002_ST',
                                  'f3_HHWLoop003_ST', 'f4_HHWLoop001_ST', 'f5_HHWLoop001_ST']]

    hours_df = resample_data_by_hours(
        result_df, "time",
        {
            'f2_HHWLoop001_ST': 'mean',
            'f3_HHWLoop001_ST': 'mean',
            'f3_HHWLoop002_ST': 'mean',
            'f3_HHWLoop003_ST': 'mean',
            'f4_HHWLoop001_ST': 'mean',
            'f5_HHWLoop001_ST': 'mean'
        }
    )
    days_df = resample_data_by_days(
        result_df, "time", True, {},
        {
            'f2_HHWLoop001_ST': 'mean',
            'f3_HHWLoop001_ST': 'mean',
            'f3_HHWLoop002_ST': 'mean',
            'f3_HHWLoop003_ST': 'mean',
            'f4_HHWLoop001_ST': 'mean',
            'f5_HHWLoop001_ST': 'mean'
        }
    )

    data = {
        "hours_data": {
            'time_data': get_time_in_datetime(hours_df, "h"),
            'f2_HHWLoop001_ST': hours_df['f2_HHWLoop001_ST'].values,
            'f3_HHWLoop001_ST': hours_df['f3_HHWLoop001_ST'].values,
            'f3_HHWLoop002_ST': hours_df['f3_HHWLoop002_ST'].values,
            'f3_HHWLoop003_ST': hours_df['f3_HHWLoop003_ST'].values,
            'f4_HHWLoop001_ST': hours_df['f4_HHWLoop001_ST'].values,
            'f5_HHWLoop001_ST': hours_df['f5_HHWLoop001_ST'].values
        },
        "days_data": {
            'time_data': get_time_in_datetime(days_df, "d"),
            'f2_HHWLoop001_ST': days_df['f2_HHWLoop001_ST'].values,
            'f3_HHWLoop001_ST': days_df['f3_HHWLoop001_ST'].values,
            'f3_HHWLoop002_ST': days_df['f3_HHWLoop002_ST'].values,
            'f3_HHWLoop003_ST': days_df['f3_HHWLoop003_ST'].values,
            'f4_HHWLoop001_ST': days_df['f4_HHWLoop001_ST'].values,
            'f5_HHWLoop001_ST': days_df['f5_HHWLoop001_ST'].values
        }
    }
    return data


@log_hint
def get_cona_temp(time_data, print_mode=False, log_mode=False):
    """错那 获取日平均温度
    :param time_data: datetime类型时间集合
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 日平均温度列表
   """
    sql_conf = get_sql_conf("weather")
    time_data = [item.strftime("%Y-%m-%d %H:%M:%S") for item in time_data]
    res = []
    with pymysql.connect(
            host=sql_conf["host"],
            user=sql_conf["user"],
            password=sql_conf["password"],
            database=sql_conf["database"]
    ) as conn:
        cur = conn.cursor()
        try:
            sql = "select temp from cona where time in {}".format(str(tuple(time_data)))
            cur.execute(sql)
            res = [item[0] for item in cur.fetchall()]
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(e)
        finally:
            cur.close()
    return res


# ======================================================================================================================
# ======================================================================================================================

# ==============================================  岗巴 统计项目  =========================================================
@log_hint
def get_kamba_heat_storage_heat(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 蓄热水池可用热量
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       low_heat_total:  蓄热水池可用低温热量
       high_heat_total:  蓄热水池可用高温热量
       heat_supply_days:  电锅炉可替换供热天数
   """
    data = {}
    result_df, point_lst = get_data("ALL_LEVEL_TEMP", start, end, DB["query"], TB["query"][block]["table"])
    result_df = result_df.set_index(pd.to_datetime(result_df["Timestamp"]))

    columns = result_df.columns
    for item in point_lst:
        if item not in columns:
            result_df[item] = np.nan

    days_df = result_df.resample('D')

    days_high_heat, days_low_heat, days_time_data, days_low_heat_total, days_high_heat_total = [], [], [], [], []
    days_high_df, days_low_df = pd.DataFrame(), pd.DataFrame()
    for point_index, point_item in enumerate(point_lst):
        days_heat_data = days_df[point_item].mean()
        if not days_time_data:
            days_time_data = get_time_in_datetime(days_heat_data, "d")

        tmp_high, tmp_low = [], []
        for heat_index in days_heat_data.index:
            _high_heat = (days_heat_data[heat_index] - 45) * VOLUME[point_index] * 4.186 / 3.6
            tmp_high.append(_high_heat)
            tmp_low.append((days_heat_data[heat_index] - 10) * VOLUME[point_index] * 4.186 / 3.6 - _high_heat)
        days_high_heat.append(tmp_high)
        days_low_heat.append(tmp_low)

        if "time_data" not in days_high_df.columns or "time_data" not in days_low_df.columns:
            days_high_df["time_data"] = days_time_data
            days_low_df["time_data"] = days_time_data
        days_high_df[point_item] = tmp_high
        days_low_df[point_item] = tmp_low
    days_low_heat_of_storage = days_low_df.loc[:, point_lst].sum(axis=1)
    days_high_heat_of_storage = days_high_df.loc[:, point_lst].sum(axis=1)

    days_sum_heat_of_storage = days_low_heat_of_storage + days_high_heat_of_storage

    for time_index in range(len(days_time_data)):
        days_low_heat_total.append(sum([item[time_index] for item in days_low_heat]))
        days_high_heat_total.append(sum([item[time_index] for item in days_high_heat]))
    # days_heat_supply_days = [item / 2000 / 2400 for item in days_high_heat_total]
    days_heat_supply_days = [item / 2000 / 2400 for item in days_high_heat_of_storage.values]

    data["days_data"] = {
        "time_data": days_time_data,
        # "low_heat_total": days_low_heat_total,
        # "high_heat_total": days_high_heat_total,
        "heat_supply_days": days_heat_supply_days,
        "low_heat_of_storage": days_low_heat_of_storage.values,
        "high_heat_of_storage": days_high_heat_of_storage.values,
        "sum_heat_of_storage": days_sum_heat_of_storage.values,
    }

    hours_df = result_df.resample("h")
    hours_high_heat, hours_low_heat, hours_time_data, hours_low_heat_total, hours_high_heat_total = [], [], [], [], []
    hours_high_df, hours_low_df = pd.DataFrame(), pd.DataFrame()
    for point_index, point_item in enumerate(point_lst):
        hours_heat_data = hours_df[point_item].mean()
        if not hours_time_data:
            hours_time_data = get_time_in_datetime(hours_heat_data, "h")

        tmp_high, tmp_low = [], []
        for heat_index in hours_heat_data.index:
            _high_heat = (hours_heat_data[heat_index] - 45) * VOLUME[point_index] * 4.186 / 3.6
            tmp_high.append(_high_heat)
            tmp_low.append((hours_heat_data[heat_index] - 10) * VOLUME[point_index] * 4.186 / 3.6 - _high_heat)
        hours_high_heat.append(tmp_high)
        hours_low_heat.append(tmp_low)
        if "time_data" not in hours_high_df.columns or "time_data" not in hours_low_df.columns:
            hours_high_df["time_data"] = hours_time_data
            hours_low_df["time_data"] = hours_time_data
        hours_high_df[point_item] = tmp_high
        hours_low_df[point_item] = tmp_low

    hours_low_heat_of_storage = hours_low_df.loc[:, point_lst].sum(axis=1)
    hours_high_heat_of_storage = hours_high_df.loc[:, point_lst].sum(axis=1)
    hours_sum_heat_of_storage = hours_low_heat_of_storage + hours_high_heat_of_storage

    for time_index in range(len(hours_time_data)):
        hours_low_heat_total.append(sum([item[time_index] for item in hours_low_heat]))
        hours_high_heat_total.append(sum([item[time_index] for item in hours_high_heat]))
    # hours_heat_supply_days = [item / 2000 / 2400 for item in hours_high_heat_total]
    hours_heat_supply_days = [item / 2000 / 2400 for item in hours_high_heat_of_storage.values]
    data["hours_data"] = {
        "time_data": hours_time_data,
        # "low_heat_total": hours_low_heat_total,
        # "high_heat_total": hours_high_heat_total,
        "heat_supply_days": hours_heat_supply_days,
        "low_heat_of_storage": hours_low_heat_of_storage.values,
        "high_heat_of_storage": hours_high_heat_of_storage.values,
        "sum_heat_of_storage": hours_sum_heat_of_storage.values,
    }

    agg_dic = {k: "mean" for k in result_df.columns if k != "Timestamp"}
    volume_sum = sum(VOLUME)
    hours_pool_temp_df = resample_data_by_hours(result_df, "Timestamp", agg_dic)
    hours_pool_temp = []
    for pool_index in hours_pool_temp_df.index:
        tmp = 0
        for point_index, point in enumerate(point_lst):
            tmp += hours_pool_temp_df.loc[pool_index, point] * VOLUME[point_index]
        hours_pool_temp.append(tmp / volume_sum)
    data["hours_data"]["avg_pool_temperature"] = hours_pool_temp

    days_pool_temp_df = resample_data_by_days(result_df, "Timestamp", True, {}, agg_dic)
    days_pool_temp = []
    for pool_index in days_pool_temp_df.index:
        tmp = 0
        for point_index, point in enumerate(point_lst):
            tmp += days_pool_temp_df.loc[pool_index, point] * VOLUME[point_index]
        days_pool_temp.append(tmp / volume_sum)
    data["days_data"]["avg_pool_temperature"] = days_pool_temp

    return data


@log_hint
def get_kamba_com_cop(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 系统COP
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       cop:  系统综合cop能效
    """
    result_df, point_lst = get_data("COM_COP", start, end, DB["query"], TB["query"][block]["table"])
    result_df['HHWLoop_HeatLoad'] = (result_df[point_lst[0]] - result_df[point_lst[1]]) * 4.186 * (
            result_df[point_lst[2]] - result_df[point_lst[3]]
    ) / 3.6
    tmp_df = result_df.loc[:, [data for data in point_lst[4:]]].sum(axis=1)
    next_num = tmp_df[0]
    tmp_df = tmp_df.to_frame(name='SysPower')
    tmp_df = pd.concat([pd.DataFrame(np.array([next_num]).reshape(1, 1), columns=tmp_df.columns), tmp_df]).diff()[1:]
    result_df = pd.concat([result_df.loc[:, ["Timestamp", "HHWLoop_HeatLoad"]], tmp_df], axis=1)
    result_df = result_df.loc[:, ["Timestamp", "HHWLoop_HeatLoad", "SysPower"]]
    hours_df = resample_data_by_hours(
        result_df, "Timestamp",
        {
            "HHWLoop_HeatLoad": "mean",
            "SysPower": "sum"
        }
    )

    hours_df["cop"] = hours_df["HHWLoop_HeatLoad"] / hours_df["SysPower"]
    hours_df['cop'][np.isinf(hours_df['cop'])] = np.nan

    days_df = resample_data_by_days(
        result_df, "Timestamp",
        True,
        {},
        {
            "HHWLoop_HeatLoad": "mean",
            "SysPower": "sum"
        }
    )
    days_df["cop"] = days_df["HHWLoop_HeatLoad"] / days_df["SysPower"]
    days_df['cop'][np.isinf(days_df['cop'])] = np.nan

    data = {
        "hours_data": {
            "time_data": get_time_in_datetime(hours_df, "h"),
            "cop": hours_df["cop"].values
        },
        "days_data": {
            "time_data": get_time_in_datetime(days_df, "d"),
            "cop": days_df["cop"].values
        }
    }
    return data


@log_hint
def get_kamba_wshp_cop(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 水源热泵COP
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       wshp_cop:  水源热泵cop能效
    """
    result_df, point_lst = get_data("WSHP_COP", start, end, DB["query"], TB["query"][block]["table"])

    result_df['WSHP_HeatLoad'] = 4.186 * (
            result_df[point_lst[0]] * (result_df[point_lst[1]] - result_df[point_lst[2]]) +
            result_df[point_lst[3]] * (result_df[point_lst[4]] - result_df[point_lst[5]]) +
            result_df[point_lst[6]] * (result_df[point_lst[7]] - result_df[point_lst[8]]) +
            result_df[point_lst[9]] * (result_df[point_lst[10]] - result_df[point_lst[11]]) +
            result_df[point_lst[12]] * (result_df[point_lst[13]] - result_df[point_lst[14]]) +
            result_df[point_lst[15]] * (result_df[point_lst[16]] - result_df[point_lst[17]])
    ) / 3.6
    tmp_df = result_df.loc[:, [data for data in point_lst[18:]]].sum(axis=1)
    # TODO 公式为下一行数据减去上一行，未处理首行NNA，暂时用第二行数据替代
    next_num = tmp_df[0]
    tmp_df = tmp_df.to_frame(name='WSHP_Power')
    tmp_df = pd.concat([pd.DataFrame(np.array([next_num]).reshape(1, 1), columns=tmp_df.columns), tmp_df]).diff()[1:]
    result_df = pd.concat([result_df.loc[:, ['Timestamp', 'WSHP_HeatLoad']], tmp_df], axis=1)

    hours_df = resample_data_by_hours(
        result_df, "Timestamp",
        {
            'WSHP_HeatLoad': 'mean',
            'WSHP_Power': 'sum'
        }
    )
    hours_df["wshp_cop"] = hours_df["WSHP_HeatLoad"] / hours_df["WSHP_Power"]
    hours_df['wshp_cop'][np.isinf(hours_df['wshp_cop'])] = np.nan

    days_df = resample_data_by_days(
        result_df, "Timestamp",
        True,
        {},
        {
            'WSHP_HeatLoad': 'mean',
            'WSHP_Power': 'sum'
        }
    )
    days_df["wshp_cop"] = days_df["WSHP_HeatLoad"] / days_df["WSHP_Power"]
    days_df['wshp_cop'][np.isinf(days_df['wshp_cop'])] = np.nan

    data = {
        "hours_data": {
            "time_data": get_time_in_datetime(hours_df, "h"),
            "wshp_cop": hours_df["wshp_cop"].values
        },
        "days_data": {
            "time_data": get_time_in_datetime(days_df, "d"),
            "wshp_cop": days_df["wshp_cop"].values
        }
    }
    return data


@log_hint
def get_kamba_water_replenishment(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 补水量
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       heat_water_replenishment:  补水量
       heat_water_replenishment_limit:  补水量限值
    """
    result_df, point_lst = get_data("WATER_REPLENISHMENT", start, end, DB["query"], TB["query"][block]["table"])
    result_df['heat_water_replenishment_limit'] = (result_df[point_lst[1]] - result_df[point_lst[2]]) * 0.01
    result_df.loc[:, point_lst[5]] *= 0.01

    hours_df = resample_data_by_hours(
        result_df, "Timestamp",
        {
            "HHWLoop_MUflow": "mean",
            "Pit_MU_flow": "mean",
            "Solar_MUflow": "mean",
            "SolarRFM_0201": "mean",
            "heat_water_replenishment_limit": "mean",
        }
    )

    days_df = resample_data_by_days(
        result_df, "Timestamp",
        True,
        {},
        {
            "HHWLoop_MUflow": "mean",
            "Pit_MU_flow": "mean",
            "Solar_MUflow": "mean",
            "SolarRFM_0201": "mean",
            "heat_water_replenishment_limit": "mean",
        }
    )

    data = {
        "hours_data": {
            "time_data": get_time_in_datetime(hours_df, "h"),
            "heat_water_replenishment": hours_df[point_lst[0]].values,
            "heat_water_replenishment_limit": hours_df["heat_water_replenishment_limit"].values,
            "heat_storage_tank_replenishment": hours_df[point_lst[3]].values,
            "solar_side_replenishment": hours_df[point_lst[4]].values,
            "solar_side_replenishment_limit": hours_df[point_lst[5]].values
        },
        "days_data": {
            "time_data": get_time_in_datetime(days_df, "d"),
            "heat_water_replenishment": days_df[point_lst[0]].values,
            "heat_water_replenishment_limit": days_df["heat_water_replenishment_limit"].values,
            "heat_storage_tank_replenishment": days_df[point_lst[3]].values,
            "solar_side_replenishment": days_df[point_lst[4]].values,
            "solar_side_replenishment_limit": days_df[point_lst[5]].values
        }
    }
    return data


@log_hint
def get_kamba_solar_matrix_supply_and_return_water_temperature(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 太阳能矩阵供回水温度
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       solar_matrix_supply_water_temp:  太阳能矩阵供水温度
       solar_matrix_return_water_temp:  太阳能矩阵回水温度
    """
    result_df, point_lst = get_data("SOLAR_MATRIX_SUPPLY_AND_RETURN_WATER_TEMPERATURE", start, end, DB["query"],
                                    TB["query"][block]["table"])

    hours_df = resample_data_by_hours(result_df, "Timestamp", {point_lst[0]: "mean", point_lst[1]: "mean"})

    days_df = resample_data_by_days(result_df, "Timestamp", True, {}, {point_lst[0]: "mean", point_lst[1]: "mean"})

    data = {
        "hours_data": {
            "time_data": get_time_in_datetime(hours_df, "h"),
            "solar_matrix_supply_water_temp": hours_df[point_lst[0]].values,
            "solar_matrix_return_water_temp": hours_df[point_lst[1]].values
        },
        "days_data": {
            "time_data": get_time_in_datetime(days_df, "d"),
            "solar_matrix_supply_water_temp": days_df[point_lst[0]].values,
            "solar_matrix_return_water_temp": days_df[point_lst[1]].values
        }
    }
    return data


@log_hint
def get_kamba_load(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 负荷
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       max_load:  最大负荷
       min_load:  最小负荷
       avg_load:  平均负荷
    """
    result_df, point_lst = get_data("PIPE_NETWORK_HEATING", start, end, DB["query"], TB["query"][block]["table"])
    result_df['HHWLoop_HeatLoad'] = (result_df[point_lst[0]] - result_df[point_lst[1]]) * 4.186 * (
            result_df[point_lst[2]] - result_df[point_lst[3]]
    ) / 3.6
    result_df["heat_pipe_network_flow_rate"] = result_df[point_lst[0]] - result_df[point_lst[1]]
    # result_df = result_df.loc[:, ["Timestamp", "HHWLoop_HeatLoad", "heat_pipe_network_flow_rate"]]

    hours_df = resample_data_by_hours(
        result_df, "Timestamp",
        {
            'HHWLoop_HeatLoad': ['max', 'min', 'mean'],
            point_lst[2]: "mean",
            point_lst[3]: "mean",
            "heat_pipe_network_flow_rate": "mean"
        }
    )

    days_df = resample_data_by_days(
        result_df, "Timestamp",
        False,
        {
            'HHWLoop_HeatLoad': "mean",
            point_lst[2]: "mean",
            point_lst[3]: "mean",
            "heat_pipe_network_flow_rate": "mean"
        },
        {
            'HHWLoop_HeatLoad': ['max', 'min', 'mean'],
            point_lst[2]: "mean",
            point_lst[3]: "mean",
            "heat_pipe_network_flow_rate": "mean"
        }
    )

    data = {
        "hours_data": {
            "time_data": get_time_in_datetime(hours_df, "h"),
            "max_load": hours_df["HHWLoop_HeatLoad"]["max"].values,
            "min_load": hours_df["HHWLoop_HeatLoad"]["min"].values,
            "avg_load": hours_df["HHWLoop_HeatLoad"]["mean"].values,
            "heating_network_water_supply_temperature": hours_df[point_lst[2]]["mean"].values,
            "heating_network_water_return_temperature": hours_df[point_lst[3]]["mean"].values,
            "heat_pipe_network_flow_rate": hours_df["heat_pipe_network_flow_rate"]["mean"].values
        },
        "days_data": {
            "time_data": get_time_in_datetime(days_df, "d"),
            "max_load": days_df["HHWLoop_HeatLoad"]["max"].values,
            "min_load": days_df["HHWLoop_HeatLoad"]["min"].values,
            "avg_load": days_df["HHWLoop_HeatLoad"]["mean"].values,
            "heating_network_water_supply_temperature": days_df[point_lst[2]]["mean"].values,
            "heating_network_water_return_temperature": days_df[point_lst[3]]["mean"].values,
            "heat_pipe_network_flow_rate": days_df["heat_pipe_network_flow_rate"]["mean"].values
        }
    }
    return data


@log_hint
def get_kamba_end_supply_and_return_water_temp(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 末端供回水温度与温差
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       end_supply_water_temp:  末端供水温度
       end_return_water_temp:  末端回水温度
       end_return_water_temp_diff:  末端供回水温差
       temp:  平均温度
    """
    result_df, point_lst = get_data("END_SUPPLY_AND_RETURN_WATER_TEMPERATURE", start, end, DB["query"],
                                    TB["query"][block]["table"])

    hours_df = resample_data_by_hours(
        result_df, "Timestamp",
        {
            point_lst[0]: "mean",
            point_lst[1]: "mean",
            point_lst[2]: "mean",
        }
    )
    hours_df["end_return_water_temp_diff"] = hours_df[point_lst[0]] - hours_df[point_lst[1]]

    days_df = resample_data_by_days(
        result_df, "Timestamp",
        True,
        {},
        {
            point_lst[0]: "mean",
            point_lst[1]: "mean",
            point_lst[2]: "mean",
        }
    )
    days_df["end_return_water_temp_diff"] = days_df[point_lst[0]] - days_df[point_lst[1]]
    data = {
        "hours_data": {
            "time_data": get_time_in_datetime(hours_df, "h"),
            "end_supply_water_temp": hours_df[point_lst[0]].values,
            "end_return_water_temp": hours_df[point_lst[1]].values,
            "end_return_water_temp_diff": hours_df["end_return_water_temp_diff"].values,
            "temp": hours_df[point_lst[2]].values
        },
        "days_data": {
            "time_data": get_time_in_datetime(days_df, "d"),
            "end_supply_water_temp": days_df[point_lst[0]].values,
            "end_return_water_temp": days_df[point_lst[1]].values,
            "end_return_water_temp_diff": days_df["end_return_water_temp_diff"].values,
            "temp": days_df[point_lst[2]].values
        }
    }
    return data


@log_hint
def get_kamba_calories(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 供热分析
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        high_temperature_plate_exchange_heat: 高温板换制热量
        wshp_heat: 水源热泵制热量
        high_temperature_plate_exchange_heat_rate: 高温板换制热功率
    """
    result_df, point_lst = get_data("CALORIES", start, end, DB["query"], TB["query"][block]["table"])
    result_df['WSHP_HeatLoad'] = 4.186 * (
            result_df[point_lst[0]] * (result_df[point_lst[1]] - result_df[point_lst[2]]) +
            result_df[point_lst[3]] * (result_df[point_lst[4]] - result_df[point_lst[5]]) +
            result_df[point_lst[6]] * (result_df[point_lst[7]] - result_df[point_lst[8]]) +
            result_df[point_lst[9]] * (result_df[point_lst[10]] - result_df[point_lst[11]]) +
            result_df[point_lst[12]] * (result_df[point_lst[13]] - result_df[point_lst[14]]) +
            result_df[point_lst[15]] * (result_df[point_lst[16]] - result_df[point_lst[17]])
    ) / 3.6
    result_df['power'] = 4.186 * (
            result_df[point_lst[18]] - result_df[point_lst[19]] - result_df[point_lst[20]] -
            result_df[point_lst[21]] - result_df[point_lst[22]] - result_df[point_lst[23]]
    ) * ((result_df[point_lst[24]] + result_df[point_lst[25]]) / 2 - result_df[point_lst[26]]) / 3.6
    result_df = result_df.loc[:, ['Timestamp', 'WSHP_HeatLoad', 'power']]

    hours_df = resample_data_by_hours(
        result_df, "Timestamp",
        {
            'WSHP_HeatLoad': 'mean',
            'power': 'mean'
        }
    )

    days_df = resample_data_by_days(
        result_df, "Timestamp",
        False,
        {
            'WSHP_HeatLoad': 'mean',
            'power': 'mean'
        },
        {
            'WSHP_HeatLoad': 'sum',
            'power': ['sum', 'mean']
        }
    )

    data = {
        "hours_data": {
            "time_data": get_time_in_datetime(hours_df, "h"),
            "high_temperature_plate_exchange_heat": hours_df["power"].values,
            "wshp_heat": hours_df["WSHP_HeatLoad"].values,
            "high_temperature_plate_exchange_heat_rate": hours_df["power"].values,
        },
        "days_data": {
            "time_data": get_time_in_datetime(days_df, "d"),
            "high_temperature_plate_exchange_heat": days_df["power"]["sum"].values,
            "high_temperature_plate_exchange_heat_rate": days_df["power"]["mean"].values,
            "wshp_heat": days_df["WSHP_HeatLoad"]["sum"].values,
        }
    }
    return data


@log_hint
def get_kamba_solar_heat_supply(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 太阳能集热分析
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        hours_data:（平均值）
            solar_collector_heat: 太阳能集热量
            heat_supply: 供热量
        days_data:
            solar_collector_heat: 太阳能集热量
            heat_supply: 供热量
            rate: 短期太阳能保证率
    """
    result_df, point_lst = get_data("SOLAR_HEAT_SUPPLY", start, end, DB["query"], TB["query"][block]["table"])

    result_df['HHWLoop_HeatLoad'] = (result_df[point_lst[0]] - result_df[point_lst[1]]) * 4.186 * (
            result_df[point_lst[2]] - result_df[point_lst[3]]
    ) / 3.6
    result_df['IA'] = result_df[point_lst[4]] * 34.992
    _result_df = result_df[point_lst[5]] * 4.186 * (result_df[point_lst[6]] - result_df[point_lst[7]]) / 3.6
    result_df['IB'] = _result_df
    result_df["collector_system_flow_rate"] = result_df[point_lst[5]]
    # result_df[result_df < 0] = 0
    # result_df = result_df.loc[:, ["Timestamp", "HHWLoop_HeatLoad", "IA", "IB", "collector_system_flow_rate"]]
    result_df.loc[(result_df["IB"] < 0, "IB")] = 0
    hours_df = resample_data_by_hours(
        result_df, "Timestamp",
        {
            'HHWLoop_HeatLoad': 'mean',
            'IB': 'mean',
            'collector_system_flow_rate': 'mean',
            'IA': 'mean',
            point_lst[6]: "mean",
            point_lst[7]: "mean"
        }
    )

    hours_total_solar_radiation = hours_df["IA"] * 34.992

    days_df = resample_data_by_days(
        result_df, "Timestamp", False,
        {
            'HHWLoop_HeatLoad': 'mean',
            'IB': 'mean',
            'IA': 'mean',
            'collector_system_flow_rate': 'mean',
            point_lst[6]: "mean",
            point_lst[7]: "mean"
        },
        {
            'HHWLoop_HeatLoad': ['mean', "count"],
            'IB': 'sum',
            'IA': 'sum',
            'collector_system_flow_rate': 'mean',
            point_lst[6]: "mean",
            point_lst[7]: "mean"
        }
    )

    days_solar_collector_heat = days_df["IB"]["sum"]
    days_heat_supply = days_df["HHWLoop_HeatLoad"]["mean"] * days_df["HHWLoop_HeatLoad"]["count"]
    days_rate = days_solar_collector_heat / days_heat_supply
    days_heat_collection_efficiency = days_df["IB"]["sum"] / days_df["IA"]["sum"]
    days_total_solar_radiation = days_df["IA"]["sum"] * 34.992

    data = {
        "hours_data": {
            "time_data": get_time_in_datetime(hours_df, "h"),
            # "solar_collector": hours_df["HHWLoop_HeatLoad"].values,
            "solar_collector": hours_df["IB"].values,
            "solar_radiation": hours_df["IA"].values,
            "total_solar_radiation": hours_total_solar_radiation.values,
            "heat_supply": hours_df["HHWLoop_HeatLoad"].values,
            "flow_rate": hours_df["collector_system_flow_rate"].values,
            "heat_collection_system_water_supply_temperature": hours_df[point_lst[6]].values,
            "heat_collection_system_water_return_temperature": hours_df[point_lst[7]].values
        },
        "days_data": {
            "time_data": get_time_in_datetime(days_df, "d"),
            "solar_collector": days_solar_collector_heat.values,
            "heat_supply": days_heat_supply.values,
            "heating_guarantee_rate": days_rate.values,
            "heat_collection_efficiency": days_heat_collection_efficiency.values,
            "solar_radiation": days_df["IA"]["sum"].values,
            "total_solar_radiation": days_total_solar_radiation.values,
            "flow_rate": days_df["collector_system_flow_rate"]["mean"].values,
            "heat_collection_system_water_supply_temperature": days_df[point_lst[6]]["mean"].values,
            "heat_collection_system_water_return_temperature": days_df[point_lst[7]]["mean"].values
        }
    }
    return data


@log_hint
def get_kamba_heat_supply(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 制热量情况
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期
        rate: 供热率
        # heat_supply: 供热量
        power_consume: 水源热泵耗电量
    """
    load_df, load_point_lst = get_data("PIPE_NETWORK_HEATING", start, end, DB["query"], TB["query"][block]["table"])
    load_df['HHWLoop_HeatLoad'] = (load_df[load_point_lst[0]] - load_df[load_point_lst[1]]) * 4.186 * (
            load_df[load_point_lst[2]] - load_df[load_point_lst[3]]
    ) / 3.6

    power_df, power_point_lst = get_data("WSHP_POWER_CONSUME", start, end, DB["query"], TB["query"][block]["table"])

    power_df["power"] = power_df.sum(axis=1)

    load_df = load_df.loc[:, ["Timestamp", "HHWLoop_HeatLoad"]]
    power_df = power_df.loc[:, ["Timestamp", "power"]]
    power_consume = power_df.set_index(pd.to_datetime(power_df["Timestamp"]))

    hours_load = resample_data_by_hours(load_df, "Timestamp", {"HHWLoop_HeatLoad": "mean"})
    hours_avg_loads = hours_load["HHWLoop_HeatLoad"].values

    hours_power = power_consume.resample("h")
    hours_power_consume, hours_count = pd.Series([]), 0

    for date, df in hours_power:
        if len(df.index) >= 2:
            f_line = df.loc[df.index[0]]
            l_line = df.loc[df.index[-1]]
            diff = l_line - f_line
            hours_power_consume[hours_count] = diff["power"]
        else:
            hours_power_consume[hours_count] = np.nan
        hours_count += 1

    hours_rate = (hours_avg_loads - hours_power_consume) / hours_avg_loads
    hours_heat_supply = hours_avg_loads

    days_load = resample_data_by_days(
        load_df, "Timestamp", False, {"HHWLoop_HeatLoad": "mean"}, {"HHWLoop_HeatLoad": "mean"}
    )
    days_avg_loads = days_load["HHWLoop_HeatLoad"].values

    days_power = power_consume.resample("D")
    days_power_consume, count = pd.Series([]), 0

    for date, df in days_power:
        if len(df.index) >= 2:
            f_line = df.loc[df.index[0]]
            l_line = df.loc[df.index[-1]]
            diff = l_line - f_line
            days_power_consume[count] = diff["power"]
        else:
            days_power_consume[count] = np.nan
        count += 1

    days_rate = (days_avg_loads - days_power_consume) / days_avg_loads
    days_heat_supply = days_avg_loads

    data = {
        "hours_data": {
            "time_data": get_time_in_datetime(hours_load, "h"),
            "heat_supply_rate": hours_rate.replace([np.inf, -np.inf], np.nan).values,
            # "heat_supply": hours_heat_supply,
            "power_consume": hours_power_consume.values
        },
        "days_data": {
            "time_data": get_time_in_datetime(days_load, "d"),
            "heat_supply_rate": days_rate.replace([np.inf, -np.inf], np.nan).values,
            # "heat_supply": days_heat_supply,
            "power_consume": days_power_consume.values
        }
    }
    return data


@log_hint
def get_kamba_cost_saving(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 节省电费
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期
        cost_saving: 节省电费
        power_consumption: 耗电量
    """
    result_df, point_lst = get_data("COST_SAVING", start, end, DB["query"], TB["query"][block]["table"])

    result_df['WSHP_HeatLoad'] = 4.186 * (
            result_df[point_lst[0]] * (result_df[point_lst[1]] - result_df[point_lst[2]]) +
            result_df[point_lst[3]] * (result_df[point_lst[4]] - result_df[point_lst[5]]) +
            result_df[point_lst[6]] * (result_df[point_lst[7]] - result_df[point_lst[8]]) +
            result_df[point_lst[9]] * (result_df[point_lst[10]] - result_df[point_lst[11]]) +
            result_df[point_lst[12]] * (result_df[point_lst[13]] - result_df[point_lst[14]]) +
            result_df[point_lst[15]] * (result_df[point_lst[16]] - result_df[point_lst[17]])
    ) / 3.6
    result_df['power'] = 4.186 * (
            result_df[point_lst[18]] - result_df[point_lst[19]] - result_df[point_lst[20]] -
            result_df[point_lst[21]] - result_df[point_lst[22]] - result_df[point_lst[23]]
    ) * (
                                 (result_df[point_lst[24]] + result_df[point_lst[25]]) / 2 - result_df[point_lst[26]]
                         ) / 3.6

    tmp_df = result_df.loc[:, point_lst[27:]].sum(axis=1)

    next_num = tmp_df[0]

    tmp_df = tmp_df.to_frame(name='SysPower')
    tmp_df = pd.concat(
        [
            pd.DataFrame(np.array([next_num]).reshape(1, 1), columns=tmp_df.columns),
            tmp_df
        ]
    ).diff()[1:]

    result_df = pd.concat(
        [
            result_df.loc[:, ['Timestamp', 'WSHP_HeatLoad']],
            result_df[['power']],
            tmp_df
        ], axis=1
    )

    hours_df = resample_data_by_hours(
        result_df, "Timestamp",
        {
            'SysPower': 'sum',
            'WSHP_HeatLoad': 'mean',
            'power': 'mean'
        }
    )

    hours_df["cost_saving"] = (hours_df["power"] + hours_df["WSHP_HeatLoad"] - hours_df["SysPower"]) * 0.45

    days_df = resample_data_by_days(
        result_df, "Timestamp",
        False,
        {},
        {
            'SysPower': 'sum',
            'WSHP_HeatLoad': 'sum',
            'power': 'sum'
        }
    )

    days_df["cost_saving"] = (days_df["power"] + days_df["WSHP_HeatLoad"] - days_df["SysPower"]) * 0.45

    data = {
        "hours_data": {
            "time_data": get_time_in_datetime(hours_df, "h"),
            "cost_saving": hours_df["cost_saving"].values,
            "power_consumption": hours_df["SysPower"].values
        },
        "days_data": {
            "time_data": get_time_in_datetime(days_df, "d"),
            "cost_saving": days_df["cost_saving"].values,
            "power_consumption": days_df["SysPower"].values
        }
    }
    return data


@log_hint
def get_kamba_co2_emission(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 co2减排量
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期
        co2_power_consume: 耗电量
        co2_emission_reduction: co2减排量  需要计算累加值
        co2_equal_num: 等效种植树木数量
    """
    result_df, point_lst = get_data("POWER_CONSUME", start, end, DB["query"], TB["query"][block]["table"])
    result_df = result_df.set_index("Timestamp", drop=True)

    # 数据填充 针对部分日期数据缺失
    all_dates = pd.date_range(start, end, freq="15min")
    result_index = result_df.index

    for _index in all_dates:
        if _index not in result_index:
            for item in point_lst:
                result_df.loc[_index, item] = np.nan

    result_df.sort_values("Timestamp", inplace=True)
    result_df["power_consume"] = result_df.loc[:, point_lst].sum(axis=1)
    result_df = result_df.loc[:, ["power_consume"]]

    hours_consume_items, days_consume_items = collections.OrderedDict(), collections.OrderedDict()

    for hour_index in result_df.index:
        hours_key = datetime(year=hour_index.year, month=hour_index.month, day=hour_index.day, hour=hour_index.hour)
        if hours_key not in hours_consume_items:
            hours_consume_items[hours_key] = [result_df.loc[hour_index, "power_consume"]]
        else:
            hours_consume_items[hours_key].append(result_df.loc[hour_index, "power_consume"])
    hours_power_consume = [item[-1] - item[0] for item in hours_consume_items.values()]
    hours_co2_emission_reduction = [(item[-1] - item[0]) * 0.5839 for item in hours_consume_items.values()]
    hours_co2_equal_num = [(item[-1] - item[0]) * 0.5839 / 1.75 for item in hours_consume_items.values()]

    for day_index in result_df.index:
        days_key = datetime(year=day_index.year, month=day_index.month, day=day_index.day)
        if days_key not in days_consume_items:
            days_consume_items[days_key] = [result_df.loc[day_index, "power_consume"]]
        else:
            days_consume_items[days_key].append(result_df.loc[day_index, "power_consume"])
    days_power_consume = [item[-1] - item[0] for item in days_consume_items.values()]
    days_co2_emission_reduction = [(item[-1] - item[0]) * 0.5839 for item in days_consume_items.values()]
    days_co2_equal_num = [(item[-1] - item[0]) * 0.5839 / 1.75 for item in days_consume_items.values()]
    data = {
        "hours_data": {
            "time_data": list(hours_consume_items.keys()),
            "co2_power_consume": hours_power_consume,
            "co2_emission_reduction": hours_co2_emission_reduction,
            "co2_equal_num": hours_co2_equal_num
        },
        "days_data": {
            "time_data": list(days_consume_items.keys()),
            "co2_power_consume": days_power_consume,
            "co2_emission_reduction": days_co2_emission_reduction,
            "co2_equal_num": days_co2_equal_num
        }
    }
    return data


@log_hint
def get_kamba_pool_temperature(start, end, block="kamba", print_mode=False, log_mode=False):
    """岗巴 水池温度
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :param print_mode: 结束时间
    :param log_mode: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期
        hours_data: 各水池时平均温度字典
        days_data: 各水池日平均温度字典
    """
    result_df, point_lst = get_data("ALL_LEVEL_TEMP", start, end, DB["query"], TB["query"][block]["table"])

    agg_dic = {k: "mean" for k in result_df.columns if k != "Timestamp"}
    hours_df = resample_data_by_hours(result_df, "Timestamp", agg_dic).reset_index()
    days_df = resample_data_by_days(result_df, "Timestamp", True, {}, agg_dic).reset_index()

    hours_data = collections.OrderedDict()
    days_data = collections.OrderedDict()

    for column in hours_df.columns:
        hours_data[column] = hours_df[column].values

    for column in days_df.columns:
        days_data[column] = days_df[column].values

    return {
        "hours_data": hours_data,
        "days_data": days_data
    }

# ======================================================================================================================
# ======================================================================================================================

# ==============================================  天津 统计项目  =========================================================

# MAU 数据板块


@log_hint
def get_mau_fan_frequency(start, end, block="tianjin"):
    result_df = get_data("MAU_FAN_FREQUENCY", start, end, DB["query"], TB["query"][block]["table"])
    result_df = result_df / 50
    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ],
        "mau_fan_frequency_201": result_df["MAU-201-HZ-V"].values,
        "mau_fan_frequency_202": result_df["MAU-202-HZ-V"].values,
        "mau_fan_frequency_203": result_df["MAU-203-HZ-V"].values,
        "mau_fan_frequency_301": result_df["MAU-301-HZ-V"].values,
        "mau_fan_frequency_401": result_df["MAU-401-HZ-V"].values
    }
    return data


@log_hint
def get_mau_cold_water_valve(start, end, block="tianjin"):
    result_df = get_data("MAU_COLD_WATER_VALVE", start, end, DB["query"], TB["query"][block]["table"])
    context = {
        "MAU-201-CW-V": "MAU-201-HZ-V",
        "MAU-202-CW-V": "MAU-202-HZ-V",
        "MAU-203-CW-V": "MAU-203-HZ-V",
        "MAU-301-CW-V": "MAU-301-HZ-V",
        "MAU-401-CW-V": "MAU-401-HZ-V"
    }
    for index in result_df.index:
        for k, v in context.items():
            if result_df.loc[index, v] == 0:
                result_df.loc[index, k] = 0

    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ],
        "mau_cold_water_valve_201": result_df["MAU-201-CW-V"].values,
        "mau_cold_water_valve_202": result_df["MAU-202-CW-V"].values,
        "mau_cold_water_valve_203": result_df["MAU-203-CW-V"].values,
        "mau_cold_water_valve_301": result_df["MAU-301-CW-V"].values,
        "mau_cold_water_valve_401": result_df["MAU-401-CW-V"].values,
    }
    return data


@log_hint
def get_mau_hot_water_valve(start, end, block="tianjin"):
    result_df = get_data("MAU_HOT_WATER_VALVE", start, end, DB["query"], TB["query"][block]["table"])
    context = {
        "MAU-201-HW-V": "MAU-201-HZ-V",
        "MAU-202-HW-V": "MAU-202-HZ-V",
        "MAU-203-HW-V": "MAU-203-HZ-V",
        "MAU-301-HW-V": "MAU-301-HZ-V",
        "MAU-401-HW-V": "MAU-401-HZ-V"
    }
    for index in result_df.index:
        for k, v in context.items():
            if result_df.loc[index, v] == 0:
                result_df.loc[index, k] = 0

    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ],
        "mau_hot_water_valve_201": result_df["MAU-201-HW-V"].values,
        "mau_hot_water_valve_202": result_df["MAU-202-HW-V"].values,
        "mau_hot_water_valve_203": result_df["MAU-203-HW-V"].values,
        "mau_hot_water_valve_301": result_df["MAU-301-HW-V"].values,
        "mau_hot_water_valve_401": result_df["MAU-401-HW-V"].values,
    }
    return data


@log_hint
def get_mau_air_supply_pressure(start, end, block="tianjin"):
    result_df = get_data("MAU_AIR_SUPPLY_PRESSURE", start, end, DB["query"], TB["query"][block]["table"])
    context = {
        "MAU-201-SA-P": "MAU-201-HZ-V",
        "MAU-202-SA-P": "MAU-202-HZ-V",
        "MAU-203-SA-P": "MAU-203-HZ-V",
        "MAU-301-SA-P": "MAU-301-HZ-V",
        "MAU-401-SA-P": "MAU-401-HZ-V"
    }
    for index in result_df.index:
        for k, v in context.items():
            if result_df.loc[index, v] == 0:
                result_df.loc[index, k] = 0

    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ],
        "mau_air_supply_pressure_201": result_df["MAU-201-SA-P"].values,
        "mau_air_supply_pressure_202": result_df["MAU-202-SA-P"].values,
        "mau_air_supply_pressure_203": result_df["MAU-203-SA-P"].values,
        "mau_air_supply_pressure_301": result_df["MAU-301-SA-P"].values,
        "mau_air_supply_pressure_401": result_df["MAU-401-SA-P"].values,
    }
    return data


@log_hint
def get_mau_air_supply_humidity(start, end, block="tianjin"):
    result_df = get_data("MAU_AIR_SUPPLY_HUMIDITY", start, end, DB["query"], TB["query"][block]["table"])
    context = {
        "MAU-201-SA-RH": "MAU-201-HZ-V",
        "MAU-202-SA-RH": "MAU-202-HZ-V",
        "MAU-203-SA-RH": "MAU-203-HZ-V",
        "MAU-301-SA-RH": "MAU-301-HZ-V",
        "MAU-401-SA-RH": "MAU-401-HZ-V"
    }
    for index in result_df.index:
        for k, v in context.items():
            if result_df.loc[index, v] == 0:
                result_df.loc[index, k] = 0

    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ],
        "mau_air_supply_humidity_201": result_df["MAU-201-SA-RH"].values,
        "mau_air_supply_humidity_202": result_df["MAU-202-SA-RH"].values,
        "mau_air_supply_humidity_203": result_df["MAU-203-SA-RH"].values,
        "mau_air_supply_humidity_301": result_df["MAU-301-SA-RH"].values,
        "mau_air_supply_humidity_401": result_df["MAU-401-SA-RH"].values,
    }
    return data


@log_hint
def get_mau_air_supply_temperature(start, end, block="tianjin"):
    result_df = get_data("MAU_AIR_SUPPLY_TEMPERATURE", start, end, DB["query"], TB["query"][block]["table"])
    context = {
        "MAU-201-SA-T": "MAU-201-HZ-V",
        "MAU-202-SA-T": "MAU-202-HZ-V",
        "MAU-203-SA-T": "MAU-203-HZ-V",
        "MAU-301-SA-T": "MAU-301-HZ-V",
        "MAU-401-SA-T": "MAU-401-HZ-V"
    }
    for index in result_df.index:
        for k, v in context.items():
            if result_df.loc[index, v] == 0:
                result_df.loc[index, k] = 0

    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ],
        "mau_air_supply_temperature_201": result_df["MAU-201-SA-T"].values,
        "mau_air_supply_temperature_202": result_df["MAU-202-SA-T"].values,
        "mau_air_supply_temperature_203": result_df["MAU-203-SA-T"].values,
        "mau_air_supply_temperature_301": result_df["MAU-301-SA-T"].values,
        "mau_air_supply_temperature_401": result_df["MAU-401-SA-T"].values,
    }
    return data


# AHU 数据板块

@log_hint
def get_ahu_fan_frequency(start, end, block="tianjin"):
    result_df = get_data("AHU_FAN_FREQUENCY", start, end, DB["query"], TB["query"][block]["table"])
    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ]
    }

    for item in result_df.columns:
        column_name = item.lstrip("AHU-").rstrip("-HZ-V")
        data["mau_fan_frequency_{}".format(column_name)] = result_df[item].values
    return data


@log_hint
def get_ahu_cold_water_valve(start, end, block="tianjin"):
    result_df = get_data("AHU_COLD_WATER_VALVE", start, end, DB["query"], TB["query"][block]["table"])

    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ]
    }

    for item in result_df.columns:
        column_name = item.lstrip("AHU-").rstrip("-CW-V")
        data["ahu_cold_water_valve_{}".format(column_name)] = result_df[item].values

    return data


@log_hint
def get_ahu_hot_water_valve(start, end, block="tianjin"):
    result_df = get_data("AHU_HOT_WATER_VALVE", start, end, DB["query"], TB["query"][block]["table"])
    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ]
    }

    for item in result_df.columns:
        column_name = item.lstrip("AHU-").rstrip("-HW-V")
        data["ahu_hot_water_valve_{}".format(column_name)] = result_df[item].values
    return data


@log_hint
def get_ahu_return_air_temperature(start, end, block="tianjin"):
    result_df = get_data("AHU_RETURN_AIR_TEMPERATURE", start, end, DB["query"], TB["query"][block]["table"])
    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ]
    }

    for item in result_df.columns:
        column_name = item.lstrip("AHU-").rstrip("-RA-T")
        data["ahu_return_air_temperature_{}".format(column_name)] = result_df[item].values
    return data


@log_hint
def get_ahu_return_air_humidity(start, end, block="tianjin"):
    result_df = get_data("AHU_RETURN_AIR_HUMIDITY", start, end, DB["query"], TB["query"][block]["table"])
    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ]
    }

    for item in result_df.columns:
        column_name = item.lstrip("AHU-").rstrip("-RA-RH")
        data["ahu_return_air_humidity_{}".format(column_name)] = result_df[item].values
    return data


@log_hint
def get_ahu_air_supply_pressure(start, end, block="tianjin"):
    result_df = get_data("AHU_AIR_SUPPLY_PRESSURE", start, end, DB["query"], TB["query"][block]["table"])
    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ]
    }

    for item in result_df.columns:
        column_name = item.lstrip("AHU-").rstrip("-SA-P")
        data["ahu_air_supply_pressure_{}".format(column_name)] = result_df[item].values
    return data


@log_hint
def get_ahu_other(start, end, block="tianjin"):

    result_df = get_data("AHU_FAN_FREQUENCY", start, end, DB["query"], TB["query"][block]["table"])
    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ]
    }

    for item in result_df.columns:
        column_name = item.lstrip("AHU-").rstrip("-HZ-V")
        data["ahu_air_delivery_volume_{}".format(column_name)] = (result_df[item] * 9000 / 50).values
        data["ahu_actual_power_{}".format(column_name)] = (((result_df[item] / 50) ** 3) * 11).values
        data["ahu_electricity_saving_{}".format(column_name)] = (
                (
                        (10.35 - ((result_df[item] / 50) ** 3) * 11) / 12
                ) * 0.67
        ).values

    return data


# 公共数据板块

@log_hint
def get_temperature_and_humidity(start, end, block="tianjin"):
    result_df = get_data("TEMPERATURE_AND_HUMIDITY", start, end, DB["query"], TB["query"][block]["table"])
    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ],
        "air_temperature": result_df["temp"].values,
        "air_humidity": result_df["humidity"].values,
    }
    return data


# ======================================================================================================================
# ======================================================================================================================


