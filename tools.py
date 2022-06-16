# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/4/25 17:21
# @Author  : MAYA
import platform
from functools import wraps
from sqlalchemy.dialects.mysql import DATETIME, DOUBLE
import json
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
from itertools import chain


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


class DataMissing(Exception):
    """自定义数据缺失异常
    """
    def __init__(self, error_info):
        super().__init__(self)
        self.error_info = error_info
        logging.error(self.error_info)

    def __str__(self):
        return self.error_info


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


def get_dtype(columns):
    res = {}
    for item in columns:
        if item == "time_data" or item == "Timestamp":
            res[item] = DATETIME
        else:
            res[item] = DOUBLE
    return res


def get_data_range(key):
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
            return result_df.reset_index()
        elif "kamba" in tb:
            common_sql = SQL_CONTEXT[tb]["COMMON_SQL"]
            key_lst = SQL_CONTEXT[tb][sql_key]
            key_lst = [POINT_DF.get(item) for item in key_lst]
            sql = common_sql.format(tb, str(tuple(key_lst)), start, end)
            result_df = pd.read_sql(sql, con=conn).pivot(
                index='Timestamp', columns='pointname', values='value'
            )
            return result_df.reset_index(), key_lst
        elif "tianjin" in tb:
            common_sql = SQL_CONTEXT[tb]["COMMON_SQL"]
            key_lst = SQL_CONTEXT[tb][sql_key]
            sql = common_sql.format(tb, str(tuple(key_lst)), start, end)
            result_df = pd.read_sql(sql, con=conn).pivot(index='date', columns='pointname', values='value')
            return result_df


def get_store_conn():
    """返回数据库连接
    """
    sql_conf = get_sql_conf(DB["store"])

    return create_engine(
        'mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
            sql_conf["user"],
            sql_conf["password"],
            sql_conf["host"],
            sql_conf["database"]
        )
    )


def get_sql_conf(db):
    # 获取数据库配置信息
    if platform.system() == "Windows":
        return {
            "user": "root",
            "password": "299521",
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
        print("函数 {} 执行".format(func))
        # logging.info("函数 {} 执行".format(func))
        try:
            res = func(*args, **kwargs)
            print("函数 {} 执行完成".format(func))
            # logging.info("函数 {} 执行完成".format(func))
            return res
        except Exception as e:
            traceback.print_exc()
            print("函数 {} 异常，异常内容：{}".format(func, e))
            # logging.info("函数 {} 异常，异常内容：{}".format(func, e))
    return wrapper


SQL_CONTEXT = {

    "cona": {
        "API_GEOTHERMAL_WELLS_HEAT_PROVIDE_SQL": """
select * from {} WHERE pointname in ('f3_HHWLoop001_RFlow', 'f3_HHWLoop002_RFlow','f3_HHWLoop003_RFlow',
'f3_HHWLoop_BypassFlow','f3_WSHP001_F','f3_WSHP002_F','f3_WSHP003_F', 'f3_WSHP004_F','f3_WSHP005_F','f3_WSHP006_F',
'f3_HHX_SRT','f3_CL003_T','f4_HHWLoop001_F','f4_HHWLoop_BypassFlow', 'f4_WSHP001_F','f4_WSHP002_F','f4_WSHP003_F',
'f4_WSHP004_F','f4_HHX_SRT','f4_CL003_T','f5_HHWLoop001_RFlow', 'f5_HHWLoop_BypassFlow','f5_WSHP001_F','f5_WSHP002_F',
'f5_WSHP003_F','f5_HHX_SRT','f5_CL003_T', 'f2_WSHP001_HHWET', 'f2_WSHP002_HHWET', 'f2_WSHP003_HHWET', 
'f2_WSHP004_HHWET', 'f2_WSHP001_HHWLT', 'f2_WSHP002_HHWLT', 'f2_WSHP003_HHWLT', 'f2_WSHP004_HHWLT', 'f3_WSHP001_F', 
'f3_WSHP002_F', 'f3_WSHP003_F', 'f3_WSHP004_F', 'f3_WSHP005_F', 'f3_WSHP006_F', 'f3_WSHP001_HHWET', 'f3_WSHP002_HHWET', 
'f3_WSHP003_HHWET', 'f3_WSHP004_HHWET', 'f3_WSHP005_HHWET', 'f3_WSHP006_HHWET', 'f3_WSHP001_HHWLT', 'f3_WSHP002_HHWLT', 
'f3_WSHP003_HHWLT', 'f3_WSHP004_HHWLT', 'f3_WSHP005_HHWLT', 'f3_WSHP006_HHWLT', 'f4_WSHP001_F', 'f4_WSHP002_F', 
'f4_WSHP003_F', 'f4_WSHP004_F', 'f4_WSHP001_HHWET', 'f4_WSHP002_HHWET', 'f4_WSHP003_HHWLT','f4_WSHP003_HHWET', 
'f4_WSHP004_HHWET', 'f4_WSHP001_HHWLT', 'f4_WSHP002_HHWLT', 'f4_WSHP004_HHWLT',  'f2_WSHP001_F', 'f5_WSHP001_F', 
'f5_WSHP002_F', 'f5_WSHP003_F', 'f2_WSHP002_F', 'f5_WSHP001_HHWET', 'f5_WSHP002_HHWET', 'f5_WSHP003_HHWET', 
'f5_WSHP001_HHWLT', 'f5_WSHP002_HHWLT', 'f5_WSHP003_HHWLT', 'f2_WSHP003_F', 'f2_WSHP004_F', 'f2_HW_F','f2_HW_T',
'f3_HW_F','f3_HW_T','f4_HW_F','f4_HW_T','f5_HW_F','f5_HW_T','f2_HW_F','f2_LW_F','f2_LW_T','f3_HW_F', 'f3_LW_F',
'f3_LW_T','f4_HW_F','f4_LW_F','f4_LW_T','f5_HW_F','f5_LW_F','f5_LW_T') and (time between '{}' and '{}')
""",
        "API_COM_COP_SQL": """
select * from {} WHERE pointname in ('f3_WSHP004_F','f2_WSHP003_HHWLT','f3_WSHP005_HHWET',
'f3_WSHP005_HHWLT','f4_WSHP004_HHWET','f3_WSHP001_HHWLT','f2_WSHP004_F','f3_WSHP003_HHWLT','f2_WSHP001_F',
'f2_WSHP003_F','f5_WSHP002_F','f5_WSHP003_F','f3_WSHP003_F','f4_WSHP002_HHWET','f4_WSHP001_HHWLT','f3_WSHP002_HHWLT',
'f5_WSHP003_HHWET','f2_WSHP003_HHWET','f3_WSHP003_HHWET','f3_WSHP001_HHWET','f4_WSHP004_HHWLT','f2_WSHP002_HHWET',
'f5_WSHP002_HHWLT','f2_WSHP001_HHWET','f3_WSHP006_HHWLT','f5_WSHP001_HHWLT','f2_WSHP004_HHWET','f3_WSHP001_F',
'f2_WSHP004_HHWLT','f3_WSHP005_F','f5_WSHP003_HHWLT','f3_WSHP004_HHWET','f3_WSHP004_HHWLT','f3_WSHP006_HHWET',
'f3_WSHP002_HHWET','f2_WSHP002_HHWLT','f4_WSHP003_F','f4_WSHP004_F','f5_WSHP001_HHWET','f5_WSHP002_HHWET',
'f5_WSHP001_F','f3_WSHP002_F','f4_WSHP003_HHWET','f4_WSHP003_HHWLT','f2_WSHP002_F','f3_WSHP006_F','f2_WSHP001_HHWLT',
'f4_WSHP002_HHWLT','f4_WSHP001_F','f4_WSHP001_HHWET','f4_WSHP002_F',

'f2_HHWLoop001_RFlow', 'f2_HHWLoop001_RT',
'f2_HHWLoop001_ST', 'f3_HHWLoop001_RFlow', 'f3_HHWLoop002_RFlow', 'f3_HHWLoop003_RFlow', 'f3_HHWLoop001_RT', 
'f3_HHWLoop002_RT', 'f3_HHWLoop003_RT', 'f3_HHWLoop001_ST', 'f3_HHWLoop002_ST', 'f3_HHWLoop003_ST', 'f4_HHWLoop001_F',
'f4_HHWLoop001_RT', 'f4_HHWLoop001_ST', 'f5_HHWLoop001_RFlow', 'f5_HHWLoop001_RT', 'f5_HHWLoop001_ST',

'f3_meter01_KW', 'f3_meter02_KW', 
'f3_meter03_KW', 'f3_meter04_KW', 'f3_meter05_KW', 'f3_meter06_KW', 'f3_meter07_KW', 'f3_meter08_KW', 'f2_meter01_KW', 
'f2_meter02_KW', 'f2_meter03_KW', 'f2_meter04_KW', 'f2_meter05_KW', 'f2_meter06_KW', 'f4_meter01_KW', 'f4_meter02_KW', 
'f4_meter03_KW', 'f4_meter04_KW', 'f4_meter05_KW', 'f4_meter06_KW', 'f4_meter07_KW', 'f5_meter01_KW', 'f5_meter02_KW', 
'f5_meter03_KW', 'f5_meter04_KW', 'f5_meter05_KW','f5_meter06_KW'  

) and (time between '{}' and '{}')
""",
        "API_COST_SAVING_SQL": """select * from {} WHERE pointname in ('f3_meter01_KW', 'f3_meter02_KW', 
'f3_meter03_KW', 'f3_meter04_KW', 'f3_meter05_KW', 'f3_meter06_KW', 'f3_meter07_KW', 'f3_meter08_KW', 'f2_meter01_KW', 
'f2_meter02_KW', 'f2_meter03_KW', 'f2_meter04_KW', 'f2_meter05_KW', 'f2_meter06_KW', 'f4_meter01_KW', 'f4_meter02_KW', 
'f4_meter03_KW', 'f4_meter04_KW', 'f4_meter05_KW', 'f4_meter06_KW', 'f4_meter07_KW', 'f5_meter01_KW', 'f5_meter02_KW', 
'f5_meter03_KW', 'f5_meter04_KW', 'f5_meter05_KW','f5_meter06_KW',

'f3_HHWLoop001_RFlow','f3_HHWLoop002_RFlow','f3_HHWLoop003_RFlow','f3_HHWLoop_BypassFlow','f3_WSHP001_F','f3_WSHP002_F',
'f3_WSHP003_F','f3_WSHP004_F','f3_WSHP005_F','f3_WSHP006_F','f3_HHX_SRT','f3_CL003_T','f4_HHWLoop001_F',
'f4_HHWLoop_BypassFlow','f4_WSHP001_F','f4_WSHP002_F','f4_WSHP003_F','f4_WSHP004_F','f4_HHX_SRT','f4_CL003_T',
'f5_HHWLoop001_RFlow','f5_HHWLoop_BypassFlow','f5_WSHP001_F','f5_WSHP002_F','f5_WSHP003_F','f5_HHX_SRT','f5_CL003_T',

'f2_WSHP001_HHWET', 'f2_WSHP002_HHWET', 'f2_WSHP003_HHWET', 'f2_WSHP004_HHWET', 'f2_WSHP001_HHWLT', 'f2_WSHP002_HHWLT', 
'f2_WSHP003_HHWLT', 'f2_WSHP004_HHWLT', 'f3_WSHP001_F', 'f3_WSHP002_F', 'f3_WSHP003_F', 'f3_WSHP004_F', 'f3_WSHP005_F', 
'f3_WSHP006_F', 'f3_WSHP001_HHWET', 'f3_WSHP002_HHWET', 'f3_WSHP003_HHWET', 'f3_WSHP004_HHWET', 'f3_WSHP005_HHWET', 
'f3_WSHP006_HHWET', 'f3_WSHP001_HHWLT', 'f3_WSHP002_HHWLT', 'f3_WSHP003_HHWLT', 'f3_WSHP004_HHWLT', 'f3_WSHP005_HHWLT', 
'f3_WSHP006_HHWLT','f4_WSHP001_F', 'f4_WSHP002_F', 'f4_WSHP003_F', 'f4_WSHP004_F', 'f4_WSHP001_HHWET', 
'f4_WSHP002_HHWET', 'f4_WSHP003_HHWLT','f4_WSHP003_HHWET', 'f4_WSHP004_HHWET', 'f4_WSHP001_HHWLT', 'f4_WSHP002_HHWLT', 
'f4_WSHP004_HHWLT', 'f2_WSHP001_F', 'f5_WSHP001_F', 'f5_WSHP002_F', 'f5_WSHP003_F', 'f2_WSHP002_F', 'f5_WSHP001_HHWET', 
'f5_WSHP002_HHWET', 'f5_WSHP003_HHWET', 'f5_WSHP001_HHWLT', 'f5_WSHP002_HHWLT', 'f5_WSHP003_HHWLT', 'f2_WSHP003_F', 
'f2_WSHP004_F'

) and (time between '{}' and '{}')
""",
        "WATER_SUPPLY_RETURN_TEMPERATURE_SQL": """select * from {} WHERE pointname in ('f3_HHWLoop001_ST','f2_HHWLoop001_ST',
'f5_HHWLoop001_RFlow','f3_HHWLoop002_RT','f3_HHWLoop002_RFlow','f5_HHWLoop001_RT',
'f5_HHWLoop001_ST','f4_HHWLoop001_ST','f5_HHWLoop001_RT','f3_HHWLoop003_RFlow','f3_HHWLoop002_ST','f2_HHWLoop001_RT',
'f3_HHWLoop001_RT','f4_HHWLoop001_RT','f2_HHWLoop001_RFlow','f3_HHWLoop001_RFlow','f3_HHWLoop003_RT','f4_HHWLoop001_F',
'f3_HHWLoop003_ST') and (time between '{}' and '{}')
""",
        "API_HEAT_PROVIDE_TEMPERATURE_SQL": """select * from {} WHERE pointname in ('f2_HW_F', 'f2_LW_F', 'f2_HW_T', 
'f2_LW_T', 'f3_HW_F', 'f3_LW_F', 'f3_HW_T', 'f3_LW_T', 'f4_HW_F', 'f4_LW_F', 'f4_HW_T', 'f4_LW_T', 'f5_HW_F', 'f5_LW_F',
'f5_HW_T', 'f5_LW_T', 'fj_SEP_T',

'f2_HHWLoop001_RFlow', 'f2_HHWLoop001_RT',
'f2_HHWLoop001_ST', 'f3_HHWLoop001_RFlow', 'f3_HHWLoop002_RFlow', 'f3_HHWLoop003_RFlow', 'f3_HHWLoop001_RT', 
'f3_HHWLoop002_RT', 'f3_HHWLoop003_RT', 'f3_HHWLoop001_ST', 'f3_HHWLoop002_ST', 'f3_HHWLoop003_ST', 'f4_HHWLoop001_F',
'f4_HHWLoop001_RT', 'f4_HHWLoop001_ST', 'f5_HHWLoop001_RFlow', 'f5_HHWLoop001_RT', 'f5_HHWLoop001_ST',

'f2_WSHP001_HHWET', 'f2_WSHP002_HHWET', 'f2_WSHP003_HHWET', 'f2_WSHP004_HHWET', 'f2_WSHP001_HHWLT', 'f2_WSHP002_HHWLT',
'f2_WSHP003_HHWLT', 'f2_WSHP004_HHWLT', 'f3_WSHP001_F', 'f3_WSHP002_F', 'f3_WSHP003_F', 'f3_WSHP004_F', 'f3_WSHP005_F',
'f3_WSHP006_F', 'f3_WSHP001_HHWET', 'f3_WSHP002_HHWET', 'f3_WSHP003_HHWET', 'f3_WSHP004_HHWET', 'f3_WSHP005_HHWET', 
'f3_WSHP006_HHWET', 'f3_WSHP001_HHWLT', 'f3_WSHP002_HHWLT', 'f3_WSHP003_HHWLT', 'f3_WSHP004_HHWLT', 
'f3_WSHP005_HHWLT', 'f3_WSHP006_HHWLT','f4_WSHP001_F', 'f4_WSHP002_F', 'f4_WSHP003_F', 'f4_WSHP004_F', 
'f4_WSHP001_HHWET', 'f4_WSHP002_HHWET', 'f4_WSHP003_HHWLT','f4_WSHP003_HHWET', 'f4_WSHP004_HHWET', 'f4_WSHP001_HHWLT',
'f4_WSHP002_HHWLT', 'f4_WSHP004_HHWLT', 'f2_WSHP001_F', 'f5_WSHP001_F', 'f5_WSHP002_F', 'f5_WSHP003_F', 
'f2_WSHP002_F', 'f5_WSHP001_HHWET', 'f5_WSHP002_HHWET', 'f5_WSHP003_HHWET', 'f5_WSHP001_HHWLT', 'f5_WSHP002_HHWLT', 
'f5_WSHP003_HHWLT', 'f2_WSHP003_F', 'f2_WSHP004_F',

'f3_HHWLoop001_RFlow',
'f3_HHWLoop002_RFlow','f3_HHWLoop003_RFlow','f3_HHWLoop_BypassFlow','f3_WSHP001_F','f3_WSHP002_F','f3_WSHP003_F',
'f3_WSHP004_F','f3_WSHP005_F','f3_WSHP006_F','f3_HHX_SRT','f3_CL003_T','f4_HHWLoop001_F','f4_HHWLoop_BypassFlow',
'f4_WSHP001_F','f4_WSHP002_F','f4_WSHP003_F','f4_WSHP004_F','f4_HHX_SRT','f4_CL003_T','f5_HHWLoop001_RFlow',
'f5_HHWLoop_BypassFlow','f5_WSHP001_F','f5_WSHP002_F','f5_WSHP003_F','f5_HHX_SRT','f5_CL003_T'
) and (time between '{}' and '{}')
""",
        "WATER_REPLENISHMENT_SQL": """select * from {} WHERE pointname in ('f2_HHWLoop001_RFlow','f3_HHWLoop001_RFlow',
'f3_HHWLoop002_RFlow','f3_HHWLoop003_RFlow','f4_HHWLoop001_F','f5_HHWLoop001_RFlow','f2_HHWLoop001_RFlow',
'f3_HHWLoop001_RFlow','f3_HHWLoop002_RFlow','f3_HHWLoop003_RFlow','f4_HHWLoop001_F','f5_HHWLoop001_RFlow'
) and (time between '{}' and '{}')
 """,
        "COMPREHENSIVE_COP_SQL": """select * from {} WHERE pointname in ('f2_HHWLoop001_RFlow','f2_HHWLoop001_ST',
'f2_HHWLoop001_RT','f3_meter01_KW','f3_meter02_KW','f3_meter03_KW','f3_meter04_KW','f3_meter05_KW','f3_meter06_KW',
'f3_meter07_KW','f3_meter08_KW','f2_WSHP001_F','f2_WSHP001_HHWLT','f2_WSHP001_HHWET','f2_WSHP002_F','f2_WSHP002_HHWLT',
'f2_WSHP002_HHWET','f2_WSHP003_F','f2_WSHP003_HHWLT','f2_WSHP003_HHWET','f2_WSHP004_F','f2_WSHP004_HHWLT',
'f2_WSHP004_HHWET', 'f3_HHWLoop001_RFlow','f3_HHWLoop001_ST','f3_HHWLoop001_RT','f3_HHWLoop002_RFlow',
'f3_HHWLoop002_ST','f3_HHWLoop002_RT','f3_HHWLoop003_RFlow','f3_HHWLoop003_ST','f3_HHWLoop003_RT','f2_meter01_KW',
'f2_meter02_KW','f2_meter03_KW','f2_meter04_KW','f2_meter05_KW','f2_meter06_KW','f3_WSHP001_F','f3_WSHP001_HHWLT',
'f3_WSHP001_HHWET','f3_WSHP002_F','f3_WSHP002_HHWLT','f3_WSHP002_HHWET','f3_WSHP003_F','f3_WSHP003_HHWLT',
'f3_WSHP003_HHWET','f3_WSHP004_F','f3_WSHP004_HHWLT','f3_WSHP004_HHWET','f3_WSHP005_F','f3_WSHP005_HHWLT',
'f3_WSHP005_HHWET','f3_WSHP006_F','f3_WSHP006_HHWLT','f3_WSHP006_HHWET', 'f4_HHWLoop001_F','f4_HHWLoop001_ST',
'f4_HHWLoop001_RT','KW_LC','f4_meter01_KW','f4_meter02_KW','f4_meter03_KW','f4_meter04_KW','f4_meter05_KW',
'f4_meter06_KW','f4_meter07_KW','f4_WSHP001_F','f4_WSHP001_HHWLT','f4_WSHP001_HHWET','f4_WSHP002_F','f4_WSHP002_HHWLT',
'f4_WSHP002_HHWET','f4_WSHP003_F','f4_WSHP003_HHWLT','f4_WSHP003_HHWET','f4_WSHP004_F','f4_WSHP004_HHWLT',
'f4_WSHP004_HHWET', 'f5_HHWLoop001_RFlow','f5_HHWLoop001_ST','f5_HHWLoop001_RT','f5_meter01_KW', 'f5_meter02_KW',
'f5_meter03_KW','f5_meter04_KW','f5_meter05_KW','f5_meter06_KW','f5_WSHP001_F','f5_WSHP001_HHWLT','f5_WSHP001_HHWET',
'f5_WSHP002_F','f5_WSHP002_HHWLT','f5_WSHP002_HHWET','f5_WSHP003_F','f5_WSHP003_HHWLT','f5_WSHP003_HHWET') and 
(time between '{}' and '{}')
""",
        "WATER_HEAT_PUMP_COP_SQL": """select * from {} WHERE pointname in ('f2_WSHP001_F','f2_WSHP001_HHWLT',
'f2_WSHP001_HHWET','f2_WSHP002_F','f2_WSHP002_HHWLT','f2_WSHP002_HHWET','f2_WSHP003_F','f2_WSHP003_HHWLT',
'f2_WSHP003_HHWET','f2_WSHP004_F','f2_WSHP004_HHWLT','f2_WSHP004_HHWET','f3_WSHP001_F','f3_WSHP001_HHWLT',
'f3_WSHP001_HHWET','f3_WSHP002_F','f3_WSHP002_HHWLT','f3_WSHP002_HHWET','f3_WSHP003_F','f3_WSHP003_HHWLT',
'f3_WSHP003_HHWET','f3_WSHP004_F','f3_WSHP004_HHWLT','f3_WSHP004_HHWET','f3_WSHP005_F','f3_WSHP005_HHWLT',
'f3_WSHP005_HHWET','f3_WSHP006_F','f3_WSHP006_HHWLT','f3_WSHP006_HHWET','f4_WSHP001_F','f4_WSHP001_HHWLT',
'f4_WSHP001_HHWET','f4_WSHP002_F','f4_WSHP002_HHWLT','f4_WSHP002_HHWET','f4_WSHP003_F','f4_WSHP003_HHWLT',
'f4_WSHP003_HHWET','f4_WSHP004_F','f4_WSHP004_HHWLT','f4_WSHP004_HHWET','f5_WSHP001_F','f5_WSHP001_HHWLT',
'f5_WSHP001_HHWET','f5_WSHP002_F','f5_WSHP002_HHWLT','f5_WSHP002_HHWET','f5_WSHP003_F','f5_WSHP003_HHWLT',
'f5_WSHP003_HHWET') and (time between '{}' and '{}')
""",
        "ROOM_NETWORK_WATER_SUPPLY_TEMPERATURE_SQL": """select * from {} WHERE pointname in ('f2_HHWLoop001_ST',
'f3_HHWLoop001_ST','f3_HHWLoop002_ST','f3_HHWLoop003_ST','f4_HHWLoop001_ST','f5_HHWLoop001_ST') and
 (time between '{}' and '{}') """
    },
    "kamba": {
        "COMMON_SQL": """select * from {} where pointname in {} and Timestamp between '{}' and '{}'""",
        "PIPE_NETWORK_NETWORK_HEATING": ['管网回水主管,流量', '集水器旁通管,流量', '管网供水主管,温度TE-0701', '管网回水主管,温度TE-0702'],
        "ALL_LEVEL_TEMP": list(
            chain(*[
                ['水池低位温度T{}'.format(num) for num in range(1, 17)],
                ['水池中位温度T{}'.format(num) for num in range(1, 13)],
                ['水池高位温度T{}'.format(num) for num in range(1, 13)]
            ])
        ),
        "COM_COP": ['管网回水主管,流量', '集水器旁通管,流量', '管网供水主管,温度TE-0701', '管网回水主管,温度TE-0702',
                '水源热泵冷凝侧1-1电度量', '水源热泵冷凝侧1-2电度量', '水源热泵冷凝侧1-3电度量', '水源热泵冷凝侧1-4电度量',
                '水源热泵冷凝侧1-5电度量', '水源热泵冷凝侧1-6电度量', '水源热泵冷凝侧1-7电度量', '水源热泵蒸发侧3-1电度量',
                '水源热泵蒸发侧3-2电度量', '水源热泵蒸发侧3-3电度量', '水源热泵蒸发侧3-4电度量', '水源热泵蒸发侧3-5电度量',
                '水源热泵蒸发侧3-6电度量', '水源热泵蒸发侧3-7电度量', '供热主泵4-1电度量', '供热主泵4-2电度量',
                '供热主泵4-3电度量', '供热主泵4-4电度量', '循环泵10-1电度量', '循环泵10-2电度量', '循环泵10-3电度量',
                '循环泵10-4电度量', '冷却塔循环14-1电度量', '冷却塔循环14-2电度量', '冷却塔循环14-3电度量',
                '蓄热水池放热5-1电度量', '水源热泵1电量', '水源热泵2电量', '水源热泵3电量', '水源热泵4电量', '水源热泵5电量',
                '水源热泵6电量'],
        "WSHP_COP": [
                '水源热泵-1冷凝器出口,流量', '水源热泵-1冷凝器出口,温度TE-0602', '水源热泵-1冷凝器进口,温度TE-0601',
                '水源热泵-2冷凝器出口,流量', '水源热泵-2冷凝器出口,温度TE-0604', '水源热泵-2冷凝器进口,温度TE-0603',
                '水源热泵-3冷凝器出口,流量', '水源热泵-3冷凝器出口,温度TE-0606', '水源热泵-3冷凝器进口,温度TE-0605',
                '水源热泵-4冷凝器出口,流量', '水源热泵-4冷凝器出口,温度TE-0608', '水源热泵-4冷凝器进口,温度TE-0607',
                '水源热泵-5冷凝器出口,流量', '水源热泵-5冷凝器出口,温度TE-0610', '水源热泵-5冷凝器进口,温度TE-0609',
                '水源热泵-6冷凝器出口,流量', '水源热泵-6冷凝器出口,温度TE-0612', '水源热泵-6冷凝器进口,温度TE-0611',
                '水源热泵1电量', '水源热泵2电量', '水源热泵3电量', '水源热泵4电量', '水源热泵5电量', '水源热泵6电量'],
        "SOLAR_COLLECTOR": ['流量FM-0201', '太阳能矩阵回水总管，温度TE-050', '太阳能矩阵供水总管，温度TE-049'],
        "WATER_REPLENISHMENT": ['供热端补水FM-0801', '管网回水主管,流量', '集水器旁通管,流量', '蓄热水池补水流量计:流量', '太阳能侧补水,流量', '流量FM-0201'],
        "SOLAR_MATRIX_SUPPLY_AND_RETURN_WATER_TEMPERATURE": ['太阳能矩阵供水总管，温度TE-049', '太阳能矩阵回水总管，温度TE-050'],
        "PIPE_NETWORK_HEATING": ['管网回水主管,流量', '集水器旁通管,流量', '管网供水主管,温度TE-0701', '管网回水主管,温度TE-0702'],
        "END_SUPPLY_AND_RETURN_WATER_TEMPERATURE": ['管网供水主管,温度TE-0701', '管网回水主管,温度TE-0702', '温度'],
        "CALORIES": ['水源热泵-1冷凝器出口,流量', '水源热泵-1冷凝器出口,温度TE-0602', '水源热泵-1冷凝器进口,温度TE-0601',
                        '水源热泵-2冷凝器出口,流量', '水源热泵-2冷凝器出口,温度TE-0604', '水源热泵-2冷凝器进口,温度TE-0603',
                        '水源热泵-3冷凝器出口,流量', '水源热泵-3冷凝器出口,温度TE-0606', '水源热泵-3冷凝器进口,温度TE-0605',
                        '水源热泵-4冷凝器出口,流量', '水源热泵-4冷凝器出口,温度TE-0608', '水源热泵-4冷凝器进口,温度TE-0607',
                        '水源热泵-5冷凝器出口,流量', '水源热泵-5冷凝器出口,温度TE-0610', '水源热泵-5冷凝器进口,温度TE-0609',
                        '水源热泵-6冷凝器出口,流量', '水源热泵-6冷凝器出口,温度TE-0612', '水源热泵-6冷凝器进口,温度TE-0611',
                        '管网回水主管,流量', '水源热泵-1冷凝器出口,流量', '水源热泵-2冷凝器出口,流量', '水源热泵-4冷凝器出口,流量',
                        '水源热泵-5冷凝器出口,流量', '水源热泵-6冷凝器出口,流量', '板换1-1二次侧出口,温度TE-0401',
                        '板换1-2二次侧出口,温度TE-0402', '管网回水主管,温度TE-0702'],
        "SOLAR_HEAT_SUPPLY": ['管网回水主管,流量', '集水器旁通管,流量', '管网供水主管,温度TE-0701', '管网回水主管,温度TE-0702',
                '辐射功率', '流量FM-0201', '太阳能矩阵回水总管，温度TE-050', '太阳能矩阵供水总管，温度TE-049', '管网回水主管,流量',
                              '集水器旁通管,流量', '管网供水主管,温度TE-0701', '管网回水主管,温度TE-0702', '温度'],
        "WSHP_POWER_CONSUME": [
                '水源热泵冷凝侧1-1电度量', '水源热泵冷凝侧1-2电度量', '水源热泵冷凝侧1-3电度量', '水源热泵冷凝侧1-4电度量', '水源热泵冷凝侧1-5电度量', '水源热泵冷凝侧1-6电度量',
                '水源热泵冷凝侧1-7电度量', '水源热泵蒸发侧3-1电度量', '水源热泵蒸发侧3-2电度量', '水源热泵蒸发侧3-3电度量', '水源热泵蒸发侧3-4电度量', '水源热泵蒸发侧3-5电度量',
                '水源热泵蒸发侧3-6电度量', '水源热泵蒸发侧3-7电度量'],
        "COST_SAVING": [
                '水源热泵-1冷凝器出口,流量', '水源热泵-1冷凝器出口,温度TE-0602', '水源热泵-1冷凝器进口,温度TE-0601',
                '水源热泵-2冷凝器出口,流量', '水源热泵-2冷凝器出口,温度TE-0604', '水源热泵-2冷凝器进口,温度TE-0603',
                '水源热泵-3冷凝器出口,流量', '水源热泵-3冷凝器出口,温度TE-0606', '水源热泵-3冷凝器进口,温度TE-0605',
                '水源热泵-4冷凝器出口,流量', '水源热泵-4冷凝器出口,温度TE-0608', '水源热泵-4冷凝器进口,温度TE-0607',
                '水源热泵-5冷凝器出口,流量', '水源热泵-5冷凝器出口,温度TE-0610', '水源热泵-5冷凝器进口,温度TE-0609',
                '水源热泵-6冷凝器出口,流量', '水源热泵-6冷凝器出口,温度TE-0612', '水源热泵-6冷凝器进口,温度TE-0611',
                '管网回水主管,流量', '水源热泵-1冷凝器出口,流量', '水源热泵-2冷凝器出口,流量', '水源热泵-4冷凝器出口,流量',
                '水源热泵-5冷凝器出口,流量', '水源热泵-6冷凝器出口,流量', '板换1-1二次侧出口,温度TE-0401',
                '板换1-2二次侧出口,温度TE-0402', '管网回水主管,温度TE-0702',
                '水源热泵冷凝侧1-1电度量', '水源热泵冷凝侧1-2电度量', '水源热泵冷凝侧1-3电度量', '水源热泵冷凝侧1-4电度量',
                '水源热泵冷凝侧1-5电度量', '水源热泵冷凝侧1-6电度量', '水源热泵冷凝侧1-7电度量', '水源热泵蒸发侧3-1电度量',
                '水源热泵蒸发侧3-2电度量', '水源热泵蒸发侧3-3电度量', '水源热泵蒸发侧3-4电度量', '水源热泵蒸发侧3-5电度量',
                '水源热泵蒸发侧3-6电度量', '水源热泵蒸发侧3-7电度量', '供热主泵4-1电度量', '供热主泵4-2电度量',
                '供热主泵4-3电度量', '供热主泵4-4电度量', '循环泵10-1电度量', '循环泵10-2电度量', '循环泵10-3电度量',
                '循环泵10-4电度量', '冷却塔循环14-1电度量', '冷却塔循环14-2电度量', '冷却塔循环14-3电度量',
                '蓄热水池放热5-1电度量', '水源热泵1电量', '水源热泵2电量', '水源热泵3电量', '水源热泵4电量', '水源热泵5电量',
                '水源热泵6电量'],
        "POWER_CONSUME": ['水源热泵冷凝侧1-1电度量', '水源热泵冷凝侧1-2电度量', '水源热泵冷凝侧1-3电度量', '水源热泵冷凝侧1-4电度量',
                    '水源热泵冷凝侧1-5电度量', '水源热泵冷凝侧1-6电度量', '水源热泵冷凝侧1-7电度量', '水源热泵蒸发侧3-1电度量',
                    '水源热泵蒸发侧3-2电度量', '水源热泵蒸发侧3-3电度量', '水源热泵蒸发侧3-4电度量', '水源热泵蒸发侧3-5电度量',
                    '水源热泵蒸发侧3-6电度量', '水源热泵蒸发侧3-7电度量', '供热主泵4-1电度量', '供热主泵4-2电度量',
                    '供热主泵4-3电度量', '供热主泵4-4电度量', '循环泵10-1电度量', '循环泵10-2电度量', '循环泵10-3电度量',
                    '循环泵10-4电度量', '冷却塔循环14-1电度量', '冷却塔循环14-2电度量', '冷却塔循环14-3电度量',
                    '蓄热水池放热5-1电度量', '水源热泵1电量', '水源热泵2电量', '水源热泵3电量', '水源热泵4电量', '水源热泵5电量',
                    '水源热泵6电量']
    },
    "tianjin": {
        "COMMON_SQL": "select * from {} where pointname in {} and date between '{}' and '{}'",
        "FAN_FREQUENCY": ["MAU-201-HZ-V", "MAU-202-HZ-V", "MAU-203-HZ-V", "MAU-301-HZ-V", "MAU-401-HZ-V"],
        "COLD_WATER_VALVE": ["MAU-201-CW-V", "MAU-202-CW-V", "MAU-203-CW-V", "MAU-301-CW-V", "MAU-401-CW-V", "MAU-201-HZ-V", "MAU-202-HZ-V", "MAU-203-HZ-V", "MAU-301-HZ-V", "MAU-401-HZ-V"],
        "HOT_WATER_VALVE": ["MAU-201-HW-V", "MAU-202-HW-V", "MAU-203-HW-V", "MAU-301-HW-V", "MAU-401-HW-V", "MAU-201-HZ-V", "MAU-202-HZ-V", "MAU-203-HZ-V", "MAU-301-HZ-V", "MAU-401-HZ-V"],
        "AIR_SUPPLY_PRESSURE": ["MAU-201-SA-P", "MAU-202-SA-P", "MAU-203-SA-P", "MAU-301-SA-P", "MAU-401-SA-P", "MAU-201-HZ-V", "MAU-202-HZ-V", "MAU-203-HZ-V", "MAU-301-HZ-V", "MAU-401-HZ-V"],
        "AIR_SUPPLY_HUMIDITY": ["MAU-201-SA-RH", "MAU-202-SA-RH", "MAU-203-SA-RH", "MAU-301-SA-RH", "MAU-401-SA-RH", "MAU-201-HZ-V", "MAU-202-HZ-V", "MAU-203-HZ-V", "MAU-301-HZ-V", "MAU-401-HZ-V"],
        "AIR_SUPPLY_TEMPERATURE": ["MAU-201-SA-T", "MAU-202-SA-T", "MAU-203-SA-T", "MAU-301-SA-T", "MAU-401-SA-T", "MAU-201-HZ-V", "MAU-202-HZ-V", "MAU-203-HZ-V", "MAU-301-HZ-V", "MAU-401-HZ-V"],
        "TEMPERATURE_AND_HUMIDITY": ["temp", "humidity"]
    }


}

POINT_DF = {"时间列 1": "Timestamp", "板换1/2串联调节阀,开度反馈AEV-0211": "HX1_2_AEV_0211 ", "开度反馈DEV-1001": "DEV-1001", "锅炉-1,温度": "BL001_HHWTemp", "锅炉-2,温度": "BL002_HHWTemp", "锅炉-3,温度": "BL003_HHWTemp", "锅炉-4,温度": "BL004_HHWTemp", "流量FM-0201": "SolarRFM_0201", "蓄热水池补水流量计:流量": "Pit_MU_flow", "管网回水主管,流量": "HHWLoop_RFlow", "集水器旁通管,流量": "HHWLoop_BypassFlow", "供热端补水FM-0801": "HHWLoop_MUflow", "太阳能侧补水,流量": "Solar_MUflow", "10-1:泵频率反馈": "SolarHCP101_HZ", "10-2:泵频率反馈": "SolarHCP102_HZ", "10-3:泵频率反馈": "SolarHCP103_HZ", "10-4:泵频率反馈": "SolarHCP104_HZ", "2-1,频率反馈": "HHWPP201_HZ", "2-2,频率反馈": "HHWPP202_HZ", "2-3,频率反馈": "HHWPP203_HZ", "1-1,频率反馈": "HHWPP101_HZ", "1-2,频率反馈": "HHWPP102_HZ", "1-3,频率反馈": "HHWPP103_HZ", "1-4,频率反馈": "HHWPP104_HZ", "1-5,频率反馈": "HHWPP105_HZ", "1-6,频率反馈": "HHWPP106_HZ", "1-7,频率反馈": "HHWPP107_HZ", "管网回水主管,温度TE-0702": "HHWLoop_RT", "管网供水主管,温度TE-0701": "HHWLoop_ST", "4-1:泵频率反馈": "HHWSP401_HZ", "4-2:泵频率反馈": "HHWSP402_HZ", "4-3:泵频率反馈": "HHWSP403_HZ", "4-4:泵频率反馈": "HHWSP404_HZ", "板换1-1二次侧出口,温度TE-0401": "Pit_DisH_HX101_SLT", "板换1-2二次侧出口,温度TE-0402": "Pit_DisH_HX102_SLT", "板换3总管,二次侧出口,温度TE-0237": "Pit_Charge_HX103_106_SLT", "8-1:泵频率反馈": "Pit_ChargeP801_HZ", "8-2:泵频率反馈": "Pit_ChargeP802_HZ", "8-3:泵频率反馈": "Pit_ChargeP803_HZ", "5-1:泵频率反馈": "Pit_DisP501_HZ", "5-2:泵频率反馈": "Pit_DisP502_HZ", "5-3:泵频率反馈": "Pit_DisP503_HZ", "水池低位温度T1": "Pit_LT01_0m00cm", "水池低位温度T2": "Pit_LT02_0m20cm", "水池低位温度T3": "Pit_LT03_0m40cm", "水池低位温度T4": "Pit_LT04_0m60cm", "水池低位温度T5": "Pit_LT05_0m80cm", "水池低位温度T6": "Pit_LT06_1m00cm", "水池低位温度T7": "Pit_LT07_1m20cm", "水池低位温度T8": "Pit_LT08_1m40cm", "水池低位温度T9": "Pit_LT09_1m60cm", "水池低位温度T10": "Pit_LT10_1m80cm", "水池低位温度T11": "Pit_LT11_2m00cm", "水池低位温度T12": "Pit_LT12_2m20cm", "水池低位温度T13": "Pit_LT13_2m40cm", "水池低位温度T14": "Pit_LT14_2m60cm", "水池低位温度T15": "Pit_LT15_2m80cm", "水池低位温度T16": "Pit_LT16_3m00cm", "水池低位温度1": "Pit_LT01_0m00cm", "水池低位温度2": "Pit_LT02_0m20cm", "水池低位温度3": "Pit_LT03_0m40cm", "水池低位温度4": "Pit_LT04_0m60cm", "水池低位温度5": "Pit_LT05_0m80cm", "水池低位温度6": "Pit_LT06_1m00cm", "水池低位温度7": "Pit_LT07_1m20cm", "水池低位温度8": "Pit_LT08_1m40cm", "水池低位温度9": "Pit_LT09_1m60cm", "水池低位温度10": "Pit_LT10_1m80cm", "水池低位温度11": "Pit_LT11_2m00cm", "水池低位温度12": "Pit_LT12_2m20cm", "水池低位温度13": "Pit_LT13_2m40cm", "水池低位温度14": "Pit_LT14_2m60cm", "水池低位温度15": "Pit_LT15_2m80cm", "水池低位温度16": "Pit_LT16_3m00cm", "水池中位温度T1": "Pit_MT01_3m20cm", "水池中位温度T2": "Pit_MT02_3m40cm", "水池中位温度T3": "Pit_MT03_3m60cm", "水池中位温度T4": "Pit_MT04_3m80cm", "水池中位温度T5": "Pit_MT05_4m00cm", "水池中位温度T6": "Pit_MT06_4m20cm", "水池中位温度T7": "Pit_MT07_4m40cm", "水池中位温度T8": "Pit_MT08_4m60cm", "水池中位温度T9": "Pit_MT09_4m80cm", "水池中位温度T10": "Pit_MT10_5m00cm", "水池中位温度T11": "Pit_MT11_5m20cm", "水池中位温度T12": "Pit_MT12_5m40cm", "水池高位温度T1": "Pit_HT01_5m73cm", "水池高位温度T2": "Pit_HT02_6m06cm", "水池高位温度T3": "Pit_HT03_6m39cm", "水池高位温度T4": "Pit_HT04_6m72cm", "水池高位温度T5": "Pit_HT05_7m05cm", "水池高位温度T6": "Pit_HT06_7m38cm", "水池高位温度T7": "Pit_HT07_7m71cm", "水池高位温度T8": "Pit_HT08_8m04cm", "水池高位温度T9": "Pit_HT09_8m37cm", "水池高位温度T10": "Pit_HT10_8m70cm", "水池高位温度T11": "Pit_HT11_9m03cm", "水池高位温度T12": "Pit_HT12_9m36cm", "水池液位:液位1": "Pit_W_level1", "水池液位:液位2": "Pit_W_level2", "蓄热水池底部,PH值": "Pit_L_PH", "蓄热水池中部,PH值": "Pit_M_PH", "蓄热水池顶部,PH值": "Pit_H_PH", "冷却塔集水盘水位:液位": "CT_W_level", "冷却塔回水温度传感器:温度TE-0901": "CT_CWLT", "冷凝器侧补水,压力": "Cond_MU_P", "蒸发器侧补水,压力": "Evap_MU_P", "太阳能侧补水,压力": "Solar_MU_P", "回水主管,压力P-0804": "HHWLoop_RP", "集水器,压力": "HHWLoop_R_COM_P", "集水器,温度": "HHWLoop_R_COM_T", "太阳能矩阵供水总管，温度TE-049": "SolarHWLoop_ST", "太阳能矩阵回水总管，温度TE-050": "SolarHWLoop_RT", "矩阵内平均温度": "SolarPT_Ave", "SEHCM01-板内温度传感器1": "SolarPT1_SEHCM01", "SEHCM01-板内温度传感器2": "SolarPT2_SEHCM01", "SEHCM01-板内温度传感器3": "SolarPT3_SEHCM01", "SEHCM01-板内温度传感器4": "SolarPT4_SEHCM01", "SEHCM01-板内温度传感器5": "SolarPT5_SEHCM01", "SEHCM01-矩阵主管进口温度传感器": "SolarInletT__SEHCM01", "SEHCM01-矩阵主管出口温度传感器": "SolarOutletT__SEHCM01", "SEHCM02-板内温度1": "SolarPT1_SEHCM02", "SEHCM02-板内温度2": "SolarPT2_SEHCM02", "SEHCM02-板内温度3": "SolarPT3_SEHCM02", "SEHCM02-板内温度4": "SolarPT4_SEHCM02", "SEHCM02-板内温度5": "SolarPT5_SEHCM02", "SEHCM02-进口温度": "SolarInletT__SEHCM02", "SEHCM02-出口温度": "SolarOutletT__SEHCM02", "SEHCM03-板内温度1": "SolarPT1_SEHCM03", "SEHCM03-板内温度2": "SolarPT2_SEHCM03", "SEHCM03-板内温度3": "SolarPT3_SEHCM03", "SEHCM03-板内温度4": "SolarPT4_SEHCM03", "SEHCM03-板内温度5": "SolarPT5_SEHCM03", "SEHCM03-进口温度": "SolarInletT__SEHCM03", "SEHCM03-出口温度": "SolarOutletT__SEHCM03", "SEHCM04-板内温度1": "SolarPT1_SEHCM04", "SEHCM04-板内温度2": "SolarPT2_SEHCM04", "SEHCM04-板内温度3": "SolarPT3_SEHCM04", "SEHCM04-板内温度4": "SolarPT4_SEHCM04", "SEHCM04-板内温度5": "SolarPT5_SEHCM04", "SEHCM04-进口温度": "SolarInletT__SEHCM04", "SEHCM04-出口温度": "SolarOutletT__SEHCM04", "SEHCM05-板内温度1": "SolarPT1_SEHCM05", "SEHCM05-板内温度2": "SolarPT2_SEHCM05", "SEHCM05-板内温度3": "SolarPT3_SEHCM05", "SEHCM05-板内温度4": "SolarPT4_SEHCM05", "SEHCM05-板内温度5": "SolarPT5_SEHCM05", "SEHCM05-进口温度": "SolarInletT__SEHCM05", "SEHCM05-出口温度": "SolarOutletT__SEHCM05", "SEHCM06-板内温度1": "SolarPT1_SEHCM06", "SEHCM06-板内温度2": "SolarPT2_SEHCM06", "SEHCM06-板内温度3": "SolarPT3_SEHCM06", "SEHCM06-板内温度4": "SolarPT4_SEHCM06", "SEHCM06-板内温度5": "SolarPT5_SEHCM06", "SEHCM06-进口温度": "SolarInletT__SEHCM06", "SEHCM06-出口温度": "SolarOutletT__SEHCM06", "SEHCM07-板内温度1": "SolarPT1_SEHCM07", "SEHCM07-板内温度2": "SolarPT2_SEHCM07", "SEHCM07-板内温度3": "SolarPT3_SEHCM07", "SEHCM07-板内温度4": "SolarPT4_SEHCM07", "SEHCM07-板内温度5": "SolarPT5_SEHCM07", "SEHCM07-进口温度": "SolarInletT__SEHCM07", "SEHCM07-出口温度": "SolarOutletT__SEHCM07", "SEHCM08-板内温度1": "SolarPT1_SEHCM08", "SEHCM08-板内温度2": "SolarPT2_SEHCM08", "SEHCM08-板内温度3": "SolarPT3_SEHCM08", "SEHCM08-板内温度4": "SolarPT4_SEHCM08", "SEHCM08-板内温度5": "SolarPT5_SEHCM08", "SEHCM08-进口温度": "SolarInletT__SEHCM08", "SEHCM08-出口温度": "SolarOutletT__SEHCM08", "软化水箱,液位": "SoftWTank_Level", "容积式换热水箱,生活热水,温度": "DHWT", "容积式换热水箱,水箱内,温度": "DHW_Tank_T", "生活高位水位:液位": "DHW_Tank_Level", "消防水池水位:液位": "Fire_Tank_Level", "板换2总管,二次侧出口,温度": "Pit_DisL_HX201_206_SLT", "板换1总管,一次侧出口,温度": "Pit_DisH_HX101_102_PLT", "板换2总管,一次侧出口,温度": "Pit_DisL_HX201_206_PLT", "水源热泵-1蒸发器进口,温度TE-0507": "WSHP001_CHWET", "水源热泵-1蒸发器出口,温度TE-0508": "WSHP001_CHWLT", "水源热泵-1冷凝器出口,流量": "WSHP001_HHWF", "水源热泵-1冷凝器进口,温度TE-0601": "WSHP001_HHWET", "水源热泵-1冷凝器出口,温度TE-0602": "WSHP001_HHWLT", "水源热泵-2蒸发器进口,温度TE-0509": "WSHP002_CHWET", "水源热泵-2蒸发器出口,温度TE-0510": "WSHP002_CHWLT", "水源热泵-2冷凝器出口,流量": "WSHP002_HHWF", "水源热泵-2冷凝器进口,温度TE-0603": "WSHP002_HHWET", "水源热泵-2冷凝器出口,温度TE-0604": "WSHP002_HHWLT", "水源热泵-3蒸发器进口,温度TE-0511": "WSHP003_CHWET", "水源热泵-3蒸发器出口,温度TE-0512": "WSHP003_CHWLT", "水源热泵-3冷凝器出口,流量": "WSHP003_HHWF", "水源热泵-3冷凝器进口,温度TE-0605": "WSHP003_HHWET", "水源热泵-3冷凝器出口,温度TE-0606": "WSHP003_HHWLT", "水源热泵-4蒸发器进口,温度TE-0513": "WSHP004_CHWET", "水源热泵-4蒸发器出口,温度TE-0514": "WSHP004_CHWLT", "水源热泵-4冷凝器出口,流量": "WSHP004_HHWF", "水源热泵-4冷凝器进口,温度TE-0607": "WSHP004_HHWET", "水源热泵-4冷凝器出口,温度TE-0608": "WSHP004_HHWLT", "水源热泵-5蒸发器进口,温度TE-0515": "WSHP005_CHWET", "水源热泵-5蒸发器出口,温度TE-0516": "WSHP005_CHWLT", "水源热泵-5冷凝器出口,流量": "WSHP005_HHWF", "水源热泵-5冷凝器进口,温度TE-0609": "WSHP005_HHWET", "水源热泵-5冷凝器出口,温度TE-0610": "WSHP005_HHWLT", "水源热泵-6蒸发器进口,温度TE-0517": "WSHP006_CHWET", "水源热泵-6蒸发器出口,温度TE-0518": "WSHP006_CHWLT", "水源热泵-6冷凝器出口,流量": "WSHP006_HHWF", "水源热泵-6冷凝器进口,温度TE-0611": "WSHP006_HHWET", "水源热泵-6冷凝器出口,温度TE-0612": "WSHP006_HHWLT", "气压": "Barometric", "温度": "OAT", "湿度": "RH", "辐射量": "SolarE", "辐射功率": "SolarW", "水源热泵冷凝侧1-1电度量": "HHWPP101KWH", "水源热泵冷凝侧1-2电度量": "HHWPP102KWH", "水源热泵冷凝侧1-3电度量": "HHWPP103KWH", "水源热泵冷凝侧1-4电度量": "HHWPP104KWH", "水源热泵冷凝侧1-5电度量": "HHWPP105KWH", "水源热泵冷凝侧1-6电度量": "HHWPP106KWH", "水源热泵冷凝侧1-7电度量": "HHWPP107KWH", "水源热泵蒸发侧3-1电度量": "CHWP301KWH", "水源热泵蒸发侧3-2电度量": "CHWP302KWH", "水源热泵蒸发侧3-3电度量": "CHWP303KWH", "水源热泵蒸发侧3-4电度量": "CHWP304KWH", "水源热泵蒸发侧3-5电度量": "CHWP305KWH", "水源热泵蒸发侧3-6电度量": "CHWP306KWH", "水源热泵蒸发侧3-7电度量": "CHWP307KWH", "供热主泵4-1电度量": "SHHWP401KWH", "供热主泵4-2电度量": "SHHWP402KWH", "供热主泵4-3电度量": "SHHWP403KWH", "供热主泵4-4电度量": "SHHWP404KWH", "循环泵10-1电度量": "SolarHCP101KWH", "循环泵10-2电度量": "SolarHCP102KWH", "循环泵10-3电度量": "SolarHCP103KWH", "循环泵10-4电度量": "SolarHCP104KWH", "冷却塔循环14-1电度量": "CTPump141KWH", "冷却塔循环14-2电度量": "CTPump142KWH", "冷却塔循环14-3电度量": "CTPump143KWH", "蓄热水池放热5-1电度量": "Pit_DisP501KWH", "蓄热水池放热5-2电度量": "Pit_DisP502KWH", "蓄热水池放热5-3电度量": "Pit_DisP503KWH", "蓄热水池8-1电度量": "Pit_ChargeP801KWH", "蓄热水池8-2电度量": "Pit_ChargeP802KWH", "蓄热水池8-3电度量": "Pit_ChargeP803KWH", "高温板换冷凝侧2-1电度量": "HHWPP201KWH", "高温板换冷凝侧2-2电度量": "HHWPP202KWH", "高温板换冷凝侧2-3电度量": "HHWPP203KWH", "冷却塔风机1电度量": "CTF001KWH", "冷却塔风机2电度量": "CTF002KWH", "冷却塔风机3电度量": "CTF003KWH", "冷却塔风机4电量": "CTF004KWH", "供热侧补水6-1电量": "HHWLoopMUP601KWH", "供热侧补水6-2电量": "HHWLoopMUP602KWH", "太阳能矩阵补水12-2电量": "SolarMUP122KWH", "太阳能矩阵补水12-1电量": "SolarMUP121KWH", "冷却塔补水7-1电量": "CTMUP701KWH", "冷却塔补水7-2电量": "CTMUP702KWH", "软化水箱补水1电量": "SoftWMUP001KWH", "软化水箱补水2电量": "SoftWMUP002KWH", "蓄热水池补水13-2电量": "PitMUP132KWH", "蓄热水池补水13-1电量": "PitMUP131KWH", "水源热泵1电量": "WSHP001KWH", "水源热泵2电量": "WSHP002KWH", "水源热泵3电量": "WSHP003KWH", "水源热泵4电量": "WSHP004KWH", "水源热泵5电量": "WSHP005KWH", "水源热泵6电量": "WSHP006KWH", "湿球温度": "WB", "冷却塔环境温度": "CT_OAT", "水池高温层平均温度": "Pit_H_T", "水池中温层平均温度": "Pit_M_T", "水池低温层平均温度": "Pit_L_T", "CWP101频率": "CWP101_HZ", "CWP102频率": "CWP102_HZ", "CWP103频率": "CWP103_HZ", "LQ-T5温度": "CT_LT", "冷却塔风机1频率": "CTF001_HZ", "冷却塔风机2频率": "CTF002_HZ", "远端供水压力": "HHWSP", "远端回水压力": "HHWRP"}

STORE_SQL = {
    "cona": "insert into "
}

DB = {
    "query": "data_center_original",
    "store": "data_center_statistical"
}

TB = {
    "query": {
        "cona": "cona",
        "kamba": "kamba",
        "tianjin": "tianjin"
    },
    "store": {
        "cona": {
            "hours": "cona_hours_data",
            "days": "cona_days_data",
        },
        "kamba": {
            "hours": "kamba_hours_data",
            "days": "kamba_days_data",
            "pool_temperature": {
                "hours": "kamba_hours_pool_temperature",
                "days": "kamba_days_pool_temperature"
            }
        },
        "tianjin": "tianjin_commons_data"
        # "tianjin": {
        #     "hours": "tianjin_hours_data",
        #     "days": "tianjin_days_data"
        # }
    }

}

height = ['0', '0.2', '0.4', '0.6', '0.8', '1', '1.2', '1.4', '1.6', '1.8', '2', '2.2', '2.4', '2.6', '2.8', '3', '3.2',
          '3.4', '3.6', '3.8', '4', '4.2', '4.4', '4.6', '4.8', '5', '5.2', '5.4', '5.73', '6.06', '6.39', '6.72',
          '7.05', '7.38', '7.71', '8.04', '8.37', '8.7', '9.03', '9.36']
VOLUME = [1031.370428, 1044.176354, 1057.061292, 1070.025243, 1083.068206, 1096.190181,
          1109.391169, 1122.671169, 1136.030181, 1149.468206, 1162.985243, 1176.581292,
          1190.256354, 1204.010428, 1217.843514, 1231.755613, 1245.746724, 1259.816848,
          1273.965984, 1288.194132, 1302.501292, 1316.887465, 1331.35265, 1367.861292,
          1382.603021, 1397.423761, 1412.323514, 2024.324037, 2146.595148, 2180.564037,
          2214.799593, 2249.301815, 2284.070704, 2319.106259, 2354.408481, 2389.97737,
          2425.812926, 2461.915148, 2498.284037, 840.8894115]

