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
from itertools import chain


# **********************************     数据解析相关函数    **************************************************************

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


# *****************************************     数据库配置    ************************************************************
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

DB = {
    "query": "data_center_original",
    "store": "data_center_statistical",
    "backup": "data_center_statistical_wide"
}

TB = {
    "query": {
        "cona": {
            "table": "cona",
            "time_index": "time"
        },
        "kamba": {
            "table": "kamba",
            "time_index": "Timestamp"
        },
        "tianjin": {
            "table": "tianjin",
            "time_index": "date"
        }
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

    },
    "backup": {
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
    }

}

# 数据参数定义
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


@log_hint
def get_fan_frequency(start, end, block="tianjin"):
    result_df = get_data("FAN_FREQUENCY", start, end, DB["query"], TB["query"][block]["table"])
    result_df = result_df / 50
    data = {
        "time_data": [
            datetime(
                year=item.year, month=item.month, day=item.day, hour=item.hour, minute=item.minute, second=item.second
            ) for item in result_df.index
        ],
        "fan_frequency_201": result_df["MAU-201-HZ-V"].values,
        "fan_frequency_202": result_df["MAU-202-HZ-V"].values,
        "fan_frequency_203": result_df["MAU-203-HZ-V"].values,
        "fan_frequency_301": result_df["MAU-301-HZ-V"].values,
        "fan_frequency_401": result_df["MAU-401-HZ-V"].values
    }
    return data


@log_hint
def get_cold_water_valve(start, end, block="tianjin"):
    result_df = get_data("COLD_WATER_VALVE", start, end, DB["query"], TB["query"][block]["table"])
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
        "cold_water_valve_201": result_df["MAU-201-CW-V"].values,
        "cold_water_valve_202": result_df["MAU-202-CW-V"].values,
        "cold_water_valve_203": result_df["MAU-203-CW-V"].values,
        "cold_water_valve_301": result_df["MAU-301-CW-V"].values,
        "cold_water_valve_401": result_df["MAU-401-CW-V"].values,
    }
    return data


@log_hint
def get_hot_water_valve(start, end, block="tianjin"):
    result_df = get_data("HOT_WATER_VALVE", start, end, DB["query"], TB["query"][block]["table"])
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
        "hot_water_valve_201": result_df["MAU-201-HW-V"].values,
        "hot_water_valve_202": result_df["MAU-202-HW-V"].values,
        "hot_water_valve_203": result_df["MAU-203-HW-V"].values,
        "hot_water_valve_301": result_df["MAU-301-HW-V"].values,
        "hot_water_valve_401": result_df["MAU-401-HW-V"].values,
    }
    return data


@log_hint
def get_air_supply_pressure(start, end, block="tianjin"):
    result_df = get_data("AIR_SUPPLY_PRESSURE", start, end, DB["query"], TB["query"][block]["table"])
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
        "air_supply_pressure_201": result_df["MAU-201-SA-P"].values,
        "air_supply_pressure_202": result_df["MAU-202-SA-P"].values,
        "air_supply_pressure_203": result_df["MAU-203-SA-P"].values,
        "air_supply_pressure_301": result_df["MAU-301-SA-P"].values,
        "air_supply_pressure_401": result_df["MAU-401-SA-P"].values,
    }
    return data


@log_hint
def get_air_supply_humidity(start, end, block="tianjin"):
    result_df = get_data("AIR_SUPPLY_HUMIDITY", start, end, DB["query"], TB["query"][block]["table"])
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
        "air_supply_humidity_201": result_df["MAU-201-SA-RH"].values,
        "air_supply_humidity_202": result_df["MAU-202-SA-RH"].values,
        "air_supply_humidity_203": result_df["MAU-203-SA-RH"].values,
        "air_supply_humidity_301": result_df["MAU-301-SA-RH"].values,
        "air_supply_humidity_401": result_df["MAU-401-SA-RH"].values,
    }
    return data


@log_hint
def get_air_supply_temperature(start, end, block="tianjin"):
    result_df = get_data("AIR_SUPPLY_TEMPERATURE", start, end, DB["query"], TB["query"][block]["table"])
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
        "air_supply_temperature_201": result_df["MAU-201-SA-T"].values,
        "air_supply_temperature_202": result_df["MAU-202-SA-T"].values,
        "air_supply_temperature_203": result_df["MAU-203-SA-T"].values,
        "air_supply_temperature_301": result_df["MAU-301-SA-T"].values,
        "air_supply_temperature_401": result_df["MAU-401-SA-T"].values,
    }
    return data


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
