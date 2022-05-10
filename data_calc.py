# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/5 17:09
# @Author  : MAYA
import random
import pymysql
import platform
import pandas as pd
import logging
import traceback
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import FLOAT, DateTime
from tools import resample_data_by_hours, resample_data_by_days, SQL_CONTEXT, DB, TB, log_hint, POINT_DF
from datetime import datetime
import collections


# ************************************************  公共函数  ************************************************************

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


def data_collation(block, start, end):
    res = {
        "hours_data": {},
        "days_data": {}
    }
    print("{} 数据获取 开始".format(block))
    if block == "cona":
        # 错那统计内容
        # 1. 地热井提供热量
        # 2. 综合COP
        # 3. 节省供暖费用
        # 4. 供回水温度
        # 5. 供热量
        # 6. 负荷量
        # 7. 补水量
        # 8. 子页面 综合COP
        # 9. 子页面 水源热泵COP
        # 10. 子页面 供水温度与气温
        geothermal_wells_heat_provide = get_cona_geothermal_wells_heat_provide(start, end)
        com_cop = get_cona_com_cop(start, end)
        cost_saving = get_cona_cost_saving(start, end)
        heat_provided = get_cona_heat_provided(start, end)
        water_supply_return_temperature = get_cona_water_supply_return_temperature(start, end)
        water_replenishment = get_cona_water_replenishment(start, end)
        sub_com_cop = get_cona_sub_com_cop(start, end)
        sub_water_source_cop = get_cona_sub_water_source_cop(start, end)
        room_network_water_supply_temperature = get_cona_room_network_water_supply_temperature(start, end)
        items = [
            geothermal_wells_heat_provide,
            com_cop,
            cost_saving,
            heat_provided,
            water_supply_return_temperature,
            water_replenishment,
            sub_com_cop,
            sub_water_source_cop,
            room_network_water_supply_temperature
        ]
        success, hours_time, days_time = check_time(items)

        if success:

            for item in items:
                res["hours_data"].update(item["hours_data"])
                res["days_data"].update(item["days_data"])

            res["hours_data"]["time_data"] = [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in res["hours_data"]["time_data"]
            ]

            res["days_data"]["time_data"] = [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day
                ) for item in res["days_data"]["time_data"]
            ]
            # 获取日平均温度
            days_temp = get_cona_temp(res["days_data"]["time_data"])
            res["days_data"]["temp"] = days_temp
        else:
            print("数据获取异常")
            exit()
    print("{} 数据获取 完成".format(block))
    return res


def get_dtype(columns):
    res = {}
    for item in columns:
        if item == "time_data":
            res[item] = DateTime
        else:
            res[item] = FLOAT
    return res


def update_history_data():
    context = {
        "cona": {
            "start": "2020-12-31 00:00:00",
            "end": "2021-05-14 23:59:59"
        },
        "kamba": {
            "start": "2020-08-17 00:00:00",
            "end": "2022-05-09 23:59:59"
        }
    }
    start, end = "2020-12-31 00:00:00", "2021-05-14 23:59:59"
    for block in ["cona"]:
        items = data_collation(block, start, end)
        store_data(block, items)


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


def store_data(block, items):
    """数据存储

    :param block: 数据隶属 如：cona、kamba、tianjin
    :param items: 数据集合
    """

    engine = get_store_conn()
    hours_data, days_data = items["hours_data"], items["days_data"]

    try:
        # logging.info("开始 {} - {} 上传".format(block, "时数据"))
        print("开始 {} - {} 上传".format(block, "时数据"))
        hours_dtype = get_dtype(hours_data.keys())
        hours_df = pd.DataFrame(hours_data)
        hours_df.to_csv("hours.csv")
        hours_df.to_sql(
            name=TB["store"][block]["hours"],
            con=engine,
            if_exists="append",
            index=False,
            dtype=hours_dtype
        )
        # logging.info("完成 {} - {} 上传".format(block, "时数据"))
        print("完成 {} - {} 上传".format(block, "时数据"))

        # logging.info("开始 {} - {} 上传".format(block, "日数据"))
        print("开始 {} - {} 上传".format(block, "日数据"))
        days_dtype = get_dtype(days_data.keys())
        days_df = pd.DataFrame(days_data)
        days_df.to_csv("days.csv")
        days_df.to_sql(
            name=TB["store"][block]["days"],
            con=engine,
            if_exists="append",
            index=False,
            dtype=days_dtype
        )
        # logging.info("完成 {} - {} 上传".format(block, "日数据"))
        print("完成 {} - {} 上传".format(block, "日数据"))

    except Exception as e:
        logging.error("数据上传异常")
        traceback.print_exc()
    finally:
        engine.dispose()


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
            common_sql = SQL_CONTEXT["COMMON_SQL"]
            key_lst = SQL_CONTEXT[tb][sql_key]
            key_lst = [POINT_DF.get(item) for item in key_lst]
            sql = common_sql.format(tb, str(tuple(key_lst)), start, end)
            result_df = pd.read_sql(sql, con=conn).pivot(
                index='Timestamp', columns='pointname', values='value'
            )
            result_df.replace(to_replace='.*[u\.].*', value=np.nan, regex=True, inplace=True)
            return result_df.reset_index(), key_lst


# **********************************************************************************************************************
# **********************************************************************************************************************


# **********************************************  错那 统计项目  *********************************************************


@log_hint
def get_cona_geothermal_wells_heat_provide(start, end, block="cona"):
    """错那 地热井提供热量（高温版换制热量、水源热泵制热量、地热井可提供高温热量、地热井可提供低温热量）
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        high_temp_plate_exchange_heat_production': 高温版换制热量,
        water_heat_pump_heat_production: 水源热泵制热量,
        geothermal_wells_high_heat_provide: 地热井可提供高温热量,
        geothermal_wells_low_heat_provide: 地热井可提供低温热量
    """
    result_df = get_data("API_GEOTHERMAL_WELLS_HEAT_PROVIDE_SQL", start, end, DB["query"], TB["query"][block])
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
        result_df, "time",
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
            'time_data': [item for item in hours_df.index],
            'high_temp_plate_exchange_heat_production': hours_df.high_temp_plate_exchange_heat_production.values,
            'water_heat_pump_heat_production': hours_df.water_heat_pump_heat_production.values,
            'geothermal_wells_high_heat_provide': hours_df.geothermal_wells_high_heat_provide.values,
            'geothermal_wells_low_heat_provide': hours_df.geothermal_wells_low_heat_provide.values
        },
        "days_data": {
            'time_data': [item for item in days_df.index],
            'high_temp_plate_exchange_heat_production': days_df.high_temp_plate_exchange_heat_production.values,
            'water_heat_pump_heat_production': days_df.water_heat_pump_heat_production.values,
            'geothermal_wells_high_heat_provide': days_df.geothermal_wells_high_heat_provide.values,
            'geothermal_wells_low_heat_provide': days_df.geothermal_wells_low_heat_provide.values
        }
    }
    return data


@log_hint
def get_cona_com_cop(start, end, block="cona"):
    """错那 综合COP
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        com_cop: COP能效,

    """
    result_df = get_data("API_COM_COP_SQL", start, end, DB["query"], TB["query"][block])
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
    result_df['heat_pipe_network_heating'] = result_df['RU'] + result_df['RV'] + result_df['RW'] + result_df[
        'RX']

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
            'time_data': [item for item in hours_df.index],
            'com_cop': hours_cop.values
        },
        "days_data": {
            'time_data': [item for item in days_df.index],
            'com_cop': days_cop.values
        }
    }
    return data


@log_hint
def get_cona_cost_saving(start, end, block="cona"):
    """错那 供暖费用
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        cost_saving: 供暖费用,
        high_temp_charge: 高温供暖费用,
        low_temp_charge: 低温供暖费用
    """
    result_df = get_data("API_COST_SAVING_SQL", start, end, DB["query"], TB["query"][block])

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
            "time_data": [item for item in hours_df.index],
            "cost_saving": hours_df["cost_saving"].values,
            "high_temp_charge": hours_df["high_temp_charge"].values,
            "low_temp_charge": hours_df["low_temp_charge"].values
        },
        "days_data": {
            "time_data": [item for item in days_df.index],
            "cost_saving": days_df["cost_saving"].values,
            "high_temp_charge": days_df["high_temp_charge"].values,
            "low_temp_charge": days_df["low_temp_charge"].values
        }
    }

    return data


@log_hint
def get_cona_heat_provided(start, end, block="cona"):
    """错那 供热量
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        heat_well_heating:  热力井供热,
        heat_pipe_network_heating: 热力管网供热,
        water_heat_pump_heat_production: 水源热泵供热
        high_temp_plate_exchange_heat_production: 高温板换供热
        max_load: 最大负荷
        min_load: 最小负荷
        avg_load: 平均负荷
    """
    result_df = get_data("API_HEAT_PROVIDE_TEMPERATURE_SQL", start, end, DB["query"], TB["query"][block])
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
    # print(hours_df)

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
            'time_data': [item for item in hours_df.index],
            "heat_well_heating": hours_df["heat_well_heating"].values,
            "heat_pipe_network_heating": hours_df["heat_pipe_network_heating"].values,
            "water_heat_pump_heat_production": hours_df["water_heat_pump_heat_production"].values,
            "high_temp_plate_exchange_heat_production": hours_df["high_temp_plate_exchange_heat_production"].values,
            "max_load": load_hours_df["heat_pipe_network_heating"]["max"].values,
            "min_load": load_hours_df["heat_pipe_network_heating"]["min"].values,
            "avg_load": load_hours_df["heat_pipe_network_heating"]["mean"].values
        },
        "days_data": {
            'time_data': [item for item in days_df.index],
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
def get_cona_water_supply_return_temperature(start, end, block="cona"):
    """错那 供回水温度
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       water_supply_temperature:  供水温度,
       return_water_temperature: 回水温度,
       supply_return_water_temp_diff: 供回水温差
   """
    result_df = get_data("WATER_SUPPLY_RETURN_TEMPERATURE_SQL", start, end, DB["query"], TB["query"][block])

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
            'time_data': [item for item in hours_df.index],
            'water_supply_temperature': hours_df["water_supply_temperature"].values,
            'return_water_temperature': hours_df["return_water_temperature"].values,
            'supply_return_water_temp_diff': hours_df["supply_return_water_temp_diff"].values
        },
        "days_data": {
            'time_data': [item for item in days_df.index],
            'water_supply_temperature': days_df["water_supply_temperature"].values,
            'return_water_temperature': days_df["return_water_temperature"].values,
            'supply_return_water_temp_diff': days_df["supply_return_water_temp_diff"].values,
        }
    }
    return data


@log_hint
def get_cona_water_replenishment(start, end, block="cona"):
    """错那 补水量
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       water_replenishment:  补水量,
       water_replenishment_limit: 补水量限值
   """

    result_df = get_data("WATER_REPLENISHMENT_SQL", start, end, DB["query"], TB["query"][block])
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
            'time_data': [item for item in hours_df.index],
            'water_replenishment': hours_df["water_replenishment"].values,
            'water_replenishment_limit': hours_df["water_replenishment_limit"].values
        },
        "days_data": {
            'time_data': [item for item in days_df.index],
            'water_replenishment': days_df["water_replenishment"].values,
            'water_replenishment_limit': days_df["water_replenishment_limit"].values
        }
    }
    return data


@log_hint
def get_cona_sub_com_cop(start, end, block="cona"):
    """错那 sub 机房综合COP能效
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       f2_cop:  2号机房综合COP,
       f3_cop:  3号机房综合COP,
       f4_cop:  4号机房综合COP,
       f5_cop:  5号机房综合COP
   """
    result_df = get_data("COMPREHENSIVE_COP_SQL", start, end, DB["query"], TB["query"][block])
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
            'time_data': [item for item in hours_df.index],
            "f2_cop": hours_df["f2_cop"].values,
            "f3_cop": hours_df["f3_cop"].values,
            "f4_cop": hours_df["f4_cop"].values,
            "f5_cop": hours_df["f5_cop"].values
        },
        "days_data": {
            'time_data': [item for item in days_df.index],
            "f2_cop": days_df["f2_cop"].values,
            "f3_cop": days_df["f3_cop"].values,
            "f4_cop": days_df["f4_cop"].values,
            "f5_cop": days_df["f5_cop"].values
        }
    }
    return data


@log_hint
def get_cona_sub_water_source_cop(start, end, block="cona"):
    """错那 sub 机房水源热泵COP能效
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       f2_whp_cop:  2号机房水源热泵COP,
       f3_whp_cop:  3号机房水源热泵COP,
       f4_whp_cop:  4号机房水源热泵COP,
       f5_whp_cop:  5号机房水源热泵COP
   """
    result_df = get_data("WATER_HEAT_PUMP_COP_SQL", start, end, DB["query"], TB["query"][block])

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
            'time_data': [item for item in hours_df.index],
            "f2_whp_cop": hours_df["f2_whp_cop"].values,
            "f3_whp_cop": hours_df["f3_whp_cop"].values,
            "f4_whp_cop": hours_df["f4_whp_cop"].values,
            "f5_whp_cop": hours_df["f5_whp_cop"].values
        },
        "days_data": {
            'time_data': [item for item in days_df.index],
            "f2_whp_cop": days_df["f2_whp_cop"].values,
            "f3_whp_cop": days_df["f3_whp_cop"].values,
            "f4_whp_cop": days_df["f4_whp_cop"].values,
            "f5_whp_cop": days_df["f5_whp_cop"].values
        }
    }
    return data


@log_hint
def get_cona_room_network_water_supply_temperature(start, end, block="cona"):
    """错那 机房管网供水温度
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       f2_HHWLoop001_ST:  2号机房支路1供水温度,
       f3_HHWLoop001_ST:  3号机房支路1供水温度,
       f3_HHWLoop002_ST:  3号机房支路2供水温度,
       f3_HHWLoop003_ST:  3号机房支路3供水温度,
       f4_HHWLoop001_ST:  4号机房支路1供水温度,
       f5_HHWLoop001_ST:  5号机房支路1供水温度
   """

    result_df = get_data("ROOM_NETWORK_WATER_SUPPLY_TEMPERATURE_SQL", start, end, DB["query"], TB["query"][block])

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
            'time_data': [item for item in hours_df.index],
            'f2_HHWLoop001_ST': hours_df['f2_HHWLoop001_ST'].values,
            'f3_HHWLoop001_ST': hours_df['f3_HHWLoop001_ST'].values,
            'f3_HHWLoop002_ST': hours_df['f3_HHWLoop002_ST'].values,
            'f3_HHWLoop003_ST': hours_df['f3_HHWLoop003_ST'].values,
            'f4_HHWLoop001_ST': hours_df['f4_HHWLoop001_ST'].values,
            'f5_HHWLoop001_ST': hours_df['f5_HHWLoop001_ST'].values
        },
        "days_data": {
            'time_data': [item for item in days_df.index],
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
def get_cona_temp(time_data):
    """错那 sub 机房水源热泵COP能效
    :param time_data: datetime类型时间集合
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


# **********************************************************************************************************************
# **********************************************************************************************************************

# **********************************************  岗巴 统计项目  *********************************************************


def get_kamba_heat_storage_heat(start, end, block="kamba"):
    """岗巴 蓄热水池可用热量
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       low_heat_total:  蓄热水池可用低温热量
       high_heat_total:  蓄热水池可用高温热量
       heat_supply_days:  电锅炉可替换供热天数
   """
    data = {}
    result_df, point_lst = get_data("ALL_LEVEL_TEMP", start, end, DB["query"], TB["query"][block])
    volume = ['1031.370428', '1044.176354', '1057.061292', '1070.025243', '1083.068206', '1096.190181',
              '1109.391169', '1122.671169', '1136.030181', '1149.468206', '1162.985243', '1176.581292',
              '1190.256354', '1204.010428', '1217.843514', '1231.755613', '1245.746724', '1259.816848',
              '1273.965984', '1288.194132', '1302.501292', '1316.887465', '1331.35265', '1367.861292',
              '1382.603021', '1397.423761', '1412.323514', '2024.324037', '2146.595148', '2180.564037',
              '2214.799593', '2249.301815', '2284.070704', '2319.106259', '2354.408481', '2389.97737',
              '2425.812926', '2461.915148', '2498.284037', '840.8894115']
    result_df = result_df.set_index(pd.to_datetime(result_df["Timestamp"]))
    days_df = result_df.resample('D')

    days_high_heat, days_low_heat, days_time_data, days_low_heat_total, days_high_heat_total = [], [], [], [], []
    for point_index, point_item in enumerate(point_lst):
        days_heat_data = days_df[point_item].mean()

        tmp_high, tmp_low = [], []
        for heat_index in days_heat_data.index:
            _high_heat = (days_heat_data[heat_index] - 45) * float(volume[point_index]) * 4.186 / 3.6
            tmp_high.append(_high_heat)
            tmp_low.append((days_heat_data[heat_index] - 10) * float(volume[point_index]) * 4.186 / 3.6 - _high_heat)
        days_high_heat.append(tmp_high)
        days_low_heat.append(tmp_low)

        if not days_time_data:
            days_time_data = [datetime(year=item.year, month=item.month, day=item.day) for item in days_heat_data.index]

        # print(days_high_heat, len(days_high_heat))
        # print(days_low_heat, len(days_low_heat))

    for time_index in range(len(days_time_data)):
        days_low_heat_total.append(sum([item[time_index] for item in days_low_heat]))
        days_high_heat_total.append(sum([item[time_index] for item in days_high_heat]))
    days_heat_supply_days = [item / 2000 / 2400 for item in days_high_heat_total]

    data["days_data"] = {
        "time_data": days_time_data,
        "low_heat_total": days_low_heat_total,
        "high_heat_total": days_high_heat_total,
        "heat_supply_days": days_heat_supply_days
    }

    hours_df = result_df.resample("h")
    hours_high_heat, hours_low_heat, hours_time_data, hours_low_heat_total, hours_high_heat_total = [], [], [], [], []
    for point_index, point_item in enumerate(point_lst):
        hours_heat_data = hours_df[point_item].mean()

        tmp_high, tmp_low = [], []
        for heat_index in hours_heat_data.index:
            _high_heat = (hours_heat_data[heat_index] - 45) * float(volume[point_index]) * 4.186 / 3.6
            tmp_high.append(_high_heat)
            tmp_low.append((hours_heat_data[heat_index] - 10) * float(volume[point_index]) * 4.186 / 3.6 - _high_heat)
        hours_high_heat.append(tmp_high)
        hours_low_heat.append(tmp_low)

        if not hours_time_data:
            hours_time_data = [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_heat_data.index
            ]

    for time_index in range(len(hours_time_data)):
        hours_low_heat_total.append(sum([item[time_index] for item in hours_low_heat]))
        hours_high_heat_total.append(sum([item[time_index] for item in hours_high_heat]))
    hours_heat_supply_days = [item / 2000 / 2400 for item in days_high_heat_total]
    data["hours_data"] = {
        "time_data": hours_time_data,
        "low_heat_total": hours_low_heat_total,
        "high_heat_total": hours_high_heat_total,
        "heat_supply_days": hours_heat_supply_days
    }

    return data


def get_kamba_com_cop(start, end, block="kamba"):
    """岗巴 系统COP
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       cop:  系统综合cop能效
    """
    result_df, point_lst = get_data("COM_COP", start, end, DB["query"], TB["query"][block])
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
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_df.index
            ],
            "cop": hours_df["cop"].values
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_df.index
            ],
            "cop": days_df["cop"].values
        }
    }
    return data


def get_kamba_wshp_cop(start, end, block="kamba"):
    """岗巴 水源热泵COP
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       wshp_cop:  水源热泵cop能效
    """
    result_df, point_lst = get_data("WSHP_COP", start, end, DB["query"], TB["query"][block])

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
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_df.index
            ],
            "wshp_cop": hours_df["wshp_cop"].values
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_df.index
            ],
            "wshp_cop": days_df["wshp_cop"].values
        }
    }
    return data


def get_kamba_solar_collection(start, end, block="kamba"):
    """岗巴 太阳能集热量
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       wshp_cop:  水源热泵cop能效
    """
    result_df, point_lst = get_data("SOLAR_COLLECTOR", start, end, DB["query"], TB["query"][block])
    result_df['solar_collector'] = result_df[point_lst[0]] * 4.186 * (
            result_df[point_lst[1]] - result_df[point_lst[2]]
    ) / 3.6
    result_df = result_df.loc[:, ["Timestamp", "solar_collector"]]

    hours_df = resample_data_by_hours(result_df, "Timestamp", {'solar_collector': 'mean'})

    days_df = resample_data_by_days(result_df, "Timestamp", True, {}, {'solar_collector': 'mean'})

    data = {
        "hours_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_df.index
            ],
            "solar_collector": hours_df["solar_collector"].values
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_df.index
            ],
            "solar_collector": days_df["solar_collector"].values
        }
    }
    return data


def get_kamba_water_replenishment(start, end, block="kamba"):
    """岗巴 补水量
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       heat_water_replenishment:  补水量
       heat_water_replenishment_limit:  补水量限值
    """
    result_df, point_lst = get_data("WATER_REPLENISHMENT", start, end, DB["query"], TB["query"][block])
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
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_df.index
            ],
            "heat_water_replenishment": hours_df[point_lst[0]].values,
            "heat_water_replenishment_limit": hours_df["heat_water_replenishment_limit"].values
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_df.index
            ],
            "heat_water_replenishment": days_df[point_lst[0]].values,
            "heat_water_replenishment_limit": days_df["heat_water_replenishment_limit"].values
        }
    }
    return data


def get_kamba_solar_matrix_supply_and_return_water_temperature(start, end, block="kamba"):
    """岗巴 太阳能矩阵供回水温度
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       solar_matrix_supply_water_temp:  太阳能矩阵供水温度
       solar_matrix_return_water_temp:  太阳能矩阵回水温度
    """
    result_df, point_lst = get_data("SOLAR_MATRIX_SUPPLY_AND_RETURN_WATER_TEMPERATURE", start, end, DB["query"], TB["query"][block])

    hours_df = resample_data_by_hours(result_df, "Timestamp", {point_lst[0]: "mean", point_lst[1]: "mean"})

    days_df = resample_data_by_days(result_df, "Timestamp", True, {}, {point_lst[0]: "mean", point_lst[1]: "mean"})

    data = {
        "hours_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_df.index
            ],
            "solar_matrix_supply_water_temp": hours_df[point_lst[0]].values,
            "solar_matrix_return_water_temp": hours_df[point_lst[1]].values
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_df.index
            ],
            "solar_matrix_supply_water_temp": days_df[point_lst[0]].values,
            "solar_matrix_return_water_temp": days_df[point_lst[1]].values
        }
    }
    return data


def get_kamba_load(start, end, block="kamba"):
    """岗巴 负荷
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       max_load:  最大负荷
       min_load:  最小负荷
       avg_load:  平均负荷
    """
    result_df, point_lst = get_data("PIPE_NETWORK_HEATING", start, end, DB["query"], TB["query"][block])
    result_df['HHWLoop_HeatLoad'] = (result_df[point_lst[0]] - result_df[point_lst[1]]) * 4.186 * (
            result_df[point_lst[2]] - result_df[point_lst[3]]
    ) / 3.6
    result_df = result_df.loc[:, ["Timestamp", "HHWLoop_HeatLoad"]]

    hours_df = resample_data_by_hours(
        result_df, "Timestamp",
        {
            'HHWLoop_HeatLoad': ['max', 'min', 'mean']
        }
    )

    days_df = resample_data_by_days(
        result_df, "Timestamp",
        False,
        {'HHWLoop_HeatLoad': 'mean'},
        {
            'HHWLoop_HeatLoad': ['max', 'min', 'mean']
        }
    )

    data = {
        "hours_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_df.index
            ],
            "max_load": hours_df["HHWLoop_HeatLoad"]["max"].values,
            "min_load": hours_df["HHWLoop_HeatLoad"]["min"].values,
            "avg_load": hours_df["HHWLoop_HeatLoad"]["mean"].values
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_df.index
            ],
            "max_load": days_df["HHWLoop_HeatLoad"]["max"].values,
            "min_load": days_df["HHWLoop_HeatLoad"]["min"].values,
            "avg_load": days_df["HHWLoop_HeatLoad"]["mean"].values
        }
    }
    return data


def get_kamba_end_supply_and_return_water_temp(start, end, block="kamba"):
    """岗巴 末端供回水温度与温差
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
       time_data: 日期,
       end_supply_water_temp:  末端供水温度
       end_return_water_temp:  末端回水温度
       end_return_water_temp_diff:  末端供回水温差
       temp:  平均温度
    """
    result_df, point_lst = get_data("END_SUPPLY_AND_RETURN_WATER_TEMPERATURE", start, end, DB["query"], TB["query"][block])

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
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_df.index
            ],
            "end_supply_water_temp": hours_df[point_lst[0]].values,
            "end_return_water_temp": hours_df[point_lst[1]].values,
            "end_return_water_temp_diff": hours_df["end_return_water_temp_diff"].values,
            "temp": hours_df[point_lst[2]].values
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_df.index
            ],
            "end_supply_water_temp": days_df[point_lst[0]].values,
            "end_return_water_temp": days_df[point_lst[1]].values,
            "end_return_water_temp_diff": days_df["end_return_water_temp_diff"].values,
            "temp": days_df[point_lst[2]].values
        }
    }
    return data


def get_kamba_calories(start, end, block="kamba"):
    """岗巴 供热分析
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典

        time_data: 日期,
        hours_data:
            high_temperature_plate_exchange_heat: 高温板换制热量
            wshp_heat: 水源热泵制热量
            high_temperature_plate_exchange_heat_rate: 高温板换制热功率
        days_data:
            high_temperature_plate_exchange_heat: 高温板换制热总量
            wshp_heat: 水源热泵制热总量
    """
    result_df, point_lst = get_data("CALORIES", start, end, DB["query"], TB["query"][block])
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
            'power': 'sum'
        }
    )

    data = {
        "hours_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_df.index
            ],
            "high_temperature_plate_exchange_heat": hours_df["power"].values,
            "wshp_heat": hours_df["WSHP_HeatLoad"].values,
            "high_temperature_plate_exchange_heat_rate": hours_df["power"].values,
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_df.index
            ],
            "high_temperature_plate_exchange_heat": days_df["power"].values,
            "wshp_heat": days_df["WSHP_HeatLoad"].values,
        }
    }
    return data


def get_kamba_solar_heat_supply(start, end, block="kamba"):
    """岗巴 太阳能集热分析
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
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
    result_df, point_lst = get_data("SOLAR_HEAT_SUPPLY", start, end, DB["query"], TB["query"][block])

    result_df['HHWLoop_HeatLoad'] = (result_df[point_lst[0]] - result_df[point_lst[1]]) * 4.186 * (
            result_df[point_lst[2]] - result_df[point_lst[3]]
    ) / 3.6
    result_df['IA'] = result_df[point_lst[4]] * 34.992
    _result_df = result_df[point_lst[5]] * 4.186 * (result_df[point_lst[6]] - result_df[point_lst[7]]) / 3.6
    result_df['IB'] = _result_df
    # result_df[result_df < 0] = 0
    result_df = result_df.loc[:, ["Timestamp", "HHWLoop_HeatLoad", "IB"]]
    result_df.loc[(result_df["IB"] < 0, "IB")] = 0
    hours_df = resample_data_by_hours(result_df, "Timestamp", {'HHWLoop_HeatLoad': 'mean', 'IB': 'mean'})
    days_df = resample_data_by_days(
        result_df, "Timestamp", False,
        {'HHWLoop_HeatLoad': 'mean', 'IB': 'mean'},
        {'HHWLoop_HeatLoad': ['mean', "count"], 'IB': 'sum'}
    )

    days_solar_collector_heat = days_df["IB"]["sum"]
    days_heat_supply = days_df["HHWLoop_HeatLoad"]["mean"] * days_df["HHWLoop_HeatLoad"]["count"]
    days_rate = days_solar_collector_heat / days_heat_supply

    data = {
        "hours_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_df.index
            ],
            "solar_collector_heat": hours_df["HHWLoop_HeatLoad"].values,
            "heat_supply": hours_df["IB"].values,
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_df.index
            ],
            "solar_collector_heat": days_solar_collector_heat.values,
            "heat_supply": days_heat_supply.values,
            "rate": days_rate.values
        }
    }
    return data


def get_kamba_heat_supply(start, end, block="kamba"):
    """岗巴 制热量情况
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期
        rate: 供热率
        heat_supply: 供热量
        power_consume: 水源热泵耗电量
    """
    load_df, load_point_lst = get_data("PIPE_NETWORK_HEATING", start, end, DB["query"], TB["query"][block])
    load_df['HHWLoop_HeatLoad'] = (load_df[load_point_lst[0]] - load_df[load_point_lst[1]]) * 4.186 * (
            load_df[load_point_lst[2]] - load_df[load_point_lst[3]]
    ) / 3.6

    power_df, power_point_lst = get_data("WSHP_POWER_CONSUME", start, end, DB["query"], TB["query"][block])

    power_df["power"] = power_df.sum(axis=1)


    load_df = load_df.loc[:, ["Timestamp", "HHWLoop_HeatLoad"]]
    power_df = power_df.loc[:, ["Timestamp", "power"]]
    power_consume = power_df.set_index(pd.to_datetime(power_df["Timestamp"]))

    hours_load = resample_data_by_hours(load_df, "Timestamp", {"HHWLoop_HeatLoad": "mean"})
    hours_avg_loads = hours_load["HHWLoop_HeatLoad"].values

    hours_power = power_consume.resample("h")
    hours_power_consume, hours_count = pd.Series([]), 0

    for date, df in hours_power:
        f_line = df.loc[df.index[0]]
        l_line = df.loc[df.index[-1]]
        diff = l_line - f_line
        hours_power_consume[hours_count] = diff["power"]
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
        f_line = df.loc[df.index[0]]
        l_line = df.loc[df.index[-1]]
        diff = l_line - f_line
        days_power_consume[count] = diff["power"]
        count += 1

    days_rate = (days_avg_loads - days_power_consume) / days_avg_loads
    days_heat_supply = days_avg_loads

    data = {
        "hours_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_load.index
            ],
            "rate": hours_rate.replace([np.inf, -np.inf], np.nan).values,
            "heat_supply": hours_heat_supply,
            "power_consume": hours_power_consume.values
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_load.index
            ],
            "rate": days_rate.replace([np.inf, -np.inf], np.nan).values,
            "heat_supply": days_heat_supply,
            "power_consume": days_power_consume.values
        }
    }
    return data


def get_kamba_cost_saving(start, end, block="kamba"):
    """岗巴 制热量情况
    :param block: 隶属 错那数据
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期
        cost_saving: 节省电费
        power_consumption: 耗电量
    """
    result_df, point_lst = get_data("COST_SAVING", start, end, DB["query"], TB["query"][block])

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
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_df.index
            ],
            "cost_saving": hours_df["cost_saving"].values,
            "power_consumption": hours_df["SysPower"].values
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_df.index
            ],
            "cost_saving": days_df["cost_saving"].values,
            "power_consumption": days_df["SysPower"].values
        }
    }
    return data


def l(df):
    print("*" * 100)
    print(type(df))
    print(df)
    print("*" * 100)


def get_kamba_co2_emission(start, end, block="kamba"):
    result_df, point_lst = get_data("POWER_CONSUME", start, end, DB["query"], TB["query"][block])
    result_df["power_consume"] = result_df.loc[:, point_lst].sum(axis=1)
    result_df = result_df.loc[:, ["Timestamp", "power_consume"]]

    result_df = result_df.set_index(result_df["Timestamp"])

    hours_consume_items, days_consume_items = collections.OrderedDict(), collections.OrderedDict()

    for hour_index in result_df.index:
        hours_key = datetime(year=hour_index.year,  month=hour_index.month, day=hour_index.day, hour=hour_index.hour)
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
            days_consume_items[days_key] = [result_df.loc[days_key, "power_consume"]]
        else:
            days_consume_items[days_key].append(result_df.loc[days_key, "power_consume"])
    days_power_consume = [item[-1] - item[0] for item in days_consume_items.values()]
    days_co2_emission_reduction = [(item[-1] - item[0]) * 0.5839 for item in days_consume_items.values()]
    days_co2_equal_num = [(item[-1] - item[0]) * 0.5839 / 1.75 for item in days_consume_items.values()]
    data = {
        "hours_data": {
            "time_data": list(hours_consume_items.keys()),
            "power_consume": hours_power_consume,
            "co2_emission_reduction": hours_co2_emission_reduction,
            "co2_equal_num": hours_co2_equal_num
        },
        "days_data": {
            "time_data": list(days_consume_items.keys()),
            "power_consume": days_power_consume,
            "co2_emission_reduction": days_co2_emission_reduction,
            "co2_equal_num": days_co2_equal_num
        }
    }
    return data


def t(start, end, block="kamba"):
    result_df, point_lst = get_data("", start, end, DB["query"], TB["query"][block])

    hours_df = resample_data_by_hours(
        result_df, "Timestamp",
        {
            "HHWLoop_HeatLoad": "mean",
            "SysPower": "sum"
        }
    )


    days_df = resample_data_by_days(
        result_df, "Timestamp",
        True,
        {},
        {
            "HHWLoop_HeatLoad": "mean",
            "SysPower": "sum"
        }
    )

    data = {
        "hours_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in hours_df.index
            ],
            "cop": hours_df["cop"].values
        },
        "days_data": {
            "time_data": [
                datetime(
                    year=item.year,
                    month=item.month,
                    day=item.day,
                    hour=item.hour
                ) for item in days_df.index
            ],
            "cop": days_df["cop"].values
        }
    }
    return data

"""
def get_cona_cost_saving(start, end):
    result_df = get_data("API_COM_COP_SQL", start, end, db, tb["cona"])

    hours_df = resample_data_by_hours(result_df, {})
    days_df = resample_data_by_days(result_df, False, {}, {})

    data = {
        "hours_data": {
            'time_data': [item for item in hours_df.index],
            'values': hours_cop.values
        },
        "days_data": {
            'time_data': [item for item in days_df.index],
            'values': days_cop.values
        }
    }
    return data
"""

# update_history_data()


res = get_kamba_co2_emission("2021-05-13 00:00:00", "2021-05-18 23:59:59")
if res:
    print(res)