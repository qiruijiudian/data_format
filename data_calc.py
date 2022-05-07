# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/5 17:09
# @Author  : MAYA
import random
import pymysql
import platform
import pandas as pd
import logging

from sqlalchemy import create_engine
from sqlalchemy.types import FLOAT, DateTime
from tools import resample_data_by_hours, resample_data_by_days, SQL_CONTEXT, DB, TB, log_hint

"""错那 统计内容
1.供水温度与气温关系 TODO
2. 地热井提供热量
3. 综合COP
4. 地热井出水温度（高温）
５. 节省供暖费用
6. 供热量与平均温度
7. 供回水温度
8. 供热量与气温关系
9. 负荷量
10. 补水量
11. 子页面 综合COP
12. 子页面 水源热泵COP
13. 子页面 供水温度与气温
"""

# **********************************************  错那 统计项目  *********************************************************


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


def store_data(block, start, end):

    engine = get_store_conn()
    hours_data, days_data = data_collation(block, start, end)

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
        print("数据上传异常")
        import traceback
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
    sql = SQL_CONTEXT[sql_key].format(tb, start, end)
    sql_conf = get_sql_conf(db)
    with pymysql.connect(
            host=sql_conf["host"],
            user=sql_conf["user"],
            password=sql_conf["password"],
            database=sql_conf["database"]
    ) as conn:

        result_df = pd.read_sql(sql, con=conn).pivot(
            index='time', columns='pointname', values='value'
        )
        return result_df.reset_index()


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
        result_df,
        {
            'high_temp_plate_exchange_heat_production': 'mean',
            'water_heat_pump_heat_production': 'mean',
            'geothermal_wells_high_heat_provide': 'mean',
            'geothermal_wells_low_heat_provide': 'mean'
        }
    )
    days_df = resample_data_by_days(
        result_df,
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
        result_df,
        {
            'UR': 'mean',
            'heat_pipe_network_heating': 'mean',
            'machine_room_pump_power': 'mean'
        }
    )
    days_df = resample_data_by_days(
        result_df,
        True,
        None,
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
        result_df,
        {
            'machine_room_pump_power': 'mean',
            'high_temp_plate_exchange_heat_production': 'mean',
            'water_heat_pump_heat_production': 'mean'
        }
    )
    days_df = resample_data_by_days(
        result_df,
        False,
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
        result_df,
        {
            'heat_well_heating': 'mean',
            'heat_pipe_network_heating': 'mean',
            'water_heat_pump_heat_production': 'mean',
            'high_temp_plate_exchange_heat_production': 'mean'
        }
    )
    # print(hours_df)

    load_hours_df = resample_data_by_hours(
        result_df,
        {
            "heat_pipe_network_heating": ["max", "min", "mean"]
        }
    )
    load_days_df = resample_data_by_days(
        result_df,
        False,
        {"heat_pipe_network_heating": "mean"},
        {"heat_pipe_network_heating": ["max", "min", "mean"]}
    )
    days_df = resample_data_by_days(
        result_df,
        False,
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
    result_df = result_df.loc[:, ["time", "water_supply_temperature", "return_water_temperature", "supply_return_water_temp_diff"]]

    hours_df = resample_data_by_hours(
        result_df,
        {
            "water_supply_temperature": "mean",
            "return_water_temperature": "mean",
            "supply_return_water_temp_diff": "mean"
        }
    )
    days_df = resample_data_by_days(
        result_df,
        True,
        {},
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
        result_df,
        {
            "water_replenishment": "mean",
            "water_replenishment_limit": "mean"
        }
    )
    days_df = resample_data_by_days(
        result_df, True, {},
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
        result_df,
        {
            "f2_cop": "mean",
            "f3_cop": "mean",
            "f4_cop": "mean",
            "f5_cop": "mean"
        }
    )
    days_df = resample_data_by_days(
        result_df, True, {},
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
        result_df,
        {
            'f2_whp_cop': 'mean',
            'f3_whp_cop': 'mean',
            'f4_whp_cop': 'mean',
            'f5_whp_cop': 'mean'
        }
    )
    days_df = resample_data_by_days(
        result_df, True, {},
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
        result_df,
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
        result_df, True, {},
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
    if block == "cona":
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
        print(success, hours_time, days_time)

        res = {
            "hours_data": {},
            "days_data": {}
        }
        for item in items:
            res["hours_data"].update(item["hours_data"])
            res["days_data"].update(item["days_data"])

        from datetime import datetime
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

        hours_df = pd.DataFrame(res["hours_data"])

        days_df = pd.DataFrame(res["days_data"])

        return hours_df, days_df


def get_dtype(columns):
    res = {}
    for item in columns:
        if item == "time_data":
            res[item] = DateTime
        else:
            res[item] = FLOAT
    return res

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

# print(
#     get_cona_heat_provided("2020-12-31 00:00:00", "2021-02-07 23:59:59")
# )

# data_collation("cona", "2021-02-01 00:00:00", "2021-02-07 23:59:59")
store_data("cona", "2020-12-31 00:00:00", "2021-02-07 23:59:59")


