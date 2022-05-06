# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/5 17:09
# @Author  : MAYA
import random
import pymysql
import platform
import pandas as pd
from tools import resample_data_by_hours, resample_data_by_days, SQL_CONTEXT

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



db = "data_center_original"
tb = {
    "cona": "cona",
    "kamba": "kamba"
}

# **********************************************  错那 统计项目  *********************************************************


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
            "database": db,
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

        # df = pd.read_sql(sql, con=conn)
        result_df = pd.read_sql(sql, con=conn).pivot(
            index='time', columns='pointname', values='value'
        )
        return result_df.reset_index()


def get_cona_geothermal_wells_heat_provide(start, end):
    """错那 地热井提供热量（高温版换制热量、水源热泵制热量、地热井可提供高温热量、地热井可提供低温热量）
    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        high_temp_plate_exchange_heat_production': 高温版换制热量,
        water_heat_pump_heat_production: 水源热泵制热量,
        geothermal_wells_high_heat_provide: 地热井可提供高温热量,
        geothermal_wells_low_heat_provide: 地热井可提供低温热量
    """
    result_df = get_data("API_GEOTHERMAL_WELLS_HEAT_PROVIDE_SQL", start, end, db, tb["cona"])
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


def get_cona_com_cop(start, end):
    """错那 综合COP

    :param start: 开始时间
    :param end: 结束时间
    :return: 包含时数据和日数据的字典
        time_data: 日期,
        values: 值集合,

    """
    result_df = get_data("API_COM_COP_SQL", start, end, db, tb["cona"])
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
            'values': hours_cop.values
        },
        "days_data": {
            'time_data': [item for item in days_df.index],
            'values': days_cop.values
        }
    }
    return data


def get_cona_cost_saving(start, end):
    result_df = get_data("API_COST_SAVING_SQL", start, end, db, tb["cona"])

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

    print(hours_df)
    print("*" * 100)
    print(days_df)
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

print(
    get_cona_cost_saving("2021-02-01 00:00:00", "2021-02-07 23:59:59")
)
