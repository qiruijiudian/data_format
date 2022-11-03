# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/10/21 14:39
# @Author  : MAYA
import collections
import numpy as np
from datetime import datetime, timedelta
from settings import REPORT_DB, VOLUME, TIME_BEGIN
from tools import get_report_data, resample_data_by_hours, resample_data_by_days, handing_missing_data, \
    get_dtype, get_time_in_datetime, log_hint, get_custom_conn, get_sql_conf, log_or_print_without_obj, \
    convert_str_2_datetime
import pandas as pd
import os
# ---------------------------------------------- Common  ----------------------------------------------------------------


# ---------------------------------------------- 岗巴  ----------------------------------------------------------------
@log_hint
def get_kamba_solar_thermal_data(start, end, block="kamba", print_mode=False, log_mode=False):

    result_df, point_lst = get_report_data("SOLAR_HEAT_SUPPLY", block, start, end)
    result_df['HHWLoop_HeatLoad'] = (result_df[point_lst[0]] - result_df[point_lst[1]]) * 4.186 * (
            result_df[point_lst[2]] - result_df[point_lst[3]]
    ) / 3.6
    result_df['IA'] = result_df[point_lst[4]] * 34.992
    _result_df = result_df[point_lst[5]] * 4.186 * (result_df[point_lst[6]] - result_df[point_lst[7]]) / 3.6
    result_df['IB'] = _result_df
    result_df["collector_system_flow_rate"] = result_df[point_lst[5]]

    result_df.loc[(result_df["IB"] < 0, "IB")] = 0

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

    days_solar_collector_heat = days_df["IB"]["sum"]    # 太阳能逐日集热量/太阳能集热量(IB)
    days_available_solar = days_df["IA"]["sum"]    # 逐日可用太阳能 IA

    days_heat_collection_efficiency = days_df["IB"]["sum"] / days_df["IA"]["sum"]   # 逐日太阳能集热效率

    data = {
        "time_data": get_time_in_datetime(days_df, "d"),
        "solar_collector": days_solar_collector_heat.values,
        "heat_collection_efficiency": days_heat_collection_efficiency.values,
        "available_solar": days_available_solar.values

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
    result_df, point_lst = get_report_data("CALORIES", block, start, end)
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
        "time_data": get_time_in_datetime(days_df, "d"),
        "high_temperature_plate_exchange_heat": days_df["power"]["sum"].values,
        "wshp_heat": days_df["WSHP_HeatLoad"]["sum"].values,
    }
    return data


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
    result_df, point_lst = get_report_data("ALL_LEVEL_TEMP", block, start, end)
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

    data = {"time_data": days_time_data, "sum_heat_of_storage": days_sum_heat_of_storage.values}

    return data


@log_hint
def get_kamba_pool_heat_data(start, end, block="kamba", print_mode=False, log_mode=False):
    """
    水池输入热量、水池输出热量、水池存储热量、损失热量
    :param start:
    :param end:
    :param block:
    :return:
    """

    result_df, point_lst = get_report_data("POOL_HEAT", block, start, end)
    # 集热量 = max(H5 * 4.186 * (CT5-CS5)/3.6, 0)
    result_df["solar_heat"] = result_df["SolarRFM_0201"] * 4.186 * (
            result_df["SolarHWLoop_RT"] - result_df["SolarHWLoop_ST"]
    ) / 3.6 / 4
    result_df["WSHP_HeatLoad"] = 4.186 * (
            result_df["WSHP001_HHWF"] * (result_df["WSHP001_HHWLT"] - result_df["WSHP001_HHWET"]) +
            result_df["WSHP002_HHWF"] * (result_df["WSHP002_HHWLT"] - result_df["WSHP002_HHWET"]) +
            result_df["WSHP003_HHWF"] * (result_df["WSHP003_HHWLT"] - result_df["WSHP003_HHWET"]) +
            result_df["WSHP004_HHWF"] * (result_df["WSHP004_HHWLT"] - result_df["WSHP004_HHWET"]) +
            result_df["WSHP005_HHWF"] * (result_df["WSHP005_HHWLT"] - result_df["WSHP005_HHWET"]) +
            result_df["WSHP006_HHWF"] * (result_df["WSHP006_HHWLT"] - result_df["WSHP006_HHWET"])
    ) / 4

    result_df['power'] = 4.186 * (
            result_df["HHWLoop_RFlow"] - result_df["WSHP001_HHWF"] - result_df["WSHP002_HHWF"] -
            result_df["WSHP004_HHWF"] - result_df["WSHP005_HHWF"] - result_df["WSHP006_HHWF"]
    ) * ((result_df["Pit_DisH_HX101_SLT"] + result_df["Pit_DisH_HX102_SLT"]) / 2 - result_df["HHWLoop_RT"]) / 3.6 / 4

    """
    JA:   CTF001_HZ
    JB:   CTF002_HZ
    AK:   Pit_ChargeP801_HZ
    AL:   Pit_ChargeP802_HZ
    AM:   Pit_ChargeP803_HZ
    """

    result_df["tower_heat_dissipation"] = result_df.apply(
        lambda x: x["solar_heat"] - x["power"] - x["WSHP_HeatLoad"] if
        ((x["CTF001_HZ"] > 5) or (x["CTF002_HZ"] > 5)) and
        (
                x["Pit_ChargeP801_HZ"] < 5 and x["Pit_ChargeP802_HZ"] < 5 and x["Pit_ChargeP803_HZ"] < 5
        ) else 0, axis=1
    )

    result_df["heat_loss"] = result_df.apply(
        lambda x: x["solar_heat"] - x["power"] - x["WSHP_HeatLoad"] if
        ((x["CTF001_HZ"] < 5) and (x["CTF002_HZ"] < 5)) and
        (
                (x["Pit_ChargeP801_HZ"] > 5) or
                (x["Pit_ChargeP802_HZ"] > 5) or
                (x["Pit_ChargeP803_HZ"] > 5)
        ) else 0, axis=1
    )
    #
    result_df["heat_output"] = result_df["WSHP_HeatLoad"] + result_df["power"]
    result_df["heat_input"] = result_df["heat_output"] + result_df["heat_loss"]
    result_df = result_df.loc[:, ["Timestamp", "tower_heat_dissipation", "heat_loss", "heat_output", "heat_input"]]
    result_df.to_csv("data.csv")

    result_df = resample_data_by_days(
        result_df, "Timestamp", just_date=True, hours_op_dic=None, days_op_dic={
            "tower_heat_dissipation": "sum",
            "heat_loss": "sum",
            "heat_output": "sum",
            "heat_input": "sum",
        }
    )

    return {

        "time_data": get_time_in_datetime(result_df, "d"),
        "pool_heat_input": result_df["heat_input"].values,
        "pool_heat_output": result_df["heat_output"].values,
        "pool_heat_loss": result_df["heat_loss"].values,
        "tower_heat_dissipation": result_df["tower_heat_dissipation"].values,
    }


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
    result_df, point_lst = get_report_data("COST_SAVING", block, start, end)

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
    sys_power = tmp_df.diff()
    t_start = convert_str_2_datetime(start)
    t_begin = convert_str_2_datetime(TIME_BEGIN["kamba"])

    if t_start > t_begin:
        q_start = t_start - timedelta(hours=1)
        q_end = t_start - timedelta(seconds=1)
        q_df, q_lst = get_report_data("COST_SAVING", block, q_start, q_end)
        prev_value = q_df.loc[:, [data for data in q_lst[27:]]].sum(axis=1).iloc[-1]
        sys_power.iloc[0] = tmp_df.iloc[0] - prev_value
    else:
        sys_power.iloc[0] = sys_power.iloc[1]

    result_df = pd.concat(
        [
            result_df.loc[:, ['Timestamp', 'WSHP_HeatLoad']],
            result_df[['power']],
            sys_power.to_frame(name="SysPower")
        ], axis=1
    )

    days_df = resample_data_by_days(
        result_df, "Timestamp",
        False,
        {
            'SysPower': 'sum',
            'WSHP_HeatLoad': 'mean',
            'power': 'mean'
        },
        {
            'SysPower': 'sum',
            'WSHP_HeatLoad': 'sum',
            'power': 'sum'
        }
    )

    days_df["cost_saving"] = (days_df["power"] + days_df["WSHP_HeatLoad"] - days_df["SysPower"]) * 0.45

    data = {

        "time_data": get_time_in_datetime(days_df, "d"),
        "cost_saving": days_df["cost_saving"].values,
        "power_consumption": days_df["SysPower"].values,
    }
    return data


@log_hint
def get_kamba_heat_supply(start, end, block="kamba", print_mode=False, log_mode=False):

    load_df, load_point_lst = get_report_data("PIPE_NETWORK_HEATING", block, start, end)
    load_df['HHWLoop_HeatLoad'] = (load_df[load_point_lst[0]] - load_df[load_point_lst[1]]) * 4.186 * (
            load_df[load_point_lst[2]] - load_df[load_point_lst[3]]
    ) / 3.6

    load_df = load_df.loc[:, ["Timestamp", "HHWLoop_HeatLoad"]]

    days_load = resample_data_by_days(
        load_df, "Timestamp", False, {"HHWLoop_HeatLoad": "mean"}, {"HHWLoop_HeatLoad": "sum"}
    )

    data = {"time_data": get_time_in_datetime(days_load, "d"), "heat_supply": days_load["HHWLoop_HeatLoad"].values}
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
    result_df, point_lst = get_report_data("COM_COP", block, start, end)
    result_df['HHWLoop_HeatLoad'] = (result_df[point_lst[0]] - result_df[point_lst[1]]) * 4.186 * (
            result_df[point_lst[2]] - result_df[point_lst[3]]
    ) / 3.6

    tmp_df = result_df.loc[:, [data for data in point_lst[4:]]].sum(axis=1)
    sys_power = tmp_df.diff()
    t_start = convert_str_2_datetime(start)
    t_begin = convert_str_2_datetime(TIME_BEGIN["kamba"])

    if t_start > t_begin:
        q_start = t_start - timedelta(hours=1)
        q_end = t_start - timedelta(seconds=1)
        q_df, q_lst = get_report_data("COM_COP", block, q_start, q_end)
        prev_value = q_df.loc[:, [data for data in q_lst[4:]]].sum(axis=1).iloc[-1]
        sys_power.iloc[0] = tmp_df.iloc[0] - prev_value
    else:
        sys_power.iloc[0] = sys_power.iloc[1]

    result_df = pd.concat([result_df.loc[:, ['Timestamp', 'HHWLoop_HeatLoad']], sys_power.to_frame(name='SysPower')],
                          axis=1)

    result_df = result_df.loc[:, ["Timestamp", "HHWLoop_HeatLoad", "SysPower"]]

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
        "time_data": get_time_in_datetime(days_df, "d"),
        "cop": days_df["cop"].values

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
    result_df, point_lst = get_report_data("COST_SAVING", block, start, end)
    result_df = result_df.set_index("Timestamp", drop=True)

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
    sys_power = tmp_df.diff()
    t_start = convert_str_2_datetime(start)
    t_begin = convert_str_2_datetime(TIME_BEGIN["kamba"])

    if t_start > t_begin:
        q_start = t_start - timedelta(hours=1)
        q_end = t_start - timedelta(seconds=1)
        q_df, q_lst = get_report_data("COST_SAVING", block, q_start, q_end)
        prev_value = q_df.loc[:, [data for data in q_lst[27:]]].sum(axis=1).iloc[-1]
        sys_power.iloc[0] = tmp_df.iloc[0] - prev_value
    else:
        sys_power.iloc[0] = sys_power.iloc[1]

    result_df = pd.concat(
        [
            result_df.reset_index().loc[:, ['Timestamp', 'WSHP_HeatLoad']],
            result_df[['power']],
            sys_power.to_frame(name="SysPower")
        ], axis=1
    )

    days_df = resample_data_by_days(
        result_df, "Timestamp",
        False,
        {
            'SysPower': 'sum',
            'WSHP_HeatLoad': 'mean',
            'power': 'mean'
        },
        {
            'SysPower': 'sum',
            'WSHP_HeatLoad': 'sum',
            'power': 'sum'
        }
    )

    days_co2_emission_reduction = (days_df["power"] + days_df["WSHP_HeatLoad"] - days_df["SysPower"]) * 0.5839

    data = {
        "time_data": get_time_in_datetime(days_df, "d"),
        "co2_emission_reduction": days_co2_emission_reduction.values,
    }
    return data


def store_report_data(start, end, block, print_mode=False, log_mode=False):
    res = {}
    if block == "kamba":
        for _func in [
            get_kamba_solar_thermal_data, get_kamba_calories, get_kamba_heat_storage_heat, get_kamba_pool_heat_data,
            get_kamba_cost_saving, get_kamba_heat_supply, get_kamba_com_cop, get_kamba_co2_emission
        ]:
            res.update(_func(start, end, block, print_mode=print_mode, log_mode=log_mode))
    df = pd.DataFrame(res)
    df = df.replace([np.inf, -np.inf], np.nan)
    store_conn = get_custom_conn(get_sql_conf(REPORT_DB["store"]))
    backup_conn = get_custom_conn(get_sql_conf(REPORT_DB["backup"]))
    try:
        store_df = df.melt(id_vars="time_data", var_name="pointname")
        log_or_print_without_obj("报表数据(长格式存储) 开始上传", print_mode, log_mode)
        store_df.to_sql(name=block, con=store_conn, if_exists="append", index=False, dtype=get_dtype(store_df.keys()))
        log_or_print_without_obj("报表数据(长格式存储) 存储完成", print_mode, log_mode)

        log_or_print_without_obj("报表数据(宽格式存储) 开始上传", print_mode, log_mode)
        df.to_sql(name=block, con=backup_conn, if_exists="append", index=False, dtype=get_dtype(df.keys()))
        log_or_print_without_obj("报表数据(宽格式存储) 存储完成", print_mode, log_mode)

    finally:
        store_conn.dispose()
        backup_conn.dispose()


def backup_report_data(block, backup_path, print_mode=False, log_mode=False):

    if not os.path.exists(backup_path):
        os.makedirs(backup_path)

    report_sql_conf = get_sql_conf(REPORT_DB["store"])
    report_wide_sql_conf = get_sql_conf(REPORT_DB["backup"])

    tables = [block]

    report_now, report_num = datetime.today().strftime("%Y%m%d"), 1
    report_name = os.path.join(backup_path, "{}_{}.sql".format(block, report_now))

    while os.path.exists(report_name):
        report_num += 1
        report_name = os.path.join(backup_path, "{}_{}[{}].sql".format(block, report_now, report_num))

    report_wide_now, report_wide_num = datetime.today().strftime("%Y%m%d"), 1
    report_wide_name = os.path.join(backup_path, "{}_wide_{}.sql".format(block, report_wide_now))

    while os.path.exists(report_wide_name):
        report_wide_num += 1
        report_wide_name = os.path.join(backup_path, "{}_wide_{}[{}].sql".format(block, report_wide_now, report_wide_num))

    report_backup_sql = "mysqldump -u{} -p{} {} {} > {}".format(
        report_sql_conf["user"],
        report_sql_conf["password"],
        report_sql_conf["database"],
        " ".join(tables),
        report_name
    )

    report_wide_backup_sql = "mysqldump -u{} -p{} {} {} > {}".format(
        report_wide_sql_conf["user"],
        report_wide_sql_conf["password"],
        report_wide_sql_conf["database"],
        " ".join(tables),
        report_wide_name
    )
    os.system(report_backup_sql)
    log_or_print_without_obj(
        "数据备份已完成 文件名：{}, 时间：{}".format(
            report_name, datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        ), print_mode, log_mode
    )

    os.system(report_wide_backup_sql)
    log_or_print_without_obj(
        "数据备份已完成 文件名：{}, 时间：{}".format(
            report_wide_name, datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        ), print_mode, log_mode
    )


# get_kamba_pool_heat_data("2021/05/11", "2021/07/11")
# data = {}
# for item in [
# get_kamba_solar_thermal_data,
# get_kamba_calories,
# get_kamba_heat_storage_heat,
# get_kamba_pool_heat_data,
# get_kamba_cost_saving,
# get_kamba_heat_supply,
# get_kamba_com_cop,
# get_kamba_co2_emission,
# store_report_data,
# backup_report_data
# ]:
#     data.update(item("2021/10/01 00:00:00", "2021/10/30 23:59:59"))
#     print()


# store_report_data("2021/10/01 00:00:00", "2021/10/03 23:59:59", "kamba")
# get_kamba_co2_emission("2021/10/01 00:00:00", "2021/10/03 23:59:59")