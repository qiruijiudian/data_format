# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/5 17:09
# @Author  : MAYA
import os
import random
import pymysql
import pandas as pd
import logging
import traceback
import numpy as np
from tools import resample_data_by_hours, resample_data_by_days, DB, TB, log_hint, check_time, \
    get_dtype, get_data, get_data_range, get_store_conn, get_sql_conf, VOLUME, get_custom_conn, log_or_print
from sqlalchemy.dialects.mysql import DATETIME, DOUBLE, VARCHAR

from datetime import datetime, timedelta
import collections


# ************************************************  公共函数  ************************************************************

class DataCalc:
    def __init__(self, block, print_mode, log_mode):
        self.block = block
        self.print_mode = print_mode
        self.log_mode = log_mode

    def get_data_range(self, key):
        print("执行")
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
                print(k, v)

                cur1.execute(
                    "select {} from {} order by {} asc limit 1".format(
                        TB["query"][self.block]["time_index"],
                        TB["query"][self.block]["table"],
                        TB["query"][self.block]["time_index"]
                    )
                )

                start = cur1.fetchone()

                cur1.execute(
                    "select {} from {} order by {} asc limit 1".format(
                        TB["query"][self.block]["time_index"],
                        TB["query"][self.block]["table"],
                        TB["query"][self.block]["time_index"]
                    )
                )
                end = cur1.fetchone()

                res[k] = {
                    "start": start, "end": end
                }



                # cur1.execute(
                #     "select {} from {} order by {} asc".format(
                #         TB["query"][self.block]["time_index"],
                #         TB["query"][self.block]["table"],
                #         TB["query"][self.block]["time_index"]
                #     )
                # )
                # all_items = cur1.fetchall()
                # if len(all_items) < 2:
                #     res[k] = {"start": "", "end": ""}
                # else:
                #     res[k] = {"start": all_items[0][0], "end": all_items[-1][0]}
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
            context2 = {k: v["hours"] if isinstance(v, dict) else "tianjin_commons_data" for k, v in TB["store"].items()}

            for k, v in context2.items():
                cur2.execute("select time_data from {} order by time_data desc limit 1;".format(v))
                item = cur2.fetchone()
                item = "" if not len(item) else item[0]
                res[k]["latest"] = item
            cur2.close()
        return res

    def data_collation(self):
        res = {"hours_data": {}, "days_data": {}}
        log_or_print(self, "{} 数据获取 开始".format(self.block))
        if self.block == "cona":

            pass
        elif self.block == "kamba":
            pass

        else:
            pass


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


def data_collation(block, start, end):
    res = {
        "hours_data": {},
        "days_data": {}
    }

    print("{} 数据获取 开始".format(block))
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
    elif block == "kamba":
        heat_storage_heat = get_kamba_heat_storage_heat(start, end)
        com_cop = get_kamba_com_cop(start, end)
        wshp_cop = get_kamba_wshp_cop(start, end)
        water_replenishment = get_kamba_water_replenishment(start, end)
        solar_matrix_supply_and_return_water_temperature = get_kamba_solar_matrix_supply_and_return_water_temperature(
            start, end)
        load = get_kamba_load(start, end)
        end_supply_and_return_water_temp = get_kamba_end_supply_and_return_water_temp(start, end)
        calories = get_kamba_calories(start, end)
        solar_heat_supply = get_kamba_solar_heat_supply(start, end)
        heat_supply = get_kamba_heat_supply(start, end)
        cost_saving = get_kamba_cost_saving(start, end)
        co2_emission = get_kamba_co2_emission(start, end)
        pool_temperature = get_kamba_pool_temperature(start, end)

        res["pool_data"] = pool_temperature

        items = [
            heat_storage_heat, com_cop, wshp_cop, water_replenishment,
            solar_matrix_supply_and_return_water_temperature, load, end_supply_and_return_water_temp, calories,
            solar_heat_supply, heat_supply, cost_saving, co2_emission
        ]
        success, hours_time, days_time = check_time(items)

        # for item in items:
        #     for key in ["hours_data", "days_data"]:
        #         print("{} start".format(key))
        #         for k, v in item[key].items():
        #             print(k, len(v))
        #         print("{} end".format(key))
        #     print("*" * 100)

        if success:

            for item in items:
                for key in ["hours_data", "days_data"]:
                    if item.get(key):
                        res[key].update(item[key])

        else:
            print("数据获取异常")
            exit()
    elif block == "tianjin":
        fan_frequency = get_fan_frequency(start, end)
        cold_water_valve = get_cold_water_valve(start, end)
        hot_water_valve = get_hot_water_valve(start, end)
        air_supply_pressure = get_air_supply_pressure(start, end)
        air_supply_humidity = get_air_supply_humidity(start, end)
        air_supply_temperature = get_air_supply_temperature(start, end)
        air_temperature_and_humidity = get_temperature_and_humidity(start, end)
        items = [
            fan_frequency,
            cold_water_valve,
            hot_water_valve,
            air_supply_pressure,
            air_supply_humidity,
            air_supply_temperature, air_temperature_and_humidity
        ]
        res = {}
        for item in items:
            res.update(item)
    print("{} 数据获取 完成".format(block))
    return res


def update_history_data(blocks=None):
    """全部历史数据更新"""
    if not blocks:
        blocks = ["cona", "kamba", "tianjin"]

    data_range = get_data_range("history")
    for block in blocks:
        start = "{} 00:00:00".format(data_range[block]["start"].strftime("%Y-%m-%d"))
        end = "{} 23:59:59".format(data_range[block]["end"].strftime("%Y-%m-%d"))
        # TODO
        # start, end = "2022-05-01 00:00:00", "2022-06-01 23:59:59"
        if block == "cona":
            items = data_collation(block, start, end)
            store_data(block, items)

        elif block == "kamba":
            items = data_collation(block, start, end)
            pool_data = items.pop("pool_data")
            store_data(block, items)

            hours_pool_df = pd.DataFrame(pool_data["hours_data"])
            days_pool_df = pd.DataFrame(pool_data["days_data"])
            pool_dtype = {k: DOUBLE if k != "Timestamp" else DATETIME for k in hours_pool_df.columns}
            store_df_to_sql(hours_pool_df, "kamba_hours_pool_data", pool_dtype)
            store_df_to_sql(days_pool_df, "kamba_days_pool_data", pool_dtype)
        # elif block == "tianjin":
        else:
            items = data_collation(block, start, end)
            data_dtype = get_dtype(items.keys())
            df = pd.DataFrame(items)
            store_df_to_sql(df, TB["store"][block], data_dtype)
        backup_data_to_long_table(block, items)


def update_realtime_data(block):
    """分板块实时数据更新
    :param block: 数据隶属，如：错那、岗巴
    :return:
    """
    data_range = get_data_range("realtime")
    latest_time = data_range[block]["latest"] + timedelta(days=1)
    start = "{} 00:00:00".format(latest_time.strftime("%Y-%m-%d"))
    end = "{} 23:59:59".format(data_range[block]["end"].strftime("%Y-%m-%d"))

    if block == "cona":
        items = data_collation(block, start, end)
        store_data(block, items)
    elif block == "kamba":
        items = data_collation(block, start, end)
        pool_data = items.pop("pool_data")
        store_data(block, items)

        hours_pool_df = pd.DataFrame(pool_data["hours_data"])
        days_pool_df = pd.DataFrame(pool_data["days_data"])
        pool_dtype = {k: DOUBLE if k != "Timestamp" else DATETIME for k in hours_pool_df.columns}
        store_df_to_sql(hours_pool_df, "kamba_hours_pool_data", pool_dtype)
        store_df_to_sql(days_pool_df, "kamba_days_pool_data", pool_dtype)

        items["pool_data"] = pool_data
    # elif block == "tianjin":
    else:

        items = data_collation(block, start, end)
        data_dtype = get_dtype(items.keys())
        df = pd.DataFrame(items)
        store_df_to_sql(df, TB["store"][block], data_dtype)
    backup_data_to_long_table(block, items)


def store_data(block, items):
    """数据存储
    :param block: 数据隶属 如：cona、kamba、tianjin
    :param items: 数据集合
    :param directly: 是否直接存储数据
    """

    engine = get_store_conn()

    hours_data, days_data = items["hours_data"], items["days_data"]

    try:
        # logging.info("开始 {} - {} 上传".format(block, "时数据"))
        print("开始 {} - {} 上传".format(block, "时数据"))
        hours_dtype = get_dtype(hours_data.keys())
        hours_df = pd.DataFrame(hours_data)
        # hours_df.to_csv("hours.csv")
        hours_df.to_sql(name=TB["store"][block]["hours"], con=engine, if_exists="append", index=False,
                        dtype=hours_dtype)
        # logging.info("完成 {} - {} 上传".format(block, "时数据"))
        print("完成 {} - {} 上传".format(block, "时数据"))

        # logging.info("开始 {} - {} 上传".format(block, "日数据"))
        print("开始 {} - {} 上传".format(block, "日数据"))
        days_dtype = get_dtype(days_data.keys())
        days_df = pd.DataFrame(days_data)
        # days_df.to_csv("days.csv")
        days_df.to_sql(name=TB["store"][block]["days"], con=engine, if_exists="append", index=False, dtype=days_dtype)
        # logging.info("完成 {} - {} 上传".format(block, "日数据"))
        print("完成 {} - {} 上传".format(block, "日数据"))
    except Exception as e:
        logging.error("数据上传异常")
        traceback.print_exc()
    finally:
        engine.dispose()


def store_df_to_sql(df, tb_name, d_type=None):
    engine = get_store_conn()
    try:
        # logging.info("开始 上传数据至 {}".format(tb_name))
        print("开始 上传数据至 {}".format(tb_name))
        if d_type:
            df.to_sql(name=tb_name, con=engine, if_exists="append", index=False, dtype=d_type)
        else:
            df.to_sql(name=tb_name, con=engine, if_exists="append", index=False)
        # logging.info("完成 数据上传 - {}".format(tb_name))
        print("完成 数据上传 - {}".format(tb_name))
    except Exception as e:
        logging.error("数据上传异常 - {}".format(tb_name))
        traceback.print_exc()
    finally:
        engine.dispose()


def backup_statistics_data(block, backup_path):
    if not os.path.exists(backup_path):
        os.makedirs(backup_path)

    sql_conf = get_sql_conf(DB["store"])
    tables = {
        "cona": ["cona_days_data", "cona_hours_data"],
        "kamba": ["kamba_hours_data", "kamba_days_data", "kamba_days_pool_data", "kamba_hours_pool_data"],
        "tianjin": ["tianjin_commons_data"]
    }
    now, num = datetime.today().strftime("%Y%m%d"), 1
    name = os.path.join(backup_path, "{}_{}.sql".format(block, now))

    while os.path.exists(name):
        num += 1
        name = os.path.join(backup_path, "{}_{}({}).sql".format(block, now, num))

    backup_sql = "mysqldump -u{} -p{} {} {} > {}".format(
        sql_conf["user"],
        sql_conf["password"],
        DB["store"],
        " ".join(tables[block]),
        name
    )
    os.system(backup_sql)
    print("数据备份已完成 文件名：{}, 时间：{}".format(
        name, datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    ))
    logging.info("数据备份已完成 文件名：{}, 时间：{}".format(
        name, datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    ))


def backup_data_to_long_table(block, items):
    custom_conf = get_sql_conf("dc_long")
    eng = get_custom_conn(custom_conf)
    d_type = {"time_data": DATETIME, "pointname": VARCHAR(length=50), "value": DOUBLE}
    try:
        if block == "cona":

            hours_df, days_df = pd.DataFrame(items["hours_data"]), pd.DataFrame(items["days_data"])
            hours_df = hours_df.melt(id_vars="time_data", var_name="pointname")
            days_df = days_df.melt(id_vars="time_data", var_name="pointname")
            hours_df.to_sql(name="cona_hours_data", con=eng, if_exists="append", index=False, dtype=d_type)
            days_df.to_sql(name="cona_days_data", con=eng, if_exists="append", index=False, dtype=d_type)
            print("历史数据 - 错那宽表备份完成")
        elif block == "kamba":
            pool_data = items.pop("pool_data")
            pool_type = {"timestamp": DATETIME, "pointname": VARCHAR(length=50), "value": DOUBLE}

            # 存储水池数据
            hours_pool_df = pd.DataFrame(pool_data["hours_data"])
            days_pool_df = pd.DataFrame(pool_data["days_data"])

            hours_pool_df.to_sql(name="kamba_hours_pool_data", con=eng, if_exists="append", index=False,
                                 dtype=pool_type)
            days_pool_df.to_sql(name="kamba_days_pool_data", con=eng, if_exists="append", index=False, dtype=pool_type)

            # 处理基础数据
            hours_df, days_df = pd.DataFrame(items["hours_data"]), pd.DataFrame(items["days_data"])
            hours_df = hours_df.melt(id_vars="time_data", var_name="pointname")
            days_df = days_df.melt(id_vars="time_data", var_name="pointname")
            hours_df.to_sql(name="kamba_hours_data", con=eng, if_exists="append", index=False, dtype=d_type)
            days_df.to_sql(name="kamba_days_data", con=eng, if_exists="append", index=False, dtype=d_type)
            print("历史数据 - 岗巴宽表备份完成")
        elif block == "tianjin":
            df = pd.DataFrame(items)
            df = df.melt(id_vars="time_data", var_name="pointname")
            df.to_sql(name="tianjin_commons_data", con=eng, if_exists="append", index=False, dtype=d_type)
            print("历史数据 - 天津宽表备份完成")
    except Exception as e:
        print("宽表备份异常")
        import traceback
        traceback.print_exc()
        eng.dispose()
    finally:
        eng.dispose()


print(DataCalc("cona", True, False).get_data_range("history"))

# 更新历史数据
# update_history_data(["kamba"])
