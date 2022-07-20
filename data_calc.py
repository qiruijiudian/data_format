# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/5 17:09
# @Author  : MAYA
import os
import platform
import pymysql
import pandas as pd
import traceback
from tools import DB, TB, check_time, get_dtype, get_data_range, get_sql_conf, log_or_print, \
    get_cona_geothermal_wells_heat_provide, get_cona_com_cop, get_cona_cost_saving, get_cona_heat_provided, \
    get_cona_water_supply_return_temperature, get_cona_water_replenishment, get_cona_sub_com_cop, \
    get_cona_sub_water_source_cop, get_cona_room_network_water_supply_temperature, get_cona_temp, \
    get_kamba_heat_storage_heat, get_kamba_com_cop, get_kamba_wshp_cop, get_kamba_water_replenishment, \
    get_kamba_solar_matrix_supply_and_return_water_temperature, get_kamba_load, \
    get_kamba_end_supply_and_return_water_temp, get_kamba_calories, get_kamba_solar_heat_supply, get_kamba_heat_supply,\
    get_kamba_cost_saving, get_kamba_co2_emission, get_kamba_pool_temperature, get_fan_frequency, get_cold_water_valve,\
    get_hot_water_valve, get_air_supply_pressure, get_air_supply_humidity, get_air_supply_temperature, \
    get_temperature_and_humidity, get_conn_by_key

from datetime import timedelta, datetime


# ************************************************  公共函数  ************************************************************

class DataCalc:
    def __init__(self, block, print_mode, log_mode):
        self.block = block
        self.print_mode = print_mode
        self.log_mode = log_mode

    def get_data_range(self, key):
        sql_conf = get_sql_conf(DB["query"])
        res = {}
        with pymysql.connect(
                host=sql_conf["host"],
                user=sql_conf["user"],
                password=sql_conf["password"],
                database=sql_conf["database"]
        ) as conn1:
            cur1 = conn1.cursor()
            context = {k: v["time_index"] for k, v in TB["query"].items() if k == self.block}
            for k, v in context.items():
                cur1.execute(
                    "select {} from {} order by {} asc limit 1".format(
                        TB["query"][self.block]["time_index"],
                        TB["query"][self.block]["table"],
                        TB["query"][self.block]["time_index"]
                    )
                )

                start = cur1.fetchone()

                cur1.execute(
                    "select {} from {} order by {} desc limit 1".format(
                        TB["query"][self.block]["time_index"],
                        TB["query"][self.block]["table"],
                        TB["query"][self.block]["time_index"]
                    )
                )
                end = cur1.fetchone()

                res[k] = {"start": start[0] or "", "end": end[0] or ""}

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

            context2 = {k: v["hours"] if isinstance(v, dict) else "tianjin_commons_data" for k, v in TB["store"].items() if k == self.block}
            for k, v in context2.items():
                cur2.execute("select time_data from {} order by time_data desc limit 1;".format(v))
                item = cur2.fetchone()
                item = item[0] or ""
                res[k]["latest"] = item
            cur2.close()
        return res

    def data_collation(self, start, end):
        res = {"hours_data": {}, "days_data": {}}
        log_or_print(self, "{} 数据获取 开始".format(self.block))
        if self.block == "cona":

            items = [
                get_cona_geothermal_wells_heat_provide(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_cona_com_cop(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_cona_cost_saving(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_cona_heat_provided(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_cona_water_supply_return_temperature(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_cona_water_replenishment(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_cona_sub_com_cop(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_cona_sub_water_source_cop(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_cona_room_network_water_supply_temperature(start, end, print_mode=self.print_mode, log_mode=self.log_mode)
            ]
            success, hours_time, days_time = check_time(items)
            if success:

                for item in items:
                    for key in ["hours_data", "days_data"]:
                        if item.get(key):
                            res[key].update(item[key])

                # 获取日平均温度
                days_temp = get_cona_temp(res["days_data"]["time_data"])
                res["days_data"]["temp"] = days_temp
            else:
                log_or_print(self, "数据获取失败")

        elif self.block == "kamba":
            items = [
                get_kamba_heat_storage_heat(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_kamba_com_cop(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_kamba_wshp_cop(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_kamba_water_replenishment(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_kamba_solar_matrix_supply_and_return_water_temperature(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_kamba_load(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_kamba_end_supply_and_return_water_temp(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_kamba_calories(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_kamba_solar_heat_supply(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_kamba_heat_supply(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_kamba_cost_saving(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
                get_kamba_co2_emission(start, end, print_mode=self.print_mode, log_mode=self.log_mode),
            ]
            res["pool_data"] = get_kamba_pool_temperature(start, end, print_mode=self.print_mode, log_mode=self.log_mode)

            success, hours_time, days_time = check_time(items)

            if success:
                for item in items:
                    for key in ["hours_data", "days_data"]:
                        if item.get(key):
                            res[key].update(item[key])

            else:
                log_or_print(self, "数据获取失败")

        else:
            items = [
                get_fan_frequency(start, end),
                get_cold_water_valve(start, end),
                get_hot_water_valve(start, end),
                get_air_supply_pressure(start, end),
                get_air_supply_humidity(start, end),
                get_air_supply_temperature(start, end),
                get_temperature_and_humidity(start, end)
            ]
            res = {}
            for item in items:
                res.update(item)
        log_or_print(self, "{} 数据获取完成".format(self.block))
        return res

    def update_history_data(self):
        # 更新历史数据

        data_range = self.get_data_range("history")
        print(data_range)

        start = "{} 00:00:00".format(data_range[self.block]["start"].strftime("%Y-%m-%d"))
        end = "{} 23:59:59".format(data_range[self.block]["end"].strftime("%Y-%m-%d"))
        items = self.data_collation(start, end)
        # 宽表备份
        self.store_data(items, True)

        # 长格式存储
        self.store_data(items, False)

    def store_data(self, items, backup=False):

        if backup:
            engine = get_conn_by_key("backup")
        else:
            engine = get_conn_by_key("store")
        try:
            if backup:
                log_or_print(self, "数据 宽格式备份开始")
                if self.block in ["cona", "kamba"]:

                    context = {
                        "hours": {"type": "时数据", "data": "hours_data"},
                        "days": {"type": "日数据", "data": "days_data"}
                    }
                    for k, v in context.items():
                        log_or_print(self, "{} {} 开始上传（宽格式备份）".format(self.block, v["type"]))
                        data = items[v["data"]]
                        d_type = get_dtype(data.keys(), True)
                        df = pd.DataFrame(data)
                        df.to_sql(name=TB["backup"][self.block][k], con=engine, if_exists="append", index=False,
                                  dtype=d_type)
                        log_or_print(self, "{} {} 上传完成（宽格式备份）".format(self.block, v["type"]))
                    if self.block == "kamba":
                        pool_data = items["pool_data"]
                        pool_context = {
                            "hours": {"type": "水池温度时数据", "data": "hours_data"},
                            "days": {"type": "水池温度日数据", "data": "days_data"}
                        }
                        for key, value in pool_context.items():
                            log_or_print(self, "{} {} 开始上传（宽格式备份）".format(self.block, value["type"]))

                            data = pool_data[value["data"]]
                            pool_type = get_dtype(data.keys(), True)
                            pool_df = pd.DataFrame(data)
                            pool_df.to_sql(
                                name=TB["backup"][self.block]["pool_temperature"][key], con=engine,
                                if_exists="append", index=False, dtype=pool_type
                            )

                            log_or_print(self, "{} {} 上传完成（宽格式备份）".format(self.block, value["type"]))

                else:
                    # 天津
                    log_or_print(self, "天津数据开始上传（宽数据备份）")
                    d_type = get_dtype(items.keys(), True)
                    df = pd.DataFrame(items)
                    df.to_sql(name=TB["backup"]["tianjin"], con=engine, if_exists="append", index=False, dtype=d_type)
                    log_or_print(self, "天津数据上传完成（宽格式备份）")
                log_or_print(self, "数据 宽格式备份完成")
            else:
                log_or_print(self, "数据 长格式存储开始")
                if self.block in ["cona", "kamba"]:
                    context = {
                        "hours": {"type": "时数据", "data": "hours_data"},
                        "days": {"type": "日数据", "data": "days_data"}
                    }

                    for k, v in context.items():
                        log_or_print(self, "{} {} 开始上传（长格式存储）".format(self.block, v["type"]))
                        data = items[v["data"]]
                        d_type = get_dtype(data.keys())
                        df = pd.DataFrame(data).melt(id_vars="time_data", var_name="pointname")
                        df.to_sql(name=TB["store"][self.block][k], con=engine, if_exists="append", index=False,
                                  dtype=d_type)
                        log_or_print(self, "{} {} 上传完成（长格式存储）".format(self.block, v["type"]))
                    if self.block == "kamba":
                        pool_data = items["pool_data"]
                        pool_context = {
                            "hours": {"type": "水池温度时数据", "data": "hours_data"},
                            "days": {"type": "水池温度日数据", "data": "days_data"}
                        }
                        for key, value in pool_context.items():
                            log_or_print(self, "{} {} 开始上传（长格式存储）".format(self.block, value["type"]))
                            data = pool_data[value["data"]]
                            pool_type = get_dtype(data.keys())
                            pool_df = pd.DataFrame(data).melt(id_vars="Timestamp", var_name="pointname")
                            pool_df.to_sql(
                                name=TB["store"][self.block]["pool_temperature"][key], con=engine,
                                if_exists="append", index=False, dtype=pool_type
                            )
                            log_or_print(self, "{} {} 上传完成（长格式存储）".format(self.block, value["type"]))

                else:
                    # 天津
                    log_or_print(self, "天津数据开始上传（长格式存储）")
                    d_type = get_dtype(items.keys())
                    df = pd.DataFrame(items).melt(id_vars="time_data", var_name="pointname")
                    df.to_sql(name=TB["store"]["tianjin"], con=engine, if_exists="append", index=False, dtype=d_type)
                    log_or_print(self, "天津数据上传完成（长格式存储）")

                log_or_print(self, "数据 长格式存储完成")

        except Exception as e:
            log_or_print(self, "数据上传失败",  e)
            traceback.print_exc()
        finally:
            engine.dispose()

    def update_realtime_data(self):
        # 更新实时数据

        data_range = get_data_range("realtime")
        latest_time = data_range[self.block]["latest"] + timedelta(days=1)
        start = "{} 00:00:00".format(latest_time.strftime("%Y-%m-%d"))
        end = "{} 23:59:59".format(data_range[self.block]["end"].strftime("%Y-%m-%d"))

        items = self.data_collation(start, end)
        # 宽表备份
        self.store_data(items, True)

        # 长格式存储
        self.store_data(items, False)

    def backup_statistics_data(self, backup_path):
        if not os.path.exists(backup_path):
            os.makedirs(backup_path)

        sql_conf = get_sql_conf(DB["store"])
        tables = []
        for k1, v1 in TB["store"].items():
            if k1 == self.block:
                if isinstance(v1, dict):
                    # 错那、岗巴
                    for k2, v2 in v1.items():
                        if isinstance(v2, dict):
                            # 岗巴水池
                            for k3, v3 in v2.items():
                                tables.append(v3)
                        else:
                            # 错那
                            tables.append(v2)
                else:
                    # 天津
                    tables.append(v1)

        now, num = datetime.today().strftime("%Y%m%d"), 1
        name = os.path.join(backup_path, "{}_{}.sql".format(self.block, now))

        while os.path.exists(name):
            num += 1
            name = os.path.join(backup_path, "{}_{}[{}].sql".format(self.block, now, num))

        backup_sql = "mysqldump -u{} -p{} {} {} > {}".format(
            sql_conf["user"],
            sql_conf["password"],
            DB["store"],
            " ".join(tables),
            name
        )
        os.system(backup_sql)
        log_or_print(
            self,
            "数据备份已完成 文件名：{}, 时间：{}".format(
                name, datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            )
        )

    def backup_statistics_wide_data(self, backup_path):
        if not os.path.exists(backup_path):
            os.makedirs(backup_path)

        sql_conf = get_sql_conf(DB["backup"])
        tables = []
        for k1, v1 in TB["store"].items():
            if k1 == self.block:
                if isinstance(v1, dict):
                    # 错那、岗巴
                    for k2, v2 in v1.items():
                        if isinstance(v2, dict):
                            # 岗巴水池
                            for k3, v3 in v2.items():
                                tables.append(v3)
                        else:
                            # 错那
                            tables.append(v2)
                else:
                    # 天津
                    tables.append(v1)

        now, num = datetime.today().strftime("%Y%m%d"), 1
        name = os.path.join(backup_path, "{}_wide_{}.sql".format(self.block, now))

        while os.path.exists(name):
            num += 1
            name = os.path.join(backup_path, "{}_wide_{}[{}].sql".format(self.block, now, num))

        backup_sql = "mysqldump -u{} -p{} {} {} > {}".format(
            sql_conf["user"],
            sql_conf["password"],
            DB["store"],
            " ".join(tables),
            name
        )
        os.system(backup_sql)
        log_or_print(
            self,
            "数据备份已完成 文件名：{}, 时间：{}".format(
                name, datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            )
        )


