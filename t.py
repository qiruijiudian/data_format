# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/9 14:04
# @Author  : MAYA
import pandas as pd
from sqlalchemy import create_engine

from data_calc import get_kamba_co2_emission
import json

import pymysql

from tools import get_sql_conf, DB


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



conn = get_store_conn()
df = pd.read_sql(
        "select time_data, solar_collector, solar_radiation, flow_rate, solar_matrix_supply_water_temp, solar_matrix_return_water_temp from kamba_days_data where time_data between '2021-05-01' and '2022-04-30'", con=conn
)
print(df)
df.to_csv("岗巴过去一年统计数据.csv")


conn.dispose()


