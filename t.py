# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/9 14:04
# @Author  : MAYA
import pandas as pd
from data_calc import get_kamba_co2_emission
import json



items = get_kamba_co2_emission("2021-07-16 00:00:00", "2021-07-20 23:59:59")
for key in ["hours_data", "days_data"]:
        data = items[key]
        for k, v in data.items():
                print(k, len(v))


hours_time = items["hours_data"]["time_data"]
days_time = items["days_data"]["time_data"]

hour_dates = pd.date_range("2021-07-16 00:00:00", "2021-07-20 23:59:59", freq="1H")
print(len(hour_dates))
day_dates = pd.date_range("2021-07-16 00:00:00", "2021-07-20 23:59:59", freq="1D")
print(len(day_dates))

print(len(hours_time) == len(hour_dates))
print(len(days_time) == len(day_dates))

print(hours_time == hour_dates)
print(days_time == day_dates)

print(hours_time)

# for item in hour_dates:


