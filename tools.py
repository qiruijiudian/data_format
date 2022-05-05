# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/4/25 17:21
# @Author  : MAYA


import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


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
            new_columns = ["date"] + list(df.columns[2:])
            df = df.loc[:, new_columns]
            return df, None
    else:
        res, columns = [], []
        with open(file) as f:
            for line in f:
                line = line.replace("No Data", "nan").replace("Data Loss", "nan")
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
        return res, columns


def get_all_columns(file):
    with open(file) as f:
        return json.load(f)


class DataMissing(Exception):
    def __init__(self, error_info):
        super().__init__(self)
        self.error_info = error_info
        logging.error(self.error_info)

    def __str__(self):
        return self.error_info
