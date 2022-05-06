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
    """获取天津数据dataframe完整的columns
    :param file: columns文件，json格式
    :return:
    """
    with open(file) as f:
        return json.load(f)


class DataMissing(Exception):
    """自定义数据缺失异常
    """
    def __init__(self, error_info):
        super().__init__(self)
        self.error_info = error_info
        logging.error(self.error_info)

    def __str__(self):
        return self.error_info


def resample_data_by_hours(df, hours_op_dic):
    """按照1小时为周期分组统计数据
    根据hours_op_dic中提供的映射数据统计数据，若不提供映射字典则默认所有数据按照平均值聚合

    Args:
        df: 数据集合
        hours_op_dic: 聚合函数映射字典，如{"a": "sum", "b": "mean}

    Returns:
        错那数据则直接返回分组后的数据(DatetimeIndexResampler类型)，岗巴数据除分组后的数据外还额外返回一个包含查询字段(转换后的英文)的列表
    """
    df = df.set_index(pd.to_datetime(df["time"]))
    if len(hours_op_dic):
        df = df.resample("h").agg(hours_op_dic)
    else:
        df = df.resample("h").mean()
    return df

    # if 'cona' in db:
    #
    #     data = df.set_index(pd.to_datetime(df.index))
    #     if len(hours_op_dic):
    #         data = data.resample('h').agg(hours_op_dic)
    #     else:
    #         data = data.resample('h').mean()
    #     return data
    # elif 'kamba' in db:
    #     data, point_lst = get_data(name, start, end, db)
    #     data = data.set_index(pd.to_datetime(data.index))
    #     if len(hours_op_dic):
    #         if isinstance(hours_op_dic, dict):
    #             data = data.resample('h').agg(hours_op_dic)
    #         elif isinstance(hours_op_dic, list):
    #             lst = [data for data in hours_op_dic if not isinstance(data, dict)]
    #             dic = [data for data in hours_op_dic if isinstance(data, dict)]
    #             _op_dic = dict(zip(point_lst, lst))
    #             op_dic = {k: v for k, v in _op_dic.items() if v}
    #             if dic:
    #                 for _data in dic:
    #                     op_dic.update(_data)
    #             data = data.resample('h').agg(op_dic)
    #     else:
    #         data = data.resample('h').mean()
    #     return data, point_lst

# def resample_data_by_hours(df, hours_op_dic):
#     """按照1小时为周期分组统计数据
#     根据hours_op_dic中提供的映射数据统计数据，若不提供映射字典则默认所有数据按照平均值聚合
#
#     Args:
#         df: 数据集合
#         hours_op_dic: 聚合函数映射字典，如{"a": "sum", "b": "mean}
#
#     Returns:
#         错那数据则直接返回分组后的数据(DatetimeIndexResampler类型)，岗巴数据除分组后的数据外还额外返回一个包含查询字段(转换后的英文)的列表
#     """
#     if 'cona' in db:
#
#         data = df.set_index(pd.to_datetime(df.index))
#         if len(hours_op_dic):
#             data = data.resample('h').agg(hours_op_dic)
#         else:
#             data = data.resample('h').mean()
#         return data
#     elif 'kamba' in db:
#         data, point_lst = get_data(name, start, end, db)
#         data = data.set_index(pd.to_datetime(data.index))
#         if len(hours_op_dic):
#             if isinstance(hours_op_dic, dict):
#                 data = data.resample('h').agg(hours_op_dic)
#             elif isinstance(hours_op_dic, list):
#                 lst = [data for data in hours_op_dic if not isinstance(data, dict)]
#                 dic = [data for data in hours_op_dic if isinstance(data, dict)]
#                 _op_dic = dict(zip(point_lst, lst))
#                 op_dic = {k: v for k, v in _op_dic.items() if v}
#                 if dic:
#                     for _data in dic:
#                         op_dic.update(_data)
#                 data = data.resample('h').agg(op_dic)
#         else:
#             data = data.resample('h').mean()
#         return data, point_lst


def resample_data_by_days(df, just_date=False, hours_op_dic=None, days_op_dic=None):
    """按照24小时为周期分组统计数据
    标准流程为先按照hours_op_dic中提供的映射数据统计数据，在此基础上按照days_op_dic设定的聚合函数来对数据进行二次聚合。

        Args:
            name:数据名称，用于调用get_data获取原始数据
            start: 开始时间
            end: 结束时间
            db: 数据库名称
            just_date: 是否只按照天周期数据做集合，默认为False，若为True则会先调用resample_data_by_hours以此基础再往后执行下一步
            hours_op_dic: 小时周期聚合函数映射字典，如{"a": "sum", "b": "mean}
            days_op_dic: 天周期聚合函数映射字典，同上，在按照1小时分组后以此来对数据进行聚合统计
        Returns:
            错那数据则直接返回分组后的数据(DataFrame类型)，岗巴数据除DataFrame数据外还额外返回一个包含查询字段(转换后的英文)的列表
        """
    df = df.set_index(pd.to_datetime(df["time"]))
    if just_date:
        df = df.resample("D")
        if days_op_dic:
            df = df.agg(days_op_dic)
        else:
            df = df.mean()
    else:
        df = resample_data_by_hours(df, hours_op_dic)
        if days_op_dic:
            df = df.agg(days_op_dic)
        else:
            df = df.mean()
    return df


    # if 'cona' in db:
    #     if just_date:
    #         data = get_data(name, start, end, db)
    #         data = data.set_index(pd.to_datetime(data.index)).resample('D')
    #         if days_op_dic:
    #             return data.agg(days_op_dic)
    #         else:
    #             return data.mean()
    #     hours_data = resample_data_by_hours(name, start, end, db, hours_op_dic)
    #     return hours_data.resample('D').agg(days_op_dic)
    # elif 'kamba' in db:
    #     if just_date:
    #         data, point_lst = get_data(name, start, end, db)
    #         data = data.set_index(pd.to_datetime(data.index)).resample('D')
    #         if days_op_dic:
    #             if isinstance(days_op_dic, dict):
    #                 return data.agg(days_op_dic), point_lst
    #             elif isinstance(days_op_dic, list):
    #                 lst = [data for data in days_op_dic if not isinstance(data, dict)]
    #                 dic = [data for data in days_op_dic if isinstance(data, dict)]
    #                 _op_dic = dict(zip(point_lst, lst))
    #                 op_dic = {k: v for k, v in _op_dic.items() if v}
    #                 if dic:
    #                     for _data in dic:
    #                         op_dic.update(_data)
    #                 return data.agg(op_dic), point_lst
    #         else:
    #             return data.mean()
    #     hours_data, point_lst = resample_data_by_hours(name, start, end, db, hours_op_dic)
    #     if isinstance(days_op_dic, dict):
    #         return hours_data.resample('D').agg(days_op_dic), point_lst
    #     elif isinstance(hours_op_dic, list):
    #         lst = [data for data in days_op_dic if not isinstance(data, dict)]
    #         dic = [data for data in days_op_dic if isinstance(data, dict)]
    #         _op_dic = dict(zip(point_lst, lst))
    #         op_dic = {k: v for k, v in _op_dic.items() if v}
    #         if dic:
    #             for _data in dic:
    #                 op_dic.update(_data)
    #         return hours_data.resample('D').agg(op_dic), point_lst



# def resample_data_by_days(name, start, end, db, just_date=False, hours_op_dic=None, days_op_dic=None):
#     """按照24小时为周期分组统计数据
#     标准流程为先按照hours_op_dic中提供的映射数据统计数据，在此基础上按照days_op_dic设定的聚合函数来对数据进行二次聚合。
#
#         Args:
#             name:数据名称，用于调用get_data获取原始数据
#             start: 开始时间
#             end: 结束时间
#             db: 数据库名称
#             just_date: 是否只按照天周期数据做集合，默认为False，若为True则会先调用resample_data_by_hours以此基础再往后执行下一步
#             hours_op_dic: 小时周期聚合函数映射字典，如{"a": "sum", "b": "mean}
#             days_op_dic: 天周期聚合函数映射字典，同上，在按照1小时分组后以此来对数据进行聚合统计
#         Returns:
#             错那数据则直接返回分组后的数据(DataFrame类型)，岗巴数据除DataFrame数据外还额外返回一个包含查询字段(转换后的英文)的列表
#         """
#     if 'cona' in db:
#         if just_date:
#             data = get_data(name, start, end, db)
#             data = data.set_index(pd.to_datetime(data.index)).resample('D')
#             if days_op_dic:
#                 return data.agg(days_op_dic)
#             else:
#                 return data.mean()
#         hours_data = resample_data_by_hours(name, start, end, db, hours_op_dic)
#         return hours_data.resample('D').agg(days_op_dic)
#     elif 'kamba' in db:
#         if just_date:
#             data, point_lst = get_data(name, start, end, db)
#             data = data.set_index(pd.to_datetime(data.index)).resample('D')
#             if days_op_dic:
#                 if isinstance(days_op_dic, dict):
#                     return data.agg(days_op_dic), point_lst
#                 elif isinstance(days_op_dic, list):
#                     lst = [data for data in days_op_dic if not isinstance(data, dict)]
#                     dic = [data for data in days_op_dic if isinstance(data, dict)]
#                     _op_dic = dict(zip(point_lst, lst))
#                     op_dic = {k: v for k, v in _op_dic.items() if v}
#                     if dic:
#                         for _data in dic:
#                             op_dic.update(_data)
#                     return data.agg(op_dic), point_lst
#             else:
#                 return data.mean()
#         hours_data, point_lst = resample_data_by_hours(name, start, end, db, hours_op_dic)
#         if isinstance(days_op_dic, dict):
#             return hours_data.resample('D').agg(days_op_dic), point_lst
#         elif isinstance(hours_op_dic, list):
#             lst = [data for data in days_op_dic if not isinstance(data, dict)]
#             dic = [data for data in days_op_dic if isinstance(data, dict)]
#             _op_dic = dict(zip(point_lst, lst))
#             op_dic = {k: v for k, v in _op_dic.items() if v}
#             if dic:
#                 for _data in dic:
#                     op_dic.update(_data)
#             return hours_data.resample('D').agg(op_dic), point_lst
SQL_CONTEXT = {
    "API_GEOTHERMAL_WELLS_HEAT_PROVIDE_SQL":  """
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
    # ""


}