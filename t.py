# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/9 14:04
# @Author  : MAYA


a = ['mean', None, None, 'mean', 'mean', 'mean',
                                                             {'heat_water_replenishment_limit': 'mean'}
                                                             ]
lst = [data for data in a if not isinstance(data, dict)]
dic = [data for data in a if isinstance(data, dict)]
print(lst, dic)
point_lst = ['HHWLoop_MUflow', 'HHWLoop_RFlow', 'HHWLoop_BypassFlow', 'Pit_MU_flow', 'Solar_MUflow', 'SolarRFM_0201']
_op_dic = dict(zip(point_lst, lst))
print(_op_dic)
op_dic = {k: v for k, v in _op_dic.items() if v}
# if dic:
# for _data in dic:
#   op_dic.update(_data)
# data = data.resample('h').agg(op_dic)
# print(a)