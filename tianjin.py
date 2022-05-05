# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/4/27 17:06
# @Author  : MAYA
import os
import platform
from main import DataFormat


base_path = "./data/conf" if platform.system() == "Windows" else "/home/data_format/data/conf"

DataFormat(os.path.join(base_path, "conf_tianjin.ini")).run()