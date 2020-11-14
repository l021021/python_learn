# -*- coding: utf-8 -*-
# coding=utf-8

import time
import json
import pickle
# import websocket
# import datetime
# # import pymysql as MySQLdb
# import ssl
# import sys
# import re
# # from random import randint
# # import pandas as pd
# # import sqlite3

# 修改json文件路径
# 正确格式的处理方式
with open('C:\\codebase\\yanzi2020\\py_code\\sugar-v5-2.json', 'r') as rawdata:
    data = rawdata.read().replace(' ', '')
# print(data)
data1 = json.JSONDecoder().decode(data)
# print(rawdata)
# if isinstance(rawdata, str):
#     rawdata1 = json.loads(rawdata)
# print(rawdata)
# f.close()

# 修改json文件路径
# 若json文件没按要求处理好，则用此处运行
# json_filename = ('C:\\codebase\\yanzi2020\\py_code\\sugar-v5-2.json')

# rawdata = json.loads(open(json_filename))
# jsDumps = json.dumps(rawdata)
# jsonloads = json.loads(jsDumps)


def print_list(parameter_list):
    for each_item in parameter_list:
        print(each_item)
        if isinstance(each_item, list) or isinstance(each_item, dict):
            print_list(each_item)
        else:
            data_2 = json.JSONDecoder().decode(each_item)
            print_list(data_2)


print_list(data1)
