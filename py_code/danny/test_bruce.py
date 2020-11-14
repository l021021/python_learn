# -*- coding: utf-8 -*-
# coding=utf-8

import time
import json
import websocket
import datetime
# import pymysql as MySQLdb
import ssl
import sys
import re
# from random import randint
# import pandas as pd
# import sqlite3
# from sqlalchemy import create_engine
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import logging
import csv
global tag

_grid = 60000  # 10M

# 修改json文件路径
# 正确格式的处理方式
# with open('C:\\Users\Andyn\Desktop\996052_2020_04_26_00_00_00_2020_04_27_14_0_00_Motion.json', encoding='utf-8') as f:
#     rawdata = json.load(f)
#     print(len(rawdata))
#     f.close()

# 修改json文件路径
# 若json文件没按要求处理好，则用此处运行
with open(
        'C:\\codebase\\log\\274189_2020_05_09_19_00_00_2020_05_09_21_00_00_Motion.json',
        encoding='utf-8') as f:
    rawdata = f.readlines()[0]
    # print('1=====================>', type(rawdata))
    # print(rawdata)
    rawdata = rawdata.replace(']', '],')
    rawdata = '[' + rawdata
    rawdata = rawdata[:-1] + ']'
    rawdata = json.loads(rawdata)
    # print(type(rawdata))
    f.close()

# print(rawdata)

sensors = {}
result = {}
keys = []

# 修改开始时间
STARTTIME = '2020-05-09 19:00:00'
start = int(time.mktime(time.strptime(STARTTIME, '%Y-%m-%d %H:%M:%S'))) * 1000

for persensorrecord in rawdata:
    for record_dict in persensorrecord:  #['DID', 'sampleTime', 'value', 'timeLastMotion'] dict keys

        # for k, v in motionrecord.items():
        #     # if k not in keys:
        #     #     keys.append(k)
        #     # print(keys)
        grid = (record_dict['sampleTime'] - start) // _grid  # 距离开始时间的格子偏差
        if record_dict['DID'] not in sensors.keys():
            sensors[record_dict['DID']] = {}  # 建立字典
            result[record_dict['DID']] = {}
            sensors[record_dict['DID']][grid] = [record_dict['value']]
        else:
            if grid not in sensors[record_dict['DID']].keys():
                sensors[record_dict['DID']][grid] = [record_dict['value']]
            else:
                sensors[record_dict['DID']][grid].append(
                    record_dict['value'])  # 相同格子的数据

tag = 'free'
lastvalue = None

for uid in sensors.keys():
    for gap in sensors[uid].keys():
        count = 0
        for motionvalue in sensors[uid][gap]:
            if lastvalue == None:
                lastvalue = motionvalue
            elif lastvalue != motionvalue:
                if tag == 'free':
                    tag = 'occupied'
                    lastvalue = motionvalue
                else:
                    lastvalue = motionvalue
            else:
                if tag == 'occupied':
                    tag = 'free'
                    count += 1
        result[uid][gap] = count  # 计算离开的次数

for i in result.keys():
    temp = list(result[i].values())
    print(i)
    print(temp)
