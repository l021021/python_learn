import time
import json
import websocket
import datetime
# import pymysql as MySQLdb
# import ssl
import sys
import re
from random import randint
import pandas as pd
import numpy as np
# import sqlite3
# from sqlalchemy import create_engine
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import logging
import csv
global tag
global last

with open(
        'C:\\codebase\\log\\229349_2020_04_20_00_00_00_2020_05_01_00_00_00_UU.json',
        encoding='utf-8') as f:
    rawdata = f.readlines()[0]
    rawdata = rawdata.replace(']', '],')
    rawdata = '[' + rawdata
    rawdata = rawdata[:-1] + ']'
    rawdata = json.loads(rawdata)
    # print(rawdata)
    f.close()

# dt=np.datatype()
data = np.array(rawdata, dtype=dt)
dic = {}
res = {}
occ = {}
STARTTIME = '2020-04-20 00:00:00'
start = int(time.mktime(time.strptime(STARTTIME, '%Y-%m-%d %H:%M:%S'))) * 1000

for subdata in rawdata:
    for ele in subdata:
        remainder = (ele['sampleTime'] - start) // 600000
        if ele['DID'] not in dic.keys():
            dic[ele['DID']] = {}
            res[ele['DID']] = {}
            occ[ele['DID']] = {}
            dic[ele['DID']][remainder] = [[ele['sampleTime']], [ele['value']]]
        else:
            if remainder not in dic[ele['DID']].keys():
                dic[ele['DID']][remainder] = [[ele['sampleTime']],
                                              [ele['value']]]
            else:
                dic[ele['DID']][remainder][0].append(ele['sampleTime'])
                dic[ele['DID']][remainder][1].append(ele['value'])

tag = 'free'
last = None
print(dic)
for uid in dic.keys():
    for gap in dic[uid].keys():
        timelist = []
        datalist = []
        for i in range(len(dic[uid][gap][1])):
            if last == None:
                last = dic[uid][gap][1][i]
            elif last != dic[uid][gap][1][i]:
                if tag == 'free':
                    tag = 'occupied'
                    last = dic[uid][gap][1][i]
                    timelist.append(dic[uid][gap][0][i])
                    datalist.append('occupied')
                else:
                    last = dic[uid][gap][1][i]
            else:
                if tag == 'occupied':
                    tag = 'free'
                    timelist.append(dic[uid][gap][0][i])
                    datalist.append('free')

        if (len(datalist)) == 0:
            timelist.append(tag)
        res[uid][gap] = [timelist, datalist]

print(res)

for uid in res.keys():
    for gap in res[uid].keys():
        occupied_time = 0.0
        first_record = ''
        last_record = ''
        util = 0.0
        begin = start + gap * 600000
        end = start + (gap + 1) * 600000

        if len(res[uid][gap][1]) == 0:
            if res[uid][gap][0] == 'free':
                util = format(0.0 * 100.0, '.3f')
            elif res[uid][gap][0] == 'occupied':
                util = format(1.0 * 100.0, '.3f')

        elif len(res[uid][gap][1]) == 1:
            first_gap = res[uid][gap][0][0] - begin
            last_gap = end - res[uid][gap][0][0]

            if res[uid][gap][1][0] == 'occupied':
                util = format(last_gap / 600000 * 100.0, '.3f')
            elif res[uid][gap][1][0] == 'free':
                util = format(first_gap / 600000 * 100.0, '.3f')

        else:
            first_gap = res[uid][gap][0][0] - begin
            last_gap = end - res[uid][gap][0][0]

            if res[uid][gap][1][0] == 'free':
                first_record = 'free'
                del res[uid][gap][0][0], res[uid][gap][1][0]

            if res[uid][gap][1][-1] == 'occupied':
                last_record = 'occupied'
                del res[uid][gap][0][-1], res[uid][gap][1][-1]

            indexList = [
                i for i in range(len(res[uid][gap][1]))
                if res[uid][gap][1][i] == 'occupied'
            ]

            for j in indexList:
                occupied_time += (
                    (res[uid][gap][0][j + 1] - res[uid][gap][0][j]) * 1.0)

            if first_record:
                occupied_time += first_gap

            if last_record:
                occupied_time += last_gap

            util = format(occupied_time / 600000 * 100, '.3f')

        occ[uid][gap] = util

print(occ)