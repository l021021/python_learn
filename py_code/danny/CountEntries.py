# -*- coding: utf-8 -*-
#coding=utf-8

import time
import json
import websocket
import datetime
# import pymysql as MySQLdb
import ssl
import sys
import re
from random import randint
import pandas as pd
import sqlite3
# from sqlalchemy import create_engine
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import logging
import csv
global tag

with open('C:\\codebase\\log\\60358_2019_12_23_00_00_00_2019_12_31_23_59_59.json', encoding='utf-8') as f:
# with open('C:\\Users\Andyn\Desktop\\test.json', encoding='utf-8') as f:
    rawdata = json.load(f)
    print(len(rawdata))
    f.close()

print(rawdata)

dic = {}
res = {}
STARTTIME = '2020-04-26 00:00:00'
start = int(time.mktime(time.strptime(STARTTIME,'%Y-%m-%d %H:%M:%S'))) * 1000

for subdata in rawdata:
    for ele in subdata:
        remainder = (ele['sampleTime'] - start) // 600000
        if ele['DID'] not in dic.keys():
            dic[ele['DID']] = {}
            res[ele['DID']] = {}
            dic[ele['DID']][remainder] = [ele['value']]
        else:
            if remainder not in dic[ele['DID']].keys():
                dic[ele['DID']][remainder] = [ele['value']]
            else:
                dic[ele['DID']][remainder].append(ele['value'])


tag = 'free'
last = None

for uid in dic.keys():
    for gap in dic[uid].keys():
        count = 0
        for item in dic[uid][gap]:
            if last == None:
                last = item
            elif last != item:
                if tag == 'free':
                    tag = 'occupied'
                    last = item
                else:
                    last = item
            else:
                if tag == 'occupied':
                    tag = 'free'
                    count += 1
        res[uid][gap] = count

for i in res.keys():
    temp = set(res[i].values())
    print(temp)