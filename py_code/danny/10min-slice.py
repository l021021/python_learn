import time
import json
import websocket
import datetime
import pymysql as MySQLdb
import ssl
import sys
import re
from random import randint
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import logging
import csv
global tag
global last


# 读取原始数据 My Style
with open("C:\\Users\Andyn\Desktop\AZ data\\447223_data.json", encoding='utf-8') as f:
    rawdata = []
    raw = f.readlines()[0]
    raw = json.loads(raw)
    rawdata.append(raw)
    f.close()

# 读取原始数据 Bruce's Style
# with open('C:\\Users\Andyn\Desktop\\185308_2020_05_20_00_00_00_2020_05_22_00_00_00_Motion.json', encoding='utf-8') as f:
#     rawdata = f.readlines()[0]
#     print('1=====================>',type(rawdata))
#     # print(rawdata)
#     rawdata = rawdata.replace(']', '],')
#     rawdata = '[' + rawdata
#     rawdata = rawdata[:-1] + ']'
#     rawdata = json.loads(rawdata)
#     print(type(rawdata))
#     f.close()



# 分别为处理原始数据用的字典，化成占用数据的字典，结果字典
dic = {}
res = {}
occ = {}

# 起始时间的文本形式和毫秒级时间戳形式
STARTTIME = '2020-06-08 00:00:00'
start = int(time.mktime(time.strptime(STARTTIME,'%Y-%m-%d %H:%M:%S'))) * 1000
TIMESTEP = 1800000


# 处理原始数据
for subdata in rawdata:
    for ele in subdata:
        remainder = (ele['sampleTime'] - start) // TIMESTEP
        if ele['DID'] not in dic.keys():
            dic[ele['DID']] = {}
            res[ele['DID']] = {}
            occ[ele['DID']] = {}
            dic[ele['DID']][remainder] = [[ele['sampleTime']],[ele['value']]]
        else:
            if remainder not in dic[ele['DID']].keys():
                dic[ele['DID']][remainder] = [[ele['sampleTime']],[ele['value']]]
            else:
                dic[ele['DID']][remainder][0].append(ele['sampleTime'])
                dic[ele['DID']][remainder][1].append(ele['value'])

'''
dic = 
{
    'did-1':{
        t1:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        t2:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        ...
        ...
        tn:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]]
    },
    'did-2':{
        t1:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        t2:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        ...
        ...
        tn:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]]
    },
    ...
    ...
    ...
    'did-n':{
        t1:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        t2:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        ...
        ...
        tn:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]]
    }
}
'''

# print("dic=============>",dic)


for uid in dic.keys():
    tag = 'free'
    last = None
    for gap in dic[uid].keys():
        # 一个用来记录时间，一个用来记录状态变化
        timelist = []
        datalist = []
        for i in range(len(dic[uid][gap][1])):
            if last == None:
                last = dic[uid][gap][1][i]
            elif last != dic[uid][gap][1][i]:
                # 当从free变为occupied时，记录状态和对应的时间
                if tag == 'free':
                    tag = 'occupied'
                    last = dic[uid][gap][1][i]
                    timelist.append(dic[uid][gap][0][i])
                    datalist.append('occupied')
                else:
                    last = dic[uid][gap][1][i]
            else:
                # 当从occupied变为free时，记录状态和对应的时间
                if tag == 'occupied':
                    tag = 'free'
                    timelist.append(dic[uid][gap][0][i])
                    datalist.append('free')
        if (len(datalist)) == 0:
            timelist.append(tag)
        res[uid][gap] = [timelist,datalist]
# print("res===========>",res)

'''
res = 
{
    'did-1':{
        t1:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]],  ===>正常格式
        t2:[[occupied],[]],                                      ===>若整个十分钟内状态无变化，则状态list为空，时间list内为当前时段内的状态，此处表示这10分钟一直处于occupied状态
        ...
        ...
        tn:[[T1],[free]]                                         ===>10分钟内只有一次状态变化
    },
    'did-2':{
        t1:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]],
        t2:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]],
        ...
        ...
        tn:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]]
    },
    ...
    ...
    ...
    'did-n':{
        t1:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]],
        t2:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]],
        ...
        ...
        tn:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]]
    }
}
'''

# 循环res字典
for uid in res.keys():
    for gap in res[uid].keys():
        occupied_time = 0.0
        first_record = ''
        last_record = ''
        util = 0.0
        begin = start + gap * TIMESTEP
        end = start + (gap+1) * TIMESTEP

        # 如果当前数据list长度为0，则代表10分钟内状态没变，直接根据状态名称获取0或100的占用值
        if len(res[uid][gap][1]) == 0:
            if res[uid][gap][0] == 'free':
                util = format(0.0 * 100.0, '.3f')
            elif res[uid][gap][0] == 'occupied':
                util = format(1.0 * 100.0, '.3f')

        # 如果当前数据list长度为1，则代表10分钟内状态只变化过1次。若状态变化为占用，则选取变化后的时间计算。若状态变化为空闲，则选取变化前的时间进行计算。
        elif len(res[uid][gap][1]) == 1:
            first_gap = res[uid][gap][0][0] - begin
            last_gap = end - res[uid][gap][0][0]

            if res[uid][gap][1][0] == 'occupied':
                util = format(last_gap / TIMESTEP * 100.0, '.3f')
            elif res[uid][gap][1][0] == 'free':
                util = format(first_gap / TIMESTEP * 100.0, '.3f')

        # 若数据长度超过1，则先选取开始到第一个数据和最后一个数据到结尾的时间。
        # 然后根据第一个以及最后一个状态决定是否删除开头及结尾的状态。
        # 处理完成后，剩下的依次计算占用时间
        # 最后根据原本开头和结尾的状态决定占用时间需不需要加上最开始选取的开头/结尾时间
        else:
            first_gap = res[uid][gap][0][0] - begin
            last_gap = end - res[uid][gap][0][-1]

            if res[uid][gap][1][0] == 'free':
                first_record = 'free'
                del res[uid][gap][0][0], res[uid][gap][1][0]

            if res[uid][gap][1][-1] == 'occupied':
                last_record = 'occupied'
                del res[uid][gap][0][-1], res[uid][gap][1][-1]

            indexList = [i for i in range(len(res[uid][gap][1])) if res[uid][gap][1][i] == 'occupied']

            for j in indexList:
                occupied_time += ((res[uid][gap][0][j+1] - res[uid][gap][0][j]) * 1.0)

            if first_record:
                occupied_time += first_gap

            if last_record:
                occupied_time += last_gap

            util = format(occupied_time / TIMESTEP * 100, '.3f')

        occ[uid][gap] = util

with open("C:\\Users\Andyn\Desktop\AZ data\\447223_06_occData_30min.csv", 'a+', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    for did in occ.keys():
        for gap in occ[did].keys():
            epoch_time = (int(gap) * TIMESTEP + start) / 1000.0
            recordTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch_time))
            util = occ[did][gap]
            writer.writerow([did,recordTime,util])
    f.close()

print("Job done!!!!!!!")
print(occ)