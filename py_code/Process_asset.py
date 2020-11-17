#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import time
import csv

# 导入数据
data = pd.read_csv('c:\\LOG\\data.csv', parse_dates=[2])

# 建立时间轴
min = data['TIME'].min()
max = data['TIME'].max()
Gridlist = pd.date_range(min.replace(microsecond=0, second=0, minute=min.minute//5*5), max+pd.DateOffset(minutes=5), freq='5T')
Gridlist = pd.DataFrame(Gridlist, columns=['Time'])

# 数据处理

# 建立ID列表
IDSet = set(data['ID'].values)

# 每个ID循环处理
for id in IDSet:
    data_per_ID = data[data.ID == id]


data1 = data.values.tolist()


# 删除MS数据
for i in range(1, len(data1)-1):
    if data1[i][1] == 'missingInput':  # 检查event是否为ms
        data1[i][2] = ''

data1 = list(filter(lambda x: x[2] != '', data1))


# 删除重复in out数据
for i in range(1, len(data1)-1):
    if (data1[i][0] == data1[i-1][0] and data1[i][1] == data1[i-1][1] and data1[i][0] == data1[i+1][0]) or data1[i][2] == 'ms':
        data1[i][2] = ''

data2 = filter(lambda x: x[2] != '', data1)
data3 = pd.DataFrame(data2, columns=['ID', 'Event', 'Time'])

Gridresult = Gridlist.copy().set_index('Time')
Gridresult['occ'] = 0.00
# Gridresult=Gridlist.copy().set_index('Time')

# Gridresult.dtypes

# data3.shape

biglist = pd.merge(data3, Gridlist, how='outer')

biglist.shape

biglist[pd.isna(biglist['ID'])]
biglist.sort_values(by=['Time'], inplace=True)


# flag 前面的状态
flag = ''
Gridresult['occ'] = np.nan  # 清空记录

# post :要写进数据的时间格子
for i in range(len(biglist)):
    event = biglist.iloc[i, 1]
    stamp = biglist.iloc[i, 2]  # 事件时间戳
    print(stamp)

    if pd.isna(event):
        # 说明这是一个插入的格子时间.没有事件,延续当前状态
        print('   Not a event')
        post = stamp
        if flag == 'free':  #
          #                 Gridresult.at[post,'occ']= 0.0000 #写入后一个格子
            if pd.isna(Gridresult.at[post, 'occ']):
                Gridresult.at[post, 'occ'] = 0.0
            print('    continue ot', post, Gridresult.at[post, 'occ'])

        elif flag == 'occupied':
            if pd.isna(Gridresult.at[post, 'occ']):
                Gridresult.at[post, 'occ'] = 1.0

#                 Gridresult.at[post,'occ']= 1.0000
    # print('    continue in', post, Gridresult.at[post,'occ'])

#         print('grid post',stamp,post,Gridresult.at[post,'occ'])

    else:  # 只是数据记录
        print('   is  a event')
        post = stamp.replace(microsecond=0, second=0, minute=stamp.minute//5*5)
        nextp = post+pd.DateOffset(minutes=5)
        offset = float((nextp-stamp).seconds/300)
#         post=stamp.replace(microsecond=0,second=0,minute=(stamp.minute//5+1)*5 if ((stamp.minute//5+1)*5!=60) else 0,hour=stamp.hour if ((stamp.minute//5+1)*5!=60) else stamp.hour+1)
        if event == 'ot':
            if pd.isna(Gridresult.at[post, 'occ']):
                Gridresult.at[post, 'occ'] = 1.0
                print('       1.0 asumed', post, Gridresult.at[post, 'occ'])

            print('      ', event, stamp, post, nextp, -offset)
            offset = -offset
            flag = 'ot'
#             offset=stamp-post
#             要在post的格子里面减去offset部分
        elif event == 'in':
            if pd.isna(Gridresult.at[post, 'occ']):
                Gridresult.at[post, 'occ'] = 0.0
                print('       0.0 asumed', post, Gridresult.at[post, 'occ'])

            print('     ',       event, stamp, post, nextp, -offset)
            flag = 'in'
            #             要在post的格子里面加上offset部分
        print('       was', post, Gridresult.at[post, 'occ'], offset)
        Gridresult.at[post, 'occ'] = float(offset+Gridresult.at[post, 'occ']) if (Gridresult.at[post, 'occ']+offset) > 0 else 0.0000
        print('       now recorded', post, Gridresult.at[post, 'occ'])

print(Gridresult)


Gridresult

Gridresult.info()


Gridresult.to_csv("c:\\LOG\\result.csv")

Gridresult
