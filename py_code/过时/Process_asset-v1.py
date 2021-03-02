import pandas as pd
import numpy as np
import time
import csv

# 导入数据
data = pd.read_csv('c:\\LOG\\wf.csv', parse_dates=[2])
data['flag'] = ''

print('records:', len(data))

# 建立时间轴
# 根据数据生成目标时间格子
min = data['TIME'].min()
max = data['TIME'].max()
Gridlist = pd.date_range(min.replace(microsecond=0, second=0, minute=min.minute//5*5), max+pd.DateOffset(minutes=5), freq='5T')
Gridlist = pd.DataFrame(Gridlist, columns=['Time'])

# 数据处理

# 建立ID列表
IDSet = set(data['ID'].values)

# 每个ID循环处理
for id in IDSet:
    print('\nProcessing ', id)
    data_per_ID = data[data.ID == id]
    data1 = data_per_ID.values.tolist()
    print('1 records:', len(data1))

    # 删除MS数据
    for i in range(1, len(data1)-1):
        if data1[i][1] == 'missingInput':  # 检查event是否为ms
            data1[i][2] = ''

    data1 = list(filter(lambda x: x[2] != '', data1))
    print('2 records:', len(data1))

    # 按照时间排序

    def takeSecond(elem):
        return elem[1]
    data1.sort(key=takeSecond)

    # 删除重复in out数据
    for i in range(1, len(data1)-1):
        if data1[i][1] == data1[i-1][1]:
            data1[i][3] = ''

    data2 = list(filter(lambda x: x[3] != 'x', data1))
    data3 = pd.DataFrame(data2, columns=['ID', 'Event', 'Time', 'Flag'])
    print('3 records:', len(data3))

    # data3 = data3.sort_values()

    Gridresult = Gridlist.copy().set_index('Time')
    Gridresult['occ'] = 0.00

    # 先生成计算需要的时间格子
    biglist = pd.merge(data3, Gridlist, how='outer')

    print('Target records to go:', biglist.shape)

    # biglist[pd.isna(biglist['ID'])]
    # biglist.sort_values(by=['Time'], inplace=True)

    # flag : 前面的状态
    flag = ''
    Gridresult['occ'] = np.nan  # 建立空记录
    # biglist = biglist.values.tolist()
    # print(biglist.shape[0])
    # post :要写进数据的时间格子
    print(biglist.shape[0])
    for i in range(biglist.shape[0]):
        event = biglist.iloc[i, 1]
        stamp = biglist.iloc[i, 2]  # 事件时间戳

        if pd.isna(event):            # !!说明这是一个插入的格子时间.没有事件,延续当前状态
            # print('No event', end='..')
            post = stamp
            if flag == 'free':  #
                #                 Gridresult.at[post,'occ']= 0.0000 #写入后一个格子
                if pd.isna(Gridresult.at[post, 'occ']):
                    Gridresult.at[post, 'occ'] = 0.0
                    print('    continue 0 ', post, Gridresult.at[post, 'occ'])

            elif flag == 'occupied':
                if pd.isna(Gridresult.at[post, 'occ']):
                    Gridresult.at[post, 'occ'] = 1.0
                    print('    continue 1 ', post, Gridresult.at[post, 'occ'])

        else:   # !!说明这是一个事件
            print(stamp, event, end='..')
            post = stamp.replace(microsecond=0, second=0, minute=stamp.minute//5*5)  # 记入格子:就是前一个整五分
            nextp = post+pd.DateOffset(minutes=5)
            offset = float((nextp-stamp).seconds/300)
            if event == 'free':
                if pd.isna(Gridresult.at[post, 'occ']):
                    Gridresult.at[post, 'occ'] = 1.0
                    print('-- 1.0 assumed', post, Gridresult.at[post, 'occ'])

                print('  !!!    ', event, stamp, post, nextp, -offset)
                offset = -offset
                flag = 'free'
    #             offset=stamp-post
    #             要在post的格子里面减去offset部分
            elif event == 'occupied':
                if pd.isna(Gridresult.at[post, 'occ']):
                    Gridresult.at[post, 'occ'] = 0.0
                    print('-- 0.0 assumed', post, Gridresult.at[post, 'occ'])

                print('  ???  ',       event, stamp, post, nextp, -offset)
                flag = 'ocuupied'
                #             要在post的格子里面加上offset部分
            print(' -- was', post, Gridresult.at[post, 'occ'], offset)
            Gridresult.at[post, 'occ'] = float(offset+Gridresult.at[post, 'occ']) if (Gridresult.at[post, 'occ']+offset) > 0 else 0.0000
            print(' -- now recorded', post, Gridresult.at[post, 'occ'])
            Gridresult['ID'] = id

    # print(Gridresult.info())
    Gridresult = Gridresult[Gridresult.occ > 0]

    Gridresult.to_csv("c:\\LOG\\result.csv", mode='a+')

# Gridresult
