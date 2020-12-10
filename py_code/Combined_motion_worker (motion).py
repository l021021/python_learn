#现在要在start之前多读去一条记录
"""
@author: Bruce

"""

import datetime
import json
import ssl
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from pprint import pprint
from time import sleep
from RT import RepeatedTimer

import matplotlib as plt
import numpy as np
import pandas as pd
import websocket

# The cirrus host to connect to:
cirrusHost = "cirrus20.yanzi.se"

#!! 需要修改的部分
# Change the username and password to the Yanzi credentials:
username = 'frank.shen@pinyuaninfo.com'
password = 'Ft@Sugarcube99'
# locationID = "879448"  # snf
# locationID = "655623"
# locationID = "74365"  # kerry
# locationID = "229349"  # ft
# locationID = "521209"  # wf
locationID = "797296"  # nf
startstr = '2020-11-01-00-00-00'
endstr = '2020-11-30-23-59-59'



"""
程序部分
"""
CSVheader = True
datatype = 'UUID'  # Motion | UUID | TEMP ...
splitDays = 20 if datatype == 'UUID' else 1
filename = "C:\\LOG\\"+locationID+"_"+startstr+"_"+endstr+'_'+datatype+"_PCT.csv"
patternr = '%Y-%m-%d-%H-%M-%S'
patternw = '%Y-%m-%d %H:%M:%S'

startdt = datetime.fromtimestamp(
    (time.mktime(time.strptime(startstr, patternr))))
enddt = datetime.fromtimestamp((time.mktime(time.strptime(endstr, patternr))))

datalists = []
csvlist = []
requestcount = 0
HBFlag = 0
msgQue = deque()
count = 0


def calOccupancy():
    global CSVheader, startdt, enddt
    data = pd.DataFrame(csvlist, columns=['ID', 'EVENT', 'TIME'])
    data['TIME'] = pd.to_datetime(data['TIME'])
    data['flag'] = '' #加入第三列,作为以后处理的标志位
    # startdt=startdt
    # enddt=enddt

    print('records:', len(data))

    # 建立时间轴
    # 根据数据生成目标时间格子
    min = startdt if startdt < data['TIME'].min() else data['TIME'].min()  
    max = enddt if enddt > data['TIME'].max() else data['TIME'].max()  
    # min = data['TIME'].min()
    # max = data['TIME'].min()
    # Gridlist = pd.date_range(min.replace(microsecond=0, second=0, minute=min.minute//5*5), max+pd.DateOffset(minutes=5), freq='5T')
    # Gridlist = pd.date_range(min, max, freq='5T')
    Gridlist = pd.date_range(min.replace(microsecond=0, second=0, minute=min.minute//5*5), max+pd.DateOffset(minutes=5), freq='5T')

    Gridlist = pd.DataFrame(Gridlist, columns=['TIME'])

    # 数据处理

    # 建立ID列表
    IDSet = set(data['ID'].values)

    # 每个ID循环处理
    for id in IDSet:
        print('\nProcessing ', id)
        data_per_ID = data[data.ID == id]
        data1 = data_per_ID.values.tolist()
        print('raw records:', len(data1))

        # 删除MS数据
        for i in range(1, len(data1)-1):
            if data1[i][1] == 'missingInput':  # 检查event是否为ms
                data1[i][3] = 'x'

        data1 = list(filter(lambda x: x[3] != 'x', data1))
        print('after ms records deleted:', len(data1))

        # 按照时间排序

        def takeSecond(elem):
            return elem[2]
        data1.sort(key=takeSecond)

        # 把value转变成free/occupied
        if datatype == "Motion":
            for i in range(1, len(data1)):
                if data1[i][1] == data1[i-1][1]:
                    data1[i][3] = 'free'
                else:
                    data1[i][3] = 'occupied'
            data1[0][3] = 'free'
            for i in range(len(data1)):
                data1[i][1] = data1[i][3]
        
        
        # 删除重复free/occupied数据
        for i in range(1, len(data1)-1):
            if data1[i][1] == data1[i-1][1]:
                data1[i][3] = 'x'
                
        data2 = list(filter(lambda x: x[3] != 'x', data1))
        
            
        
        data3 = pd.DataFrame(data2, columns=['ID', 'Event', 'TIME', 'Flag'])
        print('after dup records deleted:', len(data3))

        # data3 = data3.sort_values()

        Gridresult = Gridlist.set_index('TIME', drop=False)
        Gridresult['PCT'] = 0.00
        print('results in (GRID):', len(Gridresult))

        # 先生成计算需要的时间格子
        biglist = pd.merge(data3, Gridlist, how='outer')

        print('Target records to go:', biglist.shape[0])

        # biglist[pd.isna(biglist['ID'])]
        biglist.sort_values(by=['TIME'], inplace=True)

        # flag : 前面的状态
        flag = 'free'  # 初识状态为空
        Gridresult['PCT'] = np.nan  # 建立空记录
        # biglist = biglist.values.tolist()
        # print(biglist.shape[0])
        # post :要写进数据的时间格子
        for i in range(biglist.shape[0]):
            event = biglist.iloc[i, 1]
            stamp = biglist.iloc[i, 2]  # 事件时间戳

            if pd.isna(event):            # !!说明这是一个插入的格子时间.没有事件,延续当前状态
                # print('Not event', end='..')
                post = stamp
                if flag == 'free':  #
                    #                 Gridresult.at[post,'occ']= 0.0000 #写入后一个格子
                    if pd.isna(Gridresult.at[post, 'PCT']):
                        Gridresult.at[post, 'PCT'] = 0.0
                        # print('    continue 0 ', post, Gridresult.at[post, 'occ'])

                elif flag == 'occupied':
                    if pd.isna(Gridresult.at[post, 'PCT']):
                        Gridresult.at[post, 'PCT'] = 1.0
                        # print('    continue 1 ', post, Gridresult.at[post, 'occ'])

            else:   # !!说明这是一个事件
                print(stamp, event, end='..')
                post = stamp.replace(microsecond=0, second=0, minute=stamp.minute//5*5)  # 记入格子:就是前一个整五分
                nextp = post+pd.DateOffset(minutes=5)
                offset = float((nextp-stamp).seconds/300)
                if event == 'free':
                    if pd.isna(Gridresult.at[post, 'PCT']):
                        Gridresult.at[post, 'PCT'] = 1.0
                        print('-- 1.0 assumed', post, Gridresult.at[post, 'PCT'])

                    print('  !!!    ', event, stamp, post, nextp, -offset)
                    offset = -offset
                    flag = 'free'
        #             offset=stamp-post
        #             要在post的格子里面减去offset部分
                elif event == 'occupied':
                    if pd.isna(Gridresult.at[post, 'PCT']):
                        Gridresult.at[post, 'PCT'] = 0.0
                        print('-- 0.0 assumed', post, Gridresult.at[post, 'PCT'])

                    print('  ???  ',       event, stamp, post, nextp, -offset)
                    flag = 'occupied'
                    #             要在post的格子里面加上offset部分
                print(' -- was', post, Gridresult.at[post, 'PCT'], offset)
                Gridresult.at[post, 'PCT'] = float(offset+Gridresult.at[post, 'PCT']) if (Gridresult.at[post, 'PCT']+offset) > 0 else 0.0000
                print(' -- now recorded', post, Gridresult.at[post, 'PCT'])

        # print(Gridresult.info())
        # Gridresult = Gridresult[Gridresult.occ >= 0]
        # Gridresult.plot()
        # Grid30 = Gridresult.resample('30T', axis=0).mean()
        # filter= [Gridresult['PCT'] >= 0.5]
        # Gridresult.reset_index()

        Gridresult = Gridresult[Gridresult['TIME'] >= startdt]
        Gridresult = Gridresult[Gridresult['TIME'] <= enddt]
        Grid = Gridresult
        # Grid= Grid[Grid[]<=enddt]

        Grid['ID'] = id
        Grid.set_index('TIME', inplace=True)
        # Grid.plot()

        Grid.to_csv(filename, header=CSVheader, mode='a+')
        CSVheader = False
    print('Calculation finished,check file :',filename)
    sys.exit(1)
    


def sendPeriodicRequest():
    global HBFlag
    request = {"messageType": "PeriodicRequest",
               "timeSent": int(time.time() * 1000)}
    HBFlag += 1
    if HBFlag >= 3:
        print('(', HBFlag, 'periodic request sent )')
        print('Should disconnect ')  # !!

    else:
        print('(', HBFlag, 'periodic request missed )')
    sendMessage(request)


rt = RepeatedTimer(30, sendPeriodicRequest)


def onMessage(ws, message):
    global rt
    response = json.loads(message)
    sendFromQue()
    # print('response')
    global requestcount, HBFlag
    # HBFlag = 0

    if response["messageType"] == "ServiceResponse":
        print("Got ServiceResponse, sending login request")
        # We got a service response, let’s try to login:
        sendLoginRequest()
    elif response["messageType"] == "LoginResponse":
        if (response['responseCode']['name'] == 'success'):
            sendGetUnitsRequest(locationID)
            sendPeriodicRequest()
        else:
            sys.exit(-1)
    elif response["messageType"] == "PeriodicResponse":
        HBFlag = 0
        print("( periodic response rcvd )")
        rt.start()

    elif response["messageType"] == "SubscribeData":
        print('  Subscription     :', response)
        # print("onMessage: Got SubscribeData")
    elif response["messageType"] == "GetUnitsResponse":
        print("Requesting for records:")
        unitslist = response['list']
        # pprint(unitslist)
        if datatype=='UUID':
            for unit in unitslist:
                if 'UUID' in unit['unitAddress']['did']:
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'], locationID, startdt, numberOfSamplesBeforeStart=1)
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'], locationID, startdt, enddt)
        elif datatype=='Motion':
            for unit in unitslist:
                if 'Motion' in unit['unitAddress']['did']:
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'], locationID, startdt, numberOfSamplesBeforeStart=1)
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'], locationID, startdt, enddt)
           
    elif response["messageType"] == "GetSamplesResponse":
        requestcount = requestcount - 1
        """处理了返回列表中list不存在的情况,这是因为时间段内没有事件
        """
        if (response['responseCode']['name'] == 'success') and ('list' in response['sampleListDto']):  # response['responseCode']['name']
            datalists = response['sampleListDto']['list']
            global count
            count += len(datalists)
            print(count,  end='>')
            for li in datalists:
                eventtime = datetime.fromtimestamp(
                    int(li['sampleTime']/1000)).strftime(patternw)
                if li['resourceType'] == "SampleAsset":
                    csvlist.append([response['sampleListDto']['dataSourceAddress']
                                    ['did'], li['assetState']['name'], eventtime])
                elif li['resourceType'] == 'SampleMotion':
                    print(response['sampleListDto']['dataSourceAddress']
                          ['did'], eventtime, li['value'])
                    csvlist.append([response['sampleListDto']['dataSourceAddress']
                                    ['did'], li['value'], eventtime])
        if requestcount == 0:
            print('\n', datetime.now(), ' Mission Accomplished')
            rt.stop()
            ws.close()
            calOccupancy()
            sys.exit(0)
    else:
        print(response)


# def onError(ws, error):
#     print("Error", error)


def onClose(ws):
    print("\n----Connection to Cloud closed----\n")


def onOpen(ws):
    print("Sending service request")
    sendServiceRequest()
    # periodTimer(1, sendPeriodicRequest())


def sendFromQue():
    global msgQue
    if len(msgQue) != 0:
        ws.send(msgQue.pop())


def sendMessage(message):
    if ws.sock.connected != True:
        print("sendMessage: Could not send cirrus message, socket not open")
        return
    try:
        message['timeSent'] = int(time.time() * 1000)
        msg = json.dumps(message)
        ws.send(msg)
    except:
        print("sendMessage: Could not send cirrus message: ", message)
        return
# append message in global que


def sendMessagetoQue(message):
    global msgQue
    message['timeSent'] = int(time.time() * 1000)
    msg = json.dumps(message)
    msgQue.append(msg)
    # print('sending message')
    if len(msgQue) < 5:  # !! window size
        ws.send(msgQue.pop())


def sendServiceRequest():
    request = {"messageType": "ServiceRequest",
               "version": "1.6.4", "clientId": "123456"}
    sendMessagetoQue(request)


def sendSubscribeRequest(location_id, datatype):
    for type in datatype:
        request = {
            "messageType": "SubscribeRequest",
            "timeSent": int(time.time() * 1000),
            "unitAddress": {
                "resourceType": "UnitAddress",
                "locationId": location_id
            },
            "subscriptionType": {
                "resourceType": "SubscriptionType",
                "name": type  #
            }
        }
        sendMessagetoQue(request)
        # print('      ', request)


def sendGetUnitsRequest(locationID):
    request = {"messageType": 'GetUnitsRequest', "timeSent": int(time.time(
    ) * 1000), "locationAddress": {"resourceType": 'LocationAddress', "locationId": locationID}}
    print('sending getunits request for ' + locationID)
    sendMessagetoQue(request)


def sendLoginRequest():
    request = {"messageType": "LoginRequest",
               "username": username, "password": password}
    sendMessagetoQue(request)


def sendGetSamplesRequest(UnitDid, LocationId, start, end='', numberOfSamplesBeforeStart=0):
    # print('    ---', start, end, '  ')
    if end != '':
        if (end - start) <= timedelta(days=splitDays):  # !!为了保证返回的记录数不大于2000,限制时段的天数
            request = {
                "messageType": "GetSamplesRequest",
                "dataSourceAddress": {
                    "resourceType": "DataSourceAddress",
                    "did": UnitDid,
                    "locationId": LocationId
                },
                "timeSerieSelection": {
                    "resourceType": "TimeSerieSelection",
                    # "numberOfSamplesBeforeStart": 3,
                    "timeStart": int(start.timestamp())*1000,
                    # "timeStart" : int((time.time() - (60 * 3600)) * 1000), # 24 hours
                    "timeEnd": int(end.timestamp())*1000
                    # "timeEnd" : int((time.time() - (0 * 3600)) * 1000)
                }
            }
            # print(request["timeSerieSelection"]['timeStart'])
            global requestcount
            requestcount += 1
            print(requestcount, end='.')  # 增加请求计数器
            # pprint(request)
            sendMessagetoQue(request)
        else:
            # print('2')
            # time_stamp = int(time.mktime(time.strptime(start, pattern) + 24 * 3600*1000))
            # startday = datetime.fromtimestamp(time.mktime(time.strptime(start, pattern)))
            start_time_plus_splitDays = start+timedelta(days=splitDays)
            # start_time_plus_1D = start+datetime.timedelta(days=20)
            sendGetSamplesRequest(UnitDid, LocationId, start,
                                  start_time_plus_splitDays)
            sendGetSamplesRequest(UnitDid, LocationId,
                                  start_time_plus_splitDays, end)
    elif numberOfSamplesBeforeStart != 0:
        request = {
            "messageType": "GetSamplesRequest",
            "dataSourceAddress": {
                "resourceType": "DataSourceAddress",
                "did": UnitDid,
                "locationId": LocationId
            },
            "timeSerieSelection": {
                "resourceType": "TimeSerieSelection",
                # "numberOfSamplesBeforeStart": 3,
                "timeStart": int(start.timestamp())*1000,
                "numberOfSamplesBeforeStart": numberOfSamplesBeforeStart
            }
        }
        requestcount += 1
        print(requestcount, end='.')  # 增加请求计数器
        sendMessagetoQue(request)
    else:
        return(-1)


if __name__ == "__main__":
    print(datetime.now(), " Connecting to ",
          cirrusHost, "with user ", username)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage, on_close=onClose, on_open=onOpen, keep_running=True)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
    # periodTimer = threading.Timer(500, sendPeriodicRequest())
