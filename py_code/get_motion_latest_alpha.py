#现在要在start之前多读去一条记录
"""
@author: Bruce
首先通过调用历史书籍接口,把数据(ID 时间 事件)存在数组里,第二部处理数组,存入文件

需要填 起止时间 
       location
       数据类型
       
       文件的默认目录是 c:\LOG
       统计间隔是5分钟以上,可选.
"""

import datetime
import json
import ssl,os
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from pprint import pprint
from time import sleep
from threading import Timer
import numpy as np
from numpy.lib.function_base import gradient
import pandas as pd
import websocket
cirrusHost = "cirrus.ifangtang.net"


# The cirrus host to connect to:
username = 'frank.shen@sugarinc.cn'
password = 'iFangtang#899'

# locationID = "879448"  # snf
# locationID = "655623"
# locationID = "74365"  # kerry
# locationID = "229349"  # ft
# locationID = "521209"  # wf
locationID = "797296"  # nf

# locationID = "368307"  # yuanjin1
# locationID = "834706"  # yuanjin2
# locationID = "234190"  # yuanjin3
# locationID = "251092"  # yuanjin4
# locationID = "725728"  # yuanjin5
# locationID = "503370"  # 万科

# startstr = '2021-01-01-00-00-00' #!!这些参数会被传入的参数替代
# endstr = '2021-03-31-23-59-59'
datatype = 'UUID'  # Motion | UUID  #选择要采的数据类型

"""!! 采集何种数据和粒度:UUID和MOTION 相差可能有几十倍, 而不同的粒度,计算差异不大,但是对生成的数据差异较大
    建议大部分情况下,UUID/15分钟
    特殊需要 Motion/1分钟

"""
gran=15
timeGrid=str(gran)+'T' #!!统计间隔 30T 是30分钟 这个不会被调度程序传入而覆盖




"""
程序部分
"""
CSVheader = True
splitDays = 20 if datatype == 'UUID' else 1
# filename = "C:\\LOG\\"+locationID+"_"+startstr+"_"+endstr+'_'+datatype+'_'+timeGrid+"_PCT.csv"
filename1 = "C:\\LOG\\"+locationID+"_"+str(datetime.now())+'_'+datatype+'_'+"_RAW.csv"

patternr = '%Y-%m-%d-%H-%M-%S'
patternw = '%Y-%m-%d %H:%M:%S'

# startdt = datetime.fromtimestamp(
#     (time.mktime(time.strptime(startstr, patternr))))
# enddt = datetime.fromtimestamp((time.mktime(time.strptime(endstr, patternr))))

datalists = []
motionRecordList = [] # motion记录数组,包含ID 时间戳 时间
requestcount = 0
HBFlag = 0
msgQue = deque()
count = 0
ws=websocket




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
            sys.exit(0)
    elif response["messageType"] == "PeriodicResponse":
        HBFlag = 0
        print("( periodic response rcvd )")
        # rt.start()

    elif response["messageType"] == "SubscribeData":
        print('  Subscription     :', response)
        # print("onMessage: Got SubscribeData")
    elif response["messageType"] == "GetUnitsResponse":
        print("Requesting for records:")
        unitslist = response['list']
        # !! 根据返回的清单,诸葛发出调用历史数据的接口
        if datatype=='UUID':
            for unit in unitslist:
                if 'UUID' in unit['unitAddress']['did']:  #! 只取前一条
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'], locationID, datetime.now(), numberOfSamplesBeforeStart=1)  #返回一个开发之前的数据,满足计算所需的边界!
                    # sendGetSamplesRequest(
                    #     unit['unitAddress']['did'], locationID, startdt, enddt)
        elif datatype=='Motion':
            for unit in unitslist:
                if 'Motion' in unit['unitAddress']['did']:
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'], locationID, startdt, numberOfSamplesBeforeStart=1)
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'], locationID, startdt, enddt)
           
    elif response["messageType"] == "GetSamplesResponse":
        requestcount = requestcount - 1  #!对查询进行计算,全部返回后退出
        """处理了返回列表中list不存在的情况,这是因为时间段内没有事件
        """
        if (response['responseCode']['name'] == 'success') and ('list' in response['sampleListDto']):  # response['responseCode']['name']
            datalists = response['sampleListDto']['list']
            global count #!全部数据条数
            count += len(datalists)
            print(count,  end='>')
            for li in datalists:
                eventtime = datetime.fromtimestamp(
                    int(li['sampleTime']/1000)).strftime(patternw)
                if li['resourceType'] == "SampleAsset":
                    motionRecordList.append([response['sampleListDto']['dataSourceAddress']
                                    ['did'], li['assetState']['name'], eventtime])
                    print([response['sampleListDto']['dataSourceAddress']
                           ['did'], li['assetState']['name'], eventtime])
                elif li['resourceType'] == 'SampleMotion':
                    # print(response['sampleListDto']['dataSourceAddress']['did'], eventtime, li['value'])
                    motionRecordList.append([response['sampleListDto']['dataSourceAddress']
                                    ['did'], li['value'], eventtime])  #!记录的是counter value
        if requestcount == 0:
            print('\n', datetime.now(), ' >>>>Historical data retrieved<<<<')
            ws.close()
            # rt.stop()
            # calOccupancy()
            sys.exit(0)
    else:
        print(response)



def onClose(ws):
    print("\n----Connection to Cloud closed----\n")
    # rt.stop()
    


def onOpen(ws):
    print("Sending service request")
    sendServiceRequest()
    # periodTimer(1, sendPeriodicRequest())


def sendFromQue():
    global msgQue,ws
    if len(msgQue) != 0:
        ws.send(msgQue.pop())


def sendMessage(message):
    # global ws
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
    global msgQue,ws
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

def delfile(filename):
    if os.path.exists(filename):
        os.remove(filename)
    
def get_motion_history(location_id='', start_str='', end_str='', data_type=''):
    #可以带参数进来,否则就默认是文件头的location,start...

    global locationID, startstr, endstr, datatype, ws, filename, filename1,startdt,enddt
    if location_id != '':
        locationID = location_id
    if start_str != '':
       startstr = start_str
    if end_str != '':
       endstr = end_str
    if data_type != '':
       datatype = data_type
       

    filename1 = "C:\\LOG\\"+locationID+"_"+str(datetime.now())+'_'+datatype+'_'+"_RAW.csv"
    delfile(filename1)
    

    print(datetime.now(),'Serving request:', location_id, start_str, end_str, data_type, '\n')

    print(" Connecting to ",
          cirrusHost, "with user ", username)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage, on_close=onClose, on_open=onOpen, keep_running=True)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

   
if __name__ == "__main__":
    get_motion_history()
    print(datetime.now(),'Finished')
    os._exit(0)



