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
# locationID = "797296"  # nf

# locationID = "368307"  # yuanjin1
locationID = "834706"  # yuanjin2
# locationID = "234190"  # yuanjin3
# locationID = "251092"  # yuanjin4
# locationID = "725728"  # yuanjin5
# locationID = "503370"  # 万科

startstr = '2021-02-28-09-00-00'
endstr = '2021-04-29-17-59-59'
datatype = 'UPLOG'  # Motion | UUID  #选择要采的数据类型
timeGrid='30T' #统计间隔 30T 是30分钟



"""
程序部分
"""
CSVheader = True
splitDays = 20 if datatype == 'UUID' else 1
filename = "C:\\LOG\\"+locationID+"_"+startstr+"_"+endstr+'_'+datatype+"_PCT.csv"
filename1 = "C:\\LOG\\"+locationID+"_"+startstr+"_"+endstr+'_'+datatype+"_RAW.csv"

patternr = '%Y-%m-%d-%H-%M-%S'
patternw = '%Y-%m-%d %H:%M:%S'

startdt = datetime.fromtimestamp(
    (time.mktime(time.strptime(startstr, patternr))))
enddt = datetime.fromtimestamp((time.mktime(time.strptime(endstr, patternr))))

datalists = []
motionRecordList = [] # motion记录数组,包含ID 时间戳 时间
requestcount = 0
HBFlag = 0
msgQue = deque()
count = 0
ws=websocket



def calOccupancy():
    global CSVheader, startdt, enddt
    data = pd.DataFrame(motionRecordList, columns=['ID', 'EVENT', 'TIME'])
    data.to_csv(filename1,index=None)
    data['TIME'] = pd.to_datetime(data['TIME'])
    data['flag'] = '' #加入第三列,作为以后处理的标志位
    
    #删除单个记录的传感器:可能是已删除的
    data = pd.concat([data, data.drop_duplicates('ID', keep=False)]).drop_duplicates(keep=False)
    
    
    #解决数据中时间记录早于设定的起点的问题
    startdt1=np.datetime64(startdt)
    data['TIME'] =np.where(data['TIME']>startdt1,data['TIME'],startdt1)
    
    print('Total history records:', len(data))

    # 建立时间轴
    # 根据数据生成目标时间格子
    
    
    # #取尽量大的事件间隔，保证数据没有空洞
    # min = startdt if startdt < data['TIME'].min() else data['TIME'].min()  
    # max = enddt if enddt > data['TIME'].max() else data['TIME'].max()  
    
    #！！！ 如果有传感器长期掉线，上面的计算会导致起始日期很长！！！则采用下面的方式
    min = startdt 
    max = enddt 
    
    #!! 初始日提前一天
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
        if len(data1) < 2: 
            print('dead sensor, skiped ')
            continue
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

        print('Target raw records to go:', biglist.shape[0])

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
                # print(stamp, event, end='..')
                post = stamp.replace(microsecond=0, second=0, minute=stamp.minute//5*5)  # 记入格子:就是前一个整五分
                #防止有异常的记录,是在时间范围之外的
                if post<min:
                    post=min
                    
                nextp = post+pd.DateOffset(minutes=5)
                offset = float((nextp-stamp).seconds/300)
                if event == 'free':
                    if pd.isna(Gridresult.at[post, 'PCT']): #!!如果有记录在start和end之外.造成POST出没有记录,会报错
                        Gridresult.at[post, 'PCT'] = 1.0
                        # print('-- 1.0 assumed', post, Gridresult.at[post, 'PCT'])

                    # print('  !!!    ', event, stamp, post, nextp, -offset)
                    offset = -offset
                    flag = 'free'
        #             offset=stamp-post
        #             要在post的格子里面减去offset部分
                elif event == 'occupied':
                    if pd.isna(Gridresult.at[post, 'PCT']):
                        Gridresult.at[post, 'PCT'] = 0.0
                        # print('-- 0.0 assumed', post, Gridresult.at[post, 'PCT'])

                    # print('  ???  ',       event, stamp, post, nextp, -offset)
                    flag = 'occupied'
                    #             要在post的格子里面加上offset部分
                # print(' -- was', post, Gridresult.at[post, 'PCT'], offset)
                Gridresult.at[post, 'PCT'] = float(offset+Gridresult.at[post, 'PCT']) if (Gridresult.at[post, 'PCT']+offset) > 0 else 0.0000
                # print(' -- now recorded', post, Gridresult.at[post, 'PCT'])

        # print(Gridresult.info())
        # # Gridresult = Gridresult[Gridresult.occ >= 0]
        # filter= [Gridresult['PCT'] >= 0.5]
        # Gridresult.reset_index()

        Gridresult = Gridresult[Gridresult['TIME'] >= startdt]
        Gridresult = Gridresult[Gridresult['TIME'] <= enddt]
        Grid = Gridresult
        # Grid= Grid[Grid[]<=enddt]

        Grid.set_index('TIME', inplace=True)
        #!! 重取样
        if timeGrid!='5T':
            Grid = Grid.resample(timeGrid, axis=0).mean()
        Grid['ID'] = id
        
        # Grid.plot()

        Grid.to_csv(filename, header=CSVheader, mode='a+')
        CSVheader = False
    print('Calculation finished,check file :',filename)
    sys.exit(0)
    

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
        # pprint(unitslist)
        if datatype=='UUID':
            for unit in unitslist:
                if 'UUID' in unit['unitAddress']['did']:
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'], locationID, startdt, numberOfSamplesBeforeStart=1)  #注意修改
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'], locationID, startdt, enddt)
        elif datatype=='Motion':
            for unit in unitslist:
                if 'Motion' in unit['unitAddress']['did']:
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'], locationID, startdt, numberOfSamplesBeforeStart=1)
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'], locationID, startdt, enddt)
        elif datatype=='UPLOG':
            for unit in unitslist:
                if 'EUID' in unit['unitAddress']['did']:
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'][0:22], locationID, startdt, numberOfSamplesBeforeStart=1)
                    sendGetSamplesRequest(
                        unit['unitAddress']['did'][0:22], locationID, startdt, enddt)
           
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
                    motionRecordList.append([response['sampleListDto']['dataSourceAddress']
                                    ['did'], li['assetState']['name'], eventtime])
                elif li['resourceType'] == 'SampleMotion':
                    # print(response['sampleListDto']['dataSourceAddress']['did'], eventtime, li['value'])
                    motionRecordList.append([response['sampleListDto']['dataSourceAddress']
                                    ['did'], li['value'], eventtime])
        if requestcount == 0:
            print('\n', datetime.now(), ' >>>>Historical data retrieved<<<<')
            ws.close()
            # rt.stop()
            calOccupancy()
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

    global locationID, startstr, endstr, datatype, ws, filename, filename1,startdt,enddt,datatype
    if location_id != '':
        locationID = location_id
    if start_str != '':
       startstr = start_str
    if end_str != '':
       endstr = end_str
    if data_type != '':
       datatype = data_type
       
    
    filename = "C:\\LOG\\"+locationID+"_"+startstr+"_"+endstr+'_'+datatype+"_PCT.csv"
    filename1 = "C:\\LOG\\"+locationID+"_"+startstr+"_"+endstr+'_'+datatype+"_RAW.csv"
    
    delfile(filename)
    delfile(filename1)
    

    startdt = datetime.fromtimestamp(
        (time.mktime(time.strptime(startstr, patternr))))
    enddt = datetime.fromtimestamp((time.mktime(time.strptime(endstr, patternr))))

    print('Serving request:',location_id, start_str, end_str, data_type, '\n')
    print('default:',locationID, startstr, endstr, datatype, filename, filename1)

    print(datetime.now(), " Connecting to ",
          cirrusHost, "with user ", username)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage, on_close=onClose, on_open=onOpen, keep_running=True)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

   
if __name__ == "__main__":
    get_motion_history()
    os._exit(0)



