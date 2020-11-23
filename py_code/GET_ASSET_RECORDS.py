#现在要在start之前多读去一条记录
"""
@author: Bruce

针对拉取长时间的ASSET
@TODO: periodic 
"""

import csv
import json
import sched
from pprint import pprint
from time import sleep
import ssl
import sys
import time
from datetime import datetime, timedelta
from collections import deque
import websocket
from RT import RepeatedTimer


# The cirrus host to connect to:
cirrusHost = "cirrus20.yanzi.se"

# Change the username and password to the Yanzi credentials:
username = 'frank.shen@pinyuaninfo.com'
password = 'Ft@Sugarcube99'

# locationID = "879448"  # snf
# locationID = "655623"
# locationID = "74365"  # kerry

# locationID = "229349"  # ft
# locationID = "521209"  # wf
locationID = "879448"  # no

startstr = '2020-11-18-12-00-00'
endstr = '2020-11-20-08-00-00'


datatype = 'UUID'  # Motion | UUID | TEMP ...
splitDays = 20 if datatype == 'UUID' else 1

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


def writetofile():
    filename = "C:\\LOG\\"+locationID+"_"+startstr+"_"+endstr+"_RAW.csv"
    with open(filename, 'w', encoding='utf-8', newline='') as f:  # ! 注意修改文件名
        writer = csv.writer(f)
        writer.writerow(['ID', 'EVENT', 'TIME'])
        writer.writerows(csvlist)
        f.close()


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
        for unit in unitslist:
            if 'UUID' in unit['unitAddress']['did']:
                sendGetSamplesRequest(
                    unit['unitAddress']['did'], locationID, startdt, enddt)
            # if 'Motion' in unit['unitAddress']['did']:
                # pass

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
        if requestcount == 0:
            print('\n', datetime.now(), ' Mission Accomplished')
            writetofile()
            rt.stop()
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


def sendGetSamplesRequest(UnitDid, LocationId, start, end):
    # print('    ---', start, end, '  ')
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


if __name__ == "__main__":
    print(datetime.now(), " Connecting to ",
          cirrusHost, "with user ", username)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage, on_close=onClose, on_open=onOpen, keep_running=True)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
    # periodTimer = threading.Timer(500, sendPeriodicRequest())
