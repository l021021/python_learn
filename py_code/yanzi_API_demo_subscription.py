# -*- coding: utf-8 -*-
# coding=utf-8
"""
@author: Bruce

针对拉取长时间的ASSET
@TODO: periodic 
"""

import csv
import json
import ssl
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from pprint import pprint

import websocket

from RT import RepeatedTimer

# The cirrus host to connect to:
cirrusHost = "cirrus.ifangtang.net"

# Change the username and password to the Yanzi credentials:
username = 'frank.shen@sugarinc.cn'
password = 'iFangtang#899'

# username = '627619401@qq.com'
# password = '00000000'

# username = 'ic@isunon.com'
# password = 'Sunon@123'
# username = 'lihongyuan@wafersystems.com'
# password = 'Wafer!123'


# locationID = "879448"  # snf
# locationID = "655623"
# locationID = "74365"  # kerry
# locationID = "323602"  # kerry

# locationID = "573742"  # ft
# locationID = "521209"  # wf
# locationID = "274189" # sunon
locationID = "797296" # 诺梵

requestcount = 0
HBFlag = 0
msgQue = deque()
count = 0
sessionId=''


def sendPeriodicRequest():
    global HBFlag
    request = {"messageType": "PeriodicRequest",
               "timeSent": int(time.time() * 1000)}
    HBFlag += 1
    if HBFlag >= 3:
        print('(', HBFlag, 'periodic request sent )')
        print('Should disconnect ')  # !!

    else:
        # print('(', HBFlag, 'periodic request missed )'
        pass
    sendMessage(request)


rt = RepeatedTimer(30, sendPeriodicRequest)


def onMessage(ws, message):
    global rt
    response = json.loads(message)
    sendFromQue()
    # print('response')
    global requestcount, HBFlag,sessionId
    # HBFlag = 0

    if response["messageType"] == "ErrorResponse":
        print('Error while connecting',response)
        sys.exit(-1)

    elif response["messageType"] == "ServiceResponse":
        # pprint(response)
        # print("Got ServiceResponse, sending login request")
        # We got a service response, let’s try to login:
        sendLoginRequest()
    elif response["messageType"] == "LoginResponse":
        if (response['responseCode']['name'] == 'success'):
            print('Login to ',locationID)
            sessionId = response['sessionId']
            # sendGetUnitsRequest(locationID)
            # sendSubscribeRequest(locationID,['lifecycle']) #
            #!!订阅的数据选项
            sendSubscribeRequest(locationID,[
                # 'lifecycle',
                                            #  'config',
                                            #  'data',
                                            #  'assetSlots',
                                             'occupancy',
                                            #  'battery',
                                            #  'sensorData',
                                            #  'sensorSlots',
                                            #  'assetData'
                                             
                                            #  'occupancySlots'
                                             ]) #
            sendPeriodicRequest()
        else:
            print(response)
            sys.exit(0)
    elif response["messageType"] == "PeriodicResponse":
        HBFlag = 0
        # print("( periodic response rcvd )")
        rt.start()

    elif response["messageType"] == "SubscribeData":
        print('>', str(datetime.now().time())[0:8],'>',end='')
        # for list,*other,subscriptionType,timesent in response:
        print(response['subscriptionType']['name'],end='-')
        # print(response['list'][0]['list'][0]['resourceType'],end='--')

        try:
            if response['subscriptionType']['name'] in ['data','assetData']:
                print(response['list'][0]['dataSourceAddress']['did'],  end='-')
                print(response['list'][0]['dataSourceAddress']['variableName']['name'], end='-')
                if response['list'][0]['list'][0]['resourceType'] == 'SampleAsset':
                    # if 'name' in response['list'][0]['list'][0]['assetState']:
                    print(response['list'][0]['list'][0]['assetState']['name'])
                elif response['list'][0]['list'][0]['resourceType'] == 'SampleUtilization':
                    print('free', response['list'][0]['list'][0]['free'], 'occu:', response['list'][0]['list'][0]['occupied'])
            
                elif response['list'][0]['list'][0]['resourceType'] == 'SampleUpState':
                    print(response['list'][0]['list'][0]['deviceUpState']['name'])
                # print(response['list'][0]['list'][0]['value'],'\n ')
                # print(response['list'][0]['list'][0]['assetState']['name'])
                # for 
                else: 
                   print(response['list'][0]['list'][0]['value'])
            elif response['subscriptionType']['name'] == 'battery':
                print(response['list'][0]['dataSourceAddress']['did'], end='-')
                print(response['list'][0]['list'][0]['value'], end='-')
            # print(response['list'][0]['list'][0]['value'],'\n ')
            # print(response['list'][0]['list'][0]['assetState']['name'])
            # for 
                print((response['list'][0]['list'][0]['value'] if 'value' in response['list'][0]['list'][0] else \
                response['list'][0]['list'][0]['assetState']['name']))
                
      
            elif response['subscriptionType']['name'] =='occupancySlots':
                print(response['list'][0]['dataSourceAddress']['did'], end='-')
                print(response['list'][0]['list'][0]['sample']['assetState']['name'], response['list'][0]['list'][0]['slotSize']['name'])
            elif response['subscriptionType']['name'] =='lifecycle':
                print(response['list'][0]['eventType']['name'], end='-')
                print(response['list'][0]['unitAddress']['did'])
                # print(response['list'][0]['list'][0]['deviceUpState']['name'])
            elif response['subscriptionType']['name'] =='sensorSlots':
                print(response['list'][0]['dataSourceAddress']['did'][12:], end='-')
                print('Aggre:',response['list'][0]['list'][0]['aggregateValue'],'in ',response['list'][0]['list'][0]['numberOfValues'],
                'periods of ',response['list'][0]['list'][0]['slotSize']['name'])
            # elif response['subscriptionType']['name'] =='occupancy':
                
            #     print(response['list'][0]['dataSourceAddress']['did'])
            elif response['subscriptionType']['name'] == 'sensorData':
                print(response['list'][0]['dataSourceAddress']['did'], end='-')
                print(response['list'][0]['dataSourceAddress']['variableName']['name'], end='-')

                print((response['list'][0]['list'][0]['value'] if 'value' in response['list'][0]['list'][0] else \
                    response['list'][0]['list'][0]['assetState']['name']))
            elif response['subscriptionType']['name'] =='assetSlots':
                print(response['list'][0]['dataSourceAddress']['did'])
                print(response['list'][0]['list'][0]['aggregateValue'])
            elif response['subscriptionType']['name'] =='occupancy':
                print(response['list'][0]['dataSourceAddress']['did'], end='-')
                if response['list'][0]['list'][0]['resourceType'] == 'SampleAsset':
                # if 'name' in response['list'][0]['list'][0]['assetState']:
                    print('SampleAsset:',response['list'][0]['list'][0]['assetState']['name'])
                elif response['list'][0]['list'][0]['resourceType'] == 'SampleUtilization':
                    print('free', response['list'][0]['list'][0]['free'], 'occu:', response['list'][0]['list'][0]['occupied'])
                # elif response['list'][0]['list'][0]['resourceType'] == 'SampleAsset':
                #     print('free:',response['list'][0]['list'][0]['assetState']['free'])

            else:

                pprint(response)

                # print(response['list'][0]['dataSourceAddress']['variableName']['name'],response['list'][0]['list'][0]['value'],'\n ')
            
        except :
            print('!!!!!!!!!!!!!!!!')
            pprint(response)
        
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
        # print(response)
        pass


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
               "version": "1.8.1", "clientId": "653498331@qq.com"}
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
        sendMessage(request)
        # print('      ', request)


def setUnitPropertyRequest(locationId, did, name):
    request_data = {
        "messageType": "SetUnitPropertyRequest",
        "unitAddress": {
            "resourceType": "UnitAddress",
            "did": did,
            "locationId": locationId
        },
        "unitProperty": {
            "resourceType": "UnitProperty",
            "name": "logicalName",
            "value": name
        }
    }

    sendMessage(request_data)



def sendGetUnitsRequest(locationID):
    request = {"messageType": 'GetUnitsRequest', "timeSent": int(time.time(
    ) * 1000), "locationAddress": {"resourceType": 'LocationAddress', "locationId": locationID}}
    print('sending getunits request for ' + locationID)
    sendMessagetoQue(request)


def setUnitPropertyRequest(locationId, did, name):
    request_data = {
        "messageType": "SetUnitPropertyRequest",
        "unitAddress": {
            "resourceType": "UnitAddress",
            "did": did,
            "locationId": locationId
        },
        "unitProperty": {
            "resourceType": "UnitProperty",
            "name": "logicalName",
            "value": name
        }
    }

    sendMessage(request_data)


def get_unit_property():
    request = {
        "messageType": "GetUnitPropertyRequest",
        "timeSent": 1609746867000,
        "unitAddress": {
            "resourceType": "UnitAddress",
            "timeCreated": 1609746867000,
            "did": "UUID-F0E454CAC23D49768470DECD14069F7C",
            "locationId": '251092'
        },
        "name": "dataSource"
    }
    sendMessage(request)


def sendLoginRequest():
    if sessionId=='':
        request = {"messageType": "LoginRequest",
               "username": username, "password": password}
    else:
        request = {"messageType": "LoginRequest",
                   "sessionId": sessionId,
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
    # print(datetime.now(), " Connecting to ",          cirrusHost, "with user ", username)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage, on_close=onClose, on_open=onOpen, keep_running=True)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
