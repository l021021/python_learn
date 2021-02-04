# -*- coding: utf-8 -*-
# coding=utf-8
"""
@author: Bruce
 首先根据assetparet 属性建立 euid和UUID的对应关系，然后对应到givenname，最后可以统一改名字
 顺便把资产显示的开关打开

同时发出订阅，5分钟没有收到数据的列出来。

 输出的文件在c:\LOG\ loation_name_bad.csv
"""

import os
import ssl
import json
import sys
import time
from collections import deque
from datetime import datetime, timedelta
import pandas as pd
from pprint import pprint

import websocket
from threading import Timer

class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


# The cirrus host to connect to:
cirrusHost = "cirrus20.yanzi.se"

# Change the username and password to the Yanzi credentials:
username = 'frank.shen@pinyuaninfo.com'
password = 'Ft@Sugarcube99'

locationID = "879448"  # snf
# locationID = "252208"
# locationID = "74365"  # kerry

# locationID = "229349"  # ft
# locationID = "521209"  # wf
# locationID = "879448"  # snf
# locationID = "368307"  # yuanjin1
# locationID = "834706"  # yuanjin2
# locationID = "234190"  # yuanjin3
locationID = "251092"  # yuanjin4
locationID = "725728"  # yuanjin5
# locationID = "503370"  # wanke



datalists = []
sensorList = dict()
assetList = dict()
requestcount = 0
HBFlag = 0
msgQue = deque()
count = 0
sessionId = ''



def onMessage(ws, message):
    global requestcount, HBFlag, sessionId
    global rt
    response = json.loads(message)
    sendFromQue()
    # print('response')
    # HBFlag = 0
    # print('::',response["messageType"])

    rt.stop()

    rt.start()
    if response["messageType"] == "ErrorResponse":
        print('Error',response)
       # sys.exit(-1)
    elif response["messageType"] == "PeriodicResponse":
        HBFlag = 0
        print("( periodic response rcvd )")
        rt.start()

    elif response["messageType"] == "ServiceResponse":
        # pprint(response)
        print("Got ServiceResponse, sending login request")
        # We got a service response, let’s try to login:
        sendLoginRequest()
    elif response["messageType"] == "LoginResponse":
        if (response['responseCode']['name'] == 'success'):
            sessionId = response['sessionId']
            sendGetUnitsRequest(locationID)
            sendSubscribeRequest(locationID,'occupancy')
        else:
            print(response)
            sys.exit(-1)
    elif response["messageType"] == "GetUnitPropertyResponse":
            if (response['responseCode']['name'] == 'success'):
                # assetParentID 
                # print('\n')
                if 'value' in response['list'][0]:
                    assetList[response['unitAddress']['did']] = response['list'][0]['value']
                    sensorList[response['unitAddress']['did']][1] = response['list'][0]['value'] #传感器名字
                    # sensorList[response['unitAddress']['did']][2] = response['list'][0]['value'] #asset名字


                # print(assetList)
                # print(response)
    elif response["messageType"] == "SubscribeData":
            # print('\n-',end='-')
            # for list,*other,subscriptionType,timesent in response:
            # print(response['subscriptionType']['name'],end='-')
            # print(response['list'][0]['list'][0]['resourceType'],end='--')

            try:
                if response['subscriptionType']['name'] in ['data','assetData']:
                    print(response['list'][0]['dataSourceAddress']['did'])
                    print(response['list'][0]['dataSourceAddress']['variableName']['name'])
                    # print(response['list'][0]['list'][0]['value'],'\n ')
                    # print(response['list'][0]['list'][0]['assetState']['name'])
                    # for 
                    print((response['list'][0]['list'][0]['value'] if 'value' in response['list'][0]['list'][0] else \
                        response['list'][0]['list'][0]['assetState']['name']))
                    
                    #!! battery info   
                elif response['subscriptionType']['name'] == 'battery':
                    # print("B",end='.')               
                    # print(response['list'][0]['dataSourceAddress']['did'])
                    # # print(response['list'][0]['list'][0]['value'])
                    # print((response['list'][0]['list'][0]['value'] if 'value' in response['list'][0]['list'][0] else \
                    # response['list'][0]['list'][0]['assetState']['name']))
                    # print(response['list'][0]['list'][0]['percentFull'])
                    if int(response['list'][0]['list'][0]['percentFull'])<=20 :
                        print('\n',response['list'][0]['dataSourceAddress']['did']+'_'+response['list'][0]['dataSourceAddress']['locationId'])
                        # print(response['list'][0]['list'][0]['value'])
                        # print((response['list'][0]['list'][0]['value'] if 'value' in response['list'][0]['list'][0] else
                        #     response['list'][0]['list'][0]['assetState']['name']))
                        print(response['list'][0]['list'][0]['percentFull'])
                        response['list'][0]['list'][0]['percentFull'] = response['list'][0]['list'][0]['percentFull']+.99
                    if response['list'][0]['dataSourceAddress']['did']+'_'+response['list'][0]['dataSourceAddress']['locationId'] in batterylog:
                        batterylog[response['list'][0]['dataSourceAddress']['did']+'_'+response['list'][0]['dataSourceAddress']['locationId']].append([response['list'][0]['list'][0]['percentFull'],
                            response['list'][0]['list'][0]['sampleTime']])
                    else:
                        batterylog[response['list'][0]['dataSourceAddress']['did']+'_'+response['list'][0]['dataSourceAddress']['locationId']] = []
                        batterylog[response['list'][0]['dataSourceAddress']['did']+'_'+response['list'][0]['dataSourceAddress']['locationId']].append([response['list'][0]['list'][0]['percentFull'],
                                                                                            response['list'][0]['list'][0]['sampleTime']])
                    writetofile()
        
                elif response['subscriptionType']['name'] =='occupancySlots':
                    print(response['list'][0]['dataSourceAddress']['did'])
                    print(response['list'][0]['list'][0]['sample']['assetState']['name'])
                elif response['subscriptionType']['name'] =='lifecycle':
                    print(response['list'][0]['dataSourceAddress']['did'])
                    print(response['list'][0]['list'][0]['deviceUpState']['name'])
                elif response['subscriptionType']['name'] =='sensorSlots':
                    print(response['list'][0]['dataSourceAddress']['did'])
                    print(response['list'][0]['list'][0]['aggregateValue'])
                elif response['subscriptionType']['name'] == 'sensorData':
                    print(response['list'][0]['dataSourceAddress']['did'])
                    print(response['list'][0]['dataSourceAddress']['variableName']['name'])
                    print((response['list'][0]['list'][0]['value'] if 'value' in response['list'][0]['list'][0] else \
                        response['list'][0]['list'][0]['assetState']['name']))
                elif response['subscriptionType']['name'] =='assetSlots':
                    print(response['list'][0]['dataSourceAddress']['did'])
                    print(response['list'][0]['list'][0]['aggregateValue'])
                elif response['subscriptionType']['name'] =='occupancy':
                    print(response['list'][0]['dataSourceAddress']['did'])
                    print(response['list'][0]['list'][0]['assetState']['name'])
                else:

                    pprint(response)

                    # print(response['list'][0]['dataSourceAddress']['variableName']['name'],response['list'][0]['list'][0]['value'],'\n ')
                
            except :
                print('!!!!!!!!!!!!!!!!')
                # pprint(response)
    
    
    elif response["messageType"] == "GetUnitsResponse":
        print("Requesting for records:")
        unitslist = response['list']
        for unit in unitslist:
            # if 'UUID' in unit['unitAddress']['did'] and 'nameSetByUser' in unit:
            # pprint(unit)
            if 'nameSetByUser' in unit:
                sensorList[unit['unitAddress']['did']] = ['"'+unit['nameSetByUser']+'"', '',''] #分别是传感器名字,资产名字,对应的UUID
                if unit['unitAddress']['did'].find('EU') != -1:  # 对物理传感器获取资产名字
                    GetUnitPropertyRequest(locationID, unit['unitAddress']['did'],'assetParentId')
                    
                    # 有个问题，TODO 需要优先取ASSET的名字而不是传感器的
     
            else:
                print('skipping ', unit['unitAddress']['did']) #逻辑传感器 和网关
       
            # sys.exit(-1)
    else:
        pass
        # print('\n',response)
        


def GetUnitPropertyRequest(locationId, did, propertyName):
    request_data ={
        "messageType": "GetUnitPropertyRequest",
        "unitAddress": {
        "resourceType": "UnitAddress",
        "timeCreated": 1587560556552,
        "did": did,
        "locationId": locationId,
           },
          "name": propertyName
          }
    # assetParentId
    # print('Checking the Asset Parent for:',did)
    sendMessage(request_data)

# def onError(ws, error):
#     print("Error", error)

def setDisplayFlag(locationId, did, displayFlag):
    request_data = {
        "messageType": "SetCustomPropertyRequest",
        "address": {
            "resourceType": "UnitAddress",
            "did": did,
            "locationId": locationId
        },
        "property": {
            "resourceType": "CustomPropertyDTO",
            "timeCreated": 1607423084116,
            "name": "showLabel",
            "value": displayFlag
    }
    }

    sendMessage(request_data)


def setUnitLogicName(locationId, did, logicName):
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
            "value": logicName
        }
    }

    sendMessage(request_data)

def showResult():
    # print(datetime.now()," 30 seconds to close job")
    if len(sensorList) > 0:
        for k,v in sensorList.items():
            if k.find('EU')!=-1:
                # print(k,'--',v,end='----\n')
                print('key',k,'0:',v[0],'1:',v[1],'2:',v[2])
                if v[1] in sensorList:
                    v[2]=sensorList[v[1]][0]
                # print(v[2], 'xxx', sensorList[v[1]][1])
        list = pd.DataFrame.from_dict(sensorList, orient='index', columns=['NAME', 'ASSET','NAME1'])

        list.sort_values(by='NAME', inplace=True)
        list.describe()
        list=list[list.ASSET!='']
        # list.remove( lambda x:x.ASSET='')
        pprint(list)
        list.to_csv('C:\\LOG\\'+locationID+'_name.csv', mode='w', encoding='utf-8')
    os._exit(0)

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



def sendGetUnitsRequest(locationID):
    request = {"messageType": 'GetUnitsRequest', "timeSent": int(time.time(
    ) * 1000), "locationAddress": {"resourceType": 'LocationAddress', "locationId": locationID}}
    print('sending getunits request for ' + locationID)
    sendMessagetoQue(request)


def sendLoginRequest():
    if sessionId == '':
        request = {"messageType": "LoginRequest",
                   "username": username, "password": password}
    else:
        request = {"messageType": "LoginRequest",
                   "sessionId": sessionId,
                   "username": username, "password": password}
    sendMessagetoQue(request)


if __name__ == "__main__":
    print(datetime.now(), " Connecting to ",
          cirrusHost, "with user ", username)
    rt = RepeatedTimer(120, showResult) #5分钟内没有数据，则视为坏
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage, on_close=onClose, on_open=onOpen, keep_running=True)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
