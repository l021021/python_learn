# -*- coding: utf-8 -*-
# coding=utf-8
"""
@author: Bruce

利用本地json文件保存所有的电量,每次先读取这个文件,然后每次更新都吧更新信息加到
字典(key是ID_LOCATION)的数组中

对于小于20的传感器,会加上.99,因此在文档中查找 .99 ,就可以找到缺电的传感器

"""

import csv
import json
import ssl
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from pprint import pprint
import pandas as pd
import json
import websocket

from RT import RepeatedTimer

# The cirrus host to connect to:
cirrusHost = "cirrus.ifangtang.net"

# Change the username and password to the Yanzi credentials:
username = 'frank.shen@sugarinc.cn'
password = 'iFangtang#899'

datalists = []
csvlist = []
requestcount = 0
HBFlag = 0
msgQue = deque()
count = 0
sessionId=''

batterylog=dict()
filename = "C:\\LOG\\Battery_RAW.json"

def writetofile():
    # batteryDF=pd.DataFrame(batterylog)
    try:
        with open(filename,mode='w') as fp:
            json.dump(batterylog,fp)    
    except:
        pass
   


def readLogfile():
    # filename = "C:\\LOG\\Battery_RAW.json"
    # batteryDF=pd.DataFrame(batterylog)
    global batterylog
    with open(filename) as fp:
        batterylog=json.load(fp)
    

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
        print(response)
        sys.exit(-1)

    elif response["messageType"] == "ServiceResponse":
        # pprint(response)
        print("Got ServiceResponse, sending login request")
        # We got a service response, let’s try to login:
        sendLoginRequest()
    elif response["messageType"] == "LoginResponse":
        if (response['responseCode']['name'] == 'success'):
            sessionId = response['sessionId']
            GetLocationRequest()
         # sendGetUnitsRequest(locationID)       
            # sendSubscribeRequest(locationID,['lifecycle','config','data','assetSlots','occupancy','battery',\
            #                                  'sensorData','sensorSlots','assetData','occupancySlots']) #
            sendPeriodicRequest()
        else:
            print(response)
            sys.exit(-1)
    elif response["messageType"] == "GetLocationsResponse":
        if (response['responseCode']['name'] == 'success'):
            list1=response['list']
            for locationID in list1:
                # print(locationID['locationAddress']['locationId'])
                # response['list'][0]['locationAddress']
                sendSubscribeRequest(locationID['locationAddress']['locationId'], ['battery'])
        
    elif response["messageType"] == "PeriodicResponse":
        HBFlag = 0
        # print("( periodic response rcvd )")
        rt.start()

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



def sendGetUnitsRequest(locationID):
    request = {"messageType": 'GetUnitsRequest', "timeSent": int(time.time(
    ) * 1000), "locationAddress": {"resourceType": 'LocationAddress', "locationId": locationID}}
    print('sending getunits request for ' + locationID)
    sendMessagetoQue(request)


def sendLoginRequest():
    if sessionId=='':
        request = {"messageType": "LoginRequest",
               "username": username, "password": password}
    else:
        request = {"messageType": "LoginRequest",
                   "sessionId": sessionId,
                   "username": username, "password": password}
    sendMessagetoQue(request)


def GetLocationRequest():
    request = {"messageType": "GetLocationsRequest",
               "timeSent": int(time.time() * 1000)
                  }
    sendMessagetoQue(request)

if __name__ == "__main__":
    print(datetime.now(), " Connecting to ",
          cirrusHost, "with user ", username)
    readLogfile()
    # print(batterylog)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage, on_close=onClose, on_open=onOpen, keep_running=True)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
