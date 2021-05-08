# -*- coding: utf-8 -*-
# coding=utf-8
"""
@author: Bruce
从传感器列表,批量修改EUID和对应UUID的logical name

需要从UUID的datasource 查出传感器的euid,形成对应关系

"""




import csv
import json
import ssl
import sys
import time
from collections import deque
from datetime import datetime, timedelta
import pandas as pd
from pprint import pprint
from pandas.io.pytables import AppendableMultiSeriesTable

import websocket


# The cirrus host to connect to:
cirrusHost = "cirrus.ifangtang.net"


# The cirrus host to connect to:
username = 'frank.shen@sugarinc.cn'
password = 'iFangtang#899'

locationID = "573742" #ft
# locationID = "74365"  # kerry

# locationID = "879448"  # snf

datalists = []
sensorList = dict()
requestcount = 0
HBFlag = 0
msgQue = deque()
count = 0
sessionId = ''

names = pd.read_csv('C:\\LOG\\573742_ft_传感器_名称.csv', encoding='utf-8', index_col='NAME1')
namedict = dict()
for index, row in names.iterrows():
    namedict[index] = row
print(namedict)
def onMessage(ws, message):
    global rt
    response = json.loads(message)
    sendFromQue()
    # print('response')
    global requestcount, HBFlag, sessionId
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
            sendGetUnitsRequest(locationID)
        else:
            print(response)
            sys.exit(-1)
 
    
    elif response["messageType"] == "GetUnitsResponse":
        # print("Requesting for Units DATA:")
        unitslist = response['list']
        # pprint(unitslist)
        for unit in unitslist:
            if 'UUID' in unit['unitAddress']['did'] and 'nameSetByUser' in unit:
            # if 'nameSetByUser' in unit:
                sensorList[unit['unitAddress']['did']]=unit['nameSetByUser']
                # print(unit['nameSetByUser'])
                # print(unit['nameSetByUser'].replace('"', ''))
                if unit['nameSetByUser'].replace('"','') in namedict:
                    print(unit['unitAddress']['did'], namedict[unit['nameSetByUser'].replace('"', '')]['NAME'])
                    setUnitLogicName(locationID, unit['unitAddress']['did'], namedict[unit['nameSetByUser'].replace('"', '')]['NAME'])
        if len(sensorList)>0 :
            list=pd.DataFrame.from_dict(sensorList,orient='index',columns=['NAME'])
            
            list.sort_values(by='NAME',inplace = True)
            pprint(list)        
            # list.to_excel('C:\\LOG\\'+locationID+'_name.xls',mode='w',encoding='utf-8')
            sys.exit(1) 



# def onError(ws, error):
#     print("Error", error)

def sendGetUnitsRequest(passLoc):
    request = {
        "messageType": "GetUnitsRequest",
        "timeSent": int(time.time() * 1000),
        "locationAddress": {
            "resourceType": "LocationAddress",
            "locationId": passLoc
        }
    }

    sendMessage(request)


def setUnitLogicName(locationId, did, logicName):
    print('renaming',did,logicName)
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
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage, on_close=onClose, on_open=onOpen, keep_running=True)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
