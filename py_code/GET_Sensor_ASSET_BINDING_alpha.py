# -*- coding: utf-8 -*-
# coding=utf-8
"""
@author: Bruce
 首先根据assetparet 属性建立 euid和UUID的对应关系，然后对应到givenname，最后可以统一改名字

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

import websocket


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
# locationID = "879448"  # snf
# locationID = "251092"  # yuanjin4
# locationID = "834706"  # yuanjin2
# locationID = "234190"  # yuanjin3
# locationID = "725728"  # yuanjin5
locationID = "368307"  # yuanjin1


datalists = []
sensorList = dict()
assetList = dict()
requestcount = 0
HBFlag = 0
msgQue = deque()
count = 0
sessionId = ''



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
    elif response["messageType"] == "GetUnitPropertyResponse":
            if (response['responseCode']['name'] == 'success'):
                # assetParemtID 
                # print('\n')
                assetList[response['unitAddress']['did']]=response['list'][0]['value']
                sensorList[response['unitAddress']['did']][1] = response['list'][0]['value']

                # print(assetList)
                # print(response)
 
 
    
    elif response["messageType"] == "GetUnitsResponse":
        print("Requesting for records:")
        unitslist = response['list']
        for unit in unitslist:
            # if 'UUID' in unit['unitAddress']['did'] and 'nameSetByUser' in unit:
            # pprint(unit)
            if 'nameSetByUser' in unit:
                sensorList[unit['unitAddress']['did']]=[unit['nameSetByUser'],'']
                GetUnitPropertyRequest(locationID, unit['unitAddress']['did'],'assetParentId')
        if len(sensorList)>0 :
            list=pd.DataFrame.from_dict(sensorList,orient='index',columns=['NAME','ASSET'])
            
            list.sort_values(by='NAME',inplace = True)
            pprint(list)        
            list.to_csv('C:\\LOG\\'+locationID+'_name.csv',mode='w',encoding='utf-8')
            # sys.exit(-1)
    else:
        
        print('\n',response)
        


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
        "messageType": "SetUnitPropertyRequest",
        "unitAddress": {
            "resourceType": "UnitAddress",
            "did": did,
            "locationId": locationId
        },
        "unitProperty": {
            "resourceType": "UnitProperty",
            "name": "showUtilization",
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
