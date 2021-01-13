# -*- coding: utf-8 -*-
#coding=utf-8
"""
@author: Jeffrey
连接yanzi  api的代码，方便调试
返回数据都在onMessage中处理
"""

import time
import json
import websocket
import datetime
import ssl
import sys
import pandas as pd



# The cirrus host to connect to:
cirrusHost = "cirrus5.yanzi.se"

# Change the username and password to the Yanzi credentials:
# username = 'jianfeng@sugarinc.cn'
# password = 'yz.150363'
username = 'frank.shen@pinyuaninfo.com'
password = 'Ft@Sugarcube99'
pattern = '%Y-%m-%d %H:%M:%S'



def onMessage(ws, message):
    response = json.loads(message)
    print(response)
    if response["messageType"] == "ServiceResponse":
        print("Get ServiceResponse:=============>")
        print(response)
        print("Sending login request now")
        sendLoginRequest()

    elif response["messageType"] == "LoginResponse":
        print("Get LoginResponse:=============>")
        print(response)
        print("Sending other request now")
        # sendSubscribeRequest('697995')
        # sendGetUnitsRequest(LocationID)
        # sendPeriodicRequest()
        # get_unit_property()
        df = pd.read_csv('C:\\Users\\wjfch\\Desktop\\工作文件\\远景\\725728_name-5-finish.csv')
        # 需要更改名字和locationId
        print(df)
        name_list = df['name'].tolist()
        did_list = df['did'].tolist()
        print(name_list)
        print(did_list)
        for num, name in enumerate(name_list):
            did = did_list[num]
            name = '5-' + str(int(name))
            # 更改传感器的unitName名字和assetName, EUI-XXX对应的logicName是unitName, UUID-XXX对应的logicName是assetName
            setUnitPropertyRequest('725728', did, name)
        # sendGetSamplesRequest('EUI64-0080E10300043F62-4-Motion', '503370', 1585670400000, 1593532800000)
        # sendGetSamplesRequest('EUI64-D0CF5EFFFE793047', '229349', 1592788130000, 1592791730000)
        # get_locations()
        # get_unit_property()
    elif response["messageType"] == "GetUnitsResponse":
        print("GetUnitsResponse=============>")
        print(response)


def onError(ws, error):
    print("OnError", error)

def onClose(ws):
    print("onClose")

def onOpen(ws):
    print("onOpen: sending service request")
    sendServiceRequest()
    # sendLoginRequest()


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


def setUnitPropertyRequest(locationId, did, name):
    request_data = {
          "messageType":"SetUnitPropertyRequest",
          "unitAddress":{
            "resourceType":"UnitAddress",
            "did":did,
            "locationId":locationId
          },
          "unitProperty":{
            "resourceType":"UnitProperty",
            "name":"logicalName",
            "value":name
          }
        }

    sendMessage(request_data)


def sendGetUnitsRequest(passLoc):
    request = {
            "messageType": "GetUnitsRequest",
            "timeSent": int(time.time() * 1000),
            "locationAddress":{
                    "resourceType":"LocationAddress",
                    "locationId":passLoc
                    }
        }

    sendMessage(request)



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

def sendServiceRequest():
    request = {
        "messageType": "ServiceRequest",
        "version": "1.6.4",
        "clientId": "123456"
    }
    sendMessage(request)

def sendSubscribeRequest(location_id):
    request = {
        "messageType": "SubscribeRequest",
        "timeSent": int(time.time() * 1000),
        "unitAddress": {
            "resourceType": "UnitAddress",
            "locationId": location_id
        },
        "subscriptionType": {
            "resourceType": "SubscriptionType",
            "name": "uplog"
        }
    }
    sendMessage(request)


def sendPeriodicRequest():
    request = {
        "messageType": "PeriodicRequest",
        "timeSent": int(time.time() * 1000)
    }
    sendMessage(request)


def sendLoginRequest():
    request = {
        "messageType": "LoginRequest",
        "username": username,
        "password": password
    }
    sendMessage(request)




def get_locations():
    request = {
        "messageType": "GetLocationsRequest",
        "timeSent": 1591701559168
    }
    sendMessage(request)





def getAccountRequest():
    request_data = {
            "messageType":"GetAccountsRequest"
            }
    sendMessage(request_data)

if __name__ == "__main__":
    global periodicCount
    periodicCount = 0
    print("Connecting to ", cirrusHost, "with user ", username)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage,
                                on_error=onError,
                                on_close=onClose,
                                keep_running=True)
    ws.on_open = onOpen
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
