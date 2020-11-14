# -*- coding: utf-8 -*-
#coding=utf-8

import time
import json
import websocket
import datetime
import pymysql as MySQLdb
import ssl
import sys
import re
import pandas as pd
#import sqlite3
#from sqlalchemy import create_engine
#import smtplib
#from email.mime.text import MIMEText
#from email.utils import formataddr
import logging


# The cirrus host to connect to:
cirrusHost = "cirrus11.yanzi.se"
hostaddress = '47.96.91.140'
hostusername = 'root'
hostpwd = 'SugarcubeIOT777'

username = 'frank.shen@pinyuaninfo.com'
password = 'Internetofthing'
pattern = '%Y-%m-%d %H:%M:%S'

def pairUnits():

    # Read ID and its corresponding names from the csv file
    nameList = pd.read_csv(".\\danny\\sugar.csv")
    nl = nameList.values
    location = '_' + str(nl[0][2])

    # Connect to database
    conn = MySQLdb.connect(host=hostaddress, user=hostusername, passwd=hostpwd, db=location)
    cursor = conn.cursor()

    # For each sensor ID, look for its parent(UUID) in database and replace the sensor ID
    for pair in nl:
        pair[0] = '%' + pair[0] + '%'
        sql = "select ParentId from Unit_Info_Data where UnitDid like '%s' and ParentId not like 'N/A'" % (pair[0])
        cursor.execute(sql)
        temp = cursor.fetchall()[0][0]
        pair[0] = temp
    cursor.close()
    conn.close()
    return nl


def onMessage(ws, message):
    response = json.loads(message)
    print(response)


    if response["messageType"] == "ServiceResponse":
        print("onMessage: Got ServiceResponse, sending login request")
        # We got a service response, let’s try to login:
        sendLoginRequest()
    elif response["messageType"] == "LoginResponse":
        print("onMessage: Got LoginResponse, sending set property request")
        namelist = []
        namelist = pairUnits()
        for item in namelist:
            sendSetUnitPropertyRequest(item[0],str(item[2]),'logicalName',item[1])

    elif response["messageType"] == "SubscribeData":
        print("Subscribe data =======================>")
        # info1 = response['list'][0]['dataSourceAddress']
        # print('-----------------------',info1)
        # measure = info1['variableName']['name']
        # print('-----------------------', measure)
        # device = info1['did']
        # print('-----------------------', device)
        # if measure == 'assetUtilization':
        #     epoch_time = response['list'][0]['list'][0]['sampleTime'] / 1000.0
        #     recordtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch_time))
        #     print(device,'==============>',recordtime)
        #     print(response['list'][0]['list'][0])
        print(response)
        # if('EUI64-0080E10300037E57-4-Motion' in response['list'][0]['dataSourceAddress']['did']):
        #     print("onMessage: Got SubscribeData")
        #     value = str(response['list'][0]['list'][0]['value'])
        #     print(response['list'][0]['dataSourceAddress']['did'])
        #     print(response['list'][0]['list'])
        #     print(time.strftime(pattern,time.localtime(response['list'][0]['list'][0]['sampleTime'] / 1000.0)))
    elif response["messageType"] == "GetSamplesResponse":
        print(len(response['sampleListDto']['list']))
        for item in response['sampleListDto']['list']:
            epoch_time = item['sampleTime'] / 1000.0
            recordtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch_time))
            value = item['value']
            print(recordtime, '============>',value)
        #     misc = item['timeLastMotion'] / 1000.0
        #     misc = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(misc))
        #     print(recordtime,'--------',value,'--------',misc)

    elif response["messageType"] == "GetUnitPropertyResponse":
        print(response.keys())

    elif response["messageType"] == "SetUnitPropertyResponse":
        if response['responseCode']['name'] == 'success':
            print("Property successfully set!!!")


def onError(ws, error):
    print("OnError", error)

def onClose(ws):
    print("onClose")

def onOpen(ws):
    print("onOpen: sending service request")
    sendServiceRequest()

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


def sendServiceRequest():
    request = {
        "messageType": "ServiceRequest",
        "version": "1.6.4",
        "clientId": "123456"
    }
    sendMessage(request)

def sendGetUnitPropertyRequest(unit,location):
    request = {
        "messageType":"GetUnitPropertyRequest",
        "timeSent":int(time.time() * 1000),
        "unitAddress":{
            "resourceType":"UnitAddress",
            "timeCreated":int(time.time() * 1000),
            "did":unit,
            "locationId":location
        },
        "name":"geoJSON"
    }

    sendMessage(request)


def sendSetUnitPropertyRequest(unit,location,property,target):
    request = {
        "messageType": "SetUnitPropertyRequest",
        "timeSent": int(time.time() * 1000),
        "unitAddress": {
            "resourceType": "UnitAddress",
            "timeCreated": int(time.time() * 1000),
            "did": unit,
            "locationId": location
        },
        "unitProperty": {
            "resourceType": "UnitProperty",
            "timeCreated": int(time.time() * 1000),
            "name": property,
            "value": target
        }
    }

    sendMessage(request)


def sendLoginRequest():
    request = {
        "messageType": "LoginRequest",
        "username": username,
        "password": password
    }
    sendMessage(request)



# Establish connection with cirrus server
if __name__ == "__main__":
    print("Connecting to ", cirrusHost, "with user ", username)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage,
                                on_error=onError,
                                on_close=onClose,
                                keep_running=True)
    ws.on_open = onOpen

    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})