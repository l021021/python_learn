# -*- coding: utf-8 -*-
#coding=utf-8
"""
@author: yunshanan
"""

import time
import json
import websocket
# from websocket import WebSocketApp
# import datetime
# import pymysql as MySQLdb
import ssl
# import sys
# import re
# import pandas as pd
# import sqlite3
# from sqlalchemy import create_engine
# import smtplib
# # from email.mime.text import MIMEText
# from email.utils import formataddr
# import logging

# The cirrus host to connect to:
cirrusHost = "cirrus20.yanzi.se"

# Change the username and password to the Yanzi credentials:
username = 'frank.shen@pinyuaninfo.com'
password = 'Ft@Sugarcube99'
pattern = '%Y-%m-%d %H:%M:%S'


def onMessage(ws, message):
    print('\n')
    response = json.loads(message)
    print(response)
    print('\n')

    if response["messageType"] == "ServiceResponse":
        print("onMessage: Got ServiceResponse, sending login request")
        # We got a service response, let’s try to login:
        sendLoginRequest()
    elif response["messageType"] == "LoginResponse":
        print("onMessage: Got LoginResponse, sending get samples request")
        # We successfully logged in, let’s get the samples for the sensor unit
        sendSubscribeRequest('write your location ID here.')
    elif response["messageType"] == "SubscribeData":
        print("onMessage: Got SubscribeData")


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
            "name": "data"
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


def sendGetSamplesRequest(UnitDid, LocationId, start, end):
    # Create sample request to request the last 24 hour samples
    request = {
        "messageType": "GetSamplesRequest",
        "dataSourceAddress": {
            "resourceType": "DataSourceAddress",
            "did": UnitDid,
            "locationId": LocationId
        },
        "timeSerieSelection": {
            "resourceType": "TimeSerieSelection",
            #"numberOfSamplesBeforeStart": 3,
            "timeStart": int(
                (time.mktime(time.strptime(start, pattern))) * 1000),
            # "timeStart" : int((time.time() - (60 * 3600)) * 1000), # 24 hours
            "timeEnd": int((time.mktime(time.strptime(end, pattern))) * 1000),
            # "timeEnd" : int((time.time() - (0 * 3600)) * 1000)
        }
    }

    sendMessage(request)


if __name__ == "__main__":
    print("Connecting to ", cirrusHost, "with user ", username)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage,
                                on_error=onError,
                                on_close=onClose,
                                keep_running=True)
    ws.on_open = onOpen

    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
