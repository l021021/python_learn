#现在要在start之前多读去一条记录
"""
@author: Bruce

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

datalists = []
csvlist = []
requestcount = 0
HBFlag = 0
msgQue = deque()
count = 0
sensorList=dict()

def onMessage(ws, message):
    response = json.loads(message)
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
        else:
            sys.exit(-1)

    elif response["messageType"] == "SubscribeData":
        print('  Subscription     :', response)
        # print("onMessage: Got SubscribeData")
    elif response["messageType"] == "GetUnitsResponse":
        print("processing names of sensors and assets:")
        unitslist = response['list']
        # pprint(unitslist)
        for unit in unitslist:
            if 'UUID' in unit['unitAddress']['did'] and 'nameSetByUser' in unit:
                sensorList[unit['unitAddress']['did']]=unit['nameSetByUser']
                # pprint(sensorList)
            # else:
            #     pprint(unit)
            #     pass
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
def sendServiceRequest():
    request = {"messageType": "ServiceRequest",
               "version": "1.6.4", "clientId": "123456"}
    sendMessage(request)

def sendGetUnitsRequest(locationID):
    request = {"messageType": 'GetUnitsRequest', "timeSent": int(time.time(
    ) * 1000), "locationAddress": {"resourceType": 'LocationAddress', "locationId": locationID}}
    print('sending getunits request for ' + locationID)
    sendMessage(request)


def sendLoginRequest():
    request = {"messageType": "LoginRequest",
               "username": username, "password": password}
    sendMessage(request)


if __name__ == "__main__":
    print(datetime.now(), " Connecting to ",
          cirrusHost, "with user ", username)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage, on_close=onClose, on_open=onOpen, keep_running=True)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
    # periodTimer = threading.Timer(500, sendPeriodicRequest())
