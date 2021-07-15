# -*- coding: utf-8 -*-
# coding=utf-8
"""
@author: Bruce
这个程序示范了利用YANZI API订阅实时数据的用法
onMessage:说明了如何解析返回的json
sendPeriodicRequest: 是心跳机制,如果连续收不到心跳回复,则提示连接出错,需要重连(几乎不会发生)
Que 有关: 自己做的队列,这个程序里面用处不大
sendSubscribeRequest:说明了如何订阅不同的数据



"""

import json
import ssl
import sys
import time
import websocket
from collections import deque
from datetime import datetime
from pprint import pprint
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
cirrusHost = "cirrus.ifangtang.net"
# cirrusHost = "42.159.214.39"
# cirrusHost = "42.159.213.152"

# Change the username and password to the Yanzi credentials:
username = '653498331@qq.com'
password = '000000'

locationID = "573742"  # ft

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
            sendSubscribeRequest(locationID,
                [
                'lifecycle',
                'config',
                'data',
                'assetSlots',
                'occupancy',
                'battery',
                'sensorData',
                'sensorSlots',
                'assetData',                                             
                'occupancySlots'
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
                else: 
                   print(response['list'][0]['list'][0]['value'])
            elif response['subscriptionType']['name'] == 'battery':
                print(response['list'][0]['dataSourceAddress']['did'], end='-')
                print(response['list'][0]['list'][0]['value'], end='-')
                print((response['list'][0]['list'][0]['value'] if 'value' in response['list'][0]['list'][0] else \
                response['list'][0]['list'][0]['assetState']['name']))
            elif response['subscriptionType']['name'] =='occupancySlots':
                print(response['list'][0]['dataSourceAddress']['did'], end='-')
                print(response['list'][0]['list'][0]['sample']['assetState']['name'], response['list'][0]['list'][0]['slotSize']['name'])
            elif response['subscriptionType']['name'] =='lifecycle':
                print(response['list'][0]['unitAddress']['did'], end='-')
                print(response['list'][0]['eventType']['name'])
                # print(response['list'][0]['list'][0]['deviceUpState']['name'])
            elif response['subscriptionType']['name'] =='sensorSlots':
                print(response['list'][0]['dataSourceAddress']['did'][12:], end='-')
                print('Aggre:',response['list'][0]['list'][0]['aggregateValue'],'in ',response['list'][0]['list'][0]['numberOfValues'],
                'periods of ',response['list'][0]['list'][0]['slotSize']['name'])
            elif response['subscriptionType']['name'] == 'sensorData':
                print(response['list'][0]['dataSourceAddress']['did'], end='-')
                print(response['list'][0]['dataSourceAddress']['variableName']['name'], end='-')
                print((response['list'][0]['list'][0]['value'] if 'value' in response['list'][0]['list'][0] else \
                    response['list'][0]['list'][0]['assetState']['name']))
            elif response['subscriptionType']['name'] =='assetSlots':
                print(response['list'][0]['dataSourceAddress']['did'], end='-')
                print('aggregateValue',response['list'][0]['list'][0]['aggregateValue'])
            elif response['subscriptionType']['name'] =='occupancy':
                print(response['list'][0]['dataSourceAddress']['did'], end='-')
                if response['list'][0]['list'][0]['resourceType'] == 'SampleAsset':
                # if 'name' in response['list'][0]['list'][0]['assetState']:
                    print(response['list'][0]['list'][0]['assetState']['name'])
                elif response['list'][0]['list'][0]['resourceType'] == 'SampleUtilization':
                    print('free', response['list'][0]['list'][0]['free'], 'occu:', response['list'][0]['list'][0]['occupied'])

            else:

                pprint(response)

                # print(response['list'][0]['dataSourceAddress']['variableName']['name'],response['list'][0]['list'][0]['value'],'\n ')
            
        except :
            print('!!!!!!!!!!!!!!!!')
            pprint(response)
        
    
        else:
        # print(response)
          pass


def onError(ws, error):
    print(ws)
    print(error)

def onClose(ws):
    print(ws)

    print("\n----Connection to Cloud closed----\n")


def onOpen(ws):
    print(ws)
    print("WSS connected, Sending service request")
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


def sendLoginRequest():
    if sessionId=='':
        request = {"messageType": "LoginRequest",
               "username": username, "password": password}
    else:
        request = {"messageType": "LoginRequest",
                   "sessionId": sessionId,
                   "username": username, "password": password}
    sendMessagetoQue(request)


if __name__ == "__main__":
    websocket.enableTrace(True)
    print(datetime.now(), "Connecting to ",          cirrusHost, locationID," with user ", username)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI",
                                on_message=onMessage, on_close=onClose, on_open=onOpen, keep_running=True,on_error=onError)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
