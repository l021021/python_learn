# -*- coding: utf-8 -*-
# coding=utf-8
"""
@author: Bruce 
? 周期心跳实现
? 请求队列实现
"""

import csv
from datetime import datetime
from datetime import timedelta
import json
import pprint
import ssl
import sys
import time

import websocket

# import numpy as np
# import time

# The cirrus host to connect to:
cirrusHost = "cirrus20.yanzi.se"

# Change the username and password to the Yanzi credentials:
username = 'frank.shen@pinyuaninfo.com'
password = 'Ft@Sugarcube99'
# locationID = "879448"  #snf
# locationID = "655623"
locationID = "229349"  # ft
# locationID = "797296"  # no
pattern = '%Y-%m-%d %H:%M:%S'
startstr = '2020-10-01 00:00:00'
endstr = '2020-10-31 23:59:59'
startdt = datetime.fromtimestamp((time.mktime(time.strptime(startstr, pattern))))
enddt = datetime.fromtimestamp((time.mktime(time.strptime(endstr, pattern))))

# HBFlag = 0
datalists = []
csvlist = []
requestcount = 0
HBFlag = 0


def onMessage(ws, message):
    # eventtime = datetime.datetime.now()
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
            # print("onMessage: Got LoginResponse, sending get samples request")
            # We successfully logged in, let’s get the samples for the sensor unit
            # sendSubscribeRequest(locationID, datatype=['occupancy'])
            # sendPeriodicRequest()
            sendGetUnitsRequest(locationID)

    elif response["messageType"] == "PeriodicResponse":
        HBFlag = 0
        print(HBFlag, '# ', "periodic response-keepalive")
        sendPeriodicRequest()
    elif response["messageType"] == "SubscribeData":
        print('  Subscription     :', response)
        # print("onMessage: Got SubscribeData")
    elif response["messageType"] == "GetUnitsResponse":
        # print(response)
        unitslist = response['list']
        # pprint.pprint(unitslist)
        for unit in unitslist:
            # print(unit['unitAddress']['did'],unit['unitTypeFixed']['name'])
            if 'Motion' in unit['unitAddress']['did']:
                pass
                # print('Motion')
                # sendGetSamplesRequest(
                #     unit['unitAddress']['did'], locationID, start, end)
            elif 'UUID' in unit['unitAddress']['did']:
                # pprint.pprint(unit)
                # print('Asset')

                sendGetSamplesRequest(unit['unitAddress']['did'], locationID, startdt, enddt)
                # requestcount += 1
                # print('request sent:', requestcount)  # 增加请求计数器

    elif response["messageType"] == "GetSamplesResponse":
        # global requestcount
        requestcount = requestcount - 1
        print('request pending:', requestcount)
        # pprint.pprint(response)
        # print('list' in response['sampleListDto'])
        """处理了返回列表中list不存在的情况,这是因为时间段内没有事件
        """
        if (response['responseCode']['name'] == 'success') and ('list' in response['sampleListDto']):  # response['responseCode']['name']
            pprint.pprint(response)

            datalists = response['sampleListDto']['list']
            for li in datalists:
                # print(eventtime)
                # print(int(li['sampleTime']))
                # print(int(li['sampleTime'] / 1000))
                # print(eventtime)
                eventtime = datetime.fromtimestamp(int(li['sampleTime']/1000)).strftime(pattern)
                # print(eventtime)

                if li['resourceType'] == "SampleAsset":
                    # print(response['sampleListDto']['dataSourceAddress']['did'],
                    #       li['assetState']['name'], eventtime)
                    csvlist.append([response['sampleListDto']['dataSourceAddress']['did'], li['assetState']['name'], eventtime])
                elif li['resourceType'] == 'SampleMotion':
                    print(response['sampleListDto']['dataSourceAddress']['did'], eventtime, li['value'])
        else:
            print('skiped')
        if requestcount == 0:
            writetofile()
            sys.exit()
    else:
        print(response)


def writetofile():
    with open(r"f:\data.csv", 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow('ID,TimeStamp,Event')
        writer.writerows(csvlist)
        f.close()


def onError(ws, error):
    print("closed", error)


def onClose(ws):
    print("onClose")


def onOpen(ws):
    print("Sending service request")
    sendServiceRequest()


def sendMessage(message):
    if not ws.sock.connected:
        print("sendMessage: Could not send cirrus message, socket not open")
    else:
        message['timeSent'] = int(time.time() * 1000)
        msg = json.dumps(message)
        ws.send(msg)
        # print('sending message')


def sendServiceRequest():
    request = {"messageType": "ServiceRequest", "version": "1.6.4", "clientId": "123456"}
    sendMessage(request)


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


def sendPeriodicRequest():
    global HBFlag
    request = {"messageType": "PeriodicRequest", "timeSent": int(time.time() * 1000)}
    if HBFlag == 3:
        print('    periodic request missed (%s), will reconnect', HBFlag)
    else:
        print(' ---  periodic request send ' + HBFlag)
        HBFlag += 1

    sendMessage(request)


def sendGetUnitsRequest(locationID):
    request = {"messageType": 'GetUnitsRequest', "timeSent": int(time.time() * 1000), "locationAddress": {"resourceType": 'LocationAddress', "locationId": locationID}}
    print('sending getunits request for ' + locationID)
    sendMessage(request)


def sendLoginRequest():
    request = {"messageType": "LoginRequest", "username": username, "password": password}
    sendMessage(request)


def sendGetSamplesRequest(UnitDid, LocationId, start, end):
    # Create sample request to request the last 24 hour samples
    # print('start', UnitDid, LocationId, start, end)
    # print(end - start)
    # print(timedelta(days=1))

    if (end - start) <= timedelta(days=1):
        # print('1')
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
        # pprint.pprint(request)
        global requestcount
        requestcount += 1
        print('request just sent:', requestcount)  # 增加请求计数器
        # pprint.pprint(request)
        sendMessage(request)
    else:
        # print('2')
        # time_stamp = int(time.mktime(time.strptime(start, pattern) + 24 * 3600*1000))
        # startday = datetime.fromtimestamp(time.mktime(time.strptime(start, pattern)))
        start_time_plus_1D = start.replace(day=start.day + 1)
        sendGetSamplesRequest(UnitDid, LocationId, start, start_time_plus_1D)
        sendGetSamplesRequest(UnitDid, LocationId, start_time_plus_1D, end)


if __name__ == "__main__":
    print("Connecting to ", cirrusHost, "with user ", username)
    ws = websocket.WebSocketApp("wss://" + cirrusHost + "/cirrusAPI", on_message=onMessage, on_error=onError, on_close=onClose, on_open=onOpen, keep_running=True)

    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
