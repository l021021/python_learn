import websocket
import time
import json
import ssl

#Set up endpoint, you'll probably need to change this
cirrusAPIendpoint = "cirrus21.yanzi.se"

##########CHANGE BELOW TO YOUR OWN DATA##########

#Set up credentials. Please DONT have your credentials in your code when running on production
username = 'frank.shen@pinyuaninfo.com'
password = 'Ft@Sugarcube99'

#Set up Location ID and Device ID, please change this to your own, can be found in Yanzi Live
locationID = "LOCATION ID"  #Usually a 6 digit number
deviceID = "DEVICE ID"  #Found in Yanzi Live, ends with "-Temp"

################################################


#Handle message
def onMessage(ws, message):
    response = json.loads(message)
    if (response["messageType"]
            == "ServiceResponse") and (response["responseCode"]["name"]
                                       == "success"):
        print("ServiceRequest succeeded, sending LoginRequest")
        time.sleep(
            1
        )  #Note: Sleep is only used for presentation purposes, not actually needed in the integration.
        login(ws)
    elif (response["messageType"]
          == "LoginResponse") and (response["responseCode"]["name"]
                                   == "success"):
        print("LoginRequest succeeded, lets get some data...")
        time.sleep(
            1
        )  #Note: Sleep is only used for presentation purposes, not actually needed in the integration.
        getTemperatureLastHour(ws, locationID, deviceID)
    elif (response["messageType"]
          == "GetSamplesResponse") and (response["responseCode"]["name"]
                                        == "success"):
        print("Yaaaay, temperaturedata in abundance!")
        for element in enumerate(response["sampleListDto"]["list"]):
            print(element)
        ws.close()
    else:
        print("Error in: " + response["messageType"])
        print(response)
        ws.close()


#Handle error
def onError(ws, error):
    print("Error: " + error)


#Handle closing of websocket
def onClose(ws):
    print("Closing connection!")


#Handle opening of websocket
def onOpen(ws):
    print("Websocket open!")
    print("Checking API service status with ServiceRequest.")
    time.sleep(
        1
    )  #Note: Sleep is only used for presentation purposes, not actually needed in the integration.
    serviceRequest()


#Check API service status
def serviceRequest():
    ws.send(json.dumps({"messageType": "ServiceRequest"}))


#Login
def login(ws):
    ws.send(
        json.dumps({
            "messageType": "LoginRequest",
            "username": username,
            "password": password
        }))


#Get temperature for the last hour
def getTemperatureLastHour(ws, locationID, deviceID):
    now = int(round(time.time() * 1000))
    nowMinusOneHour = now - 60 * 60 * 1000
    ws.send(
        json.dumps({
            "messageType": "GetSamplesRequest",
            "dataSourceAddress": {
                "resourceType": "DataSourceAddress",
                "did": deviceID,
                "locationId": locationID,
                "variableName": {
                    "resourceType": "VariableName",
                    "name": "temperatureC"
                }
            },
            "timeSerieSelection": {
                "resourceType": "TimeSerieSelection",
                "timeStart": nowMinusOneHour,
                "timeEnd": now
            },
            "timeSent": now
        }))


if __name__ == "__main__":
    print("Connecting to " + cirrusAPIendpoint + " with user: " + username)
    time.sleep(
        1
    )  #Note: Sleep is only used for presentation purposes, not actually needed in the integration.

    ws = websocket.WebSocketApp("wss://" + cirrusAPIendpoint + "/cirrusAPI",
                                on_message=onMessage,
                                on_error=onError,
                                on_close=onClose)
    ws.on_open = onOpen
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
