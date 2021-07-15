import websocket
websocket.enableTrace(True)
ws = websocket.WebSocket()
ws.connect("ws://echo.websocket.org")
ws.send("Hello, Server")
print(ws.recv())
ws.close()
