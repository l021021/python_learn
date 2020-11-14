from datetime import datetime, timedelta
import time

dt = datetime(2018, 5, 1)
dt2 = datetime.strptime("2018/08/01", "%Y/%m/%d")
dt = datetime.fromtimestamp(time.time())
print(dt)
print(f"{dt.day}")
print(dt.strftime("%Y/%m"))
duration = dt2 - dt
print("seconds:", duration.seconds)
