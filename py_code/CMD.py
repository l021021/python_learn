"""
@author: Bruce
 这个是调度工具,可以以多线程的方式,使用三个工具:
 1. 检查传感器状态
 2. 得到传感器名字.利用asset来给sensor命名
 3. 得到传感器占用数据
 
 分别需要利用注释来区分三个任务
 
 传入的参数: 
  startstr = '2021-03-08-09-00-00' 开始日期
  endstr = '2021-03-12-19-59-59'  结束日爱
  datatype = 'UUID'  # Motion | UUID  #选择要采的数据类型
  locations = [ "368307" ,"834706"  ,"234190"  ,"251092" ,"725728"  ] 传感器列表
  生成的结果在c:\LOG下
 
"""

from concurrent.futures import ProcessPoolExecutor
import  os,sys

#!! 需要注释下面
from check_sensor_healthy import checkSensor
# from get_sensor_asset_binding import get_sensor_asset_binding
# from get_motion_history import get_motion_history

#!! 需要注释下面来传递参数
startstr = '2021-03-08-09-00-00'
endstr = '2021-03-12-19-59-59'
datatype = 'UUID'  # Motion | UUID  #选择要采的数据类型
locations = [ "368307" ,"834706"  ,"234190"  ,"251092" ,"725728"  ] #yuanjin
# locations = [ "800424" ,"155697" ] #teslian
# locations = ["402837", "952675", "268429", "732449", "328916"]  # huaian
# locations = ["836278", "951519", "480686", "578981", "459374", "395065","274189"]  # sunao
# locations = [ "897737" ,"521209"  ] #wafer
counter=len(locations)
def when_done(r):
    global counter
    counter -=1
    if counter==0:
        os._exit(0)
    
if __name__ == '__main__':
    with ProcessPoolExecutor() as pool:
        for loc in locations:
            #!! 需要注释下面来区分任务
            Future_result = pool.submit(checkSensor, loc)
            # Future_result = pool.submit(get_motion_history, loc, startstr, endstr, datatype)
            # Future_result = pool.submit(get_sensor_asset_binding, loc)
            Future_result.add_done_callback(when_done)
 
        # pool.shutdown(wait=False)
