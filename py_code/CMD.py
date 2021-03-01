
from concurrent.futures import ProcessPoolExecutor
import  os,sys
# from check_sensor_healthy import checkSensor
# # from get_sensor_asset_binding import get_sensor_asset_binding
from get_motion_history import get_motion_history

startstr = '2021-02-21-09-00-00'
endstr = '2021-02-21-17-59-59'
datatype = 'UUID'  # Motion | UUID  #选择要采的数据类型


locations = [ "368307" ,"834706"  ,"234190"  ,"251092" ,"725728"  ] #yuanjin
# locations = [ "800424" ,"155697" ] #teslian
# locations = [ "402837" ,"952675 "  ,"268429"  ,"732449" ,"328916"  ] #huaian
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
            # Future_result = pool.submit(checkSensor, loc)
            Future_result = pool.submit(get_motion_history, loc,startstr,endstr,datatype)
            Future_result.add_done_callback(when_done)
 
        # pool.shutdown(wait=False)
