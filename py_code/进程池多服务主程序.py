
from concurrent.futures import ProcessPoolExecutor
import  os,sys
from check_sensor_healthy import checkSensor
from get_sensor_asset_binding import get_sensor_asset_binding


locations = [ "368307" ,"834706"  ,"234190"  ,"251092" ,"725728"  ] #yuanjin
# locations = [ "800424" ,"155697" ] #teslian
# locations = [ "402837" ,"952675 "  ,"268429"  ,"732449" ,"328916"  ] #huaian
# locations = [ "897737" ,"521209"  ] #wafer
def when_done(r):
    print(r,"Job Done")
    sys.exit(0)
    
if __name__ == '__main__':
    result={}
    with ProcessPoolExecutor() as pool:
        for loc in locations:
            Future_result=pool.submit(checkSensor,loc)
            # result[loc]= Future_result.result()
            Future_result.add_done_callback(when_done)
            
            # _=pool.submit(get_sensor_asset_binding,loc)
          
        # pool.shutdown(wait=False)
# os._exit(1)
# EUI64-D0CF5EFFFE7931A9
