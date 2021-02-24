
from concurrent.futures import ProcessPoolExecutor
import  os
from check_sensor_healthy import checkSensor
from get_sensor_asset_binding import get_sensor_asset_binding


locations = [ "368307" ,"834706"  ,"234190"  ,"251092" ,"725728"  ]
# locations = [ "800424" ,"155697" ] #teslian
# locations = [ "402837" ,"952675 "  ,"268429"  ,"732449" ,"328916"  ] #huaian
# locations = [ "897737" ,"521209"  ] #wafer

if __name__ == '__main__':
    with ProcessPoolExecutor() as pool:
        for loc in locations:
            # _=pool.submit(checkSensor,loc)
            _=pool.submit(get_sensor_asset_binding,loc)
            
    # pool.shutdown(wait=True)
    os._exit(1)
# EUI64-D0CF5EFFFE7931A9
