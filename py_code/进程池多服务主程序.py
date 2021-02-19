
from concurrent.futures import ProcessPoolExecutor
import  os
from check_sensor_healthy import checkSensor
locations = [ "368307" ,"834706"  ,"234190"  ,"251092" ,"725728"  ]

if __name__ == '__main__':
    with ProcessPoolExecutor() as pool:
        for loc in locations:
            _=pool.submit(checkSensor,loc)
    # pool.shutdown(wait=True)
    os._exit(1)