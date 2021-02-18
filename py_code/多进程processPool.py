
from concurrent.futures import ProcessPoolExecutor
from CHECK_Sensor_healthy_IN_5M_ALPHA import checkSensor
locations = [ "368307" ,"834706"  ,"234190"  ,"251092" ,"725728"  ]

if __name__ == '__main__':
    with ProcessPoolExecutor() as pool:
        for loc in locations:
            _=pool.submit(checkSensor,loc)
            # pool.shutdown(wait=False)