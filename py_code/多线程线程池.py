from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from CHECK_Sensor_healthy_IN_5M_ALPHA import checkSensor
locations = [ "368307" ,"834706"  ,"234190"  ,"251092" ,"725728"  ]

if __name__ == '__main__':
    pool = pool = ThreadPoolExecutor(len(locations))
    with ProcessPoolExecutor() as pool:
        for loc in locations:
            pool.submit(checkSensor,loc)
            # pool.shutdown(wait=False)
