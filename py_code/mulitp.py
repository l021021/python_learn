from multiprocessing import Pool,freeze_support
import os
from CHECK_Sensor_healthy_IN_5M_ALPHA import checkSensor
# p=[]

locations = [ "368307" , "834706"  ,"234190"  ,"251092" ,"725728"  ]

if __name__ == '__main__':
    freeze_support()
    # global p
    p =Pool(len(locations)) 
    for loc in locations:
        p.apply_async(checkSensor, args=(loc,))
        # print('Run child process (%s)...' % ( os.getpid(p)))
    p.close()
    # p.start()
    p.join()
