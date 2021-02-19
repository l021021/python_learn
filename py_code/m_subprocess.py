from multiprocessing import Pool,freeze_support
import subprocess
from CHECK_Sensor_healthy_IN_5M_ALPHA import checkSensor
r=[]

locations = [ "368307" , "834706"  ,"234190"  ,"251092" ,"725728"  ]

if __name__ == '__main__':
    # freeze_support()
    # p =Pool(len(locations)) 
    for loc in range(len(locations)):
        r[loc]=subprocess.call.apply_async(checkSensor, args=(loc,))
        # print('Run child process (%s)...' % ( os.getpid(p)))
    p.close()
    # p.start()
    p.join()
