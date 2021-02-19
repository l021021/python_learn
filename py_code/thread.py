
import time
import threading
import random

# 新线程执行的代码:


def loop():
    print('thread %s is running...' % threading.current_thread().name)
    n = 0
    while n < 3:
        n = n + 1
        print('thread %s >>> %s' % (threading.current_thread().name, n))
        time.sleep(random.randint(1,5))
    print('thread %s ended.' % threading.current_thread().name)


print('thread %s is running...' % threading.current_thread().name)
for i in range(5):
    t = threading.Thread(target=loop)
    t.start()
    # t.join()
print('thread %s ended.' % threading.current_thread().name)
