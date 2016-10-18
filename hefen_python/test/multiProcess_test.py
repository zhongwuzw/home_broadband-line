# -*- coding: utf-8 -*-

import multiprocessing
import time

str = "sss"

def worker(interval):
    n = 5
    while n > 0:
        print("The time is {0}".format(time.ctime()))
        time.sleep(interval)
        print str
        n -= 1

if __name__ == "__main__":
    p = multiprocessing.Process(target = worker, args = (3,))
    p1 = multiprocessing.Process(target=worker, args=(3,))
    p.start()
    p1.start()
    p.join()
    p1.join()
    print "p.pid:", p.pid
    print "p.name:", p.name
    print "p.is_alive:", p.is_alive()