# -*- coding: utf-8 -*-

import datetime
import os

d = datetime.datetime.strptime("01-01-2001", "%m-%d-%Y")
time_osx = d + datetime.timedelta(seconds=8*60*60) + datetime.timedelta(seconds = 495725233.640427)
time_converted = time_osx.strftime("%a, %d %b %Y %H:%M:%S")

print time_converted

n = os.system('/Users/zhongwu/Documents/workspace/test.sh 2014')
print n
