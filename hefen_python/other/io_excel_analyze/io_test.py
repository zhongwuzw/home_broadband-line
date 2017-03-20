#!/usr/bin/python
# -*- coding: utf-8 -*-

import os

#执行中间表脚本
for i in range(3,11):
    io_10_test = './iozone -a -n 512m -g 4g -U /data10/ -f /data10/temp -Rb ./iozone10-' + str(i) + '.xls'
    io_50_test = './iozone -a -n 512m -g 4g -U /data50/ -f /data50/temp -Rb ./iozone50-' + str(i) + '.xls'
    os.system(io_10_test)
    os.system(io_50_test)

print "执行完"