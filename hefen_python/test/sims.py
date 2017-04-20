#!/usr/bin/python
# -*- coding: utf-8 -*-

file_zip1 = open("/Users/zhongwu/Documents/map.log")

str = ""

for item in file_zip1:
    str = str + "'" + item.replace('\n','') + "/',"

print str