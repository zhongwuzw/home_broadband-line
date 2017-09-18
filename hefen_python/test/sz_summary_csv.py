#!/usr/bin/python
# -*- coding: utf-8 -*-

import os

file_zip1 = open("/opt/zhongwu/test/detail.txt")

# file_zip1 = open("/Users/zhongwu/Documents/detail.txt")

def get_file_lines(file,separator):
    zip_set = set()
    for line in file:
        line = line.split(separator)[1]
        line = "/opt/script/report_insertdb_700021_999999/backReportPath" + line
        line = line.replace("\r\n","")
        line = line.replace("\r", "")
        line = line.replace("\n", "")
        # line = line.replace("detail","summary")
        zip_set.add(line)
    return zip_set

zip1_set = get_file_lines(file_zip1, "/public")

print "-----"

for item in zip1_set:
    print item
    os.system('cp ' + item + ' /opt/zhongwu/test/')
