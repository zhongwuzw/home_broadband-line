#!/usr/bin/python
# -*- coding: utf-8 -*-

file_zip1 = open("/Users/zhongwu/Downloads/51.txt")
file_zip2 = open("/Users/zhongwu/Downloads/60.txt")

def get_file_lines(file,separator):
    zip_set = set()
    for line in file:
        line = line.split(separator)[1]
        zip_set.add(line)
    return zip_set

zip1_set = get_file_lines(file_zip1, "/OTS_NEW")
zip2_set = get_file_lines(file_zip2, "/data3/OTS_NEW_8181")
zip_1_2_set = zip1_set - zip2_set

print "-----"

for item in zip_1_2_set:
    print item