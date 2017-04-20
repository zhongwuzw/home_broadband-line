#!/usr/bin/python
# -*- coding: utf-8 -*-

file_zip1 = open("/Users/zhongwu/Documents/test.log")

def get_file_lines(file):
    zip_set = set()
    for line in file:
        item1 = line.split("\t")
        zip_set.add(item1[1].replace('\n',''))
    return zip_set

zip1_set = get_file_lines(file_zip1)

str = "("
i = 0
for item in zip1_set:
    if i == 0:
        str = str + "'" + item + "'"
    else:
        str = str + ",'" + item + "'"
    i += 1
str = str + ')'
print str