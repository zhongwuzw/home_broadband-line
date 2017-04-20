#!/usr/bin/python
# -*- coding: utf-8 -*-

file_zip1 = open("/Users/zhongwu/Documents/zip_node1.log")
file_zip2 = open("/Users/zhongwu/Documents/zip_node2.log")

file_zip = open("/Users/zhongwu/Documents/zip.log")
file_zip3 = open("/Users/zhongwu/Documents/zip1.log")

file_zip4 = open("/Users/zhongwu/Documents/ziranwei_zip_ana.txt")

def get_file_lines(file):
    zip_set = set()
    for line in file:
        zip_set.add(line)
    return zip_set

zip1_set = get_file_lines(file_zip1)
zip2_set = get_file_lines(file_zip2)
zip_set = get_file_lines(file_zip)
zip3_set = get_file_lines(file_zip3)
zip4_set = get_file_lines(file_zip4)
zip_1_2_set = zip1_set | zip2_set
zip_0_3_set = zip3_set | zip_set
# print len(zip_1_2_set)
print len(zip4_set)
print len(zip_0_3_set)
zip_extra_set = zip4_set - zip_0_3_set
# zip_extra_chao_set = zip_1_2_set - zip_set

print  "-----"
print len(zip_extra_set)

for item in zip_extra_set:
    print item