#!/usr/bin/python
# -*- coding: utf-8 -*-

import xlrd
from xlutils.copy import copy

base_path = "/Users/zhongwu/Documents/"

save_file_path_10 = base_path + "iozone10_mean.xls"
save_file_path_50 = base_path + "iozone50_mean.xls"

final_save_file_path = base_path + "iozone_final.xls"

num = 3

def get_workbook_sheet(xls_path):
    book = xlrd.open_workbook(xls_path,encoding_override="utf-8")
    return book.sheet_by_index(0)

def calculate_mean_value(sh_list,save_file_path):
    sh1 = sh_list[0]
    sh1_copy = copy(sh1.book)
    for rx in range(sh1.nrows):
        ss = sh1.row(rx)[0].value
        if isinstance(ss, float) and sh1.row(rx)[0] > 0:
            for colx in range(5, 14):
                sum = 0
                for sheet in sh_list:
                    sum += sheet.cell_value(rowx=rx, colx=colx)
                mean_value = sum / len(sh_list)
                sh1_copy.get_sheet(0).write(rx,colx,mean_value)

    sh1_copy.save(save_file_path)


def compare_10_50_value(sh10, sh50, save_file_path):
    sh1_copy = copy(sh10.book)

    count10 = 0
    count50 = 0

    for rx in range(sh10.nrows):
        ss = sh10.row(rx)[0].value
        if isinstance(ss, float) and sh10.row(rx)[0] > 0:
            for colx in range(5, 14):
                difference_value = sh10.cell_value(rowx=rx,colx=colx) - sh50.cell_value(rowx=rx,colx=colx)

                if difference_value > 0 :
                    count10 += 1
                else:
                    count50 += 1
                sh1_copy.get_sheet(0).write(rx,colx,difference_value)

    print count10,count50
    sh1_copy.save(save_file_path)

xls_10_sheet_list = []
xls_50_sheet_list = []

for i in range(0, num) :
    path_10 = base_path + "iozone10-" + str(i) + ".xls"
    path_50 = base_path + "iozone50-" + str(i) + ".xls"

    xls_10_sheet_list.append(get_workbook_sheet(path_10))
    xls_50_sheet_list.append(get_workbook_sheet(path_50))

calculate_mean_value(xls_10_sheet_list, save_file_path_10)
calculate_mean_value(xls_50_sheet_list, save_file_path_50)

compare_10_50_value(get_workbook_sheet(save_file_path_10), get_workbook_sheet(save_file_path_50), final_save_file_path)

print "Done!"

