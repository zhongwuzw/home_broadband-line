#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys

def alterJsonFile(fileName):
    with open(fileName,'r') as f:
        lines = f.readlines()
        lines[0] = ''
        lines[-1] = ','

    with open(fileName, 'w+') as f:
        f.writelines(lines)

def walkDirectory(rootDir):
    for root,dirs,files in os.walk(rootDir):
        for filespath in files:
            fileName = os.path.join(root,filespath)
            if fileName.endswith(".json"):
                alterJsonFile(fileName)

rootDir = "/Users/zhongwu/Downloads/province&cityPaths"
walkDirectory(rootDir)

