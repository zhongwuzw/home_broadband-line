#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys

allLines = []

def alterJsonFile(fileName):
    with open(fileName,'r') as f:
        global allLines
        lines = f.readlines()
        allLines += lines

def walkDirectory(rootDir):
    for root,dirs,files in os.walk(rootDir):
        for filespath in files:
            fileName = os.path.join(root,filespath)
            if fileName.endswith(".json"):
                alterJsonFile(fileName)

rootDir = "/Users/zhongwu/Downloads/province&cityPaths"
walkDirectory(rootDir)
with open("/Users/zhongwu/Downloads/test.json", 'w+') as f:
    f.writelines(allLines)
