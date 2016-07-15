#!/usr/bin/python
# -*- coding: utf-8 -*-

import json

with open('/Users/zhongwu/Downloads/provinceAndCity.json', 'r') as f:
    fileStr = ''
    lines = f.readlines()
    resultJson = {}
    for line in lines:
        fileStr += line
    decodeJson = json.loads(fileStr)
    for key in decodeJson:
        code = decodeJson[key]["code"]
        value = decodeJson[key]
        resultJson[code] = value
    resultJsonStr = json.dumps(resultJson)
with open('/Users/zhongwu/Downloads/provinceAndCity.json', 'w+') as f:
    f.write(resultJsonStr)