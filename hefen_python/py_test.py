# -*- coding: utf-8 -*-

dic = {}
if dic.has_key('haha'):
    print dic['haha']
else:
    dic['haha'] = 0
dic['haha'] += 1
dic['haha'] += 1
print dic
print dic.get('sssss',0)

dic['ss'] = {'sss':'fsfd'}
print dic
print  dic['ss']['sss']

testSet = set()

testSet.add('ssss')
testSet.add('ssss')
print testSet
print  dic.get('ss',{}).get('sss',0)

print min(4,1,2,3,4)