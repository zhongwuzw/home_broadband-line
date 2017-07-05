#!/bin/bash
i=1
b=76.227
a=76.226
time=`date -d "1 days ago" +%Y%m%d`
Time=`date +%Y%m%d`
echo "---------------------------"
echo `date -d "1 days ago" +%Y%m%d`
echo `date +%Y%m%d`
echo "实时自然维summary:"
c=`sed -n "/$a/,/$b/p" /opt/ots/log/${time}unzip.log | grep summary.csv | grep "$time" | sort -u | wc -l`
o=`sed -n "/$a/,/$b/p" /opt/ots/log/${Time}unzip.log | grep summary.csv | grep "$time" | sort -u | wc -l`
p=`expr $c + $o`
echo "$p"
echo "实时自然维总zip:"
d=`cat /opt/ots/log/${time}unzip.log | grep ^Archive | grep $a | grep "$time" | sort -u | wc -l`
q=`cat /opt/ots/log/${Time}unzip.log | grep ^Archive | grep $a | grep "$time" | sort -u | wc -l`
r=`expr $d + $q`
echo "$r"
echo "实时自然维失败zip:"
e=`cat /opt/ots/log/${time}unzip.log | grep error | grep $a | grep "$time" | sort -u | wc -l`
s=`cat /opt/ots/log/${Time}unzip.log | grep error | grep $a | grep "$time" | sort -u | wc -l`
t=`expr $e + $s`
echo "$t"
echo "实时自然维zip分时:"

while [ $i -lt 24 ]
do

if [ $i -le 9  ];then

f=`grep  Archive /opt/ots/log/${time}unzip.log | grep  $a | grep  "${time}0$i" | sort -u | wc -l`
u=`grep  Archive /opt/ots/log/${Time}unzip.log | grep  $a | grep  "${time}0$i" | sort -u | wc -l`

v=`expr $f + $u`
echo "$v" 

else

g=`grep  Archive  /opt/ots/log/${time}unzip.log | grep  $a | grep  "$time$i" | sort -u | wc -l`
w=`grep  Archive  /opt/ots/log/${Time}unzip.log | grep  $a | grep  "$time$i" | sort -u | wc -l`

x=`expr $g + $w`
echo "$x" 

fi
let i++
done
echo "实时自然维解压zip失败分时:"
j=1
while [ $j -lt 24 ]
do

if [ $j -le 9  ];then

h=`grep  error /opt/ots/log/${time}unzip.log | grep  $a | grep  "${time}0$j" | sort -u | wc -l`
y=`grep  error /opt/ots/log/${Time}unzip.log | grep  $a | grep  "${time}0$j" | sort -u | wc -l`

z=`expr $h + $y`
echo "$z" 

else

k=`grep  error  /opt/ots/log/${time}unzip.log | grep  $a | grep  "$time$j" | sort -u | wc -l`
A=`grep  error  /opt/ots/log/${Time}unzip.log | grep  $a | grep  "$time$j" | sort -u | wc -l`

B=`expr $k + $A`
echo "$B" 

fi
let j++
done
echo "实时自然维summary分时:"
l=0
while [ $l -lt 24 ]
do

if [ $l -le 9  ];then

m=`sed -n "/$a/,/$b/p" /opt/ots/log/${time}unzip.log | grep summary.csv  | grep "${time}0$l" | sort -u | wc -l`
C=`sed -n "/$a/,/$b/p" /opt/ots/log/${Time}unzip.log | grep summary.csv  | grep "${time}0$l" | sort -u | wc -l`
D=`expr $m + $C`

echo "$D" 

else

n=`sed -n "/$a/,/$b/p" /opt/ots/log/${time}unzip.log | grep summary.csv  | grep "$time$l" | sort -u | wc -l`
E=`sed -n "/$a/,/$b/p" /opt/ots/log/${Time}unzip.log | grep summary.csv  | grep "$time$l" | sort -u | wc -l`

F=`expr $n + $E`
echo "$F" 

fi
let l++
done
