#!/bin/bash
file_dir=/data1/ots/
dest_filepath=/data1/OTS_NEW
bak_dir=/data1/ots_bak/
date=`date +%Y%m%d`
log=/data1/ots/log/"$date"unzip.log
for name in `ls "$file_dir" |grep zip`
do
dir_name=`unzip -l "$file_dir""$name" | grep .csv | head -n 1 | awk '{print $NF}'`
dir_name=${dir_name%/*}
if [[ ! -d "$dir_name" ]]; then
	console_output=`/usr/bin/unzip -o "$file_dir""$name" -d "$dest_filepath"`
	if [ $? == 0 ];then
	mv "$file_dir""$name" "$bak_dir""$name"
	echo "[`date +%y%m%d/%H:%M:%S`]" $console_output >> $log
	else
	echo "[`date +%y%m%d/%H:%M:%S`] error :unzip $file_dir$name" >> $log
	fi
fi
done
