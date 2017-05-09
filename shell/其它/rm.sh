#!/bin/bash
for line in "$@" ; do
    if [ "$line" == "/" ];then
        echo " can't rm / "
        exit 0;
    elif [[ "$line" == "/*" ]]; then
    	echo " can't rm / "
        exit 0;
    fi
done
rm $*
