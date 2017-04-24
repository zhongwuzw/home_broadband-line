#!/bin/bash
SRCFILE="/opt/gnode-data/tmp/table_"

for i in 0 1 2 3 4 5; do
	`rm -f ${SRCFILE}${i}.dat`
done
