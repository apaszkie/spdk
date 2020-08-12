#!/usr/bin/env bash

testdir=$(readlink -f $(dirname $0))
rootdir=$(readlink -f $testdir/../..)
source $rootdir/test/common/autotest_common.sh
source $testdir/common.sh

devices=(\
'0000:5e:00.0' \
'0000:8a:00.0' \
'0000:8b:00.0' \
'0000:b1:00.0' \
'0000:b8:00.0' \
'0000:bb:00.0' \
'0000:bd:00.0' \
'0000:d8:00.0' \
'0000:d9:00.0' \
)

$rootdir/scripts/setup.sh reset

num_disks=$1
for (( j=0; j<$num_disks; j++ )) do
	nvme=$(ls -l /sys/block/nvme* | awk -v bdf=${devices[$j]} '$0 ~ bdf {print $11}' | xargs -d/ -i  echo {} | tail -2 | head -1)
	nvme=/dev/$nvme
	host_writes=$(nvme intel smart-log-add $nvme | awk '/host_bytes_written/ {print $5}')
	nand_writes=$(nvme intel smart-log-add $nvme | awk '/nand_bytes_written/ {print $5}')
	echo $host_writes $nand_writes
done

HUGEMEM=8192 $rootdir/scripts/setup.sh
