#!/usr/bin/env bash

testdir=$(readlink -f $(dirname $0))
rootdir=$(readlink -f $testdir/../..)
source $rootdir/test/common/autotest_common.sh
source $testdir/common.sh

tests=(\
'-w write -q 32 -o 131072 -t 300' \
'-w read -q 32 -o 131072 -t 60' \
'-w randwrite -q 32 -o 4096 -t 300' \
'-w randwrite -q 32 -o 4096 -t 300' \
'-w randwrite -q 32 -o 4096 -t 300' \
'-w randread -q 128 -o 4096 -t 300' \
'-w randread -q 128 -o 4096 -t 300' \
)

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

num_disks=$1
disk_size=$2
zone_size=$3
write_unit_size=$4
core_mask=$5
io_cores=$6
RESULTS_DIR=$7

rpc_py=$rootdir/scripts/rpc.py

ftl_bdev_conf=$testdir/config/ftl.conf
gen_ftl_nvme_conf > $ftl_bdev_conf

$rootdir/test/bdev/bdevperf/bdevperf -m $core_mask -C -z -T ftl0 ${tests[0]} &
bdevperf_pid=$!

trap 'killprocess $bdevperf_pid; exit 1' SIGINT SIGTERM EXIT
waitforlisten $bdevperf_pid

nvme_ctrls=""
for (( j=0; j<$num_disks; j++ )) do
	$rpc_py bdev_nvme_attach_controller -b nvme${j} -a ${devices[$j]} -t pcie
	splits=$($rpc_py bdev_split_create -s $disk_size nvme${j}n1 1)
	nvme_ctrls+="${splits[0]} "
done

$rpc_py bdev_raid_create -z 32 -r 0 -b "$nvme_ctrls" -n raid
$rpc_py bdev_zone_block_create -z $zone_size -o 1 -b zone0 -w $write_unit_size -n raid
$rpc_py bdev_ftl_create -b ftl0 -d zone0 --core_mask $core_mask --overprovisioning 20
$rpc_py save_config > $rootdir/ftl.json
killprocess $bdevperf_pid

for (( i=0; i<${#tests[@]}; i++ )) do
	timing_enter "${tests[$i]}"

	test="${tests[$i]}"
	test=${test//-/}
	test=${test// /_}
	log=drives_"$num_disks"_size_"$disk_size"_zs_"$zone_size"_wus_"$write_unit_size"_cm_"$core_mask"_ioc_"$io_cores"_"$test"
	log="$RESULTS_DIR$log"

	$rootdir/test/bdev/bdevperf/bdevperf -m $io_cores -C -T ftl0 --json $rootdir/ftl.json ${tests[$i]} |& tee "$log"
	total_iops=$(less $log | awk '/Total/ {print $4}' | tail -1)
	total_mb=$(less $log | awk '/Total/ {print $6}' | tail -1)
	total_writes=$(less $log | awk '/total writes/ {print $3}' | tail -1)
	user_writes=$(less $log | awk '/user writes/ {print $3}' | tail -1)
	waf=$(less $log | awk '/WAF/ {print $2}' | tail -1)
	reloc_iops=$(less $log | awk '/reloc write IOPS/ {print $4}' | tail -1)

	echo $test,$num_disks,$disk_size,$zone_size,$write_unit_size,$core_mask,$io_cores,$total_iops,$total_mb,$total_writes,$user_writes,$waf,$reloc_iops >> "$RESULTS_DIR"log.csv
	timing_exit "${tests[$i]}"
done

trap - SIGINT SIGTERM EXIT
rm -f $ftl_bdev_conf
