#!/usr/bin/env bash

testdir=$(readlink -f $(dirname $0))
rootdir=$(readlink -f $testdir/../..)
source $rootdir/test/common/autotest_common.sh
source $testdir/common.sh
rpc_py=$rootdir/scripts/rpc.py

fio_kill() {
	killprocess $svcpid
}

tests=('drive-prep war war war')

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

device_cache='0000:be:00.0'

num_disks=$1
disk_size=$2
zone_size=$3
write_unit_size=$4
core_mask=$5
io_cores=$6
RESULTS_DIR=$7

if [[ $CONFIG_FIO_PLUGIN != y ]]; then
	echo "FIO not available"
	exit 1
fi

export FTL_BDEV_NAME=ftl0
export FTL_JSON_CONF=$testdir/config/ftl.json

trap "fio_kill; exit 1" SIGINT SIGTERM EXIT

"$SPDK_BIN_DIR/spdk_tgt" -m 1f --json <(gen_ftl_nvme_conf) &
svcpid=$!
waitforlisten $svcpid

nvme_ctrls=""
for (( j=0; j<$num_disks; j++ )) do
	$rpc_py bdev_nvme_attach_controller -b nvme${j} -a ${devices[$j]} -t pcie
	splits=$($rpc_py bdev_split_create -s $disk_size nvme${j}n1 1)
	nvme_ctrls+="${splits[0]} "
done

nv_cache=$($rpc_py bdev_nvme_attach_controller -b nvme${j} -a $device_cache -t pcie)
nv_cache=$($rpc_py bdev_split_create -s $disk_size ${nv_cache} 1)

$rpc_py bdev_raid_create -z 16 -r 0 -b "$nvme_ctrls" -n raid
$rpc_py bdev_zone_block_create -z $zone_size -o 1 -b zone0 -w $write_unit_size -n raid
$rpc_py bdev_ftl_create -b ftl0 -d zone0 --core_mask $core_mask --overprovisioning 25 -c ${nv_cache}
waitforbdev ftl0

(
	echo '{"subsystems": ['
	$rpc_py save_subsystem_config -n bdev
	echo ']}'
) > $FTL_JSON_CONF

killprocess $svcpid


trap - SIGINT SIGTERM EXIT

for test in ${tests}; do
	log=drives_"$num_disks"_size_"$disk_size"_zs_"$zone_size"_wus_"$write_unit_size"_cm_"$core_mask"_ioc_"$io_cores"_"$test"
	log="$RESULTS_DIR$log"

	timing_enter $test
	fio_bdev $testdir/config/fio/$test.fio
	timing_exit $test
done


