#!/usr/bin/env bash

testdir=$(readlink -f $(dirname $0))
rootdir=$(readlink -f $testdir/../..)
source $rootdir/test/common/autotest_common.sh
source $testdir/common.sh

rpc_py=$rootdir/scripts/rpc.py

num_disks=9
disk_size=$((1024*128))
zone_size=$((262144))
write_unit_size=128

RESULTS_DIR=results/$(date +%F-%R)/
mkdir -p $RESULTS_DIR

echo test,num_disks,disk_size,zone_size,write_unit_size,core_mask,io_cores,total_iops,total_mb,total_writes,user_writes,waf,reloc_iops >> "$RESULTS_DIR"log.csv

for (( i=1; i<=num_disks; i++ )) do
    for ftl_core in $(seq 2 3); do
        core_mask=$(python -c "print '{:x}'.format(int($ftl_core*'1', 2))")
        for io_core in $(seq 1 2); do
            io_cores=$(python -c "print '{:x}'.format(int(($io_core+4)*'1', 2))")
            run_test "ftl_bdevperf" $testdir/ftl_raid_perf.sh $i $disk_size $zone_size $write_unit_size $core_mask $io_cores $RESULTS_DIR
        done
    done
done
