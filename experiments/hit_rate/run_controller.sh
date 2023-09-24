#!/bin/bash

. ../scripts/shell_settings.sh
echo "memcached_ip=$memcached_ip"

. ../utils/utils.sh

num_servers=$2
num_clients=$3
workload=$4

get_out_fname $1
out_fname=$ret_out_fname

if [ -z "$out_fname" ]; then
    echo "Unsupported server type $1"
    exit
fi

# memcached_ip="10.10.1.1"

python ../controller.py hit-rate-$workload -s $num_servers -c $num_clients -o $out_fname -m $memcached_ip

if [ $workload = "mix" ]; then
    mv hit-rate-mix-$out_fname-s1-c10.json hit-rate-mix-$out_fname-s1-c10-$5-$6.json
elif [ $workload = "webmail-11" ]; then
    mv hit-rate-webmail-11-$out_fname-s1-c$num_clients.json hit-rate-webmail-11-$out_fname-s1-c$num_clients-$5.json
fi