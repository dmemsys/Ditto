#!/bin/bash

. ../scripts/shell_settings.sh
echo "memcached_ip=$memcached_ip"

. ../utils/utils.sh
get_client_config $1
config_fname=$ret_config_fname

if [ -z $config_fname ]; then
    echo "Unknown client type $1"
    exit
fi

st_server_id=$2
workload=$3
num_threads=8

# memcached_ip="10.10.1.1"

../../build/experiments/init -C -c ../configs/$config_fname -w $workload -m $memcached_ip -n -1 -i $st_server_id -t $num_threads -A 8 -T 200 -E mem
