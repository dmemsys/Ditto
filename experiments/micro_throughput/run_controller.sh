#!/bin/bash

. ../scripts/shell_settings.sh
echo "memcached_ip=$memcached_ip"

. ../utils/utils.sh

get_out_fname $1
out_fname=$ret_out_fname

if [ -z "$out_fname" ]; then
    echo "Unsupported server type $1"
    exit
fi

# memcached_ip="10.10.1.1"

num_servers=$2
num_clients=$3

python ../controller.py micro -s $num_servers -c $num_clients -o $out_fname -m $memcached_ip