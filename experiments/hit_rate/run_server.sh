#!/bin/bash

. ../scripts/shell_settings.sh
echo "memcached_ip=$memcached_ip"

. ../utils/utils.sh
get_server_config $1
config_fname=$ret_config_fname

if [ -z "$config_fname" ]; then
    echo "Unsupported server type $1"
    exit
fi

# memcached_ip="10.10.1.1"

../../build/experiments/init -S -c ../configs/$config_fname -m $memcached_ip
