#!/bin/bash

. ../scripts/shell_settings.sh

num_server=$1
my_server_ip=$2

python redis-instance-starter.py $num_server $memcached_ip $my_server_ip