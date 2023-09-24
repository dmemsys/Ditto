#!/bin/bash

. ../scripts/shell_settings.sh

my_server_ip=$1

python redis-instance-controller.py 0 $memcached_ip $my_server_ip