#!/bin/bash

url=http://$1

if [ "$2" = "twitter" ]; then
    if [ ! -d "twitter" ]; then
        mkdir twitter
    fi
    wget $url/twitter/twitter-exp.tgz
    mv twitter-exp.tgz twitter && cd twitter && tar xf twitter-exp.tgz && mv twitter-exp/* . && rm -rf twitter-exp && cd ..
elif [ "$2" = "ycsb" ]; then
    if [ ! -d ycsb ]; then
        mkdir ycsb
    fi
    wget $url/ycsb/ycsb.tgz
    mv ycsb.tgz ycsb
    cd ycsb && tar xf ycsb.tgz && cd ..
elif [ "$2" = "wiki" ]; then
    if [ ! -d wiki ]; then
        mkdir wiki
    fi
    wget $url/wiki/wiki.tgz
    mv wiki.tgz wiki
    cd wiki && tar xf wiki.tgz && cd ..
elif [ "$2" = "changing" ]; then
    if [ ! -d changing ]; then
        mkdir changing
    fi
    wget $url/changing/changing.trc
    mv changing.trc changing
    # changing-small
    wget $url/changing/changing-small.trc
    mv changing-small.trc changing
elif [ "$2" = "webmail" ]; then
    if [ ! -d fiu ]; then
        mkdir fiu
    fi
    wget $url/fiu/webmail-d16.fiu
    mv webmail-d16.fiu fiu
    # webmail-all
    wget $url/fiu/webmail-all.tgz
    mv webmail-all.tgz fiu
    cd fiu && tar xf webmail-all.tgz && cd ..
    # webmail-11
    wget $url/fiu/webmail-11.tgz
    mv webmail-11.tgz fiu
    cd fiu && tar xf webmail-11.tgz && cd ..
elif [ "$2" = "mix" ]; then
    wget $url/mix.tgz
    tar xf mix.tgz
    mv para_mix mix
    wget $url/mix/lfu-acc-trace-nn.trc
    wget $url/mix/lru-acc-trace-nn.trc
    mv lfu-acc-trace-nn.trc mix
    mv lru-acc-trace-nn.trc mix
    wget $url/mix/lfu-acc-trace-nnn.trc
    wget $url/mix/lru-acc-trace-nnn.trc
    mv lfu-acc-trace-nnn.trc mix
    mv lru-acc-trace-nnn.trc mix
elif [ "$2" = "ibm" ]; then
    wget $url/ibm.tgz
    tar xf ibm.tgz
elif [ "$2" = "cphy" ]; then
    wget $url/cphy.tgz
    tar xf cphy.tgz
else
    echo "unknown workload $2"
    exit
fi