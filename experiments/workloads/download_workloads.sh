#!/bin/bash

fid=""

if [ "$1" = "twitter" ]; then
    if [ ! -d "twitter" ]; then
        mkdir twitter
    fi
    wget https://appsrv.cse.cuhk.edu.hk/~jcshen/ditto_workloads/twitter-exp.tgz --no-check-certificate
    mv twitter-exp.tgz twitter && cd twitter && tar xf twitter-exp.tgz && mv twitter-exp/* . && rm -rf twitter-exp && cd ..
elif [ "$1" = "ycsb" ]; then
    fid="1E-hyuwWGugDidX12ZgkRBNzAvLEeoNjv"
    python download_gdrive.py $fid "$1.tgz"
    if [ ! -d ycsb ]; then
        mkdir ycsb
    fi
    mv $1.tgz ycsb
    cd ycsb && tar xf $1.tgz && cd ..
elif [ "$1" = "changing" ]; then
    fid="1bVtcZM36J-xumOFESDVl9-dOunP60Bt0"
    python download_gdrive.py $fid "$1.trc"
    if [ ! -d changing ]; then
        mkdir changing
    fi
    mv $1.trc changing
    # download changing-small
    fid="1Px08LSg-xqD8w0t8zmQVtvlng3yBilvz"
    python download_gdrive.py $fid "changing-small.trc"
    mv changing-small.trc changing
elif [ "$1" = "webmail" ]; then
    fid="1PhghC8gJBFStij5s0VD_X7SLIOIfpVHV"
    python download_gdrive.py $fid "webmail-d16.fiu"
    if [ ! -d fiu ]; then
        mkdir fiu
    fi
    mv webmail-d16.fiu fiu
    # download webmail-all
    fid="1t1lsBO0p2lSQSxPam4Rh7tTYFPdkT7cx"
    python download_gdrive.py $fid "fiu/webmail-all.tgz"
    cd fiu && tar xf webmail-all.tgz && cd ..
    # download webmail-11
    fid="18Js1JpXS5JUQP2CL8kOPQkbXoDCwV-RA"
    python download_gdrive.py $fid "fiu/webmail-11.tgz"
    cd fiu && tar xf webmail-11.tgz && cd ..
elif [ "$1" = "mix" ]; then
    fid="1UB9Y8VyVP8_Y3_gJypP9WZ1BiPSz0tIf"
    python download_gdrive.py $fid "$1.tgz"
    tar xf $1.tgz
    mv para_mix mix
    wget https://appsrv.cse.cuhk.edu.hk/~jcshen/ditto_workloads/lfu-acc-trace-nn.trc --no-check-certificate
    wget https://appsrv.cse.cuhk.edu.hk/~jcshen/ditto_workloads/lru-acc-trace-nn.trc --no-check-certificate
    mv lfu-acc-trace-nn.trc mix
    mv lru-acc-trace-nn.trc mix
    wget https://appsrv.cse.cuhk.edu.hk/~jcshen/ditto_workloads/lfu-acc-trace-nnn.trc --no-check-certificate
    wget https://appsrv.cse.cuhk.edu.hk/~jcshen/ditto_workloads/lru-acc-trace-nnn.trc --no-check-certificate
    mv lfu-acc-trace-nnn.trc mix
    mv lru-acc-trace-nnn.trc mix
elif [ "$1" = "ibm" ]; then
    wget https://appsrv.cse.cuhk.edu.hk/~jcshen/ditto_workloads/ibm.tgz --no-check-certificate
    tar xf $1.tgz
elif [ "$1" = "cphy" ]; then
    wget https://appsrv.cse.cuhk.edu.hk/~jcshen/ditto_workloads/cphy.tgz --no-check-certificate
    tar xf $1.tgz
else
    echo "unkonwn workload $1"
    exit
fi