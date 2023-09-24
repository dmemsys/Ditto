#!/bin/bash

node=$1

./download_from_peer.sh $node twitter
./download_from_peer.sh $node ycsb
./download_from_peer.sh $node changing
./download_from_peer.sh $node webmail
./download_from_peer.sh $node mix
./download_from_peer.sh $node ibm
./download_from_peer.sh $node cphy
