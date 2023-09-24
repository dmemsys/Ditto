#!/bin/bash

function get_client_config() {
    ret_config_fname=""
    if [ "$1" = "sample-lru" ]; then
        echo "client sample-lru"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-lfu" ]; then
        echo "client sample-lfu"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LFU\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-gdsf" ]; then
        echo "client $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_GDSF\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-gds" ]; then
        echo "client $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_GDS\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-lirs" ]; then
        echo "client $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LIRS\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-lrfu" ]; then
        echo "client $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LRFU\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-fifo" ]; then
        echo "client $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_FIFO\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-lfuda" ]; then
        echo "client $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LFUDA\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-lruk" ]; then
        echo "client $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LRUK\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-size" ]; then
        echo "client $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_SIZE\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-mru" ]; then
        echo "client $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_MRU\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-hyperbolic" ]; then
        echo "client $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_HYPERBOLIC\"
        ret_config_fname="client_conf_sample.json"
    elif [ "$1" = "sample-adaptive" ]; then
        python ../modify_single_config.py sample_adaptive use_async_weight=true
        python ../modify_single_config.py sample_adaptive use_freq_cache=true
        echo "client sample-adaptive"
        ret_config_fname="client_conf_sample_adaptive.json"
    elif [ "$1" = "sample-adaptive-naive" ]; then
        echo "client sample-adaptive-naive"
        python ../modify_single_config.py sample_adaptive_naive use_async_weight=false
        python ../modify_single_config.py sample_adaptive_naive use_freq_cache=false
        ret_config_fname="client_conf_sample_adaptive_naive.json"
    elif [ "$1" = "sample-adaptive-heavy" ]; then
        echo "client sample-adaptive-heavy"
        python ../modify_single_config.py sample_adaptive_heavy use_async_weight=false
        python ../modify_single_config.py sample_adaptive_heavy use_freq_cache=false
        ret_config_fname="client_conf_sample_adaptive_heavy.json"
    elif [ "$1" = "sample-adaptive-sync" ]; then
        echo "client sample-adaptive-sync"
        python ../modify_single_config.py sample_adaptive use_async_weight=false
        ret_config_fname="client_conf_sample_adaptive.json"
    elif [ "$1" = "sample-adaptive-sync-nfc" ]; then
        echo "client sample-adaptive-sync-nfc"
        python ../modify_single_config.py sample_adaptive use_async_weight=false
        python ../modify_single_config.py sample_adaptive use_freq_cache=false
        ret_config_fname="client_conf_sample_adaptive.json"
    elif [ "$1" = "sample-adaptive-async-nfc" ]; then
        echo "client sample-adaptive-async-nfc"
        python ../modify_single_config.py sample_adaptive use_async_weight=true
        python ../modify_single_config.py sample_adaptive use_freq_cache=false
        ret_config_fname="client_conf_sample_adaptive.json"
    elif [ "$1" = "non" ]; then
        echo "client non"
        ret_config_fname="client_conf_non.json"
    elif [ "$1" = "cliquemap" ]; then
        echo "client cliquemap"
        ret_config_fname="client_conf_cliquemap.json"
    elif [ "$1" = "cliquemap-precise-lru" ]; then
        echo "client cliquemap-precise-lru"
        python ../modify_single_config.py cliquemap_precise eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="client_conf_cliquemap_precise.json"
    elif [ "$1" = "cliquemap-precise-lfu" ]; then
        echo "client cliquemap-precise-lfu"
        python ../modify_single_config.py cliquemap_precise eviction_priority=\"EVICT_PRIO_LFU\"
        ret_config_fname="client_conf_cliquemap_precise.json"
    elif [ "$1" = "cliquemap-shard-lru" ]; then
        echo "client cliquemap-shard-lru"
        python ../modify_single_config.py cliquemap_precise eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="client_conf_cliquemap_precise.json"
    elif [ "$1" = "cliquemap-shard-lfu" ]; then
        echo "client cliquemap-shard-lfu"
        python ../modify_single_config.py cliquemap_precise eviction_priority=\"EVICT_PRIO_LFU\"
        ret_config_fname="client_conf_cliquemap_precise.json"
    elif [ "$1" = "cliquemap-sample-lru" ]; then
        echo "client cliquemap-sample-lru"
        python ../modify_single_config.py cliquemap eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="client_conf_cliquemap.json"
    elif [ "$1" = "cliquemap-sample-lfu" ]; then
        echo "client cliquemap-sample-lfu"
        python ../modify_single_config.py cliquemap eviction_priority=\"EVICT_PRIO_LFU\"
        ret_config_fname="client_conf_cliquemap.json"
    elif [ "$1" = "precise-lru" ]; then
        echo "client precise-lru"
        python ../modify_single_config.py precise eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="client_conf_precise.json"
    elif [ "$1" = "shard-lru" ]; then
        echo "client shard-lru"
        python ../modify_single_config.py precise eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="client_conf_precise.json"
    elif [ "$1" = "precise-lfu" ]; then
        echo "client precise-lfu"
        python ../modify_single_config.py precise eviction_priority=\"EVICT_PRIO_LFU\"
        ret_config_fname="client_conf_precise.json"
    fi
}

function get_server_config() {
    ret_config_fname=""
    if [ "$1" = "sample-lru" ]; then
        echo "server sample-lru"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-lfu" ]; then
        echo "server sample-lfu"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LFU\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-gdsf" ]; then
        echo "server $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_GDSF\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-gds" ]; then
        echo "server $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_GDS\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-lirs" ]; then
        echo "server $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LIRS\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-lrfu" ]; then
        echo "server $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LRFU\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-fifo" ]; then
        echo "server $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_FIFO\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-lfuda" ]; then
        echo "server $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LFUDA\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-lruk" ]; then
        echo "server $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_LRUK\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-size" ]; then
        echo "server $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_SIZE\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-mru" ]; then
        echo "server $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_MRU\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-hyperbolic" ]; then
        echo "server $1"
        python ../modify_single_config.py sample eviction_priority=\"EVICT_PRIO_HYPERBOLIC\"
        ret_config_fname="server_conf_sample.json"
    elif [ "$1" = "sample-adaptive" ]; then
        echo "server sample-adaptive"
        python ../modify_single_config.py sample_adaptive use_async_weight=true
        python ../modify_single_config.py sample_adaptive use_freq_cache=true
        ret_config_fname="server_conf_sample_adaptive.json"
    elif [ "$1" = "sample-adaptive-naive" ]; then
        echo "server sample-adaptive-naive"
        ret_config_fname="server_conf_sample_adaptive_naive.json"
    elif [ "$1" = "sample-adaptive-heavy" ]; then
        echo "server sample-adaptive-heavy"
        ret_config_fname="server_conf_sample_adaptive_heavy.json"
    elif [ "$1" = "sample-adaptive-sync" ]; then
        echo "server sample-adaptive-sync"
        python ../modify_single_config.py sample_adaptive use_async_weight=false
        ret_config_fname="server_conf_sample_adaptive.json"
    elif [ "$1" = "sample-adaptive-sync-nfc" ]; then
        echo "server sample-adaptive-sync-nfc"
        python ../modify_single_config.py sample_adaptive use_async_weight=false
        python ../modify_single_config.py sample_adaptive use_freq_cache=false
        ret_config_fname="server_conf_sample_adaptive.json"
    elif [ "$1" = "sample-adaptive-async-nfc" ]; then
        echo "server sample-adaptive-nfc"
        python ../modify_single_config.py sample_adaptive use_async_weight=true
        python ../modify_single_config.py sample_adaptive use_freq_cache=false
        ret_config_fname="server_conf_sample_adaptive.json"
    elif [ "$1" = "cliquemap" ]; then
        echo "server cliquemap"
        ret_config_fname="server_conf_cliquemap.json"
    elif [ "$1" = "cliquemap-precise-lru" ]; then
        echo "server cliquemap-precise-lru"
        python ../modify_single_config.py cliquemap_precise eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="server_conf_cliquemap_precise.json"
    elif [ "$1" = "cliquemap-precise-lfu" ]; then
        echo "server cliquemap-precise-lfu"
        python ../modify_single_config.py cliquemap_precise eviction_priority=\"EVICT_PRIO_LFU\"
        ret_config_fname="server_conf_cliquemap_precise.json"
    elif [ "$1" = "cliquemap-shard-lru" ]; then
        echo "server cliquemap-shard-lru"
        python ../modify_single_config.py cliquemap_precise eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="server_conf_cliquemap_precise.json"
    elif [ "$1" = "cliquemap-shard-lfu" ]; then
        echo "server cliquemap-shard-lfu"
        python ../modify_single_config.py cliquemap_precise eviction_priority=\"EVICT_PRIO_LFU\"
        ret_config_fname="server_conf_cliquemap_precise.json"
    elif [ "$1" = "cliquemap-sample-lru" ]; then
        echo "server cliquemap-sample-lru"
        python ../modify_single_config.py cliquemap eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="server_conf_cliquemap.json"
    elif [ "$1" = "cliquemap-sample-lfu" ]; then
        echo "server cliquemap-sample-lfu"
        python ../modify_single_config.py cliquemap eviction_priority=\"EVICT_PRIO_LFU\"
        ret_config_fname="server_conf_cliquemap.json"
    elif [ "$1" = "non" ]; then
        echo "server non"
        ret_config_fname="server_conf_non.json"
    elif [ "$1" = "precise-lru" ]; then
        echo "server precise-lru"
        python ../modify_single_config.py precise eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="server_conf_precise.json"
    elif [ "$1" = "shard-lru" ]; then
        echo "server shard-lru"
        python ../modify_single_config.py precise eviction_priority=\"EVICT_PRIO_LRU\"
        ret_config_fname="server_conf_precise.json"
    elif [ "$1" = "precise-lfu" ]; then
        echo "server precise-lfu"
        python ../modify_single_config.py precise eviction_priority=\"EVICT_PRIO_LFU\"
        ret_config_fname="server_conf_precise.json"
    fi
}

function get_out_fname() {
    ret_out_fname=""
    if [ "$1" = "sample-lru" ]; then
        echo "controller sample-lru"
        ret_out_fname="sample-lru"
    elif [ "$1" = "sample-lfu" ]; then
        echo "controller sample-lfu"
        ret_out_fname="sample-lfu"
    elif [ "$1" = "sample-gdsf" ]; then
        echo "controller $1"
        ret_out_fname="$1"
    elif [ "$1" = "sample-gds" ]; then
        echo "controller $1"
        ret_out_fname="$1"
    elif [ "$1" = "sample-lirs" ]; then
        echo "controller $1"
        ret_out_fname="$1"
    elif [ "$1" = "sample-lrfu" ]; then
        echo "controller $1"
        ret_out_fname="$1"
    elif [ "$1" = "sample-fifo" ]; then
        echo "controller $1"
        ret_out_fname="$1"
    elif [ "$1" = "sample-lfuda" ]; then
        echo "controller $1"
        ret_out_fname="$1"
    elif [ "$1" = "sample-lruk" ]; then
        echo "controller $1"
        ret_out_fname="$1"
    elif [ "$1" = "sample-size" ]; then
        echo "controller $1"
        ret_out_fname="$1"
    elif [ "$1" = "sample-mru" ]; then
        echo "controller $1"
        ret_out_fname="$1"
    elif [ "$1" = "sample-hyperbolic" ]; then
        echo "controller $1"
        ret_out_fname="$1"
    elif [ "$1" = "sample-adaptive" ]; then
        echo "controller sample-adaptive"
        ret_out_fname="sample-adaptive"
    elif [ "$1" = "sample-adaptive-naive" ]; then
        echo "controller sample-adaptive-naive"
        ret_out_fname="sample-adaptive-naive"
    elif [ "$1" = "sample-adaptive-heavy" ]; then
        echo "controller sample-adaptive-heavy"
        ret_out_fname="sample-adaptive-heavy"
    elif [ "$1" = "sample-adaptive-sync" ]; then
        echo "controller sample-adaptive-sync"
        ret_out_fname="sample-adaptive-sync"
    elif [ "$1" = "sample-adaptive-sync-nfc" ]; then
        echo "controller sample-adaptive-sync-nfc"
        ret_out_fname="sample-adaptive-sync-nfc"
    elif [ "$1" = "sample-adaptive-async-nfc" ]; then
        echo "controller sample-adaptive-async-nfc"
        ret_out_fname="sample-adaptive-async-nfc"
    elif [ "$1" = "cliquemap" ]; then
        echo "controller cliquemap"
        ret_out_fname="cliquemap"
    elif [ "$1" = "cliquemap-precise-lru" ]; then
        echo "controller cliquemap-precise-lru"
        ret_out_fname="cliquemap-precise-lru"
    elif [ "$1" = "cliquemap-precise-lfu" ]; then
        echo "controller cliquemap-precise-lfu"
        ret_out_fname="cliquemap-precise-lfu"
    elif [ "$1" = "cliquemap-shard-lru" ]; then
        echo "controller cliquemap-shard-lru"
        ret_out_fname="cliquemap-shard-lru"
    elif [ "$1" = "cliquemap-shard-lfu" ]; then
        echo "controller cliquemap-shard-lfu"
        ret_out_fname="cliquemap-shard-lfu"
    elif [ "$1" = "cliquemap-sample-lru" ]; then
        echo "controller cliquemap-sample-lru"
        ret_out_fname="cliquemap-sample-lru"
    elif [ "$1" = "cliquemap-sample-lfu" ]; then
        echo "controller cliquemap-sample-lfu"
        ret_out_fname="cliquemap-sample-lfu"
    elif [ "$1" = "non" ]; then
        echo "controller non"
        ret_out_fname="non"
    elif [ "$1" = "precise-lru" ]; then
        echo "controller precise-lru"
        ret_out_fname="precise-lru"
    elif [ "$1" = "shard-lru" ]; then
        echo "controller shard-lru"
        ret_out_fname="shard-lru"
    elif [ "$1" = "precise-lfu" ]; then
        echo "controller precise-lfu"
        ret_out_fname="precise-lfu"
    fi
}