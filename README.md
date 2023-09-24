# Ditto: An Elastic and Adaptive Memory-Disaggregated Caching System
This is the implementation repository of *Ditto: An Adaptive Memory-Disaggregated Caching System*. This artifact provides the source code of Ditto and scripts to reproduce experiment results in our paper.

- [Ditto: An Elastic and Adaptive Memory-Disaggregated Caching System](#ditto-an-elastic-and-adaptive-memory-disaggregated-caching-system)
  - [Supported Platforms](#supported-platforms)
  - [Create a CloudLab Cluster](#create-a-cloudlab-cluster)
  - [Source Code of Ditto](#source-code-of-ditto)
  - [Setup Environment](#setup-environment)
  - [Workloads](#workloads)
  - [Get Started](#get-started)
  - [Reproduce Experiment Results](#reproduce-experiment-results)
  - [Supplementary Results](#supplementary-results)

## Supported Platforms
- Hardware Requirements: Mellanox ConnectX-6 NIC
- Software Requirements: Ubuntu 18.04, Mellanox OFED 4.9-5.1.0.0, libmemcached-dev, cmake 3.16.8, libgtest-dev, memcached, python-memcached, redis, libhiredis, redis++

We strongly recommend you execute Ditto using r650 nodes of [CloudLab](https://cloudlab.us) with Ubuntu 18.04, since the code has been thoroughly tested there. We haven't done any tests on any other hardware environment. 

## Create a CloudLab Cluster
1. Log into your CloudLab account. You will need to apply for an account if you don't have it.
2. After logging into the CloudLab console, create an experiment profile with 10 r650 nodes with the following steps:
   1. Click `Experiments |--> Create Experiment Profile`. 
   2. Click `Upload` to upload the provided [profile](scripts/cloudlab-profile.xml). 
   3. Name your profile and click `Create`.
3. Click `Instantiate` to create a 10-node cluster with the profile. If there are not 10 r650 nodes available, please submit a reservation request via `Experiments |--> Reserve Nodes` and wait for approval. Feel free to contact us if you have trouble reserving nodes.
4. Try to log into each node and check each node using SSH commands provided in the `List View` on CloudLab. If you find some nodes have broken shells (which sometimes happens in CloudLab), you can reload them via `List View |--> Reload Selected`

## Source Code of Ditto
After logging into all 10 r650 CloudLab nodes, use the following command to clone this GitHub repository in the home directory of **all** nodes:

```
git clone https://github.com/dmemsys/Ditto.git
```

## Setup Environment
Our experiments use 10 r650 nodes. We use node-0 of the cluster as a coordinator to start processes on other nodes and collect experiment results, and we use node-1 as our memory node. Different types of nodes need different setup steps. We mark the nodes that the following steps should be executed on at the beginning of each step.
1. **All nodes**: Install required libraries and software on **all nodes**.  
    We provide you with a shell script to install all required libraries and software.  
    ```shell
    cd Ditto/scripts
    source ./setup-env.sh  # Takes about 15 minutes. Execute this command in tmux to prevent network interruption.
    ```
    ***Warning: This script is written for r650 nodes. It repartitions the disk partition of the node to expand its file system. You will need to disable line 121 to line 141 to avoid partitioning your disk.***
2. **All nodes**: Expand the file system on **all nodes**.  
    We have re-partitioned the disk in the previous step to have more space for our workloads. We need to reboot all machines to make the modified partition take effect.
    ```shell
    sudo reboot
    ```  
    After the nodes reboot, execute the following command:
    ```shell
    sudo resize2fs /dev/sda1
    ```
3. **Node-0**: Setup Memcached on **node-0**.  
    We use Memcached on node-0 to coordinate compute and memory nodes. Modify the following settings of `/etc/memcached.conf` on **node-0** in the cluster:
    ```shell
    -l <IP address of node-0> # In the form of 10.10.1.X
    ```  
    Add the following two configurations to the file:
    ```shell
    -I 128m
    -m 2048
    ```
    Execute the following command after modifying the configuration file:
    ```
    sudo service memcached restart
    ```
4. **All nodes**: Setup memory node IP and RNIC to conduct experiments on **all nodes**.
    ```shell
    cd Ditto/experiments
    python modify_config.py memory_ip_list=\[\"<node-1 IP>\"\]
    python modify_config.py ib_dev_id=3
    python modify_config.py conn_type=\"ROCE\"
    python modify_config.py ib_gid_idx=3
    ```
5. **All nodes**: Setup Memcached IP on **all nodes**.  
    Set `memcached_ip=<node-0 IP>` in `Ditto/experiments/scripts/shell_settings.sh`
6. **Node-1**: Setup hugepages on **node-1**.  
    Execute the following commands on **node-1**:
    ```shell
    sudo su
    echo 10240 > /proc/sys/vm/nr_hugepages
    exit
    ```
    ***Note: The hugepage setting losses every time we reboot the machine, so we need to re-execute this step every time we reboot the machines.***
7. **Your Laptop/PC**: Setup SSH keys on all nodes from **your own Laptop/PC**.  
    By default, nodes on CloudLab cannot SSH each other. We need to add the public key of node-0 to all other nodes to enable node-0 to access other nodes with SSH. We provide a [python script](scripts/setkey.py) to achieve this. *The following steps should be executed on your own machine that has the added public key on CloudLab.*
    - Modify cluster user: Modify the `user` variable in `Ditto/scripts/setkey.py` to your username of the CloudLab cluster.
    - Modify cluster setting: Modify the `server_list` in `Ditto/scripts/setkey.py` according to the `SSH Commands` on the `List View` of the CloudLab experiment.
    - Execute `python setkey.py`. (You will need to approve record keys to your local machine when executing the python script.)  
    ***Note: The authorized SSH keys losses every time we reboot the machine, so we need to re-execute this step every time we reboot the machines.***

## Workloads
Use the following commands to download workloads for all machines:
1. **Node-0**: Download workload from Internet:
    ```shell
    cd Ditto/experiments/workloads
    ./download_all.sh
    ```
2. **Node-0**: Start an HTTP server:
    ```shell
    cd Ditto/experiments/workloads
    python -m http.server
    ```
3. **Nodes-1~9**: Download workload from node-0:
    ```shell
    cd Ditto/experiments/workloads
    ./download_all_from_peer.sh node-0:8000
    ```
4. **Node-0**: Stop the HTTP server after other nodes have downloaded the dataset.

## Get Started
1.  **Node-1**: Set hugepage:
    ```
    sudo su
    echo 10240 > /proc/sys/vm/nr_hugepages
    exit
    ```
2. **Your Laptop/PC**: Set SSH keys on all nodes:
     - Modify the `user` variable in `Ditto/scripts/setkey.py` to your user name of CloudLab. 
     - Modify the `server_list` in `Ditto/scripts/setkeys.py` according to the `SSH Commands` on the `List View` of the CloudLab experiment.
     - Execute `python setkeys.py` on **your own Laptop/PC** to finish the setting. (You will need to approve record keys to your local machine when executing the python script.)
3. **Node-0**: Go to the root directory of Ditto (`~/Ditto`) and execute the following commands:
    ```shell
    cd experiments/scripts
    python kick-the-tires.py 256 ycsbc  # The command executes about 70 seconds.
    ```
    The kick-the-tires script is used as `python kick-the-tires.py <num_clients> <ycsb-workload>`. It automatically compiles Ditto on all nodes and executes the corresponding YCSB workload with the assigned number of client threads. The throughput and latency will be printed on the command line. You can change the command to execute Ditto with different numbers of clients and with other YCSB workloads (ycsba/ycsbb/ycsbc/ycsbd).

## Reproduce Experiment Results
We provide code and scripts in `Ditto/experiments/scripts` to reproduce our experiments. Please refer to [experiments/scripts/README.md](./experiments/scripts/README.md) for more details.

## Supplementary Results
- The absolute hit rates in Figures 18, 20, and 21 are supplemented in [experiments/results](./experiments/results).
- The supplementary results of the proposed techniques under different workloads are shown in [experiments/scripts/more_results/](./experiments/scripts/more_results/).