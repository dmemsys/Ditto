# Reproduce Experiment Results
In this folder, we provide code and scripts for reproducing figures in our paper. The name of each script corresponds to the number of each figure in the accepted version of our paper (on the artifact submission site). 

All these scripts should be executed on the coordinator node (node-0) of the cluster. Since some of the scripts take a long time to finish, we strongly recommend you execute these scripts in a *tmux session* to avoid script interruption due to network instability.

## Baselines
We compare Ditto with CliqueMap and Redis. We implement CliqueMap in the same codebase as Ditto due to the lack of existing open-source implementations. For the comparison with Redis, we use the default version installed by Ubuntu (7.0.12).

In our scripts, we use the following aliases:
- `sample-adaptive = Ditto`
- `sample-lru = Ditto-LRU`
- `sample-lfu = Ditto-LFU`

## Start to Run
***!!! Important: If you are using our provided CloudLab cluster to reproduce experiment results, please coordinate with each other to run experiments one person at a time. Concurrently running experiments cannot correctly reproduce the results in the paper due to hardware interference.***

***!!! Important: You will need to execute [steps 6 and 7](../../README.md#setup-environment) in the Setup Environment section of the top-level README every time you reboot the nodes before executing the experiments.***

Log into **node-0** of the cluster and execute following commands:
```shell
cd Ditto/experiments/scripts
# This take 16 hours to run. Let it run and just check the resutls in the next day.
sh ./run_all.sh
```

You can also run scripts one-by-one if you find some figures are skipped or show unexpected results during `run_all.sh` (due to network instability and randomness among compute nodes, which happens sometimes.)
```shell
cd Ditto/experiments/scripts
# Takes about 20 minutes
python fig1.py
# Takes about 45 minutes 
python fig2.py
# Takes about 41 minutes
python fig13.py
# Takes about 4.8 hours
python fig14.py
# Takes about 78 minutes
python fig15_16.py
# Takes about 93 minutes
python fig17.py
# Takes about 3.8 hours
python fig18.py
# Takes about 5 minutes
python fig19.py
# Takes about 10 minutes
python fig20.py
# Takes about 30 minutes
python fig21.py
# Takes about 12 minutes
python fig22.py
# Takes about 10 minutes
python fig23.py
# Takes about 11 minutes
python fig24.py
# Takes about 80 minutes
python fig25.py
```

**The JSON results can be found in `Ditto/experiments/scripts/results` and PDF figures can be found in `Ditto/experiments/scripts/figs`.**

**Note:** The results you get may not be exactly the same as the ones shown in the paper due to changes in physical machines. Some curves may fluctuate due to instability of RNICs in the cluster and hit rates may fluctuate due to the randomness when clients concurrently execute the workload. However, all results support the conclusions we made in the paper.

**Note:** Please save the results and figures to your own Laptop/PC after running all experiments in case other AECs execute the scripts and overwrite your results.