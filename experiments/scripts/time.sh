#!/bin/bash

st=$(date +%s)

python fig1.py

et=$(date +%s)
duration=$(($et - $st))
echo "Execution time: $duration seconds"