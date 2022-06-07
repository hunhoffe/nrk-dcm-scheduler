#!/bin/bash

# Reference Command: 
# java -jar -Dlog4j.configurationFile=src/main/resources/log4j2.xml target/scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar -n 64 -c 128 -m 256 -p 20 -f -a 50 -s 1

NUM_NODES=64
CORES_PER_NODE=128
MEMSLICES_PER_NODE=256

NUM_APPLICATIONS=20
USE_CAP_FUNC="true"

ALLOCATIONS_PER_STEP=50
NUM_STEPS=10

FILE_NAME="results_${NUM_NODES}_${CORES_PER_NODE}_${MEMSLICES_PER_NODE}_${NUM_APPLICATIONS}_${USE_CAP_FUNC}_${ALLOCATIONS_PER_STEP}_${NUM_STEPS}.txt"

for ALLOCATIONS_PER_STEP in 1 2 4 8 16 32 64 128 256 512
do
    FILE_NAME="results_${NUM_NODES}_${CORES_PER_NODE}_${MEMSLICES_PER_NODE}_${NUM_APPLICATIONS}_${USE_CAP_FUNC}_${ALLOCATIONS_PER_STEP}_${NUM_STEPS}.txt"
    echo $FILE_NAME
    echo "java -jar -Dlog4j.configurationFile=src/main/resources/log4j2.xml target/scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar -n $NUM_NODES -c $CORES_PER_NODE -m $MEMSLICES_PER_NODE -p $NUM_APPLICATIONS -f $USE_CAP_FUNC -a $ALLOCATIONS_PER_STEP -s $NUM_STEPS > $FILE_NAME"
    java -jar -Dlog4j.configurationFile=src/main/resources/log4j2.xml target/scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar -n $NUM_NODES -c $CORES_PER_NODE -m $MEMSLICES_PER_NODE -p $NUM_APPLICATIONS -f $USE_CAP_FUNC -a $ALLOCATIONS_PER_STEP -s $NUM_STEPS > $FILE_NAME
done
