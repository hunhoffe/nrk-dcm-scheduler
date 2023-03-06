#!/bin/bash

JAR_PATH="../target/scheduler-1.1.7-SNAPSHOT-jar-with-dependencies.jar"
MAIN_CLASS="com.vmware.bespin.scheduler.SimulationRunner"
JAVA_ARGS=""
# "-Dlog4j.configurationFile=../src/main/resources/log4j2.xml"

ITERS_PER_TEST=3
OUTPUT_DIR="batch_size_results_singlestep"

BATCH_MIN=1
BATCH_MAX=20

HOSTS=42
CORES_PER_HOST=64
MEMSLICES_PER_HOST=128

CLUSTER_UTIL=50
CLUSTER_FILL="singlestep"
NUM_PROCESSES=42

mkdir $OUTPUT_DIR || exit -1
for (( batch_size=$BATCH_MIN; batch_size<=$BATCH_MAX; batch_size++ ))
do
  for (( iter=0; iter<$ITERS_PER_TEST; iter++ ))
  do
    echo "Batch: $batch_size, Iter: $iter"
    output_file="$batch_size-$iter-$CORES_PER_HOST-$CLUSTER_FILL-$MEMSLICES_PER_HOST-$HOSTS-$NUM_PROCESSES-$CLUSTER_UTIL.log"
    cmd="java $JAVA_ARGS -cp $JAR_PATH $MAIN_CLASS -a $batch_size -c $CORES_PER_HOST -f $CLUSTER_FILL -m $MEMSLICES_PER_HOST -n $HOSTS -p $NUM_PROCESSES -u $CLUSTER_UTIL"
    echo $cmd > $OUTPUT_DIR/$output_file
    $cmd > $OUTPUT_DIR/$output_file 2>&1
  done
done
