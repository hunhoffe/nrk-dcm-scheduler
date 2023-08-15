#!/bin/bash

JAR_PATH="../target/scheduler-1.1.16-SNAPSHOT-jar-with-dependencies.jar"
MAIN_CLASS="com.vmware.bespin.simulation.SimulatorRunner"

ITERS_PER_TEST=20
BATCH_SIZES=(1)

# The lengths of all the config arrays need to be the same
MACHINE_CONFIGS=(4 8 16 32)
CORE_CONFIGS=(16 32 64 128)
MEMSLICE_CONFIGS=(128 256 512 1024)
# The number of different rack test configurations to run
NUM_CONFIGS=${#MACHINE_CONFIGS[@]}

# % of cluster to fill
RACK_UTILS=(10 50 90)
RACK_FILL="poisson"

SCHEDULERS="DCMcap DCMloc"
# SCHEDULERS="R RR DCMcap DCMloc"

# Create output dir or bail
OUTPUT_DIR="results"
mkdir $OUTPUT_DIR || exit -1

# Iterate over schedulers
for sched in $SCHEDULERS
do
  # Iterate over rack configs
  for (( config_idx=0; config_idx<${NUM_CONFIGS}; config_idx++ ));
  do
    num_machines=${MACHINE_CONFIGS[$config_idx]}
    num_cores=${CORE_CONFIGS[$config_idx]}
    num_memslices=${MEMSLICE_CONFIGS[$config_idx]}
    num_processes=$(( 4*num_machines ))

    # Iterate over rack utilization
    for rack_util in "${RACK_UTILS[@]}"
    do
      # Iterate over different batch sizes
      for batch_size in "${BATCH_SIZES[@]}"
      do
        # Perform a specific numbers of iterations for this particular test config
        for (( iter=0; iter<$ITERS_PER_TEST; iter++ ))
        do
          output_file="$sched-$RACK_FILL-$batch_size-$num_machines-$num_cores-$num_memslices-$num_processes-$rack_util-$iter.log"
          cmd="java -cp $JAR_PATH $MAIN_CLASS -a $batch_size -c $num_cores -f $RACK_FILL -m $num_memslices -n $num_machines -p $num_processes -u $rack_util -s $sched"
          echo "Fill: $RACK_FILL, Batch: $batch_size, Scheduler: $sched, ClusterConfig: (${num_machines} ${num_cores} ${num_memslices}) Util: $rack_util Iter: $iter OutputFile: $output_file"
          echo $cmd
          echo $cmd > $OUTPUT_DIR/$output_file
          $cmd >> $OUTPUT_DIR/$output_file 2>&1
        done
      done
    done
  done
done
