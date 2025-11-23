#!/bin/bash

# This is the script for Task-2 Assignment 1- Big Data Processing

IN=/Input/Trips.txt
OUT=/Output/Task2

# reading the number of iterations (v) from the first line of initialization.txt
# and extracting the initial medoids (coordinates) from the remaining lines
v=$(head -n1 initialization.txt)
tail -n +2 initialization.txt > medoids.txt

# removing previous output if it exists
hadoop fs -rm -r -f "$OUT" 
hadoop fs -rm -r -f /tmp/task2_assign* /tmp/task2_update* 

i=1
while :
do
  # Job A: Assign (map-only): assigning each trip point to its nearest medoid
  hadoop fs -rm -r -f /tmp/task2_assign$i >/dev/null 2>&1 || true
  hadoop jar ./hadoop-streaming-3.1.4.jar \
    -D mapred.reduce.tasks=0 \
    -file medoids.txt \
    -file mapper_task2.py \
    -mapper ./mapper_task2.py \
    -input "$IN" \
    -output /tmp/task2_assign$i

  # Job B: Update: recomputing medoids for each cluster
  hadoop fs -rm -r -f /tmp/task2_update$i 
  hadoop jar ./hadoop-streaming-3.1.4.jar \
    -D mapred.reduce.tasks=3 \
    -file mapper_task2_passer.py \
    -file reducer_task2.py \
    -mapper ./mapper_task2_passer.py \
    -reducer ./reducer_task2.py \
    -input /tmp/task2_assign$i \
    -output /tmp/task2_update$i

  # printing medoids of this iteration
  echo "Iteration $i:"
  hadoop fs -cat /tmp/task2_update$i/part-* | sort -n -k1,1

  # preparing medoids for next iteration: keeping only x y coordinates
  hadoop fs -cat /tmp/task2_update$i/part-* | sort -n -k1,1 | awk -F'\t' '{print $2" "$3}' > medoids_next.txt

  # convergence check to stop if medoids do not change between iterations
  if cmp -s medoids.txt medoids_next.txt ; then
    cp medoids_next.txt medoids.txt
    break
  fi
  mv -f medoids_next.txt medoids.txt

  # stopping if max iterations have reached
  if [ "$i" -ge "$v" ]; then
    break
  fi
  i=$((i+1))
done

# writing k lines "x y" to /Output/Task2
awk '{print $1 "\t" $2}' medoids.txt > Task2_output.txt
hadoop fs -mkdir -p "$OUT"
hadoop fs -put -f Task2_output.txt "$OUT/part-00000"
echo "Final medoids written to $OUT"
