#!/bin/bash

# This is the script for Task-3 Assignment 1 - Big Data Processing

IN_TRIPS=/Input/Trips.txt
IN_TAXIS=/Input/Taxis.txt
OUT_FINAL=/Output/Task3

# intermediates
J1_OUT=/tmp/task3_job1
J2_OUT=/tmp/task3_job2

# remove previous outputs if present (quietly ignore errors)
hadoop fs -rm -r -f "$OUT_FINAL" 
hadoop fs -rm -r -f "$J1_OUT"    
hadoop fs -rm -r -f "$J2_OUT"    

# Job 1: Join
hadoop jar ./hadoop-streaming-3.1.4.jar \
  -D mapred.reduce.tasks=3 \
  -file mapper_task3_join.py \
  -file reducer_task3_join.py \
  -mapper ./mapper_task3_join.py \
  -reducer ./reducer_task3_join.py \
  -input "$IN_TRIPS" \
  -input "$IN_TAXIS" \
  -output "$J1_OUT"

# Job 2: Count 
hadoop jar ./hadoop-streaming-3.1.4.jar \
  -D mapred.reduce.tasks=3 \
  -file mapper_task3_count.py \
  -file reducer_task3_count.py \
  -mapper ./mapper_task3_count.py \
  -reducer ./reducer_task3_count.py \
  -input "$J1_OUT" \
  -output "$J2_OUT"

# Job 3: Sort 
# Key layout emitted by mapper: bucket \t padded_count \t company
# We partition ONLY by key field 1 (bucket) so each reducer handles a range.
# Comparator sorts by bucket (numeric), then padded_count (numeric), then company.

hadoop jar ./hadoop-streaming-3.1.4.jar \
  -D mapred.reduce.tasks=3 \
  -D stream.map.output.field.separator=$' ' \
  -D stream.num.map.output.key.fields=3 \
  -D mapred.text.key.partitioner.options=-k1,1 \
  -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
  -D mapred.text.key.comparator.options="-k1,1n -k2,2 -k3,3" \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -file mapper_task3_sort.py \
  -file reducer_task3_sort.py \
  -mapper ./mapper_task3_sort.py \
  -reducer ./reducer_task3_sort.py \
  -input "$J2_OUT" \
  -output "$OUT_FINAL"
