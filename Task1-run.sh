#!/bin/bash

# This is the script for Task-1 Assignment 1- Big Data Processing

# removing previous output if it exists
hadoop fs -rm -r -f /Output/Task1

# running the Hadoop Streaming job
hadoop jar ./hadoop-streaming-3.1.4.jar \
  -D stream.num.map.output.key.fields=2 \
  -D mapred.reduce.tasks=3 \
  -file ./mapper_task1.py \
  -mapper ./mapper_task1.py \
  -file ./reducer_task1.py \
  -reducer ./reducer_task1.py \
  -input /Input/Trips.txt \
  -output /Output/Task1

# viewing the output
hadoop fs -cat /Output/Task1/part-00000
hadoop fs -cat /Output/Task1/part-00001
hadoop fs -cat /Output/Task1/part-00002
