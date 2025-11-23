#!/usr/bin/env python3

# Mapper (pass-through) for Task 2 update step
# takes "cluster_id\tx\ty" as input from the mapper_task2.py output.
# echo mapper — just forwards lines to reducers.
# Outputs same as input; used to fan out data to 3 reducers.


import sys

for raw in sys.stdin:
    s = raw.strip()
    if s:
        print(s)
