#!/usr/bin/env python3

# Mapper for Task 3 - Job 2 (Counting Operation)
# This is a pass-through mapper that forwards joined data from Job 1 to the counting reducer
# Input format: company_id \t 1 (from Job 1 join output)
# Output format: company_id \t 1 (unchanged - passed directly to reducer for aggregation)

import sys

# Pass-through mapper: forwards each "company_id \t 1" record to reducer unchanged
# The actual counting/aggregation will be performed by the reducer
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

     # splitting input line into fields
    f = line.split("\t")
    if len(f) == 2:
        # Hadoop will group all records with the same company_id key for the reducer
        print(line)
