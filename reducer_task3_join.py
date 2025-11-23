#!/usr/bin/env python3

# Reducer for Task 3 - Job 1 (Join Operation)
# Performs reducer-side join between Taxis.txt and Trips.txt
# Combines taxi-to-company mapping with trip records to produce company trip counts
# Input: taxi_id \t source_flag \t data (grouped by taxi_id from mapper)
# Output: company_id \t 1 (one record per trip, labeled by company)

import sys

cur_taxi_id = None
company_id = None
trip_count = 0

# function to print results for one taxi_id
def emit():
    if company_id is None or trip_count == 0:
        return
    # emitting one record per trip, each labeled with the company ID
    for _ in range(trip_count):
        print(f"{company_id}\t1")

# processing grouped input from mapper (sorted by taxi_id, then by source flag)
for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    f = line.split("\t")
    if len(f) < 3:
        continue

    taxi, tag = f[0], f[1]

    # if we see a new taxi_id, print the previous taxi's results
    if taxi != cur_taxi_id:
        if cur_taxi_id is not None:
            emit()                  # output counts for old taxi
        cur_taxi_id = taxi
        company = None              # reset for new taxi
        trip_count = 0

    # identifying records
    if tag == "X":
        company_id = f[2]
    elif tag == "T":
        trip_count += 1

# emitting results for the final taxi group
emit()
