#!/usr/bin/env python3

# Reducer for Task 3 - Job 2 (Counting Operation)
# Aggregates trip counts for each company from the joined data
# Input: company_id \t 1 (grouped by company_id from mapper)
# Output: company_id \t total_trip_count

import sys

cur_company_id = None
trips = 0

# function to print results for current company
def emit():
    if cur_company_id is not None:
        print(f"{cur_company_id}\t{trips}")

# processing grouped input from mapper (sorted by company_id)
for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    try:
        company, one = line.split("\t")
    except ValueError:
        continue

    # checking if we are still processing the same company
    if company == cur_company_id:
        trips += 1
    else:
        # new company detected - emitting results for previous company
        emit()
        cur_company_id = company
        trips = 1
# emitting results for the final company group
emit()
