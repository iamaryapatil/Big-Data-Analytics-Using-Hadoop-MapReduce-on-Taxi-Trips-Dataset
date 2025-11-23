#!/usr/bin/env python3

# Reducer for Task 1: Taxi Trip Analysis
# Takes mapper output grouped by two-field key (taxi#, trip_type)
# Prints exactly 6 TAB-separated fields:
# taxi# \t type \t total_trips \t max_fare \t min_fare \t avg_fare

import sys

current_taxi = None
current_type = None

# aggregation variables for current (taxi# + trip_type) group
total_count = 0
total_sum = 0.0
global_max = float("-inf")
global_min = float("inf")

# function to print results for each taxi group i.e (taxi# + trip_type)
def emit(taxi, ttype, cnt, s, gmax, gmin):
    if taxi is None:
        return
    # calculating average fare from total sum and total count
    avg = (s / cnt) if cnt else 0.0
    # exactly 6 fields; decimals to 2 places
    print(f"{taxi}\t{ttype}\t{cnt}\t{gmax:.2f}\t{gmin:.2f}\t{avg:.2f}")

# processing the input from mappers (grouped by taxi# + trip_type)
for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    fields = line.split("\t")
    if len(fields) != 6:
        continue

    # extracting key fields (taxi# + trip_type) + Error Handling
    taxi, ttype = fields[0], fields[1]
    try:
        cnt = int(fields[2])
        summ = float(fields[3])
        mx = float(fields[4])
        mn = float(fields[5])
    except:
        continue

    # checking if we're still processing the same (taxi# + trip_type) group
    if taxi == current_taxi and ttype == current_type:
        total_count += cnt
        total_sum += summ
        if mx > global_max:
            global_max = mx
        if mn < global_min:
            global_min = mn
    else:
        # for new group detected - emitting results for previous group
        emit(current_taxi, current_type, total_count, total_sum, global_max, global_min)
        current_taxi, current_type = taxi, ttype
        total_count = cnt
        total_sum = summ
        global_max = mx
        global_min = mn

# emitting results for the final group 
emit(current_taxi, current_type, total_count, total_sum, global_max, global_min) # parsing aggregated values from mapper
