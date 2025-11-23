#!/usr/bin/env python3

# Mapper for Task 3 - Job 1 (Join Operation)
# Processes both Taxis.txt and Trips.txt to prepare data for join operation
# Creates taxi_id-keyed records with source indicators for reducer-side join
# Output format: taxi_id \t source_flag \t data

import sys

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue

    parts = [p.strip() for p in line.split(",")]
    n = len(parts)

    if n == 4:
        # Taxis.txt: Taxi#,company,model,year
        # emitting taxi record with 'X' flag to indicate Taxis.txt source
        taxi, company, _, _ = parts
        print(f"{taxi}\tX\t{company}")
    elif n == 8:
        # Trips.txt: Trip#,Taxi#,fare,distance,pickup_x,pickup_y,dropoff_x,dropoff_y
        # emitting trip record with 'T' flag to indicate Trips.txt source
        _, taxi, _, _, _, _, _, _ = parts
        print(f"{taxi}\tT\t1")
    
