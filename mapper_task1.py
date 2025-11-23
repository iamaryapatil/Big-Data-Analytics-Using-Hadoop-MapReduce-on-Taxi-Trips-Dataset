#!/usr/bin/env python3

# Mapper for Task 1: Taxi Trip Analysis
# reads /Input/Trips.txt
# trip#,Taxi#,fare,distance,pickup_x,pickup_y,dropoff_x,dropoff_y
# emits TAB-separated: taxi# \t trip_type \t count \t sum_fare \t max_fare \t min_fare

import sys

def trip_type_from_distance(d):
    if d >= 200.0:
        return "long"
    elif d >= 100.0:
        return "medium"
    else:
        return "short"

# (taxi, type) -> [count, sum, max, min]
# In-mapper combining: dictionary to accumulate statistics per (taxi, trip_type)
# Key: (taxi, type) -> Value: [count, sum_fare, max_fare, min_fare]
agg = {}

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    # # skip header if present
    # if line.lower().startswith("trip#"):
    #     continue

    parts = [p.strip() for p in line.split(",")]
    if len(parts) < 8:
        continue

    try:
        taxi = parts[1]
        fare = float(parts[2])
        dist = float(parts[3])
    except:
        continue
    # determining trip type based on distance
    ttype = trip_type_from_distance(dist)
    key = (taxi, ttype)

    if key not in agg:
        agg[key] = [0, 0.0, float("-inf"), float("inf")]
    rec = agg[key]
    rec[0] += 1
    rec[1] += fare
    if fare > rec[2]:
        rec[2] = fare
    if fare < rec[3]:
        rec[3] = fare

# emitting aggregated results for each unique (taxi, trip_type) combination
# output format: taxi# \t trip_type \t count \t sum_fare \t max_fare \t min_fare
for (taxi, ttype), (cnt, s, mx, mn) in agg.items():
    print(f"{taxi}\t{ttype}\t{cnt}\t{s}\t{mx}\t{mn}") # first two fields form the key
