#!/usr/bin/env python3

# Mapper for Task 2: Taxi Trip Analysis
# reads /Input/Trips.txt
# Trip#,Taxi#,fare,distance,pickup_x,pickup_y,dropoff_x,dropoff_y
# emits TAB-separated cluster_id, x, y


import sys
import math

# function to read medoids from file
def load_medoids(path="medoids.txt"):
    meds = []
    try:
        with open(path, "r") as f:
            for raw in f:
                s = raw.strip()
                if not s:
                    continue 
                parts = s.split()
                if len(parts) < 2: # for x and y only
                    continue
                try:
                    x = float(parts[0]) # x
                    y = float(parts[1]) # y
                    meds.append((x, y)) # adding to medoids list
                except:
                    continue
    except FileNotFoundError:
        pass
    return meds

# function to find nearest medoid index for a point (x, y)
def nearest_medoid_idx(x, y, meds):
    best_i = -1                         # closest medoid index
    best_d2 = None
    for i, (mx, my) in enumerate(meds):
        dx = x - mx
        dy = y - my
        d2 = dx*dx + dy*dy              # euclidean distance
        # condition to update the best medoid
        if (best_d2 is None) or (d2 < best_d2) or (d2 == best_d2 and i < best_i):
            best_d2 = d2
            best_i = i
    return best_i

# main function
def main():
    medoids = load_medoids() # loading current medoids
    if not medoids:         
        sys.exit(0)

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue

        # input format Trip#,Taxi#,fare,distance,pickup_x,pickup_y,dropoff_x,dropoff_y
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 8:
            continue
        try:
            dx = float(parts[6]) # dropoff_x
            dy = float(parts[7]) # dropoff_y
        except:
            continue
        
        # finding which medoid the point belongs ro
        cid = nearest_medoid_idx(dx, dy, medoids)
        if cid >= 0:
            # emitting cluster_id, x, y
            print(f"{cid}\t{dx}\t{dy}")

if __name__ == "__main__":
    main()
