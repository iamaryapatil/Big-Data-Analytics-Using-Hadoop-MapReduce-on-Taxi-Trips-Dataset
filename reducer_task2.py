#!/usr/bin/env python3

# Reducer for Task 2: Taxi Trip Analysis
# reads TAB-separated cluster_id, x, y", grouped by cluster_id.
# outputs TAB-separated  cluster_id, medoid_x, medoid_y

import sys

# function to calculate euclidean distance
def euclid(a, b):
    dx = a[0] - b[0]
    dy = a[1] - b[1]
    return dx*dx + dy*dy

# function to emit the best medoid for a cluster
def emit_medoid(cluster_id, pts):
    if not pts:
        return

    best_idx = 0
    best_cost = None
    n = len(pts)

    # for each candidate point in cluster
    for i in range(n):
        pi = pts[i]
        s = 0.0
        # summing distances from candidate to all points
        for j in range(n):
            s += euclid(pi, pts[j])
        # updating best medoid if current candidate is better
        if best_cost is None or s < best_cost:
            best_cost = s
            best_idx = i
        # keeping the first one if equal

    mx, my = pts[best_idx]
    print(f"{cluster_id}\t{mx}\t{my}")

# main function
def main():
    cur_c = None # current cluster
    bucket = []  # gathering all points for current cluster

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        f = line.split("\t")
        if len(f) < 3:
            continue
        cid = f[0] # cluster id
        try:
            x = float(f[1]); y = float(f[2])
        except:
            continue
        
        # processing previous cluster when cluster changes
        if cid != cur_c:
            if cur_c is not None:
                emit_medoid(cur_c, bucket)
            cur_c = cid
            bucket = []
        bucket.append((x, y)) # adding point to current cluster

    # emitting medoid for last cluster
    if cur_c is not None:
        emit_medoid(cur_c, bucket)

if __name__ == "__main__":
    main()
