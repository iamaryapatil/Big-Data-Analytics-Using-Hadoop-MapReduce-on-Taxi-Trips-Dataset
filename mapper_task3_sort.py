#!/usr/bin/env python3

# Mapper for Task 3 - Job 1 (Join Operation)
# reads company_id \t trip_count (from Job 2 reducer)
# outputs bucket_id \t padded_count \t company_id \t company_id \t trip_count


import sys

# function to decide which bucket a trip_count belongs to
def bucket_of(c: int) -> int:
    if c < 100:
        return 0
    elif c < 1000:
        return 1
    else:
        return 2

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 2:
        continue

    comp, cnt_s = parts
    try:
        cnt = int(cnt_s)
    except:
        continue

    b = bucket_of(cnt)
    # zero-padding for lexical numeric sort inside each bucket
    pad = f"{cnt:010d}"
    # key has 3 fields; value carries the original pair
    print(f"{b}\t{pad}\t{comp}\t{comp}\t{cnt}")
