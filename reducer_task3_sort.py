#!/usr/bin/env python3

# Reducer for Task 2: Taxi Trip Analysis
# reads bucket_id \t padded_count \t company_id \t company_id \t trip_count
# output: company_id \t trip_count

import sys

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    f = line.split("\t")
    if len(f) != 5:
        continue

    # fields: [bucket, pad, key_company, out_company, out_count]
    _, _, _, comp, cnt_s = f
    print(f"{comp}\t{cnt_s}")
