#!/usr/bin/python

import os
from huawei_lib import Naming, ZipkinParser, bucket_by_url_txtype, ldfi_solve, get_label_2

map_dir = 'maps'
trace_dir = 'traces'

buckets = {}


def gl(data):
	return naming.canonical_name(get_label_2(data))

naming = Naming()
for file in os.listdir(map_dir):
    naming.process_file("/".join([map_dir, file]))


for file in os.listdir(trace_dir):
    parser = ZipkinParser("/".join([trace_dir, file]))
    print file + " : " + str(len(parser.traces())) + " traces"
    bins = bucket_by_url_txtype(parser)
    for key in bins.keys():
        #print "val is type " + str(type(bins[key])) + " data " + str(bins[key])
        if key in buckets:
            buckets[key] = buckets[key] + bins[key]
        else:
            buckets[key] = bins[key]



for key in buckets.keys():
    soln = ldfi_solve(buckets[key], gl)
    val = map(lambda x: naming.latest_name(str(x).replace("_", "-")), soln)
    print [key, len(buckets[key]), val]

    

