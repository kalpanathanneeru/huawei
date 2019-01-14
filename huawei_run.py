#!/usr/bin/python

import os
from huawei_lib import Naming, ZipkinParser, bucket_by_url_txtype, ldfi_solve, get_label_2
import argparse
global map_dir
trace_dir = 'traces'
file_path = "/root/ldfiSuggestionFile.log"

global Run_once_has_been_run

Run_once_has_been_run = False

ap = argparse.ArgumentParser()

ap.add_argument("-t", "--tracedir", required=True,
        help="Path for the trace file")
ap.add_argument("-m", "--mapdir", required=False,
        help="Path for the map file")

args = ap.parse_args()
trace_dir = args.tracedir

map_dir = 'maps'
if args.mapdir:
    map_dir = args.mapdir

buckets = {}

def gl(data):
	return naming.canonical_name(get_label_2(data))

naming = Naming()
for file in os.listdir(map_dir):
    naming.process_file("/".join([map_dir, file]))


for file in os.listdir(trace_dir):
    parser = ZipkinParser("/".join([trace_dir, file]))
    #print file + " : " + str(len(parser.traces())) + " traces"
    bins = bucket_by_url_txtype(parser)
    for key in bins.keys():
        #print "val is type " + str(type(bins[key])) + " data " + str(bins[key])
        if key in buckets:
            buckets[key] = buckets[key] + bins[key]
        else:
            buckets[key] = bins[key]

if os.path.exists(file_path):
    os.remove(file_path)

def writeIntoFile(key, length,data):
    #os.remove(file_path)
    textList = [str(key), str(length), str(data)]
    ensure_dir(file_path)
    file = open(file_path,"a")
    print>>file, textList
    file.close()

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

for key in buckets.keys():
    soln = ldfi_solve(buckets[key], gl)
    val = map(lambda x: naming.latest_name(str(x).replace("_", "-")), soln)
    writeIntoFile(key, len(buckets[key]), val)
    if Run_once_has_been_run == False: 
	print file_path
        Run_once_has_been_run = True
