#!/usr/bin/python

import csv, sys, json, re
import os 
from graphviz import Digraph
from parsing import *


traceid_fld = 1
spanid_fld = 3
all_annotations_fld = 4
parentid_fld = 8


def render_traces(buckets):
    current_dir = os.getcwd()
    for key, traces in buckets.items():
        outdir = os.path.join(current_dir, "out/" + key)

        if not os.path.exists(outdir):
            os.makedirs(outdir)

        os.chdir(outdir)
        for trace in traces:
            dot = trace.to_dot()
            dot.render(trace.id)

        os.chdir("../..")



def bucket_by_url(parser):
    buckets = dict()
    for trace in parser.traces():
        root = trace.get_root() 
        
        if(root != 0):
            annotation_fields = root.get_annotation_fields()
            url = root.get_url(annotation_fields)
            service = url.split("//")[1].replace("/", "_")
            if(not service in buckets):
                buckets[service] = list()
            buckets[service].append(trace)

    return buckets 
    #render_traces(buckets)




def bucket_by_url_txtype(parser):
    buckets = dict()
    for trace in parser.traces():
        root = trace.get_root()

        if(root != 0):
            annotation_fields = root.get_annotation_fields()
            url = root.get_url(annotation_fields)
            service = url.split("//")[1].replace("/", "_")

            tx_type = root.get_txtype(annotation_fields)
            tx_type = tx_type.replace("/", "_") 

            bucket_key = service + tx_type

            if(not bucket_key in buckets):
                buckets[bucket_key] = list()
            buckets[bucket_key].append(trace)  

    return buckets 
    #render_traces(buckets)


def get_input_file():
    if(len(sys.argv) < 2):
        print "usage: " + sys.argv[0] + " input_file.csv"
        sys.exit()

    trace_file = sys.argv[1]
    if(not os.path.isfile(trace_file)):
        print trace_file + " does not exist"
        sys.exit()
    
    return trace_file 


def analyze_traces(parser):
    traces = ["{high: 0, low: -206905875}",
              "{high: 0, low: -206906078}"]

    for trace in traces:
        trace_data = parser.big_dict[trace]
        root_span = trace_data.get_root()
        
        fields = root_span.get_annotation_fields()
        for f in fields:
            print f
        print "\n"
    

#trace_file = get_input_file()
#parser = ZipkinParser(trace_file)
#print("Number of traces: " + str(len(parser.traces())))
#bucket_by_url(parser)
#analyze_traces(parser)
#bucket_by_url_txtype(parser)     


