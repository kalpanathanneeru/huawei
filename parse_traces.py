#!/usr/bin/python

import csv, sys, json, re
import os 
from graphviz import Digraph


traceid_fld = 1
spanid_fld = 3
all_annotations_fld = 4
parentid_fld = 8


class Span(object):
    def __init__(self, id, parent, data):
        self.id = id
        if parent == "":
            self.parent = 0
        else:
            try:
                self.parent = parent
            except Exception:
                print "BAD value for parentid: " + parent
                self.parent = -1
        self.data = data


    def get_annotation_fields(self):
        annotations = self.data[all_annotations_fld]
        fields = re.split(",(?! )", annotations)
        return fields

    def get_txtype(self, fields):
        tx_type = fields[3].split(";")[1]
        return tx_type 

    def get_url(self, fields):
        url = ""
        for f in fields:
            if(re.match("^http\.url.*", f)):
                url = f.split(";")[1]
                break
        return url    

class Trace(object):
    def __init__(self, id):
        self.id = id
        self.spans = {}
        self.root = 0

    def new_span(self, span):
        if span.parent == 0:
            self.root = span
        self.spans[span.id] = span

    def span_cnt(self):
        return len(self.spans.keys())

    def sanity(self):
        has_root = False
        for spanid in self.spans:
            if self.spans[spanid].parent != 0:
                parent = self.spans[spanid].parent
                if parent not in self.spans.keys():
                    #commenting out so as to not clutter output for now
                    #print "orphaned id *" + str(parent) + "*"
                    #print "with other ids " + str(self.spans.keys())
                    return False
            else:
                has_root = True

        return has_root

    def get_root(self):
        return self.root 

    def root_annotations(self):
        return self.root.data

    def get_servicename(self, data):
        p = re.compile("service_name: '(\S+)',")
        m = p.search(data[5])
        if m is not None:
            return m.group(1)
        else:
            p = re.compile("serviceType;(\S+)")
            m = p.search(data[4])
            if m is not None:
                return m.group(1)     
            else:
                return "?"

        

    def to_dot(self):
        g = Digraph(comment="Callgraph", format = "pdf")
        for n in self.spans.values():
            notes = n.data[4]
            g.node(str(n.id), self.get_servicename(n.data))

        for n in self.spans.values():
            g.edge(str(n.parent), str(n.id))
            
        return g


class ZipkinParser(object):
    def __init__(self, file):
        self.big_dict = {}
        with open(file, 'r') as csvfile:
            for trace_entry in csv.reader(csvfile):
                traceid = trace_entry[traceid_fld]
                spanid = trace_entry[spanid_fld]
                parentid = trace_entry[parentid_fld]
                if not traceid in self.big_dict:
                    self.big_dict[traceid] = Trace(traceid)

                trace = self.big_dict[traceid]
                span = Span(spanid, parentid, trace_entry)
                trace.new_span(span)


    def traces(self):
        # probably want an iterator here, but not sure how to do that pythonically.
        return self.big_dict.values()                   
    


def bucket_by_url(parser):
    buckets = dict()
    for trace in parser.traces():
        root = trace.get_root() 
        
        if(root != 0):
            annotation_fields = root.get_annotation_fields()
            url = root.get_url(annotation_fields)
            if(not url in buckets):
                buckets[url] = list()
            buckets[url].append(trace)


    current_dir = os.getcwd()
    print "current dir: " + current_dir 
    for url, traces in buckets.items():
        service = url.split("//")[1]
        service = service.replace("/", "_")
        outdir = os.path.join(current_dir,  "out/" + service)
        
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        
        os.chdir(outdir)
        for trace in traces:
            dot = trace.to_dot()
            dot.render(trace.id)
        
        os.chdir("../..")


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
            #print "Bucket key: " + bucket_key 

            if(not bucket_key in buckets):
                buckets[bucket_key] = list()
            buckets[bucket_key].append(trace)  

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
    traces = ["{high: 0, low: 474771886}",
              "{high: 0, low: -869845681}"]

    for trace in traces:
        trace_data = parser.big_dict[trace]
        root_span = trace_data.get_root()
        
        fields = root_span.get_annotation_fields()
        for f in fields:
            print f
        print "\n"
    

trace_file = get_input_file()
parser = ZipkinParser(trace_file)
 

print("Number of traces: " + str(len(parser.traces())))
#bucket_by_url(parser)
#analyze_traces(parser)
bucket_by_url_txtype(parser)     


'''
goods = 0
rejects = 0

for trace in parser.traces():
    if trace.sanity():
        goods += 1
        print "ROOT ANNOTATION: " + str(trace.root_annotations())
        dot = trace.to_dot()
        dot.render(trace.id)
    else:
        rejects += 1
'''



