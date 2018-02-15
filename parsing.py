#!/usr/bin/python

import csv, sys, json, re
import os 
from graphviz import Digraph


traceid_fld = 1
spanid_fld = 3
all_annotations_fld = 4
parentid_fld = 8


def get_label_old(self, data):
    svcarr = data[9].split(":")[1].split("-")
    svcarr.pop()
    return "-".join(svcarr)


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
    def __init__(self, id, get_label=get_label_old):
        self.id = id
        self.spans = {}
        self.root = 0
        self.get_label = get_label

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

    #def services(self, lmb):
    def services(self):
        svcs = set()
        for span in self.spans.values():
            #svc = self.get_servicename(span.data)
            # more permissive notion of services
            svc = self.get_label(span.data)
            if svc is not None:
                svcs.add(svc)
        return svcs

    def get_servicename(self, data):
        p = re.compile("service_name: '(\S+)',")
        m = p.search(data[5])
        if m is not None:
            return m.group(1)
        else:
            return None

    def get_servicetype(self, data):
        p = re.compile("serviceType;(\S+)")
        m = p.search(data[4])
        if m is not None:
            return m.group(1)
        else:
            return None

    def get_label_old2(self, data):
        m = self.get_servicename(data)
        if m is not None:
            return m
        else:
            m = self.get_servicetype(data)
            if m is not None:
                return m
            else:
                return "?"

    def to_dot(self, label=self.get_label):
        g = Digraph(comment="Callgraph", format = "pdf")
        for n in self.spans.values():
            notes = n.data[4]
            g.node(str(n.id), label(n.data))

        for n in self.spans.values():
            g.edge(str(n.parent), str(n.id))
            
        return g


class ZipkinParser(object):
    def __init__(self, file, get_label):
        self.big_dict = {}
        with open(file, 'r') as csvfile:
            for trace_entry in csv.reader(csvfile):
                traceid = trace_entry[traceid_fld]
                spanid = trace_entry[spanid_fld]
                parentid = trace_entry[parentid_fld]
                if not traceid in self.big_dict:
                    self.big_dict[traceid] = Trace(traceid, get_label)

                trace = self.big_dict[traceid]
                span = Span(spanid, parentid, trace_entry)
                trace.new_span(span)


    def traces(self):
        # probably want an iterator here, but not sure how to do that pythonically.
        return self.big_dict.values()                   
    
