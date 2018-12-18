#!/usr/bin/python

import csv, sys, json, re
import os, codecs
from graphviz import Digraph
import ldfi_py.pilp
from ldfi_py.pbool import *


def get_label_2(data):
    return data[u'span_name'].split(":")[1]


def get_label_old(data):
    svcarr = data[9].split(":")[1].split("-")
    svcarr.pop()
    return "-".join(svcarr)

def bucket_by_url_txtype(parser):
    buckets = dict()
    for trace in parser.traces():
        root = trace.get_root()
        if (root != 0):
            #annotation_fields = root.get_annotation_fields()
            annotation_fields = root.get_all_annotations()
            url = root.get_url(annotation_fields)
            service = url.split("//")[1].replace("/", "_")

            tx_type = root.get_txtype(annotation_fields)
            tx_type = tx_type.replace("/", "_")

            bucket_key = service + tx_type

            if(not bucket_key in buckets):
                buckets[bucket_key] = list()
            buckets[bucket_key].append(trace)

    return buckets

def get_formula(traces, get_label):
    outerset = set()
    for trace in traces:
        outerset.add(frozenset(trace.services(get_label)))

    conjuncts = None
    for inner in outerset:
        disjunction = None
        for item in inner:
            if disjunction is None:
                disjunction = Literal(item)
            else:
                disjunction = OrFormula(Literal(item), disjunction)
        if conjuncts is None:
            conjuncts = disjunction
        else:
            conjuncts = AndFormula(disjunction, conjuncts)
    return conjuncts


class Naming(object):
    def __init__(self):
        self.reversemap = {}
        self.fwdmap = {}
        #beginnings = {}
        #endings = {}

    def process_mapping(self, mapping):
        for item in mapping:
            print "got " + str(item)
            #for i in range(0, len(item[u'oldName'])-1):
            for i in range(0, len(item[u'oldName'])):
                old = item[u'oldName'][i]
                new = item[u'newName'][i]
                # two directions:  when I go fwd looking for the old name I should find the new one...
                print "map " + old + " to " + new
                self.fwdmap[old] = new
                self.reversemap[new] = old

    def process_file(self, file):
        fn = open(file, "r")
        raw = fn.read()
        fn.close()
        jsn = json.loads(raw)
        return self.process_mapping(jsn)

    def canonical_name(self, name):
        # return the canonical (earliest) name
        
        currname = name
        while currname in self.reversemap:
            currname = self.reversemap[currname]
    
        return currname

    def latest_name(self, canonical_name):
        # return the current instance name
        # we could perform this transitive closure once and memoize it,
        # but who cares?
   

        currname = canonical_name
        while currname in self.fwdmap:
            currname = self.fwdmap[currname]

        return currname

        
    


class Span(object):
    def __init__(self, id, parent, data):
        self.id = id
        #if parent == "":
        if parent is None:
            self.parent = 0
        else:
            try:
                self.parent = parent
            except Exception:
                # huh? under what circumstances would this exception ever be thrown?
                print "BAD value for parentid: " + parent
                self.parent = -1
        self.data = data
        self.children = []

    def get_annotation_fields(self):
        #annotations = self.data[all_annotations_fld]
        annotations = self.data[u'all_annotations']
        fields = re.split(",(?! )", annotations)
        return fields

    def get_all_annotations(self):
        binary_annotations = self.data[u'binary_annotations']
        all_annotations = ''
        for a in binary_annotations:
            all_annotations += (a['k']) + ';' + codecs.decode(a['v'][2:], 'hex').decode('utf-8') + ', '

        notes = all_annotations.rstrip(', ').encode('ascii','ignore')
        fields = re.split(",\s+", notes)
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

    def add_child(self, child_span):
        self.children.append(child_span)


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

    def add_children(self):
        for key in self.spans:
            span = self.spans[key]
            if span.parent in self.spans:
                if span.parent not in [0, -1]:
                    parent_span = self.spans[span.parent]
                    parent_span.add_child(span)
        return True

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

    def services(self, lmb=get_label_old):
        svcs = set()
        for span in self.spans.values():
            #svc = self.get_servicename(span.data)
            # more permissive notion of services
            #svc = self.get_label(span.data)
            svc = lmb(span.data)
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

    def to_dot(self, label=get_label_old, fmt="pdf"):
        g = Digraph(comment="Callgraph", format = fmt)
        for n in self.spans.values():
            notes = n.data[4]
            g.node(str(n.id), label(n.data))

        for n in self.spans.values():
            g.edge(str(n.parent), str(n.id))

        return g


class ZipkinParser(object):
    def __init__(self, file, get_label=get_label_old):
        big_dict = {}
        self.dicto = {}
        with open(file, 'r') as jsonFile:
            i = 1
            for line in jsonFile:
                trace_entry = json.loads(line)
                traceid_dict = trace_entry['trace_id']
                traceid = str(traceid_dict)
                spanid = trace_entry['id']
                parentid = trace_entry['parent_id']

                if not traceid in big_dict:
                    big_dict[traceid] = Trace(traceid, get_label)

                trace = big_dict[traceid]
                span = Span(spanid, parentid, trace_entry)
                trace.new_span(span)

        for key in big_dict:
            trace = big_dict[key]
            if trace.add_children():
                self.dicto[key] = trace
            

    def traces(self):
        # probably want an iterator here, but not sure how to do that pythonically.
        return self.dicto.values()


def ldfi_solve(tracelist, func):
    formula = get_formula(tracelist, func)
    cnf = CNFFormula(formula)
    s = ldfi_py.pilp.Solver(cnf)
    crs = s.solutions()
    ini = crs.next()
    return ini


map_dir = 'maps'
trace_dir = 'traces'


if __name__ == "__main__":
    def gl(data):
        return naming.canonical_name(get_label_2(data))

    naming = Naming()
    for file in os.listdir():
        naming.process_file("map.json")

    trace_file = 'traces.json'
    trace_file = '20180920102056-20180920112056.json'
    trace_file = 'Intest.json'

    

    parser = ZipkinParser(trace_file)
    print("Number of traces: " + str(len(parser.traces())))
    buckets=  bucket_by_url_txtype(parser)
    parser2 = ZipkinParser("Intest.json")

    b2 = bucket_by_url_txtype(parser2)
    

    for key in buckets.keys():
        #print "key  " + key
        formula = get_formula(buckets[key], gl)

        #print "(FO " + str(formula)
        f2 = get_formula(b2[key], gl)
        cnf = CNFFormula(AndFormula(formula, f2))

        s = ldfi_py.pilp.Solver(cnf)
        crs = s.solutions()
        ini = crs.next()
        print "INI is " + str(ini)
        val = map(lambda x: naming.latest_name(str(x).replace("_", "-")), ini)
        print str([trace_file, key, len(buckets[key]), val])
        
    
