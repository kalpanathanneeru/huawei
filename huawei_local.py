from parsing import ZipkinParser
import re
from ldfi_py.pbool import *
from parse_traces import bucket_by_url_txtype

def get_bucket(trace):
    data = trace.root_annotations()[4]
    p = re.compile("http\.url;([^,]+)")
    m = p.search(data)
    if  m is not None:
        return m.group(1)
    else:
        return "??"

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


def get_all_buckets(trace_file):
    buckets = {}
    parser = ZipkinParser(trace_file)
    
    buckets = bucket_by_url_txtype(parser)

    print "OK"
    ##


    for trace in parser.traces():
        if trace.sanity():
            # get bucket -- for now, Tuan says http.url
            bucket = get_bucket(trace)
            if bucket not in buckets:
                buckets[bucket] = set()
            buckets[bucket].add(trace)
    return buckets
