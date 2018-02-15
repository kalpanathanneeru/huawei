from ldfi_py.pbool import *
import ldfi_py.pilp
import ldfi_py.psat
from parsing import ZipkinParser
import sys, re, os

def get_bucket(trace):
    data = trace.root_annotations()[4]
    p = re.compile("http\.url;([^,]+)")
    m = p.search(data)
    if  m is not None:
        return m.group(1)
    else:
        return "??"

def get_formula(traces):
    outerset = set()
    for trace in traces:
        outerset.add(frozenset(trace.services()))

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
     

trace_file = '1st_half_jan_26'

buckets = {}

parser = ZipkinParser(trace_file)

for trace in parser.traces():
    if trace.sanity():
        #trace.to_dot().render("fooz")
        #print "RAC " + str(trace.services())
        # get bucket -- for now, Tuan says http.url
        bucket = get_bucket(trace)
        if bucket not in buckets:
            buckets[bucket] = set()
        buckets[bucket].add(trace)
    
for key in buckets:
    #print key + " : " + str(len(buckets[key]))
    print "BUCKET " + key + "(" + str(len(buckets[key])) + ")"
    #if key == "http://127.0.0.1:8081/atswebsite/website":
    #    print "ONO"
    #    next
    #else: 
    formula = get_formula(buckets[key])
    #dir = key.replace("/", "_")
    #try:
    #    os.stat(dir)
    #except:
    #    os.mkdir(dir)

    #for trace in buckets[key]:
    #    trace.to_dot().render(dir + "/" + trace.id)    
    

    print "FORMO " + str(formula)
    cnf = CNFFormula(formula)

    for c in cnf.conjuncts():
        print "Con: " + str(c)

    s = ldfi_py.pilp.Solver(cnf)
    #s = ldfi_py.psat.Solver(cnf)

    #soln = next(s.solutions())
    #print "SOLN is " + str(soln)
    crs = s.solutions()
    for _ in range(3):
        try:
            soln = next(crs)
            print "SOLN is " + str(soln)
        except:
            print "DONE"
