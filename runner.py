from huawei_local import *
import ldfi_py.pilp
from parse_traces import get_input_file
import os

def get_label_1(data):
    svcarr = data[9].split(":")[1].split("-")
    svcarr.pop()
    return "-".join(svcarr)

def get_label_2(data):
    return data[9].split(":")[1]



class Stats():
    def __init__(self):
        self.min = 99999999
        self.max = 0
        self.sum = 0
        self.cnt = 0


    def point(self, point):
        if point < self.min:
            self.min = point
        if point > self.max:
            self.max = point
        self.sum += point
        self.cnt += 1

    def report(self):
        return [self.min, self.max, self.sum / self.cnt]


trace_file = get_input_file()
#buckets = get_all_buckets(trace_file)

#parser = ZipkinJsonParser(trace_file)
#print("Number of traces: " + str(len(parser.traces())))
#dump_traces(parser)
#bucket_by_url(parser)
#analyze_traces(parser)
#res = bucket_by_url_txtype_json(parser)

buckets = get_all_buckets(trace_file)

keys = buckets.keys()
keys.sort()

for key in keys:
#    print "KEY " + key + " -- " + str(len(buckets[key]))

#key = "_v3_auth_tokensPOST"
    fname = key.replace("/", "_")
    os.mkdir(fname)

    #depth = Stats()
    #width = Stats()
    nodes = Stats()

    i = 0
    for val in buckets[key]:
        #print "Val is " + str(val)
        #val.to_dot(label=get_label_1).render(fname + "/" + str(i))
        nodes.point(val.span_cnt())
        i += 1
    

    #print str([trace_file, key, len(buckets[key]), nodes.report()])

    formula = get_formula(buckets[key], get_label_2)

    #print "FORM " + str(formula)
    cnf = CNFFormula(formula)
    s = ldfi_py.pilp.Solver(cnf)
    crs = s.solutions()

    print str([trace_file, key, len(buckets[key]), nodes.report(), crs.next()])

    #print "NEXT : " + str(crs.next())

