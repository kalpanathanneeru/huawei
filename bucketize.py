#!/usr/bin/python

import csv
import re 

class Trace:

    def __init__(self, trace_id, span_id, annotation, 
                 duration):
        self.trace_id = trace_id;
        self.span_id = span_id;
        self.annotation = annotation;
        self.duration = duration;
    
    def __iter__(self):
        return iter([self.trace_id, self.span_id, self.annotation,
                     self.duration])


#read the data from the trace file and return list of root spans
def read_file(fn):
    num_spans = 0
    root_spans = []
    with open(fn) as csvfile:
        traces = csv.reader(csvfile, delimiter=',')
        for row in traces:
            num_spans += 1
            #empty parent string == root
            if(len(str(row[8])) == 0):
                t = Trace(row[1], row[3], row[4], row[7])
                root_spans.append(t)
    
#    print("Total number of spans: " + str(num_spans))
    return num_spans, root_spans




#write root spans to file for visual verification 
def write_to_file(spans):
    with open("root_spans.csv", "w") as f:
        writer = csv.writer(f, delimiter=',')
        for s in spans:
            writer.writerow(list(s))


#print the annotation fields
def print_annotation(spans, to_print):
    for i in range(0, to_print):
        span = spans[i]
        fields = re.split(",(?! )", span.annotation)
        
        for f in fields:
            print(f)
        print("\n")    
    

#place the root spans into buckets
def bucketize(spans):
    buckets = dict()
    for span in spans:
        fields = re.split(",(?! )", span.annotation)
        for f in fields:
            if(re.match("^http\.url.*", f)):
                url = f.split(";")[1]
                #print(url)
                if(not url in buckets):
                    buckets[url] = list()
                buckets[url].append(span)
       

    for key, value in buckets.items():
        print("{0:64s} | num spans: {1:8d}".format(key, len(value)))


total_spans, root_spans = read_file('traces1.csv')
print("Total spans: {0:8d}  | number of root spans"
      "{1:8d}".format(total_spans, len(root_spans)))

bucketize(root_spans)

to_print = 10
#print_annotation(spans, to_print)






