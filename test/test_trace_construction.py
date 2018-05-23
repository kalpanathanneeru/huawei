#!/usr/bin/python

import unittest
from ..parsing import ZipkinParser, Trace, Span, get_label_old

class ZipkinCsvTests(unittest.TestCase):

    def setUp(self):
        self.parser = ZipkinParser("huawei/test/data/csv_trace")

    def test_num_traces(self):
        num_traces = 83
        print "Test to check that all traces in the trace file are parsed"
        self.assertEqual(num_traces, len(self.parser.big_dict.keys()) )
        print "Test passed\n"

    def test_root_span(self):
        ## Note that the root label is same for all traces in this trace file
        root_span_name = 'api-gateway-2679142795'
        print "Test to check the root label name for each trace"
        for key in sorted(self.parser.big_dict.keys()):
            trace = self.parser.big_dict[key]
            root_label = trace.get_label(trace.root.data)
            self.assertEqual(root_span_name, root_label)
        print "Test passed\n"

    def test_number_spans(self):
        num_spans = [33, 17, 17, 17, 18, 17, 17, 17, 17, 17, 32, 17, 17, 17, 17, 17, 17, 17, 17, 17, 32, 17, 17, 17, 17, 17, 17, 17, 31, 17, 17, 17, 18, 17, 18, 17, 17, 17, 18, 17, 17, 18, 17, 17, 17, 18, 17, 18, 17, 17, 18, 33, 17, 31, 17, 17, 17, 17, 17, 17, 18, 18, 18, 17, 17, 17, 17, 19, 18, 18, 17, 17, 19, 17, 33, 17, 17, 17, 17, 17, 18, 17, 17]

        print "Test to check the total number of spans for each trace"

        for key in sorted(self.parser.big_dict.keys()):
            trace = self.parser.big_dict[key]
            self.assertEqual(num_spans.pop(0), trace.span_cnt())

        print "Test passed\n"

    def test_number_child_spans(self):
        # this test is only being done for the first five traces for brevity
        span_cnts_breakdown = [[2, 4, 1, 0, 2, 2, 1, 1, 0, 1, 1, 1, 2, 1, 1, 1, 1, 0, 0, 1, 3, 2, 1, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0], [2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 3, 0, 0, 0], [2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 3, 0, 0, 0], [2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 3, 0, 0, 0], [2, 1, 2, 1, 1, 1, 1, 1, 1, 2, 1, 0, 0, 0, 3, 0, 0, 0], [2, 2, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 3, 0, 0, 0, 0], [2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 3, 0, 0, 0]]

        print "Test to check that each node has correct number of children"
        num = 0
        for key in sorted(self.parser.big_dict.keys()):
            if num == 5:
                break
            trace = self.parser.big_dict[key]
            res = span_cnts_breakdown[num]
            to_process = [trace.root]
            while len(to_process) > 0:
                span = to_process.pop(0)
                to_process.extend(span.children)
                self.assertEqual(res.pop(0), len(span.children))
            num += 1
        print "Test passed"

    def test_child_span_names(self):
        span_labels = [['products-service-2041284336', 'api-gateway-2679142795', 'products-service-2041284336', 'dao-service-4222452337', 'user-service-173572575', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'dao-service-4222452337', 'user-service-173572575', 'dao-service-4222452337', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'user-service-173572575', 'dao-service-4222452337', 'products-service-2041284336', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'user-service-173572575', 'dao-service-4222452337', 'api-gateway-2679142795', 'dao-service-4222452337', 'dao-service-4222452337', 'dao-service-4222452337', 'user-service-173572575', 'user-service-173572575', 'dao-service-4222452337', 'dao-service-4222452337', 'dao-service-4222452337', 'dao-service-4222452337'], ['api-gateway-2679142795', 'products-service-2041284336', 'api-gateway-2679142795', 'products-service-2041284336', 'dao-service-4222452337', 'api-gateway-2679142795', 'products-service-2041284336', 'dao-service-4222452337', 'api-gateway-2679142795', 'products-service-2041284336', 'dao-service-4222452337', 'api-gateway-2679142795', 'dao-service-4222452337', 'dao-service-4222452337', 'dao-service-4222452337', 'dao-service-4222452337'], ['api-gateway-2679142795', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'dao-service-4222452337', 'dao-service-4222452337', 'dao-service-4222452337'], ['api-gateway-2679142795', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'dao-service-4222452337', 'dao-service-4222452337', 'dao-service-4222452337'], ['api-gateway-2679142795', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'products-service-2041284336', 'api-gateway-2679142795', 'dao-service-4222452337', 'products-service-2041284336', 'api-gateway-2679142795', 'api-gateway-2679142795', 'dao-service-4222452337', 'dao-service-4222452337', 'dao-service-4222452337', 'dao-service-4222452337']]

        # Test checks 5 traces only to keep test size manageable
        print "Test to check that each span is connected to the right children spans"
        num = 0
        for key in sorted(self.parser.big_dict.keys()):
            if num == 5:
                break
            trace = self.parser.big_dict[key]
            res = span_labels[num]
            to_process = [trace.root]
            while len(to_process) > 0:
                span = to_process.pop(0)
                to_process.extend(span.children)
                for child_span in span.children:
                    self.assertEqual(trace.get_label(child_span.data), res.pop(0))
            num += 1
        print "Test passed"

