#!/usr/bin/env python

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan
import os
import csv
import sys

def get_docs(csvfilename, index):
    fsplit = csvfilename.split('.')
    doctype = fsplit[0]
    with open(csvfilename) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row['_index'] = index
            row['_type'] = doctype
            yield row

def main():
    es = Elasticsearch([{'host' : sys.argv[1], 'port' : sys.argv[2]}])
    index = sys.argv[3]
    filenames = os.listdir('.')
    for f in filenames:
        if f.endswith('.csv'):
            for a, b in streaming_bulk(es, get_docs(f, index)):
                print a, b

if __name__ == "__main__":
    main()
