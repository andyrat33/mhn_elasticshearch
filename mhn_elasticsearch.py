#!/usr/bin/env python3

import sys
from elasticsearch import Elasticsearch
import pathlib
import argparse

def iterate_distinct_field(es, fieldname, pagesize=250, **kwargs):
    """
    Helper to get all distinct values from ElasticSearch
    (ordered by number of occurrences)
    """
    compositeQuery = {
        "size": pagesize,
        "sources": [{
                fieldname: {
                    "terms": {
                        "field": fieldname
                    }
                }
            }
        ]
    }
    # Iterate over pages
    while True:
        result = es.search(**kwargs, body={
            "aggs": {
                "values": {
                    "composite": compositeQuery
                }
            }
        })
        # Yield each bucket
        for aggregation in result["aggregations"]["values"]["buckets"]:
            yield aggregation
        # Set "after" field
        if "after_key" in result["aggregations"]["values"]:
            compositeQuery["after"] = \
                result["aggregations"]["values"]["after_key"]
        else: # Finished!
            break

parser = argparse.ArgumentParser()
# Use Cases
# 1. Passwords for dictionary (default)
# 2. IP Addresses
# Optional Arguments -e <server> [-p port default:9200]

parser = argparse.ArgumentParser(description="Get Passwords/Threat Intel from Modern Honey Net Elasticsearch and store to a named file")
parser.add_argument('-e', dest='mhn_address', action='store', help='MHN Elasticsearch host Address', default='mhn', )
parser.add_argument('-p', help="MHN Port (default 9200)", default=9200, type=int, action='store', dest='mhn_port')
subparsers = parser.add_subparsers(help='commands', dest='command')

#  passwords command
pass_parser = subparsers.add_parser('passwords', help='Create a password dictionary')
pass_parser.add_argument('filename', action='store', help='Passwords file name')
#  ip command
ip_parser = subparsers.add_parser('ip', help='IP Addresses of attackers')
ip_parser.add_argument('filename', action='store', help='File name for IP Addresses')

args = parser.parse_args()
es = Elasticsearch(hosts=args.mhn_address, port=args.mhn_port)


if args.command == 'passwords':
    #print('passwords')
    fieldname = 'ssh_password'
elif args.command == 'ip':
    #print('ip')
    fieldname = 'src_ip'

yesno = 'y'

file = pathlib.Path(args.filename)
if file.exists():
    yesno = input("File {} exists append to it? (Y/N) ".format(args.filename))

if yesno == 'y' or yesno == 'Y':
    for result in iterate_distinct_field(es, fieldname=fieldname, index="mhn-*"):
        #print(result['key']['ssh_password'])  # e.g. {'key': {'pattern': 'mypattern'}, 'doc_count': 315}
        try:
            fn = open(args.filename, mode='ab')
        except IOError as e:
            print("Error: {}".format(e))
            sys.exit()
        else:
            with fn:
                fn.write(result['key'][fieldname].encode('utf-8'))
                fn.write(b'\n')
else:
    print("Aborted")