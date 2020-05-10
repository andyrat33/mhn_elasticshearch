from elasticsearch import Elasticsearch
import pathlib
import argparse

es = Elasticsearch(hosts="mhn", port='9200')
# ['https://user:secret@localhost:443']

parser = argparse.ArgumentParser()

parser = argparse.ArgumentParser(description="Get passwords from MHN and store to a named file")
group = parser.add_mutually_exclusive_group()
group.add_argument("-f", "--file", dest="filename", help="password file", default='passwords.txt', metavar="FILE")
args = parser.parse_args()



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


yesno = 'y'

file = pathlib.Path(args.filename)
if file.exists():
    yesno = input("File {} exists append to it? (Y/N) ".format(args.filename))

if yesno == 'y' or yesno == 'Y':
    for result in iterate_distinct_field(es, fieldname="ssh_password", index="mhn-*"):
        #print(result['key']['ssh_password'])  # e.g. {'key': {'pattern': 'mypattern'}, 'doc_count': 315}
        try:
            fn = open(args.filename, mode='ab')
        except IOError as e:
            print("Error: {}".format(e))
            sys.exit()
        else:
            with fn:
                fn.write(result['key']['ssh_password'].encode('utf-8'))
                fn.write(b'\n')
else:
    print("Aborted")