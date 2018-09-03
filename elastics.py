# coding:utf-8
import json
from elasticsearch import Elasticsearch

# Define config
HOST = "172.16.100.44"
PORT = 9201
TIMEOUT = 1000
DOC_TYPE = "ngx_error_log"

BODY = {
    'size': 10000,
    "query": {
        "wildcard": {
            "error_msg": {
                "value": "*kfirewall*"
            }
        }
     }
}

# Init Elasticsearch instance
es = Elasticsearch(
    [
        {
            'host': HOST,
            'port': PORT
        }
    ],
    timeout=TIMEOUT
)

def getIP(errorMsg):
   splits = errorMsg.split(',') 
   if len(splits) > 3:
       clientIP = splits[2]
       if clientIP is not None:
           info = clientIP.split(': ')
           return info[1]

# Process hits here
def process_hits(hits, result):
    for item in hits:
        source = item['_source']
        hostname = source['hostname']
        host = source['host']
        error_msg = source['error_msg']
        ip = getIP(error_msg)
        if ip  not in result.keys():
            result[ip] = {'hostname': set([hostname]) , 'host':set([host]), 'count': 1}
        else:
            result[ip]['count'] = result[ip]['count'] + 1
            result[ip]['hostname'].add(hostname)
            result[ip]['host'].add(host)

        #print("result['ip']': " + str(result[ip]['count']))
        #print("result['ip']': " + str(len(result[ip]['hostname'])))


def queryfromes(index):
    # Check index exists
    if not es.indices.exists(index=index):
        print("index not exists")
        return

    result = {}
    # Init scroll by search
    page = es.search(
        index=index,
        doc_type=DOC_TYPE,
        scroll = '4m',
        size = 10000,
        body=BODY
    )

    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    process_hits(page['hits']['hits'], result)

    # start scrolling
    while(scroll_size > 0):
        print "Scrolling..."
        page = es.scroll(scroll_id = sid, scroll ='2m')
        # update the scroll id
        sid = page['_scroll_id']

        scroll_size = len(page['hits']['hits'])
        #count = count + scroll_size
        process_hits(page['hits']['hits'], result)
        #print(count)
    return result

#queryfromes('ngx_error_log_2018_08_28')
