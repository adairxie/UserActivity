# coding:utf-8
import json
from elasticsearch import Elasticsearch

# Define config
HOST = "172.16.100.444"
PORT = 9201
TIMEOUT = 1000
DOC_TYPE = "tjkd_app_log"

BODY = {
    'size': 0,
    'aggs': {
        'agg_fingerprint': {
            'terms': {
                'field': "fingerprint",
                'size': 2147483647
            },
            'aggs': {
                'day_online_time': {
                    'sum': {
                        'field': "session_time"
                    }
                },
                'target_port_num': {
                    'cardinality': {
                        'field': "target_port"
                    }
                },
                'timestamp': {
                    'min': {
                        'field': "Timestamp"
                    }
                },
                'accesskey': {
                    'terms': {
                        'field': "accesskey",
                        'size': 1
                    }
                }
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

# Process hits here
def process_hits(hits):
    result = []
    for item in hits:
        tmp = {}
        tmp['fingerprint'] = item['key']
        tmp['day_access_count'] = item['doc_count']
        tmp['accesskey'] = item['accesskey']['buckets'][0]['key']
        tmp['day_online_time'] = item['day_online_time']['value']
        tmp['target_port_num'] = item['target_port_num']['value']
        tmp['timestamp'] = item['timestamp']['value_as_string']
        result.append(tmp)

    return result


def queryfromes(index):
    # Check index exists
    if not es.indices.exists(index=index):
        logg.info("index " + index + " not exists")
        return

    # Init scroll by search
    data = es.search(
        index=index,
        doc_type=DOC_TYPE,
        body=BODY
    )

    # Befor scroll, process current batch of hits
    if data is not None:
        return process_hits(data['aggregations']['agg_fingerprint']['buckets'])
    return
