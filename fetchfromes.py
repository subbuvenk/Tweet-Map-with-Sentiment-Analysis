import elasticsearch
from elasticsearch import Elasticsearch

es = Elasticsearch()
def fetch(keyword):
    res = es.search(index="tweets", doc_type="tweet", body={"query": {"match": {"tweet": keyword}}}, size=1000, from_=0)
    count = res['hits']['total']
    list_of_tweets=[]
    for doc in res['hits']['hits']:
        list_of_tweets.append([doc['_source']['tweet'],doc['_source']['geo']['coordinates'][0], doc['_source']['geo']['coordinates'][1],doc['_source']['sentiment']])
    if not list_of_tweets:
        return (None,0)
    return (list_of_tweets,count)
