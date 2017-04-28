from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from tweepy import Stream
import boto.sqs
import time
import json
import sys
import fetch
from boto.sqs.connection import SQSConnection
import elasticsearch
from elasticsearch import Elasticsearch


con_key ='TQQvceMyQ8JpIp4yPDJ51eqkU'
con_secret ='tmdzEtXmD9dZhXfdt7hAUClL1ZC8efirBmhPYLztbb4iMzWIrL'
acess_token ='357005134-tlDx6j7FL3YfT749102zQcLU8uQ5ZlUj1KQxvkqc'
acess_secret = 'hp99szIiMvtOCvrDYDztPYCypJkBdLMtS4dfUuknyrRd0'

sqs = boto.sqs.connect_to_region('us-west-2', aws_access_key_id='AKIAJPK3CMEWGPMYOV4A', aws_secret_access_key='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')
# sqs = SQSConnection('AKIAJPK3CMEWGPMYOV4A', 'tfFm764fstOUnTHTvxEYtb7o3POZRbEFLpDvyRBc')
es = Elasticsearch()
print 'Connected with SQS... '

def pushToQueue(sqs, qname, jsontype):
    queue1 = sqs.get_queue(qname)
    sqs.send_message(queue1, jsontype)
    print 'Pushed to queue'
    print queue1 

  

class listener(StreamListener):

    def on_data(self, raw_data):
        all_data = json.loads(raw_data)

        if 'text' in all_data:
            tweets = all_data["text"]
            username = all_data["user"]["screen_name"]
            location = all_data["user"]["location"]
            geo=all_data["geo"]
            coordinates=all_data["coordinates"]
            if geo!=None:
                doc = {
                        'time': time.time(),
                        'username': username,
                        'tweet': tweets,
                        'location': location,
                        'geo': geo,
                        'coordinates': coordinates
                        }
                # jsontype = json.dumps(doc)
                # pushToQueue(sqs, 'Q1', jsontype)
                es.index(index="tweets", doc_type='tweet', body=doc)
                print ' entry'
                

    def on_error(self, status_code):
        print status_code

    def on_connect(self):
        print self

auth = OAuthHandler(con_key, con_secret)
auth.set_access_token(acess_token, acess_secret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["like","travel","peace","good","love","today","work","nyc"])
