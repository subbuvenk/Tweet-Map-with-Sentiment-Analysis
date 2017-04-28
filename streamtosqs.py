from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from tweepy import Stream
import boto.sqs
import time
import json
import sys
import fetch
from boto.sqs.connection import SQSConnection


con_key ='CON_KEY'
con_secret ='CON_SECRET'
acess_token ='ACCESS_TOKEN'
acess_secret = 'ACCESS_SECRET'

sqs = boto.sqs.connect_to_region('us-west-2', aws_access_key_id='ACCESS_KEY', aws_secret_access_key='SECRET_ACCESS')
# sqs = SQSConnection('ACCESS_KEY', 'SECRET_ACCESS')
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
            lang = all_data["lang"]
            if geo!=None and lang  == "en":
                doc = {
                        'time': time.time(),
                        'username': username,
                        'tweet': tweets,
                        'location': location,
                        'geo': geo,
                        'coordinates': coordinates
                        }
                print doc
                jsontype = json.dumps(doc)
                pushToQueue(sqs, 'Q1', jsontype)
                

    def on_error(self, status_code):
        print status_code

    def on_connect(self):
        print self

auth = OAuthHandler(con_key, con_secret)
auth.set_access_token(acess_token, acess_secret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["photo","travel","cricket","play","love","today","work","outfit"])
