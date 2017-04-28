import boto.sqs 
import boto3
from boto.sqs.message import RawMessage
import json
import time 
import requests
import monkeylearn
from monkeylearn import MonkeyLearn
import re
import elasticsearch
from elasticsearch import Elasticsearch


REGION  = 'us-west-2'
QUEUE1   = 'Q1'
counter = 0
 
 #initializations
es = Elasticsearch()
arn = 'arn:aws:sns:us-west-2:250741115549:notify_sent'
ml = MonkeyLearn('c15ecc6f9d03c4706e6ec0989e2c8ba7515a3ff5')
module_id = 'cl_qkjxv9Ly'
sqs =  boto.sqs.connect_to_region(REGION, aws_access_key_id='AKIAJPK3CMEWGPMYOV4A', aws_secret_access_key='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')
# sns = boto3.client('sns', aws_access_key_id='AKIAJPK3CMEWGPMYOV4A', aws_secret_access_key='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')
sns = boto3.resource('sns',aws_access_key_id='AKIAJPK3CMEWGPMYOV4A', aws_secret_access_key='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')
platform_endpoint = sns.PlatformEndpoint(arn)

#Sending to queue method
# def pushToQueue(sqs, qname, jsontype):
#     queue1 = sqs.get_queue(qname)
#     sqs.send_message(queue1, jsontype)
#     print 'Pushed to queue' 


def queue_count(REGION, QUEUE):
    conn = boto.sqs.connect_to_region(REGION, aws_access_key_id='AKIAJPK3CMEWGPMYOV4A', aws_secret_access_key='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')
    q = conn.get_queue(QUEUE)
    count = q.count()
    return count 


def get_messages(REGION, QUEUE):
    conn = boto.sqs.connect_to_region(REGION, aws_access_key_id='AKIAJPK3CMEWGPMYOV4A', aws_secret_access_key='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')
    q = conn.get_queue(QUEUE)
    mess = q.set_message_class(RawMessage)
    mess = q.get_messages()

    for result in mess:
        rst = result.get_body()
        delete_msg = json.loads(rst)
        rst = json.loads(rst)

        tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", rst['tweet']).split())

        res = ml.classifiers.classify(module_id, [rst['tweet']], sandbox=True)

        #sent1 represents string: positive, negative, neutral
        #sent2 represents a digit: negative=0, neutral=1, positive=2

        rst['sentiment'] = res.result[0][0]['label']

        if rst['sentiment'] == 'positive':
            rst['sent_score'] = 2
        elif rst['sentiment'] == 'neutral':
            rst['sent_score'] = 1
        elif rst['sentiment'] == 'negative':
            rst['sent_score'] = 0

        # print '-'*50
        # print rst
        # print '-'*50

        #pushing to SNS
        # es.index(index="tweets", doc_type='tweet', body=rst)
        snsMessage = json.dumps(rst)
        response = platform_endpoint.publish(Message=snsMessage, Subject='Notification')
        

        q.delete_message(result)
        print "Deleted msg from queue"

        # text_list = ["This is a text to test your classifier", "This is some more text"]
        # res = ml.classifiers.classify(module_id, rst['tweet'], sandbox=True)
        # print res.result

        #add sentiment values from monkey learn
        #do this by appending to rst dictionary the sentiment keys
        #then push to another sqs like you said I guess
        

while True:
    if queue_count(REGION, QUEUE1) > 0:
        get_messages(REGION, QUEUE1)
        time.sleep(2)
        counter = counter + 1
        print counter
    else:
        time.sleep(10)

