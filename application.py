from flask import request
from flask import Flask,render_template
import requests
import fetchfromes
import elasticsearch
from elasticsearch import Elasticsearch
import json
es = Elasticsearch()
application = Flask(__name__)
@application.route('/', methods=['POST', 'GET'])
def index(tweets=None):
	error=None
	if request.method=='POST':
		key=request.form['keyword']
		list_of_tweets, count=fetchfromes.fetch(key)
		return render_template('home.html',tweets=list_of_tweets, keyword=key, count = count)
	return render_template('home.html',tweets=None, count=0)

@application.route('/update', methods=['POST'])
def update(tweets=None):
	if request.method == 'POST':
		try:
			js = request.get_json()
		except:
			pass

		hdr = request.headers.get('X-Amz-Sns-Message-Type')
		# subscribe to the SNS topic
		if hdr == 'SubscriptionConfirmation' and 'SubscribeURL' in js:
			r = requests.get(js['SubscribeURL'])

		if hdr == 'Notification':
			msg = js['Message']
			print msg
			es.index(index="tweets", doc_type='tweet', body=msg)
			print 'updated'

		return 'OK\n'

if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    application.debug = True
    application.run(host= '0.0.0.0')



