__author__ = 'danielqueiroz'

import time
import pycurl
import urllib
import json
import pymongo
import oauth2 as oauth

MONGO_PORT = 27017

MONGO_HOST = "localhost"

USER_AGENT = 'TwitterMining 1.0'

API_ENDPOINT_URL = 'https://stream.twitter.com/1.1/statuses/filter.json'

POST_PARAMS = {'include_entities': 0,
               'stall_warning': 'true',
               'track': 'globo'}

OAUTH_KEYS = {'consumer_key': '',
              'consumer_secret': '',
              'access_token_key': '',
              'access_token_secret': ''}

class TweetStream:

    def __init__(self):
         self.load_keys()
         self.release_connection()

    def load_keys(self):
         self.oauth_consumer = oauth.Consumer(key=OAUTH_KEYS['consumer_key'], secret=OAUTH_KEYS['consumer_secret'])
         self.oauth_token = oauth.Token(key=OAUTH_KEYS['access_token_key'], secret=OAUTH_KEYS['access_token_secret'])

    def release_connection(self):
         self.buffer = ''
         self.conn = None
         self.timeout = False

    def setup_connection(self):
        self.conn = pycurl.Curl()
        self.conn.setopt(pycurl.POST, 1)
        self.conn.setopt(pycurl.LOW_SPEED_LIMIT, 1)
        self.conn.setopt(pycurl.LOW_SPEED_TIME, False)
        self.conn.setopt(pycurl.URL, API_ENDPOINT_URL)
        self.conn.setopt(pycurl.USERAGENT, USER_AGENT)
        self.conn.setopt(pycurl.ENCODING, 'deflate, gzip')
        self.conn.setopt(pycurl.WRITEFUNCTION, self.handle_tweet)
        self.conn.setopt(pycurl.POSTFIELDS, urllib.urlencode(POST_PARAMS))
        self.conn.setopt(pycurl.HTTPHEADER, ['Host: stream.twitter.com', 'Authorization: %s' % self.get_oauth_header()])

    def setup_database_connection(self):
        self.connection = pymongo.Connection(MONGO_HOST, MONGO_PORT)
        self.db = self.connection.twitterstream

    def get_oauth_header(self):
        params = {'oauth_version': '1.0',
                  'oauth_nonce': oauth.generate_nonce(),
                  'oauth_timestamp': int(time.time())}
        req = oauth.Request(method='POST', parameters=params, url='%s?%s' % (API_ENDPOINT_URL, urllib.urlencode(POST_PARAMS)))
        req.sign_request(oauth.SignatureMethod_HMAC_SHA1(), self.oauth_consumer, self.oauth_token)
        return req.to_header()['Authorization'].encode('utf-8')

    def start(self):
        while True:
            self.setup_connection()
            try:
                self.conn.perform()
            except:
                print 'Network error: %s' % self.conn.errstr()
                print 'Waiting 0.25 seconds before trying again'
                time.sleep(0.25)
                continue

    def handle_tweet(self, data):
        self.buffer += data
        if data.endswith('\r\n') and self.buffer.strip():
            message = json.loads(self.buffer)
            self.buffer = ''
            msg = ''
            if message.get('limit'):
                print 'Rate limiting caused us to miss %s tweets' % (message['limit'].get('track'))
            elif message.get('disconnect'):
                raise Exception('Got disconnect: %s' % message['disconnect'].get('reason'))
            elif message.get('warning'):
                print 'Got warning: %s' % message['warning'].get('message')
            else:
                try:
                    self.db.tweets.save(message)
                except:
                    print 'Database Error: Cannot insert tweet'
                print 'Got tweet with text: %s' % message.get('text')


if __name__ == '__main__':
    ts = TweetStream()
    ts.setup_connection()
    ts.setup_database_connection()
    ts.start()

