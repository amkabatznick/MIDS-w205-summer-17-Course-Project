import tweepy
import os
import sys
import json
import Levenshtein
import psycopg2
import datetime
import time

#twitter keys set as environmental variables for security
try:
    consumer_key = os.environ['TWITTER_CONSUMER_KEY']
    consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
    access_token = os.environ['TWITTER_ACCESS_TOKEN']
    access_token_secret = os.environ['TWITTER_ACCESS_SECRET']
except KeyError:
    sys.stderr.wirte("TWITTER_*environment variables not se\n")
    sys.exit(1)

#twitter authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

#connect to postgres
conn = psycopg2.connect(database="twitterdata", user="postgres", password="pass",
host="localhost", port="5432")
cur = conn.cursor()

max_tweets = 1000
return_per_page = 100 #max allowable
search_string = 'charlottesville'
since_id = None
max_id = -1L

tweetcount = 0
while tweetcount < max_tweets:
        if max_id <= 0:
                if not since_id:
                        new_tweets =api.search(q = search_string, count = return_per_page)
                else:
                        new_tweets = api.search(q = search_string, count = return_per_page,
since = since_id)
        else:
                if not since_id:
                        new_tweets = api.search(q = search_string, count = return_per_page,
max_id = str(max_id -1))
                else:
                        new_tweets = api.search(q = search_string, count = return_per_page,
max_id = str(max_id -1), since_id = since_id)

        if not new_tweets:
                break
        status_list_dicts = [dict(status._json) for status in new_tweets]
        for i in range(len(status_list_dicts)):
                try:
                        tweet = status_list_dicts[i]['text'].encode('utf-8')
                except:
                        continue
                split_tweet = tweet.split()
                hashtags = dict(status_list_dicts[i]['entities'])['hashtags']
                for word in split_tweet:
                        if Levenshtein.distance(word, search_string) <= 3:
                                date = str(status_list_dicts[i]['created_at'])
                                date = datetime.datetime.strptime(date, '%a %b %d %H:%M:%S +
0000 %Y')
                                cur.execute("INSERT INTO tweets (tweet, timestamp) VALUES (%
s, %s)", (tweet, date))
                        else:
                                for d in hashtags:
                                        d = dict(d)
                                        try:
                                                hashtag_str = str(d['text']).encode('utf-8')
                                        except:
                                                continue
                                        if Levenshtein.distance(hashtag_str,search_string) <
= 3:
                                                date = str(status_list_dicts[i]['created_at'
])
                                                date = datetime.datetime.strptime(date, '%a
%b %d %H:%M:%S +0000 %Y')
                                                cur.execute("INSERT INTO tweets (tweet, time
stamp) VALUES (%s, %s)", (tweet, date))
                        conn.commit()
        tweetcount += len(new_tweets)
        print "downloaded {} new tweets!".format(len(new_tweets))
        max_id = new_tweets[-1].id

