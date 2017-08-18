
import tweepy
import os
import sys
import json
import Levenshtein

try:
    consumer_key = os.environ['TWITTER_CONSUMER_KEY']
    consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
    access_token = os.environ['TWITTER_ACCESS_TOKEN']
    access_token_secret = os.environ['TWITTER_ACCESS_SECRET']
except KeyError:
    sys.stderr.wirte("TWITTER_*environment variables not se\n")
    sys.exit(1)

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

public_tweets = api.home_timeline()
for tweet in public_tweets:
    print tweet.text

search_string = 'charlottesville'
status_list = api.search(search_string)
status_list_dicts = [dict(status._json) for status in status_list]
key_set = set()
for i in range(len(status_list_dicts)):
        tweet = status_list_dicts[i]['text'].encode('utf-8').split()
        hashtags = dict(status_list_dicts[i]['entities'])['hashtags']
        for word in tweet:
            if Levenshtein.distance(word, search_string) <= 3:
                #write tweet and time to postgres
                print "word", word
                continue
            else:
                for d in hashtags:
                    d = dict(d)
                    hashtag_str = str(d['text'])
                    if Levenshtein.distance(hashtag_str,search_string) <= 3:
                    #write tweet and time to postgres
                        print "hashtag", hashtag_str
                        pass

        #print status_list_dicts[i]['entities']
        #print status_list_dicts[i]['metadata']
        #print status_list_dicts[i]['retweeted_status']
        #print status_list_dicts[i]['possibly_sensitive']
        #print status_list_dicts[i]['created_at']
        #if 'extended_entities' in status_list_dicts[i]:
        #       print status_list_dicts[i]['extended_entities']
