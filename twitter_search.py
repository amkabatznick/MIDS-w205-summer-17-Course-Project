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
conn = psycopg2.connect(database="nyt", user="postgres", password="pass",
host="localhost", port="5432")
cur = conn.cursor()

def fuzzy_substring(needle, haystack):
    """Calculates the fuzzy match of needle in haystack,
    using a modified version of the Levenshtein distance
    algorithm.
    The function is modified from the levenshtein function
    in the bktree module by Adam Hupp"""

    m, n = len(needle), len(haystack)

    # base cases
    if m == 1:
        return not needle in haystack
    if not n:
        return m

    row1 = [0] * (n+1)
    for i in range(0,m):
        row2 = [i+1]
        for j in range(0,n):
            cost = ( needle[i] != haystack[j] )

            row2.append( min(row1[j+1]+1, # deletion
                               row2[j]+1, #insertion
                               row1[j]+cost) #substitution
                           )
        row1 = row2
    return min(row1)

max_tweets = 1000
return_per_page = 100 #max allowable
search_string = 'solar eclipse'
since_id = None
max_id = -1L

cur.execute("SELECT facet_detail_id, facet_details FROM facet_details;")
all_facets = cur.fetchall()
found_facet = False
for facet in all_facets:
        if Levenshtein.distance(facet[1],search_string) <= 3:
                facet_detail_id = facet[0]
                found_facet = True
                break

if not found_facet:
        cur.execute("INSERT INTO facet_details (facet_details) SELECT %s WHERE NOT EXISTS (S
ELECT facet_details FROM facet_details WHERE facet_details = %s) RETURNING facet_detail_id",
(search_string,search_string))
        facet_detail_id = cur.fetchone()[0]
        conn.commit()

tweetcount = 0
mod = 1000
while tweetcount < max_tweets:
        if max_id <= 0:
                if not since_id:
                        new_tweets =api.search(q = search_string, lang = 'en', count = retur
n_per_page, geocode = '39.76452,-104.995198,15mi')
                else:
                        new_tweets = api.search(q = search_string, lang = 'en', count = retu
rn_per_page, since = since_id, geocode = '39.76452,-104.995198,15mi')
        else:
                if not since_id:
                        new_tweets = api.search(q = search_string, lang = 'en', count = retu
rn_per_page,
max_id = str(max_id -1), geocode = '39.76452,-104.995198,15mi')
                else:
                        new_tweets = api.search(q = search_string, lang = 'en', count = retu
rn_per_page, max_id = str(max_id -1), since_id = since_id, geocode = '39.76452,-104.995198,1
5mi')

        if not new_tweets:
                break
        status_list_dicts = [dict(status._json) for status in new_tweets]
        for i in range(len(status_list_dicts)):
                try:
                        tweet = status_list_dicts[i]['text'].encode('utf-8')
                except:
                        continue
                hashtags = dict(status_list_dicts[i]['entities'])['hashtags']
                if fuzzy_substring(search_string, tweet) <= 3:
                        date = str(status_list_dicts[i]['created_at'])
                        date = datetime.datetime.strptime(date, '%a %b %d %H:%M:%S +0000 %Y'
)
                        cur.execute("INSERT INTO tweets (tweet, date, facet_detail_id) VALUE
S (%s, %s, %s)", (tweet, date, facet_detail_id))
                else:
                        for d in hashtags:
                                d = dict(d)
                                try:
                                        hashtag_str = str(d['text']).encode('utf-8')
                                except:
                                        continue
                                if fuzzy_substring(search_string,hashtag_str) <= 3:
                                        date = str(status_list_dicts[i]['created_at'])
                                        date = datetime.datetime.strptime(date, '%a %b %d %H
:%M:%S +0000 %Y')
                                        cur.execute("INSERT INTO tweets (tweet, date, facet_
detail_id) VALUES (%s, %s, %s)", (tweet, date, facet_detail_id))
                        conn.commit()
        tweetcount += len(new_tweets)
        print "downloaded {} new tweets!".format(len(new_tweets))
        max_id = new_tweets[-1].id

