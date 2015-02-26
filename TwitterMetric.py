__author__ = 'RichardKotermanski'
import pymongo
import tweepy
from tweepy import *
from tweepy.parsers import ModelParser
import re
import operator
import time
import threading
import json
import logging
import sys
from bson.json_util import dumps

__user_list__ = ["LilTunechi", "VanessaHudgens", "813286", "22412376", "268414482", "PutinRF", "rickykoter", "arrijabba"]
num_historical_tweets_to_analyze = 10


consumer_key= "1uhm0XFRPSqqpFL3qH54Xe8qK"
consumer_secret = "HsS0M2kCLf7DUo3XdmmQoNj8p4Smtg7aCe7ULbvRVoSUn88jaw"
access_token= "853958474-yc7csayuDGZaKbUL6hBzmvZy3KA3Da7fHRklT2UF"
access_token_secret = "iXBDDg4XyZ7gO8DVfbnDxP2bu2i8j7duaNdgNslgTixrB"

extern_link_count = 0
avg_time = 0.0
status_count = 0.0
sources = {}
words = {}
friends = 0
followers = 0
statuses_all = []
db = None


def print_status_stats(status_stats):
    print("///////////////////////////////////////////////////////////")
    print("Name: " + status_stats['twitter_handle'])
    print("User ID: " + str(status_stats['user_id']))
    print("Average Post Time: " + status_stats['average_post_time'])
    print("Preferred App: " + status_stats['preferred_app'])
    print("Friends: " + str(status_stats['friends_count']))
    print("Followers: " + str(status_stats['followers_count']))
    print("Ratio of external links in statuses: " + str(status_stats['external_link_ratio']))
    print("Top Ten Words/Strings Used: ")
    for word in status_stats['top_ten_used_words']:
        print("\t- " + word)
    print("///////////////////////////////////////////////////////////")


def process_stream_status(status_in):
    global db
    if db:
        updated_post = {}
        posts = db['posts']
        post = posts.find_one({'twitter_handle': status_in.author.screen_name})
        user = api.get_user(status_in.author.screen_name)
        if post:
            sec_avg = float(post['num_analyzed'])*float(post['seconds_of_day_for_avg_time'])
            statuses = post['analyzed_statuses']
            statuses.append(dumps(status_in._json))
            apps = post['all_apps_usage']
            s = status_in.source
            if s not in apps:
                apps[s] = 1
            else:
                apps[s] += 1

            wrds = post['all_words_used_in_statuses']
            split_text = re.findall('\w+ ', unicode(status_in.text))
            for word in split_text:
                if word not in wrds:
                    wrds[word] = 1
                else:
                    wrds[word] += 1

            link_count = post['external_link_ratio']*(len(statuses)-1)
            m = re.search('((http|https):\/\/(?!www.twitter.com)[\w\.\/\-=?#]+)', status_in.text)
            if m and len(m.group(0)) > 0:
                link_count += 1

            most_used_source = dict(sorted(apps.iteritems(), key=operator.itemgetter(1), reverse=True)[:1]).keys()[0]
            top_10_words = dict(sorted(wrds.iteritems(), key=operator.itemgetter(1), reverse=True)[:10]).keys()
            sec_avg += status_in.created_at.hour*3600+status_in.created_at.minute*60+status_in.created_at.second
            sec_avg /= len(statuses)
            m, s = divmod(sec_avg, 60)
            h, m = divmod(m, 60)
            updated_post = {
                "user_id": user.id,
                "twitter_handle": post['twitter_handle'],
                "friends_count": user.friends_count,
                "followers_count": user.followers_count,
                "average_post_time": "%d:%02d:%02d" % (h, m, s),
                "seconds_of_day_for_avg_time": sec_avg,
                "top_ten_used_words": top_10_words,
                "external_link_ratio": float(link_count)/float(len(statuses)),
                "tweets_analyzed": int(status_count),
                "preferred_app": unicode(most_used_source),
                "all_words_used_in_statuses": wrds,
                "all_apps_usage": apps,
                "analyzed_statuses": statuses,
                "num_analyzed": len(statuses)
            }
        else:
            statuses = [json.dumps(status_in)]
            apps = {}
            if status_in['source'] not in apps:
                apps[status_in['source']] = 1
            else:
                apps[status_in['source']] += 1

            wrds = {}
            split_text = re.findall('\w+ ', unicode(status_in.text))
            for word in split_text:
                if word not in wrds:
                    wrds[word] = 1
                else:
                    wrds[word] += 1
            most_used_source = dict(sorted(apps.iteritems(), key=operator.itemgetter(1), reverse=True)[:1]).keys()[0]
            top_10_words = dict(sorted(wrds.iteritems(), key=operator.itemgetter(1), reverse=True)[:10]).keys()
            sec_avg = status_in.created_at.hour*3600+status_in.created_at.minute*60+status_in.created_at.second
            m, s = divmod(sec_avg, 60)
            h, m = divmod(m, 60)
            updated_post = {
                "user_id": user.id,
                "twitter_handle": str(user.screen_name),
                "friends_count": user.friend_count,
                "followers_count": user.followers_count,
                "average_post_time": "%d:%02d:%02d" % (h, m, s),
                "seconds_of_day_for_avg_time": sec_avg,
                "top_ten_used_words": top_10_words,
                "external_link_ratio": float(extern_link_count),
                "tweets_analyzed": int(status_count),
                "preferred_app": unicode(most_used_source),
                "all_words_used_in_statuses": wrds,
                "all_apps_usage": apps,
                "analyzed_statuses": statuses,
                "num_analyzed": len(statuses)
            }

        posts.update({'twitter_handle': post['twitter_handle']}, {"$set": updated_post}, upsert=True)
        print(updated_post)
        print_status_stats(updated_post)


def process_status(status_in, user_account):
    user_handle = user_account.screen_name
    global extern_link_count, avg_time, words, sources, status_count, friends, followers, db, statuses_all
    statuses_all.append(dumps(status_in._json))
    if unicode(status_in.source) not in sources:
        sources[unicode(status_in.source)] = 1
    else:
        sources[unicode(status_in.source)] += 1
    split_text = re.findall('\w+ ', unicode(status_in.text))
    for word in split_text:
        if word not in words:
            words[word] = 1
        else:
            words[word] += 1


    m = re.search('((http|https):\/\/(?!www.twitter.com)[\w\.\/\-=?#]+)', status_in.text)
    if m and len(m.group(0)) > 0:
        extern_link_count += 1

    avg_time += status_in.created_at.hour*3600+status_in.created_at.minute*60+status_in.created_at.second

    status_count += 1.0

    most_used_source = dict(sorted(sources.iteritems(), key=operator.itemgetter(1), reverse=True)[:1]).keys()[0]
    top_10_words = dict(sorted(words.iteritems(), key=operator.itemgetter(1), reverse=True)[:10]).keys()
    avg_time /= status_count
    m, s = divmod(avg_time, 60)
    h, m = divmod(m, 60)
    friends = user_account.friends_count
    followers = user_account.followers_count
    post = {
        "user_id": user_account.id,
        "twitter_handle": str(user_handle),
        "friends_count": int(friends),
        "followers_count": int(followers),
        "average_post_time": "%d:%02d:%02d" % (h, m, s),
        "seconds_of_day_for_avg_time": avg_time,
        "top_ten_used_words": top_10_words,
        "external_link_ratio": float(extern_link_count)/float(len(statuses_all)),
        "tweets_analyzed": int(status_count),
        "preferred_app": unicode(most_used_source),
        "all_words_used_in_statuses": words,
        "all_apps_usage": sources,
        "analyzed_statuses": statuses_all,
        "num_analyzed": len(statuses_all)
    }
    if db:
        posts = db['posts']
        posts.update({'twitter_handle': user_handle}, {"$set": post}, upsert=True)


class TweetListener(StreamListener):
    def __init__(self):
        super(TweetListener, self).__init__()
        self.counter = 0

    def on_status(self, status):
        if status.author.screen_name in __user_list__ or  status.author.id in __user_list__:
            process_stream_status(status)
            self.counter += 1
        return

    def on_limit(self, track):
        print("Limit exceeded")
        return

    def on_error(self, status_code):
        print('Error:\t' + str(status_code) + "\n")
        logging.warning('Error:\t' + str(status_code) + "\n")# will print a message to the console
        return False

    def on_timeout(self):
        print("Sleeping for 60s\n")
        time.sleep(60)
        return


def main():
    # This handles Twitter authetification and the connection to Twitter Streaming API
    global api, db, extern_link_count, avg_time, status_count, sources, words, friends, followers, statuses_all

    user_accounts = []
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    client = pymongo.MongoClient("localhost", 27017)
    db = client.TwitterStatusStats
    threads = []
    ids = []
    for user in __user_list__:
        extern_link_count = 0
        avg_time = 0.0
        status_count = 0.0
        sources = {}
        words = {}
        statuses_all = []
        friends = 0
        followers = 0
        user_account = api.get_user(user)
        user_accounts.append(user_account)
        ids.append(str(user_account.id))
        statuses_past = tweepy.Cursor(api.user_timeline, id=user_account.screen_name).items(num_historical_tweets_to_analyze)
        t = None
        for status in statuses_past:
            process_status(status, user_account)
    for user in __user_list__:
        user_account = api.get_user(user)
        stat = db['posts'].find_one({'twitter_handle': str(user_account.screen_name)})
        if stat:
            print_status_stats(stat)

    stream = tweepy.Stream(auth=api.auth, listener=TweetListener())

    try:
        stream.filter(follow=ids, async=True)
    except:
        print "Unexpected Streaming error:", sys.exc_info()[0], sys.exc_info()
        stream.disconnect()

    for t in threads:
        t.join()
if __name__ == "__main__":
    main()
