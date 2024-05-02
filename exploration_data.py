from pymongo import MongoClient
from pprint import pprint
from datetime import datetime


# establish connection to MongoDB
client = MongoClient()

# database and collection
db = client["airlines"]
tweets_all = db["tweets_all"]

# note that this is as performed on the RAW data, which may well contain duplicates etc.

# total number of tweets
tweets_count = db.tweets_all.count_documents({})
print(tweets_count) # result: 6511146

# number of tweets with replies
reply_count = db.tweets_all.count_documents({"reply_count":{"gt":0}})
print(reply_count) # result: 0
# PROBLEM !!!! ALL TWEETS HAVE NON_REPLY_COUNT = 0

# number of tweets that are replies ...
# ... based on in_reply_to_status_id_str
reply_tweets_status = db.tweets_all.count_documents({"in_reply_to_status_id_str": {"$ne": None}})
print(reply_tweets_status) # result: 1893340

# ... based on in_reply_to_user_id_str
reply_tweets_user_id = db.tweets_all.count_documents({"in_reply_to_user_id_str": {"$ne": None}})
print(reply_tweets_user_id) # result: 2353210

# all languages
lang_list = db.tweets_all.distinct("lang")
print(lang_list)

lang_list = ['am', 'ar', 'bg', 'bn', 'ca', 'ckb', 'cs', 'cy', 'da', 'de', 'dv', 'el', 'en', 'es', 'et', 'eu',
             'fa', 'fi', 'fr', 'gu', 'hi', 'ht', 'hu', 'hy', 'in', 'is', 'it', 'iw', 'ja', 'ka', 'kn', 'ko',
             'lo', 'lt', 'lv', 'ml', 'mr', 'my', 'ne', 'nl', 'no', 'or', 'pa', 'pl', 'ps', 'pt', 'ro', 'ru',
             'sd', 'si', 'sl', 'sr', 'sv', 'ta', 'te', 'th', 'tl', 'tr', 'uk', 'und', 'ur', 'vi', 'zh']

# number of english tweets
en_tweets_count = db.tweets_all.count_documents({"lang":"en"})
print(en_tweets_count) # result: 4730503

# number of dutch tweets
nl_tweets_count = db.tweets_all.count_documents({"lang":"nl"})
print(nl_tweets_count) # result: 206641

# number of undefined language tweets
und_tweets_count = db.tweets_all.count_documents({"lang": "und"})
print(und_tweets_count) # result: 190928

# number of deleted tweets
deleted_tweets_count = db.tweets_all.distinct("delete")
print(len(deleted_tweets_count))  # result: 2180




# close connection
client.close()