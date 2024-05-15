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


# finds the amount of tweets send per day || FUNCTION TAKES TOO LONG!!!
date_dict = {}
for tweet in tweets_all.find():
    try:
        date_string = tweet["created_at"]
        date_object = datetime.strptime(date_string, "%a %b %d %H:%M:%S %z %Y")
        date = (date_object.year, date_object.month, date_object.day)

        if date not in date_dict:
            date_dict[date] = 1
        else:
            date_dict[date] += 1
    except:
        continue

# prints full dictionary
print(date_dict)

# prints the date with the least amount of tweets (first day of the dataset)
print(min(date_dict.keys()))

# THE ONLY UNIQUE VALUE IS 0
quote_unique = db.tweets_all.distinct("quote_count")
print(quote_unique)

# THE ONLY UNIQUE VALUE IS 0
retweet_unique = db.tweets_all.distinct("retweet_count")
print(retweet_unique)

# THE ONLY UNIQUE VALUE IS 0
favorite_unique = db.tweets_all.distinct("favorite_count")
print(favorite_unique)

# THE ONLY UNIQUE VALUE IS 0
reply_unique = db.tweets_all.distinct("reply_count")
print(reply_unique)

# Both True and False
truncated_unique = db.tweets_all.distinct("truncated")
print(truncated_unique)

# Both True and False
quote_status_unique = db.tweets_all.distinct("is_quote_status")
print(quote_status_unique)

# Both True and False
quote_status_unique = db.tweets_all.distinct("possibly_sensitive")
print(quote_status_unique)

# every value is null
contributors_unique = db.tweets_all.distinct("contributors")
print(contributors_unique)


# close connection
client.close()