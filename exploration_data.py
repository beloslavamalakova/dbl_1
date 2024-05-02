from pymongo import MongoClient

# establish connection to MongoDB
client = MongoClient()

db = client["airlines"]
tweets_all = db["tweets_all"]

# total number of tweets
tweets_count = db.tweets_all.count_documents({})
print(tweets_count) # result: 6511146

# number of tweets with no reply
non_reply_count = db.tweets_all.count_documents({"reply_count":0})
print(non_reply_count) # result: 6508820
# PROBLEM !!!! ALL TWEETS HAVE NON_REPLY_COUNT = 0

# all languages
lang_list = db.tweets_all.distinct("lang")
print(lang_list)

lang_list = ['am', 'ar', 'bg', 'bn', 'ca', 'ckb', 'cs', 'cy', 'da', 'de', 'dv', 'el', 'en', 'es', 'et', 'eu',
             'fa', 'fi', 'fr', 'gu', 'hi', 'ht', 'hu', 'hy', 'in', 'is', 'it', 'iw', 'ja', 'ka', 'kn', 'ko',
             'lo', 'lt', 'lv', 'ml', 'mr', 'my', 'ne', 'nl', 'no', 'or', 'pa', 'pl', 'ps', 'pt', 'ro', 'ru',
             'sd', 'si', 'sl', 'sr', 'sv', 'ta', 'te', 'th', 'tl', 'tr', 'uk', 'und', 'ur', 'vi', 'zh']

# number of tweets in english
en_tweets_count = db.tweets_all.count_documents({"lang":"en"})
print(en_tweets_count) # result: 4730503

# number of tweets in dutch
nl_tweets_count = db.tweets_all.count_documents({"lang":"nl"})
print(nl_tweets_count) # result: 206641

# close connection
client.close()