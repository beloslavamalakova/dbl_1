import json
from pymongo import MongoClient
import os
from timeit import default_timer as timer
from datetime import datetime
from pprint import pprint
import matplotlib.pyplot as plt

# establish connection to MongoDB
client = MongoClient()

# database and collection
db = client["airlines"]
tweets_collection = db["tweets_collection"]


# DATA EXPLORATION

# from load_data.py
error = {'data/airlines-1558611772040.json': 63, 'data/airlines-1558623303180.json': 120,
         'data/airlines-1559860041436.json': 63, 'data/airlines-1560138591670.json': 1,
         'data/airlines-1565894560588.json': 1, 'data/airlines-1569957146471.json': 1,
         'data/airlines-1570104381202.json': 1, 'data/airlines-1573229502947.json': 1,
         'data/airlines-1575313134067.json': 1, 'data/airlines-1583171908044.json': 1,
         'data/airlines-1583253051533.json': 5}

duplicates = 414685

non_tweet_objects = 2326


# total number of unique tweet objects
def doc_count():
    return db.tweets_collection.count_documents({})
# print(doc_count()) # result: 6094135


# create index for created_at_datetime
db.tweets_collection.create_index([("created_at_datetime", 1)])


# first tweet
def first_tweet():
    return db.tweets_collection.find({}, {"created_at_datetime":1}).sort("created_at_datetime")
# print(first_tweet()[0]["created_at_datetime"]) # result: 2019-05-22 12:20:00


# last tweet
def last_tweet():
    return db.tweets_collection.find({}, {"created_at_datetime": 1}).sort([("created_at_datetime", -1)])
# print(last_tweet()[0]["created_at_datetime"]) # result: 2020-03-30 18:43:16


# distribution over time
def distribution_over_time():
    dates = [date["created_at_datetime"] for date in db.tweets_collection.find({}, {"created_at_datetime":1, "_id":0})]

    # plot histogram
    plt.figure(figsize=(10,8))
    plt.hist(dates, bins=10, edgecolor="black")
    plt.xticks(rotation="vertical")

    # adding labels and title
    plt.xlabel('Date', size=14)
    plt.ylabel('Frequency', size=14)
    plt.title('Distribution of tweets over time', size=20, fontweight="bold")

    # show plot
    plt.savefig("plots/distribution_over_time.png")
    plt.show()
# distribution_over_time()


# create index for language
db.tweets_collection.create_index([("lang", 1)])


# distinct languages
def languages():
    return list(db.tweets_collection.distinct("lang"))
# print(languages())


# frequencies of languages
def frequencies_languages():
    tweet_count = 6094135

    pipeline = [
        {
            '$group': {
                '_id': {"language": "$lang"},
                'count': {"$sum": 1}
            }
        },
        {
            '$sort': {'count': -1}
        }
    ]
    # plot barchart
    color = ["cyan", "yellow", "blue", "orange", "grey", "green"]

    languages = list(db.tweets_collection.aggregate(pipeline))

    top_5 = {lang["_id"]["language"]: lang["count"] / tweet_count for lang in languages[:5]}
    top_5["other"] = sum([lang["count"] for lang in languages[5:]]) / tweet_count

    plt.figure(figsize=(6, 6))
    plt.bar(list(top_5.keys()), list(top_5.values()), color=color)

    # adding labels and title
    plt.xlabel('Language', size=12)
    plt.ylabel('Relative frequency', size=12)
    plt.title('Frequencies of tweet languages', size=16, fontweight="bold")

    # show plot
    plt.savefig("plots/frequencies_languages")
    plt.show()
# frequencies_languages()


# example undefined language tweet
def und_tweet():
    und_tweet = db.tweets_collection.find({"lang":"und"}, {"text":1, "lang":1})[2]
    return und_tweet
# print(und_tweet())


# number of retweets
def retweets():
    quote_tweet = db.tweets_collection.count_documents({"quoted_status": {"$exists": True}, "retweeted_status": {"$exists": False}})
    retweet_tweet = db.tweets_collection.count_documents({"retweeted_status": {"$exists": True}, "quoted_status": {"$exists": False}})
    both = db.tweets_collection.count_documents({"quoted_status": {"$exists": True}, "retweeted_status": {"$exists": True}})

    labels = ["retweet tweet", "quote tweet", "both"]
    values = [retweet_tweet, quote_tweet, both]

    # plot barchart
    plt.figure(figsize=(6, 6))
    plt.bar(labels, values, color=["blue", "orange", "green"])

    # adding labels and title
    plt.xlabel('Tweet type', size=12)
    plt.ylabel('Frequency', size=12)
    plt.title('Frequency of retweets', size=16, fontweight="bold")

    # show plot
    plt.savefig("plots/frequencies_retweets")
    plt.show()
# retweets()


# number of original tweets
def original_tweets():
    original_tweet = db.tweets_collection.count_documents({"quoted_status": {"$exists": False}, "retweeted_status": {"$exists": False}})
    return original_tweet
# print(original_tweets()) # result: 3042450


# create index of user mentions
db.tweets_collection.create_index("user_mentions.id")

# response rate of KLM
airlines = {"KLM": 56377143, "AirFrance": 106062176, "British_Airways": 18332190, "AmericanAir": 22536055,
            "Lufthansa": 124476322, "AirBerlin": 26223583, "AirBerlin assist": 2182373406,
            "easyJet": 38676903, "RyanAir": 1542862735, "SingaporeAir": 253340062, "Qantas": 218730857,
            "EtihadAirways": 45621423, "VirginAtlantic": 20626359}
# print(len(airlines))


# create index of user id
db.tweets_collection.create_index("user.id")


def klm_mention():
    return db.tweets_collection.count_documents({"entities.user_mentions.id": airlines["KLM"]})
# print(klm_mention) # result: 226887


# tweets from KLM
def klm_tweets():
    klm_total = db.tweets_collection.count_documents({"user.id": airlines["KLM"]})
    klm_original = db.tweets_collection.count_documents({"user.id": airlines["KLM"], "in_reply_to_status_id": None, "in_reply_to_user_id": None})
    klm_reply = db.tweets_collection.count_documents({"user.id": airlines["KLM"], "in_reply_to_status_id": {"$ne": None}, "in_reply_to_user_id": {"$ne": None}})
    klm_reply_users = db.tweets_collection.distinct("in_reply_to_user_id", {"user.id": airlines["KLM"],"in_reply_to_status_id": {"$ne": None}, "in_reply_to_user_id": {"$ne": None}})

    return klm_total, klm_original, klm_reply, (klm_reply / klm_total), (klm_total - klm_reply - klm_original), len(klm_reply_users)
# print(klm_tweets()) # result: (34251, 155, 33993, 0.9924673732153806, 103, 17094)


def weekly_distribution():
    dates = [doc["created_at_datetime"] for doc in db.tweets_collection.find({}, {"created_at_datetime": 1, "_id": 0})]

    weekly_distribution = {'Monday': 0, 'Tuesday': 0, 'Wednesday': 0, 'Thursday': 0, 'Friday': 0, 'Saturday': 0,
                           'Sunday': 0}
    for date in dates:
        day_of_week = datetime.strftime(date, "%A")
        weekly_distribution[day_of_week] += 1

    plt.bar(weekly_distribution.keys(), weekly_distribution.values())
    plt.title('Weekly Distribution of Tweets')
    plt.tick_params(axis='x', labelsize=8)
    plt.xlabel('Day of the Week', size=9)
    plt.ylabel('Count')
    plt.show()
# weekly_distribution()


# close connection
client.close()
