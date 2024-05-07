# prerequisites
import json
from pymongo import MongoClient
import os
from timeit import default_timer as timer
from datetime import datetime

"""
The following code assumes that the data is stored in the same directory load_data.py. 
That is, a folder called "data" is stored in the same directory as the the current file and 
"data" contains all the json airlines files. Again, the "data" folder should contain ONLY 
the json airlines files. 
"""

# establish connection to MongoDB
client = MongoClient()

# create database named "airlines"
airlines = client["airlines"]
# create collection named "tweets_select_att"
tweets_select_att = airlines["tweets_att"]

# store erroneous tweet objects
error = {}  # key:value => json_file: nr of docs with error

# store tweet id of duplicates
duplicates = []

# store non-tweet objects
non_tweet_objects = []

# load "data" into MongoDB

def load_airlines(path: str) -> None:
    """
    Loads a json airlines file into a database collection.
    :param path: path to the json airlines file
    :return: None.
    """
    with open(path) as file:
        for doc in file:
            # some documents are erroneous
            try:
                data = json.loads(doc)
            except:
                error[path] = error.get(path, 0) + 1
                continue

            # some documents do not have id as immediate field (e.g. delete objects)
            try:
                # assign tweet id as id for document
                data["_id"] = data["id"]

                # transform date into a datetime.datetime object
                date_str = data["created_at"]
                data["created_at_datetime"] = datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")
            except:
                non_tweet_objects.append(data)
                continue

            # selecting on attributes
            data_att = {}
            data_att["_id"] = data["_id"]
            data_att["created_at"] = data["created_at"]
            data_att["created_at_datetime"] = data["created_at_datetime"]

            # if truncated, full text appears in extended tweets
            if data["truncated"] == True:
                data_att["text"] = data["extended_tweet"]["full_text"]
            else:
                data_att["text"] = data["text"]

            data_att["in_reply_to_status_id"] = data["in_reply_to_status_id"]
            data_att["in_reply_to_user_id"] = data["in_reply_to_user_id"]
            data_att["in_reply_to_screen_name"] = data["in_reply_to_screen_name"]
            data_att["is_quote_status"] = data["is_quote_status"]
            data_att["entities"] = data["entities"]

            # not all documents have the possibly_sensitive field
            if "possibly_sensitive" in data:
                data_att["possibly_sensitive"] = data["possibly_sensitive"]

            data_att["filter_level"] = data["filter_level"]
            data_att["lang"] = data["lang"]
            data_att["timestamp_ms"] = data["timestamp_ms"]
            data_att["is_quote_status"] = data["is_quote_status"]

            # if quote, additional quote related fields may surface
            if data["is_quote_status"] == True and "quoted_status" in data:
                data_att["quoted_status"] = data["quoted_status"]
                data_att["quoted_status_id"] = data["quoted_status_id"]

            # if retweet, additional retweeted_status field surfaces
            if "retweeted_status" in data:
                data_att["retweeted_status"] = data["retweeted_status"]

            # user object
            data_att["user"] = {}
            data_att["user"]["id"] = data["user"]["id"]
            data_att["user"]["name"] = data["user"]["name"]
            data_att["user"]["screen_name"] = data["user"]["screen_name"]
            data_att["user"]["location"] = data["user"]["location"]
            data_att["user"]["protected"] = data["user"]["protected"]
            data_att["user"]["verified"] = data["user"]["verified"]
            data_att["user"]["followers_count"] = data["user"]["followers_count"]
            data_att["user"]["friends_count"] = data["user"]["friends_count"]
            data_att["user"]["listed_count"] = data["user"]["listed_count"]
            data_att["user"]["favourites_count"] = data["user"]["favourites_count"]
            data_att["user"]["statuses_count"] = data["user"]["statuses_count"]
            data_att["user"]["created_at"] = data["user"]["created_at"]

            data_att["entities"] = data["entities"]

            # some documents are duplicates
            try:
                tweets_select_att.insert_one(data_att)
            except:
                duplicates.append(data["_id"])

def load_data():
    """ Loads data into a database collection.
    """
    count = 1
    for json in os.listdir("data"):  # iterate through all airlines files in data
        print(count)
        count += 1

        load_airlines("data/" + json)

"""
Uncomment the code below in order to load the data. Make sure that the 
database either does not exist yet or is empty at the start to avoid duplication errors. In case 
one is not able to load all the data in one run, drop the database and restart. 

The counter is simply a visual to check how many json files have been inserted up to that point. 
There are a total of 567 airlines files, so the counter should run until 567 once it is finished. 

Furthermore, the timer measures the total time (in sec) it took to load the data. 

The error dictionary shows the amount of documents in a respective json file that were erroneous
and thus could not be loaded into the database. 

Duplicates is a list of duplicate documents. We have set the indexes to be that of the tweet id. 
Since indexes must be unique, if a tweet document has the same index as a tweet object 
that already exists in the database, it will be added to the duplicates list. 

Lastly, we have a non_tweet_objects variable, which is a list of documents that are NOT tweet objects. 
For this reason, we have chosen to ommit them from the database, since we will not be using them for
sentiment analysis. 

After loading all the data, make sure to comment the code again, since loading the data into 
the database is a one-time procedure. 
"""

# # Uncomment this block of code and then run the file once!
# start = timer()
# load_data()
# end = timer()
#
# print(end - start)
# print(error)
# print(len(duplicates))
# print(len(non_tweet_objects))

# close connection
client.close()