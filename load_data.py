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
# create collection named "tweets"
tweets = airlines["tweets"]

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

            # some documents are duplicates
            try:
                tweets.insert_one(data)
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

After loading all the data, make sure to comment the code again, since loading the data into 
the database is a one-time procedure. 
"""

# # Uncomment this block of code and then run the file once!
start = timer()
load_data()
end = timer()

print(end - start)
print(error)
print(len(duplicates))
print(len(non_tweet_objects))


# close connection
client.close()