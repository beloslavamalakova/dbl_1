# prerequisites
import json
from pymongo import MongoClient
import os
from pprint import pprint

# establish connection to MongoDB
client = MongoClient()

# create database named "airlines"
airlines = client["airlines"]
# create collection named "tweets_all"
tweets_all = airlines["tweets_all"]

# store erroneous tweet objects
error = {} # key:value => json_file: nr of docs with error

# load "data" into MongoDB
def load_airlines(path: str) -> None:
    """
    Loads a json airlines file into a database collection.
    :param path: path to the json airlines file
    :return: None.
    """
    with open(path) as file:
        for doc in file:
            try:
                data = json.loads(doc)
                tweets_all.insert_one(data)
            except:
                error[path] = error.get(path, 0) + 1

def load_data():
    """ Loads data into a database collection.
    """
    for json in os.listdir("data"): # iterate through all airlines files in data
        load_airlines("data/"+json)

# close connection
client.close()