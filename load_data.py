# prerequisites
import json
from pymongo import MongoClient
import os
from timeit import default_timer as timer

"""
The following code assumes that the data is stored in the same directory load_data.py. 
That is, a folder called "data" is stored in the same directory as the the current file and 
"data" contains all the json airlines files. 
"""

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
    count = 1
    for json in os.listdir("data"): # iterate through all airlines files in data
        print(count)
        count += 1

        load_airlines("data/"+json)

"""
Uncomment the code below in order to load the data. Make sure that the 
database either does not exist yet or is empty at the start to avoid duplication errors. 

The counter is simply a visual to check how many json files have been inserted up to that point. 
There are a total of 567 airlines files, so the counter should run until 567 once it is finished. 

Furthermore, the timer measures the total time (in sec) it took to load the data. 

The error dictionary shows the amount of documents in a respective json file that were erroneous
and thus could not be loaded into the database. 

After loading all the data, make sure to comment the code again, since loading the data into 
the database is a one-time procedure. 

Testing

"""

# # Uncomment this block of code and then run the file once!
# start = timer()
# load_data()
# end = timer()
#
# print(end - start)
# print(error)

# close connection
client.close()