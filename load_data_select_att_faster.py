import ujson
from pymongo import MongoClient
import os
from datetime import datetime
from multiprocessing import Pool, Manager
from timeit import default_timer as timer
import logging
from typing import List, Dict
import pymongo

#Changing data design after currently takes:
logging.basicConfig(filename='data_processing.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

#Change this varialbe if yuo want to create a newly named database.
#######################################################################
name_database = "airlines_fixed_attttttttrrrr_5"

#No need to adjust these when creating a collection.
name_collection = "tweets_collection"
tweets_collection_name = "tweets"
quoted_tweets_collection_name = "quoted_tweets"
users_collection_name = "users"

#Connection with client & Connecting database
client = MongoClient()
airlines = client[name_database]
tweets_select_att = airlines[name_collection]

#Database Design Creation, only uncomment if you are also updating database design.
tweets = airlines[tweets_collection_name]
quoted_tweets = airlines[quoted_tweets_collection_name]
users = airlines[users_collection_name]

def load_airlines(path: str, duplicates: List[str], non_tweet_objects: List[Dict[str, str]], error: Dict[str, int]) -> None:
    """
    Processes and loads airline tweet data from data folder.
    Read tweet data line by line and filters attributes, and batches them for insertion.

    Args:
        path (str): The path to the folder containing files to import.
        duplicates (list): A list to store duplicate tweets.
        non_tweet_objects (list): A list to store non-tweet objects.
        error (dict): A dictionary to track errors.

    Variables:
        path (str): Path to the folder containing files.
        duplicates (list): A list to store duplicate tweet IDs.
        non_tweet_objects (list): A list to store non-tweet objects.
        error (dict): A dictionary to track errors occurred during processing.
        batch_size (int): The size of each batch for insertion into MongoDB.
        batch (list): A list to store tweet data batches.
        processed_count (int): The count of processed tweets.
        file_errors (int): The count of JSON decode errors encountered.
        data (dict): Temporary variable to hold tweet data while processing.

    Performance:
        Without database design it runs in 2.54 min on my system
        Duplicates 414685
        Non-tweet Objects: 2326
        Adjust batch_size to system memory and overall performance of system.
        On a system with 64GB RAM, 2000 batch_size is recommended by me :p
        On a system with 32GB RAM or lower, 1000 batch_size can be better.
    """

    print(f"Processing: {path}")

    # Adjust this number as mentioned in docstring.
    # Amount of tweets to process before insertion.
    batch_size = 2000
    batch = []

    #Processing Errors:
    processed_count = 0
    file_errors = 0

    with open(path) as file:
        for doc in file:
            try:
                data = ujson.loads(doc)

                #Check for non-tweet objects
                if "id" not in data:
                    non_tweet_objects.append(data)
                    continue

                #Add extra id attribute.
                data["_id"] = data["id"]

                #Parsing to date time
                data["created_at_datetime"] = datetime.strptime(data["created_at"], "%a %b %d %H:%M:%S %z %Y")

                #Extracting for batch prep
                data_att = extract_tweet_attributes(data)

                #Adding to batch
                batch.append(data_att)
                processed_count += 1

                #Check if batch size reached
                if len(batch) >= batch_size:
                    insert_batch(batch, path, duplicates)
                    batch = []

            #Logging error when occured.
            except ujson.JSONDecodeError as e:
                file_errors += 1
                logging.error(f"Decode error in file {path}: {e}")
                continue

    #Inserting tweets in batch
    if batch:
        insert_batch(batch, path, duplicates)

    #Log errors
    if file_errors > 0:
        error[path] = file_errors
        logging.error(f"File {path} had {file_errors} JSON decode errors")

    logging.info(f"Finished processing file {path}: {processed_count} records processed")


def extract_tweet_attributes(data: Dict[str, any]) -> Dict[str, any]:
    """
    Extract the attributes we want to filter from the tweets.
    """

    if data.get("truncated", False):
        text = data["extended_tweet"]["full_text"]
        entities = data["extended_tweet"]["entities"]
    else:
        text = data["text"]
        entities = data["entities"]

    filtered_entities = {
        "hashtags": entities.get("hashtags", []),
        "user_mentions": entities.get("user_mentions", [])
    }

    attributes = {
        "_id": data["_id"],
        "id": data["id"],
        "created_at_datetime": data["created_at_datetime"],
        "text": text,
        "entities": filtered_entities,
        "in_reply_to_status_id": data["in_reply_to_status_id"],
        "in_reply_to_user_id": data["in_reply_to_user_id"],
        "in_reply_to_screen_name": data["in_reply_to_screen_name"],
        "is_quote_status": data["is_quote_status"],
        "filter_level": data["filter_level"],
        "lang": data["lang"],
        "user": {
            "id": data["user"]["id"],
            "name": data["user"]["name"],
            "screen_name": data["user"]["screen_name"],
            "location": data["user"]["location"],
            "protected": data["user"]["protected"],
            "verified": data["user"]["verified"],
            "followers_count": data["user"]["followers_count"],
            "friends_count": data["user"]["friends_count"],
            "listed_count": data["user"]["listed_count"],
            "favourites_count": data["user"]["favourites_count"],
            "created_at": data["user"]["created_at"]
        },
        "possibly_sensitive": data.get("possibly_sensitive")
    }

    if data.get("is_quote_status", False) and "quoted_status" in data:
        quoted_status = data["quoted_status"]

        quoted_text = quoted_status["text"]
        quoted_entities = quoted_status["entities"]

        quoted_filtered_entities = {
            "hashtags": quoted_entities.get("hashtags", []),
            "user_mentions": quoted_entities.get("user_mentions", [])
        }

        attributes["quoted_status"] = {
            "id": quoted_status["id"],
            "created_at": quoted_status["created_at"],
            "text": quoted_text,
            "entities": quoted_filtered_entities,
            "in_reply_to_status_id": quoted_status["in_reply_to_status_id"],
            "in_reply_to_user_id": quoted_status["in_reply_to_user_id"],
            "in_reply_to_screen_name": quoted_status["in_reply_to_screen_name"],
            "is_quote_status": quoted_status["is_quote_status"],
            "filter_level": quoted_status.get("filter_level"),
            "lang": quoted_status.get("lang"),
            "user": {
                "id": quoted_status["user"]["id"],
                "name": quoted_status["user"].get("name"),
                "screen_name": quoted_status["user"]["screen_name"],
                "location": quoted_status["user"].get("location"),
                "protected": quoted_status["user"]["protected"],
                "verified": quoted_status["user"]["verified"],
                "followers_count": quoted_status["user"]["followers_count"],
                "friends_count": quoted_status["user"]["friends_count"],
                "listed_count": quoted_status["user"]["listed_count"],
                "favourites_count": quoted_status["user"]["favourites_count"],
                "created_at": quoted_status["user"]["created_at"]
            },
            "possibly_sensitive": quoted_status.get("possibly_sensitive")
        }

    if "retweeted_status" in data:
        retweeted_status = data["retweeted_status"]
        if retweeted_status.get("truncated", False):
            retweeted_text = retweeted_status["extended_tweet"]["full_text"]
            retweeted_entities = retweeted_status["extended_tweet"]["entities"]
        else:
            retweeted_text = retweeted_status["text"]
            retweeted_entities = retweeted_status["entities"]

        retweeted_filtered_entities = {
            "hashtags": retweeted_entities.get("hashtags", []),
            "user_mentions": retweeted_entities.get("user_mentions", [])
        }

        attributes["retweeted_status"] = {
            "id": retweeted_status["id"],
            "created_at": retweeted_status["created_at"],
            "text": retweeted_text,
            "entities": retweeted_filtered_entities,
            "user": {
                "id": retweeted_status["user"]["id"],
                "screen_name": retweeted_status["user"]["screen_name"]
            }
        }

    return attributes

def insert_batch(batch: List[dict], path: str, duplicates: List[str]) -> None:
    """
    Inserts a batch of tweets into the MongoDB collection (Used in load).

    """
    try:
        #Insert into mongodb collection, ordered false because we don't care about order and it gives better
        #performance.
        tweets_select_att.insert_many(batch, ordered=False)


    except pymongo.errors.BulkWriteError as e:

        #Log error in bulk write
        logging.error(f"Bulk write error for file {path}: {e.details}")

        #Loop to identify dupl.
        for err in e.details['writeErrors']:
            if err['code'] == 11000:
                #Adding to dupl list.
                duplicates.append(err['op']['_id'])

# def update_database_design(name_collection):
#     """
#     Restructures the existing MongoDB collection
#     """
#     global airlines, tweets, quoted_tweets, users
#
#     #Create indexes for new coll to improve performance
#     tweets.create_index([("_id", pymongo.ASCENDING)])
#     quoted_tweets.create_index([("_id", pymongo.ASCENDING)])
#     users.create_index([("_id", pymongo.ASCENDING)])
#
#     manager = Manager()
#     user_ids = manager.list()
#     tweet_updates = manager.list()
#     quoted_tweet_ids = manager.list()
#
#     processed_count = 0
#     print_frequency = 10000
#
#     collection_data_full = airlines[name_collection].find({})
#
#     for data in collection_data_full:
#         user_data = {
#             "_id": data["user"]["id"],
#             "name": data["user"]["name"],
#             "screen_name": data["user"]["screen_name"],
#             "location": data["user"]["location"],
#             "protected": data["user"]["protected"],
#             "verified": data["user"]["verified"],
#             "followers_count": data["user"]["followers_count"],
#             "friends_count": data["user"]["friends_count"],
#             "listed_count": data["user"]["listed_count"],
#             "favourites_count": data["user"]["favourites_count"],
#             "statuses_count": data["user"]["statuses_count"],
#             "created_at": data["user"]["created_at"]
#         }
#         user_ids.append(user_data)
#
#         tweet_data = {
#             "_id": data["_id"],
#             "id": data["id"],
#             "created_at": data["created_at"],
#             "text": data.get("extended_tweet", {}).get("full_text", data.get("text", "")),
#             "in_reply_to_status_id": data.get("in_reply_to_status_id"),
#             "in_reply_to_user_id": data.get("in_reply_to_user_id"),
#             "in_reply_to_screen_name": data.get("in_reply_to_screen_name"),
#             "is_quote_status": data.get("is_quote_status"),
#             "entities": data.get("entities"),
#             "filter_level": data.get("filter_level"),
#             "lang": data.get("lang"),
#             "possibly_sensitive": data.get("possibly_sensitive", False)
#         }
#         tweet_updates.append(tweet_data)
#
#         if 'quoted_status' in data:
#
#             quoted_tweet_data = {
#                 "_id": data["quoted_status"]["id"],
#                 #IDK WHAT AATTRR???
#             }
#             quoted_tweet_ids.append(quoted_tweet_data)
#
#         processed_count += 1
#         if processed_count % print_frequency == 0:
#             print(f"Adjusted Design for {processed_count} tweets.")
#
#     #Using multiprocessing to update strucutred data in prallel
#     with Pool(processes=os.cpu_count()) as pool:
#         pool.map(update_users_collection, user_ids)
#         pool.map(update_tweets_collection, tweet_updates)
#         pool.map(update_quoted_tweets_collection, quoted_tweet_ids)
#
#     print("Data Design Change Done")
#
# def update_users_collection(user_data):
#     user_data["_id"] = user_data["id"]
#
#     #Upserting the user data into users collection, if document with same id exists
#     #If no doc exists a new one is created
#     users.replace_one({"_id": user_data["_id"]}, user_data, upsert=True)
#
# def update_tweets_collection(tweet_data):
#     tweets.replace_one({"_id": tweet_data["_id"]}, tweet_data, upsert=True)
#
# def update_quoted_tweets_collection(quoted_tweet_data):
#     quoted_tweet_data["_id"] = quoted_tweet_data["id"]
#     quoted_tweets.replace_one({"_id": quoted_tweet_data["_id"]}, quoted_tweet_data, upsert=True)

def load_data():
    """
    Parallel processing of tweet data files, handling duplicates, non-tweets,
    and errors across multiple processes.
    """
    manager = Manager()
    duplicates = manager.list()
    non_tweet_objects = manager.list()
    error = manager.dict()
    files = [os.path.join("data", filename) for filename in os.listdir("data")]
    with Pool(processes=os.cpu_count()) as pool:
        pool.starmap(load_airlines, [(file, duplicates, non_tweet_objects, error) for file in files])

    #Comment this out if you don't want to update the database design.
    #update_database_design(name_collection)

    return duplicates, non_tweet_objects, error

def main():
    """
    Main execution function to process tweet data.
    This function is intended to be called when the script is run directly.
    """
    start = timer()
    duplicates, non_tweet_objects, error = load_data()
    end = timer()
    time_taken = (end - start) / 60
    print(f"Time taken: {time_taken:.2f} minutes")
    print("Processed count and Errors:", error)
    print("Duplicates:", len(duplicates))
    print("Non-tweet objects:", len(non_tweet_objects))
    client.close()

if __name__ == '__main__':
    main()

