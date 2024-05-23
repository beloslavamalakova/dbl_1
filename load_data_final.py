import ujson
from pymongo import MongoClient
import os
from datetime import datetime
from multiprocessing import Pool, Manager
from timeit import default_timer as timer
import logging
from typing import List, Dict
import pymongo

logging.basicConfig(filename='data_processing.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Change the name of the database in mongodb by changing name_database
name_database = "airlines"

name_collection = "tweets_collection"

client = MongoClient()
airlines = client[name_database]
tweets_select_att = airlines[name_collection]


def load_airlines(path: str, duplicates: List[str], non_tweet_objects: List[Dict[str, str]], error: Dict[str, int]):
    """
    Without database design it runs in 2.54 min on my system
    Duplicates 414685
    Non-tweet Objects: 2326
    Adjust batch_size to system memory and overall performance of system.
    """

    print(f"Processing: {path}")

    # Adjust the batch_size here:
    batch_size = 2000
    batch = []

    processed_count = 0
    file_errors = 0

    with open(path) as file:
        for doc in file:
            try:
                data = ujson.loads(doc)

                if "id" not in data:
                    non_tweet_objects.append(data)
                    continue

                data["_id"] = data["id"]
                data["created_at_datetime"] = datetime.strptime(data["created_at"], "%a %b %d %H:%M:%S %z %Y")
                data_att = extract_tweet_attributes(data)
                batch.append(data_att)
                processed_count += 1

                if len(batch) >= batch_size:
                    insert_batch(batch, path, duplicates)
                    batch = []

            except ujson.JSONDecodeError as e:
                file_errors += 1
                logging.error(f"Decode error in file {path}: {e}")
                continue

    if batch:
        insert_batch(batch, path, duplicates)

    if file_errors > 0:
        error[path] = file_errors
        logging.error(f"File {path} had {file_errors} JSON decode errors")

    logging.info(f"Finished processing file {path}: {processed_count} records processed")


def extract_tweet_attributes(data: Dict[str, any]):
    """
    Extract the attributes we want to filter from the tweets.
    """

    if data["truncated"]:
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


def insert_batch(batch: List[dict], path: str, duplicates: List[str]):
    """
    Inserts a batch of tweets into the MongoDB collection (Used in load).
    """
    try:
        # Insert into mongodb collection, ordered false because we don't care about order and it gives better
        # performance.
        tweets_select_att.insert_many(batch, ordered=False)

    except pymongo.errors.BulkWriteError as e:

        # Log error in bulk write
        logging.error(f"Bulk write error for file {path}: {e.details}")

        # Loop to identify dupl.
        for err in e.details['writeErrors']:
            if err['code'] == 11000:
                # Adding to dupl list.
                duplicates.append(err['op']['_id'])


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
