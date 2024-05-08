import ujson
from pymongo import MongoClient
import os
from datetime import datetime
from multiprocessing import Pool
from timeit import default_timer as timer
import pymongo

# Create or get the database and collection
client = MongoClient()
airlines = client["airlines_turbo"]
tweets_select_att = airlines["tweets_att"]

# Initialize error tracking structures
error = {}  # Tracks errors by file
duplicates = []  # List of duplicate ids
non_tweet_objects = []  # List of non-tweet objects

def load_airlines(path: str):
    """
    Loads JSON data from a specified path to the MongoDB in batches using ujson for faster parsing.
    """
    batch_size = 1000  # Adjust batch size based on your system's capability
    batch = []

    with open(path) as file:
        for doc in file:
            try:
                data = ujson.loads(doc)
                if "id" not in data:
                    non_tweet_objects.append(data)
                    continue

                data["_id"] = data["id"]
                data["created_at_datetime"] = datetime.strptime(data["created_at"], "%a %b %d %H:%M:%S %z %Y")

                # Replicate all attributes
                data_att = {
                    "_id": data["_id"],
                    "created_at": data["created_at"],
                    "created_at_datetime": data["created_at_datetime"],
                    "text": data["extended_tweet"]["full_text"] if data.get("truncated", False) else data["text"],
                    "in_reply_to_status_id": data["in_reply_to_status_id"],
                    "in_reply_to_user_id": data["in_reply_to_user_id"],
                    "in_reply_to_screen_name": data["in_reply_to_screen_name"],
                    "is_quote_status": data["is_quote_status"],
                    "possibly_sensitive": data.get("possibly_sensitive"),
                    "filter_level": data["filter_level"],
                    "lang": data["lang"],
                    "timestamp_ms": data["timestamp_ms"],
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
                        "statuses_count": data["user"]["statuses_count"],
                        "created_at": data["user"]["created_at"]
                    },
                    "entities": data["entities"]
                }

                if data.get("is_quote_status", False):
                    if "quoted_status" in data:
                        data_att["quoted_status"] = data["quoted_status"]
                        data_att["quoted_status_id"] = data["quoted_status_id"]

                if "retweeted_status" in data:
                    data_att["retweeted_status"] = data["retweeted_status"]

                batch.append(data_att)
                if len(batch) >= batch_size:
                    insert_batch(batch)
                    batch = []

            except ujson.JSONDecodeError:
                error[path] = error.get(path, 0) + 1

    if batch:
        insert_batch(batch)

def insert_batch(batch):
    """
    Insert a batch of documents into MongoDB, handling duplicates.
    """
    try:
        tweets_select_att.insert_many(batch, ordered=False)
    except pymongo.errors.BulkWriteError as e:
        duplicates.extend([err['op']['_id'] for err in e.details['writeErrors'] if err['code'] == 11000])

def process_files(file_path):
    """
    Function to process each file path. Designed to be used with multiprocessing.
    """
    print(f"Processing {file_path}")
    load_airlines(file_path)

def load_data():
    files = [os.path.join("data", filename) for filename in os.listdir("data")]
    with Pool(processes=os.cpu_count()) as pool:
        pool.map(process_files, files)

def main():
    start = timer()
    load_data()
    end = timer()
    print(f"Time taken: {end - start} seconds")
    print("Errors:", error)
    print("Duplicates:", len(duplicates))
    print("Non-tweet objects:", len(non_tweet_objects))
    client.close()

if __name__ == '__main__':
    main()
