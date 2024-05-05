from pymongo import MongoClient, UpdateOne
import time

def log_time(description, start_time):
    elapsed_time = time.time() - start_time
    print(f"{description}: {elapsed_time:.2f} seconds")

client = MongoClient()
db = client["airlines"]
old_collection = db["tweets_all"]

user_collection = db["users"]
tweet_with_user_collection = db["tweet_with_user_id"]

start_time = time.time()

# Batch upsert operations to increase efficiency since it is such a large dataset.
# Reducing number of read and write operations
user_operations = []

#Storing all unique users so we eliminate duplicate stored users.
unique_user_ids = set()

# If statement can be removed if data is cleaned so if tweet _id not none.
for tweet in old_collection.find():
    tweet_id = tweet.get("id")
    if tweet_id is None:
        continue

    user = tweet.get("user")
    user_id = None

    #If statement can removed later if data is cleaned.
    if user and "id" in user:
        user_id = user["id"]

        if user_id not in unique_user_ids:
            unique_user_ids.add(user_id)

            user["_id"] = user_id

            #Checking for uniuque user.
            user_operations.append(UpdateOne({"_id": user["_id"]}, {"$set": user}, upsert=True))

        tweet["user_id"] = user_id


        #Removing the original tweet object from the entire thing.
        del tweet["user"]

        #Maybe add removing the entire previous collection too. Can be done later, but no use in losing original database.

    #To directly insert into collection
    tweet_with_user_collection.insert_one(tweet)

if user_operations:
    start_time = time.time()

    #Batch operations
    user_collection.bulk_write(user_operations)
    log_time("Batch", start_time)

log_time("Processing", start_time)

print("Done")
