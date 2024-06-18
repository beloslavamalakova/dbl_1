from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient()
db = client['airlines']
collection = db['tweets_collection']

# Function to remove retweets
def remove_tweets(collection):
    query = {
        "retweeted_status": {"$exists": True},  # Check if retweeted_status exists
    }
    result = collection.delete_many(query)
    return result.deleted_count


# Remove retweets from the collection
deleted_count = remove_tweets(collection)
print(f"Number of retweets removed: {deleted_count}")
