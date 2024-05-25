from pymongo import MongoClient
import re

# Connect to MongoDB
client = MongoClient()
db = client['airlines']
collection = db['tweets_collection']

# Function to clean text
def clean_text(text):
    """ Cleans text by removing URLs, emojis, special characters (excluding @ and #), converting to lowercase,
        and trimming extra spaces. """
    # Remove URLs
    text = re.sub(r'http\S+', '', text)  # This regex removes any http or https URLs

    # Remove Emojis
    text = re.sub(r'[\U00010000-\U0010ffff]', '', text)  # Regex to remove emojis

    # Remove Special Characters, but keep @, #, letters, numbers, spaces, question marks, and exclamation points
    text = re.sub(r'[^\w\s@#?!]', '', text)

    # Convert text to lowercase
    text = text.lower()

    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()

    return text

# Function to update tweets
def update_tweets():
    # Find all tweets
    tweets = collection.find({})

    # Update each tweet
    for tweet in tweets:
        original_text = tweet['text']  # Directly access text field
        cleaned_text = clean_text(original_text)
        # Update the document with cleaned text
        collection.update_one({'_id': tweet['_id']}, {'$set': {'cleaned_text': cleaned_text}})

def dry_run_update_tweets_test():
    # The amount of tweets that the task is performed on can be limited
    tweets = collection.find({})

    for tweet in tweets:
        original_text = tweet['text']  # Directly access text field
        cleaned_text = clean_text(original_text)
        print("Original Text: ", original_text)
        print("Cleaned Text: ", cleaned_text)
        print("-------")

# Function to remove retweets
def remove_tweets(collection):
    query = {
        "retweeted_status": {"$exists": True},  # Check if retweeted_status exists
    }
    result = collection.delete_many(query)
    return result.deleted_count

# Perform a dry run to see the effects of cleaning without updating the database
dry_run_update_tweets_test()

# Once satisfied, update the tweets in the database
update_tweets()

# Remove retweets from the collection
deleted_count = remove_tweets(collection)
print(f"Number of retweets removed: {deleted_count}")
