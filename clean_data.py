from pymongo import MongoClient
import re

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['airlines']
collection = db['tweets_att']

# REMOVE LINKS, EMOJIS AND SPECIAL CHARACTERS FROM TEXT

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


def update_tweets():
    """ Update each tweet's text in the database by removing links, emojis, and special characters. """
    # Find all tweets
    tweets = collection.find({})

    # Update each tweet
    for tweet in tweets:
        cleaned_text = clean_text(tweet['text'])
        # Update the document with cleaned text
        collection.update_one({'_id': tweet['_id']}, {'$set': {'cleaned_text': cleaned_text}})


def dry_run_update_tweets_test():
    """ Prints the before and after texts for each tweet without updating the database. """
    tweets = collection.find({})  # The amount of tweets that the task is performed on can be limited

    for tweet in tweets:
        original_text = tweet['text']
        cleaned_text = clean_text(original_text)
        print("Original Text: ", original_text)
        print("Cleaned Text: ", cleaned_text)
        print("-------")


# REMOVE RETWEETS

def remove_tweets(collection):
    query = {
        "retweeted_status": {"$exists": True},  # Check if retweeted_status exists
        "is_quote_status": False               # Check if is_quote_status is False
    }
    result = collection.delete_many(query)
    return result.deleted_count