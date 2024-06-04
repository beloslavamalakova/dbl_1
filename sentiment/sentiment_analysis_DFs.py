import ssl
import nltk
import certifi
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd
import matplotlib.pyplot as plt
import re
import numpy as np


# Function to clean tweet text without removing special characters
def clean_tweet_text(text):
    # Remove URLs
    text = re.sub(r"http\S+|www\S+|https\S+", '', text, flags=re.MULTILINE)
    return text

# Function cleaning df of conv with length 1, filtering English, cleaning text
def load_clean_conv(df_path, airline_id):
    # load df conv
    df_conversations = pd.read_csv(df_path)
    df_conversations = df_conversations.loc[df_conversations["lang"] == "en"]

    # clean conv starting with airline
    df_conversations = df_conversations.set_index(["conv_starter"], drop=False)
    conv_filter = df_conversations[
        (df_conversations["_id"] == df_conversations["conv_starter"]) & (df_conversations["user"] == airline_id)]
    df_conversations.drop(conv_filter.index, inplace=True)
    df_conversations = df_conversations.reset_index(drop=True)

    # remove conv with length 1
    df_conversations = df_conversations.groupby('conv_starter').filter(lambda x: len(x) > 1)

    # Apply cleaning function to the text column
    df_conversations['cleaned_text'] = df_conversations['text'].apply(clean_tweet_text)
    print("DF cleaned")
    return df_conversations

# Function to get sentiment score
def get_sentiment_score(text):
    return analyser.polarity_scores(text)['compound']

# Function to categorize sentiment based on score
def categorize_sentiment(score):
    if score > 0.05:
        return "Positive"
    elif score < -0.05:
        return "Negative"
    else:
        return "Neutral"

def sentiment_analysis(df_conversations):
    # Apply sentiment analysis conditionally based on language
    df_conversations['sentiment_score'] = df_conversations.apply(lambda row: get_sentiment_score(row['cleaned_text']), axis=1)

    # Apply categorization function to the sentiment score column
    df_conversations['sentiment'] = df_conversations['sentiment_score'].apply(categorize_sentiment)
    print('Sentiment analysis done')
    return df_conversations


############################################################# MAIN #####################################################
repository_path = 'C:/Users/20221237/PycharmProjects/dbl_1/' # change this to your local path

# Configure SSL context to use certifis CA bundle
ssl_context = ssl.create_default_context(cafile=certifi.where())
ssl._create_default_https_context = lambda: ssl_context

# Download the vader_lexicon data
#nltk.download('vader_lexicon')

analyser = SentimentIntensityAnalyzer()

df_conversations = load_clean_conv(repository_path + 'conversations/airfrance_conversation.csv', 106062176)
df_conv_sentiment = sentiment_analysis(df_conversations)
df_conv_sentiment.to_csv(repository_path + 'sentiment/airfrance_conv_sentiment.csv')

df_conversations = load_clean_conv(repository_path + 'conversations/british_airways_conversation.csv', 18332190)
df_conv_sentiment = sentiment_analysis(df_conversations)
df_conv_sentiment.to_csv(repository_path + 'sentiment/british_airways_conv_sentiment.csv')

df_conversations = load_clean_conv(repository_path + 'conversations/klm_conversation.csv', 56377143)
df_conv_sentiment = sentiment_analysis(df_conversations)
df_conv_sentiment.to_csv(repository_path + 'sentiment/klm_conv_sentiment.csv')

df_conversations = load_clean_conv(repository_path + 'conversations/lufthansa_conversation.csv', 124476322)
df_conv_sentiment = sentiment_analysis(df_conversations)
df_conv_sentiment.to_csv(repository_path + 'sentiment/lufthansa_conv_sentiment.csv')

df_conversations = pd.read_csv(repository_path + 'conversations/klm_non_reply_tweets')
df_conversations = df_conversations.loc[df_conversations["lang"] == "en"]
df_conversations['cleaned_text'] = df_conversations['text'].apply(clean_tweet_text)
df_conv_sentiment = sentiment_analysis(df_conversations)
df_conv_sentiment.to_csv(repository_path + 'sentiment/klm_non_reply_sentiment.csv')