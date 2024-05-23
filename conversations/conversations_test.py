from pymongo import MongoClient
from pprint import pprint
import matplotlib.pyplot as plt
import pandas as pd

# establish connection to MongoDB
client = MongoClient()

# database and collection
db = client["airlines"]
tweets_collection = db["tweets_collection"]

# create indexes
db.tweets_collection.create_index('user.id')
db.tweets_collection.create_index('in_reply_to_status_id')
db.tweets_collection.create_index('in_reply_to_user_id')
db.tweets_collection.create_index('created_at_datetime')

# START TODO

# ADJUSTABLE VARIABLES

# id of airline
klm_id = 56377143

# projected fields
projection = {'created_at_datetime': 1,
            'text': 1, 'cleaned_text': 1,
            'in_reply_to_status_id': 1,
            'in_reply_to_user_id': 1,
            'lang': 1,
            'user.id': 1, 'user.name': 1}

# all reply tweets from airline
klm_replies = db.tweets_collection.find({'user.id': klm_id,
                                'in_reply_to_status_id': {'$ne': None},
                                'in_reply_to_user_id': {'$ne': None}},
                                projection
                                ).sort([('created_at_datetime',1)])

# random tweet
tweet = klm_replies[5]

# finds all the tweets in a thread (may contain involve users)
def conversation_all(tweet):
    """
    Finds all the tweets in a thread (may involve multiple users).
    :param tweet: a tweet object
    :return: a list containing all the tweets in a thread.
    """
    conv_starter = recursive_up_all(tweet)

    conv_all = [conv_starter]

    recursive_down_all(conv_starter, conv_all)

    return conv_all
def recursive_up_all(tweet):
    """
    Starting from a tweet, the function recurs up until it finds the conversation starter.
    :param tweet: a tweet object
    :return: a tweet object that is the conversation starter in a thread.
    """
    in_reply_to_status_id = tweet['in_reply_to_status_id']
    in_reply_to_user_id = tweet['in_reply_to_user_id']

    # 'tweet' replies to at most one tweet
    recursive_tweet = db.tweets_collection.find_one({'_id': in_reply_to_status_id, 'user.id': in_reply_to_user_id},
                                              projection)

    # if 'tweet' is not a reply tweet, return it
    if recursive_tweet == None:
        return tweet
    # 'if 'tweet' is a reply tweet, continue
    else:
        return recursive_up_all(recursive_tweet)

def recursive_down_all(tweet, conv_all):
    """
    Starting from the conversation starter, the function recurs down until it finds all reply tweets
    :param tweet: a tweet object
    :param conv_all: a list containing tweet objects
    :return: None.
    """
    tweet_id = tweet['_id']
    user_id = tweet['user']['id']

    # multiple tweets may reply to 'tweet'
    recursive_tweets = db.tweets_collection.find({'in_reply_to_status_id': tweet_id, 'in_reply_to_user_id': user_id},
                                           projection)

    for r in recursive_tweets:
        conv_all.append(r)
        recursive_down_all(r, conv_all)

# finds all the tweets in a thread (between airline and one client only)
def conversation(tweet):
    """
    Finds all the tweets in a thread (between airline and one client only).
    :param tweet: a tweet object
    :return: a list containing all the tweets in a thread between an airline and one client.
    """
    # an airline and one client
    users = [tweet['user']['id'], tweet['in_reply_to_user_id']]

    conv_starter = recursive_up(tweet, users)

    conv = [conv_starter]

    recursive_down(conv_starter, users, conv)

    return conv

def recursive_up(tweet, users):
    """
    Starting from a tweet, the function recurs up until it finds the conversation starter.
    :param tweet: a tweet object
    :param users: a list containing an airline and one client
    :return: a tweet object that is the conversation starter in a thread.
    """
    in_reply_to_status_id = tweet['in_reply_to_status_id']
    in_reply_to_user_id = tweet['in_reply_to_user_id']

    # 'tweet' replies to at most one tweet
    # only airline and one client are part of the conversation
    recursive_tweet = db.tweets_collection.find_one({'_id': in_reply_to_status_id, 'user.id': in_reply_to_user_id,
                                                     'user.id': {'$in': users}},
                                              projection)

    # if 'tweet' is not a reply tweet, return it
    if recursive_tweet == None:
        return tweet
    # 'if 'tweet' is a reply tweet, continue
    else:
        return recursive_up(recursive_tweet, users)

def recursive_down(tweet, users, conv):
    """
    Starting from the conversation starter, the function recurs down until it finds all reply tweets
    :param tweet: a tweet object
    :param users: a list containing an airline and one client
    :param conv: a list containing tweet objects
    :return: None.
    """
    tweet_id = tweet['_id']
    user_id = tweet['user']['id']

    # multiple tweets can reply to 'tweet'
    # only airline and one client are part of the conversation
    recursive_tweets = db.tweets_collection.find({'in_reply_to_status_id': tweet_id, 'in_reply_to_user_id': user_id,
                                                  'user.id': {'$in': users}},
                                           projection)

    for r in recursive_tweets:
        conv.append(r)
        recursive_down(r, users, conv)


# finds (multiple) conversations in a thread
def find_conversations(tweet):
    """
    Finds (multiple) conversations in a thread.
    :param df: a dataframe containing conversations
    :param tweet: a tweet object
    :return: a list of conversations.
    """
    # an airline and one client
    users = [tweet['user']['id'], tweet['in_reply_to_user_id']]

    conv_starter = find_conversation_starter(tweet, users)

    conv_enders = []
    find_conversation_enders(conv_starter, users, conv_enders)

    conversations = []

    for e in conv_enders:
        conv = [e]
        find_conversation(e, users, conv)
        conversations.append(conv)

    return conversations

def find_conversation_starter(tweet, users):
    """
    Starting from a tweet, the function recurs up until it finds the conversation starter.
    :param tweet: a tweet object
    :param users: a list of an airline and one client
    :return: a tweet object that is the conversation starter in a thread.
    """
    in_reply_to_status_id = tweet['in_reply_to_status_id']
    in_reply_to_user_id = tweet['in_reply_to_user_id']

    recursive_tweet = db.tweets_collection.find_one({'_id': in_reply_to_status_id, 'user.id': in_reply_to_user_id,
                                                     'user.id': {'$in': users}},
                                              projection)

    if recursive_tweet == None:
        return tweet
    else:
        return find_conversation_starter(recursive_tweet, users)

def find_conversation_enders(tweet, users, conv_enders):
    """
    Starting from the conversation starter, the function recurs down until it finds all conversation enders.
    :param tweet: a tweet object
    :param users: a list of an airline and one client
    :param conv_enders: a list of tweet objects
    :return: None.
    """
    tweet_id = tweet['_id']
    user_id = tweet['user']['id']

    # multiple tweets can reply to 'tweet'
    recursive_tweets = db.tweets_collection.find({'in_reply_to_status_id': tweet_id, 'in_reply_to_user_id': user_id,
                                                  'user.id': {'$in': users}},
                                           projection)

    for r in recursive_tweets:
        # if no tweets by users in 'users' reply to 'tweet', it is the last tweet in a thread
        if db.tweets_collection.find_one({'in_reply_to_status_id': r['_id'], 'in_reply_to_user_id': r['user']['id'],
                                          'user.id': {'$in': users}}) == None:
            conv_enders.append(r)
        find_conversation_enders(r, users, conv_enders)

def find_conversation(tweet, users, conv):
    """
    Finds a conversation by recursing upwards starting from a tweet.
    :param tweet: a tweet object
    :param users: a list containing an airline and one client
    :param conv: a list of tweet objects
    :return: None.
    """
    in_reply_to_status_id = tweet['in_reply_to_status_id']
    in_reply_to_user_id = tweet['in_reply_to_user_id']

    # 'tweet' replies to at most one tweet
    # only airline and one client are part of the conversation
    recursive_tweet = db.tweets_collection.find_one({'_id': in_reply_to_status_id, 'user.id': in_reply_to_user_id,
                                                     'user.id': {'$in': users}},
                                              projection)

    if recursive_tweet != None:
        conv.insert(0, recursive_tweet)
        find_conversation(recursive_tweet, users, conv)


# for c in conversation_all(tweet):
#     print(c['user']['id'])
#     pprint(c)
#
# print('end conversation all')
#
for c in conversation(tweet):
    print(c['user']['id'])
    pprint(c)

print('end conversation')

def conversations_dataframe(replies):
    """
    Creates a dataframe containing conversation tweets.
    :param replies: a list of reply tweets by an airline
    :return: a dataframe containing conversation tweets.
    """
    # initialize empty dataframe
    df = pd.DataFrame()
    count = 1

    for tweet in replies[:50]:
        print(count)
        count += 1

        # if tweet is already in database, ignore it
        if not (df == tweet['_id']).any().any():
            for conv in find_conversations(tweet):
                df_temp = pd.DataFrame(conv)

                df_temp['conv_starter'] = conv[0]['_id']
                df_temp['conv_ender'] = conv[-1]['_id']

                df = pd.concat([df, df_temp], ignore_index=True)

    return df

def idk():
    df = pd.DataFrame()
    count=0
    for tweet in klm_replies:
        print(count)
        count+=1
        if not (df==tweet['_id']).any().any():
            convo = conversation(tweet)
            df_temp = pd.DataFrame(convo)

            df = pd.concat([df, df_temp], ignore_index=True)
    return df

# print(df.head())

# dataframe containing conversation tweets
df_conversations = conversations_dataframe(klm_replies)

# dataframe containing conversation tweets with a multi-index
df_conversations_ind = df_conversations.set_index(['conv_starter', 'conv_ender', '_id'])

print(df_conversations.info())
print(df_conversations.head())

print(df_conversations_ind.info())
print(df_conversations_ind.head(10))

#END TODO

# close connection
client.close()
