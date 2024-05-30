from pymongo import MongoClient
from pprint import pprint
from timeit import default_timer as timer
import pandas as pd
import datetime
import random

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

# id of airline(s)
klm_id = 56377143

airfrance_id = 106062176
british_airways_id = 18332190
lufthansa_id = 124476322


# # random tweet
# tweet = klm_replies[208]
#
# # finds all the tweets in a thread (may contain involve multiple users)
# def conversation_all(tweet):
#     """
#     Finds all the tweets in a thread (may involve multiple users).
#     :param tweet: a tweet object
#     :return: a list containing all the tweets in a thread.
#     """
#     conv_starter = recursive_up_all(tweet)
#
#     conv_all = [conv_starter]
#
#     recursive_down_all(conv_starter, conv_all)
#
#     return conv_all
# def recursive_up_all(tweet):
#     """
#     Starting from a tweet, the function recurs up until it finds the conversation starter.
#     :param tweet: a tweet object
#     :return: a tweet object that is the conversation starter in a thread.
#     """
#     in_reply_to_status_id = tweet['in_reply_to_status_id']
#     in_reply_to_user_id = tweet['in_reply_to_user_id']
#
#     # 'tweet' replies to at most one tweet
#     recursive_tweet = db.tweets_collection.find_one({'_id': in_reply_to_status_id, 'user.id': in_reply_to_user_id},
#                                               projection)
#
#     # if 'tweet' is not a reply tweet, return it
#     if recursive_tweet == None:
#         return tweet
#     # 'if 'tweet' is a reply tweet, continue
#     else:
#         return recursive_up_all(recursive_tweet)
#
# def recursive_down_all(tweet, conv_all):
#     """
#     Starting from the conversation starter, the function recurs down until it finds all reply tweets.
#     :param tweet: a tweet object
#     :param conv_all: a list containing tweet objects
#     :return: None.
#     """
#     tweet_id = tweet['_id']
#     user_id = tweet['user']['id']
#
#     # multiple tweets may reply to 'tweet'
#     recursive_tweets = db.tweets_collection.find({'in_reply_to_status_id': tweet_id, 'in_reply_to_user_id': user_id},
#                                            projection)
#
#     for r in recursive_tweets:
#         conv_all.append(r)
#         recursive_down_all(r, conv_all)
#
# # finds (multiple) conversations in a thread
# def find_conversation_starter(tweet, users):
#     """
#     Starting from a tweet, the function recurs up until it finds the conversation starter.
#     :param tweet: a tweet object
#     :param users: a list of an airline and one client
#     :return: a tweet object that is the conversation starter in a thread.
#     """
#     in_reply_to_status_id = tweet['in_reply_to_status_id']
#     in_reply_to_user_id = tweet['in_reply_to_user_id']
#
#     recursive_tweet = db.tweets_collection.find_one({'_id': in_reply_to_status_id, 'user.id': in_reply_to_user_id,
#                                                      'user.id': {'$in': users}},
#                                               projection)
#
#     if recursive_tweet == None:
#         return tweet
#     else:
#         return find_conversation_starter(recursive_tweet, users)
#
# def find_conversation_enders(tweet, users, conv_enders):
#     """
#     Starting from the conversation starter, the function recurs down until it finds all conversation enders.
#     :param tweet: a tweet object
#     :param users: a list of an airline and one client
#     :param conv_enders: a list of tweet objects
#     :return: None.
#     """
#     tweet_id = tweet['_id']
#     user_id = tweet['user']['id']
#
#     # multiple tweets can reply to 'tweet'
#     recursive_tweets = db.tweets_collection.find({'in_reply_to_status_id': tweet_id, 'in_reply_to_user_id': user_id,
#                                                   'user.id': {'$in': users}},
#                                            projection)
#
#     for r in recursive_tweets:
#         # if no tweets by users in 'users' reply to 'tweet', it is the last tweet in a thread
#         if db.tweets_collection.find_one({'in_reply_to_status_id': r['_id'], 'in_reply_to_user_id': r['user']['id'],
#                                           'user.id': {'$in': users}}) == None:
#             conv_enders.append(r)
#         find_conversation_enders(r, users, conv_enders)
#
# def find_conversation(tweet, users, conv):
#     """
#     Finds a conversation by recursing upwards starting from a tweet.
#     :param tweet: a tweet object
#     :param users: a list containing an airline and one client
#     :param conv: a list of tweet objects
#     :return: None.
#     """
#     in_reply_to_status_id = tweet['in_reply_to_status_id']
#     in_reply_to_user_id = tweet['in_reply_to_user_id']
#
#     # 'tweet' replies to at most one tweet
#     # only airline and one client are part of the conversation
#     recursive_tweet = db.tweets_collection.find_one({'_id': in_reply_to_status_id, 'user.id': in_reply_to_user_id,
#                                                      'user.id': {'$in': users}},
#                                               projection)
#
#     if recursive_tweet != None:
#         conv.insert(0, recursive_tweet)
#         find_conversation(recursive_tweet, users, conv)
#
# def conversations(tweet):
#     """
#     Finds (multiple) conversations in a thread.
#     :param df: a dataframe containing conversations
#     :param tweet: a tweet object
#     :return: a list of conversations.
#     """
#     # an airline and one client
#     users = [tweet['user']['id'], tweet['in_reply_to_user_id']]
#
#     conv_starter = find_conversation_starter(tweet, users)
#
#     conv_enders = []
#     find_conversation_enders(conv_starter, users, conv_enders)
#
#     convs = []
#
#     for e in conv_enders:
#         conv = [e]
#         find_conversation(e, users, conv)
#         convs.append(conv)
#
#     return convs
#
# def conversations_dataframe(replies):
#     """
#     Creates a dataframe containing conversation tweets. Uses the 'conversations' function.
#     :param replies: a list of reply tweets by an airline
#     :return: a dataframe containing conversation tweets.
#     """
#     # initialize empty dataframe
#     df_conversations = pd.DataFrame()
#
#     for tweet in replies:
#
#         # if tweet is already in database, ignore it
#         if not (df_conversations == tweet['_id']).any().any():
#             for conv in conversations(tweet):
#                 df_temp = pd.DataFrame(conv)
#
#                 # user is still stored in dictionary format
#                 df_temp['user'] = df_temp['user'].apply(lambda x: pd.Series(x['id']))
#
#                 # created for ease to group conversations
#                 df_temp['conv_starter'] = conv[0]['_id']
#                 df_temp['conv_ender'] = conv[-1]['_id']
#
#                 df_conversations = pd.concat([df_conversations, df_temp], ignore_index=True)
#
#     return df_conversations




# FUNCTIONS FOR MINING CONVERSATIONS

# finds all the tweets in a thread (between airline and one client only)
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
                                                     'user.id': users[1]},
                                              projection)

    # if 'tweet' is not a reply tweet, return it
    if recursive_tweet == None:
        return tweet
    # 'if 'tweet' is a reply tweet, continue
    else:
        return recursive_up(recursive_tweet, users)

def recursive_down(tweet, users, conv):
    """
    Starting from the conversation starter, the function recurs down until it finds all reply tweets.
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
        # response time of a reply tweet to the replied tweet in minutes
        r['response_time'] = (r['created_at_datetime'] - tweet['created_at_datetime']).total_seconds() / 60
        conv.append(r)
        recursive_down(r, users, conv)

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

def conversation_dateframe(airline_id):
    """
    Creates a dataframe containing conversation tweets. Uses the 'conversation' function.
    :param replies: a list of reply tweets by an airline / airlines
    :return: a dataframe containing conversation tweets.
    """
    # projected fields
    projection = {'created_at_datetime': 1,
                  'text': 1,
                  'in_reply_to_status_id': 1,
                  'in_reply_to_user_id': 1,
                  'lang': 1,
                  'user.id': 1}

    # all reply tweets from airline
    airline_replies = db.tweets_collection.find({'user.id': airline_id,
                                                'in_reply_to_status_id': {'$ne': None},
                                                'in_reply_to_user_id': {'$ne': None}},
                                                projection
                                                ).sort([('created_at_datetime', 1)])

    # initialize empty dataframe
    df_conversation = pd.DataFrame()

    for tweet in airline_replies:
        # if tweet is already in database, ignore it
        if not (df_conversation == tweet['_id']).any().any():
            conv = conversation(tweet)
            df_temp = pd.DataFrame(conv)

            # user is still stored in dictionary format
            df_temp['user'] = df_temp['user'].apply(lambda x: pd.Series(x['id']))

            # created for ease to group conversations
            df_temp['conv_starter'] = conv[0]['_id']

            df_conversation = pd.concat([df_conversation, df_temp], ignore_index=True)

    return df_conversation


# TESTING IN TERMINAL

# for c in conversation_all(tweet):
#     print(c['user']['id'])
#     pprint(c)
#
# print('end conversation all')
#
# for c in conversation(tweet):
#     pprint(c)
#
# print('end conversation')
#
# for c in conversations(tweet):
#     pprint(c)


# MINING CONVERSATIONS

# dataframe containing klm conversation tweets (using conversation function)
start = timer()
klm_conversation = conversation_dateframe(klm_id)
end = timer()
print(f'create_df_klm: {(end - start) / 60} minutes')

# uploading dataframe to csv file
start = timer()
klm_conversation.to_csv('conversations\klm_conversation.csv', sep=',', index=False, encoding='utf-8')
end = timer()
print(f'df_klm_to_csv: {(end - start) / 60} minutes')


# dataframe containing airfrance conversation tweets
start = timer()
airfrance_conversation = conversation_dateframe(airfrance_id)
end = timer()
print(f'create_df_airfrance: {(end - start) / 60} minutes')

# uploading dataframe to csv file
start = timer()
airfrance_conversation.to_csv('conversations\\airfrance_conversation.csv', sep=',', index=False, encoding='utf-8')
end = timer()
print(f'df_airfrance_to_csv: {(end - start) / 60} minutes')


# dataframe containing british airways conversation tweets
start = timer()
british_airways_conversation = conversation_dateframe(british_airways_id)
end = timer()
print(f'create_df_british_airways: {(end - start) / 60} minutes')

# uploading dataframe to csv file
start = timer()
british_airways_conversation.to_csv('conversations\\british_airways_conversation.csv', sep=',', index=False, encoding='utf-8')
end = timer()
print(f'df_british_airways_to_csv: {(end - start) / 60} minutes')


# dataframe containing lufthansa conversation tweets
start = timer()
lufthansa_conversation = conversation_dateframe(lufthansa_id)
end = timer()
print(f'create_df_lufthansa: {(end - start) / 60} minutes')

# uploading dataframe to csv file
start = timer()
lufthansa_conversation.to_csv('conversations\lufthansa_conversation.csv', sep=',', index=False, encoding='utf-8')
end = timer()
print(f'df_lufthansa_to_csv: {(end - start) / 60} minutes')



# NON-REPLY TWEETS DIRECTED AT KLM

def non_reply_tweets_dataframe():
    """
    Creates a dataframe containing single tweets directed at KLM in a user mention with no reply from KLM.
    :return: a dataframe containing non-reply tweets directed at KLM.
    """
    # projection
    projection = {'created_at_datetime':1,
                  'text':1,
                  'lang':1,
                  'user':1}

    # query for in_reply_to_status_id == None: only want single stand-alone tweets mentioning KLM that are not
    # part of another conversation
    klm_mention = db.tweets_collection.find({"entities.user_mentions.id": klm_id,
                                            'in_reply_to_status_id': None,
                                            'in_reply_to_user_id': {'$in': [None, klm_id]}},
                                            projection).sort([('created_at_datetime', 1)])

    # create dataframe
    non_reply_tweets = []

    for tweet in klm_mention:
        # check if KLM has replied to this tweet
        if db.tweets_collection.find_one({'user.id': klm_id, 'in_reply_to_status_id': tweet['_id']}) == None:
            non_reply_tweets.append(tweet)

    df_non_reply = pd.DataFrame(non_reply_tweets)

    # user is still stored in dictionary format
    df_non_reply['user'] = df_non_reply['user'].apply(lambda x: pd.Series(x['id']))

    return df_non_reply

# dataframe containing single tweets directed at KLM with no response from KLM
start = timer()
df_non_reply = non_reply_tweets_dataframe()
end = timer()
print(f'create_df_non_reply: {(end-start) / 60} minutes')

# uploading dataframe to csv file
start = timer()
df_non_reply.to_csv('conversations\klm_non_reply_tweets')
end = timer()
print(f'df_non_reply_to_csv: {(end-start) / 60} minutes')


#END TODO

# close connection
client.close()
