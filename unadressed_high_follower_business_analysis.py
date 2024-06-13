import pandas as pd

conversations_tweets_KLM = pd.read_csv('D:\\Twitter Data CBL 1\\Github Repo\\dbl_1\\sentiment\\klm_conv_sentiment.csv')
non_reply_tweets_KLM = pd.read_csv('D:\\Twitter Data CBL 1\\Github Repo\\dbl_1\\sentiment\\klm_non_reply_sentiment.csv')

# conversation_filter = conversations_tweets_KLM[(conversations_tweets_KLM["_id"] != conversations_tweets_KLM["conv_starter"]) & (conversations_tweets_KLM["user"] == 56377143)]
# conversations_tweets_KLM.drop(conversation_filter.index, inplace=True)

total_non_reply = len(non_reply_tweets_KLM)
total_conversation_starters = len(conversations_tweets_KLM)

print(f"TOTAL CONV STARTERS: {total_conversation_starters}")
print(f"TOTAL NON REPLY: {total_non_reply}")

influencer_threshold = 10000

influencer_conversation_starter = conversations_tweets_KLM[conversations_tweets_KLM['follower_count'] >= influencer_threshold]
influencer_non_reply = non_reply_tweets_KLM[non_reply_tweets_KLM['follower_count'] >= influencer_threshold]
non_influencer_conversation_starter = conversations_tweets_KLM[conversations_tweets_KLM['follower_count'] < influencer_threshold]
non_influencer_non_reply = non_reply_tweets_KLM[non_reply_tweets_KLM['follower_count'] < influencer_threshold]

amount_non_influencer_conversation_starter = len(non_influencer_conversation_starter)
print(f"Number of low-influence conversation starters KLM IS ADDRESSING: {amount_non_influencer_conversation_starter}")

amount_non_influencer_non_reply = len(non_influencer_non_reply)
print(f"Number of low-influence NON REPLY starters KLM IS NOT ADDRESSING: {amount_non_influencer_non_reply}")

# non_reply_id = set(non_reply_tweets_KLM['_id'])
# conversation_id = set(conversations_tweets_KLM['_id'])
# check_duplicates = non_reply_id.intersection(conversation_id)
# print("check_duplicates")

amount_influencer_conversation_starter = len(influencer_conversation_starter)
print(f"Number of high-influence conversation starters KLM IS ADDRESSING: {amount_influencer_conversation_starter}")

amount_influencer_non_reply = len(influencer_non_reply)
print(f"Number of high-influence NON REPLY starters KLM IS NOT ADDRESSING: {amount_influencer_non_reply}")








