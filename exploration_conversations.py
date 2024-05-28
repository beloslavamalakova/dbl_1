import matplotlib.pyplot as plt
import pandas as pd
import datetime

# EXPLORATION KLM CONVERSATIONS

# id of airline
klm_id = 56377143

# dataframe of klm conversations
df_klm_conv = pd.read_csv('conversations\klm_conversation.csv')
df_klm_conv['created_at_datetime'] = pd.to_datetime(df_klm_conv['created_at_datetime'])
# print(df_klm_conv.info())
# print(df_klm_conv.head())

# all tweets with the same conversation starter belong to one conversation
df_klm_conv_group = df_klm_conv.groupby('conv_starter')

# the number of conversations
n_conversations = df_klm_conv_group.ngroups
# print(n_conversations) # result: 21670

# length of conversations
l_conversations = df_klm_conv_group.size()
# print(l_conversations) # comment: there exist conversations of length 1

# histogram length of conversations (INCLUDE MAX, MIN?)
# l_conversations.hist(range=[0,15])
l_conversations.value_counts().plot(kind='bar')
plt.show()

# number of conversations of length 1
n_length_1 = len(l_conversations[l_conversations == 1])
# print(n_length_1) # result: 831

# average length of conversations including of length 1
avg_l_conversations = l_conversations.mean()
# print(avg_l_conversations) # result: 3.295293031841255

# average length of conversations excluding of length 1
avg_l_conversations_excl = (l_conversations[l_conversations != 1]).mean()
# print(avg_l_conversations_excl) # result: 3.386822784202697

# series with response time of klm replies (INCLUDE MAX, MIN?)
s_klm_replies = df_klm_conv[df_klm_conv['user'] == klm_id]['response_time']
s_klm_replies.hist(range=[0,200])
plt.show()

# average response time by klm in minutes
klm_avg_response_time = s_klm_replies.mean()
# print(klm_avg_response_time) # result: 232.87321616800452

# median response time by klm in minutes
klm_med_response_time = s_klm_replies.median()
# print(klm_med_response_time) # result: 14.716666666666669

# dataframe conversation starters by clients
df_conv_starters = df_klm_conv[(df_klm_conv['_id'] == df_klm_conv['conv_starter']) & (df_klm_conv['user'] != klm_id)]

# distribution of conversation (starters) by clients over time: histogram
df_conv_starters['created_at_datetime'].hist()
plt.show()

# conversations grouped by (week)day: bar chart
df_conv_starters_daily = df_conv_starters.copy()
df_conv_starters_daily['day'] = df_conv_starters['created_at_datetime'].apply(lambda r:r.isoweekday())
df_conv_starters_daily['day'].value_counts(sort=False).plot(kind='bar')
plt.show()

# conversations grouped by month: bar chart
df_conv_starters_monthly = df_conv_starters.copy()
df_conv_starters_monthly['month'] = df_conv_starters['created_at_datetime'].dt.month
df_conv_starters_monthly['month'].value_counts(sort=False).plot(kind='bar')
plt.show()


