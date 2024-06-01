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

# number of conversations
# print(df_klm_conv_group.ngroups) # result: 21670

# conversation length of each conversation
l_conversations = df_klm_conv_group.size()

# bar chart length of conversations
def conversation_length(l_conversations):
    # all conversation lengths
    l_conversations_count = l_conversations.value_counts().sort_index()

    # plot bar chart
    l_conversations_count.plot(kind='bar', color='blue')

    # adding labels and title
    plt.xlabel('conversation length', size=12)
    plt.ylabel('frequency', size=12)
    plt.title('Length of conversations', size=16, fontweight="bold")
    plt.grid(axis='y')

    # show plot
    plt.savefig("plots/conversation_length_all")
    plt.show()


    # top 10 conversation lengths
    # plot bar chart
    l_conversations_count[:10].plot(kind='bar', color=(['red'] + ['blue'] * 9))

    # adding labels and title
    plt.xlabel('conversation length', size=12)
    plt.ylabel('frequency', size=12)
    plt.title('Top 10 lengths of conversations', size=16, fontweight="bold")
    plt.grid(axis='y')

    # show plot
    plt.savefig("plots/conversation_length_top_10")
    plt.show()

# conversation_length(l_conversations)






# after exploration, it was found that some conversations start with KLM !

# filter conversations that only start with a client
df_klm_conv = df_klm_conv.set_index(['conv_starter'], drop=False)
klm_filter = df_klm_conv[(df_klm_conv['_id'] == df_klm_conv['conv_starter']) & (df_klm_conv['user'] == klm_id)]
df_klm_conv.drop(klm_filter.index, inplace=True)
df_klm_conv.reset_index(drop=True, inplace=True)
# print(df_klm_conv)

# group conversation by conversation starter
df_klm_conv_group = df_klm_conv.groupby(['conv_starter'])

# the number of conversations
n_conversations = df_klm_conv_group.ngroups
# print(n_conversations) # result: 20562

# series with response time of klm replies
klm_response_time = df_klm_conv[df_klm_conv['user'] == klm_id]['response_time']
# print(klm_response_time)

# histogram response time
def response_time(response_time):
    # plot histogram
    response_time.hist(range=[0,200], color='blue')

    # adding labels and title
    plt.xlabel('response time (in minutes)', size=12)
    plt.ylabel('frequency', size=12)
    plt.title('Response time', size=16, fontweight="bold")

    # show plot
    plt.savefig("plots/response_time_klm")
    plt.show()

# response_time(klm_response_time)

# percentage response time is within 30 min
response_within_30_min = (len(klm_response_time[klm_response_time <= 30]) / len(klm_response_time)) * 100
# print(response_within_30_min) # result: 73.3870720568854

# percentage response time is within 1 hour
response_within_1_hour = (len(klm_response_time[klm_response_time <= 60]) / len(klm_response_time)) * 100
# print(response_within_1_hour) # result: 83.92435712753242

# maximum response time
# print(klm_response_time.max()) # result: 174565.5 (or around 4 months)

# average response time by klm in minutes
klm_avg_response_time = klm_response_time.mean()
# print(klm_avg_response_time) # result: 233.1197244613357 (or around 4 hours)

# median response time by klm in minutes
klm_med_response_time = klm_response_time.median()
# print(klm_med_response_time) # result: 14.733333333333333

# dataframe conversation starters by clients
df_conv_starters = df_klm_conv[(df_klm_conv['_id'] == df_klm_conv['conv_starter'])]

def conversations_distribution():
    # conversations grouped by (week)day: bar chart
    df_conv_starters_daily = df_conv_starters.copy()
    df_conv_starters_daily['day'] = df_conv_starters['created_at_datetime'].apply(lambda r:r.isoweekday())

    colors = ['b', 'b', 'b', 'orange', 'orange', 'b', 'b']

    df_conv_starters_daily['day'].value_counts(sort=False).plot(kind='bar', color=colors)

    # adding labels and title
    plt.xlabel('day of the week', size=12)
    plt.ylabel('frequency', size=12)
    plt.title('Daily distribution conversations', size=16, fontweight="bold")

    # show plot
    plt.savefig("plots/conversation_distribution_daily")
    plt.show()


    # conversations grouped by month: bar chart
    df_conv_starters_monthly = df_conv_starters.copy()
    df_conv_starters_monthly['month'] = df_conv_starters['created_at_datetime'].dt.month
    df_conv_starters_monthly['month'].value_counts(sort=False).plot(kind='bar', color='blue')

    # adding labels and title
    plt.xlabel('month', size=12)
    plt.ylabel('frequency', size=12)
    plt.title('Monthly distribution conversations', size=16, fontweight="bold")
    plt.annotate('2019', xy=(0.01,0.2), xycoords='axes fraction')
    plt.annotate('2020', xy=(0.74,0.54), xycoords='axes fraction')

    # show plot
    plt.savefig("plots/conversation_distribution_monthly")
    plt.show()

# conversations_distribution()

# languages of conversation starters
def conversation_languages():
    all_languages = df_conv_starters.groupby('lang').size().sort_values(ascending=False)
    select_languages = all_languages[:2]
    select_languages['other'] = all_languages[2:].sum()

    # plot bar chart
    select_languages.plot(kind='bar', color=['blue', 'orange', 'gray'])

    # adding labels and title
    plt.xlabel('language', size=12)
    plt.ylabel('frequency', size=12)
    plt.title('Languages of conversation starters', size=16, fontweight="bold")

    # show plot
    plt.savefig("plots/conversation_languages")
    plt.show()

# conversation_languages()

# repeat conversation length on filtered conversations
l_conversations = df_klm_conv_group.size()
# average conversation length
# print(l_conversations.mean()) # result: 3.3860033070712965


# STATISTICS KLM VS OTHER AIRLINES

# id other airlines
airfrance_id = 106062176
british_airways_id = 18332190
lufthansa_id = 124476322

# dataframe of other airlines
df_airfrance_conv = pd.read_csv('conversations\\airfrance_conversation.csv')
df_british_airways_conv = pd.read_csv('conversations\\british_airways_conversation.csv')
df_lufthansa_conv = pd.read_csv('conversations\lufthansa_conversation.csv')

# filter conversations such that they only start with a client
df_airfrance_conv = df_airfrance_conv.set_index(['conv_starter'], drop=False)
airfrance_filter = df_airfrance_conv[(df_airfrance_conv['_id'] == df_airfrance_conv['conv_starter']) & (df_airfrance_conv['user'] == airfrance_id)]
df_airfrance_conv.drop(airfrance_filter.index, inplace=True)
df_airfrance_conv.reset_index(drop=True, inplace=True)

df_british_airways_conv = df_british_airways_conv.set_index(['conv_starter'], drop=False)
british_airways_filter = df_british_airways_conv[(df_british_airways_conv['_id'] == df_british_airways_conv['conv_starter']) & (df_british_airways_conv['user'] == british_airways_id)]
df_british_airways_conv.drop(british_airways_filter.index, inplace=True)
df_british_airways_conv.reset_index(drop=True, inplace=True)

df_lufthansa_conv = df_lufthansa_conv.set_index(['conv_starter'], drop=False)
lufthansa_filter = df_lufthansa_conv[(df_lufthansa_conv['_id'] == df_lufthansa_conv['conv_starter']) & (df_lufthansa_conv['user'] == lufthansa_id)]
df_lufthansa_conv.drop(lufthansa_filter.index, inplace=True)
df_lufthansa_conv.reset_index(drop=True, inplace=True)

# group conversations by conversation starter
df_airfrance_conv_group = df_airfrance_conv.groupby(['conv_starter'])
df_british_airways_conv_group = df_british_airways_conv.groupby(['conv_starter'])
df_lufthansa_conv_group = df_lufthansa_conv.groupby(['conv_starter'])

# the number of conversations
n_conversations_airfrance = df_airfrance_conv_group.ngroups
# print(n_conversations_airfrance) # result: 5968
n_conversations_british_airways = df_british_airways_conv_group.ngroups
# print(n_conversations_british_airways) # result: 73427
n_conversations_lufthansa = df_lufthansa_conv_group.ngroups
# print(n_conversations_lufthansa) # result: 9028

# conversation starters
df_conv_starters_airfrance = df_airfrance_conv[df_airfrance_conv['_id'] == df_airfrance_conv['conv_starter']]
df_conv_starters_british_airways = df_british_airways_conv[df_british_airways_conv['_id'] == df_british_airways_conv['conv_starter']]
df_conv_starters_lufthansa = df_lufthansa_conv[df_lufthansa_conv['_id'] == df_lufthansa_conv['conv_starter']]

# top languages of conversation starters
languages_airfrance = df_conv_starters_airfrance.groupby('lang').size().sort_values(ascending=False)
# print(languages_airfrance)
languages_british_airways = df_conv_starters_british_airways.groupby('lang').size().sort_values(ascending=False)
# print(languages_british_airways)
languages_lufthansa = df_conv_starters_lufthansa.groupby('lang').size().sort_values(ascending=False)
# print(languages_lufthansa)

# comparison response times
airfrance_response_time = df_airfrance_conv[df_airfrance_conv['user'] == airfrance_id]['response_time']
# print(airfrance_response_time.mean()) # result: 385.7925550934034
# print((len(airfrance_response_time[airfrance_response_time <= 60]) / len(airfrance_response_time)) * 100) # result: 57.57443082311734
british_airways_response_time = df_british_airways_conv[df_british_airways_conv['user'] == british_airways_id]['response_time']
# print(british_airways_response_time.mean()) # result: 679.3996913700634
# print((len(airfrance_response_time[airfrance_response_time <= 60]) / len(airfrance_response_time)) * 100) # result: 39.93271574841541
lufthansa_response_time = df_lufthansa_conv[df_lufthansa_conv['user'] == lufthansa_id]['response_time']
# print(lufthansa_response_time.mean()) # result: 36.5021921265579
# print((len(lufthansa_response_time[lufthansa_response_time <= 60]) / len(lufthansa_response_time)) * 100) # result: 92.81867145421903

# comparison average conversation length
l_conversations_airfrance = df_airfrance_conv_group.size()
# print(l_conversations_airfrance.mean()) # result: 3.2027479892761392
l_conversations_british_airways = df_british_airways_conv_group.size()
# print(l_conversations_british_airways.mean()) # result: 3.093208220409386
l_conversations_lufthansa = df_lufthansa_conv_group.size()
# print(l_conversations_lufthansa.mean()) # result: 3.445945945945946



