import matplotlib.pyplot as plt
import pandas as pd
import datetime

# EXPLORATION KLM CONVERSATIONS

# id of airline
klm_id = 56377143

# dataframe of klm conversations
df_klm_conv = pd.read_csv('conversations\klm_conversation.csv')
df_klm_conv['created_at_datetime'] = pd.to_datetime(df_klm_conv['created_at_datetime'])
print(df_klm_conv.info())
print(df_klm_conv.head())

# all tweets with the same conversation starter belong to one conversation
df_klm_conv_group = df_klm_conv.groupby('conv_starter')

# the number of conversations
n_conversations = df_klm_conv_group.ngroups
# print(n_conversations) # result: 21670

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

conversation_length(l_conversations)

# DECIDE WHAT TO DO WITH THIS
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
klm_response_time = df_klm_conv[df_klm_conv['user'] == klm_id]['response_time']

# histogram response time
def response_time(klm_response_time):
    # plot histogram
    klm_response_time.hist(range=[0,200], color='blue')

    # adding labels and title
    plt.xlabel('response time (in minutes)', size=12)
    plt.ylabel('frequency', size=12)
    plt.title('Response time KLM', size=16, fontweight="bold")

    # show plot
    plt.savefig("plots/response_time_klm")
    plt.show()

response_time(klm_response_time)

# maximum response time
# print(klm_response_time.max()) # result: 174565.5 (or around 4 months)

# average response time by klm in minutes
klm_avg_response_time = klm_response_time.mean()
# print(klm_avg_response_time) # result: 232.87321616800452

# median response time by klm in minutes
klm_med_response_time = klm_response_time.median()
# print(klm_med_response_time) # result: 14.716666666666669

# dataframe conversation starters by clients
df_conv_starters = df_klm_conv[(df_klm_conv['_id'] == df_klm_conv['conv_starter']) & (df_klm_conv['user'] != klm_id)]

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
    plt.annotate('2020', xy=(0.65,0.58), xycoords='axes fraction')

    # show plot
    plt.savefig("plots/conversation_distribution_monthly")
    plt.show()

conversations_distribution()

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

conversation_languages()




