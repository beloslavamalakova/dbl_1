import matplotlib.pyplot as plt
import pandas as pd
import datetime

# variables start and end date
start_date = input('Provide a START DATE between 2019-05-22 and 2020-03-30 formatted as year-month-day: ')
end_date = input('Provide a END DATE between 2019-05-22 and 2020-03-30 formatted as year-month-day: ')



# id of airline
klm_id = 56377143

# dataframe of klm conversations
df_klm_conv = pd.read_csv('conversations\klm_conversation.csv')
df_klm_conv['created_at_datetime'] = pd.to_datetime(df_klm_conv['created_at_datetime'])
# limits entries to a specific period
mask = (df_klm_conv['created_at_datetime'] >= start_date) & (df_klm_conv['created_at_datetime'] <= end_date)
df_klm_conv = df_klm_conv[mask]

# dataframe of non-reply tweets directed at KLM
df_non_reply = pd.read_csv('conversations\klm_non_reply_tweets.csv')
df_non_reply['created_at_datetime'] = pd.to_datetime(df_non_reply['created_at_datetime'])
# limits entries to a specific period
mask = (df_non_reply['created_at_datetime'] >= start_date) & (df_non_reply['created_at_datetime'] <= end_date)
df_non_reply = df_non_reply[mask]

# filter conversations that only start with a client
df_klm_conv = df_klm_conv.set_index(['conv_starter'], drop=False)
klm_filter = df_klm_conv[(df_klm_conv['_id'] == df_klm_conv['conv_starter']) & (df_klm_conv['user'] == klm_id)]
df_klm_conv.drop(klm_filter.index, inplace=True)
df_klm_conv.reset_index(drop=True, inplace=True)

# dataframe conversation starters by clients
df_conv_starters = df_klm_conv[(df_klm_conv['_id'] == df_klm_conv['conv_starter'])]

# languages by count of klm conversation starters
languages_conv_starters = df_conv_starters.groupby('lang').size().sort_values(ascending=False)

# languages by count of non_reply tweets directed at KLM
languages_non_reply = df_non_reply.groupby('lang').size().sort_values(ascending=False)

def distribution_languages(languages_conv_starters, languages_non_reply):
    """
    Creates a grouped bar chart with (1) the relative frequencies of languages in conversation starters and (2) the
    relative frequencies of languages in single non-reply tweets directed at KLM.
    :param languages_conv_starters: languages in conversations and their count
    :param languages_non_reply: languages in non-reply tweets and their count
    :return: None (the bar chart plot is saved to the 'plots' directory.
    """
    # distribution of English, Dutch and 'Other' languages in conversation starters
    select_languages_conv = languages_conv_starters[:2]
    select_languages_conv['other'] = languages_conv_starters[2:].sum()
    conv_total = select_languages_conv.sum()
    p_conv = select_languages_conv / conv_total

    # distribution of English, Dutch and 'Other' languages in single non-reply tweets directed at KLM
    select_languages_non_reply = languages_non_reply[:2]
    select_languages_non_reply['other'] = languages_non_reply[2:].sum()
    non_reply_total = select_languages_non_reply.sum()
    p_non_reply = select_languages_non_reply / non_reply_total

    # merge the two series into a single dataframe
    select_languages = pd.concat([p_non_reply, p_conv], axis=1)

    # plot grouped bar chart
    select_languages.plot(kind='bar', color=['blue', 'orange'])

    # adding labels and title
    plt.ylabel('relative frequency', size=12)
    plt.legend([f'single non-reply tweets (total: {non_reply_total})', f'conversation starter tweets (total: {conv_total})'])
    plt.title('Distribution of Languages', size=16, fontweight="bold")

    # show plot
    plt.savefig("plots/distribution_languages")
    plt.show()

distribution_languages(languages_conv_starters, languages_non_reply)