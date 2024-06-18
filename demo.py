import matplotlib.pyplot as plt
import pandas as pd
import datetime
from collections import Counter
import numpy as np


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
df_non_reply = pd.read_csv('conversations\klm_non_reply_tweets')
df_non_reply['created_at_datetime'] = pd.to_datetime(df_non_reply['created_at_datetime'])
# limits entries to a specific period
mask = (df_non_reply['created_at_datetime'] >= start_date) & (df_non_reply['created_at_datetime'] <= end_date)
df_non_reply = df_non_reply[mask]

# filter conversations that only start with a client
df_klm_conv = df_klm_conv.set_index(['conv_starter'], drop=False)
klm_filter = df_klm_conv[(df_klm_conv['_id'] == df_klm_conv['conv_starter']) & (df_klm_conv['user'] == klm_id)]
df_klm_conv.drop(klm_filter.index, inplace=True)
df_klm_conv.reset_index(drop=True, inplace=True)


klm_sentiment_df = pd.read_csv('sentiment/klm_conv_sentiment.csv')
mask = (klm_sentiment_df['created_at_datetime'] >= start_date) & (klm_sentiment_df['created_at_datetime'] <= end_date)
klm_sentiment_df = klm_sentiment_df[mask]

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


def sentiment_evolution(klm_sentiment_df):
    # Group by 'conv_starter'
    grouped = klm_sentiment_df.groupby('conv_starter')

    # Create an array of arrays for each conversation
    conversations_array = [
        [[row['sentiment_score'], row['sentiment'], 'airline' if row['user'] == klm_id else 'customer']
         for _, row in group.iterrows()]
        for _, group in grouped
    ]

    airline_conv_sentiment = []

    for i in range(0, len(conversations_array)):
        specific_conversation = conversations_array[i]

        # Extract sentiment scores and user types
        sentiment_scores = [tweet[0] for tweet in specific_conversation]
        sentiment = [tweet[1] for tweet in specific_conversation]
        user_types = [tweet[2] for tweet in specific_conversation]

        if 'customer' not in user_types: continue

        first_cust = user_types.index('customer')
        last_cust = len(user_types) - 1 - user_types[::-1].index('customer')
        if first_cust == last_cust: continue

        airline_conv_sentiment.append(sentiment[first_cust] + '->' + sentiment[last_cust])

    # Calculate counts
    counts = Counter(airline_conv_sentiment)
    sentiment_keys = list(counts.keys())
    sentiment_keys.sort()
    sorted_sentiment_count = {i: counts[i] for i in sentiment_keys}

    # Define a color mapping for each key
    color_mapping = {
        'Negative->Positive': 'royalblue',
        'Neutral->Positive': 'royalblue',
        'Positive->Positive': 'royalblue',
        'Neutral->Neutral': 'grey',
        'Negative->Neutral': 'grey',
        'Positive->Neutral': 'grey',
        'Neutral->Negative': 'purple',
        'Positive->Negative': 'purple',
        'Negative->Negative': 'purple',
    }

    # Create a list of colors for each bar based on the color mapping
    colors = [color_mapping[key] for key in sorted_sentiment_count.keys()]

    plt.figure(figsize=(20, 5))
    bars = plt.bar(sorted_sentiment_count.keys(), sorted_sentiment_count.values(), color=colors)
    plt.title('KLM Bar Plot of Sentiment Evolution')
    plt.xlabel('Sentiment Evolution')
    plt.ylabel('Count')

    # Annotating each bar with the count value
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, yval, int(yval), va='bottom')  # va: vertical alignment

    plt.savefig("plots/sentiment_evolution")
    plt.show()

distribution_languages(languages_conv_starters, languages_non_reply)
sentiment_evolution(klm_sentiment_df)