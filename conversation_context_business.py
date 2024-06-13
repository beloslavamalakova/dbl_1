import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import matplotlib.pyplot as plt
import textwrap

nltk.download('stopwords') #Words to remove
nltk.download('punkt') #Tokenization of text
nltk.download('wordnet') #For lemmatizing

# Things I want to do:
"""
Find out the most common complaint word group from the negative sentiment KLM conversation starters.
Find out the most common complaint word group from the negative sentiment KLM non-reply tweets.

Find out which are the most common word group of positive sentiment conversation they are addressing.
Find out which are the most common word groups of positive sentiment conversations of non-reply tweets

Compare to the relevant airlines

Find out the most common addressed conversation from all the other airlines.

Further exploration:

When were certain topics most relevant?
Can we match events to topics?
etc.


"""


# CHANGE TO OWN PATH
# IN THE FUTURE ALSO LOOK AT NON REPLY???

file_paths = {
    'klm': {
            'conversations': ["D:\\Twitter Data CBL 1\\Github Repo\\dbl_1\\sentiment\\klm_conv_sentiment.csv"],
            'non_reply': "D:\\Twitter Data CBL 1\\Github Repo\\dbl_1\\sentiment\\klm_non_reply_sentiment.csv"
        },
    'airfrance': {
        'conversations': ["D:\\Twitter Data CBL 1\\Github Repo\\dbl_1\\sentiment\\airfrance_conv_sentiment.csv"]
    },
    'british_airways': {
        'conversations': ["D:\\Twitter Data CBL 1\\Github Repo\\dbl_1\\sentiment\\british_airways_conv_sentiment.csv"]
    },
    'lufthansa': {
        'conversations': ["D:\\Twitter Data CBL 1\\Github Repo\\dbl_1\\sentiment\\lufthansa_conv_sentiment.csv"]
    }
}

number_of_top_words = 10


def adjust_text(text):
    """

    We adjust the text so it can be used properly for the models.

    1) Remove all stopwords from the nltk library
    2) convert to lowercase
    2) lemmatize words so put them in root form
    3) check if word is apphabetic
    3) tokenize text. (advanced splitting)
    filtering the custom stop words if they don't seem relevant in the final graphs.

    """
    stop_words = set(stopwords.words('english'))
    custom_stop_words = {'klm', 'http', 'dm', 'hi', 'hello', 'airfrance', 'really',
                         'hello', 'hey'
                         }
    all_stop_words = stop_words.union(custom_stop_words)
    lemmatizer = WordNetLemmatizer()
    words_tokenized = word_tokenize(text)

    filtered_words = []
    for word in words_tokenized:
        lower_word = word.lower()
        if lower_word.isalpha() and lower_word not in all_stop_words:
            filtered_words.append(lemmatizer.lemmatize(lower_word))
    return ' '.join(filtered_words)





def load_and_preprocess_data(file_paths):
    """

    Apply the adjust_text and load the csv.

    """
    all_dataframes = []

    for file_path in file_paths:
        dataframe = pd.read_csv(file_path)

        dataframe['cleaned_text'] = dataframe['cleaned_text'].apply(adjust_text)

        all_dataframes.append(dataframe)

    combined_dataframe = pd.concat(all_dataframes, ignore_index=True)
    return combined_dataframe


# def filter_conversation_starter(dataframe):
#     """
#
#     Only look at the first tweets so we filter where the id matches the conversation starter id
#
#     """
#
#     dataframe = dataframe[dataframe['_id'] == dataframe['conv_starter']]
#     print(dataframe)
#     dataframe = dataframe["user"] != 56377143
#
#     return dataframe

def vectorize_text(text_data):
    """

    Vectorize and turn into matrices the data so the machine learning model
    can actually use the data.

    """
    vectorizer = CountVectorizer(max_df=0.95, min_df=2, max_features=10000, stop_words='english', ngram_range=(1, 2))
    matrix = vectorizer.fit_transform(text_data)
    return vectorizer, matrix

def fit_lda(matrix):
    """

    Actually fit and apply the LDA to the matrix

    batch_size=128, (NUMBER OF DOCUMENTS TO USE IN EACH EM ITERATION)
    doc_topic_prior=None,
    evaluate_every=-1,
    learning_decay=0.7, (RATE AT WHICH LEARNING UPDATES DECREASE)
    learning_method=None,
    learning_offset=10.0, (A PARAMETER TO DOWN WEIGHT EARLY ITERATIONS)
    max_doc_update_iter=100,
    max_iter=10, (NUMBER OF ITERATIONS WE WANT ALGO TO RUN)
    mean_change_tol=0.001,
    n_components=10, (THE AMOUNT OF TAOPICS WE WANT TO DISCOVER)
    n_jobs=1,
    n_topics=None,
    perp_tol=0.1,
    random_state=None, (RANDOM NUM GENERATOR)
    topic_word_prior=None,
    total_samples=1000000.0,
    verbose=0),

    """

    LDA = LatentDirichletAllocation(
        n_components=10, #number of topics
        max_iter=200, #max num of iterations
        learning_decay=0.9, #learning decay (for
        learning_offset=20,
        batch_size=256, # numer of documents to use for each iter
        random_state=42 #random seed reproducability
    )
    LDA.fit(matrix)
    return LDA


def display_topics(model, feature_names, number_of_top_words):
    """

    We get the topics and then



    """
    topics = {}
    for topic_index, topic in enumerate(model.components_):
        top_word_indices = topic.argsort()[:-number_of_top_words - 1:-1]
        top_words = [feature_names[i] for i in top_word_indices]
        topics[topic_index] = top_words
    return topics

def plot_topic_distribution(topic_counts, topics, title):

    topic_labels = [", ".join(topics[i]) for i in topic_counts.index]

    colors = ['skyblue', 'lightgreen', 'salmon', 'lightgrey', 'lightskyblue', 'peachpuff', 'khaki', 'lightseagreen',
              'plum', 'lightsteelblue']

    wrapped_labels = [textwrap.fill(label, 70) for label in topic_labels]

    plt.figure(figsize=(12, 8))

    plt.xlabel('Amount of Tweets')
    plt.ylabel('Topics')

    plt.yticks(range(len(topic_counts)), wrapped_labels)
    plt.tight_layout()
    plt.gca().invert_yaxis()
    plt.title(title, fontsize=16, pad=20)
    plt.show()


#ONLY FOR NON KLM
def clean_conversations(dataframe, airline_id):
    """

    Based on other code. Only for processing the other airlines besides KLM.


    """
    conv_filter = dataframe[(dataframe["_id"] != dataframe["conv_starter"]) & (dataframe["user"] != airline_id)]
    dataframe.drop(conv_filter.index, inplace=True)

    return dataframe


for airline, paths in file_paths.items():
    # FILTER for the conversation starters only.
    conversations = load_and_preprocess_data(paths['conversations'])

    conversations = clean_conversations(conversations, airline)

    ################ALSO FOR KLM

    #FILTERING THE CONVERSATION STARTERS IN CONVERSATION FUNCTION
    #conversations = filter_conversation_starter(conversations)

    #FILTER BASED ON THE SENTIMENT
    negative_conversations = conversations[conversations['sentiment'] == 'Negative'].copy()
    positive_conversations = conversations[conversations['sentiment'] == 'Positive'].copy()
    neutral_conversations = conversations[conversations['sentiment'] == 'Neutral'].copy()


    #VECTORIZING THE TEXT AND TURNING INTO MATRICES
    vectorizer_negative, matrix_negative = vectorize_text(negative_conversations['cleaned_text'])
    vectorizer_positive, matrix_positive = vectorize_text(positive_conversations['cleaned_text'])
    vectorizer_neutral, matrix_neutral = vectorize_text(neutral_conversations['cleaned_text'])


    #FITTING TO LDA MODEL
    LDA_Negative = fit_lda(matrix_negative)
    LDA_Positive = fit_lda(matrix_positive)
    LDA_Neutral = fit_lda(matrix_neutral)

    ### TOPICS
    topics_negative = display_topics(LDA_Negative, vectorizer_negative.get_feature_names_out(), number_of_top_words)
    topics_positive = display_topics(LDA_Positive, vectorizer_positive.get_feature_names_out(), number_of_top_words)
    topics_neutral = display_topics(LDA_Neutral, vectorizer_neutral.get_feature_names_out(), number_of_top_words)


    #WHAT IS TRANSFORM DOING?
    topic_assignments_negative = LDA_Negative.transform(matrix_negative)
    topic_assignments_positive = LDA_Positive.transform(matrix_positive)
    topic_assignments_neutral = LDA_Neutral.transform(matrix_neutral)


    ### WHAT IS THIS TOPIC ASSIGNMENT ARGMAX DOING?
    negative_conversations.loc[:, 'topic'] = topic_assignments_negative.argmax(axis=1)
    positive_conversations.loc[:, 'topic'] = topic_assignments_positive.argmax(axis=1)
    neutral_conversations.loc[:, 'topic'] = topic_assignments_neutral.argmax(axis=1)


    ###COUNTING THE AMOUNT OF TOPICS IN THE DOCUMENT CLUSTER
    topic_counts_negative = negative_conversations['topic'].value_counts()
    topic_counts_positive = positive_conversations['topic'].value_counts()
    topic_counts_neutral = neutral_conversations['topic'].value_counts()

    plot_topic_distribution(topic_counts_negative, topics_negative,f'{airline.upper()} | Negative Sentiment | Conversation')
    plot_topic_distribution(topic_counts_positive, topics_positive,f'{airline.upper()} | Positive Sentiment | Conversation')
    plot_topic_distribution(topic_counts_neutral, topics_neutral,f'{airline.upper()} | Neutral Sentiment | Conversation')

##################### PROCESSING OF THE NON REPLY TWEET ONLY
if 'non_reply' in file_paths['klm']:
    non_reply_path = file_paths['klm']['non_reply']
    non_reply_df = pd.read_csv(non_reply_path)
    non_reply_df['cleaned_text'] = non_reply_df['cleaned_text'].apply(adjust_text)

    negative_non_reply = non_reply_df[non_reply_df['sentiment'] == 'Negative'].copy()
    positive_non_reply = non_reply_df[non_reply_df['sentiment'] == 'Positive'].copy()
    neutral_non_reply = non_reply_df[non_reply_df['sentiment'] == 'Neutral'].copy()

    vectorizer_negative_non_reply, matrix_negative_non_reply = vectorize_text(negative_non_reply['cleaned_text'])
    vectorizer_positive_non_reply, matrix_positive_non_reply = vectorize_text(positive_non_reply['cleaned_text'])
    vectorizer_neutral_non_reply, matrix_neutral_non_reply = vectorize_text(neutral_non_reply['cleaned_text'])

    lda_negative_non_reply = fit_lda(matrix_negative_non_reply)
    lda_positive_non_reply = fit_lda(matrix_positive_non_reply)
    lda_neutral_non_reply = fit_lda(matrix_neutral_non_reply)

    topics_negative_non_reply = display_topics(lda_negative_non_reply,
                                               vectorizer_negative_non_reply.get_feature_names_out(), number_of_top_words)
    topics_positive_non_reply = display_topics(lda_positive_non_reply,
                                               vectorizer_positive_non_reply.get_feature_names_out(), number_of_top_words)
    topics_neutral_non_reply = display_topics(lda_neutral_non_reply,
                                              vectorizer_neutral_non_reply.get_feature_names_out(), number_of_top_words)

    topic_assignments_negative_non_reply = lda_negative_non_reply.transform(matrix_negative_non_reply)
    topic_assignments_positive_non_reply = lda_positive_non_reply.transform(matrix_positive_non_reply)
    topic_assignments_neutral_non_reply = lda_neutral_non_reply.transform(matrix_neutral_non_reply)

    negative_non_reply.loc[:, 'topic'] = topic_assignments_negative_non_reply.argmax(axis=1)
    positive_non_reply.loc[:, 'topic'] = topic_assignments_positive_non_reply.argmax(axis=1)
    neutral_non_reply.loc[:, 'topic'] = topic_assignments_neutral_non_reply.argmax(axis=1)

    topic_counts_negative_non_reply = negative_non_reply['topic'].value_counts()
    topic_counts_positive_non_reply = positive_non_reply['topic'].value_counts()
    topic_counts_neutral_non_reply = neutral_non_reply['topic'].value_counts()

    plot_topic_distribution(topic_counts_negative_non_reply, topics_negative_non_reply,
                            'KLM Non-reply | Negative Sentiment | Conversation')
    plot_topic_distribution(topic_counts_positive_non_reply, topics_positive_non_reply,
                            'KLM Non-reply | Positive Sentiment | Conversation')
    plot_topic_distribution(topic_counts_neutral_non_reply, topics_neutral_non_reply,
                            'KLM Non-reply | Neutral Sentiment | Conversation')
