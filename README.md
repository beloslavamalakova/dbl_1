# DBL DATA CHALLENGE

## Packages
In order to run the code, the following packages were used:
- pymongo 4.7.0
- matplotlib 3.8.4
- pandas 2.1.1
- os 
- datetime
- json 
- timeit

## Main Directory
Overview of the all directories and files in the main directory:
- dir: conversations (empty)
- dir: plots 
- dir: sentiment
  - file: Evaluation.ipynb
  - file: Manual_eval_sample.ipynb
  - file: evaluation.py
  - file: sentiment_analysis.ipynb
  - file: sentiment_analysis_DFs.py
  - file: translation_final.csv
- file: load_data.py
- file: exploration_data.py
- file: remove_retweets.py
- file: mining_conversations.py
- file: exploration_conversations.py
- file: unadressed_high_follower_business_analysis.py
- file: conversation_context_business.py
- file: demo.py 


## Proposed Order of Execution
### load_data.py
The first step is to load the data into a database. You will need to install **MongoDB** as the document database of
choice with **MongoDB Compass** as GUI for querying, aggregating, and analyzing the MongoDB data in a visual 
environment.

The data needs to be stored under a specific name in a specific directory. That is, all the separate json 
airlines files need to be stored collectively in a folder named "data". Copy this folder over such that it is in 
the same directory as load_data.py (main directory). 

Then, the only thing that you need to do is run the entire file. During the loading of the data, it immediately includes 
only the selected attributes. 

After the loading of the data, check MongoDB Compass to see whether there indeed exists a new database called 
"airlines", which will have all the raw data stored in the "tweets_collection" collection. 


### exploration_data.py
After the loading of the data, it is time to explore the raw data. In the file, you will find many subsections that are 
marked by a comment, referring to what it explores. In order to execute that particular exploration, simply unmark the 
line that will present the final result. Any plots that are created will automatically be stored to the "plots"
directory. 

### remove_retweets.py
This file removes retweets from the database. Simply run the file without any additional steps. In the end, it returns
a count of the number of retweets that were removed. 

### mining_conversations.py
To execute the file, simply run the entire file with no additional steps. 

IMPORTANT: being tabbed out of the window that is running the code might break the connection with mongoDB. Which will 
result in the code failing to run. So it is advised to not tab out while running this file. The expected running time of 
this file is around 1.5h.

This python file mines the raw conversations for the following airlines: KLM, AirFrance, British Airways and Lufthansa. 
The conversations are stored as csv files in the format (name of the airline)_conversations.csv
in the "conversations" directory. The time it takes to mine the conversations will be shown in the terminal for each 
airline. For KLM, it should take within 10 minutes and even shorter for AirFrance and Lufthansa. However, the expected time for 
British Airways is very high. 

Furthermore, it also collects all single non-reply tweets directed at KLM, which are also stored in the "conversations"
directory beneath the name of "klm_non_reply_tweets.csv".

### exploration_conversations.py
After the mining of the conversations, it is important to explore the various conversations and non-reply tweets 
specific to KLM. Similar to the "data_explorations.py" file, the exploration is divided into smaller subsections. 
In order to visualize the result, mark out the final line of execution that will produce the result. Any plots that 
are created will be automatically stored to the "plots" directory. The exploration is basic, providing results on topics 
such as conversation length, distribution and response rate per language. Any further complex exploration specific to the 
business idea will be handled in separate files that follow below. 

### sentiment/translation_google_colab.ipynb
This can ONLY be run in Google Colab. And can be run at any point after running sentiment/sentiment_analysis_DFs.py. 
The only extra package that needs to be installed is dl-translate. The pip-install code for this is in the notebook. 
This notebook takes the Dutch tweets from KLM's conversations, translates the Dutch text to English. And computes the 
sentiment with VADER.
In the notebook, the klm_conv_standard has to be uploaded. And when everything is done it will download the resulting 
dataframe as "translation_final.csv". The notebook has extra instructions for all these steps.

### unadressed_high_follower_business_analysis.py
To run this file you need to have pandas installed. 
In the unadressed_high_follower_business_analyiss.py file, we determine basic row counts for starter tweets.
Furthermore, we determine the number of conversation starters and starters not replied to for both influencers (> 10000 followers)
and non-influencers.

### conversation_context_business.py

Packages needed to run:

import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import matplotlib.pyplot as plt
import textwrap
nltk.download('stopwords')
nltk.download('punkt') 
nltk.download('wordnet')








### sentiment - sentiment_analysis_DFs.py
This file runs sentiment analysis using Vader on KLM and its competitors, saving the sentiment (Positive/Neutral/Negative)
and the sentiment score of each tweet. Then, in "sentiment_analysis.ipynb" the sentiment evolution of conversations is analyzed.

### Testing of the results:
We tested both with manually labeled data and with an already existing dataset on Kaggle: https://www.kaggle.com/datasets/crowdflower/twitter-airline-sentiment/data
The notebook for testing is Evaluation.ipynb


### demo.py
The demo file contains all the relevant plots that will be used on the poster. Simply run the file with no additional 
steps in order to view every plot that was created. 
