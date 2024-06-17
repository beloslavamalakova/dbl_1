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
- file: load_data_final.py
- file: exploration_data.py
- file: clean_data_final.py
- file: mining_conversations.py
- file: exploration_conversations.py
- file: unadressed_high_follower_business_analysis.py
- file: conversation_context_business.py
- file: demo.py 


## Proposed Order of Execution
### load_data_final.py
The first step is to load the data into a database. You will need to install **MongoDB** as the document database of
choice with **MongoDB Compass** as GUI for querying, aggregating, and analyzing the MongoDB data in a visual 
environment.

The data needs to be stored under a specific name in a specific directory. That is, all the separate json 
airlines files need to be stored collectively in a folder named "data". Copy this folder over such that it is in 
the same directory as load_data.py (main directory). 

Then, the only thing that you need to do is run the entire file. 

After the loading of the data, check MongoDB Compass to see whether there indeed exists a new database called 
"airlines", which will have all the raw data stored in the "tweets_collection" collection. 


### exploration_data.py
After the loading of the data, it is time to explore the raw data. In the file, you will find many subsections that are 
marked by a comment, referring to what it explores. In order to execute that particular exploration, simply unmark the 
line that will present the final result. Any plots that are created will automatically be stored to the "plots"
directory. 

### clean_data_final.py
xxx

### mining_conversations.py
To execute the file, simply run the entire file with no additional steps. 

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

### unadressed_high_follower_business_analysis.py

### conversation_context_business.py


### sentiment hhh


### demo.py
The demo file contains all the relevant plots that will be used on the poster. Simply run the file with no additional 
steps in order to view every plot that was created. 