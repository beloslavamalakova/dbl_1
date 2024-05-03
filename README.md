# DBL DATA CHALLENGE

## Packages
In order to run the code, the following packages need to be installed: 
- json 
- pymongo 4.7.0
- os 
- datetime
- timeit

## Python Files

### load_data.py
The first step is to load the data into a database. You will need to install **MongoDB** as the document database of
choice with **MongoDB Compass** as GUI for querying, aggregating, and analyzing the MongoDB data in a visual 
environment.

The data needs to be stored under a specific name in a specific directory. That is, all the separate json 
airlines files need to be stored collectively in a folder named "data". Copy this folder over such that it is in 
the same directory as load_data.py. 

For any further instructions in order to load the data into the database, see the load_data.py file. 

After the loading of the data, check MongoDB Compass to see whether there indeed exists a new database called 
"airlines", which will have all the raw data stored in the "tweets_all" collection. 


