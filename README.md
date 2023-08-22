Assignment1
Spark Core Assignment Dataset Description: 

Consider the two data files (users.csv, transactions.csv). 

 Users file has the following fields: 

User_ID(represent same user id as transaction userid) 

EmailID 

NativeLanguage 

Location 

Transactions file has the following fields: 

Transaction_ID 

Product_ID 

UserID 

Price 

Product_Description 

Questionnaire: 

By making use of Spark Core (i.e., without using Spark SQL) find out: 

Count of unique locations where each product is sold. 

Find out products bought by each user. 

Total spending done by each user on each product. 

Assignment2
Each log line comprises of a standard part (up to .rb:) and an operation-specific part. The standard part fields are like so: 

Logging level, one of DEBUG, INFO, WARN, ERROR (separated by ,) 

A timestamp (separated by ,) 

The downloader id, denoting the downloader instance (separated by --) 

The retrieval stage, denoted by the Ruby class name, one of: 

event_processing 

ght_data_retrieval 

api_client 

retriever 

ghtorrent 

Questions: 

Write a function to load it in an RDD. 

How many lines does the RDD contain?	 

Count the number of WARNing messages 

How many repositories where processed in total? Use the api_client lines only. 

Which client did most HTTP requests? 

Which client did most FAILED HTTP requests? Use group_by to provide an answer. 

What is the most active hour of day? 

What is the most active repository (hint: use messages from the ghtorrent.rb layer only)? 

 
