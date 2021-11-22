
The purpose of this project was to help the Sparkify company help analyze their streaming data. Their analytic team is looking to see what music their users are listening to. 

Our analytic department will use Postgres to create a database. We will use Postgres to optimize query on in the database. Lastly, create an ETL pipeline to allow Sparkify to be able to analyze their data. 

The database will be designed to use a star schema. The star schema is the simplest syle of a data mart schema. The star schema consists of one or more fact tables referencing any number of dimensions. This schema also helps with handling simpler queries. 
Dimension tables are used to reduce duplication in the database. 
The db will have 4 dimension tables: 
1.users - users in the app
2.songs - songs in music database
3.artists - artists in music database
4.time - timestamps of records in songplays broken down into specific units

One fact table will be created called songplay

The 4 will be used to create the fact table.



The extract transform and load (ETL) will help move the data from the file into the database. Using python we will create each table, insert the data from the file and load into the database. 

Once the data has been uploaded the Sparkify anayltic team will be able to perform various tasks with data such as reports and/or machine learning models. 

To run the ETL process, the user will need to first run the sql_queries.py. In the terminal screen run the code by first typing in python sql_queries.py. This file is used to create the template for the tables to be created in the database. It provides guidance to remove any duplicate tables and insert the data as well. 
Next, run the create_tables.py use sql_queries.py file to build the tables needed. Lastly, etl.py is the etl used to gather the data from the files on the server and uploaded into the databases to allow the analytic team use the data. 



References
Github - nareshk1290
https://chrisalbon.com/#Python


