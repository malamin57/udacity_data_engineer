The purpose of this project is to create a database for a startup called Sparkify. We are asked to create an analytic solution for their music app. For this phase of the project we are building a data warehouse to store the data of the log files and users history. To complete this task we are creating an extracting transform and load (ETL) pipeline to move the logs into the data warehouse. We are Amazon Web Service (AWS) redshift to copy the logs which are in json format and located in a S3 bucket. Once the data has been extracted, the data will be transformed to follow a star schema for the data. 

The star schema separates business process data into facts, which hold the measurable, quantitative data about a business, and dimensions which are descriptive attributes related to fact data. Examples of fact data include sales price, sale quantity, and time, distance, speed and weight measurements. Related dimension attribute examples include product models, product colors, product sizes, geographic locations, and salesperson names.The fact table in this project is called songplays. It has the following columns songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.  These are the following dimension tables: 
 
1.user- user_id, first_name, last_name, gender, level
2.songs - song_id, title, artist_id, year, duration
3.artists - artist_id, name, location, latitude, longitude
4.time. - tart_time, hour, day, week, month, year, weekday
 
Each dimension table has one column that matches the fact table. This gives each column the user to have more that gives more context to each column in the fact table. 

To run scripts, the user will need to first run the create_tables.py file. Type python create_tables.py in the terminal. 
This script will create the tables to allow the data to be into tables and has instructions on how to insert the tables. 
Next run the etl.py scripts to begin the etl process to move the data from the log file into the redshift data warehouse. 


Below are a sample output for each of the dimension 

select * from artist_table

| artist\_id         | artist\_name                 | location          | latitude | longitude  |
| ------------------ | ---------------------------- | ----------------- | -------- | ---------- |
| ARPLTRF11E8F5C15C5 | Lost Immigrants              | United States     |          |
| ARNUJQM1187FB3EE72 | Number Twelve Looks Like You | Bergen County, NJ |          |
| AR7WK5411A348EF5EA | Minitel Rose                 | PARIS - NANTES    | 48.85692 | 2.34121    |
| AR1Y2PT1187FB5B9CE | John Wesley                  | Brandon           | 27.94017 | \-82.32547 |

select * from song_table

| song\_id           | title          | artist\_id         | year | duration |
| ------------------ | -------------- | ------------------ | ---- | -------- |
| SOEAXKZ12A81C208DA | Just A Girl    | ARJP2NO1187FB3AFB9 | 1999 | 256      |
| SOCKYOP12A58A7C4EC | Drama          | AROJWW21187FB574E6 | 2003 | 195      |
| SOIGIVK12AB018E9AA | Ionized        | ARNCNV91187FB4D552 | 1996 | 444      |
| SOEBWSU12AB018377B | Sane As Can Be | ARDTIOA1187B99E779 | 2000 | 105      |

select * from user_table

| user\_id | firstname | lastname | gender | level |
| -------- | --------- | -------- | ------ | ----- |
|          |           |          | M      | free  |
|          |           |          | M      | free  |
|          |           |          | F      | free  |
|          |           |          | F      | free  |
            
select * from time_table;

| start\_time | hour | day | week | month | year | weekday |
| ----------- | ---- | --- | ---- | ----- | ---- | ------- |
| 09:11.8     | 18   | 331 | 48   | 11    | 2018 | 2       |
| 33:59.8     | 8    | 330 | 48   | 11    | 2018 | 1       |
| 33:56.8     | 15   | 330 | 48   | 11    | 2018 | 1       |
| 43:00.8     | 12   | 328 | 47   | 11    | 2018 | 6       |
| 00:37.8     | 15   | 315 | 45   | 11    | 2018 | 0       |
| 46:38.8     | 17   | 324 | 47   | 11    | 2018 | 2       |
| 21:12.8     | 14   | 320 | 46   | 11    | 2018 | 5       |
| 25:34.8     | 18   | 330 | 48   | 11    | 2018 | 1       |

select * from songplay_table;

| songplay\_id | start\_time | userid | level | song\_id           | artist\_id         | sessionid | location                                       | useragent |
| ------------ | ----------- | ------ | ----- | ------------------ | ------------------ | --------- | ---------------------------------------------- | --------- |
| 1            | 21:12.8     |        | paid  | SOLRYQR12A670215BF | ARNLO5S1187B9B80CC |           | Red Bluff, CA                                  |           |
| 5            | 43:00.8     |        | paid  | SONQBUB12A6D4F8ED0 | ARFCUN31187B9AD578 |           | Tampa-St. Petersburg-Clearwater, FL            |           |
| 9            | 25:34.8     |        | free  | SONQBUB12A6D4F8ED0 | ARFCUN31187B9AD578 |           | Palestine, TX                                  |           |
| 2            | 33:56.8     |        | paid  | SODOLVO12B0B80B2F4 | AR6XPWV1187B9ADAEB |           | Detroit-Warren-Dearborn, MI                    |           |
| 3            | 46:38.8     |        | paid  | SOCHRXB12A8AE48069 | ARTDQRC1187FB4EFD4 |           | San Francisco-Oakland-Hayward, CA              |           |
| 7            | 33:59.8     |        | paid  | SOVWWJW12A670206BE | AR3ZL6A1187B995B37 |           | Waterloo-Cedar Falls, IA                       |           |
| 0            | 00:37.8     |        | free  | SOCHRXB12A8AE48069 | ARTDQRC1187FB4EFD4 |           | Nashville-Davidson--Murfreesboro--Franklin, TN |           |
| 4            | 09:11.8     |        | paid  | SOCHRXB12A8AE48069 | ARTDQRC1187FB4EFD4 |           | San Francisco-Oakland-Hayward, CA              |           |

Reference
https://ruslanmv.com/blog/Create-Data-Warehouse-with-Redshift

https://knowledge.udacity.com/questions/148404

 
