The purpose of this project is to create a database for a startup called Sparkify. We are asked to create an analytic solution for their music app. For this phase of the project we are building a data warehouse to store the data of the log files and users history. To complete this task we are creating an extracting transform and load (ETL) pipeline to move the logs into the data lake. We are using an application called Spark to copy the data from a s3 bucket. The Spark application will transform into a star schema and afterwards load the data into another s3 bucket. 

The star schema separates business process data into facts, which hold the measurable, quantitative data about a business, and dimensions which are descriptive attributes related to fact data. Examples of fact data include sales price, sale quantity, and time, distance, speed and weight measurements. Related dimension attribute examples include product models, product colors, product sizes, geographic locations, and salesperson names.The fact table in this project is called songplays. It has the following columns songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.  These are the following dimension tables: 
 
1.user- user_id, first_name, last_name, gender, level
2.songs - song_id, title, artist_id, year, duration
3.artists - artist_id, name, location, latitude, longitude
4.time. - tart_time, hour, day, week, month, year, weekday
5.songplay - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


Here is a diagram showing the star schema



[![Screen-Shot-2021-11-20-at-6-42-56-PM.png](https://i.postimg.cc/hPWD5hmR/Screen-Shot-2021-11-20-at-6-42-56-PM.png)](https://postimg.cc/0rn16kZV)



 
Each dimension table has one column that matches the fact table. This gives each column the user to have more that gives more context to each column in the fact table. 

To run the script the user will have to type run the file etl.py. Also the user will need to provide their AWS keys and secert access into the the dl.cfg file. 

def process_song_data(spark, input_data, output_data)
This function will gather the song file from the s3 bucket. 

def process_log_data(spark, input_data, output_data):
This function will gather the process log file 


songplay table output 
| datetime | userId | level | song\_id | artist\_id | sessionId |  location |   userAgent | year | month |
| 11/14/18 5:06       | 10     |  free | SOQFYBD12AB0182188  | ARAADXM1187FB3ECDB  | 484       | Washington-Arling... |Mozilla/5.0(Mac...  | 2018 | 11    |
| 11/14/18 5:06       | 10     |  free | SOGDBUF12A8C140FAA  | AR558FS1187FB45658  | 484       | Washington-Arling... | Mozilla/5.0 (Mac...  | 2018 | 11    |
| 11/19/18 9:14       | 24     |  paid | SOQFYBD12AB0182188  | ARAADXM1187FB3ECDB  | 672       | Lake Havasu City-... | Mozilla/5.0 (Win...  | 2018 | 11    |
| 11/19/18 9:14       | 24     |  paid | SOGDBUF12A8C140FAA  | AR558FS1187FB45658  | 672       | Lake Havasu City-... | Mozilla/5.0 (Win...  | 2018 | 11    |
| 11/27/18 22:35      | 80     |  paid | SOQFYBD12AB0182188  | ARAADXM1187FB3ECDB  | 992       | Portland-South Po... | Mozilla/5.0 (Mac...  | 2018 | 11    |
| 11/27/18 22:35      | 80     |  paid | SOGDBUF12A8C140FAA  | AR558FS1187FB45658  | 992       | Portland-South Po... | Mozilla/5.0 (Mac...  | 2018 | 11    |


reference
https://github.com/nareshk1290/Udacity-Data-Engineering/blob/master/Data%20Lakes%20with%20Spark/Project%20Data%20Lake%20with%20Spark/etl.py