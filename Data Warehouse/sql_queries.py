import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table  IF EXISTS staging_events"
staging_songs_table_drop = "drop table  IF EXISTS staging_songs"
songplay_table_drop = "drop table  IF EXISTS songplay_table"
user_table_drop = "drop table IF EXISTS user_table "
song_table_drop = "drop table IF EXISTS song_table"
artist_table_drop = "drop table IF EXISTS artist_table"
time_table_drop = "drop table IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS "staging_events"  (
artist varchar(max),
auth varchar(45),
firstName varchar(45),
gender varchar(45),
itemInSession varchar(45),
lastName varchar(45),
length varchar(45),
level varchar(45),
location varchar(max),
method varchar(45),
page varchar(45),
registration varchar(45),
sessionId varchar(45),
song varchar(max),
status varchar(45),
ts varchar(max),
userAgent varchar(45),
userId int


)
""")

staging_songs_table_create = (""" create table IF NOT EXISTS "staging_songs" (
artist_id varchar(45) ,
artist_latitude varchar(45),
artist_location varchar(max),
artist_longitude varchar(45),
artist_name varchar(max),
duration float,
num_songs varchar(45),
song_id varchar(45),
title varchar(max),
year int)

""")

songplay_table_create = ("""
create table IF NOT EXISTS "songplay_table" (
songplay_id int IDENTITY(0,1) Primary Key Not Null, 
start_time timestamp , 
userId int, 
level varchar(45), 
song_id varchar(45) , 
artist_id varchar(45), 
sessionId varchar(45) , 
location varchar(max) , 
userAgent varchar(45)
)
""")

user_table_create = ("""
create table IF NOT EXISTS "user_table"  (
user_id int ,
firstName varchar(45),
lastName varchar(45),
gender varchar(10),
level varchar(45)
)
""")

song_table_create = ("""
create table IF NOT EXISTS "song_table" (
song_id varchar(45) primary key not null,
title varchar(max),
artist_id varchar(45), 
year int, 
duration int
)
""")

artist_table_create = ("""
create table IF NOT EXISTS "artist_table" (
artist_id varchar(max) primary key, 
artist_name varchar(max),
location varchar(max),
lattitude varchar(max),
longitude varchar(max)

)
""")

time_table_create = ("""
create table IF NOT EXISTS time_table (
start_time timestamp ,
hour int, 
day int ,
week int,
month int,
year int,
weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""

Copy staging_events from 's3://udacity-dend/log_data'
iam_role 'arn:aws:iam::905031933462:role/myRedshiftRole'
json 'auto' compupdate off region 'us-west-2';


""").format()

staging_songs_copy = ("""

Copy staging_songs from 's3://udacity-dend/song_data/A/A/'
iam_role 'arn:aws:iam::905031933462:role/myRedshiftRole'
json 'auto' compupdate off region 'us-west-2';


""").format()

# FINAL TABLES

songplay_table_insert = ("""


INSERT INTO songplay_table (start_time, userId , level, song_id, artist_id, sessionId, location, userAgent) 
select timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
se.userId,
se.level,
ss.song_id, 
ss.artist_id,
se.sessionId, 
se.location,
se.userAgent
from staging_events se
join staging_songs ss
on  se.song = ss.title
and ss.artist_name = se.artist
and se.length = ss.duration
where se.page = 'NextSong';



""")

user_table_insert = ("""

insert into user_table
select 
distinct userId, 
firstName, 
lastName, 
gender, 
level
from staging_events
WHERE page='NextSong';


""")

song_table_insert = ("""
insert into song_table
select 
song_id, 
title, 
artist_id, 
year, 
duration
from staging_songs;


""")

artist_table_insert = ("""
insert into artist_table 
select 
artist_id, 
artist_name, 
artist_location, 
artist_latitude, 
artist_longitude
from staging_songs;





""")

time_table_insert = ("""
insert into time_table (start_time, hour, day, week, month, year, weekDay)
SELECT start_time,
DATE_PART(hrs, start_time) as hour,

DATE_PART(dayofyear, start_time) as day,

DATE_PART(w, start_time) as week,

DATE_PART(mons ,start_time) as month,

DATE_PART(yrs , start_time) as year,

DATE_PART(dow, start_time) as weekDay

from songplay_table;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
