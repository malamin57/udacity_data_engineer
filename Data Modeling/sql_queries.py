# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists 
songplays (songplay_id SERIAL PRIMARY KEY,start_time timestamp ,user_id int,level varchar,song_id varchar,artist_id varchar,session_id int,location varchar,user_agent varchar
);
""")

user_table_create = ("""
create table if not exists users (
    user_id int PRIMARY KEY,first_name varchar,last_name varchar,gender varchar,level varchar
);
""")

song_table_create = ("""
create table if not exists songs (
    song_id varchar PRIMARY KEY,title varchar,artist_id varchar,year int,duration int
);
""")

artist_table_create = ("""
create table if not exists artists (
    artist_id varchar PRIMARY KEY,name varchar,location varchar,lattitude float,longitude float
);
""")

time_table_create = ("""
create table if not exists time (
    start_time TIMESTAMP PRIMARY KEY,hour int,day int,week int,month int,year int,weekday int
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) on conflict (songplay_id) do nothing
;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s) on conflict do nothing;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
VALUES (%s, %s, %s, %s, %s) on conflict do nothing;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s) on conflict do nothing;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, s.artist_id FROM songs s 
JOIN artists a on s.artist_id = a.artist_id
WHERE s.title = %s
AND a.name = %s
AND s.duration = %s
;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

