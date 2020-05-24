import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_event;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE stage_event (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession varchar,
    lastName varchar,
    length varchar,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    session_id varchar,
    song varchar,
    status varchar,
    ts varchar,
    user_agent varchar,
    user_id varchar
);

""")

staging_songs_table_create = ("""
CREATE TABLE stage_songs (
    num_songs varchar,
    artist_id varchar,
    artist_latitude float8,
    artist_longitude float8,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float8,
    year int
    );
""")

songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id INT PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id VARCHAR NOT NULL REFERENCES dim_user(user_id),
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL DISTKEY REFERENCES dim_song(song_id),
    artist_id VARCHAR NOT NULL REFERENCES dim_artist(artist_id),
    session_id VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE dim_user(
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
)
DISTSTYLE AUTO;
""")

song_table_create = ("""
CREATE TABLE dim_song(
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL SORTKEY,
    artist_id VARCHAR NOT NULL DISTKEY,
    year INT,
    duration FLOAT8
);
""")

artist_table_create = ("""
CREATE TABLE dim_artist(
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT8,
    longitude FLOAT8
)
DISTSTYLE AUTO;
""")

time_table_create = ("""
CREATE TABLE dim_time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT
)
DISTSTYLE AUTO;

""")

# STAGING TABLES

staging_events_copy = """
copy STAGE_EVENT from '{}'
credentials 'aws_iam_role={}'
region 'us-west-2'
format as JSON '{}';
""".format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = """
copy STAGE_SONGS from '{}'
credentials 'aws_iam_role={}'
region 'us-west-2'
JSON 'auto';
""".format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(
	songplay_id,
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    )
    SELECT 
    DISTINCT 
	md5(events.sessionid || events.start_time) as songplay_id
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
    se.user_id as user_id,
    se.level as level,
    ss.song_id as song_id,
    ss.artist_id as artist_id,
    se.session_id as session_id,
    se.location as location,
    se.user_agent as user_agent
    FROM stage_event se join 
    stage_songs ss
    on se.song = ss.title AND 
    se.artist=ss.artist_name
    AND se.length= ss.duration
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO dim_user (
   user_id,
    first_name,
    last_name,
    gender,
    level)
    select distinct 
    user_id,
    firstName,
    lastName,
    gender,
    level
    from stage_event
""")

song_table_insert = ("""
INSERT INTO dim_song(
    song_id,
    title,
    artist_id,
    year,
    duration) 
    select distinct song_id,
    title,
    artist_id,
    year,
    duration from stage_songs
""")

artist_table_insert = ("""
    INSERT INTO dim_artist(artist_id,
    name,
    location,
    latitude,
    longitude)
    select distinct artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude 
    from stage_songs
    
""")

time_table_insert = ("""
INSERT INTO dim_time (
    start_time,
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
)
        SELECT 
        start_time, 
        EXTRACT(hour from start_time) AS hour,
        EXTRACT(day from start_time) AS day,
        EXTRACT(week from start_time) AS week,
        EXTRACT(month from start_time) AS month,
        EXTRACT(year from start_time) AS year, 
        EXTRACT(dow from start_time) AS weekday 
        FROM (
        select DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time 
        FROM stage_event se
        where se.page='NextSong'
        );

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,song_table_create, artist_table_create, time_table_create, user_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
