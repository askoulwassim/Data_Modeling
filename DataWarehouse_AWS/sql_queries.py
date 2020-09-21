import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists1"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""

	CREATE TABLE IF NOT EXISTS staging_events ( \
		artist text, \
		auth text, \
		first_name text, \
		gender text, \
		ItemInSession int, \
		lastName text, \
		length float, \
		level text, \
		location text, \
		method text, \
		page text, \
		registration text, \
		sessionId int, \
		song text, \
		status int, \
		ts timestamp, \
		user_agent text, \
		user_id int \
	);

""")

staging_songs_table_create = ("""

	CREATE TABLE IF NOT EXISTS staging_songs ( \
		song_id text, \
		artist_id text, \
		artist_latitude float, \
		artist_longitude float, \
		artist_location text, \
		artist_name text, \
		duration float, \
		num_songs int, \
		title text, \
		year int \
	);

""")

songplay_table_create = ("""

	CREATE table IF NOT EXISTS songplays ( \
		songplay_id IDENTITY(0,1) PRIMARY KEY sortkey distkey, \
		start_time bigint NOT NULL sortkey, \
		user_id int NOT NULL sortkey, \
		level text, \
		song_id text NOT NULL sortkey, \
		artist_id text NOT NULL sortkey, \
		session_id int, \
		location varchar, \
		user_agent varchar \
	);
""")

user_table_create = ("""

	CREATE table IF NOT EXISTS users ( \
		user_id int PRIMARY KEY sortkey, \
		first_name varchar, \
		last_name varchar, \
		gender varchar, \
		level text \
	);
""")

song_table_create = ("""

	CREATE table IF NOT EXISTS songs ( \
		song_id text PRIMARY KEY sortkey, \
		title varchar, \
		artist_id text NOT NULL, \
		year int, \
		duration float \
	);
""")

artist_table_create = ("""

	CREATE table IF NOT EXISTS artists ( \
		artist_id text PRIMARY KEY sortkey, \
		name varchar, \
		location varchar, \
		latitude float, \
		longitude float \
	);
""")

time_table_create = ("""

	CREATE table IF NOT EXISTS time_t ( \
		start_time bigint PRIMARY KEY sortkey, \
		hour int, \
		day int, \
		week int, \
		month int, \
		year int, \
		weekday int \
	);
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                            from {} credentials  
                            'aws_iam_role={}'   
                            json {}  
                            compupdate off
                            region 'us-west-2';
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""copy staging_songs
							from {} credentials 
							'aws_iam_role={}' 
							JSON 'auto' truncatecolumns 
							compupdate off 
							region 'us-west-2';
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

user_table_insert = ("""

    INSERT INTO users(user_id, first_name, last_name, gender, level)

                SELECT DISTINCT
                      e.userId     as user_id,
                      e.firstName  as first_name,
                      e.lastName   as last_name,
                      e.gender     as gender,
                      e.level      as level

                FROM  public.staging_events e
                WHERE e.page   = 'NextSong'
                  and e.userId NOT IN (
                             SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""

    INSERT INTO songs(song_id, title, artist_id, year, duration)
    
                SELECT DISTINCT
                      s.song_id     as song_id,
                      s.title       as title,
                      s.artist_id   as artist_id,
                      s.year        as year,
                      s.duration    as duration

                FROM  public.staging_songs s;
""")

artist_table_insert = ("""

    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    
                SELECT DISTINCT
                      s.artist_id           as artist_id,
                      s.artist_name         as name,
                      s.artist_location     as location,
                      s.artist_latitude     as latitude,
                      s.artist_longitude    as longitude

                FROM  public.staging_songs s;
""")

songplay_table_insert = ("""

	INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

			SELECT ts, userId, level, song_id, artist_id, sessionId, location, userAgent

				FROM (
					SELECT se.ts, se.userId, se.level, sa.song_id, sa.artist_id, se.sessionId, se.location, se.userAgent
					FROM staging_events se

					JOIN (
						SELECT songs.song_id, artists.artist_id, songs.title, artists.name, songs.duration
						FROM songs

						JOIN artists
						ON songs.artist_id = artists.artist_id) AS sa
						ON (sa.title = se.song
						AND sa.name = se.artist
						AND sa.duration = se.length)
						WHERE se.page = 'NextSong');

""")

time_table_insert = ("""

	INSERT INTO time_t(start_time, hour, day, week, month, year, weekday)

			SELECT a.start_time,
					EXTRACT (HOUR FROM a.start_time), \
					EXTRACT (DAY FROM a.start_time), \
					EXTRACT (WEEK FROM a.start_time), \
					EXTRACT (MONTH FROM a.start_time), \
					EXTRACT (YEAR FROM a.start_time), \
					EXTRACT (WEEKDAY FROM a.start_time) 

			FROM (SELECT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM songplays) a;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]
