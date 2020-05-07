# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""

	CREATE table IF NOT EXISTS songplays ( \
		songplay_id SERIAL PRIMARY KEY, \
		start_time bigint NOT NULL, \
		user_id int NOT NULL, \
		level text, \
		song_id text, \
		artist_id text, \
		session_id int, \
		location varchar, \
		user_agent varchar \
	);
""")

user_table_create = ("""

	CREATE table IF NOT EXISTS users ( \
		user_id int PRIMARY KEY, \
		first_name varchar, \
		last_name varchar, \
		gender varchar, \
		level text \
	);
""")

song_table_create = ("""

	CREATE table IF NOT EXISTS songs ( \
		song_id text PRIMARY KEY, \
		title varchar, \
		artist_id text NOT NULL, \
		year int, \
		duration float \
	);
""")

artist_table_create = ("""

	CREATE table IF NOT EXISTS artists ( \
		artist_id text PRIMARY KEY, \
		name varchar, \
		location varchar, \
		latitude int, \
		longitude int \
	);
""")

time_table_create = ("""

	CREATE table IF NOT EXISTS time_t ( \
		start_time bigint PRIMARY KEY, \
		hour int, \
		day int, \
		week int, \
		month int, \
		year int, \
		weekday int \
	);
""")

# INSERT RECORDS

songplay_table_insert = (""" 

	INSERT into songplays ( \
		start_time, \
		user_id, \
		level, \
		song_id, \
		artist_id, \
		session_id, \
		location, \
		user_agent \
	)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""

	INSERT into users ( \
		user_id, \
		first_name, \
		last_name, \
		gender, \
		level \
	)
	VALUES (%s, %s, %s, %s, %s)
	ON CONFLICT (user_id)
	DO UPDATE
	SET level = EXCLUDED.level;
""")

song_table_insert = ("""

	INSERT into songs ( \
		song_id, \
		title, \
		artist_id, \
		year, \
		duration \
	)
	VALUES (%s, %s, %s, %s, %s)
	ON CONFLICT (song_id)
	DO NOTHING;
""")

artist_table_insert = ("""

	INSERT into artists ( \
		artist_id, \
		name, \
		location, \
		latitude, \
		longitude \
	)
	VALUES (%s, %s, %s, %s, %s)
	ON CONFLICT (artist_id)
	DO NOTHING;
""")


time_table_insert = ("""

	INSERT into time_t ( \
		start_time, \
		hour, \
		day, \
		week, \
		month, \
		year, \
		weekday \
	)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
	ON CONFLICT (start_time)
	DO NOTHING;
""")


# FIND SONGS

song_select = (""" SELECT s.song_id, a.artist_id
                   FROM songs s
                   JOIN artists a
                   ON s.artist_id = a.artist_id
                   WHERE s.title = %s 
                   AND a.name = %s
                   AND s.duration = %s ;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]