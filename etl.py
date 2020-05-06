import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *

# Opens a song file and process the data into the song table and the artist table
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    df = df.where(pd.notnull(df), None)

    # insert song record
    song_data = df.iloc[[0],[6,7,1,9,8]].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.iloc[[0],[1,5,4,2,3]].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)

# Opens a user log file and process the data into the time, user, and songplay tables
def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)
    df = df.where(pd.notnull(df), None)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = df["ts"].tolist()
    
    # insert time data records
    time_data = []
    for i in t:
        time_data.append(datetime.datetime.fromtimestamp(i/1000.0))

    column_labels = []
    for i in time_data:
        column_labels.append([i.timestamp(), i.hour, i.day, i.isocalendar()[1], i.month, i.year, i.weekday()])
    
    time_df = pd.DataFrame(column_labels, columns = ["timestamp", "hour", "day", "week_of_year", "month", "year", "weekday"])

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.iloc[[0],[-1,2,5,3,7]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song.replace("'", "''"), row.artist.replace("'", "''"), row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

# Gets the file extensions of the appropriate given data files extracted from the data.tar
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=1234")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()