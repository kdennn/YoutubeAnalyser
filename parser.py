import argparse
import json
import logging
import argparse
import os
import sys
from datetime import datetime
import sqlite3
import pandas as pd
import yt_dlp

# TODO
# I can save the df in a format such as Parquet or Feather?
# what can to parse for?

"""
def init_db():
    conn = sqlite3.connect('youtube_data.db')
    cursor = conn.cursor()
    #Table for Channels
    cursor.execute('''CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, name TEXT, last_updated DATETIME)''')
    # Table for videos
    cursor.execute('''CREATE TABLE IF NOT EXISTS videos (video_id TEXT PRIMARY KEY, title TEXT, is_short BOOLEAN, FOREIGN KEY(channel_id) REFERENCES channels(id))''')
"""

def init_db():
    """Init. the sql Database"""

    conn = sqlite3.connect('youtube_data.db')
    cursor = conn.cursor()
    #Table for Channels
    cursor.execute('''CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, name TEXT, has_shorts BOOLEAN, last_updated DATETIME)''')
    # Table for videos
    # Fixed: Added 'channel_id TEXT' before defining the FOREIGN KEY constraint
    cursor.execute('''CREATE TABLE IF NOT EXISTS videos (video_id TEXT PRIMARY KEY, title TEXT, is_short BOOLEAN, channel_id TEXT, FOREIGN KEY(channel_id) REFERENCES channels(id))''')
    conn.commit()

def get_channel_info(channel_url):
    """Parses yt-Channel url and adds all of its shorts to the db"""
    # connect to db
    conn = sqlite3.connect('youtube_data.db')
    cursor = conn.cursor()

    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'skip_download': True,
        'nocheckcertificate': True,
        'ignoreerrors': True,
        'no_warnings': True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            target_url = channel_url.rstrip('/') + '/shorts'
            #logging.debug(f"{target_url}")
            channel_id = channel_url.replace('https://www.youtube.com/channel/', '')

            try:
                info = ydl.extract_info(target_url, download=False)
            except yt_dlp.utils.DownloadError as e0:
                #cursor.execute("INSERT INTO channels VALUES (?, ?, ?, ?)", (channel_id, "Not added", False, datetime.now()))
                logging.debug(f"Error for Short with {channel_id}: {e0}" )
                return
            except yt_dlp.utils.ExtractorError as e1:
                logging.debug(f"No shorts found for {channel_id}: {e1}")
                return


            if info is None:
                cursor.execute("INSERT OR IGNORE INTO channels VALUES (?, ?, ?, ?)",(channel_id, info.get('title'), False, str(datetime.now())))
                conn.commit()
                logging.debug(f"No shorts found for {channel_id} (channel has no shorts tab)")
                return



            # add channel to channels list to the db
            cursor.execute("INSERT OR IGNORE INTO channels VALUES (?, ?, ?, ?)", (channel_id, info.get('title'), True, str(datetime.now())))
            logging.debug(f"Adding shorts of Channel id: {channel_id}")

            # save Shorts in db
            try:
                entries = info.get('entries', [])
            except Exception as e2:
                logging.debug(f"Can't get info from ydl.extract_info from {target_url} with error {e2}")

            for entry in entries:
                video_id = entry.get('id')
                title = entry.get('title')
                cursor.execute("INSERT OR IGNORE INTO videos VALUES (?, ?, ?, ?)", (video_id, title, True, channel_id))

            logging.debug(f"Added all video to Database, commiting next.")
            conn.commit()

        except Exception as e:
            logging.debug(f"Failed to fetch channel info for {channel_url}: {e}")



def shortsParser(dfarg):
    """This function removes all shorts from a dataframe"""

    # connect to db
    conn = sqlite3.connect('youtube_data.db')
    cursor = conn.cursor()

    # get the dataframe
    # get video list of every yt channel
    # only get list of YouTube Channels with more than 30 Videos

    mask = dfarg.groupby('channel_id')['channel_id'].transform('count') > 30
    #id_channel_list = dfarg.loc[mask, 'channel_id'].drop_duplicates().tolist()

    # check if db is empty, if not -->
    # search every channel in the db, if it's not there request it
    filtered_df = dfarg[mask][['channel_id', 'channel_url']].drop_duplicates()
    cursor.execute("SELECT id FROM channels")

    # add set comprehension
    stored_ids = {item[0] for item in cursor.fetchall()}

    total_rows = len(filtered_df)
    count = 0

    # add all the missing shorts into the database
    for _, row in filtered_df.iterrows():
        count += 1
        # if we already have the info in the db, skip
        if row['channel_id'] not in stored_ids:
            logging.debug(f"Fetching for Channel {count}/{total_rows} info from: {row['channel_url']}")
            get_channel_info(row['channel_url'])

    # Get all video IDs that are marked as shorts in the database
    cursor.execute("SELECT video_id FROM videos")
    short_video_ids = {row[0] for row in cursor.fetchall()}

    # Mark videos in the dataframe that match shorts in the database
    dfarg['is_short'] = dfarg['video_id'].isin(short_video_ids)

    return dfarg



def parsing():
    """read json file into a panda dataframe to parse easily"""

    dfMain = pd.read_json("Verlauf/wiedergabeverlauf.json")
    logging.debug(f"parsed json file to df")

    # clean Dataframe
    logging.debug(f"Staring to clean df")

    dfMain['channel_name'] = dfMain['subtitles'].str[0].str['name']
    dfMain['channel_id'] = dfMain['subtitles'].str[0].str['url'].str.replace('https://www.youtube.com/channel/', '', regex=False)
    dfMain['channel_url'] = dfMain['subtitles'].str[0].str['url']
    dfMain['video_id'] = dfMain['titleUrl'].str.replace('https://www.youtube.com/watch?v=', '', regex=False)
    dfMain['datetime'] = pd.to_datetime(dfMain['time'], format='mixed')
    dfMain['is_short'] = False
    logging.debug(f"Finished formatting df")

    dfMain = shortsParser(dfMain)

    logging.debug(f"Total videos: {len(dfMain)}, Shorts detected: {dfMain['is_short'].sum()}")

    #videos_without_shorts = dfMain[dfMain['is_short'] == False]['channel_name'].value_counts()
    #print(videos_without_shorts.head(10).to_string())


    videos_without_name = dfMain[dfMain['is_short'] == False]['channel_name'].count()
    top10 = dfMain[dfMain['is_short'] == False]['channel_name'].value_counts().head(10)
    #print(f"{videos_without_name} \n {top10}")

    #logging.debug(f"Parsing for Vox channel")
    #filter_ = dfMain[dfMain['channel_name'] == 'Vox']
    #logging.debug(f'finished parsing')
    #print(filter_)


    #parse dataframe and gather general stats

    # parse for time of day by minute a video was watched
    # how often was the same video watched
    # longest video sessions <-- might take longer, calculate video length add times together
    # most frequent words in title


def main():
    parser = argparse.ArgumentParser(
        prog='parser.py',
        description="Loads json file and begins to parses it into dataframes for analysis"
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='Enable verbose output'
    )

    # add function to decide what json file to parse
    args = parser.parse_args()

    # Setup Logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_format = '[%(asctime)s][%(levelname)s]%(message)s'
    if args.verbose:
        log_format = '[%(asctime)s][%(levelname)s][%(module)s:%(lineno)d] %(message)s'

    logging.basicConfig(level=log_level, format=log_format)

    init_db()
    parsing()




if __name__ == "__main__":
    main()