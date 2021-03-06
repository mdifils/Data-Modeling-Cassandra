# ----------------------Importing packages---------------------------------
import os
import numpy as np
import pandas as pd
from cassandra.cluster import Cluster

######################### CONNECTION TO APACHE CASSANDRA ######################
# Remember that cassandra service in docker-compose.yml file
# was defined as 'cassandra_node'

cluster = Cluster(['cassandra_node'])

# Setting a session to be able to execute queries
session = cluster.connect()

# --------Dropping tables if keyspace exists or creating a new keyspace--------

# If keyspace exists drop tables
try:
    session.set_keyspace('sparkify_ks')
    for tab in ['length_playlist_session', 'user_playlist_session', 'song_user']:
        query = f"DROP TABLE IF EXISTS sparkify.{tab}"
        session.execute(query)
        print(f"Dropping {tab} ...")

# If keyspace doesn't exists, create a new one
except:
    # creating a new keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkify_ks
        WITH REPLICATION = {
            'class' : 'SimpleStrategy',
            'replication_factor' : 1
        }"""
                    )
    print("KEYSPACE sparkify_ks created")
    # setting the new created keyspace
    session.set_keyspace('sparkify_ks')

######################## LOADING DATA INTO DATAFRAME ##########################


def files_to_dataframe(dataset_folder: str) -> pd.DataFrame:
    """
    - This function takes as input a string representing the data folder.
    - It returns a dataframe that contains all data extracted from all CSV files
    """

    # Get the current working directory
    cwd = os.getcwd()

    # Set the folder path of all files
    files_folderpath = cwd + dataset_folder

    # Get the list of filename
    filename_list = os.listdir(files_folderpath)

    # number of all CSV files
    num_files = len(filename_list)
    print(f'{num_files} files found')

    # I'm going to associate a dataframe to every csv file
    # Then I'm going to put all those dataframes into a list
    # Finally I'm going stack or concatenate vertically all
    # dataframes into a single big dataframe which will have
    # the same number of columns but the number of rows will
    # be the sum of all dataframes.
    df_list = []

    # Every csv file has the same number of columns but here are
    # the interesting ones
    column_list = ['artist', 'firstName', 'gender', 'itemInSession', 'lastName',
                   'length', 'level', 'location', 'sessionId', 'song', 'userId']

    for i, item in enumerate(filename_list, 1):

        # Joinning each filename with files_folderpath to get filepath
        filepath = os.path.join(files_folderpath, item)

        # each file is associated to a dataframe
        df = pd.read_csv(filepath)

        # Taking only interesting columns
        df = df[column_list]

        # Appending each dataframe to the list
        df_list.append(df)
        print(f'{i}/{num_files} files processed')

    # Putting all dataframes together
    all_files_dataframe = pd.concat(df_list, ignore_index=True, axis=0)

    return all_files_dataframe


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Takes dataframe as input and returns a transformed dataframe
    - renames some columns to meet PEP8 requirements
    - Handles NaN for columns of type object (string) 
    """
    # Renaming column names
    df.rename(columns={'firstName': 'first_name',
                       'itemInSession': 'item_in_session',
                                        'lastName': 'last_name',
                                        'sessionId': 'session_id',
                                        'userId': 'user_id'}, inplace=True)

    # Converting NaN into empty string for columns of type object
    # pandas type 'object' means the column values are strings
    # NaN (not a number or null value) is a float (numpy.nan).
    # So when inserting values into tables Cassandra will complain
    # for not being able to convert NaN as text

    # Get the list of dataframe columns
    cols = df.columns
    for col in cols:
        if df.dtypes[col] == object:
            # if the type of column is object then replace
            # all NaN by empty string
            df[col] = df[col].replace(np.nan, '')

    # Replace NaN in user_id column by 0
    # So user_id=0 means null value then
    df['user_id'] = df['user_id'].fillna(0)
    # Then converting user_id from float to integer type
    df['user_id'] = df['user_id'].astype(int)

    return df


df = files_to_dataframe('/event_data')
df = transform(df)

# I could convert this dataframe into CSV
# --df.to_csv('event_data_new.csv', index=False, encoding='utf-8')---
# but I won't use it because I prefer to work with dataframe only

################### PROCESSING FIRST QUERY ##########################

print("\nPROCESSING FIRST QUERY\n")
print("CREATING length_playlist_session table")

# -------------------CREATING TABLE----------------------------------

select_query1 = """
    SELECT artist, song, length 
    FROM length_playlist_session
    WHERE session_id = 338 AND item_in_session = 4
"""

# The WHERE clause gives us the table composite key

create_query1 = """
    CREATE TABLE IF NOT EXISTS length_playlist_session(
        session_id int, 
        item_in_session int, 
        artist text, 
        song text,
        length float,
        PRIMARY KEY(session_id, item_in_session)
    )"""
try:
    session.execute(create_query1)
except Exception as e:
    print(e)

print("INSERTING INTO length_playlist_session")

# ------------------------INSERTING INTO TABLE-------------------------

insert_query1 = """
    INSERT INTO length_playlist_session
    (session_id, item_in_session, artist, song, length)
    VALUES (%s, %s, %s, %s, %s)
"""

# Iteracting over each row of the dataframe
for i, row in df.iterrows():
    # for each row, just take interested valuess
    data = (row.session_id, row.item_in_session,
            row.artist, row.song, row.length)
    session.execute(insert_query1, data)

print("TESTING FIRST QUERY\n")

# -----------------------TESTING-------------------------------------

try:
    rows = session.execute(select_query1)
except Exception as e:
    print(e)

print("""QUERY 1:\n
    Give me the artist, song title and song's length in the music app 
    history that was heard during sessionId = 338, and itemInSession = 4
\n""")

print(f"{'ARTIST' :<20}  {'SONG':<55} LENGTH\n")

for row in rows:
    print(f"{row.artist:<20} {row.song:<55} {row.length}")

##########################PROCESSING SECOND QUERY ##########################

print("\nPROCESSING SECOND QUERY\n")
print("CREATING user_playlist_session table")

# ----------------------CREATING TABLE-------------------------------------

select_query2 = """
    SELECT artist, song, first_name, last_name 
    FROM user_playlist_session
    WHERE user_id = 10 AND session_id = 182
"""

# The WHERE clause gives us the table composite partition key
# (user_id,session_id) Then adding one more clustering column item_in_session
# for sorting reason

create_query2 = """
    CREATE TABLE IF NOT EXISTS user_playlist_session(
        user_id float, 
        session_id int, 
        item_in_session int,
        artist text, 
        song text, 
        first_name text, 
        last_name text,
        PRIMARY KEY((user_id, session_id), item_in_session)
    )"""
try:
    session.execute(create_query2)
except Exception as e:
    print(e)


print("INSERTING INTO user_playlist_session")

# ------------------INSERTING INTO TABLE--------------------------------------

insert_query2 = """
    INSERT INTO user_playlist_session
    (user_id, session_id, item_in_session,
    artist, song, first_name, last_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

for i, row in df.iterrows():
    data = (row.user_id, row.session_id, row.item_in_session,
            row.artist, row.song, row.first_name, row.last_name)
    session.execute(insert_query2, data)

print("TESTING SECOND QUERY\n")

# -----------------------TESTING--------------------------------

try:
    rows = session.execute(select_query2)
except Exception as e:
    print(e)

print("""QUERY 2:\n
    Give me only the following: name of artist, song
    (sorted by itemInSession) and user (first and last name)
    for userid = 10, sessionid = 182
\n""")

print(f"{'ARTIST':<20}  {'SONG':<55} {'FIRSTNAME' :<10} LASTNAME\n")

for row in rows:
    print(f"{row.artist:<20} {row.song:<55} {row.first_name:<10} \
    {row.last_name}")


############################ PROCESSING THIRD QUERY ##########################

print("\nPROCESSING THIRD QUERY\n")
print("CREATING song_user table")

# ---------------------CREATING TABLE--------------------------------

select_query3 = """
    SELECT first_name, last_name 
    FROM song_user
    WHERE song = 'All Hands Against His Own'
"""

# The WHERE clause gives us our partition key
# Then adding user_id as clustering column to make the key unique

create_query3 = """
    CREATE TABLE IF NOT EXISTS song_user(
        song text, 
        user_id float, 
        first_name text,
        last_name text, 
        PRIMARY KEY(song, user_id)
    )"""
try:
    session.execute(create_query3)
except Exception as e:
    print(e)

print("INSERTING INTO song_user")

# --------------INSERTING INTO TABLE--------------------------------

insert_query3 = """
    INSERT INTO song_user(song, user_id, first_name, last_name)
    VALUES (%s, %s, %s, %s)
"""

# partition key may not be empty
df_query3 = df[df['song'] != '']

for i, row in df_query3.iterrows():
    data = (row.song, row.user_id, row.first_name, row.last_name)
    session.execute(insert_query3, data)


print("TESTING THIRD QUERY\n")

# ------------------------------TESTING-------------------------------------

try:
    rows = session.execute(select_query3)
except Exception as e:
    print(e)

print("""QUERY 3:\n
    Give me every user name (first and last) in my music app
    history who listened to the song 'All Hands Against His Own'
\n""")

print(f"{'FIRSTNAME' :<12}  LASTNAME\n")

for row in rows:
    print(f"{row.first_name:<12} {row.last_name}")

###########DROP TABLES CLOSING CONNECTION #####################################

print("\n")
# -----------------DROPPING TABLES-------------------------

try:
    for tab in ['length_playlist_session', 'user_playlist_session', 'song_user']:
        query = f"DROP TABLE IF EXISTS sparkify.{tab}"
        session.execute(query)
        print(f"Dropping {tab} ...")
except:
    print("Tables are not dropped")

# ----------------------DROPPING KEYSPACE------------------------

try:
    r = session.execute("DROP KEYSPACE IF EXISTS sparkify_ks")
    print("\nKEYSPACE sparkify_ks dropped\n")
except:
    print("\nKEYSPACE sparkify_ks not dropped\n")

# ------------------------------CLOSING CONNECTION---------------------

session.shutdown()
cluster.shutdown()
