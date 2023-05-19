import pymongo
import mysql.connector
import pandas as pd
import numpy as np

# MongoDB connection (replace with your connection details)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['spreadsheet_data']
collection = db['imbd_highest_rating']

# Retrieve data from MongoDB
data = collection.find({}, {'_id': 0}) 

# Convert MongoDB data to DataFrame
df = pd.DataFrame(list(data))

df = df.replace({np.nan: None})

# MySQL connection (replace with your credentials)
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="eai_rw",
    password="BidSt3yn100%",
    database="gold_price_data"
)

cursor = mysql_conn.cursor()

# Insert data into MySQL table
for _, row in df.iterrows():
    insert_query = """
    INSERT INTO imdb_highest_ratings (id, title, type, genres, averageRating, numVotes, releaseYear)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    title=VALUES(title),
    type=VALUES(type),
    genres=VALUES(genres),
    averageRating=VALUES(averageRating),
    numVotes=VALUES(numVotes),
    releaseYear=VALUES(releaseYear)
    """
    cursor.execute(insert_query, tuple(row))

# Commit the transaction and close the connection
mysql_conn.commit()
cursor.close()
mysql_conn.close()

print("Data has been successfully migrated from MongoDB to MySQL.")
