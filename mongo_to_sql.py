import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['spreadsheet_data']
collection = db['imbd_highest_rating']

data = collection.find({}, {'_id': 0}) 

df = pd.DataFrame(list(data))

df = df.replace({np.nan: None})

mysql_conn = mysql.connector.connect(
    host="localhost",
    user="eai_rw",
    password="BidSt3yn100%",
    database="gold_price_data"   # Your MySQL database name
)

cursor = mysql_conn.cursor()

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

