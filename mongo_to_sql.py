import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['spreadsheet_data']
collection = db['imbd_highest_rating']

data = collection.find({}, {'_id': 0}) 

df = pd.DataFrame(list(data))

print(f"Reading mongo data was successful")
