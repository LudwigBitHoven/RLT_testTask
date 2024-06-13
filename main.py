from pymongo import MongoClient

# довольно простая реализация подключения
client = MongoClient("mongodb://localhost:27017/")
db = client.sampleDB
payment_collection = db.sample_collection
