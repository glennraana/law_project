import json
from pymongo import MongoClient

# Koble til MongoDB-databasen "laws"
client = MongoClient("mongodb+srv://Cluster80101:VXJYYkR6bFpL@cluster80101.oa4vk.mongodb.net/Laws?retryWrites=true&w=majority")
db = client.Laws

# Velg eller opprett collection for kilder
sources_collection = db.sources

# Last inn registry.json-filen (pass på at banen er riktig)
with open(r"/Users/glenn/als_project/als/registry.json", "r") as f:
    registry = json.load(f)

# Sett inn alle kildene i databasen
result = sources_collection.insert_many(registry["data_sources"])
print("Inserted source IDs:", result.inserted_ids)
