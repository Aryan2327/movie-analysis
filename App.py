import pymongo
import csv
import json
from pymongo import MongoClient

def insert(collection):
    with open('movies.csv', mode = 'r', encoding='utf-8') as csv_file:
        dict_objs = csv.DictReader(csv_file)
        id = 1
        for d in dict_objs:
            d["_id"] = id
            #json_obj = json.dumps(d)
            collection.insert_one(d)
            id += 1



if __name__ == "__main__":
    client = pymongo.MongoClient(
        "mongodb+srv://aryan2:password@cluster0.frmhm.mongodb.net/admin?retryWrites=true&w=majority")
    db = client['movie_information']
    collection = db["movies"]