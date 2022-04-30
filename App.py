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

def popular1(collection):
    intConversion = {
        "$addFields": {
            "convertedScore": { "$toDouble": "$score" },
            "convertedVotes": { "$toDouble": "$votes" },
        }
    }
    
    project = {
        "$project": { "_id": 0, "metric": { "$multiply": [ "$convertedScore", "$convertedVotes" ] } },
    }
    
    sort = {
        "$sort": {"metric": -1}
    }
    
    limit = { "$limit": 200 }

    result = collection.aggregate(
        [
            intConversion,
            project,
            sort,
            limit
        ]
    )
    for i in result:
        print(i)



if __name__ == "__main__":
    client = pymongo.MongoClient(
        "mongodb+srv://george:sujoysikdar@cluster0.frmhm.mongodb.net/admin?retryWrites=true&w=majority")
    db = client['movie_information']
    collection = db["movies"]

    popular1(collection)
    