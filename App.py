import pymongo
import csv
import json
import seaborn as sns
import matplotlib.pyplot as plt
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

def topPopularGenres(collection):
    collection.update_many({"$or": [{"score": ""}, {"votes": ""}]}, {"$set": {"score": 0, "votes": 0}})
    intConversion = {
        "$addFields": {
            "convertedScore": { "$toDouble": "$score" },
            "convertedVotes": { "$toDouble": "$votes" },
        }
    }

    project = {
        "$project": { "_id": 0, "name": 1, "genre": 1, "metric": { "$multiply": [ "$convertedScore", "$convertedVotes" ] } },
    }

    sort = {
        "$sort": {"metric": -1}
    }

    limit = { "$limit": 300}

    group = {"$group": {"_id": "$genre", "total_amount": {"$sum": 1}}}

    result = collection.aggregate(
        [
            intConversion,
            project,
            sort,
            limit,
            group
        ]
    )
    #cursor = collection.find().sort({"score": -1}).limit(15)
    
    genres = []
    totals = []

    for json in result:
        print(json)
        genres.append(json["_id"])
        totals.append(json["total_amount"])
    
    figure = plt.figure(figsize = (20, 10))

    plt.bar(genres, totals, color ='blue', width = 0.4)
 
    plt.xlabel("Genres")
    plt.ylabel("Amount of movies")
    plt.title("Top Genres in Top 300 Most Popular Movies")
    plt.show()

def 

if __name__ == "__main__":
    client = pymongo.MongoClient(
        "mongodb+srv://george:sujoysikdar@cluster0.frmhm.mongodb.net/admin?retryWrites=true&w=majority")
    db = client['movie_information']
    collection = db["movies"]

    topPopularGenres(collection)
    