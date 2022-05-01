import pymongo
import csv
import json
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from pymongo import MongoClient
from sklearn.linear_model import LinearRegression

def insert(collection):
    with open('movies.csv', mode = 'r', encoding='utf-8') as csv_file:
        dict_objs = csv.DictReader(csv_file)
        id = 1
        for d in dict_objs:
            d["_id"] = id
            #json_obj = json.dumps(d)
            collection.insert_one(d)
            id += 1

"""
Use MongoDB's aggregration pipeline to compute the most popular/acclaimed movies based on genre.
Provide a data visualization indicating the disparity of genres among popular/acclaimed films.
"""
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
    

    # Based on official documentation of matplotlib.
    figure = plt.figure(figsize = (20, 10))

    plt.bar(genres, totals, color ='blue', width = 0.4)
 
    plt.xlabel("Genres")
    plt.ylabel("Amount of movies")
    plt.title("Top Genres in Top 300 Most Popular Movies")
    plt.show()

"""
Utilize MongoDB querying to access various collections from the entire database of movie information. Top genre per country map

"""

    
"""
Utilize MongoDB querying to access various collections from the entire database of movie information.
Visualize the relationship between movie budget-revenue ratio and the country of the movie's production GDP. 
We also provide a model via simple linear regression.
"""
def budgetRevenueRelationship(collection1, collection2):
    # result = collection1.aggregate(
    #     [{"$group": {"_id": "$country", "total_amount": {"$sum": 1}}}]
    # )
    # for i in result:
    #     print(i)

    remove_whitespace = {
        "$addFields": {
            "country": {"$trim": {"input": "$Country"}}
        }
    }

    join = {
        "$lookup": {
            "from": "movies",
            "localField": "country",
            "foreignField": "country",
            "as": "MovieInfo"
        }
    }

    # project = {
    #     "$project": {"_id": 0, "country": 1, "GDP ($ per capita)": 1, "Population": 1,
    #                  "budgetRevenueRatio": {"$multiply": ["$budget", "$gross"]}},
    # }

    joined_res = collection2.aggregate([
        remove_whitespace,
        join,
    ])
    # Process data to be scattered/analyzed
    revGross = []
    gdps = []
    for x in joined_res:
        if x["MovieInfo"]:
            total = 0
            count = 0
            for y in x["MovieInfo"]:
                if (len(y["budget"]) != 0 and len(y["gross"]) != 0):
                    temp = float(y["budget"]) / float(y["gross"])
                    y["budgetGrossRatio"] = temp
                    total += temp
                    count += 1
                #print(y)
            if count != 0:
                avg = total / count
                if (avg > 12):
                    for m in x["MovieInfo"]:
                        if ("budgetGrossRatio" in m.keys()):
                            print(m["_id"], m["budgetGrossRatio"])
                revGross.append(avg)
                gdps.append(int(x["GDP ($ per capita)"]))

    #print(revGross)
    #print(gdps)

    figure = plt.figure(figsize=(15, 10))
    plt.scatter(revGross, gdps, color='green')

    plt.xlabel("BudgetGrossRatio")
    plt.ylabel("GDP ($ per capita)")
    plt.title("GDP vs Average Budget-Gross Ratio Per Country")

    model = LinearRegression()
    revGross = np.array(revGross)
    gdps = np.array(gdps)
    model.fit(revGross[:,np.newaxis], gdps)
    #print(model.coef_, model.intercept_)

    plt.plot(revGross, model.predict(revGross[:,np.newaxis]), color='red')

    plt.show()



"""
Utilize MongoDB querying to access various collections from the entire database of movie information.
profit (gross - budget) vs score
"""
def profitScoreRelationship(collection1):

    project = {
        "$project": { "_id": 0, "name": 1, "score": 1, "metric": { "$subtract": [ "$gross", "$budget" ] } },
    }
    profit = []
    score = []
if __name__ == "__main__":
    client = pymongo.MongoClient(
        "mongodb+srv://george:sujoysikdar@cluster0.frmhm.mongodb.net/admin?retryWrites=true&w=majority")
    db = client['movie_information']
    collection1 = db["movies"]
    collection2 = db["countries"]

    while(1):
        print("Select the following data analysis functionalities for our movies NOSQL database: \n 1: Compute the most popular/acclaimed movies based on genre with visualization \n 2: Show the top genre of movies per country to display on map \n 3: Analyze the relationship of GDP vs. the budget-gross ratio of movies per country \n 4: Analyze the relationship between profit vs. the score of a movie")
        inp = input("Enter 1, 2, 3, or 4: ")
        if (inp == "1"):
            topPopularGenres(collection1)
        # elif (input == "2"):
        #     #TODO
        elif (inp == "3"):
            budgetRevenueRelationship(collection1, collection2)
        # elif (input == "4"):
        #     #TODO
        elif (inp == "q"):
            break
        else:
            print("Invalid input! Please type in either 1, 2, 3, or 4.")