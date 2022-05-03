import pymongo
import csv
import json
import matplotlib.pyplot as plt
import numpy as np
from pymongo import MongoClient
from sklearn.linear_model import LinearRegression
import pygal
from pygal.style import Style
from countryCodes import countryCodes


def insert(collection):
    with open('movies.csv', mode='r', encoding='utf-8') as csv_file:
        dict_objs = csv.DictReader(csv_file)
        id = 1
        for d in dict_objs:
            d["_id"] = id
            # json_obj = json.dumps(d)
            collection.insert_one(d)
            id += 1


"""
Use MongoDB's aggregration pipeline to compute the most popular/acclaimed movies based on genre.
Provide a data visualization indicating the disparity of genres among popular/acclaimed films.
"""


def topPopularGenres(collection):
    collection.update_many({"$or": [{"score": ""}, {"votes": ""}]}, {"$set": {"score": 0, "votes": 0}})
    doubleConversion = {
        "$addFields": {
            "convertedScore": {"$toDouble": "$score"},
            "convertedVotes": {"$toDouble": "$votes"},
        }
    }

    project = {
        "$project": {"_id": 0, "name": 1, "genre": 1, "metric": {"$multiply": ["$convertedScore", "$convertedVotes"]}},
    }

    sort = {
        "$sort": {"metric": -1}
    }

    limit = {"$limit": 300}

    group = {"$group": {"_id": "$genre", "total_amount": {"$sum": 1}}}

    result = collection.aggregate(
        [
            doubleConversion,
            project,
            sort,
            limit,
            group
        ]
    )
    # cursor = collection.find().sort({"score": -1}).limit(15)

    genres = []
    totals = []

    for json in result:
        print(json)
        genres.append(json["_id"])
        totals.append(json["total_amount"])

    # Based on official documentation of matplotlib.
    figure = plt.figure(figsize=(20, 10))

    plt.bar(genres, totals, color='blue', width=0.4)

    plt.xlabel("Genres")
    plt.ylabel("Amount of movies")
    plt.title("Top Genres in Top 300 Most Popular Movies")
    plt.show()


"""
Utilize MongoDB querying to access various collections from the entire database of movie information. Top genre per country map

"""


def topGenreCountry(collection1):
    genresCountryMappings = {}

    results = []
    for key, val in countryCodes.items():
        match = {"$match": {"country": key}}
        doubleConversion = {
            "$addFields": {
                "convertedScore": {"$toDouble": "$score"},
                "convertedVotes": {"$toDouble": "$votes"},
            }
        }
        project = {
            "$project": {"_id": 0, "country": 1, "genre": 1,
                         "metric": {"$multiply": ["$convertedScore", "$convertedVotes"]}},
        }
        sort = {"$sort": {"metric": -1}}  # Descending order sort
        limit = {"$limit": 300}
        group = {"$group": {"_id": "$genre", "total_amount": {"$sum": 1}}}
        maximum1 = {"$sort": {"total_amount": -1}}
        maximum2 = {"$limit": 1}
        res = collection1.aggregate([match, doubleConversion, project, sort, limit, group, maximum1, maximum2])
        for json in res:
            if json["_id"] in genresCountryMappings:
                (genresCountryMappings[json["_id"]]).append(val)
            else:
                genresCountryMappings[json["_id"]] = [val]

    print(genresCountryMappings)
    wmap = pygal.maps.world.World()
    wmap.title = 'Top Genre per Country'

    for key, val in genresCountryMappings.items():
        wmap.add(key, val)
    wmap.render_to_file("worldmap.svg")
    print("success")

# {'_id': 'Kenya', 'total_amount': 1}
# {'_id': 'Yugoslavia', 'total_amount': 5}
# {'_id': 'Spain', 'total_amount': 47}
# {'_id': 'Ireland', 'total_amount': 43}
# {'_id': 'Sweden', 'total_amount': 25}
# {'_id': 'Japan', 'total_amount': 80}
# {'_id': 'Iran', 'total_amount': 10}
# {'_id': 'Czech Republic', 'total_amount': 8}
# {'_id': 'France', 'total_amount': 279}
# {'_id': 'Greece', 'total_amount': 2}
# {'_id': 'Federal Republic of Yugoslavia', 'total_amount': 2}
# {'_id': 'Poland', 'total_amount': 4}
# {'_id': 'Serbia', 'total_amount': 1}
# {'_id': 'Libya', 'total_amount': 1}
# {'_id': 'West Germany', 'total_amount': 12}
# {'_id': 'Canada', 'total_amount': 190}
# {'_id': 'Italy', 'total_amount': 61}
# {'_id': 'Denmark', 'total_amount': 32}
# {'_id': 'Vietnam', 'total_amount': 2}
# {'_id': 'Iceland', 'total_amount': 2}
# {'_id': 'United Kingdom', 'total_amount': 816}
# {'_id': 'Colombia', 'total_amount': 1}
# {'_id': 'Turkey', 'total_amount': 3}
# {'_id': 'Jamaica', 'total_amount': 1}
# {'_id': 'Taiwan', 'total_amount': 7}
# {'_id': 'Malta', 'total_amount': 1}
# {'_id': 'United Arab Emirates', 'total_amount': 2}
# {'_id': 'Netherlands', 'total_amount': 12}
# {'_id': 'New Zealand', 'total_amount': 25}
# {'_id': 'China', 'total_amount': 40}
# {'_id': 'Romania', 'total_amount': 1}
# {'_id': 'Indonesia', 'total_amount': 2}
# {'_id': 'Thailand', 'total_amount': 6}
# {'_id': 'Philippines', 'total_amount': 3}
# {'_id': '', 'total_amount': 3}
# {'_id': 'India', 'total_amount': 62}
# {'_id': 'South Africa', 'total_amount': 8}
# {'_id': 'Israel', 'total_amount': 5}
# {'_id': 'Germany', 'total_amount': 117}
# {'_id': 'Portugal', 'total_amount': 2}
# {'_id': 'Australia', 'total_amount': 92}
# {'_id': 'Soviet Union', 'total_amount': 2}


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
                # print(y)
            if count != 0:
                avg = total / count
                revGross.append(avg)
                gdps.append(int(x["GDP ($ per capita)"]))

    # print(revGross)
    # print(gdps)

    figure = plt.figure(figsize=(15, 10))
    plt.scatter(revGross, gdps, color='green')

    plt.xlabel("BudgetGrossRatio")
    plt.ylabel("GDP ($ per capita)")
    plt.title("GDP vs Average Budget-Gross Ratio Per Country")

    model = LinearRegression()
    revGross = np.array(revGross)
    gdps = np.array(gdps)
    model.fit(revGross[:, np.newaxis], gdps)
    # print(model.coef_, model.intercept_)

    plt.plot(revGross, model.predict(revGross[:, np.newaxis]), color='red')

    plt.show()


"""
Utilize MongoDB querying to access various collections from the entire database of movie information.
profit (gross - budget) vs score. Top directors based on said metric.
"""


def profitScoreMetricAnalysis(collection1):
    collection1.update_many({"$or": [{"score": ""}, {"gross": ""}, {"budget": ""}]},
                            {"$set": {"score": 0, "budget": 0, "gross": 0}})
    doubleConversion = {
        "$addFields": {
            "convertedGross": {"$toDouble": "$gross"},
            "convertedBudget": {"$toDouble": "$budget"},
            "convertedScore": {"$toDouble": "$score"}
        }
    }

    project = {
        "$project": {"_id": 0, "convertedScore": 1, "profit": {"$subtract": ["$convertedGross", "$convertedBudget"]}},
    }

    result = collection1.aggregate(
        [
            doubleConversion,
            project
        ]
    )

    profit = []
    score = []

    for json in result:
        profit.append(json["profit"])
        score.append(json["convertedScore"])

    figure = plt.figure(figsize=(15, 10))
    plt.scatter(profit, score, color='blue')

    plt.xlabel("Profit")
    plt.ylabel("Score")
    plt.title("Profit vs Score")
    profit = np.log(np.array(profit))
    score = np.array(score)
    log_curve = np.polyfit(profit, score, 1)
    print(log_curve)

    """
    model.fit(profit[:,np.newaxis], score)
    plt.plot(profit, model.predict(profit[:,np.newaxis]), color='red')
    plt.show()
    """


if __name__ == "__main__":
    client = pymongo.MongoClient(
        "mongodb+srv://george:sujoysikdar@cluster0.frmhm.mongodb.net/admin?retryWrites=true&w=majority")
    db = client['movie_information']
    collection1 = db["movies"]
    collection2 = db["countries"]

    while (1):
        print(
            "Select the following data analysis functionalities for our movies NOSQL database: \n 1: Compute the most popular/acclaimed movies based on genre with visualization \n 2: Show the top genre of movies per country to display on map \n 3: Analyze the relationship of GDP vs. the budget-gross ratio of movies per country \n 4: Analyze the relationship between profit vs. the score of a movie \n q: Quit program")
        inp = input("Enter 1, 2, 3, or 4: ")
        if (inp == "1"):
            topPopularGenres(collection1)
        elif (inp == "2"):
            topGenreCountry(collection1)
        elif (inp == "3"):
            budgetRevenueRelationship(collection1, collection2)
        elif (inp == "4"):
            profitScoreMetricAnalysis(collection1)
        elif (inp == "q"):
            break
        else:
            print("Invalid input! Please type in either 1, 2, 3, 4, or q.")
