from pymongo import MongoClient
import os
import sys
import random
import re
from tabulate import tabulate
from colorama import init, Fore, Back, Style
init(strip=not sys.stdout.isatty())

def connectMongo(con):
    conn = MongoClient('mongodb+srv://apraveen:dsci551@cluster0.qae19sx.mongodb.net/?retryWrites=true&w=majority')
    db1 = conn["dsci551"]
    db = db1[con]
    return db


db2 = connectMongo('dataNode')



def printf(res):
    printb("----------------------------------------------------------------------------------------------------------")
    printb("COMBINED RESULT")
    printb("----------------------------------------------------------------------------------------------------------")
    for i in range(1,len(res)+1):
        for j in res[i-1]:
            print(j)
    printb("----------------------------------------------------------------------------------------------------------")

def eprintf(res):
    # printb("----------------------------------------------------------------------------------------------------------")
    for i in range(1,len(res)+1):
        printb(
            "----------------------------------------------------------------------------------------------------------")
        printb("PARTITION"+str(i))
        printb(
            "----------------------------------------------------------------------------------------------------------")
        for j in res[i-1]:
            print(j)
        print("\n")
    # printb("----------------------------------------------------------------------------------------------------------")

def printb(string):
    print('\033[1m' + str(string) + '\033[0m')

def inputb(string):
    return input('\033[1m' + str(string) + '\033[0m')

def projectOp(projectList, file, num):
    pList = {}
    for p in projectList:
        pList[p] = "$"+str(file)+str(num)+"."+p
    pList["_id"] = 0
    return pList


# -----------------------------------------------------searchFunction 1----------------------------------------------------
# Function 1 - retrieve the details of the movie by searching for the title given by the user
# After the step 1 we repeat it for all three partitions and combine the results
def searchFunc1(file, aName, mName, pList, numP):
    finalRes = []
    file = file.strip(".csv")
    if pList == "all":
        pTemp = {"_id": 0}
    for p in range(1,numP+1):
        if pList != "all":
            pTemp = projectOp(pList, file, p)
        pRes = db2.aggregate([{"$unwind": "$"+str(file)+str(p)},{"$match": {str(file)+str(p)+"."+str(aName): mName}},{"$project": pTemp}])
        finalRes.append(list(pRes))
    return finalRes

# -----------------------------------------------------searchFunction 2----------------------------------------------------
# Function 2 - retrieve the movie details of the movies between ratings a,b
def searchFunc2(file, aName, gt, lt, pList, numP):
    finalRes = []
    gt,lt = float(gt),float(lt)
    file = file.strip(".csv")
    if pList == "all":
        pTemp = {"_id": 0}
    for p in range(1,numP+1):
        if pList != "all":
            pTemp = projectOp(pList, file, p)
        # print(pList)
        pRes = db2.aggregate([{"$unwind": "$"+str(file)+str(p)},{"$match": {str(file)+str(p)+"."+str(aName): {"$gt": gt, "$lt": lt}}},{"$project": pTemp}])
        finalRes.append(list(pRes))
    return finalRes
# {"$gt": [{"$toInt":"$rating"}, 7]}

def searchFunc3(file, aName, sName, pList, numP):
    finalRes = []
    file = file.strip(".csv")
    if pList == "all":
        pTemp = {"_id": 0}
    for p in range(1,numP+1):
        if pList != "all":
            pTemp = projectOp(pList, file, p)
        pRes = db2.aggregate(
            [{"$unwind": "$"+str(file)+str(p)}, {"$match": {str(file) + str(p) + "."+str(aName): {"$regex": sName}}},
             {"$project": pTemp}])
        finalRes.append(list(pRes))
    return finalRes

def q1(X,p):
    res = db2.aggregate([{"$unwind": "$movie" + str(p)}, {"$match": {"movie" + str(p) + ".certificate": "U"}},
                   {"$project": {"title": "$movie" + str(p) + ".title", "_id": 0}}, {"$sort": {"title": -1}},
                   {"$limit": X}])
    return res

def q2(X,p):
    res = db2.aggregate([{"$unwind": "$movie" + str(p)}, {"$match": {"movie"+str(p)+".rating": {"$gte": 8, "$lte": 10}}},
                   {"$project": {"title1": "$movie" + str(p) + ".title", "_id": 0, "rating": "$movie"+str(p)+".rating"}},
                   {"$sort": {"rating": -1}},
                   {"$limit": X}])
    return res

def q2_1(X,p):
    res = db2.aggregate(
        [{"$unwind": "$movie" + str(p)}, {"$match": {"movie" + str(p) + ".rating": {"$gte": 8, "$lte": 10}}},
         {"$project": {"title1": "$movie" + str(p) + ".title", "_id": 0, "rating": "$movie" + str(p) + ".rating"}},
         {"$sort": {"rating": -1}},
         {"$limit": X},
         {"$project": {"title": {"$concat": ["$title1",":",{"$toString":"$rating"}]}}}])
    return res

def q3(X,p):
    res = db2.aggregate(
        [{"$unwind": "$movie" + str(p)}, {"$match": {"movie" + str(p) + ".runtime": {"$gte": 0, "$lte": 200}}},
         {"$project": {"title1": "$movie" + str(p) + ".title", "_id": 0, "runtime": "$movie" + str(p) + ".runtime"}},
         {"$sort": {"runtime": 1}},
         {"$limit": X}])
    return res

def q3_1(X,p):
    res = db2.aggregate(
        [{"$unwind": "$movie" + str(p)}, {"$match": {"movie" + str(p) + ".runtime": {"$gte": 0, "$lte": 200}}},
         {"$project": {"title1": "$movie" + str(p) + ".title", "_id": 0, "runtime": "$movie" + str(p) + ".runtime"}},
         {"$sort": {"runtime": 1}},
         {"$limit": X},
         {"$project": {"title": {"$concat": ["$title1",":",{"$toString":"$runtime"}]}}}])
    return res


def getQuery(query,X ,p):
    if query == 1:
        return q1(X,p)
    elif query == 2:
        return q2(X,p),q2_1(X,p)
    elif query == 3:
        return q3(X,p),q3_1(X,p)

def inOp(query, X, numP):
    certList = []
    valList = []
    for p in range(1,numP+1):
        tempList = []
        pRes = getQuery(query,X,p)
        pRes = list(pRes)
        # print(pRes)
        for p in pRes:
            tempList.append(p["title"])
        certList.append(tempList)

    certList1 = [j for i in certList for j in i]
    certList1 = sorted(certList1, reverse=True)[:X]
    valList.append(certList1)

    print()
    printb("MAP PHASE DESCRIPTION")
    printb("The input from each partition goes into three separate Mappers")
    printb("If we take demo.csv as an example with 3 partitioned files")
    printb("demo1 --> map1, demo2 --> map2, demo3 --> map3")
    printb("Within each mapper we return the search condition and output records which")
    printb("equate to this condition")
    print()
    print('\033[1m' + "PARTITIONED RESULTS" + '\033[0m')
    certList = [list(x) for x in zip(*certList)]
    header = ["Partition1","Partition2","Partition3","Partiton4"]
    print(tabulate(certList, header, tablefmt="psql"))

    print()
    printb("REDUCE PHASE DESCRIPTION")
    printb("The job of the reducer is to then apply some aggregation operation by combining together")
    printb("All these outputs from the map phase. In this case we just do groupBy and output the")
    printb("the combined results. This is the case for search based functions. However for analytics")
    printb("based queries we will be reducing each key by applying an operation after grouping.")
    print()

    print('\033[1m' + "COMBINED RESULTS" + '\033[0m')
    header = ["Combined"]
    valList = [list(x) for x in zip(*valList)]
    print(tabulate(valList, header, tablefmt="psql"))

def inOp1(query, X, numP):
    certList = []
    valList = []
    valList1 = []

    for p in range(1,numP+1):
        tempList1,tempList2 = [],[]
        pRes,pRes1 = getQuery(query,X,p)
        pRes,pRes1 = list(pRes),list(pRes1)
        for p in pRes1:
            tempList2.append(p['title'])
        valList.append(tempList2)
        for p in pRes:
            tempList1.append(list(p.values()))
        certList.append(tempList1)
    # print(certList)

    certList1 = [j for i in certList for j in i]
    if query == 2:
        a = True
    else:
        a = False
    certList1 = sorted(certList1,key=lambda x: (x[1],x[0]),reverse=a)[:X]
    certListFinal = []

    for i in certList1:
        string=":".join(map(str,i))
        certListFinal.append(string)
    valList1.append(certListFinal)

    print()
    printb("MAP PHASE DESCRIPTION")
    printb("The input from each partition goes into three separate Mappers")
    printb("If we take demo.csv as an example with 3 partitioned files")
    printb("demo1 --> map1, demo2 --> map2, demo3 --> map3")
    printb("Within each mapper we return the search condition and output records which")
    printb("equate to this condition")
    print()
    print('\033[1m' + "PARTITIONED RESULTS" + '\033[0m')
    valList = [list(x) for x in zip(*valList)]
    header = ["Partition1","Partition2","Partition3","Partiton4"]
    print(tabulate(valList, header, tablefmt="psql"))

    print()
    printb("REDUCE PHASE DESCRIPTION")
    printb("The job of the reducer is to then apply some aggregation operation by combining together")
    printb("All these outputs from the map phase. In this case we just do groupBy and output the")
    printb("the combined results. This is the case for search based functions. However for analytics")
    printb("based queries we will be reducing each key by applying an operation after grouping.")
    print()

    print('\033[1m' + "COMBINED RESULTS" + '\033[0m')
    header = ["Combined"]
    valList1 = [list(x) for x in zip(*valList1)]
    print(tabulate(valList1, header, tablefmt="psql"))

# # -----------------------------------------------------searchFunction 3----------------------------------------------------
# # Function 3 - retrieve the movie details of the movies whose stars include person A
# def searchFunc3(sName, pList):
#     finalRes = []
#     if pList == "all":
#         pTemp = {"_id": 0}
#     for p in range(1, 4):
#         if pList != "all":
#             pTemp = projectOp(pList, p)
#         # print(pList)
#         pRes = db2.aggregate([{"$unwind": "$movie"+str(p)}, {"$match": {"movie"+str(p)+".stars": {"$regex": sName}}},{"$project": pTemp}])
#         finalRes.append(list(pRes))
#     print(finalRes)
#
#
# def searchFunc4(certName, pList):
#     finalRes = []
#     if pList == "all":
#         pTemp = {"_id": 0}
#     for p in range(1, 4):
#         if pList != "all":
#             pTemp = projectOp(pList, p)
#         # print(pList)
#         pRes = db2.aggregate([{"$unwind": "$movie"+str(p)}, {"$match": {"movie"+str(p)+".certificate": certName}},{"$project": pTemp}])
#         finalRes.append(list(pRes))
#     print(finalRes)
#
#
# def searchFunc5():
#
#     p1Res = db2.aggregate([{"$unwind": "$movie1"}, {"$match": {"movie1.genre": {"$regex": "Crime"}}},{"$project": {"movie1.genre": 1}}])
#     p1Res = db2.aggregate(
#         [{"$unwind": "$movie1"}, {"$match": {"movie1.genre": {"$regex": "Romance"}}}, {"$project": {"movie1.genre": 1}}])
#     print(list(p1Res))


# if vCheck == "kasondfs" and len(vTemp) == 5:
#     # if len(vTemp) == 4:
#     #     temp = 1
#     # else:
#     #     if vTemp[4]=="-e":
#     #         temp = 0
#     #     else:
#     #         print("Check syntax")
#     #         continue
#     if vTemp[1] in searchList:
#         numP = getNumPartitions()
#         if vTemp[1] == 'title':
#             # print(list)
#             p2, p1 = searchPrep(vTemp)
#             res = searchFunc1(p1, p2, numP)
#             if temp == 1:
#                 printf(res)
#             else:
#                 eprintf(res)
#             continue

if __name__ == '__main__':
    # insertData("movies.csv", "", 3)
    # printf(searchFunc3("movies.csv","stars","Al Pacino",["rating","title","stars"],4))
    # searchFunc4("12A",["title", "certificate"])
    printf(searchFunc1("b.csv","name","akash",["name"],3))
    # searchFunc5()
    # printf(searchFunc2("movies.csv","title","Inception",["title"],4))
    # inOp1(3,4)
    # str1 = 'https://m.media-amazon.com/images/S/sash/4FyxwxECzL-U1J8.png,Before the Devil Knows Youre Dead,15,117 min,"Crime, Drama, Thriller",7.3,"When two brothers organize the robbery of their parents jewelry store the job goes horribly wrong, triggering a series of events that sends them, their father and one brothers wife barreling towards a shattering climax.",Sidney Lumet,"('Philip Seymour Hoffman',), ('Ethan Hawke',), ('Albert Finney',), ('Marisa Tomei',)","1,03,986",$7.08M'
    # print(str1.strip(','))

    #OP1- GET TOP 3 MOVIES WHICH ARE FAMILY FRIEDNLY TO WATCH[DESC ORDER]
    #OP2- GET TOP 3 RATED MOVIES[ASC ORDER]