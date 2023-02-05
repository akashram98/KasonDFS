from pymongo import MongoClient
import os
import random
import sys
# from colorama import init
# init(strip=not sys.stdout.isatty())  # strip colors if stdout is redirected
from termcolor import cprint, colored
from pyfiglet import figlet_format
from search import *
from tabulate import tabulate
from colorama import init, Fore, Back, Style
init(strip=not sys.stdout.isatty())

# def printb(text):
#     print(Fore.BLUE + text)
#
# def printy(text):
#     print(Fore.YELLOW + text)

def helpMessage1():
    # print()
    # printb("-----------------------------------------------------------------------------------------------------------")
    # printb("List of linux based commands available for Kson and syntax, examples for each:")
    # printb("-----------------------------------------------------------------------------------------------------------")
    print()
    printb("LINUX COMMAND LIST")
    val = [["mkdir","mkdir [path_of_folder]","mkdir /user/john"],
           ["ls","ls [path_of_folder/file]","ls /user"],
           ["cat","cat [path_of_file]","cat /user/h1.csv"],
           ["rm","rm [path_of_folder/file]","rm /user/h1.csv"],
           ["put","put [name_of_file] [path_in_kson_dfs] [num_partitions]","put cars.csv /user/john 3"],
           ["getPartitionLocations","getPartitionLocations [file_name]","getPartitionLocations h1.csv"],
           ["readPartition","readPartition [file_name] [partition_number]","readPartition h1.csv 2"]]
    h1 = ["COMMAND","SYNTAX","EXAMPLE"]
    print(tabulate(val,h1,tablefmt="psql"))
    print()

# helpMessage1 = \
#     "-----------------------------------------------------------------------------------------------------------" \
#     "\nList of linux based commands available for Kason and syntax, examples for each:" \
#     "\n-----------------------------------------------------------------------------------------------------------" \
#     "\nmkdir - mkdir path_of_folder - mkdir /user/john" \
#     "\nls - ls path_of_folder/file - ls /user" \
#     "\ncat - cat path_of_file - cat /user/h1.csv" \
#     "\nrm - rm path_of_folder/file - rm /user/h1.csv" \
#     "\nput - put name_of_file path_in_kson_dfs num_partitions - put cars.csv /user/john 3" \
#     "\ngetPartitionLocations - getPartitionLocations file_name - getPartitionLocations h1.csv" \
#     "\nreadPartition - readPartition file_name partition_number - readPartition h1.csv 2" \
#     "\n-----------------------------------------------------------------------------------------------------------\n"

def helpMessage2():
    print()
    printb("DESCRIPTION OF MOVIES DATASET")
    val = [["poster","STRING","url of movie poster"],
           ["title","STRING","Title of the movie"],
           ["certificate","STRING","Certification Level"],
           ["runtime","INTEGER","Runtime of the movie in minutes"],
           ["genre","ARRAY","List of all genres the movie falls under"],
           ["rating","INTEGER","Avg rating of the movie"],
           ["about","ARRAY","List of descriptions associated with the movie"],
           ["director","STRING","Director Name"],
           ["stars","ARRAY","List of the stars of the movie"],
           ["votes","STRING","Number of votes cast for the movie"],
           ["gross_earn","STRING","Gross earning of the movie[in millions]"]]
    h1 = ["ATTRIBUTE NAME","DATA TYPE","DESC"]
    print(tabulate(val,h1,tablefmt="psql"))
    print()

# helpMessage2 = \
#     "-----------------------------------------------------------------------------------------------------------" \
#     "\nDESCRIPTION OF DATASET" \
#     "\n-----------------------------------------------------------------------------------------------------------" \
#     "\nDatatype and Attributes" \
#     "\nposter: STRING - url of movie poster" \
#     "\ntitle: STRING - Title of the movie " \
#     "\ncertificate: STRING - Certification Level" \
#     "\nruntime: INTEGER - Runtime of the movie in minutes  " \
#     "\ngenre: ARRAY - List of all genres the movie falls under" \
#     "\nrating: INTEGER - Avg rating of the movie" \
#     "\nabout: ARRAY - List of descriptions associated with the movie" \
#     "\ndirector: STRING - Director Name" \
#     "\nstars: ARRAY - List of the stars of the movie" \
#     "\nvotes: STRING - Number of votes cast for the movie" \
#     "\ngross_earn: STRING - Gross earning of the movie[in millions]" \
#     "\n-----------------------------------------------------------------------------------------------------------"

def helpMessage3():
    print()
    printb("QUERY TYPES")
    val = [["Built In Queries","BI [dataset]","BI movies.csv"],
           ["User Defined Queries","ksondfs [dataset] [searchAttribute] [typeOfQuery]","ksondfs movies.csv title [eq/in/range]"]]
    h1 = ["TYPE","SYNTAX","EXAMPLE"]
    print(tabulate(val,h1,tablefmt="fancy_grid"))
    print()
    print("-----------------------------------------------------------------------------------------------------------")
    printb("USER DEFINED QUERIES:")
    print("After entering the query the wizard will prompt you to enter the value for the search attribute")
    print("and the columns that you want to display")
    print("There are three types of queries: eq,in and range")
    print("eq - checks for equality - Ex: title=\"abcd\"")
    print("in - checks if element is in Array - Ex: Is \"Wang\" in stars array")
    print("range - checks if value is between a and b - Ex: price between 18 and 20")
    print("-----------------------------------------------------------------------------------------------------------")


    print()



# helpMessage3 = \
#     "-----------------------------------------------------------------------------------------------------------" \
#     "\nSyntax for search commands - kasondfs attributeName \"[searchItem]\" \"[projectionList]\"" \
#     "\nExample: kasondfs title \"Inception\" \"title,stars\""\
#     "\n-----------------------------------------------------------------------------------------------------------" \
#     "\nsearchItem Options"
#
# helpMessage3_1 = [["1","Search by title","title [nameOfMovie]","[nameOfMovie]-STRING"],["2","Search ratings between a and b[a,b integers]","rating [a] [b]","[a,b]-INTEGER"]
#                   ,["3","Search according to stars/actors","stars [nameStar]","[nameStar]-STRING"]
#                   ]
# headerList3_1 = ["SNO","DESCRIPTION","SYNTAX","DATATYPE OF THE [] PARAMETER"]



def connectMongo(con):
    conn = MongoClient('mongodb+srv://apraveen:dsci551@cluster0.qae19sx.mongodb.net/?retryWrites=true&w=majority')
    db1 = conn["dsci551"]
    db = db1[con]
    return db

def connectMongo1(con):
    conn = MongoClient('mongodb+srv://apraveen:dsci551@cluster0.qae19sx.mongodb.net/?retryWrites=true&w=majority')
    db1 = conn["dsci551"]
    db = db1[con]
    return db

db2_1 = connectMongo1('dataNode')
# cat, rm

cmdList = ["mkdir", "ls", "cat", "rm", "put", "getPartitionLocations", "readPartition"]
root = "X:/desktop/USC_fall_2022/DSCI_551/project/root"
pathPartitions= "X:/desktop/USC_fall_2022/DSCI_551/project/data"
db = connectMongo("iNodeList")
db1 = connectMongo("pChildRel")
destData = ["X:/desktop/USC_fall_2022/DSCI_551/project/data1","X:/desktop/USC_fall_2022/DSCI_551/project/data2","X:/desktop/USC_fall_2022/DSCI_551/project/data3"]
# print(db1)

def getNumPartitions(name):
    numP = db.find({"name": name}, {"numPartitions": 1})
    numP = list(numP)
    numP = numP[0]['numPartitions']
    return numP

def insertData1(fileName, path, numPartitions):

    file = open(fileName, errors="ignore")
    numPartitions = int(numPartitions)
    l1 = int(round((len(file.readlines())-1) / numPartitions))
    # print(l1)
    file.close()
    file = open(fileName, errors="ignore")
    # print(l1)
    headerList = file.readline()
    headerList = headerList.replace("\n", "")
    headerList = headerList.split(",")
    # f = file.readline()
    # f = f.replace("\n", "")
    # f = f.split(",")
    f = file.readline()
    f = f.replace("\n", "")
    f = f.split(",")
    dict1 = {}
    num = 1
    count = 0
    for i in range(1, numPartitions + 1):
        dict = {}
        f1 = fileName.rstrip(".csv") + str(i)
        dict[f1] = []
        dict["name"] = f1
        dict["fileName"] = fileName
        db2_1.insert_one(dict)
    # print(dict)
    # print(headerList)
    # print(file.readline().replace("\n","").split(","))
    while f!=['']:
        # print(f)
        # print(f)
        for (i, j) in zip(headerList, f):
            dict1[i] = j
        f1 = fileName.rstrip(".csv") + str(num)
        # print(f1)
        db2_1.update_one({"name": f1}, {"$push": {f1: dict1}})
        # print(dict1)
        dict1 = {}
        count = count + 1
        # print(count)
        if (count == l1) and num != numPartitions:
            count = 0
            num = num + 1
        f = file.readline()
        f = f.replace("\n", "")
        f = f.split(",")
            # print(num)

def insertData(fileName, path, numPartitions):

    file = open(fileName, errors="ignore")
    numPartitions = int(numPartitions)
    l1 = int(round(len(file.readlines()) / numPartitions))
    file.close()
    file = open(fileName, errors="ignore")
    # print(l1)
    headerList = file.readline()
    headerList = headerList.replace("\n", "")
    headerList = headerList.split(",")
    f = file.readline()
    dict1 = {}
    num = 1
    count = 0
    for i in range(1, numPartitions + 1):
        dict = {}
        f1 = fileName.rstrip(".csv") + str(i)
        dict[f1] = []
        dict["name"] = f1
        dict["fileName"] = fileName
        db2_1.insert_one(dict)

    while (f):
        # print(f)
        f = file.readline()
        f = f.replace("\n", "")
        f = f.replace("'","")
        f = re.split(''',(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''', f)

        # print(f)
        if f == ['']:
            break
        for (i, j) in zip(headerList, f):
            if i == 'rating':
                j = float(j)
            elif i == 'votes':
                j = j.strip('"')
                j = j.replace(",", "")
                # j = int(j)
            elif i == 'runtime':
                numbers = re.findall('[0-9]+', j)
                j = int(numbers[0])
            # elif i == "gross_earn":
            #     floating = re.findall("\d+\.\d+", j)
            #     j = float(floating[0])
            elif '"' in j:
                j = j.strip('"')
                # j = j.split(',')
                j = j.strip('"\'\\')
                j = j.replace('(', '').replace(',)', '')
                j = j.replace("'", '').replace("'", '')
                j = j.split(",")
            dict1[i] = j
        f1 = fileName.rstrip(".csv") + str(num)
        db2_1.update_one({"name": f1}, {"$push": {f1: dict1}})
        dict1 = {}
        count = count + 1
        # print(count)
        if (count == l1) and num != numPartitions:
            count = 0
            num = num + 1
            # print(num)


def insert(db, doc):
    db.insert_one(doc)

def delete(db, doc):
    db.delete_one(doc)

def putFunc(fileName, path, numPartitions):
    file = open(fileName, errors="ignore")
    l = file.readlines()
    length = len(l)
    numPartitions = int(numPartitions)
    l1 = int(round(length/numPartitions))
    start = 0
    stop = l1
    # print(l1)
    partitionDict = {}
    for num in range(1,numPartitions+1):
        # print(num)
        pickLoc = random.randint(0,len(destData)-1)
        pickLoc = destData[pickLoc]+"/"+fileName.rstrip(".csv")+str(num)+".csv"
        # print(pickLoc)
        partitionDict[str(num)] = pickLoc
        file1 = open(pickLoc,"x")
        if num == numPartitions:
            # print("last")
            for i in range(start,length):
                # print(l[i])
                file1.write(l[i])
            # print(l[start:])
            break
        # print("hi")
        for i in range(start, stop):
            file1.write(l[i])
            # print(l[i])
        # print(l[start:stop])
        start = stop
        stop = stop + l1

    p = path+"/"+fileName

    meta = {
        "filePath": p,
        "fileType": "data",
        "name": fileName,
        "numPartitions": numPartitions,
        "partitionLoc": partitionDict,
        "parent": path.split("/")[-1]
    }

    db.update_one({"filePath": p}, {"$set": meta}, upsert=True)
    pChildUpdate()
    print("File updated")
    # print(meta)

    # for i in l:
    #     print(i,end="")
    # print(l)
    # print("hi")
    return

def getPartitionLocations(file):
    temp = db.find_one({"name": file},{"partitionLoc": 1})
    if temp:
        print(temp["partitionLoc"])
    else:
        print("No such File/ It is a directory")

def catFunction(path):
    temp = db.find_one({"filePath": path, "fileType":"data"},{"name":1, "numPartitions": 1})
    if temp:
        fileName = temp["name"]
    else:
        print("No such File")
        return
    # print(temp)
    numPartitions = temp["numPartitions"]
    for num in range(1,numPartitions+1):
        readPartitionFunc(fileName, num)

def readPartitionFunc(file, numP):
    numP = str(numP)
    temp = db.find_one({"name": file}, {"partitionLoc": 1})
    if temp:
        if int(numP) > len(temp['partitionLoc']):
            print("Partition does not exist")
            return
        # print(len(temp['partitionLoc']))
        reqLoc = temp["partitionLoc"][numP]
        file = open(reqLoc, "r")
        for line in file.readlines():
            print(line, end="")
        # print(len(temp["partitionLoc"]))
        if numP == str(len(temp["partitionLoc"])):
            print("")
    else:
        print("No such file found")
    return


def lsFunc(path):
    if path=="/":
        path=""
    childList = db1.find_one({"filePath": path},{"childList": 1})
    # print(childList)
    if childList:
        for i in childList["childList"]:
            # print(i)
            res = db.find_one({"name": i},{"_id": 0, "parent": 0, "partitionLoc": 0})
            # print(res)
            for key, val in res.items():
                print(key+":"+str(val)+" ", end="")
            print("")
    else:
        print("No such path")
        return



def mkdirFunc(dirName):
    if db.find_one({"filePath": dirName}):
        print("Already exists")
        return
    # os.mkdir(root+dirName)
    dirList = dirName.split("/")
    pDir = dirList[-2]
    name = dirList[-1]
    dirData = {
        "filePath": dirName,
        "fileType": "Directory",
        "name": name,
        "parent": pDir
    }
    db.update_one({"filePath": dirName},{"$set": dirData}, upsert=True)
    db1.update_one({"parent": name}, {"$set":{"childList": []}}, upsert=True)
    # insert(connectMongo(), dirData)

def pChildUpdate():
    iNodeList = list(db.find({},{"name": 1, "parent": 1, "filePath": 1}))
    for iNode in iNodeList:
        db1.update_one({"parent": iNode['name']}, {"$set": {"childList": [], "filePath": iNode["filePath"]}}, upsert=True)
    for iNode in iNodeList:
        # if not iNode["parent"]:
        #     continue
        if "parent" not in iNode.keys():
            continue
        # print(iNode["parent"], iNode["name"])
        # print(list(db1.find({"parent": iNode["parent"]})))
        db1.update_one({"parent": iNode["parent"]}, {"$push": {"childList": iNode["name"]}})

def rmFunc(path):
    temp = db.find_one({"filePath": path, "fileType": "data"}, {"name": 1, "numPartitions": 1, "partitionLoc": 1})
    if temp:
        for i in temp["partitionLoc"].values():
            os.remove(i)
            # print(i)
        print("Deleted")
        # print(temp)
        db.delete_one({"_id": temp["_id"]})
        db1.delete_one({"filePath": path})
        db2_1.delete_many({"fileName": temp['name']})
        pChildUpdate()
    else:
        print("Specified file path does not exist")

def searchPrep(vTemp):
    list = []
    for i in vTemp[3].strip('"').split(","):
        list.append(i)
    v2 = vTemp[2].strip('"')
    return list,v2

def builtIn(vTemp):
    checkCon = vTemp[0]
    if checkCon!="BI":
        return

    if len(vTemp) != 2:
        print("CHECK SYNTAX!")
        return

    if vTemp[1] != "movies.csv":
        print('\033[1m' + "The built-in functions are currently only available for the movies dataset" + '\033[0m')
        print()
        return

    print("\nList of built-in functions are:\n")
    header = ["COMMAND CODE","DESCRIPTION"]
    val = [["OP1","TOP X MOVIES WHICH ARE FAMILY FRIEDNLY TO WATCH[DESC ORDER]"],
           ["OP2","TOP X RATED MOVIES[ASC ORDER]"],
           ["OP3","TOP X MOVIES WITH LEAST RUNTIME[ASC ORDER]"]]
    print(tabulate(val, header))
    # print("\nType [exit BI] to exit BI function wizard")
    inp = input('\nEnter the command you want to test ')
    if inp not in ["OP1", "OP2", "OP3"]:
        print("Incorrect Command Code\n")
        return
    X = input("Enter the value for X ")
    if not X.isdigit():
        print("Not a Valid Value\n")
        return
    numP=getNumPartitions("movies.csv")
    print()
    if inp == "OP1":
        inOp(1,int(X),numP)
    elif inp == "OP2":
        inOp1(2,int(X),numP)
    elif inp == "OP3":
        inOp1(3,int(X),numP)
    print()

def userDef(vTemp):
    numP = getNumPartitions(vTemp[1])
    query = vTemp[3]
    print()
    if query == "eq":
        inp = input("Enter the value to be searched: ")
        projectList = input("Enter comma separated projection list[without spaces]: ")
        projectList = projectList.split(",")
        searchRes = searchFunc1(vTemp[1], vTemp[2], inp.strip('"'), projectList, numP)
        # printf(searchRes)
    elif query == "range":
        inp1 = input("Enter the lower bound value[a]: ")
        inp2 = input("Enter the upper bound value[b]: ")
        projectList = input("Enter comma separated projection list[without spaces]: ")
        projectList = projectList.split(",")
        searchRes = searchFunc2(vTemp[1], vTemp[2], inp1, inp2, projectList, numP)
        # printf(searchRes)
    elif query == "in":
        inp = input("Enter the value to be searched: ")
        projectList = input("Enter comma separated projection list[without spaces]: ")
        projectList = projectList.split(",")
        print(vTemp[1])
        searchRes = searchFunc3(vTemp[1], vTemp[2], inp.strip('"'), projectList, numP)
    print()
    if not any(searchRes):
        printb("No values returned | Check query")
    else:
        printb("RESULTS RETRIEVED")
        exp = inputb("Do you want to view the results in EXPLANATION MODE[Y/N]: ")
        print()
        if exp=="Y":
            print()
            printb("MAP PHASE DESCRIPTION")
            printb("The input from each partition goes into three separate Mappers")
            printb("If we take demo.csv as an example with 3 partitioned files")
            printb("demo1 --> map1, demo2 --> map2, demo3 --> map3")
            printb("Within each mapper we return the search condition and output records which")
            printb("equate to this condition")
            print()
            printb("MAP RESULTS")
            eprintf(searchRes)
            printb("REDUCE PHASE DESCRIPTION")
            printb("The job of the reducer is to then apply some aggregation operation by combining together")
            printb("All these outputs from the map phase. In this case we just do groupBy and output the")
            printb("the combined results. This is the case for search based functions. However for analytics")
            printb("based queries we will be reducing each key by applying an operation after grouping.")
            print()
            printb("REDUCE PHASE RESULTS")
            printf(searchRes)
        else:
            printf(searchRes)
    print()
    return

def startKason():
    cmd = ""
    print("")
    while cmd != "exit":
        cmd = input("Kson_DFS_shell-->")
        if cmd == "klinux":
            helpMessage1()
            continue
        if cmd == "kdataset desc":
            helpMessage2()
            continue
        if cmd == "ksearch":
            helpMessage3()
            # print(helpMessage3)
            # print(tabulate(helpMessage3_1, headerList3_1, tablefmt="fancy_grid"))
            # print("Type [kasondfs dataset desc] for list of attributes in dataset\n")
            continue
        vTemp = cmd.split(" ")
        vCheck = vTemp[0]
        if vCheck == "exit":
            break
        if vCheck == "BI":
            builtIn(vTemp)
            continue
        if vCheck == "ksondfs" and len(vTemp) == 4 and vTemp[3] in ["eq","in","range"]:
            userDef(vTemp)
            continue
        if vCheck not in cmdList:
            print("Error!!! Incorrect command")
            print()
            continue
        if len(vTemp) != 2 and vCheck in ["mkdir","ls","getPartitionLocations","cat","rm"]:
            print("missing or extra parameters, check syntax")
            print()
            continue
        if len(vTemp) != 4 and vCheck in ["put"]:
            print("missing or extra parameters, check syntax")
            print()
            continue
        if len(vTemp) != 3 and vCheck in ["readPartition"]:
            print("missing or extra parameters, check syntax")
            print()
            continue
        if vCheck == "mkdir":
            path = vTemp[1]
            mkdirFunc(path)
            pChildUpdate()
        elif vCheck == "ls":
            path = vTemp[1]
            lsFunc(path)
        elif vCheck == "put":
            path = vTemp[1]
            putFunc(path, vTemp[2], vTemp[3])
            if vTemp[1] == "movies.csv":
                insertData("movies.csv", "", vTemp[3])
            else:
                insertData1(path,"",vTemp[3])
        elif vCheck == "getPartitionLocations":
            path = vTemp[1]
            getPartitionLocations(path)
        elif vCheck == "readPartition":
            path = vTemp[1]
            numP = vTemp[2]
            readPartitionFunc(path, numP)
        elif vCheck == "cat":
            path = vTemp[1]
            catFunction(path)
        elif vCheck == "rm":
            path = vTemp[1]
            rmFunc(path)
        print()

def startScreen():
    print("-----------------------------------------------------------------------------------------------------------")
    cprint(figlet_format('KSONDFS'))
    # print("-----------------------------------------------------------------------------------------------------------")
    print("Type [klinux] for list of edfs commands || Type [kdataset desc] for description of the movies dataset")
    print("Type [ksearch] for search commands || Type [exit] to come out of ksondfs shell")
    startKason()


if __name__=='__main__':

    # print("hi")
    # startKason()
    startScreen()
    # insertData1("b.csv","",3)
    # print(getNumPartitions())
    # searchFunc4("12A", ["title", "certificate"])
    # text = colored('Hello, World!', 'red', attrs=['reverse', 'blink'])
    # mkdirFunc("/ad")
    # pChildUpdate()
    # lsFunc("/")
    # putFunc("b.csv", "/ad", 3)
    # pChildUpdate()
    # getPartitionLocations("b.csv")
    # readPartitionFunc("b.csv", 1)
    # catFunction("/ad/b.csv")
    # rmFunc("/ad/b.csv")
    # print(list(db.find()))







