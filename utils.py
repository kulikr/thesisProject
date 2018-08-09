import datetime
import json as jsn
import csv
import pandas as pd
import numpy as np
import os
import pickle
import time
import redis
import math
from statistics import mode
import socket
from itertools import zip_longest

numOfUsers =0

 # Converts date to a string format
def convertDateToString(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

# Write dictionary to a file using json library
def writeDictToFile(dict, fileName, seperators =['\n',':']):
    json = jsn.dumps(dict, default=convertDateToString, separators=seperators)
    f = open( fileName , "w")
    f.write(json)
    f.close()


# Writes sessions batch do the database
def writeSessionsToDb(sessions,month):
    server_sessions= getServer(6379,month)
    server_users = getServer(6379,0)
    for user, userSessions in sessions.items():
        saveUserSessionToDb(user,userSessions,server_sessions,server_users)

# Write the openSessions dict to the database
def writeOpenSessionsToDb(openSessions,month):
    server= getServer(6379,month)
    pipe = server.pipeline()
    index = 0
    for user in openSessions:
        pickled_object = pickle.dumps(openSessions[user])
        pipe.rpush(user, pickled_object)
        index+=1
        if index %1000 ==0:
            pipe.execute()
            pipe = server.pipeline()


# Writes events distribution to file
def writeSessionsEventDistToCsv(sessions):
    randomSession = sessions[0]
    filePath = './TmpFiles/sessions_events_dist_'+str(randomSession['lastEvent'].month )
    fileExists = os.path.isfile(filePath)

    sessions = pd.DataFrame(sessions)

    if not fileExists:
        sessions.to_csv(filePath, header=True, index=False, mode='w')
    else:
        sessions.to_csv(filePath, header=False, index=False, mode='a')

# Splits raw month data into two parts
def splitMonthData(path):
        with open(path,'r') as f:
            with open(path+"_1.out",'w+') as new_f1:
                with open(path+"_2.out",'w+') as new_f2:
                    i=0
                    for line in f.readlines():

                        if(i < 100000):
                            new_f1.write(line)
                        else:
                            new_f2.write(line)

                        i += 1

# Samples a specified number of events from a raw data file
def sampleData(path, numOfEvents):
    with open(path, 'r') as f:
        with open(path + "_sample.out", 'w+') as new_f1:
            i = 0
            for line in f.readlines():

                if (i < numOfEvents):
                    new_f1.write(line)

                i += 1


def writeUsersEventsDistToCsv(usersEventsPerSession, month):

    filePath = './TmpFiles/events_per_user'
    fileExists = os.path.isfile(filePath)


    if(fileExists):

        with open(filePath, 'rb') as f:
            read_time = time.time()
            fileDict = pickle.load(f)
            print("read time is: "+str(time.time()-read_time))

        merge_time = time.time()
        mergeDicts(fileDict,usersEventsPerSession)
        print("merge time is: " + str(time.time() - merge_time))

    with open(filePath, 'wb+') as f:
        pickle.dump(usersEventsPerSession, f)


def writeNumOfSessions(numOfSessions, numOfBasketSessions,numOfBuySessions ,numOfEvents,currentMonth):
    with open('./charts/Number_Of_Basket_Sessions_'+str(currentMonth),'w+')as f:
        f.write("Month : "+str(currentMonth)+"\n" )
        f.write("The Total Number Of Sessions:"+str(numOfSessions)+"\n")
        f.write("The Total Number Of Events:"+str(numOfEvents)+"\n")
        f.write("The Total Number Of Sessions With Basket Event:"+str(numOfBasketSessions)+"\n")
        f.write("The Total Number Of Sessions That ended With Buy:"+str(numOfBuySessions)+"\n\n")

        f.write("Buy Rate For Basket Sessions"+str(numOfBuySessions/numOfBasketSessions))


# Edit the items data to extract only the required data columns
def editItemsCsv(path):
    for filename in os.listdir(path):
        df = pd.read_csv(path+"//"+filename, sep=';')
        filename_new = filename.split('_')[3]


        df = df.iloc[:,[0,6,7,9,10,11]]
        df.to_csv(path+"//"+filename_new+".csv")
        os.remove(path+"//"+filename)


# Merges two dicts and returns the merged dict
def mergeDicts(dict1, dict2):
    for key in dict1:
        if key in dict2:
            dict2[key] = dict1[key]+dict2[key]
        else:
            dict2[key] = dict1[key]

    return dict2


#
def getMonthFromPath(path):
    splitted_path = path.split("_")
    return splitted_path[-1]


def getCurrentMonth():
    directory_in_str = os.getcwd()+"./TmpFiles"
    directory = os.fsencode(directory_in_str)
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        splitted_filename = filename.split('_')
        if(filename.split('_')[0]=="sessions"):
            return splitted_filename[3]


# Returns server instance according to the specified db_num (namespace)
def getServer(port, db_num):
    r = redis.StrictRedis(host='localhost', port=port, db=db_num)
    return r


# Save all the sessions of a specific user into the database
def saveUserSessionToDb(userId, userSessions, server_sessions,server_users):
    pickled_objects=[]
    pipe_sessions = server_sessions.pipeline()
    pipe_users = server_users.pipeline()
    for session in userSessions:
        pickled_object = pickle.dumps(session)
        pipe_sessions.rpush(userId,pickled_object)
        pipe_users.rpush("users",userId)

    pipe_sessions.execute()
    pipe_users.execute()


# Extracts all the sessions of the specified user from the server's instance
def getUserSessionsFromDbByUserId(userId, server):
    elements = server.lrange(userId, 0, -1)
    sessions = []
    for element in elements:
        session = pickle.loads(element)
        sessions.append(session)
    return sessions


# Appends the dataframe content to the specified file
def appendDfToFile(dict_list, fileName):
    fileExists = os.path.isfile(fileName)
    df = pd.DataFrame()
    isFirst = True
    for userSessions in dict_list:
        for session in userSessions:
            extractSessionFeatures(session)
            del session["lastEvent"]
            del session["firstEvent"]
            del session["category1Vector"]
            del session["category2Vector"]
            del session["lastBasketEvent"]
            del session["priceVector"]
            del session["basketDwellVector"]
            del session["basketEvents"]
            del session["numOfPurchasedItems"]
            del session["numOfSaleItemsBought"]
            del session["totalAmountOfPayment"]
            del session["dwellVector"]
            del session["day"]
            del session["month"]

            if isFirst:
                df = pd.DataFrame.from_dict([session])
                nulls = df[df[['weekday']].isnull().any(axis=1)]
                if len(nulls.index) > 0:
                    ben=4
                isFirst=False
            else:
                df=pd.concat([df,pd.DataFrame.from_dict([session])], ignore_index=True)
                nulls = df[df[['weekday']].isnull().any(axis=1)]
                if len(nulls.index) > 0:
                    ben=4
    with open(fileName, mode='a', encoding="utf-8") as f:
        print(len(df.index))
        df1 = df[df[['weekday']].isnull().any(axis=1)]
        if len(df1) > 0:
            print(df1)
        if fileExists:
            df.to_csv(f, header=False, encoding="utf-8")
        else:
            df.to_csv(f, header=True, encoding="utf-8")


# Appends the dict content to the specified file
def appendDictToFile(dict, fileName):
    json = jsn.dumps(dict, default=convertDateToString, separators=['\n',':'])
    f = open( fileName , "a+")
    f.write(json)
    f.close()


# Reads a batch of keys(usernames) from the server and returns the updated cursor value
def readKeysBatchFromServer(cursor):
    server = getServer(6379,8)
    cursor,sessionsBatch = server.scan(cursor=cursor,match = None, count=500)
    return cursor, sessionsBatch


# Saves dictionary as csv file
def dictToCsv(dictPath, csvFileName):
    with open(dictPath,mode = "r") as read_f:
        lines = read_f.readlines()
        dict= ""
        for i in range(len(lines)):
            if i == len(lines)-1:
                dict.append(lines[i]+",")
            else:
                dict.append(lines[i])
        dict = jsn.loads(dict)
        with open(csvFileName,"w+") as write_f:
            w = csv.DictWriter(write_f, dict.keys())
            w.writerow(dict)


# Builds csv file from the database (one csv for tmp users and one for registered users)
def buildCsvFromDb(server, month):
    cursor, sessionBatch = readKeysBatchFromServer(0)
    keys = encodeKeys(sessionBatch)
    regUsersSessions, tmpUsersSessions = getValuesFromDb(keys, server)

    global numOfUsers
    numOfUsers+= len(keys)

    month = str(month)
    appendDfToFile(regUsersSessions, "reg_" + month)
    appendDfToFile(tmpUsersSessions, "tmp_" + month)
    while cursor != 0 and numOfUsers < 200000:

        numOfUsers += len(keys)
        print("Number Of Users Read Sessions: "+ str(numOfUsers))
        cursor, sessionBatch = readKeysBatchFromServer(cursor)
        keys = encodeKeys(sessionBatch)
        regUsersSessions, tmpUsersSessions = getValuesFromDb(keys, server)
        appendDfToFile(regUsersSessions, "reg_" + month)
        appendDfToFile(tmpUsersSessions, "tmp_" + month)


# Extracts features from the session's dict to be ready for writing to the csv file
def extractSessionFeatures(dict):
    dict["maxBasketDwell"] = -1
    dict["minBasketDwell"] = -1
    dict["AvgBasketDwell"] = -1
    dict["numberOfBasketEvents"] = -1
    dict["timeToFirstBasket"] = -1
    dict["dwellStd"]=-1
    dict["avgDwell"]= 0

    if len(dict["dwellVector"]) != 0:
        dict["dwellStd"] = np.array(dict["dwellVector"]).std()
        dict["avgDwell"] = np.array(dict["dwellVector"]).mean()

    if len(dict["basketDwellVector"]) != 0:
        dict["maxBasketDwell"] = max(dict["basketDwellVector"])
        dict["minBasketDwell"] = min(dict["basketDwellVector"])
        dict["AvgBasketDwell"] = sum(dict["basketDwellVector"])/ len(dict["basketDwellVector"])
        dict["numberOfBasketEvents"] = len(dict["basketDwellVector"]) - (1 - dict["isBasketSession"])
        dict["timeToFirstBasket"] = dict["basketDwellVector"][0]
        dict["basketPrice"] = 0
        basketCategories = []
        priceVector = []
        i=0
        for index in dict["basketEvents"]:
            if dict["priceVector"][i] != 'x':
                dict["basketPrice"] += dict["priceVector"][i]
                priceVector.append(dict["priceVector"][i])
                i += 1
            basketCategories.append(dict["category2Vector"][index-1])

        dict["b_numOfCategories"] = len(set(basketCategories))
        dict["b_cat_entropy"] = cat_vector_entropy(basketCategories)
        dict["b_dominant_category"] = get_mode(basketCategories)

        if len(priceVector) != 0:
            arr_priceVector = np.array(priceVector, dtype=float)
            dict["b_price_mean"] = arr_priceVector.mean()
            dict["b_price_std"] = arr_priceVector.std()
            dict["b_minimalItemPrice"] = arr_priceVector.min()



    # category vector
    dict["numOfCategories"] = len(set(dict["category2Vector"]))
    dict["cat_entropy"] = cat_vector_entropy(dict["category2Vector"])
    dict["dominant_category"] = get_mode(dict["category2Vector"])

    # items price
    priceVector = []
    dict["price_mean"] = 0
    dict["price_std"] = 0
    dict["minimalItemPrice"] = 0

    for price in dict['priceVector']:
        if price != 'x':
            priceVector.append(price)

    if len(priceVector) != 0:
        priceVector = np.array(priceVector, dtype=float)
        dict["price_mean"] = priceVector.mean()
        dict["price_std"] = priceVector.std()
        dict["minimalItemPrice"] = priceVector.min()
        dict["maximalItemPrice"] = priceVector.max()
    if dict["weekday"] > 8 or dict["weekday"] < 0:
        ben=4


# Return the entropy of a category in the vector
def cat_entropy(vector, category):
    numOfAppear = 0
    for cat in vector:
        if cat==category:
            numOfAppear+=1
    px = numOfAppear / len(vector)
    lpx = math.log2(px)
    ent = -px * lpx
    return ent


# Returns the entropy of the categorical vector
def cat_vector_entropy(vector):
    entropy = 0
    for category in set(vector):
        entropy+=cat_entropy(vector, category)
    return entropy


# Turns bytes UTF-8 encoded to str
def encodeKeys(byteKeys):
    strKeys = []
    for key in byteKeys:
        strKeys.append(key.decode("UTF-8"))
    return strKeys


# Returns the sessions of the users in 'users' list
def getValuesFromDb(usersIds, server):
    tmpUsers = []
    regUsers = []
    for userId in usersIds:
        elements = server.lrange(userId, 0, -1)
        sessions = []
        for element in elements:
            session = pickle.loads(element)
            sessions.append(session)
        if isTemporaryUser(userId):
            tmpUsers.append(sessions)
        else:
            regUsers.append(sessions)
    return regUsers,tmpUsers


# returns whether the user is a temporary user
def isTemporaryUser(username):
    if '-' in username:
        return True
    return False


# Returns the most common value of a categorical vector
def get_mode(vector):
    max_appear = 0
    max_value = ""
    for value in set(vector):
        if vector.count(value) > max_appear:
            max_value = value
            max_appear = vector.count(value)

    return max_value


# Extracts only the basket sessions from the dataframe
def getBasketDataFrame(month,user_type = "reg", path="C:\\Users\\kulikr\\Desktop\\BasketSessions\\DataCsv\\"):
    df = pd.read_csv(path+user_type+"_"+str(month))
    len(df)
    columns_list = [col for col in df.columns if "vector" not in col]
    columns_list = [col for col in columns_list if "Vector" not in col]
    columns_list.remove('Unnamed: 0')
    df = df[columns_list]
    df_basket = df.loc[df["isBasketSession"] == True]
    # csv_error = csv_basket.loc[csv_basket["isBasketSession"] == False]
    # print("csv len:"+str(len(csv)))
    print("csv basket len:"+str(len(df_basket.index)))
    # print("csv false in basket len:"+str(len(csv_error)))
    del df_basket["basketBuyDwell"]
    del df_basket["isBasketSession"]
    basket_features = {"timeToFirstBasket", "AvgBasketDwell", "maxBasketDwell", "basketPrice", "minBasketDwell",
                       "numOfBasketEvents", "numberOfBasketEvents"}
    rename_dict ={}
    for feature in basket_features:
        rename_dict[feature]="b_"+feature
    df_basket.rename(columns=rename_dict, inplace=True)
    return df_basket

def getGeneralDataFrame(month):
    csv = pd.read_csv("C:\\Users\\kulikr\\Desktop\\BasketSessions\\tmp_"+str(month))
    general_features = [col for col in csv.columns if "b_" not in col]
    general_features = [col for col in general_features if "basket" not in col]
    remove_list = ["AvgBasketDwell", "numOfBasketEvents", "timeTofirstBasket", "totalAmountOfPayment", 'Unnamed: 0']
    general_features = [col for col in general_features if col not in remove_list]
    csv_general = csv[general_features]



def ensure_dir(file_path):
    directory_in_str =os.getcwd()+file_path
    print(directory_in_str)
    if not os.path.isdir(directory_in_str):
        os.makedirs(directory_in_str)