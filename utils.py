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

def convertDateToString(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def writeDictToFile(dict, fileName):
    json = jsn.dumps(dict, default=convertDateToString, separators=['\n',':'])
    f = open( fileName , "w")
    f.write(json)
    f.close()


def writeMonthDataToFile(sessions, accumulated_data, month):
    sessionsToDf(sessions,month)
    writeDictToFile(accumulated_data, "accumulated_"+month)


def sessionsToDf(sessions,fileEnding):
    with open('./TmpFiles/sessions_'+fileEnding+'.csv', 'w') as f:
        dict_writer = csv.DictWriter(f, sessions[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(sessions)


def writeSessionsToDb(sessions,month):
    server_sessions= getServer(6379,month)
    server_users = getServer(6379,0)
    for user, userSessions in sessions.items():
        saveUserSessionToDb(user,userSessions,server_sessions,server_users)


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


def writeSessionsDataToCsv(sessions):
    randomSession = sessions[0]
    filePath = './TmpFiles/sessions_events_dist_'+str(randomSession['lastEvent'].month )
    fileExists = os.path.isfile(filePath)

    sessions = pd.DataFrame(sessions)

    if not fileExists:
        sessions.to_csv(filePath, header=True, index=False, mode='w')
    else:
        sessions.to_csv(filePath, header=False, index=False, mode='a')


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


def sampleData(path):
    with open(path, 'r') as f:
        with open(path + "_sample.out", 'w+') as new_f1:
            i = 0
            for line in f.readlines():

                if (i < 100000):
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


def editItemsCsv(path):
    for filename in os.listdir(path):
        df = pd.read_csv(path+"//"+filename, sep=';')
        filename_new = filename.split('_')[3]


        df = df.iloc[:,[0,6,7,9,10,11]]
        df.to_csv(path+"//"+filename_new+".csv")
        os.remove(path+"//"+filename)


def mergeDicts(dict1, dict2):
    for key in dict1:
        if key in dict2:
            dict2[key] = dict1[key]+dict2[key]
        else:
            dict2[key] = dict1[key]

    return dict2


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


def getServer(port, db_num):
    r = redis.StrictRedis(host='localhost', port=port, db=db_num)
    return r


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


def getUserSessionsFromDbByUserId(userId, server):
    elements = server.lrange(userId, 0, -1)
    sessions = []
    for element in elements:
        session = pickle.loads(element)
        sessions.append(session)
    return sessions


def appendDictListToFile(dict_list, fileName):
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
            if isFirst:
                df = pd.DataFrame.from_dict([session])
                isFirst=False
            else:
                df=pd.concat([df,pd.DataFrame.from_dict([session])], ignore_index=True)

    with open(fileName, mode='a', encoding="utf-8") as f:
        print(len(df.index))
        if fileExists:
            df.to_csv(f, header=False, encoding="utf-8")
        else:
            df.to_csv(f, header=True, encoding="utf-8")




def appendDictToFile(dict, fileName):
    json = jsn.dumps(dict, default=convertDateToString, separators=['\n',':'])
    f = open( fileName , "a+")
    f.write(json)
    f.close()


def readKeysBatchFromServer(cursor):
    server = getServer(6379,8)
    cursor,sessionsBatch = server.scan(cursor=cursor,match = None, count=500)
    return cursor, sessionsBatch


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


def buildDfFromDb(server,month):
    cursor, sessionBatch = readKeysBatchFromServer(0)
    keys = encodeKeys(sessionBatch)
    regUsersSessions, tmpUsersSessions = getValuesFromDb(keys, server)
    ###### ************** #########

    global numOfUsers
    numOfUsers+= len(keys)

    month = str(month)
    appendDictListToFile(regUsersSessions, "regCsv_"+month)
    appendDictListToFile(tmpUsersSessions, "tmpCsv_"+month)
    while cursor != 0 and numOfUsers < 200000:

        numOfUsers += len(keys)
        print(numOfUsers)
        cursor, sessionBatch = readKeysBatchFromServer(cursor)
        keys = encodeKeys(sessionBatch)
        regUsersSessions, tmpUsersSessions = getValuesFromDb(keys, server)
        appendDictListToFile(regUsersSessions, "regCsv_" + month)
        appendDictListToFile(tmpUsersSessions, "tmpCsv_" + month)


def extractSessionFeatures(dict):
    dict["maxBasketDwell"] = -1
    dict["minBasketDwell"] = -1
    dict["AvgBasketDwell"] = -1
    dict["numberOfBasketEvents"] = -1
    dict["timeToFirstBasket"] = -1
    dict["dwellStd"]=-1

    if len(dict["dwellVector"]) != 0:
        dict["dwellStd"] = np.array(dict["dwellVector"]).std()

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




def cat_entropy(vector, category):
    numOfAppear = 0
    for cat in vector:
        if cat==category:
            numOfAppear+=1
    px = numOfAppear / len(vector)
    lpx = math.log2(px)
    ent = -px * lpx
    return ent

def cat_vector_entropy(vector):
    entropy = 0
    for category in set(vector):
        entropy+=cat_entropy(vector, category)
    return entropy


def encodeKeys(byteKeys):
    strKeys = []
    for key in byteKeys:
        strKeys.append(key.decode("UTF-8"))
    return strKeys


def getValuesFromDb(users,server):
    tmpUsers = []
    regUsers = []
    for user in users:
        elements = server.lrange(user, 0, -1)
        sessions = []
        for element in elements:
            session = pickle.loads(element)
            sessions.append(session)
        if isTemporaryUser(user):
            tmpUsers.append(sessions)
        else:
            regUsers.append(sessions)
    return regUsers,tmpUsers


# returns whether the user is a temporary user
def isTemporaryUser(username):
    if '-' in username:
        return True
    return False

def get_mode(vector):
    max_appear = 0
    max_value = ""
    for value in set(vector):
        if vector.count(value) > max_appear:
            max_value = value

    return max_value
