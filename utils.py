import json as jsn
import csv
import pandas as pd
import os
import pickle
import time


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


def writeSessionsDataToCsv(sessions):
    randomSession = sessions[0]
    filePath = './TmpFiles/sessions_events_dist_'+str(randomSession['lastEvent'].month )
    fileExists = os.path.isfile(filePath)

    sessions=pd.DataFrame(sessions)

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

                        if(i < 10000000):
                            new_f1.write(line)
                        else:
                            new_f2.write(line)

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