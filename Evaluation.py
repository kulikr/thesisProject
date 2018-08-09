from matplotlib import pyplot as plot
import pandas as pd
import numpy as np
import utils as utl
import pickle

# Old code region
def discretizeData(X):
    for column in X:
        if column not in ["lastEvent","boughtItems","isBuySession","events"]:
            X[column] = pd.cut(np.array(X[column]), 3, labels=["low", "medium", "high"])
    return X

def drawPlotBars(X_true, X_false,  className):

    for x in X_true:
        if x not in ["lastEvent","boughtItems","isBuySession","events"]:
            length=3
            width=0.35
            fig, ax = plot.subplots()
            low = X_true[X_true[x]=="low"][x]
            medium = X_true[X_true[x] == "medium"][x]
            high = X_true[X_true[x] == "high"][x]

            rects1 = ax.bar([0.35,0.7,1.05], [len(low),len(medium),len(high)] , 0.35, color='r')

            # add some text for labels, title and axes ticks
            ax.set_ylabel('Num Of Instances')
            ax.set_title("Buy Sessions " + x + "Values Distribution")
            ax.set_xticks([0.35,0.7,1.05,1.5,1.85,2.1])
            ax.set_xticklabels(('low', 'medium', 'high','low', 'medium', 'high'))

            low = X_false[X_false[x]=="low"][x]
            medium = X_false[X_false[x] == "medium"][x]
            high = X_false[X_false[x] == "high"][x]
            rects2 = ax.bar([1.5,1.85,2.1], [len(low),len(medium),len(high)], width, color='y')

            ax.legend((rects1[0], rects2[0]), ('Buy Session', 'No Buy Session'))
            fig.savefig("./charts/" + "value distribution" + x + ".png")  # save the figure to file
            plot.close(fig)


def drawFeatureBar(label_x, label_y,  x_true, x_false, length):
    width=0.35
    plot.title("plot " + label_x + "-->" + label_y)
    plot.xlabel(label_x)
    plot.bar(length, x_false, 0.35)

    plot.savefig("./charts/" + "plotBar_" + label_x + ".png")
    plot.close()


def evaluateData(path):

    data = pd.read_csv(path)
    data = data[data["numOfEvents"] != 1]

    return len(data[data["numOfPurchasedItems"] != 1])

    data = discretizeData(data)
    data_true = data[data["isBuySession"] == 1]
    data_false = data[data["isBuySession"] == 0]
    X_true= data_true.iloc[:, :-1]
    X_false= data_false.iloc[:, :-1]



    drawPlotBars(X_true,X_false ,"buySession")


def readAndSplitData(path):
    data = pd.read_csv(path)
    data = data[data["numOfEvents"] != 1]

    data_true = data[data["isBuySession"] == 1]
    data_false = data[data["isBuySession"] == 0]
    X_true= data_true.iloc[:, :-1]
    X_false= data_false.iloc[:, :-1]
    return X_true, X_false


def readData(path):
    data = pd.read_csv(path)
    className = "isBuySession"
    data=data[data["numOfEvents"]!=1]

    y = data[className]
    X = data.iloc[:, :-1]
    return X, y, className


def drawAllFeaturesScatters(X, y, className):
    for x in X:
        drawScatter(x, className, X[x], y)


def drawScatter(label_x, label_y, x, y):
    plot.title("scatter: " + label_x + "-->" + label_y)
    plot.xlabel(label_x)
    plot.ylabel(label_y)

    plot.scatter(x, y)

    plot.savefig("./charts/" + "scatter_" + label_x + ".png")
    plot.close()


def plotUsersHistograms(path):
    with open(path,"rb") as f:
        usersSessions = pickle.load(f)

    registeredLengthDistribution= np.zeros((300), dtype=int)
    tmpLengthDistribution = np.zeros((300), dtype=int)
    for key,value in usersSessions.items():
        username = key
        sessions = value

        if username == 'null':
            continue

        if len(sessions) > 100:
            ben = 0

        if isTemporaryUser(username):
            tmpLengthDistribution[len(sessions)]+=1
        else:
            registeredLengthDistribution[len(sessions)]+=1


    plot.hist(tmpLengthDistribution, bins='auto')  # arguments are passed to np.histogram
    plot.title("Number Of Sessions Distribution - Tmp Users")
    plot.xlabel("Number Of Sessions")
    plot.ylabel("Number Of Users")
    plot.savefig("./charts/" + "SessionsHistTmp.png")


    plot.hist(registeredLengthDistribution, bins='auto')  # arguments are passed to np.histogram
    plot.title("Number Of Sessions Distribution - Registered Users")
    plot.xlabel("Number Of Sessions")
    plot.ylabel("Number Of Users")
    plot.savefig("./charts/" + "SessionsHistRegistered.png")


def plotUsersAverageSessionLength(path):
    with open(path,"rb") as f:
        usersSessions = pickle.load(f)


    tmpUsersAvgNumOfEvents = 0
    tmpUsersAvgSessionLength = 0
    registeredUsersAvgNumOfSessions = 0
    registeredUsersAvgNumOfEvents = 0
    registeredUsersAvgSessionLength = 0
    registeredUsersTotalNumberOfSessions= 0
    tmpUsersTotalNumberOfSessions = 0
    numOfTmpUsers = 0
    numOfRegisteredUsers = 0
    numOfTmpUsersMultipleSessions = 0

    registeredUsersNumOfLongSessionsUsers=0

    usernames =[]
    sessions_length = []
    for key,value in usersSessions.items():
        username = key
        sessions = value

        if isTemporaryUser(username):
            numOfTmpUsers += 1
            numOfSessions = len(sessions)
            tmpUsersTotalNumberOfSessions += numOfSessions
            numOfEvents = sum(sessions)

            userAvgSessionLength = numOfEvents/numOfSessions
            if(numOfSessions > 1):
                numOfTmpUsersMultipleSessions+=1

            tmpUsersAvgNumOfEvents = calculateAvgIteratively(numOfTmpUsers, tmpUsersAvgNumOfEvents, numOfEvents )
            tmpUsersAvgSessionLength = calculateAvgIteratively(numOfTmpUsers, tmpUsersAvgSessionLength, userAvgSessionLength)

        else:
            numOfRegisteredUsers += 1
            numOfSessions = len(sessions)
            registeredUsersTotalNumberOfSessions+=numOfSessions
            numOfEvents = sum(sessions)
            numOfLongSessions = sum(session > 4 for session in sessions)
            userAvgSessionLength = numOfEvents / numOfSessions
            if ( numOfLongSessions > 1 ):
                registeredUsersNumOfLongSessionsUsers += 1 #count how many

            registeredUsersAvgNumOfSessions = calculateAvgIteratively(numOfRegisteredUsers, registeredUsersAvgNumOfSessions, numOfSessions)
            registeredUsersAvgNumOfEvents = calculateAvgIteratively(numOfRegisteredUsers, registeredUsersAvgNumOfEvents, numOfEvents)
            registeredUsersAvgSessionLength= calculateAvgIteratively(numOfRegisteredUsers, registeredUsersAvgSessionLength,
                                                               userAvgSessionLength)


        with open("./charts/users_event_dist","w+") as f:
            f.write("### Temporary Users - Total Sessions in data:"+str(tmpUsersTotalNumberOfSessions)+"\n\n")
            f.write("Average Number Of Total Events Per User: "+str(tmpUsersAvgNumOfEvents)+"\n")
            f.write("Average Session length(event count): "+str(tmpUsersAvgSessionLength)+"\n")
            f.write("### Registered Users - Total Sessions in data:"+str(registeredUsersTotalNumberOfSessions)+"\n\n")
            f.write("Average Number Of Total Events Per User : "+str(registeredUsersAvgNumOfEvents)+"\n")
            f.write("Average Session length(event count): "+str(registeredUsersAvgSessionLength)+"\n")
            f.write("Average Number Of sessions for registered user: " + str(registeredUsersAvgNumOfSessions) + "\n")


def plotEventDistribution(path):
    sessionsData = pd.read_csv(path)

    N = 4
    events_names= ['click','buy','basket','clickrecommended']
    clickMeans = (sessionsData['click'].mean(), sessionsData['buy'].mean(), sessionsData['basket'].mean(), sessionsData['clickrecommended'].mean())
    clickStd = (sessionsData['click'].std(), sessionsData['buy'].std(), sessionsData['basket'].std(), sessionsData['clickrecommended'].std())
    month = utl.getMonthFromPath(path)

    ind = np.arange(N)  # the x locations for the groups
    width = 0.35  # the width of the bars: can also be len(x) sequence

    p1 = plot.bar(ind, clickMeans, width, yerr=clickStd)


    plot.ylabel('Avg Per Session')
    plot.title('Event Types Avg Per Session')
    plot.xticks(ind, ('Click', 'Buy', 'Basket', 'ClickRecommended'))
    plot.yticks(np.linspace(0, 4, 17))




    plot.savefig("./charts/" + "EventDist_"+month+".png")

    month = utl.getMonthFromPath(path)
    with open("./TmpFiles/event_means_"+month, mode="w+") as f:
        for i in range(len(clickMeans)):
            f.write(str(events_names[i])+"-\nmean:"+str(clickMeans[i])+" Std: "+str(clickStd[i])+"\n")
    plot.close()




# returns whether the user is a temporary user
def isTemporaryUser(username):
    if '-' in username:
        return True
    return False


def calculateAvgIteratively(new_N, current_avg, to_be_added):
    if (new_N > 1):
        newAverage = ( current_avg * (new_N - 1) / new_N ) + ( to_be_added / new_N )
    else:
        newAverage = to_be_added
    return newAverage


def evaluateWithDb(server, month):
    fileName = "numOfCategoriesInBasketDist"
    basketAndBuySessionsDist,regAndTmpCount = initBB_dict()
    cursor, sessionBatch = utl.readKeysBatchFromServer(0)
    keys = encodeKeys(sessionBatch)
    regUsersSessions, tmpUsersSessions = utl.getValuesFromDb(keys,server)
    print(regUsersSessions[0])

    numOfCategoriesDict = {}
    initNumOfCategoriesDict(numOfCategoriesDict)

    categoriesDistDict = {}
    initCategoriesDistDict(categoriesDistDict)

    updateNumOfCategoriesDict(categoriesDistDict,numOfCategoriesDict,regUsersSessions,tmpUsersSessions)
    resDict = {}
    initResDict(resDict)

    # the session length distribution for both registered and temporary users
    updateSessionLengthDict(resDict,regUsersSessions,tmpUsersSessions)

    #Distribution of basket sessions by user type, weekday, hour.
    updateWithBatchData(basketAndBuySessionsDist, regAndTmpCount, regUsersSessions, tmpUsersSessions)

    #generic try
    # updateBatchSessions(resDict, regUsersSessions, tmpUsersSessions)


    batchCount = 0
    while cursor != 0:
        try:
            cursor,sessionBatch=utl.readKeysBatchFromServer(cursor)
            keys = encodeKeys(sessionBatch)
            if cursor == 0:
                break

            regUsersSessions, tmpUsersSessions = utl.getValuesFromDb(keys, server)

            updateNumOfCategoriesDict(categoriesDistDict,numOfCategoriesDict, regUsersSessions, tmpUsersSessions)
            updateWithBatchData(basketAndBuySessionsDist, regAndTmpCount, regUsersSessions, tmpUsersSessions)
            updateSessionLengthDict(resDict,regUsersSessions,tmpUsersSessions)
        except Exception as e:
            print(e)
        batchCount+=1
        print("batch number is : "+str(batchCount))
    utl.writeDictToFile(numOfCategoriesDict,"numOfCategoriesInSessions_"+month+".json")
    utl.writeDictToFile(categoriesDistDict,"categoriesDistInSessions"+month+".json")
    utl.writeDictToFile(basketAndBuySessionsDist,"basketAndBuyDist"+month+".json")
    utl.writeDictToFile(regAndTmpCount, "count"+month+".json")
    return numOfCategoriesDict, categoriesDistDict


def encodeKeys(byteKeys):
    strKeys = []
    for key in byteKeys:
        strKeys.append(key.decode("UTF-8"))
    return strKeys



def initBB_dict():
    basketAndBuySessionDist = {}
    regAndTmpCount = {}

    regAndTmpCount['tmpUsers']=0
    regAndTmpCount['regUsers']=0
    regAndTmpCount['regSessions']=0
    regAndTmpCount['tmpSessions']=0
    regAndTmpCount['regAvgSessionLength']=0
    regAndTmpCount['tmpAvgSessionLength'] = 0
    regAndTmpCount['regAvgNumOfSessions'] = 0
    regAndTmpCount['regAvgTotalDwell']=0
    regAndTmpCount['regAvgAvgDwell']=0


    hourbins = 24


    for userType in ['reg', 'tmp']:
        basketAndBuySessionDist[userType] = {}
        for sessionType in ['basket', 'basket_buy', 'basket_noBuy']:
            basketAndBuySessionDist[userType][sessionType] = {}
            basketAndBuySessionDist[userType][sessionType]["weekday"] = {}
            basketAndBuySessionDist[userType][sessionType]["hour"] = {}
            basketAndBuySessionDist[userType][sessionType]["count"] = 0
            basketAndBuySessionDist[userType][sessionType]["avgDwell"] = 0
            basketAndBuySessionDist[userType][sessionType]["totalDwell"] = 0
            for i in range(7):
                basketAndBuySessionDist[userType][sessionType]["weekday"][str(i)] = 0
            for j in range(hourbins):
                basketAndBuySessionDist[userType][sessionType]["hour"][str(j)] = 0

    return basketAndBuySessionDist, regAndTmpCount


#updates the evaluation with current batch data
def updateSessionLengthDict(sessionLengthDict, regUsersSessions, tmpUsersSessions):

    #iterate temporary users sessions
    for user in tmpUsersSessions:
        for session in user:
            if session['isBasketSession']:
                if session['isBuySession']:
                    sessionType='buy'
                else:
                    sessionType='noBuy'

                updateSessionLengthBySessionType(sessionLengthDict['basket']['tmp'],sessionType,session['numOfEvents'])
            updateSessionLengthForUserType(sessionLengthDict['tmp'],session['numOfEvents'])

    #iterate registered users sessions
    for user in regUsersSessions:
        for session in user:
            if session['isBasketSession']:
                if session['isBuySession']:
                    sessionType = 'buy'
                else:
                    sessionType = 'noBuy'

                updateSessionLengthBySessionType(sessionLengthDict['basket']['reg'], sessionType, session['numOfEvents'])
            updateSessionLengthForUserType(sessionLengthDict['reg'], session['numOfEvents'])


#updates the evaluation with current batch data
def updateNumOfCategoriesDict(categoriesDistDict,numOfCategoriesDict, regUsersSessions, tmpUsersSessions):

    #iterate temporary users sessions
    for user in tmpUsersSessions:
        sessionType = ""
        for session in user:
            isBuySession = session['isBuySession']
            categoriesSet =set(session['category2Vector'])
            if session['isBasketSession']:
                if session['isBuySession']:
                    sessionType='buy'
                else:
                    sessionType='noBuy'
            else:
                sessionType = 'other'

            updateNumOfBasketCategoriesBySessionType(numOfCategoriesDict['tmp'], sessionType,isBuySession, len(categoriesSet))
            updateCategoriesDistDict(categoriesDistDict['tmp'],sessionType, isBuySession,categoriesSet)

    #iterate registered users sessions
    for user in regUsersSessions:
        sessionType = ""
        for session in user:
            isBuySession = session['isBuySession']
            categoriesSet = set(session['category2Vector'])
            if session['isBasketSession']:
                if session['isBuySession']:
                    sessionType = 'buy'
                else:
                    sessionType = 'noBuy'
            else:
                sessionType = 'other'

            updateNumOfBasketCategoriesBySessionType(numOfCategoriesDict['reg'], sessionType,isBuySession, len(categoriesSet))
            updateCategoriesDistDict(categoriesDistDict['reg'], sessionType,isBuySession, categoriesSet)


def updateCategoriesDistDict(dict, sessionType, isBuySession, categoryVector):
    if sessionType == 'other':
        if isBuySession:
            buyString = 'buy'
        else:
            buyString = 'noBuy'

        for category in categoryVector:
            if category in dict[sessionType][buyString]:
                dict[sessionType][buyString][category] += 1
            else:
                dict[sessionType][buyString][category] = 1



    else:
        for category in categoryVector:
            if category in dict[sessionType]:
                dict[sessionType][category] += 1
            else:
                dict[sessionType][category] = 1


def updateNumOfBasketCategoriesBySessionType(dict, sessionType,isBuySession, numOfCategories):
    if sessionType == 'other':
        if isBuySession:
            buyString = 'buy'
        else:
            buyString = 'noBuy'
        dict[sessionType][buyString][str(numOfCategories)] += 1

    dict[sessionType][str(numOfCategories)] += 1


def updateSessionLengthBySessionType(dict, sessionType, sessionLength):
    dict[sessionType][str(sessionLength)]+=1


def updateSessionLengthForUserType(sessionLengthDict, sessionLength):
 sessionLengthDict[str(sessionLength)]+=1


#updates the evaluation with current batch data
def updateWithBatchData(basketAndBuySessionsDist, regAndTmpCount, regUsersSessions, tmpUsersSessions):

    #iterate temporary users sessions
    for user in tmpUsersSessions:
        regAndTmpCount['tmpUsers']+=1
        userType='tmp'
        for session in user:
            day = str(session['weekday'])
            hour = int(session['hour'])
            #hourBin = str(int(getHourBin(hour)))
            regAndTmpCount[userType+'Sessions'] += 1
            regAndTmpCount[userType+'AvgSessionLength'] = calculateAvgIteratively \
                (regAndTmpCount[userType+'Sessions'], regAndTmpCount[userType+'AvgSessionLength'], session['numOfEvents'])
            if session['isBasketSession']:
                if session['isBuySession']:
                    sessionType='buy'
                else:
                    sessionType='noBuy'
                updateBasketAndBuyDict(basketAndBuySessionsDist,regAndTmpCount,userType,sessionType,str(hour),day,session)

    #iterate registered users sessions
    for user in regUsersSessions:
        regAndTmpCount['regUsers']+=1
        userType = 'reg'
        regAndTmpCount['regAvgNumOfSessions'] = calculateAvgIteratively \
            (regAndTmpCount['regUsers'], regAndTmpCount['regAvgNumOfSessions'], len(user))

        for session in user:
            day = str(session['weekday'])
            hour = int(session['hour'])
            #hourBin = str(getHourBin(hour))
            regAndTmpCount[userType+'Sessions'] += 1
            regAndTmpCount[userType+'AvgSessionLength'] = calculateAvgIteratively \
                (regAndTmpCount[userType+'Sessions'], regAndTmpCount[userType+'AvgSessionLength'], session['numOfEvents'])
            if session['isBasketSession']:
                if session['isBuySession']:
                    sessionType='buy'
                else:
                    sessionType='noBuy'
                updateBasketAndBuyDict(basketAndBuySessionsDist,regAndTmpCount,userType,sessionType,str(hour),day,session)


#
def updateBasketAndBuyDict(basketAndBuySessionsDist,regAndTmpCount,userType,sessionType,hourBin,day,session):

    basketAndBuySessionsDist[userType]['basket']['weekday'][day] += 1
    basketAndBuySessionsDist[userType]['basket']['hour'][hourBin] += 1
    basketAndBuySessionsDist[userType]['basket_'+sessionType]['count'] += 1
    basketAndBuySessionsDist[userType]['basket_'+sessionType]['weekday'][day] += 1
    basketAndBuySessionsDist[userType]['basket_'+sessionType]['hour'][hourBin] += 1
    basketAndBuySessionsDist[userType]['basket_'+sessionType]['avgDwell'] = calculateAvgIteratively \
        (basketAndBuySessionsDist[userType]['basket_'+sessionType]['count'],
         basketAndBuySessionsDist[userType]['basket_'+sessionType]['avgDwell'], session['totalDwell']/session['numOfEvents'])
    basketAndBuySessionsDist[userType]['basket_'+sessionType]['totalDwell'] = calculateAvgIteratively\
        (basketAndBuySessionsDist[userType]['basket_'+sessionType]['count'],
         basketAndBuySessionsDist[userType]['basket_'+sessionType]['totalDwell'], session['totalDwell'])


def getHourBin(hour):
    step=1
    for i in range(24):
        if step*i<hour<step*(i+1):
            return i
    return 24/step-1


def getLengthDist(usersSessions):
    dictLength = {}
    for i in range(300):
        dictLength[str(i)] = 0
    for list in usersSessions:
        for session in list:
            dictLength[str(len(session))]+=1
    return dictLength


def getRegisteredSessionLengthDist(usersSessions):
    dictLength = {}
    for i in range(300):
        dictLength[str(i)] = 0

# Same key values are added from both dictionaries
def aggregateDicts(dict1, dict2):
    for i in range(300):
        dict1[str(i)] += dict2[str(i)]


def updateDictSessionLength(dict,regDict,tmpDict):
    dict['registered'] = aggregateDicts(dict['registered'], regDict)
    dict['temporary'] = aggregateDicts(dict['temporary'], tmpDict)


def getBasketSessions(userSessions):
    basketSessions=[]
    for list in userSessions:
        basketSessions.append([])
        for session in list:
            if session['isBasketSession']:
                basketSessions[-1].append(session)

    return basketSessions


def initResDict(resDict):
    userTypes = {'reg', 'tmp'}
    sessionTypes = {'buy', 'noBuy'}
    resDict['basket'] = {}

    for user in userTypes:
        resDict[user] = {}
        resDict['basket'][user]={}
        initCountDict(resDict[user])
        for session in sessionTypes:
            resDict['basket'][user][session]={}
            initCountDict(resDict['basket'][user][session])


def initNumOfCategoriesDict(resDict):
    userTypes = {'reg', 'tmp'}
    sessionTypes = {'buy', 'noBuy','other'}

    for user in userTypes:
        resDict[user]={}
        for session in sessionTypes:
            resDict[user][session]={}
            initCountDict(resDict[user][session])
        resDict[user]['other']['buy'] = {}
        resDict[user]['other']['noBuy'] = {}
        initCountDict(resDict[user]['other']['buy'])
        initCountDict(resDict[user]['other']['noBuy'])


def initCategoriesDistDict(resDict):
    userTypes = {'reg', 'tmp'}
    sessionTypes = {'buy', 'noBuy','other'}

    for user in userTypes:
        resDict[user]={}
        for session in sessionTypes:
            resDict[user][session]={}

        resDict[user]['other']['buy']={}
        resDict[user]['other']['noBuy'] = {}


def initCountDict(dict):
    for i in range(100):
        dict[str(i)]=0