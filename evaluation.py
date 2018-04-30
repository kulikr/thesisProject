from matplotlib import pyplot as plot
import pandas as pd
import numpy as np
import utils as utl
import pickle


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




