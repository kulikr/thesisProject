import time
import re as re
import utils as utl
import math
import copy
from dateutil.parser import parse
from PreProcess import PreProcess_items


event_types = ["transfer", "basket", "click", "clickrecommended", "buy", "rendered"]

### need to take care of the transfer event
# Dictionary that contains the batch events and a threshold for the length of a batch


# create sessions from the events objects
def extractSessions(eventList, session_idle_threshold):
    openSessions = {}
    closedSessions = []
    accumulatedData = {}
    numberOfBasketSessions = 0
    numberOfBuySessions = 0

    usersEventsPerSession = {}
    numOfSessions = 0
    numOfEvents = 0

    # iterate the log events
    for i in range(len(eventList)):

        event = eventList.pop(0)

        if(event['eventType'] == 'rendered'):
            continue

        currentUser = event['userId']
        currentMonth= event['timestamp'].month

        if(i%1000 == 0):
            print(i)


        # Cases the event is a transfer there is a need to map the new id to the old id
        if event['eventType'] == 'transfer':
            if event['userId'] not in openSessions:
                continue
            if event['userId'] != event['newUserId']:
                openSessions[event['newUserId']] = openSessions[event['userId']]
                del openSessions[event['userId']]

                if event['userId'] in usersEventsPerSession:
                    usersEventsPerSession[event['newUserId']] = copy.deepcopy(usersEventsPerSession[event['userId']])
                    del usersEventsPerSession[event['userId']]

        else:
            numOfEvents += 1


        # If the user has an active session
        if currentUser in openSessions:

            idleTime = (event['timestamp'] - openSessions[currentUser]['lastEvent']).total_seconds()


            # If the user was idle for to long add the event to new session and add the session to the closed sessions
            if (idleTime > session_idle_threshold):
                #Close current sessions

                numOfSessions+=1
                if(openSessions[currentUser]['isBasketSession']):
                    numberOfBasketSessions+=1

                if (openSessions[currentUser]['isBuySession']):
                    numberOfBuySessions += 1

                updateUsersSessionDict(usersEventsPerSession, openSessions, currentUser)
                closedSessions.append(openSessions[currentUser])

                if (len(closedSessions) == 1000):
                    print("writing next 1000 sessions")
                    utl.writeSessionsDataToCsv(closedSessions)
                    closedSessions.clear()
                #Create new session
                createNewSession(event, currentUser, openSessions)

            # If the event is part of the session append it in the user session events
            else:
                updateCurrentSessionWithEvent(event, currentUser, openSessions)
        else:
            createNewSession(event, currentUser, openSessions)

    # write remain sessions to the sessions file

    for user in openSessions:
        numOfSessions += 1
        if (openSessions[user]['isBasketSession']):
            numberOfBasketSessions += 1

        if (openSessions[user]['isBuySession']):
            numberOfBuySessions += 1


        updateUsersSessionDict(usersEventsPerSession, openSessions, user)

    utl.writeSessionsDataToCsv(list(openSessions.values()))
    utl.writeUsersEventsDistToCsv(usersEventsPerSession, currentMonth)
    utl.writeNumOfSessions(numOfSessions,numberOfBasketSessions,numberOfBuySessions,numOfEvents,currentMonth)

    return accumulatedData


# Function that creates new session and appends it to the open session list
def createNewSession(event, currentUser, openSessions):
    openSessions[currentUser] = {}
    openSessions[currentUser]['userId'] = currentUser
    openSessions[currentUser]['isBasketSession'] = 0
    openSessions[currentUser]['isBuySession'] = 0
    # openSessions[currentUser]['year'] = event['timestamp'].year
    # openSessions[currentUser]['month'] = event['timestamp'].month
    # openSessions[currentUser]['day'] = event['timestamp'].day
    # openSessions[currentUser]['hour'] = event['timestamp'].hour
    # openSessions[currentUser]['weekday'] = event['timestamp'].weekday()
    # openSessions[currentUser]['isWeekend'] = event['timestamp'].weekday() in [5, 6]
    openSessions[currentUser]['lastEvent'] = event['timestamp']
    if event["eventType"] != "rendered":
        openSessions[currentUser]["numOfEvents"] = 1
    else:
        openSessions[currentUser]["numOfEvents"] = 0

    for eventType in event_types:
        if eventType not in ["rendered","transfer"]:
            openSessions[currentUser][eventType] = 0
    openSessions[currentUser][event["eventType"]] = 1

    openSessions[currentUser]["isBasketSession"] = openSessions[currentUser]["isBasketSession"] or \
                                                 event["eventType"] == "basket"
    openSessions[currentUser]['isBuySession'] = openSessions[currentUser]["isBuySession"] or \
                                                 event["eventType"] == "buy"


# updates the session features with the event data
def updateCurrentSessionWithEvent(event, currentUser, openSessions):
    if event["eventType"] != "rendered" and event["eventType"] != "transfer":
        openSessions[currentUser]['numOfEvents'] += 1
        openSessions[currentUser][event['eventType']] += 1

    openSessions[currentUser]["isBasketSession"] = openSessions[currentUser]["isBasketSession"] or event["eventType"] == "basket"
    openSessions[currentUser]['isBuySession'] = openSessions[currentUser]["isBuySession"] or \
                                                 event["eventType"] == "buy"
    openSessions[currentUser]['lastEvent'] = event['timestamp']


# updates dictionary that contains for each user a vector of his session's lengths
def updateUsersSessionDict(usersEventsPerSession, openSessions, currentUser):

    if currentUser not in usersEventsPerSession:
        usersEventsPerSession[currentUser] = []
    usersEventsPerSession[currentUser].append(openSessions[currentUser]['numOfEvents'])

# returns whether the user is a temporary user
def isTemporaryUser(username):
    if '-' in username:
        return True
    return False

