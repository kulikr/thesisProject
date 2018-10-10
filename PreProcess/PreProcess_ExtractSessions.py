import time
import re as re
import utils as utl
from dateutil.parser import parse
from PreProcess.PreProcess_items import itemsPreProcess
from PreProcess import PreProcess_ExtractEvents as pre_events
import copy

session_id = 0
labels_dict = {}

# create sessions from the events objects
def extractSessions(eventsPath, session_idle_threshold):
    eventsInBatch = 100000
    eventList,position= pre_events.readEventsFromFile(eventsPath,eventsInBatch,0)
    openSessions = {}
    closedSessions = {}
    meaningfulEvents = ["transfer","basket","click","clickrecommended","buy"]
    accumulatedData={}
    initAccumulatedData(accumulatedData)

    #init null user events distribution and event distribution count
    for event in meaningfulEvents:
        accumulatedData['nullUser'][event] = 0
        accumulatedData['eventDist'][event] = 0

    currentDate = eventList[0]['timestamp']
    pre_items = itemsPreProcess(currentDate)
    itemsDict = pre_items.getItemsDict()

    dbBatchNum=0
    while 1:
        dbBatchNum+=1

        if eventList == None:
            break
        # iterate the log events
        for i in range(len(eventList)):

            if i%10000 == 0:
                print(str(i))
            event = eventList.pop(0)

            currentUser = event['userId']
            eventDate = event['timestamp']

            # Ignore events of 'null' users
            if currentUser == 'null':
                accumulatedData['nullUser'][event['eventType']] += 1
                continue
            else:
                accumulatedData['eventDist'][event['eventType']] += 1

            isTmpUser = isTemporaryUser(currentUser)
            updateAccumulatedEvents(isTmpUser, accumulatedData)

            # Update current date and load the current items dictionary
            if currentDate.date() != eventDate.date():
                currentDate = eventDate
                pre_items.readItemsDictFromCsv(currentDate)
                itemsDict = pre_items.getItemsDict()

            # Cases the event is a transfer there is a need to map the new id to the old id
            if event['eventType'] == 'transfer':
                if event['userId'] not in openSessions:
                    continue

                if event['userId'] != event['newUserId']:
                    openSessions[event['newUserId']] = openSessions[event['userId']]
                    del openSessions[event['userId']]
                    currentUser = event['newUserId']
                else:
                    continue

            accumulatedData['numOfEvents'] += 1
            isBasketEvent = event['eventType'] == 'basket'
            isBuyEvent = event['eventType'] == 'buy'

            if isBuyEvent:
                accumulatedData['numOfBuys'] += 1


            # If the user has an active session
            if currentUser in openSessions:

                # calculate the time user was inactive
                idleTime = (event['timestamp'] - openSessions[currentUser]['lastEvent']).total_seconds()

                # #Update the aggregated items data
                # updateItemsData(itemsAggregated, itemsDict, event, lastItemId, idleTime, isBuyEvent)

                # If the user was idle for more than threshold, add the event to new session and add the session to the closed sessions
                if idleTime > session_idle_threshold:
                    updateAccumulatedSessions(currentUser, openSessions[currentUser], accumulatedData)
                    # Remove user from open sessions and add to the closed sessions
                    saveToClosedSessionsDict(openSessions, closedSessions, currentUser)

                    if (len(closedSessions) == 1000):
                        print("number of events:" +str(i+100000*dbBatchNum)+" current date:"+str(currentDate.date()))
                        utl.writeSessionsToDb(closedSessions, currentDate.month)
                        closedSessions.clear()

                    #After appending the last user's session to the closed sessions create new session
                    createNewSession(event, itemsDict, itemsDict, currentUser, openSessions, isBuyEvent, isBasketEvent)

                # If the event is part of the session append it in the user session events
                else:
                    updateCurrentSessionWithEvent(event, pre_items ,itemsDict,currentUser, openSessions, isBuyEvent, isBasketEvent)
                    # updates both session dwell features and global dwell variables
                    updateDwell(accumulatedData, openSessions, idleTime, currentUser)
            else:
                createNewSession(event, itemsDict, pre_items , currentUser, openSessions, isBuyEvent, isBasketEvent)

            if event["eventType"] != "transfer":
                lastItemId = event["itemId"]
            else:
                lastItemId = ""
        if position == -1:
            break
        eventList, position = pre_events.readEventsFromFile(eventsPath, eventsInBatch, position)

    try:
    # write remain sessions to the user sessions db
        utl.writeSessionsToDb(openSessions, currentDate.month)
    except:
        print("failed during open sessions (not so bad)")
    return accumulatedData

# create sessions from the events objects
def extractMultipleSessions(eventsPath, session_idle_threshold):
    eventsInBatch = 100000
    eventList,position= pre_events.readEventsFromFile(eventsPath,eventsInBatch,0)
    openSessions = {}
    closedSessions = {}
    meaningfulEvents = ["transfer","basket","click","clickrecommended","buy"]
    accumulatedData={}
    initAccumulatedData(accumulatedData)

    #init null user events distribution and event distribution count
    for event in meaningfulEvents:
        accumulatedData['nullUser'][event] = 0
        accumulatedData['eventDist'][event] = 0

    currentDate = eventList[0]['timestamp']
    pre_items = itemsPreProcess(currentDate)
    itemsDict = pre_items.getItemsDict()

    dbBatchNum=0
    while 1:
        dbBatchNum+=1
            # iterate the log events
        if eventList == None:
            break
        for i in range(len(eventList)):

            if i%10000 == 0:
                print(str(i))
            event = eventList.pop(0)

            currentUser = event['userId']
            eventDate = event['timestamp']

            # Ignore events of 'null' users
            if currentUser == 'null':
                accumulatedData['nullUser'][event['eventType']] += 1
                continue
            else:
                accumulatedData['eventDist'][event['eventType']] += 1

            isTmpUser = isTemporaryUser(currentUser)
            updateAccumulatedEvents(isTmpUser, accumulatedData)

            # Update current date and load the current items dictionary
            if currentDate.date() != eventDate.date():
                currentDate = eventDate
                pre_items.readItemsDictFromCsv(currentDate)
                itemsDict = pre_items.getItemsDict()

            # Cases the event is a transfer there is a need to map the new id to the old id
            if event['eventType'] == 'transfer':
                if event['userId'] not in openSessions:
                    continue

                if event['userId'] != event['newUserId']:
                    openSessions[event['newUserId']] = openSessions[event['userId']]
                    del openSessions[event['userId']]
                    currentUser = event['newUserId']
                else:
                    continue

            accumulatedData['numOfEvents'] += 1
            isBasketEvent = event['eventType'] == 'basket'
            isBuyEvent = event['eventType'] == 'buy'


            # If the user has an active session
            if currentUser in openSessions:
                # calculate the time user was inactive
                idleTime = (event['timestamp'] - openSessions[currentUser]['lastEvent']).total_seconds()

                # #Update the aggregated items data
                # updateItemsData(itemsAggregated, itemsDict, event, lastItemId, idleTime, isBuyEvent)

                # If the user was idle for more than threshold, add the event to new session and add the session to the closed sessions
                if idleTime > session_idle_threshold:
                    updateAccumulatedSessions(currentUser, openSessions[currentUser], accumulatedData)
                    if openSessions[currentUser]["isBasketSession"]:
                        saveToClosedSessionsDict(openSessions, closedSessions, currentUser)
                    else:
                        del openSessions[currentUser]
                    # Remove user from open sessions and add to the closed sessions


                    if (len(closedSessions) == 1000):
                        print("number of events:" +str(i+100000*dbBatchNum)+" current date:"+str(currentDate.date()))
                        utl.multiple_writeSessionsToDb(closedSessions, currentDate.month)
                        closedSessions.clear()

                    #After appending the last user's session to the closed sessions create new session
                    multiple_createNewSession(event, itemsDict, itemsDict, currentUser, openSessions, isBuyEvent, isBasketEvent)

                # If the event is part of the session append it in the user session events
                else:
                    if openSessions[currentUser]["isBuySession"]:
                        openSessions[currentUser]['lastEvent'] = event['timestamp']
                        continue
                    multiple_updateCurrentSessionWithEvent(event, pre_items ,itemsDict,currentUser, openSessions, isBuyEvent, isBasketEvent)
                    # updates both session dwell features and global dwell variables
                    updateDwell(accumulatedData, openSessions, idleTime, currentUser)
                    if openSessions[currentUser]["isBasketSession"]:
                        ################# COPY OF SESSION ??????
                        saveCopyToClosedSessionsDict(openSessions, closedSessions, currentUser)
            else:
                multiple_createNewSession(event, itemsDict, pre_items , currentUser, openSessions, isBuyEvent, isBasketEvent)

            if event["eventType"] != "transfer":
                lastItemId = event["itemId"]
            else:
                lastItemId = ""
        if position == -1:
            break
        eventList, position = pre_events.readEventsFromFile(eventsPath, eventsInBatch, position)

    for user in openSessions:
        updateAccumulatedSessions(user, openSessions[user], accumulatedData)
    try:
    # write remain sessions to the user sessions db

        utl.writeSessionsToDb(openSessions, currentDate.month)
    except:
        print("failed during open sessions (not so bad)")
    return accumulatedData


#update the buy and session accumulated_06 data dictionary
def updateAccumulatedSessions(userId, session, accumulatedData):
    if isTemporaryUser(userId):
        if session['isBasketSession']:
            accumulatedData['numOfTmpBasketSessions'] += 1

        if session['isBuySession']:
            accumulatedData['numOfTmpBuySessions'] += 1
    else:
        if session['isBasketSession']:
            accumulatedData['numOfRegBasketSessions'] += 1

        if session['isBuySession']:
            accumulatedData['numOfRegBuySessions'] += 1
    updateAccumulatedSessionsCount(accumulatedData,isTemporaryUser(userId))


# Function that creates new session and adds the entry to the open session dict
def multiple_createNewSession(event, itemsDict, pre_items , currentUser, openSessions, isBuyEvent, isBasketEvent):
    global session_id
    global labels_dict
    openSessions[currentUser] = {}
    openSessions[currentUser]["sessionId"] = session_id
    labels_dict[str(session_id)] = isBuyEvent
    session_id+=1
    openSessions[currentUser]['isBuySession'] = isBuyEvent
    openSessions[currentUser]['isBasketSession'] = isBasketEvent
    openSessions[currentUser]['dwellVector'] = []

    openSessions[currentUser]['numOfEvents'] = 1

    if(isBasketEvent):
        openSessions[currentUser]['numOfBasketEvents'] = 1
    else:
        openSessions[currentUser]['numOfBasketEvents'] = 0

    # Dwell features
    openSessions[currentUser]['maxDwell'] = 0
    openSessions[currentUser]['minDwell'] = 1000000000000
    openSessions[currentUser]['totalDwell'] = 0

    # Purchase properties - updated on 'updatePurchaseProperties'
    openSessions[currentUser]['numOfPurchasedItems'] = 0  # update the number when session is saved
    openSessions[currentUser]['numOfSaleItemsBought'] = 0  # not yet implemented
    openSessions[currentUser]['totalAmountOfPayment'] = 0  # amount paid by the user in the session
    openSessions[currentUser]['maximalBoughtItemPrice'] = 0

    # Time Features - no update needed
    openSessions[currentUser]['month'] = event['timestamp'].month
    openSessions[currentUser]['day'] = event['timestamp'].day
    openSessions[currentUser]['hour'] = event['timestamp'].hour
    openSessions[currentUser]['weekday'] = event['timestamp'].weekday()
    openSessions[currentUser]['lastEvent'] = event['timestamp']
    openSessions[currentUser]['firstEvent'] = event['timestamp']


    # Items Features - updated on 'updateSessionWithitemsFeatures'
    openSessions[currentUser]['category1Vector'] = []
    openSessions[currentUser]['category2Vector'] = []
    openSessions[currentUser]['priceVector'] = []
    openSessions[currentUser]['basketEvents'] =[]

    # Measure time between basket events (first dwell is from the beginning to first basket event)
    openSessions[currentUser]['basketDwellVector']= []
    # Measure time between basket and first buy event
    openSessions[currentUser]['basketBuyDwell'] = 0

    openSessions[currentUser]['lastBasketEvent'] = event['timestamp']
    if isBasketEvent:
        openSessions[currentUser]['basketEvents'].append(openSessions[currentUser]['numOfEvents'])



    # Updates
    updateSessionWithItemsFeatures(currentUser, pre_items, openSessions, itemsDict,event['itemId'])

    if isBuyEvent:
        updatePurchaseProperties(event, openSessions, currentUser)

# updates the session features with the event data
def multiple_updateCurrentSessionWithEvent(event, pre_items,itemsDict, currentUser, openSessions, isBuyEvent, isBasketEvent):
    openSessions[currentUser]['numOfEvents'] += 1

    if isBasketEvent:
        openSessions[currentUser]['numOfBasketEvents'] += 1
        openSessions[currentUser]['basketDwellVector'].append((event['timestamp']-openSessions[currentUser]['lastBasketEvent']).total_seconds())
        openSessions[currentUser]['lastBasketEvent'] = event['timestamp']
        openSessions[currentUser]['basketEvents'].append(openSessions[currentUser]['numOfEvents'])

    openSessions[currentUser]['isBasketSession'] = openSessions[currentUser]['isBasketSession'] or isBasketEvent
    openSessions[currentUser]['isBuySession'] = openSessions[currentUser]['isBuySession'] or isBuyEvent


    openSessions[currentUser]['lastEvent'] = event['timestamp']

    # openSessions[currentUser]['events'].append(event)
    if isBuyEvent:
        labels_dict[str(openSessions[currentUser]["sessionId"])] = isBuyEvent
        updatePurchaseProperties(event, openSessions, currentUser)
        if openSessions[currentUser]['basketBuyDwell'] == 0:
            openSessions[currentUser]['basketBuyDwell'] = (event['timestamp'] - openSessions[currentUser]['lastBasketEvent']).total_seconds()
    if event["eventType"] == "transfer":
        return
    updateSessionWithItemsFeatures(currentUser, pre_items, openSessions, itemsDict, event['itemId'])



# Function that creates new session and adds the entry to the open session dict
def createNewSession(event, itemsDict, pre_items , currentUser, openSessions, isBuyEvent, isBasketEvent):
    openSessions[currentUser] = {}
    openSessions[currentUser]['isBuySession'] = isBuyEvent
    openSessions[currentUser]['isBasketSession'] = isBasketEvent
    openSessions[currentUser]['dwellVector'] = []

    openSessions[currentUser]['numOfEvents'] = 1

    if(isBasketEvent):
        openSessions[currentUser]['numOfBasketEvents'] = 1
    else:
        openSessions[currentUser]['numOfBasketEvents'] = 0

    # Dwell features
    openSessions[currentUser]['maxDwell'] = 0
    openSessions[currentUser]['minDwell'] = 1000000000000
    openSessions[currentUser]['totalDwell'] = 0

    # Purchase properties - updated on 'updatePurchaseProperties'
    openSessions[currentUser]['numOfPurchasedItems'] = 0  # update the number when session is saved
    openSessions[currentUser]['numOfSaleItemsBought'] = 0  # not yet implemented
    openSessions[currentUser]['totalAmountOfPayment'] = 0  # amount paid by the user in the session
    openSessions[currentUser]['maximalBoughtItemPrice'] = 0

    # Time Features - no update needed
    openSessions[currentUser]['month'] = event['timestamp'].month
    openSessions[currentUser]['day'] = event['timestamp'].day
    openSessions[currentUser]['hour'] = event['timestamp'].hour
    openSessions[currentUser]['weekday'] = event['timestamp'].weekday()
    openSessions[currentUser]['lastEvent'] = event['timestamp']
    openSessions[currentUser]['firstEvent'] = event['timestamp']


    # Items Features - updated on 'updateSessionWithitemsFeatures'
    openSessions[currentUser]['category1Vector'] = []
    openSessions[currentUser]['category2Vector'] = []
    openSessions[currentUser]['priceVector'] = []
    openSessions[currentUser]['basketEvents'] =[]

    # Measure time between basket events (first dwell is from the beginning to first basket event)
    openSessions[currentUser]['basketDwellVector']= []
    # Measure time between basket and first buy event
    openSessions[currentUser]['basketBuyDwell'] = 0

    openSessions[currentUser]['lastBasketEvent'] = event['timestamp']
    if isBasketEvent:
        openSessions[currentUser]['basketEvents'].append(openSessions[currentUser]['numOfEvents'])



    # Updates
    updateSessionWithItemsFeatures(currentUser, pre_items, openSessions, itemsDict,event['itemId'])

    if isBuyEvent:
        updatePurchaseProperties(event, openSessions, currentUser)


    # openSessions[currentUser]['boughtItems'] = {}  # create {key:item , value:quantity}


# updates the session features with the event data
def updateCurrentSessionWithEvent(event, pre_items,itemsDict, currentUser, openSessions, isBuyEvent, isBasketEvent):
    openSessions[currentUser]['numOfEvents'] += 1

    if isBasketEvent:
        openSessions[currentUser]['numOfBasketEvents'] += 1
        openSessions[currentUser]['basketDwellVector'].append((event['timestamp']-openSessions[currentUser]['lastBasketEvent']).total_seconds())
        openSessions[currentUser]['lastBasketEvent'] = event['timestamp']
        openSessions[currentUser]['basketEvents'].append(openSessions[currentUser]['numOfEvents'])

    openSessions[currentUser]['isBasketSession'] = openSessions[currentUser]['isBasketSession'] or isBasketEvent
    openSessions[currentUser]['isBuySession'] = openSessions[currentUser]['isBuySession'] or isBuyEvent



    openSessions[currentUser]['lastEvent'] = event['timestamp']

    # openSessions[currentUser]['events'].append(event)
    if isBuyEvent:
        updatePurchaseProperties(event, openSessions, currentUser)
        if openSessions[currentUser]['basketBuyDwell'] == 0:
            openSessions[currentUser]['basketBuyDwell'] = (event['timestamp'] - openSessions[currentUser]['lastBasketEvent']).total_seconds()
    if event["eventType"] == "transfer":
        return
    updateSessionWithItemsFeatures(currentUser, pre_items, openSessions, itemsDict, event['itemId'])


# add bought items to a quantity dictionary
def updatePurchaseProperties(event, openSessions, currentUser):
    try:
        splittedInfo = re.split("=|&|E", event["extraInfo"])

        quantity = int(splittedInfo[1])
        price = float(splittedInfo[3])

        # ## ADD TO ITEMS DICT
        # if (event['itemId'] in openSessions[currentUser]['boughtItems']):
        #     openSessions[currentUser]['boughtItems'][event['itemId']] += quantity
        # else:
        #     openSessions[currentUser]['boughtItems'][event['itemId']] = quantity

        openSessions[currentUser]['numOfPurchasedItems'] += 1

        if price > openSessions[currentUser]['maximalBoughtItemPrice']:
            openSessions[currentUser]['maximalBoughtItemPrice'] = float(splittedInfo[3])

        # openSessions[currentUser]['numOfSaleItemsBought'] = 0  # not yet implemented (need to preProcess items catalogue)
        openSessions[currentUser]['totalAmountOfPayment'] += price * quantity
    except:
        pass


# Updates both the global dwelldwell time as well as the sessions dwell time
def updateDwell(accumulatedData, openSessions, currentDwell, currentUser):
    updateSessionDwell(openSessions, currentDwell, currentUser)
    updateAvgDwell(accumulatedData, currentDwell)


# Updates the global dwell time stored in 'accumulatedData' (avg dwell, total number of dwells)
def updateAvgDwell(accumulatedData, currentDwell):
    accumulatedData['numOfDwells'] += 1
    numOfDwells = accumulatedData['numOfDwells']
    averageDwell = accumulatedData['averageDwell']
    if (accumulatedData['numOfDwells'] > 1):
        accumulatedData['averageDwell'] = averageDwell * (
                numOfDwells - 1) / numOfDwells + currentDwell / numOfDwells
    else:
        accumulatedData['averageDwell'] = currentDwell
    return accumulatedData


# Updates the global dwell time stored in 'accumulatedData' (avg dwell, total number of dwells)
def updateAvgNumOfEvents(accumulatedData, currentSession):
    numOfSessions = accumulatedData['numOfSessions']
    avgNumOfEvents = accumulatedData['avgNumOfEvents']
    currentNumOfEvents = currentSession['numOfEvents']

    if (numOfSessions > 1):
        accumulatedData['avgNumOfEvents'] = avgNumOfEvents * (
                numOfSessions - 1) / numOfSessions + currentNumOfEvents / numOfSessions
    else:
        accumulatedData['avgNumOfEvents'] = currentNumOfEvents
    return accumulatedData


# Updates the current session dwell data stored at 'openSessions' (min dwell,max dwell)
def updateSessionDwell(openSessions, currentDwell, currentUser):
    if (currentDwell > openSessions[currentUser]['maxDwell']):
        openSessions[currentUser]['maxDwell'] = currentDwell
    if (currentDwell < openSessions[currentUser]['minDwell']):
        openSessions[currentUser]['minDwell'] = currentDwell
    openSessions[currentUser]['totalDwell'] += currentDwell
    openSessions[currentUser]['dwellVector'].append(currentDwell)


# Uses the transfer dictionary to link the current username clicks with the username given by the system before login
def updateCurrentUser(currentUser, transferDict):
    # Check whether the current user as an older ID (before login a temp id in must cases)
    if currentUser in transferDict:
        return transferDict[currentUser]
    else:
        return currentUser


# updates the current session with its items data
def updateSessionWithItemsFeatures(currentUser, pre_items, openSessions, itemsDict, itemId):

    if itemId in itemsDict:
        openSessions[currentUser]['category1Vector'].append(itemsDict[itemId]['categorie'])
        openSessions[currentUser]['category2Vector'].append(itemsDict[itemId]['Kategorie 1'])
        openSessions[currentUser]['priceVector'].append(itemsDict[itemId]['price'])

    else:
        openSessions[currentUser]['category1Vector'].append('x')
        openSessions[currentUser]['category2Vector'].append('x')
        openSessions[currentUser]['priceVector'].append('x')


# update the items data dictionary
def initItemDict(itemsAggregated, itemId):
    itemsAggregated[itemId]={}
    itemsAggregated[itemId]['numOfClicks'] = 0
    itemsAggregated[itemId]['totalDwell']= 0
    itemsAggregated[itemId]['numOfBuys'] = 0
    itemsAggregated[itemId]['promotedBuys'] = 0


def updateItemsData(itemsAggregated, itemsDict, event, lastItemId, idleTime, isBuyEvent):
    itemId = event["itemId"]
    if itemId not in itemsAggregated:
        initItemDict(itemsAggregated,itemId)

    itemsAggregated[itemId]['numOfClicks']+=1
    itemsAggregated[lastItemId]['totalDwell']+=idleTime
    itemsAggregated[itemId]['numOfBuys']+=1
    if itemId in itemsDict:
        if isItemPromoted(event, itemsDict):
            itemsAggregated[itemId]['promotedBuys']+=1


# returns whether the current event item is promoted or not
def isItemPromoted(event, itemsDict):
    itemId = event["itemId"]
    if itemsDict[itemId,"wt_start"] == 0:  # if there is start date to the dict it means it has promotion
        return False
    else:
        start_date = parse(itemsDict[itemId,"wt_start"])
        end_date = parse(itemsDict[itemId, "wt_end"])

    event_date = event["timestamp"]

    if start_date <= event_date <= end_date:
        return True
    return False


# returns whether the user is a temporary user
def isTemporaryUser(username):
    if '-' in username:
        return True
    return False


# Add the ended session to the closed session list
def saveToClosedSessionsDict(openSessions, closedSessions, userId):
    if userId not in closedSessions:
        closedSessions[userId]=[]
    closedSessions[userId].append(openSessions[userId])
    del openSessions[userId]

# Add the ended session to the closed session list
def saveCopyToClosedSessionsDict(openSessions, closedSessions, userId):
    if userId not in closedSessions:
        closedSessions[userId]=[]
    openSessions[userId]["isBuySession"] = labels_dict[str(openSessions[userId]["sessionId"])]
    closedSessions[userId].append(copy.deepcopy(openSessions[userId]))


def updateAccumulatedEvents(isTmpUser, accumulatedData):
    if isTmpUser:
        accumulatedData['numOfRegEvents'] = 0
        accumulatedData['numOfTmpEvents'] = 0


def updateAccumulatedSessionsCount(accumulatedData, isTmpUser):
    if isTmpUser:
        accumulatedData['numOfTmpSessions'] += 1
    else:
        accumulatedData['numOfRegSessions'] += 1

    accumulatedData['numOfSessions']+=1


def initAccumulatedData(accumulatedData):
    accumulatedData['numOfSessions'] = 0
    accumulatedData['numOfBuys'] = 0
    accumulatedData['numOfRegBasketSessions'] = 0
    accumulatedData['numOfRegBuySessions'] = 0
    accumulatedData['numOfTmpBasketSessions'] = 0
    accumulatedData['numOfTmpBuySessions'] = 0
    accumulatedData['numOfRegSessions'] = 0
    accumulatedData['numOfTmpSessions'] = 0
    accumulatedData['numOfEvents']=0
    accumulatedData['numOfRegEvents'] = 0
    accumulatedData['numOfTmpEvents'] = 0
    accumulatedData['averageDwell'] = 0
    accumulatedData['numOfDwells'] = 0
    accumulatedData['nullUser'] = {}
    accumulatedData['eventDist'] = {}