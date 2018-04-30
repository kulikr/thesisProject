import time
import re as re
import utils as utl
import math
import copy
from dateutil.parser import parse
from PreProcess.PreProcess_items import itemsPreProcess



### need to take care of the transfer event
# Dictionary that contains the batch events and a threshold for the length of a batch

# create sessions from the events objects
def extractSessions(eventList, session_idle_threshold):
    openSessions = {}
    closedSessions = []
    transferDict = {}
    accumulatedData = {}
    usersEventsPerSession = {}
    itemsAggregated = {}
    meaningfulEvents = ["transfer","basket","click","clickrecommended","buy"]
    lastItemId = ""
    accumulatedData['numOfSessions'] = 0
    accumulatedData['numOfBuys'] = 0
    accumulatedData['numOfBasketSessions'] = 0
    accumulatedData['averageDwell'] = 0
    accumulatedData['numOfDwells'] = 0


    currentDate = eventList[0]['timestamp']
    pre_items = itemsPreProcess(currentDate)
    itemsDict = pre_items.getItemsDict()

    # iterate the log events
    for i in range(len(eventList)):
        event = eventList.pop(0)

        # Examine only important events
        if event["eventType"] not in meaningfulEvents:
            continue

        currentUser = event['userId']
        eventDate = event['timestamp']

        # Ignore events of 'null' users
        if(currentUser == 'null'):
            continue

        accumulatedData['numOfEvents']+=1

        # Update current date and load the current items dictionary
        if currentDate.date() != eventDate.date():
            currentDate = eventDate
            itemsDict = pre_items.readItemsDictFromCsv(currentDate)


        # Cases the event is a transfer there is a need to map the new id to the old id
        if event['eventType'] == 'transfer':
            if event['userId'] not in openSessions:
                continue

            if event['userId'] != event['newUserId']:
                openSessions[event['newUserId']] = openSessions[event['userId']]
                del openSessions[event['userId']]

                if event['userId'] in usersEventsPerSession:
                    usersEventsPerSession[event['newUserId']] = (usersEventsPerSession[event['userId']])
                    del usersEventsPerSession[event['userId']]

        isBasketEvent = event['eventType'] == 'basket'

        isBuyEvent = event['eventType'] == 'buy'

        if isBuyEvent:
            accumulatedData['numOfBuys'] += 1


        # If the user has an active session
        if currentUser in openSessions:

            # calculate the time user was inactive
            idleTime = (event['timestamp'] - openSessions[currentUser]['lastEvent']).total_seconds()

            #Update the aggregated items data
            updateItemsData(itemsAggregated, itemsDict, event, lastItemId, idleTime, isBuyEvent)

            # If the user was idle for more than threshold, add the event to new session and add the session to the closed sessions
            if (idleTime > session_idle_threshold):
                accumulatedData['numOfSessions'] += 1
                updateUsersSessionDict(usersEventsPerSession, openSessions, transferDict, currentUser)
                # Remove user from open sessions and add to the closed sessions
                closedSessions.append(openSessions[currentUser])
                del openSessions[currentUser]

                if (len(closedSessions) == 5000):
                    utl.writeSessionsDataToCsv(openSessions.values())
                    closedSessions.clear()

                #After appending the last user's session to the closed sessions create new session
                createNewSession(event, itemsDict, itemsDict, currentUser, openSessions, isBuyEvent, isBasketEvent)

            # If the event is part of the session append it in the user session events
            else:
                updateCurrentSessionWithEvent(event, itemsDict,currentUser, openSessions, isBuyEvent, isBasketEvent)
                # updates both session dwell features and global dwell variables
                updateDwell(accumulatedData, openSessions, idleTime, currentUser)
        else:
            createNewSession(event, itemsDict, pre_items , currentUser, openSessions, isBuyEvent, isBasketEvent)

        if event["eventType"] != "transfer":
            lastItemId = event["itemId"]
        else:
            lastItemId = ""

    accumulatedData['numOfSessions'] += len(openSessions)

    # write remain sessions to the sessions file
    utl.writeSessionsDataToCsv(openSessions)

    return accumulatedData


# Function that creates new session and appends it to the open session list
def createNewSession(event, itemsDict, pre_items , currentUser, openSessions, isBuyEvent, isBasketEvent):
    openSessions[currentUser] = {}
    openSessions[currentUser]['isBuySession'] = isBuyEvent
    openSessions[currentUser]['isBasketSession'] = isBasketEvent

    openSessions[currentUser]['numOfEvents'] = 1

    if(isBasketEvent):
        openSessions[currentUser]['numOfBasketEvents'] = 1
    else:
        openSessions[currentUser]['numOfBasketEvents'] = 0

    # Dwell features
    openSessions[currentUser]['maxDwell'] = 0
    openSessions[currentUser]['minDwell'] = 1000000000000
    openSessions[currentUser]['avgDwell'] = 0
    openSessions[currentUser]['totalDwell'] = 0

    # Purchase properties - updated on 'updatePurchaseProperties'
    openSessions[currentUser]['maximalItemPrice'] = 0
    openSessions[currentUser]['numOfPurchasedItems'] = 0  # update the number when session is saved
    openSessions[currentUser]['numOfSaleItemsBought'] = 0  # not yet implemented
    openSessions[currentUser]['totalAmountOfPayment'] = 0  # amount paid by the user in the session


    # Time Features - no update needed
    openSessions[currentUser]['year'] = event['timestamp'].year
    openSessions[currentUser]['month'] = event['timestamp'].month
    openSessions[currentUser]['day'] = event['timestamp'].day
    openSessions[currentUser]['hour'] = event['timestamp'].hour
    openSessions[currentUser]['weekday'] = event['timestamp'].weekday()
    openSessions[currentUser]['isWeekend'] = event['timestamp'].weekday() in [5, 6]
    openSessions[currentUser]['lastEvent'] = event['timestamp']

    # Items Features - updated on 'updateSessionWithitemsFeatures'
    openSessions[currentUser]['category1Vector'] = []
    openSessions[currentUser]['category2Vector'] = []
    openSessions[currentUser]['priceVector'] = []

    # Updates
    updateSessionWithItemsFeatures(currentUser, pre_items, openSessions, itemsDict[event['itemId']])

    if (isBuyEvent):
        updatePurchaseProperties(event, openSessions, currentUser)


    # openSessions[currentUser]['boughtItems'] = {}  # create {key:item , value:quantity}

# updates the session features with the event data
def updateCurrentSessionWithEvent(event, itemsDict, currentUser, openSessions, isBuyEvent, isBasketEvent):

    openSessions[currentUser]['isBasketSession'] = (openSessions[currentUser]['isBasketSession'] or isBasketEvent)
    if(isBasketEvent):
        openSessions[currentUser]['numOfBasketEvents'] += 1
    openSessions[currentUser]['isBuySession'] = (openSessions[currentUser]['isBuySession'] or isBuyEvent)


    openSessions[currentUser]['numOfEvents'] += 1

    openSessions[currentUser]['lastEvent'] = event['timestamp']
    # openSessions[currentUser]['events'].append(event)
    if (isBuyEvent):
        updatePurchaseProperties(event, openSessions, currentUser)
    updateSessionWithItemsFeatures(currentUser, openSessions, itemsDict)


# add bought items to a quantity dictionary
def updatePurchaseProperties(event, openSessions, currentUser):
    splittedInfo = re.split("=|&|E", event["extraInfo"])

    quantity = int(splittedInfo[1])
    price = float(splittedInfo[3])

    # ## ADD TO ITEMS DICT
    # if (event['itemId'] in openSessions[currentUser]['boughtItems']):
    #     openSessions[currentUser]['boughtItems'][event['itemId']] += quantity
    # else:
    #     openSessions[currentUser]['boughtItems'][event['itemId']] = quantity

    openSessions[currentUser]['numOfPurchasedItems'] += 1

    if (price > openSessions[currentUser]['maximalItemPrice']):
        openSessions[currentUser]['maximalItemPrice'] = float(splittedInfo[3])

    openSessions[currentUser]['numOfSaleItemsBought'] = 0  # not yet implemented (need to preProcess items catalogue)
    openSessions[currentUser]['totalAmountOfPayment'] += price * quantity


# Updates both the global dwell time as well as the sessions dwell time
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


# Uses the transfer dictionary to link the current username clicks with the username given by the system before login
def updateCurrentUser(currentUser, transferDict):
    # Check whether the current user as an older ID (before login a temp id in must cases)
    if currentUser in transferDict:
        return transferDict[currentUser]
    else:
        return currentUser




# updates the current session with its items data
def updateSessionWithItemsFeatures(currentUser, pre_items, openSessions, itemInfo):
    openSessions[currentUser]['category1Vector'].append(pre_items.getCategory1NumericalValue(itemInfo['categorie']))
    openSessions[currentUser]['category2Vector'].append(pre_items.getCategory2NumericalValue(itemInfo['Kategorie 1']))
    openSessions[currentUser]['priceVector'].append(itemInfo('price'))


# update the items data dictionary
def updateItemsData(itemsAggregated, itemsDict, event, lastItemId, idleTime, isBuyEvent):
    itemId = event["itemId"]
    itemsAggregated[itemId]['numOfClicks']+=1
    itemsAggregated[lastItemId]['totalDwell']+=idleTime
    itemsAggregated[itemId]['numOfBuys']+=1
    if isItemPromoted(event, itemsDict):
        itemsAggregated[itemId]['promotedBuys']+=1


# returns whether the current event item is promoted or not
def isItemPromoted(event, itemsDict):
    itemId = event["itemId"]
    if math.isnan(itemsDict[itemId,"wt_start"]):  # if there is start date to the dict it means it has promotion
        return False
    else:
        start_date = parse(itemsDict[itemId,"wt_start"])
        end_date = parse(itemsDict[itemId, "wt_end"])

    event_date = event["timestamp"]

    if start_date <= event_date <= end_date:
        return True
    return False


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