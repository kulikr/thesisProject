import utils as utl
from PreProcess.PreProcess_items import itemsPreProcess
from PreProcess import PreProcess_ExtractEvents as pre_events
import copy

eventEntries = ["timestamp","eventType", "dwellTime", "price", "category1", "category2"]



def extractSessions(eventsPath, session_idle_threshold):
    session_id = 0
    labels_dict = {}
    eventsInBatch = 100000
    eventList,position= pre_events.readEventsFromFile(eventsPath,eventsInBatch,0)
    openSessions = {}
    closedSessions = {}
    currentDate = eventList[0]['timestamp']
    pre_items = itemsPreProcess(currentDate)

    # Load The Current Date Items Catalogue to Memory
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
                continue

            isTmpUser = utl.isTemporaryUser(currentUser)
            isBasketEvent = event['eventType'] == 'basket'
            isBuyEvent = event['eventType'] == 'buy'


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



            # If the user has an active session
            if currentUser in openSessions:

                # calculate the time user was inactive
                idleTime = (event['timestamp'] - openSessions[currentUser]['events'][-1]['timestamp']).total_seconds()

                # If the user was idle for more than threshold, add the event to new session and add the session to the closed sessions
                if idleTime > session_idle_threshold:
                    # Remove user from open sessions and add to the closed sessions
                    openSessions[currentUser]["isTmp"] = utl.isTemporaryUser(currentUser)
                    saveToClosedSessionsDict(openSessions, closedSessions, currentUser)

                    if (len(closedSessions) == 1000):
                        print("number of events:" +str(i+100000*dbBatchNum)+" current date:"+str(currentDate.date()))
                        # clear the sessions that ended from memory to disk
                        utl.writeSessionsToFile(closedSessions, currentDate.month)
                        closedSessions.clear()

                    #After appending the last user's session to the closed sessions create new session
                    createNewSession_eventwise(event, itemsDict, itemsDict, currentUser, openSessions, isTmpUser)

                # If the event is part of the session append it in the user session events
                else:
                    if openSessions[currentUser]["isBuySession"]:
                        continue
                    updateCurrentSessionWithEvent(event, pre_items ,itemsDict,currentUser, openSessions,idleTime)
                    saveCopyToClosedSessionsDict(openSessions, closedSessions, currentUser)
            else:
                createNewSession_eventwise(event, itemsDict, pre_items, currentUser, openSessions, isTmpUser)

            if event["eventType"] != "transfer":
                lastItemId = event["itemId"]
            else:
                lastItemId = ""
        if position == -1:
            break
        eventList, position = pre_events.readEventsFromFile(eventsPath, eventsInBatch, position)

    try:
    # write remain sessions to the user sessions db
        for user in openSessions:
            openSessions[user]["isTmp"] = utl.isTemporaryUser(user)

        utl.writeSessionsToFile(openSessions, currentDate.month)
    except:
        print("failed during open sessions (not so bad)")


# updates the current session with its items data
def updateEventWithItemsFeatures(event, pre_items, itemsDict, itemId):
    if itemId in itemsDict:
        event['category1'] = itemsDict[itemId]['categorie']
        event['category2'] = itemsDict[itemId]['Kategorie 1']
        event['price'] = itemsDict[itemId]['price']

######### ************** #############
#####################################

# Function that creates new session and adds the entry to the open session dict
def createNewSession_eventwise(event, itemsDict, pre_items, currentUser, openSessions, isTmpUser, labels_dict):
    global eventEntries
    global sessionId
    openSessions[currentUser] = {}
    openSessions[currentUser]['events'] = []

    openSessions[currentUser]["sessionId"] = sessionId
    openSessions[currentUser]["isBuySession"] = event['eventType'] == 'buy'
    labels_dict[str(sessionId)] = event['eventType'] == 'buy'
    openSessions[currentUser]["isBasketSession"] = event['eventType'] == 'basket'
    sessionId +=1

    event["dwellTime"] = 0
    event["price"] = 0
    event["category1"] = "x"
    event["category2"] = "x"

    if not event["eventType"] == "transfer":
        updateEventWithItemsFeatures(event, pre_items, itemsDict, event['itemId'])

    event = {key: event[key] for key in event if key in eventEntries}

    openSessions[currentUser]['events'].append(event)
    # Updates


# Function that creates new session and adds the entry to the open session dict
def createNewSession_sessionwise(event, itemsDict, pre_items, currentUser, openSessions, isBuyEvent, isBasketEvent):
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

######################################



## ********************************** ##
#######################################

# updates the session features with the event data
def updateCurrentSessionWithEvent(event, pre_items, itemsDict, currentUser, openSessions, dwellTime, labels_dict):
    global eventEntries
    event["dwellTime"] = 0
    event["price"] = 0
    event["category1"] = "x"
    event["category2"] = "x"

    isBasketEvent = event['eventType'] == 'basket'
    isBuyEvent = event['eventType'] == 'buy'

    openSessions[currentUser]['isBasketSession'] = openSessions[currentUser]['isBasketSession'] or isBasketEvent
    openSessions[currentUser]['isBuySession'] = openSessions[currentUser]['isBuySession'] or isBuyEvent
    if isBuyEvent:
        labels_dict[str(openSessions[currentUser]["sessionId"])] = isBuyEvent
    if not event["eventType"] == "transfer":
        updateEventWithItemsFeatures(event, pre_items, itemsDict, event['itemId'])

    # delete irrelevant dictionary entries
    event = {key:event[key] for key in event if key in eventEntries}

    # update last event dwell time
    openSessions[currentUser]['events'][- 1]['dwellTime'] = dwellTime
    openSessions[currentUser]['events'].append(event)


# updates the session features with the event data
def multiple_updateCurrentSessionWithEvent(event, pre_items,itemsDict, currentUser, openSessions, isBuyEvent, isBasketEvent, labels_dict):
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
        if openSessions[currentUser]['basketBuyDwell'] == 0:
            openSessions[currentUser]['basketBuyDwell'] = (event['timestamp'] - openSessions[currentUser]['lastBasketEvent']).total_seconds()
    if event["eventType"] == "transfer":
        return
    updateSessionWithItemsFeatures(currentUser, pre_items, openSessions, itemsDict, event['itemId'])

#######################################

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
    closedSessions[userId].append(copy.deepcopy(openSessions[userId]))


