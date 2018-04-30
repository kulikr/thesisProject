import re as re
import datetime as dt
import time as time


monthDict = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10,
             "Nov": 11, "Dec": 12}



# Returns a list of dictionaries contains for each event the 'timestamp', 'eventType', 'userId', 'itemId'
def extractEvents(filePath):
    events = []
    eventsList = getEventsList(filePath)
    check = time.time()
    splittedEvents = [re.split(";", clickEvent) for clickEvent in eventsList]
    print("split time stamps from rest of the event with re.split:" + str(time.time() - check))
    check = time.time()
    timeStamps = extractTimeStamps(splittedEvents)
    print("time to extract all time stamps - splitting and creating time and date object" + str(time.time() - check))

    eventData = [re.split("/|\?", splitEvent[1]) for splitEvent in splittedEvents]

    # iterate the splitted lines of the file and extract the event features
    for i in range(len(eventData)):

        if len(eventData[i]) < 5:
            ben =0
            continue
        new_event=dict()

        eventType = eventData[i][3]

        new_event["eventType"] = eventType
        new_event["timestamp"] = timeStamps[i]

        if (eventType == 'transfer'):
            if eventData[i][4] == 'null' or eventData[i][5] == 'null':
                continue
            else:
                new_event["userId"] = eventData[i][4]
                new_event["newUserId"] = eventData[i][5]
        else:
            new_event['userId'] = eventData[i][4]
            new_event['itemId'] = eventData[i][6]

            # in case there is extra information related to the event
            if (len(eventData[i]) > 7):
                new_event['extraInfo'] = eventData[i][7]
        events.append(new_event)
    return events


# Read the events from the given path and returns list of events(seperated by the new line)
def getEventsList(filePath):
    read_data = []

    with open(filePath, 'r') as f:
        i=0
        for line in f:

            line = f.readline()  # no 's' at the end of `readline()`

            splittedLine = line.rstrip('\n')
            splittedLine = splittedLine.strip('[')
            read_data.append(splittedLine)

    print("read data length is: " + str(len(read_data)))
    return read_data


# Turns the time stamp into a dateTime object with the event time and return list of dateTime objects
def extractTimeStamps(splittedEvents):
    timeStamps = [re.split(':|\/', splitEvent[0]) for splitEvent in splittedEvents]
    timeStampList = []
    for i in range(len(timeStamps)):
        if timeStamps[i][1] not in monthDict:
            timeStampList.append(None)
            continue
        eventTime = dt.datetime(int(timeStamps[i][2]), monthDict[timeStamps[i][1]], int(timeStamps[i][0]),
                                int(timeStamps[i][3]), int(timeStamps[i][4]), int(timeStamps[i][5]))
        timeStampList.append(eventTime)
    return timeStampList