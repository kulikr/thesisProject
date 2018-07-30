import re as re
import datetime as dt
import pickle
import time as time


monthDict = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10,
             "Nov": 11, "Dec": 12}



# Returns a list of dictionaries contains for each event the 'timestamp', 'eventType', 'userId', 'itemId'
def extractEvents(filePath):
    events = []
    eventsList = getEventsList(filePath)

    #split events
    splittedEvents = [re.split(";", clickEvent) for clickEvent in eventsList]
    print("events splitted")
    # clear memory
    eventsList.clear()
    #extract timestamps from string
    timeStamps = extractTimeStamps([splittedEvent[0] for splittedEvent in splittedEvents])
    print("timestamps splitted")
    #extract the rest of the data from the event
    eventData = [re.split("/|\?", splitEvent[1]) for splitEvent in splittedEvents]
    splittedEvents.clear()
    print("rest of event splitted")
    counter = 0
    # iterate the splitted lines of the file and extract the event features
    for i in range(len(eventData)):
        try:
            eventData_inst = eventData.pop()
            timestamp_inst = timeStamps.pop()

            #Avoid unused events
            if len(eventData_inst) < 5:
                continue
            eventType = eventData_inst[3]
            meaningfulEvents = ["transfer", "basket", "click", "clickrecommended", "buy"]

            if eventType not in meaningfulEvents:
                continue

            new_event = dict()
            new_event["eventType"] = eventType
            new_event["timestamp"] = timestamp_inst

            if eventType == 'transfer':
                if eventData_inst[4] == 'null' or eventData_inst[5] == 'null':
                    continue
                else:
                    new_event["userId"] = eventData_inst[4]
                    new_event["newUserId"] = eventData_inst[5]
            else:
                new_event['userId'] = eventData_inst[4]
                new_event['itemId'] = str(int(eventData_inst[6]))

                # in case there is extra information related to the event
                if (len(eventData_inst) > 7):
                    new_event['extraInfo'] = eventData_inst[7]
            counter+=1
            if counter%10000 == 0:
                print(str(counter))
            events.append(new_event)
        except:
            print("exception event num : "+str(i))

    #sort the events by their data
    return sorted(events, key=lambda k: k['timestamp'], reverse=True)


# Read the events from the given path and returns list of events(seperated by the new line)
def getEventsList(filePath):
    read_data = []

    with open(filePath, 'r') as f:
        i=0
        for line in f:
            splittedLine = line.rstrip('\n')
            splittedLine = splittedLine.strip('[')
            read_data.append(splittedLine)

    print("read data length is: " + str(len(read_data)))
    return read_data


# Turns the time stamp into a dateTime object with the event time and return list of dateTime objects
def extractTimeStamps(stringTimestamps):
    timestampList=[]
    for i in range(len(stringTimestamps)):
        try:
            timestamp=_date_parser(stringTimestamps[i])
        except ValueError:
            timestamp=dt.datetime(3000,1,1,1,1,1)
        timestampList.append(timestamp)
    return timestampList


# date parser for reading the data as datetime objects
def _date_parser(string):
    return dt.datetime.strptime(string, "%d/%b/%Y:%H:%M:%S")


# Reads the events from a given path starting from position
def readEventsFromFile(path, numOfEventsToRead, position):
    eventList = []
    with(open(path, mode='rb')) as f:
        f.seek(position)
        i=0
        while i < numOfEventsToRead:
            try:
                eventList.append(pickle.load(f))
            except EOFError:
                print("file read to end")
                return None,-1

            i+=1
        position = f.tell()
    return eventList, position


# reads data sample of events
def readEventsSampleFromFile(path, numOfEventsToRead):
    eventList = []
    with(open(path, mode='rb')) as f:
        i=0                                                     
        while i < numOfEventsToRead:
            try:
                eventList.append(pickle.load(f))
            except EOFError:
                print("file read to end")
                return None,-1

            i+=1
        position = f.tell()
    return eventList
