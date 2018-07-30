
import os
import datetime as dt
import pandas as pd
import time
import pickle
import utils
import numpy as np
import socket
import redis
import evaluation as evl

# r = redis.Redis(
#     host=socket.gethostname(),
#     port=6379)
# returns server instance (redis)
# path = "C://research_data//04-2016.out"
#
# with open(path, 'r') as f:
#         with open(path.rstrip(".out") + "_sample.out", 'w+') as new_f1:
#             i = 0
#             for line in f.readlines():
#
#                 if (i < 100000):
#                     new_f1.write(line)
#                 i += 1


def getServer(port, db_num):
    r = redis.StrictRedis(host='localhost', port=port, db=db_num)
    return r

def saveUserSessionToDb(userId ,userSessions, server):
    pickled_objects=[]
    pipe = server.pipeline()
    for session in userSessions:
        pickled_object = pickle.dumps(session)
        pipe.rpush(userId,pickled_object)

    pipe.execute()

def getUserSessionsFromDb(userId, server):
    elements = server.lrange(userId, 0, -1)
    sessions = []
    for element in elements:
        session = pickle.loads(element)
        sessions.append(session)
    return sessions

# filename="numOfCategoriesInBasketSessions.json"
# utils.dictToCsv(filename,"Graph_"+filename)

for server_num in [5,6,7,8,9]:
    r = getServer(6379,server_num)
    print(str(r.dbsize()))

#
# cursor = 0
# while 1:
#     cursor, sessionBatch = utils.readSessionsBatchFromServer(cursor)
#     regUsersSessions, tmpUsersSessions = evl.getValuesFromDb(evl.getStringKeys(sessionBatch), r)
#
#     for userSessions in regUsersSessions:
#         for session in userSessions:
#             if session['numOfEvents'] > 100:
#                 ben =4
#
#     for userSessions in regUsersSessions:
#         for session in userSessions:
#             if session['numOfEvents'] > 100:
#                 ben =4

#
# batch = utils.readSessionsBatchFromServer()
# # r.flushdb()
# # sessions = [{"userId" : "1", "a":1, "b":2},{"userId":"1","a":2,"b":1}]
# # saveUserSessionToDb(sessions[0]["userId"],sessions,r)
# # read_sessions = getUserSessionsFromDb("1",r)
# # print(read_sessions)
# ben=2




