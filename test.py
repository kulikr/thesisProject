
import os
import datetime as dt
import pandas as pd
import time
import pickle
import utils
import numpy as np
import socket
import redis
import Evaluation as evl
import utils as utl
import pickle
from PreProcess.PreProcess_items import itemsPreProcess
from PreProcess import PreProcess_ExtractEvents as pre_events

# sessions = []
# # for i in range(2):
# #     session = {}
# #     session["stam"] = 1
# #     session["harta"] = 0
# #     session["isBasketSession"] = np.nan
# #     sessions.append(session)
# #
# # df = pd.DataFrame(sessions)

# Reads the events from a given path starting from position
def read_sessions_from_file(path, numOfSessionsToRead, position=0):
    sessionsList = []
    with(open(path, mode='rb')) as f:
        f.seek(position)
        i = 0
        while i < numOfSessionsToRead:
            try:
                tmp=pickle.load(f)
                if isinstance(tmp, (list,)):
                    sessionsList.append(tmp[0])
                else:
                    sessionsList.append(tmp)
            except EOFError:
                print("file read to end")
                return sessionsList, -1
            i += 1
        position = f.tell()
    return sessionsList, position





