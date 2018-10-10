from PreProcess import PreProcess_ExtractEvents as pre_events, PreProcess_ExtractSessions as pre_sessions, PreProcess_items as pre_items, PreProcess_ExtractSessions_Eventwise as pre_eventwise
import matplotlib.pyplot as plt
# import seaborn as sns
import numpy as np
import Evaluation as evl
import os
import utils as utl
from sklearn import preprocessing as pre


directory_in_str="C:\\research_data\\current_run"

directory = os.fsencode(directory_in_str)

utl.ensure_dir("./Charts")
utl.ensure_dir("./TmpFiles")


# import pickle
# for file in os.listdir(directory):
#     filename = os.fsdecode(file)
#     if filename.endswith(".out"):
#         eventList = pre_events.extractEvents(directory_in_str+"//"+filename)
#     month = utl.getMonthFromPath(filename)
#
#     with open("events_"+month+".csv","wb") as f:
#         for i in range(len(eventList)):
#             event = eventList.pop()
#             pickle.dump(event,f)


# for month in ['08']:
#      pre_eventwise.extractSessions("EventsPickled\\events_"+ month +".csv", 3600)


for month in ['08']:
     pre_sessions.extractMultipleSessions("EventsPickled\\events_"+ month +".csv", 3600)





















