from PreProcess import PreProcess_ExtractEvents as pre_events, PreProcess_ExtractSessions as pre_sessions, PreProcess_items as pre_items
import matplotlib.pyplot as plt
# import seaborn as sns
import numpy as np
import evaluation as evl
import os
import utils as utl
from sklearn import preprocessing as pre


def ensure_dir(file_path):
    directory_in_str =os.getcwd()+file_path
    print(directory_in_str)
    if not os.path.isdir(directory_in_str):
        os.makedirs(directory_in_str)


def plot_results(title, x_label, y_label, x, y):
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)

    plt.bar(x, y)
    plt.legend(loc='upper right')
    plt.savefig("./" + title + ".png")
    plt.close()


def plotDictDistribution(dict,title, x_label,y_label):
    x = []
    y = []

    for key in sorted(dict, key=number_map.__getitem__):
        x.append(int(key))
        y.append(dict[key])

    plot_results(title, x_label, y_label, x, y)



directory_in_str="C:\\research_data\\current_run"

directory = os.fsencode(directory_in_str)

ensure_dir("./Charts")
ensure_dir("./TmpFiles")

#Extract events and sessions
# eventList = pre_events.extractEvents("C:\\data_sample2.txt")
# accumulated_data = tmp_extractSessions.extractSessions(eventList, 6000)
# for file in os.listdir(directory):
#     filename = os.fsdecode(file)
#     utl.splitMonthData(directory_in_str+"\\"+filename)

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

for month in ['08']:
    accumulated_data = pre_sessions.extractSessions("events_"+ month +".csv", 3600)
    utl.writeDictToFile(accumulated_data, "accumulated_"+month)
    server = utl.getServer(6379, int(month))
    server.save()






















