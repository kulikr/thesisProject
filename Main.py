from PreProcess import PreProcess_ExtractEvents as pre_events, PreProcess_ExtractSessions as pre_sessions, PreProcess_items as pre_items
import tmp_extractSessions
import matplotlib.pyplot as plt
import seaborn as sns
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


def save_dist_plot(title, feature, filepath):
    plt.title(title)
    sns.distplot(feature)
    plt.legend(loc='upper right')
    plt.savefig("./" + title + ".png")
    plt.close()


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


def plotBuyRate(dict1,dict2,title, x_label,y_label):
    x=[]
    y=[]
    tmp_y=[]

    for key in sorted(dict1, key=number_map.__getitem__):
        x.append(int(key))
        value= dict1[key]/dict2[key]
        y.append(value)

    y=pre.normalize(np.reshape(y,(1,-1)))
    ben=2

    plot_results(title, x_label,y_label,x,y)


directory_in_str="C:\\research_data"

directory = os.fsencode(directory_in_str)

ensure_dir("./Charts")
ensure_dir("./TmpFiles")

# #Extract events and sessions
# eventList = pre_events.extractEvents("C:\\data_sample2.txt")
# accumulated_data = tmp_extractSessions.extractSessions(eventList, 6000)
# for file in os.listdir(directory):
#     filename = os.fsdecode(file)
#     utl.splitMonthData(directory_in_str+"\\"+filename)



for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith(".out"):
        eventList = pre_events.extractEvents(directory_in_str+"\\"+filename)
        accumulated_data = tmp_extractSessions.extractSessions(eventList, 5000)
        evl.plotEventDistribution("./TmpFiles/sessions_events_dist_"+utl.getCurrentMonth())
        evl.plotUsersAverageSessionLength("./TmpFiles/events_per_user")

        # evl.plotUsersHistograms("./TmpFiles/events_per_user")

# evl.plotEventDistribution("./TmpFiles/sessions_events_dist_"+utl.getCurrentMonth())
# evl.plotUsersAverageSessionLength("./TmpFiles/events_per_user")


#Evaluate data


#evl.evaluateData("./sessions_04.csv")

#utl.writeMonthDataToFile(sessions,accumulated_data, "04")
#plotDictDistribution(sessionsLength,"session length distribution", "session length", "number of session")
#plotBuyRate(buyPerSessionLength,sessionsLength,"buy rate","session length","buy rate")

#print("num of buys : " + str(accumulated_data['numOfBuys']))
#print("num of sessions : " + str(len(sessions)))

# print(buyPerSessionLength)




