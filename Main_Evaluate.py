from PreProcess import PreProcess_ExtractEvents as pre_events, PreProcess_ExtractSessions as pre_sessions, PreProcess_items as pre_items
import matplotlib.pyplot as plt
# import seaborn as sns
import numpy as np
import Evaluation as evl
import os
import utils as utl
from sklearn import preprocessing as pre



month = 8
#
# path = "./evaluation_"+str(month)+"/"
# extractedMonths = [6,7,8,9]
#
# # for month in extractedMonths:
# #     server = utl.getServer(6379,month)
# #     numOfCategoriesDict, categoriesDistDict = evl.evaluateWithDb(server,str(month))
# #     utl.writeDictToFile(numOfCategoriesDict, path+"_"+str(month))


server = utl.getServer(6379, month)
utl.buildCsvFromDb(server, month)
