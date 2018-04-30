import os
import pandas as pd
import datetime as dt
import pickle
import csv


class itemsPreProcess:

    PATH_ITEMS = "C:\\Users\\royku\\Desktop\\Research\\files"

    def __init__(self,date):
        self.itemsDict = {}
        self.dict_category1 = {}
        self.dict_category2 = {}
        self.readItemsDictFromCsv(date)
        self.readCategory1DictFromFile()
        self.readCategory2DictFromFile()
        self.path = "C:\\Users\\royku\\Desktop\\Research\\files"

    def readItemsDictFromCsv(self, date):

        itemsDict = None
        while(dict == None and date < dt.date(2017,4,21)):
            date=date+dt.timedelta(days=1)
            year = date.year
            month = date.month
            day = date.day


            if (month < 10):
                monthString = "0" + str(month)
            else:
                monthString = str(month)
            if (day < 10):
                dayString = "0" + str(day)
            else:
                dayString = str(day)

            dateString = str(year) + "-" + monthString + "-" + dayString
            if os.path.isfile(self.path + "//" + dateString + ".csv"):
                df = pd.read_csv(self.path + "//" + dateString + ".csv")
                for idx, row in df.iterrows():
                    print(row)
                    itemsDict[str(row["itemId"])] = df.iloc[idx, 2:]
        if(itemsDict != None):
            self.itemsDict = itemsDict

    def getItemsDict(self, date):
            return self.itemsDict


    def writeCategory1DictToFile(self):
        with open(self.PATH_ITEMS+'items_dict.txt','w') as file:
            self.dict_category1 = pickle.loads(file.dump(self.dict_category1))

    def writeCategory2DictToFile(self):
        with open(self.PATH_ITEMS+'items_dict.txt','w') as file:
            self.dict_category1 = pickle.loads(file.dump(self.dict_category2))

    def readCategory1DictFromFile(self):
        if os.path.isfile(self.PATH_ITEMS+'category1_dict.txt'):
            #read dict of categorie
            with open(self.PATH_ITEMS+'category1_dict.txt','r') as file:
                self.dict_category1 = pickle.loads(file.read())

    def readCategory2DictFromFile(self):

        if os.path.isfile(self.PATH_ITEMS + 'category2_dict.txt.txt'):
            # read dict of categorie
            with open(self.PATH_ITEMS + 'category1_dict.txt', 'r') as file:
                self.dict_category1 = pickle.loads(file.read())

    def getCategory1NumericalValue(self ,key):
        dict = self.dict_category1
        if key not in dict:
            dict[key] = len(dict)

        self.dict_category1=dict

        return dict

    def getCategory2NumericalValue(self, key):
        dict = self.dict_category2
        if key not in dict:
            dict[key] = len(dict)

        self.dict_category2 = dict

        return dict
