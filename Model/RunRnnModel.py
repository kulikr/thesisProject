import pickle
import numpy
import matplotlib.pyplot as plt
import pandas as pd
import copy
import math

from sklearn import preprocessing

from PreProcess import Model_PreProcess as pre_model
from keras.models import Sequential
from keras.layers import Dense, Flatten
from keras.layers import LSTM, GRU, Input
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error

path="../sessions_8.pickle"
num_of_instances = 10000
lengths_arr = [5]
num_of_epochs = 10

batch_size = 32
input_dim = 54
timesteps = 5
num_classes = 2
id=0
labels_dict={}

diff = []

# Expected input batch shape: (batch_size, timesteps, data_dim)
# Note that we have to provide the full batch_input_shape since the network is stateful.
# the sample of index i in batch k is the follow-up for the sample i in batch k-1.


def get_instances_by_length(lengths_arr, sessions_batch):
    global id
    instances = []
    if sessions_batch== None or len(sessions_batch) == 0:
        return []
    for session in sessions_batch:
        session_length = len(session['events'])
        for length in [length for length in lengths_arr if length <= session_length]:
            for index in range(session_length-length):
                for event_index in range(index,index+length):
                    new_event=copy.deepcopy(session['events'][event_index])
                    new_event["sessionId"]=id
                    instances.append(new_event)
                labels_dict[str(id)] = session["isBuySession"]
                id+=1
    return instances


# Reads the events from a given path starting from position
def read_sessions_from_file(path, numOfSessionsToRead, position=0):
    sessionsList = []
    with(open(path, mode='rb')) as f:
        f.seek(position)
        i = 0
        while i < numOfSessionsToRead:
            try:
                sessionsList.append(pickle.load(f)[0])
            except EOFError:
                print("file read to end")
                return None, -1

            i += 1
        position = f.tell()
    return sessionsList, position


def train_on_instances_batch(instances, model):
    ben=4



def preprocess_instances(train_x, train_y):


    main_categories = []
    with open("C:\\Users\\kulikr\\Desktop\\BasketSessions\\TmpFiles\\main_categories.txt", encoding="utf=8") as f:
        for line in f:
            main_categories.append(line.strip())

    # One-Hot-Encoding
    train_x = pre_model.process_categorical_train_only(train_x, "eventType", ["click","clickrecommended", "buy", "basket", "transfer"])
    train_x = pre_model.process_categorical_train_only(train_x, "category2",main_categories)

    # for category in main_categories:
    #     if category not in train_x.columns:
    #         train_x[category]=0

    # Normalization
    tmp_id = train_x["sessionId"].copy(deep=True)
    min_max_scaler = preprocessing.MinMaxScaler()
    np_scaled = min_max_scaler.fit_transform(train_x)
    train_x_normalized = pd.DataFrame(np_scaled,columns=train_x.columns)
    train_x_normalized["sessionId"]= tmp_id
    return train_x_normalized


def create_instances_by_len(train_x, len_timesteps = 5, input_dim = 401):
    train_x_new = np.zeros(shape=(0, len_timesteps, len(train_x.columns)))
    train_y_new = []
    for i in range(len(labels_dict)):
        instance = train_x[train_x["sessionId"]==i]
        if instance.size != 0:
            instance = instance.values
            instance = instance.reshape(1,instance.shape[0], instance.shape[1])
            label = labels_dict[str(i)]
            train_x_new = np.append(train_x_new,instance, axis=0)
            if label:
                train_y_new.append(1)
            else:
                train_y_new.append(0)

    return train_x_new,train_y_new

model = Sequential()
model.add(LSTM(5, input_shape=(timesteps, input_dim), return_sequences=True))
model.add(Flatten())
model.add(Dense(20, activation='softmax', kernel_initializer='normal'))
model.add(Dense(1, activation='sigmoid', kernel_initializer='normal'))
model.compile(loss='categorical_crossentropy',
              optimizer='rmsprop',
              metrics=['accuracy'])


def create_batch_file(train_x, train_y, num_of_files):
    np.save("rnn_dataset/train_"+str(num_of_files),train_x)
    with open("rnn_dataset/labels_"+str(num_of_files)+".txt",mode="w+") as f:
        for label in train_y:
            f.write(str(label))

def create_dataset_by_timesteps(timesteps):
    #Read sessions from file
    train_x = []
    batch_x = []
    num_of_files=0

    # Read sessions batch from file
    sessions_batch, position = read_sessions_from_file(path, 100)
    # Get only basket sessions
    sessions_batch = [session for session in sessions_batch if session['isBasketSession']]
    while 1:
        # cut the session to fixed size instances
        batch_x = get_instances_by_length(lengths_arr, sessions_batch)
        # append the new instances data and labels to train_x , train_y respectively
        train_x += batch_x

        # If there are enough instances in the batch - train on batch
        if len(train_x) >= num_of_instances:
            train_x = pd.DataFrame(train_x)
            del train_x["timestamp"]
            del train_x["category1"]
            train_x = preprocess_instances(train_x, labels_dict)
            print(train_x.columns)
            train_x, train_y = create_instances_by_len(train_x,len_timesteps=timesteps)
            create_batch_file(train_x, train_y, num_of_files)
            num_of_files+=1
            train_x = []

        if position == -1:
            break
        sessions_batch, position = read_sessions_from_file(path, 100, position)
        sessions_batch = [session for session in sessions_batch if session['isBasketSession']]


create_dataset_by_timesteps(timesteps)
