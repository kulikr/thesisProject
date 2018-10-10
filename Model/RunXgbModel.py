import PreProcess.Model_PreProcess as pre
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, recall_score, precision_score, roc_auc_score, auc
import Model.OptimizeXgbParameters as opt
import xgboost as xgb
import utils as utl
from datetime import datetime as time
import math
import os


def set_and_update_run_num():
    f = open("../run_num.txt", mode='r')
    run_num = int(f.read())
    f.close()
    f = open("../run_num.txt", mode='w')
    run_num += 1
    f.write(str(run_num))
    f.close()
    return run_num


# Returns accuracy, precision ,recall and accuracy
def evaluate_predictions(y_test, predictions):
    acc = accuracy_score(y_test, predictions)
    recall = recall_score(y_test, predictions, average=None)[1]
    precision = precision_score(y_test, predictions, average=None)[1]
    auc_score = roc_auc_score(y_test, predictions)
    return acc, recall, precision, auc_score


# Rrints the results
def printResults(acc, recall, precision, auc_score):
    print("Model Accuracy: " + str(acc) + "\n")
    print("Model Precision: " + str(precision) + "\n")
    print("Model Recall: " + str(recall) + "\n")
    print("Model AUC: " + str(auc_score) + "\n")


# Write the results of the evaluation to file
def write_eval_results(path, DATA_SIZE, acc, precision, recall, auc_score):
    with open("./results/" + path + "/results.txt", mode='w') as f:
        f.write("*** Results ***")
        f.write("Number of instances :" + str(DATA_SIZE) + "\n")
        f.write("Number of training instances :" + str(round(0.85 * DATA_SIZE)) + "\n")
        f.write("Number of test instances :" + str(round(0.15 * DATA_SIZE)) + "\n")
        f.write("*** Evaluation ***")
        f.write("Model Accuracy: " + str(acc) + "\n")
        f.write("Model Precision: " + str(precision) + "\n")
        f.write("Model Recall: " + str(recall) + "\n")
        f.write("Model Auc :" + str(auc_score) + "\n")
        f.write("\n")


# Write the model params to file
def write_model_params(path, model, THRESH_DEVIATION, THRESH_IMPORTANCE, THRESH_SIMILARITY):
    with open("./results/" + path + "/params.txt", mode='w') as f:
        f.write("*** Model Params ***")
        f.write("Gradient Boosting Params: " + str(model.get_params()))
        f.write("\n")
        f.write("*** Feature Selection ***")
        f.write("threshold_deviation: " + str(THRESH_DEVIATION) + "\n")
        f.write("threshold_importance: " + str(THRESH_IMPORTANCE) + "\n")
        f.write("threshold_similarity: " + str(THRESH_SIMILARITY) + "\n")
        f.write("\n")


# Write the model runtimes to file
def write_model_runtimes(path, DATA_SIZE, feature_selection_time, opt_n_estimators_time, grid_time, training_time,
                         prediction_time):
    with open("./results/" + path + "/run_times.txt", mode='w') as f:
        f.write("*** Run Time ***")
        f.write("Number of instances :" + str(DATA_SIZE) + "\n")
        f.write("Feature Selection time is: " + str(feature_selection_time) + "\n")
        f.write("Optimizing n_estimators time is: " + str(opt_n_estimators_time) + "\n")
        f.write("Grid Search time is: " + str(grid_time) + "\n")
        f.write("Training time is: " + str(training_time) + "\n")
        f.write("Prediction time is: " + str(prediction_time) + "\n")

    # PreProcessing for the data columns


# Run the model
def run_model(data, run_type, use_basket_features=True, is_grid_search=True, is_opt_n_estimators=True):
    # threshold for the features selection
    THRESH_DEVIATION = 0.05
    THRESH_IMPORTANCE = 0.01
    THRESH_SIMILARITY = 0.75
    # save the full data size
    DATA_SIZE = len(data.index)
    CATEGORICAL_COLUMNS = ['weekday', 'hour']
    opt_n_estimators_time = 0
    grid_time = 0

    # Reads the run num from file
    run_num = set_and_update_run_num()
    if use_basket_features:
        path = str(run_num) + "_" + run_type + "_basket"
    else:
        path = str(run_num) + "_" + run_type + "_noBasket"

    # Build directory for the run results and meta-data
    utl.ensure_dir("./results/" + path + "")

    # ### TO DELETE
    # data = data[[column for column in data.columns if column.startswith("b_") or column == "isBuySession"]]

    # use basket features only if requested
    if not use_basket_features:
        data = data[[column for column in data.columns if not column.startswith("b_")]]

    # preprocess the target variable to 1,0
    pre.boolToNumeric(data, "isBuySession")

    # features and labels as X and y
    y = data.isBuySession
    X = data

    # split to train and test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    START_TIME = time.now()
    for column_name, column_type in X_train.dtypes.iteritems():
        # elif column_name in ordinal_arrays.keys():
        #     train, test = pre.process_ordinal(train, test, column_name, ordinal_arrays[column_name])

        if column_type == bool:
            X_train.dropna(subset=[column_name], inplace=True)
            X_test.dropna(subset=[column_name], inplace=True)
            X_train = pre.boolToNumeric(X_train, column_name)
            X_test = pre.boolToNumeric(X_test, column_name)

        elif column_type == object or column_name in CATEGORICAL_COLUMNS:
            X_train, X_test = pre.process_categorical(X_train, X_test, column_name, CATEGORICAL_COLUMNS)
            print("After categorical processing of " + column_name + " The shape is " + str(X_train.shape))
        else:
            X_train, X_test = pre.process_numeric(X_train, X_test, column_name)
            print("After numeric processing of " + column_name + " The shape is " + str(X_train.shape))

    # contains all the columns names
    predictors = list(X_train.columns)
    predictors.remove("isBuySession")
    predictors = pre.features_selection(X_train, predictors, THRESH_DEVIATION, THRESH_IMPORTANCE, THRESH_SIMILARITY)
    # predictors = []
    # with open(".\selected_features.txt") as f:
    #     for line in f:
    #         predictors.append(line.strip())
    feature_selection_time = ((time.now() - START_TIME).total_seconds()) / 60
    print("Feature Selection Ended - Time :" + str(feature_selection_time) + " Minutes")

    # Define dart params in case dart used as booster
    dart_params = {'rate_drop': 0.1, 'skip_drop': 0.5}
    # Define model hyper params
    model = xgb.XGBClassifier(max_depth=5, learning_rate=0.08, n_estimators=800, silent=True,
                              objective='reg:linear', booster='dart', gamma=0.05, min_child_weight=1,
                              max_delta_step=0, subsample=0.5, colsample_bytree=0.8, colsample_bylevel=1,
                              scale_pos_weight=1, base_score=0.5, random_state=0, missing=None, kwargs=dart_params)

    if is_opt_n_estimators:
        START_TIME = time.now()
        opt.optimize_n_estimators(model, X_train, predictors, y_train, path, rounds=500)  # Was 1000 rounds
        opt_n_estimators_time = ((time.now() - START_TIME).total_seconds()) / 60

        print("n_estimators Search Ended - Time :" + str(opt_n_estimators_time) + " Minutes")

    param_test = {
        'max_depth': range(5, 7, 1),
        'min_child_weight': range(1, 3, 1)
    }

    if is_grid_search == True:
        # Grid search the parameters
        START_TIME = time.now()
        opt.optimize_params(model, X_train, predictors, param_test)
        grid_time = ((time.now() - START_TIME).total_seconds()) / 60
        print("Grid Search Ended - Time :" + str(grid_time) + " Minutes")

    # Delete the target class from features dataframe
    del X_train["isBuySession"]
    del X_test["isBuySession"]

    # Train model
    START_TIME = time.now()
    model.fit(X_train[predictors], y_train)
    training_time = ((time.now() - START_TIME).total_seconds()) / 60

    # Predict
    START_TIME = time.now()
    y_pred = model.predict(X_test[predictors])
    prediction_time = ((time.now() - START_TIME).total_seconds()) / 60

    # Evaluate model results
    acc, recall, precision, auc_score = evaluate_predictions(y_test, y_pred)

    # prints results to console
    printResults(acc, recall, precision, auc_score)

    # Write results and params to file
    write_eval_results(path, DATA_SIZE, acc, precision, recall, auc_score)
    write_model_params(path, model, THRESH_DEVIATION, THRESH_IMPORTANCE, THRESH_SIMILARITY)
    write_model_runtimes(path, DATA_SIZE, feature_selection_time, opt_n_estimators_time, grid_time, training_time,
                         prediction_time)


months = ["8"]

data_tmp = pd.DataFrame()
data_reg = pd.DataFrame()
data_all = pd.DataFrame()

### MERGE DIFFERENT MONTHS DATA
# for month in months:
#     if len(data_tmp.index) == 0:
#         data_tmp = utl.getBasketDataFrame(month, user_type="tmp")
#         print(len(data_tmp))
#         data_reg = utl.getBasketDataFrame(month, user_type="reg")
#     else:
#         data_tmp = data_tmp.append(utl.getBasketDataFrame(month, user_type="tmp"))
#         data_reg = data_reg.append(utl.getBasketDataFrame(month, user_type="reg"))


# data_tmp["isLoggedUser"] = 0
# data_reg["isLoggedUser"] = 1
# data_all = data_tmp.append(data_reg)
#
# problem_balance_reg = len(data_reg[data_reg["isBuySession"]].index)/len(data_reg.index)
# problem_balance_tmp = len(data_tmp[data_tmp["isBuySession"]].index)/len(data_tmp.index)
# problem_balance_all = len(data_all[data_all["isBuySession"]].index)/len(data_all.index)


# data_reg = utl.getBasketDataFrame("8", user_type="reg")
# data_reg = data_reg.iloc[:100000,:]
# problem_balance_reg = len(data_reg[data_reg["isBuySession"]].index)/len(data_reg.index)
# run_model(data_reg,run_type ="registered",use_basket_features=False)
# run_model(data_reg,run_type ="registered",use_basket_features=True)
# data_reg =data_reg.iloc[0:0]
#
# ### RUN TEMPORARY USERS
# data_tmp = utl.getBasketDataFrame("8", user_type="tmp")
# data_tmp = data_tmp.iloc[:20000,:]
# problem_balance_tmp = len(data_tmp[data_tmp["isBuySession"]].index)/len(data_tmp.index)
# run_model(data_tmp, run_type = "tmp",use_basket_features=False)
# run_model(data_tmp, run_type = "tmp",use_basket_features=True)
# data_tmp = data_tmp.iloc[0:0]


### RUN ALL USERS

data_reg = utl.getBasketDataFrame("8", user_type="reg")
data_reg = data_reg.iloc[:135000, :]
data_tmp = utl.getBasketDataFrame("8", user_type="tmp")
data_tmp = data_tmp.iloc[:500000, :]
data_tmp["isLoggedUser"] = 0
data_reg["isLoggedUser"] = 1
data_all = data_tmp.append(data_reg)
problem_balance_all = len(data_all[data_all["isBuySession"]].index) / len(data_all.index)

run_model(data_all, run_type="all", use_basket_features=True, is_opt_n_estimators=False, is_grid_search=False)
# run_model(data_all, run_type = "all",use_basket_features=False)
with open("./results/problemBalance.csv", mode="w+") as f:
    #     f.write("registered problem balance: "+str(problem_balance_reg)+"\n")
    #     f.write("temporary problem balance: "+str(problem_balance_reg)+"\n")
    f.write("all users problem balance: " + str(problem_balance_all) + "\n")
