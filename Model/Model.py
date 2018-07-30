import PreProcess.Model_PreProcess as pre
# import xgboost as xgb

import pandas as pd
import Model.OptimizeXgbParameters as opt
reg_csv = pd.read_csv("./regCsv_8.csv")
tmp_csv = pd.read_csv("./tmpCsv_8.csv")


reg_train, reg_test = "" ,"" #split reg_csv to train test
test = pd.read_csv("./test.csv") # split tmp csv to train test

# Read ordinal features meta data
for column_name, column_type in train.dtypes.iteritems():
    if column_name == "isBuySession":
        train["isBuySession"][train["isBuySession"]] = 1
        train["isBuySession"][not train["isBuySession"]] = 0
        train, test = pre.process_categorical(train, test, column_name)
    # elif column_name in ordinal_arrays.keys():
    #     train, test = pre.process_ordinal(train, test, column_name, ordinal_arrays[column_name])
    elif column_type != object:
        train, test = pre.process_numeric(train, test, column_name)
    else:
        train, test = pre.process_categorical(train, test, column_name)

# contains all the columns names ( without 'SalePrice' and without 'Id' )
predictors = list(test)
# Remove index
predictors.remove("Id")

_tresh_deviation=0.05
_tresh_importance=0.045
_tresh_similarity=0.75

predictors=pre.features_selection(train,predictors,_tresh_deviation,_tresh_importance,_tresh_similarity)


train_matrix = train.as_matrix(predictors)
test_matrix = test.as_matrix(predictors)
labels = train["SalePrice"].values

dart_params = {'rate_drop': 0.1, 'skip_drop': 0.5}

model = XGBRegressor(max_depth=8, learning_rate=0.05, n_estimators=10000, silent=True,
                     objective='reg:linear', booster='dart', gamma=0.0468, min_child_weight=4,
                     max_delta_step=0, subsample=0.5213, colsample_bytree=0.8, colsample_bylevel=1,
                     scale_pos_weight=1, base_score=0.5, random_state=0, missing=None, kwargs=dart_params)

opt.optimize_n_estimators(model,train,predictors,labels)

param_test = {
 'max_depth':range(3,10,2),
 'min_child_weight':range(1,6,2)
}
opt.optimize_params(model,train,predictors,param_test)

model.fit(train[predictors], labels)

res= model.predict(test[predictors])

print(res)


f=open("./run_num.txt",mode='r')
run_num=int(f.read())
f.close()
f=open("./run_num.txt",mode='w')
run_num+=1
f.write(str(run_num))
f.close()

ids = range(1461, (1461 + len(res)))
result_df = pd.DataFrame({"Id": ids, "SalePrice": res})
result_df.to_csv("./submission_"+str(run_num)+".csv",index=False)

f=open("./params_"+str(run_num)+".txt",mode='w')
f.write(str(model.get_params()))
f.write("\n")
f.write("tresh_deviation: "+str(_tresh_deviation)+"\n")
f.write("tresh_importance: "+str(_tresh_importance)+"\n")
f.write("tresh_similarity: "+str(_tresh_similarity)+"\n")
f.close()