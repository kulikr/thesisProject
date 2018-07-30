import xgboost as xgb
import pandas as pd
import matplotlib.pyplot as plt
import sklearn.model_selection as mod_s
from sklearn import cross_validation, metrics
plt.interactive(True)



def optimize_n_estimators(alg, dtrain, predictors, labels, useTrainCV=True, cv_folds=6, early_stopping_rounds=50):
    if useTrainCV:
        xgb_param = alg.get_xgb_params()
        xgtrain = xgb.DMatrix(dtrain[predictors].values, label=labels)
        cvresult = xgb.cv(xgb_param, xgtrain, num_boost_round=alg.get_params()['n_estimators'], nfold=cv_folds,
                          metrics='rmse', early_stopping_rounds=early_stopping_rounds)
        alg.set_params(n_estimators=cvresult.shape[0])

    # Fit the algorithm on the data
    alg.fit(dtrain[predictors], labels, eval_metric='rmse')

    # feature importance
    print(str(alg.get_booster().get_score(importance_type='weight')))
    xgb.plot_importance(alg)
    plt.show()


def optimize_params(alg, train, predictors, param_test, target='SalePrice'):
    gsearch =mod_s.GridSearchCV(estimator=alg, param_grid=param_test, scoring='neg_mean_absolute_error', n_jobs=1, iid=False, cv=6)
    gsearch.fit(train[predictors], train[target])
    alg.set_params(**gsearch.best_params_)