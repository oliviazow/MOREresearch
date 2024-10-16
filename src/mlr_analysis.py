import pandas as pd
from pprint import pprint
import os
import datetime
from math import log
from main import layoffDataFullSimpl
from main import colnamesFull
from layoff_data_simplifier import simplifierMap
import numpy as np
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, root_mean_squared_error, r2_score
from sklearn import preprocessing
from sklearn.preprocessing import OrdinalEncoder
from statistics import stdev

# data preprocessing
ind_var_setup = ["AI Mentioned", "Reason Mentioned 1", "Reason Mentioned 2", "Reason Mentioned 3", "Date of Layoff",
                 "Percentage", "Number Laid Off"]
dep_var = ["Stock Return On Closest Trading Date Post-Announcement (t)", "Stock Return 3 Days After t"]
other_vars = list(set(colnamesFull) - set(ind_var_setup + dep_var))
layoffDataFullSimpl.drop(other_vars, axis=1, inplace=True)
# print(layoffDataFullSimpl.columns)

layoffDataFullSimpl["Log(Number Laid Off)"] = [log(x) for x in layoffDataFullSimpl["Number Laid Off"] if
                                               type(x) is float]
layoffDataFullSimpl["Year"] = pd.DatetimeIndex(layoffDataFullSimpl["Date of Layoff"]).year
colnamesFull.insert(7, "Log(Number Laid Off)")
colnamesFull.append("Year")
ord_enc = OrdinalEncoder()
layoffDataFullSimpl["Year_code"] = ord_enc.fit_transform(layoffDataFullSimpl[["Year"]])
missing_reasons = ["business realignment", "definite business realignment", "automation", "optimization", "focus on ai",
                   "ftx collapse", "covid-19"]
simplified_reasons = list(set(simplifierMap.values())) + missing_reasons
for reason in simplified_reasons:
    layoffDataFullSimpl[str(reason)] = np.where((layoffDataFullSimpl["Reason Mentioned 1"] == reason) |
                                                (layoffDataFullSimpl["Reason Mentioned 2"] == reason) |
                                                (layoffDataFullSimpl["Reason Mentioned 3"] == reason),
                                                1, 0)

layoffDataFullSimpl.drop(["Reason Mentioned 1", "Reason Mentioned 2", "Reason Mentioned 3"], axis=1, inplace=True)
layoffDataFullSimpl["Post"] = np.where(layoffDataFullSimpl["Year"] >= 2023, 1, 0)
layoffDataFullSimpl["Post*AI"] = layoffDataFullSimpl["Post"] * layoffDataFullSimpl["AI Mentioned"]
layoffDataFullSimpl["Post*Percentage"] = layoffDataFullSimpl["Post"] * layoffDataFullSimpl["Percentage"]

publicDf = layoffDataFullSimpl.dropna()

# check if there are any NA values left
print(publicDf[publicDf.isna().any(axis=1)])
# ------

# first model
ind_var = ["AI Mentioned", "Year_code", "Percentage", "Log(Number Laid Off)"] + simplified_reasons
xDf = publicDf[ind_var]
dep_var.remove("Stock Return 3 Days After t")
yDf = publicDf[dep_var]

x_train, x_test, y_train, y_test = train_test_split(xDf, yDf, test_size=.3, random_state=101)
model = LinearRegression()
model.fit(x_train, y_train)
predictions = model.predict(x_test)

importance = list(model.coef_)[0]
metrics = ["Mean squared error: " + str(mean_squared_error(y_test, predictions)),
           "Root mean squared error: " + str(root_mean_squared_error(y_test, predictions)),
           "Mean absolute error: " + str(mean_absolute_error(y_test, predictions)),
           "R2 Score: " + str(r2_score(y_test, predictions))]
f = open(r"%s\docs\mlr_analysis_results.txt" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)), "w")
f.write("1st Model:\n")
for i, v in enumerate(importance):
    f.write(str(ind_var[i]) + ": " + str(round(v, 4)))
    f.write("\n")
f.writelines("\n".join(metrics))

# second model
ind_var2 = ["AI Mentioned", "Post", "Post*AI", "Post*Percentage", "Percentage",
            "Log(Number Laid Off)"] + simplified_reasons
xDf = publicDf[ind_var2]
yDf = publicDf[dep_var]

x_train, x_test, y_train, y_test = train_test_split(xDf, yDf, test_size=.3, random_state=101)
model2 = LinearRegression()
model2.fit(x_train, y_train)
predictions = model2.predict(x_test)

importance = list(model2.coef_)[0]
metrics = ["Mean squared error: " + str(mean_squared_error(y_test, predictions)),
           "Root mean squared error: " + str(root_mean_squared_error(y_test, predictions)),
           "Mean absolute error: " + str(mean_absolute_error(y_test, predictions)),
           "R2 Score: " + str(r2_score(y_test, predictions))]
f.write("\n\n2nd Model:\n")
for i, v in enumerate(importance):
    f.write(str(ind_var2[i]) + ": " + str(round(v, 4)))
    f.write("\n")
f.writelines("\n".join(metrics))
f.write("\n\nStandard deviation: " + str(stdev(publicDf[dep_var[0]].tolist())))

# third model: Predicting stock return for day t + 3
dep_var.append("Stock Return 3 Days After t")
dep_var.remove("Stock Return On Closest Trading Date Post-Announcement (t)")
xDf = publicDf[ind_var]
yDf = publicDf[dep_var]

x_train, x_test, y_train, y_test = train_test_split(xDf, yDf, test_size=.3, random_state=101)
model3 = LinearRegression()
model3.fit(x_train, y_train)
predictions = model3.predict(x_test)

importance = list(model3.coef_)[0]
metrics = ["Mean squared error: " + str(mean_squared_error(y_test, predictions)),
           "Root mean squared error: " + str(root_mean_squared_error(y_test, predictions)),
           "Mean absolute error: " + str(mean_absolute_error(y_test, predictions)),
           "R2 Score: " + str(r2_score(y_test, predictions))]
f.write("\n\n3rd Model:\n")
for i, v in enumerate(importance):
    f.write(str(ind_var[i]) + ": " + str(round(v, 4)))
    f.write("\n")
f.writelines("\n".join(metrics))

# fourth model: also predicting stock return for day t+3
xDf = publicDf[ind_var2]
x_train, x_test, y_train, y_test = train_test_split(xDf, yDf, test_size=.3, random_state=101)
model4 = LinearRegression()
model4.fit(x_train, y_train)
predictions = model4.predict(x_test)

importance = list(model4.coef_)[0]
metrics = ["Mean squared error: " + str(mean_squared_error(y_test, predictions)),
           "Root mean squared error: " + str(root_mean_squared_error(y_test, predictions)),
           "Mean absolute error: " + str(mean_absolute_error(y_test, predictions)),
           "R2 Score: " + str(r2_score(y_test, predictions))]
f.write("\n\n4th Model:\n")
for i, v in enumerate(importance):
    f.write(str(ind_var2[i]) + ": " + str(round(v, 4)))
    f.write("\n")
f.writelines("\n".join(metrics))
f.write("\n\nStandard deviation: " + str(stdev(publicDf[dep_var[0]].tolist())))
f.close()

descriptionDf = xDf.describe(include="all")
descriptionDf.to_csv(r"%s\data\mlrAnalysisDescStats.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
