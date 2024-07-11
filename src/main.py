from pprint import pprint
import pandas as pd
import os
import datetime
import yahooquery as yq

layoffDatadf = pd.read_csv(r"%s\data\layoffData.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
layoffDataWReturnsdf = pd.read_csv(r"%s\data\layoffDataWithReturns.csv" % os.path.normpath(os.path.join(os.getcwd(),
                                                                                                        os.pardir)))
layoffDataFull = pd.read_csv(r"%s\data\layoffDataFull.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))

colnamesFull = ["Company", "IsUS", "Location HQ", "Industry", "Number Laid Off", "Date of Layoff", "Source",
                "Dollars Raised (mil)", "Stage", "Country", "Percentage",
                "Job Positions Laid Off Desc", "Job Positions Laid Off Specific",
                "Reason Mentioned 1", "Reason Mentioned 2", "Reason Mentioned 3", "Unprofitable", "AI Mentioned",
                "AI Relation", "Expansion Mentioned", "Ticker", "Exchange", "Stock Delisted",
                "Announced Post-Trading Hours", "Stock Return On Closest Trading Date Post-Announcement (t)",
                "Stock Return 3 Days Before t",
                "Stock Return 2 Days Before t","Stock Return 1 Day Before t","Stock Return 1 Day After t",
                "Stock Return 2 Days After t","Stock Return 3 Days After t","List of Employees Laid Off"]

extraLayoffDetails = ["Job Positions Laid Off Desc", "Job Positions Laid Off Specific", "Reason Mentioned 1",
                      "Reason Mentioned 2", "Reason Mentioned 3", "Unprofitable", "AI Mentioned", "AI Relation",
                      "Expansion Mentioned"]

booleanCols = ["IsUS", "Unprofitable", "AI Mentioned", "Expansion Mentioned", "Announced Post-Trading Hours"]
for col in booleanCols:
    layoffDataFull.loc[layoffDataFull[col] == "No", col] = False
    layoffDataFull.loc[layoffDataFull[col] == "Yes", col] = True
    layoffDataFull.loc[layoffDataFull[col] == "FALSE", col] = False
    layoffDataFull.loc[layoffDataFull[col] == "TRUE", col] = True
    print(layoffDataFull[col].unique())

layoffDataFull.to_csv(r"%s\data\layoffDataFull.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
                      index=False)