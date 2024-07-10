from pprint import pprint
import pandas as pd
import os
import datetime
import yahooquery as yq

layoffDatadf = pd.read_csv(r"%s\data\layoffData.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
layoffDataWReturnsdf = pd.read_csv(r"%s\data\layoffDataWithReturns.csv" % os.path.normpath(os.path.join(os.getcwd(),
                                                                                                        os.pardir)))
layoffDataFull = pd.read_csv(r"%s\data\layoffDataFull.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
layoffDataFull2 = pd.read_csv(r"%s\data\layoffDataFull2.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))

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

layoffDataFull2 = layoffDataFull2[colnamesFull]
layoffDataFull2.to_csv(r"%s\data\layoffDataFull2.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
                       index=False)

layoffDataFull2 = layoffDataFull2.iloc[501:]
for col in ["Unprofitable", "AI Mentioned", "Expansion Mentioned"]:
    layoffDataFull2.loc[layoffDataFull2[col] == "Yes", col] = True
    layoffDataFull2.loc[layoffDataFull2[col] == "No", col] = False
    layoffDataFull2.loc[layoffDataFull2[col] == "no", col] = False
    print(layoffDataFull2[col].unique())

layoffDataFull = layoffDataFull.iloc[:498]
layoffDataFull = pd.concat([layoffDataFull, layoffDataFull2])

layoffDataFull.to_csv(r"%s\data\layoffDataFull.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
                      index=False)
