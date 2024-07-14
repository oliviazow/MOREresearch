from pprint import pprint
import pandas as pd
import os
import datetime
import yahooquery as yq

layoffDatadf = pd.read_csv(r"%s\data\layoffData.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
layoffDataWReturnsdf = pd.read_csv(r"%s\data\layoffDataWithReturns.csv" % os.path.normpath(os.path.join(os.getcwd(),
                                                                                                        os.pardir)))
layoffDataFull = pd.read_csv(r"%s\data\layoffDataFull.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
if "/" in layoffDataFull["Date of Layoff"].iat[0]:
    layoffDataFull["Date of Layoff"] = [datetime.datetime.strptime(x, f"%m/%d/%Y").date() for x in
                                        layoffDataFull["Date of Layoff"]]
elif "-" in layoffDataFull["Date of Layoff"].iat[0]:
    layoffDataFull["Date of Layoff"] = [datetime.datetime.strptime(x, f"%Y-%m-%d").date() for x in
                                        layoffDataFull["Date of Layoff"]]

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
listedCols = ["Job Positions Laid Off Desc", "Job Positions Laid Off Specific", "AI Relation"]

layoffDataFullTest = pd.read_csv(r"%s\data\layoffDataFullTest.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
if "/" in layoffDataFullTest["Date of Layoff"].iat[0]:
    layoffDataFullTest["Date of Layoff"] = [datetime.datetime.strptime(x, f"%m/%d/%Y").date() for x in
                                            layoffDataFullTest["Date of Layoff"]]
elif "-" in layoffDataFullTest["Date of Layoff"].iat[0]:
    layoffDataFullTest["Date of Layoff"] = [datetime.datetime.strptime(x, f"%Y-%m-%d").date() for x in
                                            layoffDataFullTest["Date of Layoff"]]
dfCheck = pd.merge(layoffDataFull, layoffDataFullTest, how="left", on=colnamesFull, indicator=True)
dfCheckFiltered = dfCheck[dfCheck["_merge"] != "both"]
print(dfCheckFiltered)


for col in listedCols:
    layoffDataFull[col] = [x.lower().split(", ") if type(x) is str else None for x in layoffDataFull[col]]

jobDescs = layoffDataFull.loc[layoffDataFull["Job Positions Laid Off Desc"].notna(),
                              "Job Positions Laid Off Desc"].tolist()
jobsSpec = layoffDataFull.loc[layoffDataFull["Job Positions Laid Off Specific"].notna(),
                              "Job Positions Laid Off Specific"].tolist()
AIrelations = layoffDataFull.loc[layoffDataFull["AI Relation"].notna(), "AI Relation"].tolist()
layoffReasons1 = layoffDataFull["Reason Mentioned 1"].dropna().tolist()
layoffReasons2 = layoffDataFull["Reason Mentioned 2"].dropna().tolist()
layoffReasons3 = layoffDataFull["Reason Mentioned 3"].dropna().tolist()
layoffReasons = layoffReasons1 + layoffReasons2 + layoffReasons3
# pprint(layoffReasons)

placeholder = []
for listOfThings in jobDescs:
    for i in range(len(listOfThings)):
        placeholder.append(listOfThings[i])
jobDescs = placeholder
jobDescsUniq = list(set(jobDescs))
# pprint(jobDescsUniq)

placeholder = []
for listOfThings in jobsSpec:
    for i in range(len(listOfThings)):
        placeholder.append(listOfThings[i])
jobsSpec = placeholder
jobsSpecUniq = list(set(jobsSpec))
# pprint(jobsSpecUniq)

placeholder = []
for listOfThings in AIrelations:
    for i in range(len(listOfThings)):
        placeholder.append(listOfThings[i])
AIrelations = placeholder
AIrelationsUniq = list(set(AIrelations))
# pprint(AIrelationsUniq)