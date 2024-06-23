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
                "Reason Mentioned 1","Reason Mentioned 2","AI Mentioned","AI Relation",
                "Expansion Mentioned", "Ticker", "Exchange", "Stock Delisted",
                "Stock Return On Closest Trading Date Post-Announcement (t)","Stock Return 3 Days Before t",
                "Stock Return 2 Days Before t","Stock Return 1 Day Before t","Stock Return 1 Day After t",
                "Stock Return 2 Days After t","Stock Return 3 Days After t","List of Employees Laid Off"]

extraLayoffDetails = ["Area of Layoffs 1", "Area of Layoffs 2", "Area of Layoffs 3", "Job Positions Laid Off 1",
                      "Job Positions Laid Off 2", "Reason Mentioned 1", "Reason Mentioned 2", "AI Mentioned",
                      "AI Relation", "Expansion Mentioned"]