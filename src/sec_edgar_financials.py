from pprint import pprint
import pandas as pd
import numpy as np
import os
import datetime
import json


def get_cik(ticker):
    with open(r"%s\data\company_tickers_exchange.json" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
              "r") as f:
        CIK_dict = json.load(f)
    CIK_df = pd.DataFrame(CIK_dict["data"], columns=CIK_dict["fields"])
    filteredCIK_df = CIK_df[CIK_df["ticker"] == ticker]
    if filteredCIK_df.empty:
        return "Company not found"

    return filteredCIK_df.iat[0, 0]


def find_all_ciks(entry_list):
    # publicCosTickersDf = pd.read_csv(r"%s\data\publicCompanyTickers.csv" % os.path.normpath(os.path.join(os.getcwd(),
    #                                                                                                    os.pardir)))
    # tickersList = publicCosTickersDf["Ticker"].tolist()
    tickersList = entry_list
    cikList = []
    for t in tickersList:
        if "." in t:
            placeholderList = t.split(".")
            t = placeholderList[0]
            print(t)
        cikList.append(get_cik(t))
    # publicCosTickersDf["cik"] = cikList

    return cikList

# financialsDf = pd.read_csv(r"%s\data\10K10Qdataset.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))

if '__main__' == __name__:
    testList = ["SFIX", "DHER.DE", "GOTO.JK", "0700.HK", "TKWY.AS", "ROO.L"]
    print(find_all_ciks(testList))
