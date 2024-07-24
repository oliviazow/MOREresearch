from pprint import pprint
import pandas as pd
import numpy as np
import os
import datetime
import json
import requests
# import dask.dataframe as dd
from main import layoffDataFullSimpl

def get_cik(ticker):
    with open(r"%s\data\company_tickers_exchange.json" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
              "r") as f:
        CIK_dict = json.load(f)
    CIK_df = pd.DataFrame(CIK_dict["data"], columns=CIK_dict["fields"])
    filteredCIK_df = CIK_df[CIK_df["ticker"] == ticker]
    if filteredCIK_df.empty:
        raise Exception("Company not found")

    return filteredCIK_df.iat[0, 0]


def find_all_ciks():
    publicCosTickersDf = pd.read_csv(r"%s\data\publicCompanyTickers.csv" % os.path.normpath(os.path.join(os.getcwd(),
                                                                                                         os.pardir)))
    tickersList = publicCosTickersDf["Ticker"].tolist()
    cikList = []
    for t in tickersList:
        if "." in t:
            placeholderList = t.split(".")
            t = placeholderList[0]
            print(t)
        try:
            cik = get_cik(t)
        except Exception as e:
            cikList.append(None)
            print("Exception: " + str(e))
        else:
            cikList.append(int(cik))
    publicCosTickersDf["cik"] = cikList
    publicCosTickersDf.to_csv(r"%s\data\publicCompanyTickers.csv" % os.path.normpath(os.path.join(
                              os.getcwd(), os.pardir)), index=False)


def edgar_financials_retrieval(cik):
    header = {
        "User-Agent": "oz45@georgetown.edu"
    }
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json/"
    company_facts = requests.get(url, headers=header).json()
    # with open(r"%s\data\companyFactsUSA.json" % os.path.normpath(os.path.join(os.getcwd(),
    #                                                                        os.pardir)), "w", encoding='utf-8') as f:
    #     json.dump(company_facts, f, ensure_ascii=False, indent=4)
    if not layoffDataFullSimpl[layoffDataFullSimpl["cik"] == cik].iat[0, 1]:
        currency = [*company_facts["facts"]["ifrs-full"]["IncreaseDecreaseInCashAndCashEquivalents"]["units"].keys()][0]
        cashDf = pd.DataFrame(company_facts["facts"]["ifrs-full"]["IncreaseDecreaseInCashAndCashEquivalents"]["units"]
                              [currency])
        revenueDf = pd.DataFrame(company_facts["facts"]["ifrs-full"]["Revenue"]["units"][currency])
        incomeLossDf = pd.DataFrame(company_facts["facts"]["ifrs-full"]["ProfitLoss"]["units"][currency])
    else:
        cashDf = pd.DataFrame(company_facts["facts"]["us-gaap"]
                              ["CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecrease"
                               "IncludingExchangeRateEffect"]["units"]["USD"])
        revenueDf = pd.DataFrame(company_facts["facts"]["us-gaap"]["RevenueFromContractWithCustomerExcludingAssessedTax"]
                                 ["units"]["USD"])
        incomeLossDf = pd.DataFrame(company_facts["facts"]["us-gaap"]["NetIncomeLoss"]["units"]["USD"])
    print(cashDf)
    print(revenueDf)
    print(incomeLossDf)


if '__main__' == __name__:
    testList = ["SFIX", "DHER.DE", "GOTO.JK", "0700.HK", "TKWY.AS", "ROO.L"]
    publicCos = pd.read_csv(r"%s\data\publicCompanyTickers.csv" % os.path.normpath(os.path.join(os.getcwd(),
                                                                                                os.pardir)))
    # financialsDf = dd.read_csv(r"%s\data\10K10Qdataset.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    # print(financialsDf[(financialsDf["cik"] == publicCos.at[0, "cik"]) & (financialsDf["companyFact"] == "Cash")])
    # edgar_financials_retrieval(int(publicCos.at[0, "cik"])) # PropertyGuru
    edgar_financials_retrieval(int(publicCos.at[5, "cik"])) # Warby Parker

