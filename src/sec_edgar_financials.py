from pprint import pprint
import pandas as pd
import numpy as np
import os
import datetime
import json
import requests
# import dask.dataframe as dd
from main import layoffDataFullSimpl
from main import colnamesFull


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


def get_sic(cik):
    header = {
        "User-Agent": "oz45@georgetown.edu"
    }
    url = f"https://data.sec.gov/submissions/CIK{str(cik).zfill(10)}.json/"
    company_filings = requests.get(url, headers=header).json()
    sic = company_filings["sic"]
    # sic_desc = company_filings["sicDescription"]
    return sic


def find_all_sics():
    dictList = []
    cikList = layoffDataFullSimpl[layoffDataFullSimpl["cik"].notna()]["cik"].unique().tolist()
    for c in cikList:
        dictEntry = dict(cik=int(c), sic=get_sic(int(c)))
        dictList.append(dictEntry)
        print(c)
    cikSicDf = pd.DataFrame.from_records(dictList)
    mergedDf = pd.merge(layoffDataFullSimpl, cikSicDf, how="left", on=["cik"])
    mergedDf = mergedDf[colnamesFull]
    mergedDf.to_csv(r"%s\data\layoffDataFullSimpl.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
                    index=False)


def edgar_financials_df_retrieval(cik):
    header = {
        "User-Agent": "oz45@georgetown.edu"
    }
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json/"
    company_facts = requests.get(url, headers=header).json()

    url = f"https://data.sec.gov/submissions/CIK{str(cik).zfill(10)}.json/"
    company_filings = requests.get(url, headers=header).json()
    # --------- save to local --------------------------------------------------------------------------------
    with open(r"%s\data\sageTherapeutics.json" % os.path.normpath(os.path.join(os.getcwd(),
                                                                           os.pardir)), "w", encoding='utf-8') as f:
        json.dump(company_facts, f, ensure_ascii=False, indent=4)
    # with open(r"%s\data\companyFilings.json" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)), "w",
    #           encoding="utf-8") as f:
    #     json.dump(company_filings, f, ensure_ascii=False, indent=4)
    # ---------------------------------------------------------------------------------------------------------
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
        try:
            revenueDf = pd.DataFrame(company_facts["facts"]["us-gaap"]["RevenueFromContractWithCustomerExcluding"
                                                                       "AssessedTax"]["units"]["USD"])
        except KeyError as e:
            revenueDf = pd.DataFrame(company_facts["facts"]["us-gaap"]["Revenues"]["units"]["USD"])
            print("Error: " + str(e))
        incomeLossDf = pd.DataFrame(company_facts["facts"]["us-gaap"]["NetIncomeLoss"]["units"]["USD"])
    filingTimeDf = pd.DataFrame(company_filings["filings"]["recent"])
    filingTimeDf = filingTimeDf[["accessionNumber", "acceptanceDateTime", "form"]]
    filingTimeDf.rename(columns={"accessionNumber": "accn"}, inplace=True)
    revenueDf = pd.merge(revenueDf, filingTimeDf, how="left", on=["accn", "form"])
    incomeLossDf = pd.merge(incomeLossDf, filingTimeDf, how="left", on=["accn", "form"])

    return revenueDf, incomeLossDf


def get_revenues_by_date(df, date, announced_post):
    dateCols = ["start", "end", "filed"]
    if announced_post:
        date += datetime.timedelta(days=1)
    for col in dateCols:
        df[col] = [datetime.datetime.strptime(x, f"%Y-%m-%d").date() for x in df[col]]

    df["acceptanceDateTime"] = [datetime.datetime.strptime(str(x), f"%Y-%m-%dT%H:%M:%S.%fZ") if type(x) is str
                                else x for x in df["acceptanceDateTime"]]

    if df[df["form"] == "20-F"].empty and df[df["form"] == "40-F"].empty:
        df = df[(df["filed"] <= date) & (df["form"] == "10-Q")]
        if df.empty:
            return "N/A"
        # print(df)
        dateFloor = df.tail(1).iat[0, 1]  # 1 = col for end
        thisQ = df[df["start"] > (dateFloor - datetime.timedelta(days=120))]
        if thisQ.empty:
            return "N/A"
        revThisYr = thisQ.iat[0, 2]  # 2 = col for value
        lastDateFloor = dateFloor - datetime.timedelta(days=360)  # gives date a little later than a year earlier
        df = df[df["end"] < lastDateFloor]  # looks for dates earlier than < approx. 1yr ago
        if df.empty:
            return "N/A"
        lastDateFloor = df.tail(1).iat[0, 1]
        lastQ = df[df["start"] > (lastDateFloor - datetime.timedelta(days=120))]
        if lastQ.empty:
            return "N/A"
        # print(lastQ)
        revLastYr = lastQ.iat[0, 2]
        if revLastYr == 0:
            return "N/A"
        yoyRev = (revThisYr/revLastYr) - 1
    else:
        df = df[df["filed"] <= date]
        if df.empty:
            return "N/A"
        revThisYr = df.tail(1).iat[0, 2]
        lastDateFloor = df.tail(1).iat[0, 1] - datetime.timedelta(days=360)
        df = df[df["end"] < lastDateFloor]
        if df.empty:
            return "N/A"
        revLastYr = df.tail(1).iat[0, 2]
        yoyRev = (revThisYr/revLastYr) - 1

    return yoyRev


def get_net_income_by_date(df, date, announced_post):
    dateCols = ["start", "end", "filed"]
    if announced_post:
        date += datetime.timedelta(days=1)
    for col in dateCols:
        df[col] = [datetime.datetime.strptime(x, f"%Y-%m-%d").date() for x in df[col]]

    if df[df["form"] == "20-F"].empty and df[df["form"] == "40-F"].empty:
        df = df[(df["filed"] <= date) & (df["form"] == "10-Q")]
        if df.empty:
            return "N/A"
        dateFloor = df.tail(1).iat[0, 1]
        thisQ = df[df["start"] > (dateFloor - datetime.timedelta(days=120))]
        netIncomeLoss = thisQ.iat[0, 2]
    else:
        df = df[(df["filed"] <= date)]
        if df.empty:
            return "N/A"
        netIncomeLoss = df.iat[-1, 2]

    return netIncomeLoss


def add_to_layoff_df():
    financialsList = []
    ciks = layoffDataFullSimpl.loc[layoffDataFullSimpl["cik"].notna(), "cik"].tail(10)
    # ciks = [1874178, 1828318]
    for c in ciks:
        filteredDf = layoffDataFullSimpl[layoffDataFullSimpl["cik"] == c]
        for count, date in enumerate(filteredDf["Date of Layoff"].tolist()):
            dictEntry = dict.fromkeys(["cik", "Date of Layoff", "YoYRev", "NetIncomeLoss"])
            print(filteredDf.iat[0, 0])
            dictEntry["cik"] = c
            dictEntry["Date of Layoff"] = date
            revenueDf, incomeLossDf = edgar_financials_df_retrieval(int(c))
            dictEntry["YoYRev"] = get_revenues_by_date(revenueDf, date, filteredDf.iat[count, 24])
            # announced post-trading hours = 24th column
            dictEntry["NetIncomeLoss"] = get_net_income_by_date(incomeLossDf, date, filteredDf.iat[count, 24])
            financialsList.append(dictEntry)
    pprint(financialsList)
    financialsDf = pd.DataFrame.from_records(financialsList)
    mergedDf = pd.merge(layoffDataFullSimpl, financialsDf, how="left", on=["cik", "Date of Layoff"])


if '__main__' == __name__:
    publicCos = pd.read_csv(r"%s\data\publicCompanyTickers.csv" % os.path.normpath(os.path.join(os.getcwd(),
                                                                                                os.pardir)))
    # financialsDf = dd.read_csv(r"%s\data\10K10Qdataset.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    # secRevenueDf, secIncomeLossDf = edgar_financials_df_retrieval(int(publicCos.at[0, "cik"])) # PropertyGuru
    # secRevenueDf, secIncomeLossDf = edgar_financials_df_retrieval(int(publicCos.at[5, "cik"])) # Warby Parker
    # print(secRevenueDf)

    # idx = layoffDataFullSimpl.index[layoffDataFullSimpl["Company"] == "The RealReal"].tolist()[0]  # Bitfarms
    # secRevenueDf, secIncomeLossDf = edgar_financials_df_retrieval(int(layoffDataFullSimpl.at[idx, "cik"]))
    # print(get_revenues_by_date(secRevenueDf, layoffDataFullSimpl.at[idx, "Date of Layoff"], layoffDataFullSimpl.at[
    #       idx, "Announced Post-Trading Hours"]))

    # secRevenueDf, secIncomeLossDf = edgar_financials_df_retrieval(int(publicCos.at[0, "cik"]))
    # idx = layoffDataFullSimpl.index[layoffDataFullSimpl["Company"] == "PropertyGuru"].tolist()[0]
    # print(get_revenues_by_date(secRevenueDf, layoffDataFullSimpl.at[idx, "Date of Layoff"], layoffDataFullSimpl.at[
    #       idx, "Announced Post-Trading Hours"]))
    # print(get_net_income_by_date(secIncomeLossDf, layoffDataFullSimpl.at[idx, "Date of Layoff"], layoffDataFullSimpl.at[
    #       idx, "Announced Post-Trading Hours"]))

    # add_to_layoff_df()
    find_all_sics()

