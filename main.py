from pprint import pprint
import pandas as pd
import datetime
import yahooquery as yq
import yfinance as yf
from dataclean import data

dataRows = data["data"]["table"]["rows"]

def prepare_data():
    dataColumns = data["data"]["table"]["columns"]
    dataLocationChoices = dataColumns[1]["typeOptions"]["choices"]
    dataIndustryChoices = dataColumns[5]["typeOptions"]["choices"]
    dataStageChoices = dataColumns[8]["typeOptions"]["choices"]
    dataCountryChoices = dataColumns[10]["typeOptions"]["choices"]

    # dictionary of column ids to column names
    columnIdNameMap = {}
    locationIdNameMap = {}
    industryIdNameMap = {}
    stageIdNameMap = {}
    countryIdNameMap = {}

    for col in dataColumns:
        columnIdNameMap[col["id"]] = col["name"]
    for id, dct in dataLocationChoices.items():
        locationIdNameMap[id] = dct['name']
    for id, dct in dataIndustryChoices.items():
        industryIdNameMap[id] = dct['name']
    for id, dct in dataStageChoices.items():
        stageIdNameMap[id] = dct['name']
    for id, dct in dataCountryChoices.items():
        countryIdNameMap[id] = dct['name']

    return columnIdNameMap, locationIdNameMap, industryIdNameMap, stageIdNameMap, countryIdNameMap

def test_data_preparation():
    columnIdNameMap, locationIdNameMap, industryIdNameMap, stageIdNameMap, countryIdNameMap = prepare_data()
    pprint(columnIdNameMap)
    pprint(locationIdNameMap)
    pprint(industryIdNameMap)
    pprint(stageIdNameMap)
    pprint(countryIdNameMap)


def convert():
    columnIdNameMap, locationIdNameMap, industryIdNameMap, stageIdNameMap, countryIdNameMap = prepare_data()
    converted_data = []
    for layoffDict in dataRows:
        dct = layoffDict["cellValuesByColumnId"]
        record = {}
        for id, val in dct.items():
            if 'Company' == columnIdNameMap[id]:
                record['Company'] = val
            elif 'Location HQ' == columnIdNameMap[id]:
                if len(val) > 1:
                    record["IsUS"] = False
                else:
                    record["IsUS"] = True
                record["Location HQ"] = locationIdNameMap[val[0]]
            elif "Industry" == columnIdNameMap[id]:
                record["Industry"] = industryIdNameMap[val]
            elif "# Laid Off" == columnIdNameMap[id]:
                record["Number Laid Off"] = val
            elif "%" == columnIdNameMap[id]:
                record["Percentage"] = val
            elif "Date" == columnIdNameMap[id]:
                record["Date of Layoff"] = datetime.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S.%fZ").date()
            elif "Source" == columnIdNameMap[id]:
                record["Source"] = val
            elif "$ Raised (mm)" == columnIdNameMap[id]:
                record["Dollars Raised (mil)"] = val
            elif "Stage" == columnIdNameMap[id]:
                record["Stage"] = stageIdNameMap[val]
            elif "Country" == columnIdNameMap[id]:
                record["Country"] = countryIdNameMap[val]
            else:
                pass
        converted_data.append(record)
    return converted_data


df = pd.DataFrame.from_records(convert())
# print(df)
# df.to_csv(r"C:\Users\olivi\Documents\moreResearch\layoffData.csv")

publicCos = df[df["Stage"] == "Post-IPO"]["Company"].unique().tolist()


def find_ticker_and_exchange(name):
    try:
        data = yq.search(name)
    except ValueError: # Will catch JSONDecodeError
        print("ValueError")
    else:
        quotes = data['quotes']
        if len(quotes) == 0:
            return None

        symbol = quotes[0]['symbol'] if "symbol" in quotes[0] else None
        exch = quotes[0]["exchDisp"] if "exchDisp" in quotes[0] else None

        return symbol, exch


def test_get_tickers():
    company_names = ["Groupon", "Capital One", "Akili Interactive", "Microsoft", "Apple", "Meta"]
    info = []

    for name in company_names:
        dict_entry = {}
        if find_ticker_and_exchange(name):
            ticker, exchange = find_ticker_and_exchange(name)
            if ticker and exchange:
                dict_entry["Company"] = name
                dict_entry["Ticker"] = ticker
                dict_entry["Exchange"] = exchange
                info.append(dict_entry)

    return info

def get_trading_symbols_df(companies):
    output = []
    for co in companies:
        dictEntry = {}
        print(co)
        if find_ticker_and_exchange(co) is not None:
            ticker, exchange = find_ticker_and_exchange(co)
            dictEntry["Company"] = co
            dictEntry["Ticker"] = ticker
            dictEntry["Exchange"] = exchange
            output.append(dictEntry)

    return pd.DataFrame.from_records(output)


pprint(test_get_tickers())
