from pprint import pprint
import pandas as pd
import os
import yahooquery as yq
import datetime
from yahooquery import Ticker
from exchangeAbb import abb_reversed

def find_ticker_and_exchange(name):
    try:
        data = yq.search(name)
    except ValueError as e: # Will catch JSONDecodeError
        raise e
    except BaseException as e:
        raise e
    else:
        if data is None:
            raise Exception(f'Company {name} is not found.')
        quotes = data['quotes']
        if len(quotes) == 0:
            raise Exception(f'Ticker/exchange is not found for {name}')
        symbol = quotes[0]['symbol'] if "symbol" in quotes[0] else None
        exch = quotes[0]["exchDisp"] if "exchDisp" in quotes[0] else None

        return symbol, exch


def test_get_tickers():
    company_names = ["BMW", "Capital One"]
    return get_trading_symbols_df(company_names)


def get_trading_symbols_df(companies):
    output = []
    count = 0
    for co in companies:
        print("---->Searching for company:" + co)
        dictEntry = {}
        try:
            ticker, exchange = find_ticker_and_exchange(co)
        except Exception as e:
            print("Exception: " + str(e))
        else:
            dictEntry["Company"] = co
            dictEntry["Ticker"] = ticker
            dictEntry["Exchange"] = exchange
            output.append(dictEntry)
            count += 1
    print("====>Found tickers for %s of %s companies" % (count, len(companies)))

    return pd.DataFrame.from_records(output)


def get_and_save_tickers():
    # load public company csv file
    pubCompanies = pd.read_csv(r"%s\data\publicCompanies.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    pubCompanies = pubCompanies["Company"].tolist()
    pubCompanies = set([c.strip() for c in pubCompanies])
    # load public company csv file with tickers found & not found
    dfPubCompanyTickers = None
    pubCompaniesWithoutTickers = pubCompanies
    pubCompanyTickersFile = r"%s\data\publicCompanyTickers.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir))
    if os.path.exists(pubCompanyTickersFile):
        dfPubCompanyTickers = pd.read_csv(pubCompanyTickersFile)
        if dfPubCompanyTickers is not None and not dfPubCompanyTickers.empty:
            pubCompaniesWithoutTickers = pubCompanies - set(dfPubCompanyTickers["Company"].tolist())
    # pubCompanies need to find tickers
    pubCompaniesWithoutTickers = [c.strip() for c in pubCompaniesWithoutTickers]
    print("---->Finding tickers for %s companies." % len(pubCompaniesWithoutTickers))
    dfTickers = get_trading_symbols_df(pubCompaniesWithoutTickers)
    # Save tickers found
    if dfTickers is not None and not dfTickers.empty:
        if dfPubCompanyTickers is not None and not dfPubCompanyTickers.empty:
            dfPubCompanyTickers = pd.concat([dfPubCompanyTickers, dfTickers])
        else:
            dfPubCompanyTickers = dfTickers
        dfPubCompaniesWithoutTickers = pd.DataFrame(list(pubCompanies - set(dfPubCompanyTickers["Company"].tolist())),
                                                    columns=["Company"])
        print('===>Saving tickers for %s companies.' % (len(dfPubCompanyTickers)))
        dfPubCompanyTickers.to_csv(pubCompanyTickersFile, index=False)
        dfPubCompaniesWithoutTickers.to_csv(r"%s\data\publicCompanyNoTicker.csv" % os.path.normpath(os.path.join(
            os.getcwd(), os.pardir)), index=False)


def clean_exchange_names():
    df = pd.read_csv(r"%s\data\publicCompanyTickers.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    df["Exchange"] = df["Exchange"].apply(lambda x: abb_reversed[x] if x in abb_reversed.keys() else x)
    pprint(df)
    df.to_csv(r"%s\data\publicCompanyTickers.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
              index=False)


def join_trading_symbols_df(dataframe):
    tradingDf = pd.read_csv(r"%s\data\publicCompanyTickers.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    joinedDf = pd.merge(dataframe, tradingDf, how="left", on="Company")
    joinedDf.to_csv(r"%s\data\layoffData.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)), index=False)


def get_prices(stock, layoffdate):
    symbol = Ticker(stock)
    symbolPrices = symbol.history(interval="1d", start=layoffdate - datetime.timedelta(days=7),
                                  end=layoffdate + datetime.timedelta(days=7))
    # print(symbolPrices)
    if symbolPrices.empty:
        raise Exception(f"Historical prices for {stock} not found.")
    symbolPrices.reset_index(level="symbol", inplace=True)
    if layoffdate in symbolPrices.index:
        layoffdateIndex = symbolPrices.index.get_loc(layoffdate)
    else:
        dateToCheck = layoffdate
        while dateToCheck not in symbolPrices.index:
            dateToCheck += datetime.timedelta(days=1)
        layoffdateIndex = symbolPrices.index.get_loc(dateToCheck)
    # print(layoffdateIndex)
    symbolPrices = symbolPrices.iloc[layoffdateIndex - 4:layoffdateIndex + 4]
    # print(symbolPrices)
    symbolPrices["Date of Layoff"] = [layoffdate] * len(symbolPrices.index)
    return symbolPrices


def get_daily_returns_1wk(pricesdf):
    keynames = ["Stock Return 3 Days Before t", "Stock Return 2 Days Before t",
                "Stock Return 1 Day Before t", "Stock Return On Closest Trading Date Post-Announcement (t)",
                "Stock Return 1 Day After t", "Stock Return 2 Days After t",
                "Stock Return 3 Days After t"]
    adjcloses = pricesdf["adjclose"]
    dailyReturns = [((adjcloses.iloc[x]/adjcloses.iloc[x - 1]) - 1) for x in range(len(adjcloses)) if x > 0]
    dateReturnMap = dict(zip(keynames, dailyReturns))
    dateReturnMap["Ticker"] = pricesdf["symbol"].iloc[0]
    dateReturnMap["Date of Layoff"] = pricesdf["Date of Layoff"].iloc[0]

    # pprint(dateReturnMap)
    return dateReturnMap


def get_returns_dataframe(dataframe):
    returns = []
    failed = []
    tickers = dataframe["Ticker"].tolist()
    for count, ticker in enumerate(tickers):
        print(ticker)
        try:
            dictEntry = get_daily_returns_1wk(get_prices(ticker, dataframe.iat[count, 5]))
            returns.append(dictEntry)
        except BaseException as e:
            print("Exception: " + str(e))
            failed.append(dict([("Ticker", ticker), ("Date of Layoff", dataframe.iat[count, 5])]))
    returnsdf = pd.DataFrame.from_records(returns)
    colnames = ["Date of Layoff", "Ticker", "Stock Return On Closest Trading Date Post-Announcement (t)",
                "Stock Return 3 Days Before t", "Stock Return 2 Days Before t", "Stock Return 1 Day Before t",
                "Stock Return 1 Day After t", "Stock Return 2 Days After t",
                "Stock Return 3 Days After t"]
    returnsdf = returnsdf[colnames]
    failed_df = pd.DataFrame.from_records(failed)
    failed_df.to_csv(r"%s\data\tickersNoHistoricalPrices.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
                     index=False)

    return returnsdf


if '__main__' == __name__:
    # get_and_save_tickers()
    df = pd.read_csv(r"%s\data\layoffData.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    # df.drop(columns=["Ticker", "Exchange", "Stock Delisted"], inplace=True)
    df["Date of Layoff"] = [datetime.datetime.strptime(x, f"%m/%d/%Y").date() for x in df["Date of Layoff"]]
    # join_trading_symbols_df(df)

    publicSample = df[(df["Ticker"].notna()) & (df["Stock Delisted"] == False)]
    # test = publicSample[publicSample["Ticker"] == "GPRO"]
    stockReturnsdf = get_returns_dataframe(publicSample)
    # print(stockReturnsdf)
    stockReturnsdf.to_csv(r"%s\data\returnsTest.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))

    df = pd.merge(df, stockReturnsdf, how="left", on=["Date of Layoff", "Ticker"])
    df.to_csv(r"%s\data\layoffDataWithReturns.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
