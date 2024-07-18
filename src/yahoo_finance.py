from pprint import pprint
import pandas as pd
import os
import yahooquery as yq
import datetime
from yahooquery import Ticker
from exchangeAbb import abb_reversed
from main import colnamesFull

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
    tradingDf["Stage"] = ["Post-IPO"] * len(tradingDf.index)
    joinedDf = pd.merge(dataframe, tradingDf, how="left", on=["Company", "Stage"])
    joinedDf.to_csv(r"%s\data\layoffData.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)), index=False)


def get_prices(stock, layoffdate, ann_time):
    symbol = Ticker(stock)
    dateOfInt = layoffdate
    if ann_time:
        dateOfInt = layoffdate + datetime.timedelta(days=1)

    symbolPrices = symbol.history(interval="1d", start=dateOfInt - datetime.timedelta(days=7),
                                  end=dateOfInt + datetime.timedelta(days=7))
    # print(symbolPrices)
    if symbolPrices.empty:
        raise Exception(f"Historical prices for {stock} not found.")
    symbolPrices.reset_index(level="symbol", inplace=True)
    if dateOfInt in symbolPrices.index:
        dateOfIntIndex = symbolPrices.index.get_loc(dateOfInt)
    else:
        dateToCheck = dateOfInt
        while dateToCheck not in symbolPrices.index:
            dateToCheck += datetime.timedelta(days=1)
        dateOfIntIndex = symbolPrices.index.get_loc(dateToCheck)
    # print(dateOfIntIndex)
    symbolPrices = symbolPrices.iloc[dateOfIntIndex - 4:dateOfIntIndex + 4]
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


def get_returns_dataframe(dataframe, layoffdate_col, ann_time_col):
    returns = []
    failed = []
    tickers = dataframe["Ticker"].tolist()
    for count, ticker in enumerate(tickers):
        print(ticker)
        try:
            dictEntry = get_daily_returns_1wk(get_prices(ticker, dataframe.iat[count, layoffdate_col],
                                                         dataframe.iat[count, ann_time_col]))
            returns.append(dictEntry)
        except BaseException as e:
            print("Exception: " + str(e))
            failed.append(dict([("Ticker", ticker), ("Date of Layoff", dataframe.iat[count, layoffdate_col])]))
    returnsdf = pd.DataFrame.from_records(returns)
    failed_df = pd.DataFrame.from_records(failed)
    failed_df.to_csv(r"%s\data\tickersNoHistoricalPrices.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
                     index=False)
    if returnsdf.empty:
        return None
    colnames = ["Date of Layoff", "Ticker", "Stock Return On Closest Trading Date Post-Announcement (t)",
                "Stock Return 3 Days Before t", "Stock Return 2 Days Before t", "Stock Return 1 Day Before t",
                "Stock Return 1 Day After t", "Stock Return 2 Days After t",
                "Stock Return 3 Days After t"]
    returnsdf = returnsdf[colnames]

    return returnsdf


def get_returns_not_yet_found(df):
    publicSample = df[(df["Ticker"].notna()) & (df["Stock Delisted"] == False)]

    # Trying to find stock returns for companies that haven't been found yet
    completedReturns = pd.read_csv(r"%s\data\returnsTest.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    completedReturns["Date of Layoff"] = [datetime.datetime.strptime(x, f"%Y-%m-%d").date() for x in
                                          completedReturns["Date of Layoff"]]
    completedReturnsTickerDate = completedReturns[["Date of Layoff", "Ticker", "Announced Post-Trading Hours"]]
    publicSampleTickerDate = publicSample[["Date of Layoff", "Ticker", "Announced Post-Trading Hours"]]
    dfDifference = pd.merge(publicSampleTickerDate, completedReturnsTickerDate, how="left",
                            on=["Date of Layoff", "Ticker", "Announced Post-Trading Hours"], indicator=True)
    dfDifference = dfDifference[dfDifference["_merge"] == "left_only"]
    dfDifference.drop(columns=["_merge"], inplace=True)

    stockReturnsdf = get_returns_dataframe(dfDifference, 0, 2)

    if stockReturnsdf is not None:
        df = pd.merge(df, stockReturnsdf, how="left", on=["Date of Layoff", "Ticker"])
        df.to_csv(r"%s\data\layoffDataWithReturns.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
                  index=False)
        # stockReturnsdf = pd.concat([completedReturns, stockReturnsdf])
        stockReturnsdf.to_csv(r"%s\data\returnsTest.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
                              index=False)


def get_financials(accts, ticker):
    company = Ticker(ticker)
    return company.get_financial_data(types=accts, frequency="q", trailing=False)


if '__main__' == __name__:
    # get_and_save_tickers()
    financials = ["NetIncome", "TotalRevenue", "BeginningCashPosition", "EndCashPosition", "NormalizedEBITDA"]
    # Want to measure profitability (net income, positive cash flow, EBITDA), Year-over-year revenue

    df = pd.read_csv(r"%s\data\layoffDataFull.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    # df.drop(columns=["Ticker", "Exchange", "Stock Delisted"], inplace=True)
    if "/" in df["Date of Layoff"].iat[0]:
        df["Date of Layoff"] = [datetime.datetime.strptime(x, f"%m/%d/%Y").date() for x in df["Date of Layoff"]]
    elif "-" in df["Date of Layoff"].iat[0]:
        df["Date of Layoff"] = [datetime.datetime.strptime(x, f"%Y-%m-%d").date() for x in df["Date of Layoff"]]
    # join_trading_symbols_df(df)

    sample = df[(df["Ticker"].notna()) & (df["Stock Delisted"] == False)]
    sample.reset_index(drop=True, inplace=True)
    # print(sample.at[0, "Ticker"])
    qResults = get_financials(financials, sample.at[0, "Ticker"])
    print(qResults)