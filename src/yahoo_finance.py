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
        except BaseException as e:
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
    joinedDf = pd.merge(dataframe, tradingDf, how="left")
    joinedDf.to_csv(r"%s\data\layoffData.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))


def get_prices(stock, startdate):
    symbol = Ticker(stock)
    symbolPrices = symbol.history(interval="1d", start=startdate, end= startdate + datetime.timedelta(days=17))
    symbolPrices.reset_index(level="symbol", inplace=True)
    symbolPrices.drop(columns=["symbol"], inplace=True)

    # startdateIndex = symbolPrices.index.get_loc(startdate.strftime("%Y-%m-%d"))
    # print(startdateIndex)
    # symbolPrices = symbolPrices.iloc[startdateIndex - 3:startdateIndex + 4]
    return symbolPrices

def get_daily_returns_1wk(pricesdf, startdate):
    dates = pricesdf["date"]
    dates = dates[(dates.index(startdate) - 3):(dates.index(startdate) + 3)]
    adjcloses = pricesdf["adjclose"]
    dailyReturns = [((adjcloses.iloc[x + 1]/adjcloses.iloc[x]) - 1) for x in range(len(adjcloses)) if x < (len(adjcloses) - 1)]
    dateReturnMap = dict(zip(dates, dailyReturns))
    return dateReturnMap

if '__main__' == __name__:
    # get_and_save_tickers()
    df = pd.read_csv(r"%s\data\layoffData.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    # join_trading_symbols_df(df)

    publicSample = df[(df["Stage"] == "Post-IPO") & (df["IsUS"])].head(5)
    publicSampleTickers = publicSample["Ticker"].tolist()

    # for ticker in publicSampleTickers:
    #     pprint(get_prices(ticker))
    googleHist = get_prices("GOOG", datetime.datetime.strptime(df.at[10, "Date of Layoff"], "%Y-%m-%d").date() -
                            datetime.timedelta(days=5))
    pprint(googleHist.index)