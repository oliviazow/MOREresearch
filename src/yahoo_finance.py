import yahooquery as yq
import pandas as pd
import os
from pprint import pprint


def find_ticker_and_exchange(name):
    try:
        data = yq.search(name)
    except ValueError as e:
        raise e
    except BaseException as e:
        raise e
    else:
        if data is None:
            raise Exception(f'Company {name} is not found.')
        quotes = data['quotes']
        if len(quotes) == 0:
            raise Exception(f'Ticker/exchange is not found for {name}.')
        symbol = quotes[0]['symbol'] if 'symbol' in quotes[0] else None
        exchange = quotes[0]['exchDisp'] if 'exchDisp' in quotes[0] else None
        return symbol, exchange


def get_trading_symbols_df(companies):
    output = []
    cnt = 0
    for name in companies:
        print('--->Find ticker for company:', name)
        dictEntry = {}
        try:
            ticker, exchange = find_ticker_and_exchange(name)
        except BaseException as e:
            print("Exception: " + str(e))
        else:
            dictEntry["Company"] = name
            dictEntry["Ticker"] = ticker
            dictEntry["Exchange"] = exchange
            output.append(dictEntry)
            cnt += 1
    print('===>Found ticker for %s of %s companies' % (cnt, len(companies)))
    return pd.DataFrame.from_records(output)


def test_get_tickers():
    company_names = ["Groupon", "Capital One", "Akili Interactive", "Microsoft", "Apple", "Meta"]
    pprint(get_trading_symbols_df(company_names))


def get_and_save_tickers():
    # load public company csv file
    pubCompanies = pd.read_csv(r"%s\data\publicCompanies.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    print(pubCompanies)
    pubCompanies = set(pubCompanies['Company'].tolist())
    # load public company csv file with tickers found
    dfPubCompanyTickers = None
    pubCompaniesWithTickers = set()
    pubCompanyTickersFile = r"%s\data\publicCompanyTickers.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir))
    if os.path.exists(pubCompanyTickersFile):
        dfPubCompanyTickers = pd.read_csv(pubCompanyTickersFile)
        if dfPubCompanyTickers is not None and not dfPubCompanyTickers.empty:
            pubCompaniesWithTickers = set(dfPubCompanyTickers['Company'].tolist())
    # pubCompanies need to find tickers
    pubCompaniesToCheck = pubCompanies - pubCompaniesWithTickers
    pubCompaniesToCheck = [c.strip() for c in pubCompaniesToCheck]
    print('===>To find tickers for %s companies.' % (len(pubCompaniesToCheck)))
    dfTickers = get_trading_symbols_df(pubCompaniesToCheck)
    # Save the tickers found
    if dfTickers is not None and not dfTickers.empty:
        if dfPubCompanyTickers is not None and not dfPubCompanyTickers.empty:
            dfPubCompanyTickers = pd.concat([dfPubCompanyTickers, dfTickers])
        else:
            dfPubCompanyTickers = dfTickers
        print('===>Save tickers for %s companies.' % (len(dfPubCompanyTickers)))
        dfPubCompanyTickers.to_csv(pubCompanyTickersFile, index=False)


if '__main__' == __name__:
    get_and_save_tickers()