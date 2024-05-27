from pprint import pprint
import pandas as pd
import datetime
import yahooquery as yq
import yfinance as yf

from layoff_data import data


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


# pprint(data)

from symbols import symbols
# pprint(symbols)
pprint(len(symbols))
exchanges = set([s['exchange'] for s in symbols])
pprint(exchanges)
pprint(len(exchanges))

pprint(publicCos)
pprint(len(publicCos))