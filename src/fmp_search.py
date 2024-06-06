from fmp_api import fmp_api_key
import json
import pandas as pd
import os
from pprint import pprint
from urllib.request import urlopen


def get_jsonparsed_data(url):
    response = urlopen(url)
    data = response.read().decode("utf-8")
    return json.loads(data)


def find_tickers():
    df = pd.read_csv(r"%s\data\publicCompanyNoTicker.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    cosToSearch = df["Company"].tolist()
    companyTickerMap = {}
    for co in cosToSearch:
        co = co.replace(" ", "-")
        currentUrl = f"https://financialmodelingprep.com/api/v3/search-name?query={co}&limit=10&apikey={fmp_api_key}"
        companyTickerMap[co] = get_jsonparsed_data(currentUrl)

    return companyTickerMap


def get_quote(ticker):
    currentUrl = f"https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={fmp_api_key}"
    return get_jsonparsed_data(currentUrl)


def find_companies_to_search():
    publicCompanyTickers = pd.read_csv(r"%s\data\publicCompanyTickers.csv" % os.path.normpath(os.path.join(os.getcwd(),
                                                                                                           os.pardir)))
    publicCompanyTickersDelisted = publicCompanyTickers[publicCompanyTickers["Stock Delisted"]]
    return publicCompanyTickersDelisted


if '__main__' == __name__:
    companiesToSearch = find_companies_to_search()

    pprint(get_quote("PTRA"))
