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


df = pd.read_csv(r"%s\data\publicCompanyNoTicker.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
companiesToSearch = df["Company"].tolist()
companyTickerMap = {}
for co in companiesToSearch:
    co = co.replace(" ", "-")
    currentUrl = f"https://financialmodelingprep.com/api/v3/search-name?query={co}&limit=10&apikey={fmp_api_key}"
    companyTickerMap[co] = get_jsonparsed_data(currentUrl)

pprint(companyTickerMap)
