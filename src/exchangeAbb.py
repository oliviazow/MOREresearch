import pandas as pd
import os

abb = {
    "AMEX": "American Stock Exchange",
    "AMS": "Euronext Amsterdam",
    "BAN": "Stock Exchange of Thailand",
    "BLN": "Borse Berlin",
    "BOM": "Bombay Stock Exchange",
    "BRU": "Euronext Brussels",
    "CME": "Chicago Mercantile Exchange",
    "DUS": "Boerse Dusseldorf",
    "EDA": "Xetra Stock Exchange",
    "EDP": "Eurex Exchange",
    "EUF": "Euronext Paris",
    "FCS": "Shenzhen Stock Exchange",
    "FCZ": "Shanghai Stock Exchange",
    "FRA": "Frankfurt Stock Exchange",
    "HAM": "Hamburg Stock Exchange",
    "HAN": "Hannover Stock Exchange",
    "HKSE": "Hong Kong Stock Exchange",
    "IDX": "Indonesia Stock Exchange",
    "JASDAQ": "JASDAQ Securities Exchange",
    "JNET": "Osaka Securities Exchange",
    "KAR": "Karachi Stock Exchange",
    "KUL": "Kuala Lumpur Stock Exchange",
    "MTF":	"MTS France SAS",
    "MUN":	"Borse Muenchen",
    "NAG":	"Nagoya Stock Exchange",
    "NASDAQ": "NASDAQ",
    "NSM":	"NASDAQ Global Select Market",
    "NSX":	"Australian Stock Exchange",
    "NYSE":	"New York Stock Exchange",
    "NYSEARC": "New York Stock Exchange Archipelago",
    "OSL":	"Oslo Stock Exchange",
    "OTCMKTS": "OTC Markets",
    "SEO":	"Korea Stock Exchange",
    "SIN":	"Singapore Stock Exchange",
    "SSO":	"Stockholm Stock Exchange",
    "STU":	"Boerse Stuttgart",
    "TAI":	"Taiwan Stock Exchange",
    "TOK":	"Tokyo Stock Exchange",
    "TSX": "Toronto Stock Exchange",
    "VIE":	"Vienna Stock Exchange",
    "WEL":	"New Zealand Stock Exchange",
    "XETRA": "Deutsche Boerse AG",
    "XLON":	"London Stock Exchange",
    "XSTC":	"Hochiminh Stock Exchange",
    "XSWX":	"Six Swiss Exchange",
    "XVTX": "Six Swiss Exchange",
}
for key, value in abb.items():
    if "Euronext " in value:
        abb[key] = value.replace("Euronext ", "").strip()
    elif " Stock Exchange" in value:
        abb[key] = value.replace(" Stock Exchange", "").strip()

abb_reversed = dict(zip(abb.values(), abb.keys()))