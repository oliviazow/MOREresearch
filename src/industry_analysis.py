from pprint import pprint
import pandas as pd
import os
import datetime
import requests
from statistics import multimode
from main import layoffDataFullSimpl
from main import colnamesFull


def get_digit(number, n):
    return (number // 10**n) % 10


def get_naics(sic, digits=3):  # can only do 3 or 2
    naicsSicCrosswalkDf = pd.read_csv(r"%s\data\2022-NAICS-to-SIC-Crosswalk.csv" %
                                      os.path.normpath(os.path.join(os.getcwd(), os.pardir)), encoding="latin1")
    naicsSicCrosswalkDf.drop(naicsSicCrosswalkDf[naicsSicCrosswalkDf["Related SIC Code"] == "Aux"].index, inplace=True)
    naicsSicCrosswalkDf.drop(naicsSicCrosswalkDf[naicsSicCrosswalkDf["2017 NAICS Code"] == "Added"].index, inplace=True)
    naicsSicCrosswalkDf["Related SIC Code"] = [float(x) for x in naicsSicCrosswalkDf["Related SIC Code"]]
    naicsSicCrosswalkDf["2017 NAICS Code"] = [float(x) for x in naicsSicCrosswalkDf["2017 NAICS Code"]]

    i = 4  # find nonzero digits
    while i > 0:
        if get_digit(sic, i) == 0:
            i -= 1
        else:
            break

    sicFilteredDf = naicsSicCrosswalkDf[(naicsSicCrosswalkDf["Related SIC Code"] // (10 ** (4 - i))) ==
                                        (float(sic) // (10 ** (4 - i)))]
    if sicFilteredDf.empty:
        raise Exception("SIC Not Found")

    if digits == 3:
        naicsList = [x // (10 ** 3) for x in sicFilteredDf["2017 NAICS Code"]]
    else:
        naicsList = [x // (10 ** 4) for x in sicFilteredDf["2017 NAICS Code"]]

    return multimode(naicsList)


def get_and_add_naics():
    dictList = []
    sicList = layoffDataFullSimpl[layoffDataFullSimpl["sic"].notna()]["sic"].unique().tolist()
    for sic in sicList:
        try:
            dictEntry = dict(sic=int(sic), naics=get_naics(int(sic))[0])
        except Exception as e:
            dictEntry = dict(sic=int(sic), naics=None)
            print("Exception: " + str(e))
        dictList.append(dictEntry)
        print(sic)
    sicNaicsDf = pd.DataFrame.from_records(dictList)
    mergedDf = pd.merge(layoffDataFullSimpl, sicNaicsDf, how="left", on=["sic"])
    mergedDf = mergedDf[colnamesFull]
    print("cool")
    # mergedDf.to_csv(r"%s\data\layoffDataFullSimpl.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
    #                 index=False)


if "__main__" == __name__:
    get_and_add_naics()

