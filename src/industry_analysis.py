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
    # print("cool")
    mergedDf.to_csv(r"%s\data\layoffDataFullSimpl.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
                    index=False)


def clean_aiie(digits=3):
    aiieDf = pd.read_csv(r"%s\data\aiie.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
    aiieDf[f"NAICS {digits} Digits"] = [float(x) // (10 ** (4 - digits)) for x in aiieDf["NAICS"]]
    return aiieDf.groupby(by=[f"NAICS {digits} Digits"])[["AIIE"]].mean().reset_index()


def assemble_industry_dataset(digits=3):
    aiieDf = clean_aiie(digits)
    if digits == 2:
        layoffDataFullSimpl["naics"] = [float(x) // 10 for x in layoffDataFullSimpl["naics"]]
    else:
        layoffDataFullSimpl["naics"] = [float(x) for x in layoffDataFullSimpl["naics"]]
    layoffsDfCount = layoffDataFullSimpl.groupby(by=["naics"])[["Company"]].count().reset_index()
    layoffsDfCompanySize = layoffDataFullSimpl.groupby(by=["naics"])[["Dollars Raised (mil)"]].mean().reset_index()
    layoffsDf = pd.merge(layoffsDfCount, layoffsDfCompanySize, how="left", on="naics")
    mergedDf = pd.merge(layoffsDf, aiieDf, how="left", left_on="naics", right_on=f"NAICS {digits} Digits")
    mergedDf.to_csv(fr"%s\data\industryExposure{digits}Digit.csv" % os.path.normpath(
                                       os.path.join(os.getcwd(), os.pardir)), index=False)
    return mergedDf


if "__main__" == __name__:
    print(clean_aiie())
    print(assemble_industry_dataset(3))
    print(assemble_industry_dataset(2))
    # assemble_industry_dataset(3).to_csv(r"%s\data\industryExposure3Digit.csv" % os.path.normpath(
    #                                    os.path.join(os.getcwd(), os.pardir)), index=False)
    # assemble_industry_dataset(2).to_csv(r"%s\data\industryExposure2Digit.csv" % os.path.normpath(
    #     os.path.join(os.getcwd(), os.pardir)), index=False)
