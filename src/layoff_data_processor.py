from pprint import pprint
import pandas as pd
import datetime
import os
from layoff_data import data

dataRows = data["data"]["table"]["rows"]


def prepare_data(data):
    dataColumns = data["data"]["table"]["columns"]
    dataLocationChoices = dataColumns[1]["typeOptions"]["choices"]
    dataIndustryChoices = dataColumns[5]["typeOptions"]["choices"]
    dataStageChoices = dataColumns[8]["typeOptions"]["choices"]
    dataCountryChoices = dataColumns[10]["typeOptions"]["choices"]

    # dictionary of column ids to column names
    columnIdNameMap = {}
    locationIdNameMap = {}
    industryIdNameMap = {}
    stageIdNameMap = {}
    countryIdNameMap = {}

    for col in dataColumns:
        columnIdNameMap[col["id"]] = col["name"]
    for _id, dct in dataLocationChoices.items():
        locationIdNameMap[_id] = dct['name']
    for _id, dct in dataIndustryChoices.items():
        industryIdNameMap[_id] = dct['name']
    for _id, dct in dataStageChoices.items():
        stageIdNameMap[_id] = dct['name']
    for _id, dct in dataCountryChoices.items():
        countryIdNameMap[_id] = dct['name']

    return columnIdNameMap, locationIdNameMap, industryIdNameMap, stageIdNameMap, countryIdNameMap


def test_data_preparation():
    columnIdNameMap, locationIdNameMap, industryIdNameMap, stageIdNameMap, countryIdNameMap = prepare_data(data)
    pprint(columnIdNameMap)
    pprint(locationIdNameMap)
    pprint(industryIdNameMap)
    pprint(stageIdNameMap)
    pprint(countryIdNameMap)


def convert2dict(data):
    columnIdNameMap, locationIdNameMap, industryIdNameMap, stageIdNameMap, countryIdNameMap = prepare_data(data)
    converted_data = []
    for layoffDict in dataRows:
        dct = layoffDict["cellValuesByColumnId"]
        record = {}
        for _id, val in dct.items():
            if 'Company' == columnIdNameMap[_id]:
                record['Company'] = val
            elif 'Location HQ' == columnIdNameMap[_id]:
                if len(val) > 1:
                    record["IsUS"] = False
                else:
                    record["IsUS"] = True
                record["Location HQ"] = locationIdNameMap[val[0]]
            elif "Industry" == columnIdNameMap[_id]:
                record["Industry"] = industryIdNameMap[val]
            elif "# Laid Off" == columnIdNameMap[_id]:
                record["Number Laid Off"] = val
            elif "%" == columnIdNameMap[_id]:
                record["Percentage"] = val
            elif "Date" == columnIdNameMap[_id]:
                record["Date of Layoff"] = datetime.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S.%fZ").date()
            elif "Source" == columnIdNameMap[_id]:
                record["Source"] = val
            elif "$ Raised (mm)" == columnIdNameMap[_id]:
                record["Dollars Raised (mil)"] = val
            elif "List of Employees Laid Off" == columnIdNameMap[_id]:
                record["List of Employees Laid Off"] = val
            elif "Stage" == columnIdNameMap[_id]:
                record["Stage"] = stageIdNameMap[val]
            elif "Country" == columnIdNameMap[_id]:
                record["Country"] = countryIdNameMap[val]
            else:
                pass
        converted_data.append(record)
    return converted_data


def convert2df(data):
    return pd.DataFrame.from_records(convert2dict(data))


if '__main__' == __name__:
    df = convert2df(data)
    df["Company"] = df["Company"].apply(lambda x: x.strip())
    df.to_csv(r"%s\data\layoffData.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)), index=False)

    publicCos = df[df["Stage"] == "Post-IPO"][["Company"]].drop_duplicates()
    publicCos.to_csv(r"%s\data\publicCompanies.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
                     index=False)
