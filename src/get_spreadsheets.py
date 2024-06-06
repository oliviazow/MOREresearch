import gspread
import os
import csv
import pandas as pd
from pprint import pprint
from main import layoffDataWReturnsdf

gc = gspread.oauth(
    credentials_filename=r"%s\docs\credentials.json" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)),
    authorized_user_filename=r"%s\docs\authorized_user.json" % os.path.normpath(os.path.join(os.getcwd(), os.pardir))
)

employeeListsdf = layoffDataWReturnsdf[["Company", "Date of Layoff", "List of Employees Laid Off"]].dropna()
companyListMap = employeeListsdf.to_dict("records")

# test = {"Indeed": "https://docs.google.com/spreadsheets/d/11pw38vkuXKudOZs0YiXBFnOS2tQ476pg3FA_qTLLM0k/edit#gid=0"}


def download_gsheets(sheets):
    downloaded = 0
    failed = 0
    for item in sheets:
        url = item["List of Employees Laid Off"]
        if "docs.google.com/spreadsheets" in url:
            coName = item["Company"]
            date = item["Date of Layoff"]
            filepath = f"%s\\docs\\employee_lists\\{coName}_{date}\\" % os.path.normpath(os.path.join(os.getcwd(),
                                                                                                      os.pardir))
            if os.path.exists(filepath):
                if len(os.listdir(filepath)) > 0:
                    downloaded += 1
                    continue
            try:
                sh = gc.open_by_url(url)
                for j, worksheet in enumerate(sh.worksheets()):
                    if not os.path.exists(filepath):
                        os.makedirs(filepath)
                    filename = coName + "-worksheet" + str(j) + ".csv"
                    with open(filepath + filename, "w", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerows(worksheet.get_all_values())
                downloaded += 1
                print(f"Number {str(downloaded)}: " + coName + " done!")
            except PermissionError:
                print("Error: Permission denied")
                failed += 1
            except gspread.exceptions.SpreadsheetNotFound:
                print("Error: Spreadsheet not found")
                failed += 1

    return failed


if "__main__" == __name__:
    pprint(companyListMap)
    print(download_gsheets(companyListMap))
    # download_gsheets(companyListMap)
