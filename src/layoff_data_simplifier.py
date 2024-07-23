from pprint import pprint
import pandas as pd
import os
import datetime
from main import layoffDataFullSimpl
from main import layoffReasonCols

simplifierMap = {
    "Acute liquidity issues": "Liquidity and Cash Flow",
    "Preserve cash": "Liquidity and Cash Flow",
    "Difficulty collecting payments": "Liquidity and Cash Flow",
    "Insolvency": "Liquidity and Cash Flow",
    "Increasing Group cash demands": "Liquidity and Cash Flow",
    "Chase profitability": "Profitability and Revenue",
    "Cost challenges": "Profitability and Revenue",
    "Hit financial targets": "Profitability and Revenue",
    "Lower earnings": "Profitability and Revenue",
    "Slower growth": "Profitability and Revenue",
    "Slower relative growth": "Profitability and Revenue",
    "Cut costs": "Profitability and Revenue",
    "Investing ahead of revenue growth": "Profitability and Revenue",
    "Customer enrollment decisions/delays": "Profitability and Revenue",
    "Slower conversion": "Profitability and Revenue",
    "Net bookings": "Profitability and Revenue",
    "Debt refinancing": "Debt and Financing",
    "Failed to restructure debt/secure financing": "Debt and Financing",
    "Overleverage": "Debt and Financing",
    "Reduced capital availability/changing investor inclinations": "Debt and Financing",
    "Funding issues": "Debt and Financing",
    "Competition": "Competition",
    "Competition from generative AI": "Competition",
    "Loss of market share": "Competition",
    "Ad tracking crackdown": "Market Conditions",
    "Market conditions": "Market Conditions",
    "Price war": "Market Conditions",
    "Slow advertising market": "Market Conditions",
    "Slower housing market": "Market Conditions",
    "Remote work trends": "Market Conditions",
    "Industry pushback": "Industry Relations",
    "Delayed product launch": "Product and Production",
    "Disappointing product": "Product and Production",
    "Discontinuing product/service": "Product and Production",
    "Product needs significant investment": "Product and Production",
    "Product safety issues": "Product and Production",
    "Product shipment delays": "Product and Production",
    "Product too specialized": "Product and Production",
    "Production challenges": "Product and Production",
    "Reduced discovery-stage work": "Product and Production",
    "Reduced hardware availability": "Product and Production",
    "Failed study/clinical trial": "Product and Production",
    "Commercialization delays": "Product and Production",
    "Slower than expected technology development": "Product and Production",
    "Employee compensation system change": "Employee and HR",
    "Employee performance": "Employee and HR",
    "Reduced hiring": "Employee and HR",
    "Lack of local, senior leadership": "Employee and HR",
    "Promote in-person collaboration": "Employee and HR",
    "Facility closure": "Facility and Infrastructure",
    "Manufacturing issues": "Facility and Infrastructure",
    "Office closures": "Facility and Infrastructure",
    "Relocate labor to lower-cost locations": "Facility and Infrastructure",
    "Relocating": "Facility and Infrastructure",
    "Move production to market": "Facility and Infrastructure",
    "Lower demand": "Capacity and Demand",
    "Overcapacity": "Capacity and Demand",
    "Partnership/agreement termination": "Partnership and Agreement",
    "New partnership/agreement": "Partnership and Agreement",
    "Regulatory challenges": "Regulatory and Legal",
    "Litigation": "Regulatory and Legal",
    "Lack of insurer coverage": "Regulatory and Legal",
    "Overhiring": "Rapid Expansion",
    "Scaled too fast": "Rapid Expansion",
    "Deprioritize regional market": "Regional Market Prioritization",
    "Leaving regional market(s)": "Regional Market Prioritization",
    "Prioritize regional market": "Regional Market Prioritization",
    "Geopolitical conflict": "Geopolitical and Macroeconomic Conditions",
    "Macroeconomic conditions": "Geopolitical and Macroeconomic Conditions",
    "Supply chain issues": "Geopolitical and Macroeconomic Conditions",
    "Change in government incentives": "Geopolitical and Macroeconomic Conditions",
    "Post-acquisition restructuring": "M&A Activities",
    "Post-asset sale restructuring": "M&A Activities",
    "Pre-acquisition restructuring": "M&A Activities",
    "Sell business": "M&A Activities",
    "Failure to sell company": "M&A Activities"
}

simplifierMapLvl2 = {
    "Liquidity and Cash Flow": "Financial Issues",
    "Profitability and Revenue": "Financial Issues",
    "Debt and Financing": "Financial Issues",
    "Competition": "Market and Competition",
    "Market Conditions": "Market and Competition",
    "Industry Relations": "Market and Competition",
    "Automation": "Operational Challenges",
    "Product and Production": "Operational Challenges",
    "Employee and HR": "Operational Challenges",
    "Facility and Infrastructure": "Operational Challenges",
    "Capacity and Demand": "Operational Challenges",
    "Partnership and Agreement": "Operational Challenges",
    "Regulatory and Legal": "Operational Challenges",
    "Optimization": "Operational Challenges",
    "Rapid Expansion": "Operational Challenges",
    "Regional Market Prioritization": "Strategic Focus",
    "Focus on AI": "Strategic Focus",
    "Definite Business Realignment": "Strategic Focus",
    "Business realignment": "Strategic Focus",
    "Geopolitical and Macroeconomic Conditions": "External Factors",
    "Covid-19": "External Factors",
    "M&A Activities": "Organizational Changes",
    "FTX Collapse": "Specific Incidents"
}

simplifierMap = dict((k.lower(), v.lower()) for k, v in simplifierMap.items())
simplifierMapLvl2 = {k.lower(): v.lower() for k, v in simplifierMapLvl2.items()}

if "__main__" == __name__:
    for col in layoffReasonCols:
        layoffDataFullSimpl[col] = [simplifierMap[x.lower()] if x in simplifierMap else x for x in
                                    layoffDataFullSimpl[col]]
    pprint(layoffDataFullSimpl[layoffDataFullSimpl["Reason Mentioned 1"].notna()][layoffReasonCols])
    layoffDataFullSimpl.to_csv(r"%s\data\layoffDataFullSimpl.csv" % os.path.normpath(os.path.join(
                                os.getcwd(), os.pardir)), index=False)

    for col in layoffReasonCols:
        layoffDataFullSimpl[col] = [simplifierMapLvl2[x.lower()] if x in simplifierMapLvl2 else x for x in
                                    layoffDataFullSimpl[col]]
    pprint(layoffDataFullSimpl[layoffDataFullSimpl["Reason Mentioned 1"].notna()][layoffReasonCols])
    layoffDataFullSimpl.to_csv(r"%s\data\layoffDataFullVerySimpl.csv" % os.path.normpath(os.path.join(
                                os.getcwd(), os.pardir)), index=False)

