from pprint import pprint
import pandas as pd
import os
import datetime
import yahooquery as yq

layoffDatadf = pd.read_csv(r"%s\data\layoffData.csv" % os.path.normpath(os.path.join(os.getcwd(), os.pardir)))
layoffDataWReturnsdf = pd.read_csv(r"%s\data\layoffDataWithReturns.csv" % os.path.normpath(os.path.join(os.getcwd(),
                                                                                                        os.pardir)))
