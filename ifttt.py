import argparse, subprocess
import json, os, time
from pandas.io.json import json_normalize
from summarizeDataFrame import summarizeDataset
from datetime import datetime , timedelta

import numpy as np
import pandas as pd
from time import strptime
import search_elastic as se
import sys


# Initialized variables

elasticdatetimecolumn = '_source.timestamp'

parser = argparse.ArgumentParser(description="Pass inputs")
parser.add_argument("-start","--start_date", required=False, default=None,
    help="start date in MM/dd/yyyy format, -start=MM/dd/yyyy")
parser.add_argument("-end","--end_date", required=False, default=None,
    help="end date in MM/dd/yyyy format, -end=MM/dd/yyyy")
args = vars(parser.parse_args())

try:
    start_date = args["start_date"]
    end_date = args["end_date"]
except KeyError:
    start_date, end_date = None, None

# JSON Query
#
# body = {
#     "query": {
#         "range" : {
#             "timestamp" : {
#                 "gte" : "now-4d",
#                  "lt" :  "now/d"
#             }
#         }
#     }
# }

#body = {"query": {"range": {"timestamp": {"gte": "now-1d/d", "lt": "now/d"}}}}

def fetch_data(start=start_date, end=end_date):
    body = {
    	"query": {
        	"range": {
                "timestamp": {
                    "gte": start, #"gte": "01/01/2018", format=MM/dd/yyyy
                    "lte": end, #"lte": "04/09/2018", format=MM/dd/yyyy
                    "format": "MM/dd/yyyy"
                }
            }
        }
    }
    if start and end:
        # Elasticsearch instance
        data = se.search_elastic('ifttt*', query=body)
    else:
        data = se.search_elastic('ifttt*')



    # Store data to dataframe
    d = pd.DataFrame(json_normalize(data))



    #Number of transactions

    totalT=int(len(d))



    if totalT == 1:

        df = json_normalize(d.ix[0, 'hits.hits'])
    else:


    # Append hits to dataframe
        df = pd.DataFrame([])

        for x in range( 0 , totalT - 1) :

            ed=json_normalize(d.ix[x, 'hits.hits'] )
            df = df.append(ed)

    # Garbarge collect dataframe

    #del d

    #
    df['DateTime'] =pd.to_datetime(df[elasticdatetimecolumn])
    df.sort_values(by=['DateTime'],inplace = True)

    # Convert to EST
    df['DateTime'] = df['DateTime'] - timedelta(hours=4)


    print('\n',"Total Transactions:",totalT ,'\n')
    print("Total Rows:",len(df) ,'\n')

    print(df.tail())
    #summarizeDataset(df)


    df.to_csv("ifttt.csv", index=False)
    sys.modules[__name__].__dict__.clear()


fetch_data(start_date, end_date)

    #
    #
    # #
    #
    # #df.to_csv("/home/david/Desktop/new.csv" , sep='\t' , index=False)
    #
    #
