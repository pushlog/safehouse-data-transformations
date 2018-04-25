import argparse, subprocess
import json, os, time
from pandas.io.json import json_normalize
from summarizeDataFrame import summarizeDataset
from datetime import datetime

import numpy as np
import pandas as pd
from time import strptime
import search_elastic as se

# Initialized variables

elasticdatetimecolumn = '_source.timestamp'

# JSON Query

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

parser = argparse.ArgumentParser(description="Pass inputs")
parser.add_argument("-start","--start_date", required=True,
    help="start date in MM/dd/yyyy format, -start=MM/dd/yyyy")
parser.add_argument("-end","--end_date", required=True,
    help="end date in MM/dd/yyyy format, -end=MM/dd/yyyy")
args = vars(parser.parse_args())

start_date = args["start_date"]
end_date = args["end_date"]


#body = {"query": {"range": {"timestamp": {"gte": "now-1d/d", "lt": "now/d"}}}}

total_row_count = 0

def fetch_data(start=start_date, end=end_date):
    global total_row_count

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


    # Elasticsearch instance
    data = se.search_elastic('gammarf', query=body)

    # Store data to dataframe
    d = pd.DataFrame(json_normalize(data))

    #Number of transactions
    totalT=int(len(d))

    if totalT == 1:
        df = json_normalize(d.ix[0, 'hits.hits'])
    else:
        # Append hits to dataframe
        df = pd.DataFrame([])

        for x in range(0, totalT - 1):
            ed=json_normalize(d.ix[x, 'hits.hits'] )
            df = df.append(ed)

    # Garbarge collect dataframe

    #del d

    #
    df['DateTime'] =pd.to_datetime(df[elasticdatetimecolumn])
    df.sort_values(by=['DateTime'],inplace = True)

    print('\n',"Total Transactions:",totalT ,'\n')
    total_row_count += df.shape[0]
    print("Total Rows:",total_row_count ,'\n')

    ## Save data
    colnames = df.columns.values.tolist()

    start_time_datasave = time.time()

    # save column names
    cols2save = os.path.join("data/raw","gamma_df_colnames.npy")
    np.save(cols2save, colnames)

    file2save = os.path.join("data/raw","gamma_df_" + str(total_row_count) + ".npy")
    np.save(file2save, df)

    print("Time taken to save data (mins): ", (time.time() - start_time_datasave) / 60)

    # #df.to_csv("/home/david/Desktop/new.csv" , sep='\t' , index=False)
    #
    #

start_time = time.time()
while total_row_count <= 1000000:
    print("Current row count: ", total_row_count)
    fetch_data(start_date, end_date)

print("Total Time taken in (mins): ", (time.time() - start_time) / 60)
