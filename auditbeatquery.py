
import subprocess
import json
from pandas.io.json import json_normalize
from summarizeDataFrame import summarizeDataset
from datetime import datetime

import pandas as pd

# Initialized variables

import search_elastic as se

# Initialized variables

elasticdatetimecolumn = '_source.@timestamp'

data = se.search_elastic('auditbeat-6.2.2-2018.02.27')

d = pd.DataFrame(json_normalize(data))
print(pd.DataFrame.keys(d))

df = pd.DataFrame(d['hits.hits'])
print(df)

totalHits=str(data['hits']['total'])

print("Total Hits: ",totalHits )
print("Total Recieved",len(df) ,'\n')


# Convert timestamp to date time to sort by datetime

df['DateTime'] =pd.to_datetime(df[elasticdatetimecolumn])

df.sort_values(by=['DateTime'],inplace = True)



#print(df2.dtype)
#summarizeDataset(df)
print(df.head())

#print(df['DateTime'])
#

#df.to_csv("/home/david/Desktop/new.csv" , sep='\t' , index=False)







#
#     df = json_normalize(item['hits']['hits'])

#print(json_normalize(item['hits']['hits']))
# new=json_normalize(data['responses'])
# data1 = pd.read_json(string,typ='index')
#res['hits']['hits']
#df = pd.concat(map(pd.DataFrame.from_dict, data), axis=1)['hits'].T
#print(new.head())
