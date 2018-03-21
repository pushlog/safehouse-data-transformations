
import subprocess
import json
from pandas.io.json import json_normalize
from summarizeDataFrame import summarizeDataset
from datetime import datetime

import pandas as pd
from time import strptime
import search_elastic as se

# Initialized variables

elasticdatetimecolumn = '_source.@timestamp'

data = se.search_elastic('domoticz-2018-03-19')
print(json.dumps(data, indent=4))

df=json_normalize(data['hits']['hits'])

totalHits=str(data['hits']['total'])

print("Total Hits: ",totalHits )
print("Total Recieved",len(df) ,'\n')

df2=df['_id']

df3 = df2.str.split(' ', expand=True)

#df4 = df3.ix[:, 1]

#df3.ix[:, 1]=df3.ix[:, 1].apply(lambda x: strptime(x,'%b').tm_mon)

#df['time'] = pd.to_datetime(df['hour'].astype(int).astype(str)+':'+df['min'].astype(int).astype(str)+':'+df['sec'].astype(int).astype(str), format = '%H:%M:%S').dt.time

#df3['time'] = pd.to_datetime(df3.ix[:, 1].astype(int).astype(str) , format = '%H').dt.time


# Convert timestamp to date time to sort by datetime
#
# df['DateTime'] =pd.to_datetime(df[elasticdatetimecolumn])
#
# df.sort_values(by=['DateTime'],inplace = True)
#
#
#
# #print(df2.dtype)
summarizeDataset(df3)
print(df3)

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
