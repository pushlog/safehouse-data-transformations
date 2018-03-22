
import subprocess
import json
from pandas.io.json import json_normalize
from summarizeDataFrame import summarizeDataset
from datetime import datetime
import pandas as pd
import search_elastic as se

# Initialized variables

elasticdatetimecolumn = '_source.@timestamp'


# Query ElasticSearch with Scroll
data = se.search_elastic('auditbeat-6.2.2-2018.02.27')


# Store data to dataframe
d = pd.DataFrame(json_normalize(data))

# Number of transactions

totalT=int(len(d))

if totalT == 1:

    df = json_normalize(d.ix[0, 'hits.hits'])
else:


# Append hits to dataframe
    df = pd.DataFrame([])

    for x in range( 0 , totalT - 1) :

        ed=json_normalize(d.ix[x, 'hits.hits'] )
        df = df.append(ed)

#Garbage collect dataframe
del d

# Convert timestamp to date time to sort by datetime
df['DateTime'] =pd.to_datetime(df[elasticdatetimecolumn])
df.sort_values(by=['DateTime'],inplace = True)



# View Meta Data
print('\n',"Total Transactions:",totalT ,'\n')
print("Total Rows:",len(df) ,'\n')
print(df.head())


#summarizeDataset(df)

#

#df.to_csv("/home/david/Desktop/new.csv" , sep='\t' , index=False)



