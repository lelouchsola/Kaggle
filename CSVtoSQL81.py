
# coding: utf-8

# In[1]:

'''
Speed up insertion from pandas.DataFrame .to_sql.
Please refer to: https://stackoverflow.com/questions/52927213/how-to-speed-up-insertion-from-pandas-dataframe-to-sql
'''
from pandas.io.sql import SQLTable

def _execute_insert(self, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))

SQLTable._execute_insert = _execute_insert


# In[1]:


import pandas as pd
import os
import pymssql
from datetime import datetime
from sqlalchemy import create_engine
import shutil
import sys


# In[3]:


class DataPreprocessor():
    '''the original data is repartition. The first step is to merge them as one dataframe'''
    def __init__(self, name = 'Preprocess1'):
        self.name = name
    
    def LoadData(self, path):
        '''load data and merge them
           input:path, the file path
           return:df, the merged dataframe'''
        df = pd.DataFrame()
        '''load data, merge the repartition'''
        filenames = os.listdir(path)
        filenames.remove('_SUCCESS')
        for filename in filenames:
            file_temp = path + '/' + filename
            df_temp = pd.read_csv(file_temp, compression='gzip')
            df = pd.concat([df, df_temp], ignore_index = True)
        
        '''rename the column name'''
        df = df.rename(columns={'DESIGN_ID': 'DID', 'MACHINE_ID': 'Oven_ID'})
        return df
    
    def SIDInfoChange(self, df_sid):
        res = pd.DataFrame()
        groups = df_sid.groupby('NUMBER_OF_DIE_IN_PKG')
        for number, group in groups:
            if number != 1:
                for i in range(number):
                    df_temp = group.copy()
                    df_temp['SUMMARY_ID'] = df_temp['SUMMARY_ID'] + '_D' + str(i)
                    res = pd.concat([res, df_temp], ignore_index = True)
            else:
                res = pd.concat([res, group], ignore_index = True)
        return res


# In[4]:


'''Data Preprocess, merge all gz file into one dataframe'''
DP = DataPreprocessor()

'''load SDP data'''
path = '/home/gechen/Project/BIBshading/perfect_touch/data/FFailInfo_update/'
df = DP.LoadData(path)

'''load multiple die data'''
path_multidie = '/home/gechen/Project/BIBshading/perfect_touch/data/FFailInfo_update_MultiDie/'
df_MultiDie = DP.LoadData(path_multidie)

'''load data related to summary ID'''
SIDInfo = pd.read_csv('/home/gechen/Project/BIBshading/perfect_touch/data/SummaryIDInfo/SummaryIDInfo.csv')
SIDInfo_modify = DP.SIDInfoChange(SIDInfo) # add _D0 or _D1 to multi-Die lots to align with the data in Table BIBShadingData


# In[2]:


'''create the engine to connect Python and SQL server'''
engine = create_engine('mssql+pymssql://burnloopguest:burnloop007@XAWSQL81.xacn.micron.com\XTMSSPROD81/BurnLoop')


# In[22]:


'''write the info related to Summary ID into SQL server'''
chunksize = 1000
for i in range(0, len(SIDInfo_modify), chunksize):
    SIDInfo_modify[i:i+chunksize].to_sql('BIBShading_SumIDInfo', con=engine, index=False, if_exists='append')
    print ('insert number:', i)


# In[6]:


'''append the update SDP data into the table "BIBshadingData" in SQL server '''
'''it costs much shorter time using loop to insert data into SQL server, '''
time1 = datetime.now()
chunksize = 1000
for i in range(0, len(df), chunksize):
    df[i:i+chunksize].to_sql('BIBShadingData', con=engine, index=False, if_exists='append')
    print ('insert number:', i)
time2 = datetime.now()
print ('to_Sql cost time:', str(time2 - time1))


# In[6]:


'''append the update multi_die data into the table "BIBshadingData" in SQL server '''
'''it costs much shorter time using loop to insert data into SQL server'''
time1 = datetime.now()
chunksize = 1000
for i in range(0, len(df_MultiDie), chunksize):
    df_MultiDie[i:i+chunksize].to_sql('BIBShadingData', con=engine, index=False, if_exists='append')
    print ('insert number:', i)
time2 = datetime.now()
print ('to_Sql cost time:', str(time2 - time1))



# In[3]:


'''write the update time into SQL server'''
updatetime = pd.Series(datetime.now())
updatetime.to_sql('BIBshadingData_UpdateTime', con=engine, index=False, if_exists='replace') # xxxxxx is table name in sql


# In[4]:


'''remove the original csv data saved in Singapore server'''
try:
    shutil.rmtree('/home/gechen/Project/BIBshading/perfect_touch/data/FFailInfo_update/') # drop the extracted SDP data
    shutil.rmtree('/home/gechen/Project/BIBshading/perfect_touch/data/FFailInfo_update_MultiDie/') # drop the extracted multi-die data
    print ('Temp data has been removed')
except:
    print ('Failed to remove data')


# In[5]:


sys.exit()


