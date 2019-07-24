
# coding: utf-8



# In[1]:


'''import necessary lib, add useful path into system path'''
import os
import sys

module_path = '/home/gechen/Project/BIBshading/perfect_touch'

if module_path not in sys.path:
    sys.path.append(module_path)
    
import re
import subprocess
from datetime import datetime, timedelta

import pandas as pd
import numpy as np


# In[2]:


def concat_filter(filter_dict):
    """ 
    @para: dict
    @return: string

    concat the filter dict into strings 
    """

    filters=''
    if not isinstance(filter_dict, dict):
        raise TypeError("The input type should be dict")
    
    for k,v in filter_dict.items():
        filters += " "
        if isinstance(v,str):
            filters += k + "=" + v
        elif isinstance(v,list):
            filters += k + "=" + ",".join(v)
        else:
            raise TypeError("The jsums filter dict value should only be list or str")

    return filters.strip()

def LastThurday(today):
    """
    @para: today's datetime
    @return: last Thurday's datetime
    
    get the last Thurday's datetime
    """
    offset = (today.weekday() - 3) % 7
    return today - timedelta(days=offset)

def TTENDXQCommand(DesignID, StartTime, EndTime, FormatCol, Filters, MyStep, FilePath):
    """
    @para:
    1. DesignID: chosen DID 
    2. StartTime: data start time
    3. EndTime: data end time
    4. FormatCol: columns you want to return. For example, 'HDFS_PATH','NUMBER_OF_DIE_IN_PKG'
    5. Filters: filters you want to use. For example, "STANDARD_FLOW", "TEST_FACILITY"
    6. MyStep: chosen step
    7. FilePath: file path for saving the parquet path file
    @return:
    TTEDXQ: TTEDXQ command
    """
    TTENDXQ_CMD="/home/pesoft/hadoop/bin/ttendxq.1.0.0-SNAPSHOT -design={DID} -start={start_datetime} -end={end_datetime} -format={cols} {filters} -step={MyStep} > {filepath}"
    TTEDXQ = TTENDXQ_CMD.format(DID = DesignID, start_datetime = StartTime, end_datetime = EndTime, cols = ','.join(FormatCol), filters = concat_filter(Filters), MyStep = MyStep, filepath = FilePath)
    return TTEDXQ

APPNAME = "perfect touch" # decide the app name

'''define the input parameters of TTENDXQCommand'''
DIDList=['Z11B', 'Z21C']
StartTime = LastThurday(datetime.today() - timedelta(7)).strftime("%Y-%m-%d") + "T19:00:00" #get the Thursday datetime before last week (上上周四) as the start time
EndTime =LastThurday(datetime.today()).strftime("%Y-%m-%d") + "T18:59:59" # get the last Thursday datetime as the end time
FormatCol = ['HDFS_PATH','NUMBER_OF_DIE_IN_PKG', 'FAB_FACILITY_CODE', 'CONFIGURATION_WIDTH', 'SERVER_FLOW_REQD', 'PRODUCT_GRADE', 'VERSION','SUMMARY_ID', 'MFG_WORKWEEK'] # columns you want to return
Filters = {"STANDARD_FLOW": "YES", "TEST_FACILITY": "XIAN"} # filters
MyStep = 'BURN1' # chose step
FilePath = '/home/gechen/Project/BIBshading/perfect_touch/temp/parquet' # file path for saving the parquet path file


# In[3]:


"""
TTEDXQ NOTE: At the moment, you can only do one design at a time with the -design option. Functionality to handle multiple designs is planned for the next release. 
So use a loop to get parquet path info of multiple DID
"""
Index_DID = 0
parquet_df_SDP = pd.DataFrame() # save SPD parquet info
parquet_df_MultiDie = pd.DataFrame() # save Multiple die parts
parquet_df_Allinfo = pd.DataFrame() # save all the info related with Summary ID

for DID in DIDList:
    FilePath_temp = FilePath + str(Index_DID) # define a different saving path for different DIDs
    Index_DID += 1
    TTENDXQ_temp = TTENDXQCommand(DID, StartTime, EndTime, FormatCol, Filters, MyStep, FilePath_temp) # return TTENDXQ command for each DID
    print (TTENDXQ_temp)
    subprocess.call(TTENDXQ_temp, stderr=subprocess.STDOUT, shell=True) # use TTENDXQ to get different DIDs' parquet file paths
    ParqueDf_temp = pd.read_csv(FilePath_temp, header=0) # read the parquet file path csv of one DID

    parquet_df_Allinfo = pd.concat([parquet_df_Allinfo,ParqueDf_temp]) # collect all DIDs' info related to summary ID
    
    '''only consider the SDP lot'''
    Parquet_SDP_temp = ParqueDf_temp[ParqueDf_temp['NUMBER_OF_DIE_IN_PKG'] == 1]
    parquet_df_SDP = pd.concat([parquet_df_SDP, Parquet_SDP_temp])
    
    '''Multiple die parts'''
    Parquet_MultiDie_temp = ParqueDf_temp[ParqueDf_temp['NUMBER_OF_DIE_IN_PKG'] != 1]
    parquet_df_MultiDie = pd.concat([parquet_df_MultiDie, Parquet_MultiDie_temp])

'''convert the dataframe into list so that it can be used in the later TTEQCMD'''
parquet_list_SDP = parquet_df_SDP['HDFS_PATH'].tolist()
parquet_list_SDP = list(set(parquet_list_SDP)) # drop the duplicate file path


# In[11]:

'''save the information related to Summary ID, save the multiple die parquet file paths'''
parquet_df_Allinfo = parquet_df_Allinfo[['SUMMARY_ID', 'MFG_WORKWEEK','NUMBER_OF_DIE_IN_PKG', 'FAB_FACILITY_CODE', 'CONFIGURATION_WIDTH', 'SERVER_FLOW_REQD', 'PRODUCT_GRADE', 'VERSION']] # change the order of columns
parquet_df_Allinfo.to_csv('/home/gechen/Project/BIBshading/perfect_touch/data/SummaryIDInfo/SummaryIDInfo.csv', index=False)
parquet_df_MultiDie[['SUMMARY_ID', 'NUMBER_OF_DIE_IN_PKG', 'HDFS_PATH']].to_csv('/home/gechen/Project/BIBshading/perfect_touch/data/MultiDiePath/MultiDiePath.csv', index=False)


# In[4]:


"""start sparksession"""
from libs.StartSpark import StartSpark

spark=StartSpark('perfect_touch')
sparksession=spark.start_sparksession()


# In[5]:


'''import necessary pyspark lib for UDF'''
from pyspark.sql.types import ArrayType, StringType, DataType, StructType, StructField, IntegerType, BooleanType
from pyspark.sql.functions import udf,explode,col
from libs.StartTTEQ import StartTTEQ
from pyspark.sql.functions import split, substring, expr, when
from pyspark.sql.functions import collect_list,trim


"""
define functions for selecting BIB info
"""
def GetJTS(array):
    """
    @para:array, array in pyspark dataframe
    @return, array, only contain the elements with even index, note that the first element' index is 0, even.
    
    use to get JTS ID
    """
    return array[::2]
    
def GetSBT(array):
    """
    @para:array, array in pyspark dataframe
    @return, array, only contain the elements with odd index, note that the first element' index is 0, even.
    
    use to get SBT ID
    """
    return array[1::2]

def GetBIBType(array):
    """
    @para:array, array in pyspark dataframe
    @return, array, only contain the elements with even index, note that the first element' index is 0, even.
    
    use to get BIB type
    """
    return array[::2]

def SiteStatusReplace(x):
    return when(col(x) == True, 1).otherwise(0)

"""define UDFs with the functions above"""
JTSQuery = udf(lambda x: GetJTS(x), ArrayType(StringType()))
SBTQuery = udf(lambda x: GetSBT(x), ArrayType(StringType()))
BIBTypeQuery = udf(lambda x: GetBIBType(x), ArrayType(StringType()))

"""use zip() to define UDF"""
zip_ = udf(lambda x1, x2, x3, x4, x5: list(zip(x1, x2, x3, x4, x5)), ArrayType(StructType([StructField("1st", StringType()),
                                                                                                   StructField("2nd", StringType()),
                                                                                                   StructField("3rd", StringType()),
                                                                                                   StructField("4th", StringType()),
                                                                                                   StructField("5th", StringType())])))
"""
define UDFs to convert sitestatus to hexadecimal format
"""
def SiteStatus_BinToHex(array):
    HexRec = []
    for i in range(int(len(array)/4)):
        BinRec = ['0b']
        for ii in range(4):
            if array[i*4+ii] == 0:
                BinRec.append('0')
            else:
                BinRec.append('1')
        BinRec = "".join(BinRec)
        HexTemp = hex(int(BinRec,2))[-1:]
        HexRec.append(HexTemp)
    HexRec = "".join(HexRec)
    return HexRec
GetBinToHex = udf(lambda x: SiteStatus_BinToHex(x), StringType())



# In[6]:


"""
prepare TTEQ command for BIB info.
"""
TTEQ_FORMAT_ATTRS =['SUMMARY_ID','BURN_BOARD_DATA'] # section attributes                    
datafiles = ','.join(parquet_list_SDP) 
section = 'BURN_BOARD'  # BIB info is recorded BurnBoard session
format_attrs = ','.join(TTEQ_FORMAT_ATTRS)
tteq_cmd =["-data="+datafiles,"%section="+section,"-format="+format_attrs]


# In[7]:


"""
The BIB info we need include: 1. BIBtype, 2.BIB ID, 3. JTS ID, 4. SBT ID, 5.SLOT POSITION
the original BIB info is shown like this
+---------------+---------------------------------+
|SUMMARY_ID     |BURN_BOARD_DATA                  |
+---------------+---------------------------------+
|DQV/F1/B2/31UNU|$$_BEGIN_BOARD                   |
|DQV/F1/B2/31UNU|ZONE:                 4          |
|DQV/F1/B2/31UNU|SLOT:                 0          |
|DQV/F1/B2/31UNU|SITE_STRING_KEY:      P17        |
|DQV/F1/B2/31UNU|ZONE_SLOT:            4-0        |
|DQV/F1/B2/31UNU|SLOT_NO:              16         |
|DQV/F1/B2/31UNU|SLOT_POSITION:        P17        |
|DQV/F1/B2/31UNU|STATUS:               valid      |
|DQV/F1/B2/31UNU|PARTIAL_BOARD:        NO         |
|DQV/F1/B2/31UNU|ID:                   48771214   |
|DQV/F1/B2/31UNU|TYPE:                 6048       |
|DQV/F1/B2/31UNU|NUM_COLUMNS:          16         |
|DQV/F1/B2/31UNU|NUM_ROWS:             20         |
|DQV/F1/B2/31UNU|NUM_SITES:            320        |
|DQV/F1/B2/31UNU|MPN:                  594-12084  |
|DQV/F1/B2/31UNU|SIG_NUM:              13502      |
|DQV/F1/B2/31UNU|SIG_REV:              C          |
|DQV/F1/B2/31UNU|SIG_SERIAL_NUM:       621171/035 |
|DQV/F1/B2/31UNU|SLOT_ID:              1417329/228|
|DQV/F1/B2/31UNU|MIDPLANE_ID:          624945/047 |
+---------------+---------------------------------+

In this chart, between "$$_BEGIN_BOARD" and "$$_END_BOARD", we can find:
    "TYPE": BIB type
    "ID": BIB ID
    "SLOT_POSITION": SLOT POSITION
but we can also find:
There are two ATTR called "MACHINE_ID " in one single BIB info, the first one is JTS ID, the second one is SBT ID.
There are two ATTR called "TYPE" in one single BIB info, the first one is BIB type, the second one is socket type.

"""
tteq = StartTTEQ(sparksession) # define a tteq object
df_BIBinfo = tteq.start_query(tteq_cmd, section) # use tteq command to get all data related to BIB


# In[8]:



'''
split BURN_BOARD_DATA into two columns by ":", 1. ATTR (before :), 2. VALUE (after :)
+---------------+------------------------------+---------------+---------------------------+
|SUMMARY_ID     |BURN_BOARD_DATA               |ATTR           |VALUE                      |
+---------------+------------------------------+---------------+---------------------------+
|DQV/F1/B2/31UNU|$$_BEGIN_BOARD                |$$_BEGIN_BOARD |null                       |
|DQV/F1/B2/31UNU|ZONE:                 4       |ZONE           |                 4         |
|DQV/F1/B2/31UNU|SLOT:                 0       |SLOT           |                 0         |
|DQV/F1/B2/31UNU|SITE_STRING_KEY:      P17     |SITE_STRING_KEY|      P17                  |
|DQV/F1/B2/31UNU|ZONE_SLOT:            4-0     |ZONE_SLOT      |            4-0            |
|DQV/F1/B2/31UNU|SLOT_NO:              16      |SLOT_NO        |              16           |
|DQV/F1/B2/31UNU|SLOT_POSITION:        P17     |SLOT_POSITION  |        P17                |
|DQV/F1/B2/31UNU|STATUS:               valid   |STATUS         |               valid       |
|DQV/F1/B2/31UNU|PARTIAL_BOARD:        NO      |PARTIAL_BOARD  |        NO                 |
|DQV/F1/B2/31UNU|ID:                   48771214|ID             |                   48771214|
+---------------+------------------------------+---------------+---------------------------+
'''
split_col = split("BURN_BOARD_DATA", ':')
df_BIBinfo = df_BIBinfo.withColumn("ATTR", split_col.getItem(0))
df_BIBinfo = df_BIBinfo.withColumn('VALUE', split_col.getItem(1))
#df_BIBinfo.show(10, False)


# In[9]:


'''
delete the blankspace in Value column, now the pyspark df is like this:
+---------------+---------------+--------+
|SUMMARY_ID     |ATTR           |Value   |
+---------------+---------------+--------+
|DQV/F1/B2/31UNU|$$_BEGIN_BOARD |null    |
|DQV/F1/B2/31UNU|ZONE           |4       |
|DQV/F1/B2/31UNU|SLOT           |0       |
|DQV/F1/B2/31UNU|SITE_STRING_KEY|P17     |
|DQV/F1/B2/31UNU|ZONE_SLOT      |4-0     |
|DQV/F1/B2/31UNU|SLOT_NO        |16      |
|DQV/F1/B2/31UNU|SLOT_POSITION  |P17     |
|DQV/F1/B2/31UNU|STATUS         |valid   |
|DQV/F1/B2/31UNU|PARTIAL_BOARD  |NO      |
|DQV/F1/B2/31UNU|ID             |48771214|
+---------------+---------------+--------+
'''
df_BIBinfo = df_BIBinfo.select('SUMMARY_ID', "ATTR", trim(col("VALUE")).alias('Value'))
#df_BIBinfo.show(10, False)


# In[10]:


"""
choose the ATTR we need, as previous says, there are different ATTRs with the same ATTR name.
The chart now is like this:
+---------------+-------------+------------+
|SUMMARY_ID     |ATTR         |Value       |
+---------------+-------------+------------+
|DQV/F1/B2/31UNU|SLOT_POSITION|P17         |
|DQV/F1/B2/31UNU|ID           |48771214    |
|DQV/F1/B2/31UNU|TYPE         |6048        |
|DQV/F1/B2/31UNU|TYPE         |402         |
|DQV/F1/B2/31UNU|MACHINE_ID   |JTS8700-0044|
|DQV/F1/B2/31UNU|MACHINE_ID   |SBT6600-0036|
|DQV/F1/B2/31UNU|SLOT_POSITION|P18         |
|DQV/F1/B2/31UNU|ID           |48430303    |
|DQV/F1/B2/31UNU|TYPE         |6048        |
|DQV/F1/B2/31UNU|TYPE         |402         |
+---------------+-------------+------------+
"""
df_BIBinfo = df_BIBinfo.filter(col('ATTR').isin(['SLOT_POSITION', 'ID', 'TYPE', 'MACHINE_ID']))
#df_BIBinfo.show(10, False)


# In[11]:


"""
group the data by summary ID (one summary ID one row),
then pivot the dataframe.
ID: all BIB ID in this lot
MACHINE_ID: all JTS ID and SBT ID in this lot (odd is JTS, even is SBT)
SLOT_POSITION: all slot position in this lot
TYPE: all BIB type and socket type in this lot (odd is BIB type, even is socket type)

+---------------+--------------------+--------------------+--------------------+--------------------+
|     SUMMARY_ID|                  ID|          MACHINE_ID|       SLOT_POSITION|                TYPE|
+---------------+--------------------+--------------------+--------------------+--------------------+
|DQG/2C/D2/31UNU|[55108656, 550993...|[JTS8700-0038, SB...|[P01, P02, P03, P...|[6046, 496, 6046,...|
|DQX/LZ/92/31UNU|[54788976, 548003...|[JTS8500-0030, SB...|[P01, P02, P03, P...|[6048, 402, 6048,...|
|DQF/GD/B2/31UNU|[50046342, 537365...|[JTS8700-0008, SB...|[P12, P13, P14, P...|[6048, 402, 6048,...|
|DQW/CH/C2/31UNU|[52194155, 540860...|[JTS8500-0051, SB...|[P02, P03, P04, P...|[6041, 491, 6041,...|
|DQG/3D/F2/31UNU|[50712981, 507156...|[JTS8700-0002, SB...|[P03, P04, P05, P...|[6048, 402, 6048,...|
|DQB/TG/B2/31UNU|[54779065, 484312...|[JTS8500-0012, SB...|[P01, P02, P03, P...|[6048, 402, 6048,...|
|DQS/QV/C2/31UNU|[49171815, 512871...|[JTS8700-0024, SB...|[P01, P02, P03, P...|[6048, 408, 6048,...|
|DQJ/HD/F2/31UNU|[53745005, 517674...|[JTS8700-0038, SB...|[P01, P02, P03, P...|[6048, 402, 6048,...|
|HD9/26/00/2BUNU|[53752751, 507752...|[JTS8700-0020, SB...|[P01, P02, P03, P...|[6048, 402, 6048,...|
|DQ2/YS/92/31UNU|[48815173, 506874...|[JTS8500-0022, SB...|[P14, P15, P16, P...|[6048, 402, 6048,...|

"""
df_BIBinfo = df_BIBinfo.groupby('SUMMARY_ID').pivot('ATTR').agg(collect_list("Value"))
#df_BIBinfo.show(10)


# In[12]:


"""
use UDF to get BIBType, JTS ID, SBT ID

+---------------+--------------------+--------------------+--------------------+--------------------+--------------------+
|     SUMMARY_ID|                  ID|       SLOT_POSITION|             BIBType|                 JTS|                 SBT|
+---------------+--------------------+--------------------+--------------------+--------------------+--------------------+
|DQG/2C/D2/31UNU|[55108656, 550993...|[P01, P02, P03, P...|[6046, 6046, 6046...|[JTS8700-0038, JT...|[SBT6600-0028, SB...|
|DQ1/8P/C2/31UNU|[51311077, 483492...|[P01, P02, P03, P...|[6048, 6048, 6048...|[JTS8700-0002, JT...|[SBT6600-0004, SB...|
|DQJ/HD/F2/31UNU|[53745005, 517674...|[P01, P02, P03, P...|[6048, 6048, 6048...|[JTS8700-0038, JT...|[SBT6600-0025, SB...|
|HD9/26/00/2BUNU|[53752751, 507752...|[P01, P02, P03, P...|[6048, 6048, 6048...|[JTS8700-0020, JT...|[SBT6600-0004, SB...|
|DQX/LZ/92/31UNU|[54788976, 548003...|[P01, P02, P03, P...|[6048, 6048, 6048...|[JTS8500-0030, JT...|[SBT6600-0030, SB...|
|DQS/QV/C2/31UNU|[49171815, 512871...|[P01, P02, P03, P...|[6048, 6048, 6048...|[JTS8700-0024, JT...|[SBT6600-0007, SB...|
|DQF/GD/B2/31UNU|[50046342, 537365...|[P12, P13, P14, P...|[6048, 6048, 6048...|[JTS8700-0008, JT...|[SBT6600-0042, SB...|
|DQV/KT/92/31UNU|[51298696, 517364...|[P06, P07, P08, P...|[6048, 6048, 6048...|[JTS8500-0022, JT...|[SBT6600-0039, SB...|
|DQW/CH/C2/31UNU|[52194155, 540860...|[P02, P03, P04, P...|[6041, 6041, 6041...|[JTS8500-0051, JT...|[SBT6600-0035, SB...|
|DQT/LB/F2/31UNU|[47528434, 491718...|[P01, P02, P03, P...|[6048, 6048, 6048...|[JTS8500-0018, JT...|[SBT6600-0025, SB...|
"""
df_BIBinfo = df_BIBinfo.select("SUMMARY_ID", "ID", "SLOT_POSITION", BIBTypeQuery("TYPE").alias('BIBType'),
                          JTSQuery("MACHINE_ID").alias('JTS'), 
                          SBTQuery("MACHINE_ID").alias('SBT'))
#df_BIBinfo.show(10)


# In[13]:


"""
use zip() to convert the dataframe, before is one Lot row, now is one BIB one row.
+---------------+--------+------------+-------+------------+------------+
|SUMMARY_ID     |BIBID   |SlotPosition|BIBType|JTSID       |SBTID       |
+---------------+--------+------------+-------+------------+------------+
|HD9/26/00/2BUNU|53752751|P01         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|50775295|P02         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|50030779|P03         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|54786401|P04         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|50073920|P05         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|50687440|P06         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|53746514|P07         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|46712513|P08         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|48773868|P09         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|50743293|P10         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|51754197|P11         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|50788839|P12         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|50702342|P13         |6048   |JTS8700-0020|SBT6600-0004|
|HD9/26/00/2BUNU|51310998|P14         |6048   |JTS8700-0020|SBT6600-0004|

Now all the operations on BurnBoard session are completed.
"""
BIB_info = df_BIBinfo.withColumn("tmp", zip_("ID", "SLOT_POSITION", "BIBType", "JTS", "SBT")).withColumn("tmp", explode("tmp")).select("SUMMARY_ID",col("tmp.1st").alias("BIBID"), 
                                                                                                            col("tmp.2nd").alias("SlotPosition"),
                                                                                                            col("tmp.3rd").alias("BIBType"),
                                                                                                          col("tmp.4th").alias("JTSID"),
                                                                                                          col("tmp.5th").alias("SBTID"))

#BIB_info.show(20, False)


# In[14]:


"""
start to extract first fail info
"""
"""
Original data:
SUMMARY_ID: summary ID,
FID: Fuse ID
Site: The location of each component, first three charaters are slot position, last three are the location in BIB
DESIGN_ID: DID
MACHINE_ID: O

+---------------+--------------------+------+---------+-----------+--------------------+-----------+------------+
|     SUMMARY_ID|                 FID|  SITE|DESIGN_ID| MACHINE_ID|      DEF_FIRST_FAIL|SITE_STATUS|MFG_WORKWEEK|
+---------------+--------------------+------+---------+-----------+--------------------+-----------+------------+
|DQ1/8P/C2/31UNU|076271B:24:P08:05...|P14G11|     Z11B|AX5601-0060|                    |       true|      201925|
|DQ1/8P/C2/31UNU|076271B:22:P10:10...|P11I18|     Z11B|AX5601-0060|                    |       true|      201925|
|DQ1/8P/C2/31UNU|076271B:16:P04:48...|P23B17|     Z11B|AX5601-0060|                    |       true|      201925|
|DQ1/8P/C2/31UNU|076271B:16:P06:21...|P09P20|     Z11B|AX5601-0060|                    |       true|      201925|
|DQ1/8P/C2/31UNU|076271B:25:N08:42...|P14E03|     Z11B|AX5601-0060|                    |       true|      201925|
|DQ1/8P/C2/31UNU|076271B:15:N09:40...|P25F13|     Z11B|AX5601-0060|                    |       true|      201925|
|DQ1/8P/C2/31UNU|076271B:25:P08:13...|P22O04|     Z11B|AX5601-0060|                    |       true|      201925|
|DQ1/8P/C2/31UNU|076271B:02:P13:31...|P03L07|     Z11B|AX5601-0060|at88::rowcopy1s_b...|       true|      201925|
|DQ1/8P/C2/31UNU|076271B:15:P06:13...|P01C16|     Z11B|AX5601-0060|                    |       true|      201925|
|DQ1/8P/C2/31UNU|076271B:21:P11:27...|P19D05|     Z11B|AX5601-0060|                    |       true|      201925|
|DQ1/8P/C2/31UNU|076271B:14:N09:32...|P31E06|     Z11B|AX5601-0060|                    |       true|      201925|
"""
TTEQ_FORMAT_ATTRS =['SUMMARY_ID','FID', 'SITE', 'DESIGN_ID','MACHINE_ID', 'DEF_FIRST_FAIL','SITE_STATUS','MFG_WORKWEEK'] # section attributes                    
datafiles = ','.join(parquet_list_SDP)
section = 'TEST' # first fail info is record in TEST session
format_attrs = ','.join(TTEQ_FORMAT_ATTRS)


tteq_cmd =[
    "-data="+datafiles,
    "%section="+section,
    "-format="+format_attrs
]

tteq2 = StartTTEQ(sparksession)
df_FFailInfo = tteq2.start_query(tteq_cmd, section) # can't add blankspace


# In[15]:


'''
do some data engineering on First fail info table
'''
df_FFailInfo = df_FFailInfo.withColumn('FID',substring('FID',0,17)) # cut off "FID", only keep the first 17 characters
df_FFailInfo = df_FFailInfo.withColumn("SlotPosition",substring("SITE", 1, 3)) # split "SITE", the first 3 characters are the slot position of this component
df_FFailInfo = df_FFailInfo.withColumn("BIBLocation",substring("SITE", 4, 3)) # split "SITE", the 4-6 characters are the socket location on BIB
df_FFailInfo = df_FFailInfo.withColumn("DieNum",substring("SITE", 8,2)) # "SITE", the last two characters represents the die number of this component. It is NA for SDP
df_FFailInfo = df_FFailInfo.drop("SITE") # drop the column "SITE"   
split_col = split("DEF_FIRST_FAIL", '#') # define a split function
df_FFailInfo = df_FFailInfo.withColumn("FirstFail", split_col.getItem(0)) # split "DEF_FIRST_FAIL" by #, the characters before "#" is the component's first fail register.


# In[16]:


"""
1. join the First fail info dataframe with the BIB info dataframe,
2. convert SITE_STATUS columns from True or False into 1 or 0,
3. aggregate several rows (with same summary id and BIB id) into one line, use an array to keep the FID level info in one single BIB.
"""
BIBDataCollection = df_FFailInfo.join(BIB_info.withColumnRenamed('summaryID','SUMMARY_ID'), ['SUMMARY_ID','SlotPosition']).sort("BIBID","BIBLocation")
BIBDataCollection2 = BIBDataCollection.withColumn("SITE_STATUS2", SiteStatusReplace("SITE_STATUS"))
df_22 = BIBDataCollection2.groupBy("SUMMARY_ID","DESIGN_ID","BIBID","BIBType", "JTSID", "SBTID", "MACHINE_ID", "MFG_WORKWEEK").agg(collect_list("SITE_STATUS2").alias("site_status2"),collect_list("FirstFail").alias("first_fail")).sort("SUMMARY_ID", "BIBID")


# In[17]:


"""
convert the aggregated site status array into hexadecimal format string. For example:[1, 1, 1, 1, .....] to  "ffffffffffffff6ff..."
Note that the column with array type can't be saved as .CSV type because it is not strutured data.
"""
df_222 = df_22.select("SUMMARY_ID","DESIGN_ID", "BIBID","BIBType", "JTSID", "SBTID","MACHINE_ID", "MFG_WORKWEEK", GetBinToHex("site_status2").alias('hex_site_status'), "first_fail")


# In[18]:


"""
define an UDF to convert first_fail array into string.
"""
def array_to_string(my_list):
    '''
    @input:
        my_list: first_fail array in column "FirstFail"
    @return:
        a string, join the list and use "," to split different elements.
    '''
    return ','.join([str(elem) for elem in my_list])

'''define the UDF'''
array_to_string_udf = udf(array_to_string,StringType())

'''use the UDF to convert first_fail array into string. For example: [ , , , , , , atn10::ddmh, ....] into string ",,,,,,atn10::ddmh..." '''
df_222 = df_222.withColumn('first_fail_string',array_to_string_udf("first_fail"))
df_FFail1 = df_222.drop("first_fail") # After convert first_fail array into string, drop the useless first_fail column



# In[19]:


'''save the extracted data by repartition method'''
df_FFail1.repartition(10).write.csv('FFailInfo_update',mode='overwrite',compression='gzip',header='true')  


# In[ ]:


'''run linux commands to download the extracted data'''
def run_cmd(args_list):
    """
    run linux commands
    """
    #import subprocess
    print('Running system command:{0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err 

(ret, out, err) = run_cmd(['hdfs','dfs','-copyToLocal','FFailInfo_update','/home/gechen/Project/BIBshading/perfect_touch/data/']) 


# In[26]:


'''stop the sparksession, then stop the Python program'''
sparksession.stop()
print ('SDP data is extracted successfully')
sys.exit()

