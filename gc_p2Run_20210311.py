# Change log: ITD_GROSS_PAID_AMOUNT moved to p1 - optimization
# Change log: ITD_NET_PAID_AMOUNT calculated here
# Change log: tataaig_filter filter applied while reading ri_td_reinsurer_wise_claims - optimization
# Change log: date filter applied while reading gc_clm_mot_additional/code modification for NUM_UPDATE_NO - optimization
# Change log: date filter applied while reading gc_clm_mot_additional_dtls/code modification for NUM_UPDATE_NO - optimization
# Change log: date filter applied while reading gc_clm_activity_checklist - optimization
# Change log: code modification in gc_clm_assessment_summary for NUM_UPDATE_NO - optimization
# Change log: code modification in gc_clm_assessment_info for NUM_UPDATE_NO - optimization
# Change log: commented reading of gc_clm_general_details index and join as well as not req further - optimization
# Change log: commented reading of ri_tmp_cession_unit_details index and join as well as not req further - optimization
# Change log: joining of ri_mm_claim_unit_details index with ncn - optimization
# Change log: filter of GRPID applied while reading on underwriting_gc_cnfgtr_otherdtls_grid_tab_alias - optimization
# Change log: Reading REPORT_DATE and SOURCE from metric/p1
# Change log: NET_PCT, ITD_NET, YTD_NET, ITD_NET_REOPEN logic change
# Change log: TXT_CATASTROPHE_DESC/CATASTROPHE_TYPE moved to p1
# Change log: Checking status for POL_CANCELLATION_DATE
# Change log: UTR related fields added
# Change log: ACCORDANCE_FG and ACCORDANCE_COMMENTS fixed (mapping change)

import sys
envir='prod'
if envir == 'prod':
    if len(sys.argv) < 2:
        print("Usage: Need atleast 1 argument: \n1.RunType: monthly/daily")
        sys.exit(-1)
    else:
        runtype = sys.argv[1]
        if runtype == 'monthly':
            if len(sys.argv) != 3:
                print("Usage: Need 2 arguments \n1.Run Type \n2.Report Date")
                sys.exit(-1)
            else:
                report_date_str = sys.argv[2]
                app_name = "Claim_Report_GC_P2_"+runtype+": "+report_date_str
        elif runtype == 'daily':
            app_name = "Claim_Report_GC_P2_"+runtype

no_of_cpu = 8
max_cores = 16
executor_mem = '42g'
# runtype = 'monthly'
# report_date_str = '2020-06-30'

# env = 'prod'
# if env=='prod':
#     mesos_ip = 'mesos://10.35.12.205:5050'
# else:
#     mesos_ip = 'mesos://10.35.12.5:5050'

##P2
from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import asc,lit
#warnings.filterwarnings('error')
import pyspark
from datetime import datetime,timedelta
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
conf = pyspark.SparkConf()
import calendar
import time
import numpy as np
import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, StringType, DecimalType
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
import simplejson as json
import json, pprint, requests
mesos_ip = 'mesos://10.35.12.205:5050'
# spark.stop()
conf.setMaster(mesos_ip)

conf.set('spark.executor.cores',no_of_cpu)
conf.set('spark.cores.max',max_cores)
conf.set('spark.executor.memory',executor_mem)
conf.set('spark.driver.memory','6g')
# conf.set("spark.ui.port","4048")
conf.set('spark.sql.shuffle.partitions','300')
conf.set("spark.app.name", app_name)
conf.set("spark.driver.maxResultSize","0")
conf.set('spark.es.scroll.size','10000')
conf.set('spark.network.timeout','600s')
conf.set('spark.sql.crossJoin.enabled', 'true')
conf.set('spark.executor.heartbeatInterval','60s')
conf.set("spark.driver.cores","4")
conf.set("spark.driver.extraJavaOptions","-Xms4g -Xmx12g")
conf.set("spark.files.overwrite","true");
conf.set("spark.kryoserializer.buffer", "70");
conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC");
conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC");
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"); 
conf.set("spark.broadcast.compress", "true");
conf.set("spark.shuffle.compress", "true"); 
conf.set("spark.shuffle.spill.compress", "true");
from pyspark.sql.functions import broadcast
conf.set('es.nodes.wan.only','true')
conf.set('spark.es.mapping.date.rich','false')
conf.set('es.http.timeout', '5m')
conf.set('es.http.retries', '10')
spark = pyspark.SparkContext(conf=conf)
sqlContext = SQLContext(spark)

# Load Data into PySpark DataFrames
# Prodcom Data Frame
import json, pprint, requests
import pyspark.sql.functions as sf

##Define Required Functions

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)

def last_day_of_previous_month(any_day):
    previous_month = any_day.replace(day = 1)
    return previous_month - datetime.timedelta(days=1)

def first_day_of_last_month(any_day):
    previous_month = any_day.replace(day = 1)
    previous_month = previous_month - datetime.timedelta(days=1)
    return previous_month.replace(day = 1)

def first_day_of_month(any_day):
    if any_day != None:
        return any_day.replace(day = 1)
    else:
        return any_day

def string_to_date(any_day):
    if any_day != None:
        return datetime.datetime.strptime(any_day[:10],'%Y-%m-%d')
    else:
        return any_day

from pyspark.sql.types import IntegerType
def day_of_date(any_date):
    if any_date != None:
        return any_date.day
    else:
        return any_date

def year_of_date(any_date):
    if any_date != None:
        return any_date.year
    else:
        return any_date

from pyspark.sql.types import StringType
def first_ten(any_string):
    if any_string != None:
        return any_string[0:10]
    else:
        return any_string      

def after_ten(any_string):
    if any_string != None:
        return any_string[10:]
    else:
        return any_string 

def after_one(any_string):
    if any_string != None:
        return any_string[1:]
    else:
        return any_string

def after_two(any_string):
    if any_string != None:
        return any_string[2:]
    else:
        return any_string

def first_two(any_string):
    if any_string != None:
        if(len(any_string) >= 2):
            return any_string[0:2]
        else:
            return '0'
    else:
        return any_string

def three_four(any_string):
    if any_string != None:
        if(len(any_string) >= 4):
            return any_string[2:4]
        else:
            return '0'
    else:
        return any_string

def after_four(any_string):
    if any_string != None:
        if(len(any_string) > 4):
            return any_string[4:7]
        else:
            return '0'
    else:
        return any_string

def lengthSt(any_string):
    if any_string != None:
        return len(any_string)
    else:
        return 0

DayofDate = udf(lambda x: day_of_date(x), IntegerType())

# Returns the last date of the column containing dates
lastDayofMonth = udf(lambda x: last_day_of_month(x),DateType())

# Returns the first date of month for the column containing dates
firstDayofMonth = udf(lambda x: first_day_of_month(x), DateType())

# Returns the first date of month for the column containing dates
firstTenChar = udf(lambda x: first_ten(x), StringType())

afterTenChar = udf(lambda x: after_ten(x), StringType())

firstTwoChar = udf(lambda x: first_two(x), StringType())

ThirdFourthChar = udf(lambda x: three_four(x), StringType())

afterFourChar = udf(lambda x: after_four(x), StringType())

afterOneChar = udf(lambda x: after_one(x), StringType())

afterTwoChar = udf(lambda x: after_two(x), StringType())

LengthOfString = udf(lambda x: lengthSt(x), IntegerType())

# Returns year of date for the column containing dates
yearOfDate = udf(lambda x: year_of_date(x), IntegerType())

#from pyspark.sql.types import DateType
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import lit

# First convert all the Dates into Date format
# First define a udf for converting string into date format
StringToDateFunc = udf(lambda x: string_to_date(x), DateType())

import functools 
# Function row-wise joining of dataframes
def unionAll(dfs):
    return functools.reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), dfs)

# from decimal import *
# num_claim_no_str = ["30010031209055865001","30015031199061038001"]
# num_refer_no_list = [201909210094392,201902020094593]
# claim_unit_list = [2037569572,2035821014,2035621701,2037569570,2035842855,2037599130]
# cession_no_list = [17569542,15821010,17569544,17599102,15842851,15621697]
# num_claim_no_new = [Decimal(i) for i in num_claim_no_str]

if runtype == 'monthly':
    report_date = last_day_of_month(string_to_date(report_date_str).date())
    report_date_str = str(report_date)
elif runtype == 'daily':
    report_date = datetime.date.today()- datetime.timedelta(days=1)
    report_date_str = str(report_date)
    
report_date_filter = str(string_to_date(report_date_str).date()+ datetime.timedelta(days=1))
start_date = str(first_day_of_month(report_date))
end_date = report_date_str
# YYMM = str(report_date.year) + str('{:02d}'.format(report_date.month))
# YYMMDD = report_date.strftime('%Y%m%d')

print("report_date_str",report_date_str)
print("report_date",report_date)
print("start_date",start_date)
print("end_date",end_date)

# url_uat = "jdbc:postgresql://10.33.195.103:5432/gpadmin"
url_prod = "jdbc:postgresql://10.35.12.194:5432/gpadmin"
# server_port = "1100-1160"
server_port = "1108"
user_prod="gpspark"
pwd_prod="spark@456"
dbschema_etl = "public"

def upperAllColName(df):
    for col in df.columns:
        df=df.withColumnRenamed(col, (col).upper())
    return df

####  NON-FINANCIAL PART 2 START
## claim_report_gc_p1 (claim_report_gc_p1)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": "datamarts",
         "dbtable": "claim_report_gc_p1",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":8} 
clm_00 = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.filter(col("REPORT_DATE").between(to_date(lit(start_date), format='yyyy-MM-dd'),to_date(lit(end_date), format='yyyy-MM-dd')))\
.drop('LOAD_DATE','CHEQUE_BOUNCE')\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))
clm_00 = upperAllColName(clm_00)

clm_00.persist()

### gc_clm_gen_info_extra (gc_clm_gen_info_extra)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_gen_info_extra",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":6} 
gc_clm_gen_info_extra = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','TXT_INFO11','TXT_INFO13','TXT_INFO15')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
.withColumnRenamed('TXT_INFO11', 'CAUSE_OF_LOSS')\
.withColumnRenamed('TXT_INFO13', 'ANAT_PROP')\
.withColumnRenamed('TXT_INFO15', 'INGURY_DAMAGE')\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_gen_info_extra = gc_clm_gen_info_extra.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))
# gc_clm_gen_info_extra = gc_clm_gen_info_extra.repartition('NUM_CLAIM_NO')

### gc_clm_mot_additional (gc_clm_mot_additional)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_mot_additional",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":4} 
gc_clm_mot_additional = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_SERIAL_NO','NUM_ADDITIONAL_INFO_TYPE_CD','TXT_INFO1','TXT_INFO10','TXT_INFO15','TXT_INFO16','TXT_INFO17','TXT_INFO18','TXT_INFO19','TXT_INFO2','TXT_INFO23','TXT_INFO25','TXT_INFO28','TXT_INFO29','TXT_INFO3','TXT_INFO30','TXT_INFO34','TXT_INFO37','TXT_INFO39','TXT_INFO4','TXT_INFO40','TXT_INFO41','TXT_INFO5','TXT_INFO6','TXT_INFO7','TXT_INFO8','TXT_INFO9')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_mot_additional = gc_clm_mot_additional.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## gc_clm_mot_additional_dtls (gc_clm_mot_additional_dtls)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_mot_additional_dtls",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clm_mot_additional_dtls = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_SERIAL_NO','NUM_DETAIL_SERIAL_NO','NUM_ADDITIONAL_INFO_TYPE_CD','TXT_INFO10','TXT_INFO12','TXT_INFO13','TXT_INFO2','TXT_INFO3','TXT_INFO5','TXT_INFO8','TXT_INFO9','TXT_INFO6')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_mot_additional_dtls = gc_clm_mot_additional_dtls.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## gc_clm_assessment_summary (gc_clm_assessment_summary)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_assessment_summary",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clm_assessment_summary = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_SERIAL_NO','NUM_ASSESSMENT_TYPE_CD','TXT_INFO12','TXT_INFO68','TXT_INFO69','TXT_INFO73','TXT_INFO9', 'TXT_INFO70','TXT_INFO71','TXT_INFO72','NUM_PAYMENT_UPDATE_NO')\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_assessment_summary = gc_clm_assessment_summary.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## gc_clm_assessment_info (gc_clm_assessment_info)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_assessment_info",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":6} 
gc_clm_assessment_info = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_SERIAL_NO','NUM_ASSESSMENT_TYPE_CD','TXT_INFO13','TXT_INFO16','TXT_INFO17','TXT_INFO30','TXT_INFO34','TXT_INFO6')\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_assessment_info = gc_clm_assessment_info.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## gc_clm_activity_checklist (gc_clm_activity_checklist)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_activity_checklist",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":8} 
gc_clm_activity_checklist_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_ACTIVITY_NUMBER','TXT_ACTIVITY','DAT_INSERT_DATE','TXT_CHECK_STATUS')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_activity_checklist_base = gc_clm_activity_checklist_base.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## gc_clm_neft_upload_comp (gc_clm_neft_upload_comp)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_neft_upload_comp",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":8} 
gc_clm_neft_upload_comp_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','TXT_INFO3','TXT_NEFTOUTWARDS_CHQ_NO','TXT_NEFTOUTWARDS_UTR_NO','DAT_BOOK_DATE','TXT_TRANS_DETAIL_LINE4','TXT_PROCESS_FLAG','DAT_DATE_OF_INSERT')\
.filter(col("DAT_SYSTEM_PAYMENT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_neft_upload_comp_base = gc_clm_neft_upload_comp_base.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

# ## covers_details (covers_details)
# gscPythonOptions = {
#          "url": url_prod,
#          "user": user_prod,
#          "password": pwd_prod,
#          "dbschema": dbschema_etl,
#          "dbtable": "policy_gc_covers_details",
#          "partitionColumn":"row_num",
#          "server.port":server_port,
#          "partitions":12} 
# cvr_dtl = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
# .select('REFERENCE_NUM', 'PRODUCT_INDEX', 'COVERGRPINDX', 'SI','COVER_GROUP')\
# .filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("REFERENCE_NUM").isin(num_refer_no_list))

## risk_headers (risk_headers)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "policy_gc_risk_headers",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":18} 
risk_headers_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('REFERENCE_NUM','INFORMATION3','INFORMATION4','INFORMATION5','INFORMATION6','INFORMATION7','INFORMATION8','INFORMATION9','INFORMATION10','INFORMATION11','INFORMATION12','INFORMATION14','INFORMATION15','INFORMATION16','INFORMATION17','INFORMATION19','INFORMATION21','INFORMATION22','INFORMATION23','INFORMATION24','INFORMATION33','INFORMATION34','INFORMATION35','INFORMATION36','INFORMATION37','INFORMATION38','INFORMATION47','INFORMATION49','INFORMATION59','INFORMATION65','INFORMATION102','INFORMATION104','INFORMATION106','INFORMATION114','INFORMATION182','INFORMATION225','RISK1','INFORMATION273','INFORMATION274','INFORMATION275','INFORMATION276','INFORMATION277')\
# .filter(col("REFERENCE_NUM").isin(num_refer_no_list))
# risk_headers_base = risk_headers_base.repartition('REFERENCE_NUM')

# risk_headers_base.count()

## risk_details (risk_details)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "policy_gc_risk_details",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":16} 
risk_details = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('REFERENCE_NUM','INFORMATION12','INFORMATION20','INFORMATION24','INFORMATION25','INFORMATION26','INFORMATION33','INFORMATION34','DAT_INSERT_DATE')\
# .filter(col("REFERENCE_NUM").isin(num_refer_no_list))

status_filter=['APC','ACDC','ACDE','ERFC']
## cnfgtr_d_all_transactions (cnfgtr_d_all_transactions)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "underwriting_gc_cnfgtr_d_all_transactions",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":4} 
cnfgtr_d_all_transactions_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TRANS_ID','STATUS')\
.filter(col("STATUS").isin(status_filter))\
# .filter(col("TRANS_ID").isin(num_refer_no_list))

grpid_filter = ["GRP365","GRP511","GRP510", "GRP626", "GRP500","GRP535","GRP711","GRP697","GRP703","GRP732","GRP868"]
## cnfgtr_otherdtls_grid_tab (cnfgtr_otherdtls_grid_tab)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "underwriting_gc_cnfgtr_otherdtls_grid_tab",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":4} 
cnfgtr_otherdtls_grid_tab = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REFERENCE_NUMBER','GRPID','SEQ_NO','BLK_ID','INFORMATION5','INFORMATION7','INFORMATION20','INFORMATION22','INFORMATION23','INFORMATION24')\
.filter(col("GRPID").isin(grpid_filter))\
# .filter(col("NUM_REFERENCE_NUMBER").isin(num_refer_no_list))

#####################Report Column NET_PCT and NET_AMOUNT
## ri_td_claim_details (ri_td_claim_details)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "policy_gc_ri_td_claim_details",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":4} 
ri_td_claim_details = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_TRANSACTION_CONTROL_NO')\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

ri_td_claim_details = ri_td_claim_details.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## ri_mm_claim_unit_details (ri_mm_claim_unit_details)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "policy_gc_ri_mm_claim_unit_details",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":4} 
ri_mm_claim_unit_details = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NUMBER','NUM_UPDATE_NUMBER','NUM_CLAIM_UNIT_ID')\
.withColumnRenamed('NUM_CLAIM_NUMBER','NUM_CLAIM_NO')\
.withColumnRenamed('NUM_UPDATE_NUMBER','NUM_UPDATE_NO')\
# .filter(col("NUM_CLAIM_NUMBER").isin(num_claim_no_new))

ri_mm_claim_unit_details = ri_mm_claim_unit_details.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## ri_tm_claim_cession (ri_tm_claim_cession)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "policy_gc_ri_tm_claim_cession",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":4} 
ri_tm_claim_cession = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_UNIT_ID','NUM_CESSION_NUMBER','CUR_OS_RETAINED_AMOUNT','CUR_OS_RELEASED_AMOUNT','NUM_EXP_OS_RETAINED_AMOUNT','NUM_EXP_OS_RELEASED_AMOUNT','CUR_CLAIM_PAID_AMOUNT','CUR_CLAIM_RECOVERED_AMOUNT','CUR_EXPENSE_PAID_AMOUNT','CUR_EXPENSE_RECOVERED_AMOUNT')\
.withColumnRenamed('CUR_OS_RETAINED_AMOUNT','CUR_OS_RETAINED_AMOUNT_CC')\
.withColumnRenamed('CUR_OS_RELEASED_AMOUNT','CUR_OS_RELEASED_AMOUNT_CC')\
.withColumnRenamed('NUM_EXP_OS_RETAINED_AMOUNT','NUM_EXP_OS_RETAINED_AMOUNT_CC')\
.withColumnRenamed('NUM_EXP_OS_RELEASED_AMOUNT','NUM_EXP_OS_RELEASED_AMOUNT_CC')\
.withColumnRenamed('CUR_CLAIM_PAID_AMOUNT','CUR_CLAIM_PAID_AMOUNT_CC')\
.withColumnRenamed('CUR_CLAIM_RECOVERED_AMOUNT','CUR_CLAIM_RECOVERED_AMOUNT_CC')\
.withColumnRenamed('CUR_EXPENSE_PAID_AMOUNT','CUR_EXPENSE_PAID_AMOUNT_CC')\
.withColumnRenamed('CUR_EXPENSE_RECOVERED_AMOUNT','CUR_EXPENSE_RECOVERED_AMOUNT_CC')\
# .filter(col("NUM_CLAIM_UNIT_ID").isin(claim_unit_list))

## ri_td_reinsurer_wise_claims (ri_td_reinsurer_wise_claims)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "policy_gc_ri_td_reinsurer_wise_claims",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":4} 
ri_td_reinsurer_wise_claims = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REINSURER_CODE','NUM_CESSION_NUMBER','CUR_OS_RETAINED_AMOUNT','CUR_OS_RELEASED_AMOUNT','CUR_CLAIM_PAID_AMOUNT','CUR_CLAIM_RECOVERED_AMOUNT','NUM_EXP_OS_RETAINED_AMOUNT','NUM_EXP_OS_RELEASED_AMOUNT','CUR_EXPENSE_PAID_AMOUNT','CUR_EXPENSE_RECOVERED_AMOUNT','TXT_CESSION_TYPE')\
.withColumnRenamed('CUR_OS_RETAINED_AMOUNT','CUR_OS_RETAINED_AMOUNT_RWC')\
.withColumnRenamed('CUR_OS_RELEASED_AMOUNT','CUR_OS_RELEASED_AMOUNT_RWC')\
.withColumnRenamed('NUM_EXP_OS_RETAINED_AMOUNT','NUM_EXP_OS_RETAINED_AMOUNT_RWC')\
.withColumnRenamed('NUM_EXP_OS_RELEASED_AMOUNT','NUM_EXP_OS_RELEASED_AMOUNT_RWC')\
.withColumnRenamed('CUR_CLAIM_PAID_AMOUNT','CUR_CLAIM_PAID_AMOUNT_RWC')\
.withColumnRenamed('CUR_CLAIM_RECOVERED_AMOUNT','CUR_CLAIM_RECOVERED_AMOUNT_RWC')\
.withColumnRenamed('CUR_EXPENSE_PAID_AMOUNT','CUR_EXPENSE_PAID_AMOUNT_RWC')\
.withColumnRenamed('CUR_EXPENSE_RECOVERED_AMOUNT','CUR_EXPENSE_RECOVERED_AMOUNT_RWC')\
# .filter(col("NUM_CESSION_NUMBER").isin(cession_no_list))

## ri_mm_cession_types (ri_mm_cession_types)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_ri_mm_cession_types",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
ri_mm_cession_types = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TXT_CESSION_TYPE','NUM_CESSION_TYPE_CD','TXT_CESSION_TYPE_DESCRIPTION')\
.withColumn('NUM_CESSION_TYPE_CD', col('NUM_CESSION_TYPE_CD').cast(DecimalType(2,0)).cast(StringType()))

## gc_clm_claim_audit (gc_clm_claim_audit)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_claim_audit",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clm_claim_audit1 = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_CLM_AUD_QUSTN_ID','TXT_CLM_AUD_QUSTN_ANSWR','TXT_FILE_NO')

gc_clm_claim_audit1 = gc_clm_claim_audit1.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## genmst_tab_office (genmst_tab_office)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_genmst_tab_office",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
genmst_tab_office_off = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TXT_OFFICE','NUM_OFFICE_CD','NUM_PARENT_OFFICE_CD','TXT_OFFICE_TYPE')

## covercodemaster (covercodemaster)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_covercodemaster",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
covercodemaster2 = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_COVER_CODE','TXT_COVER_DESCRIPTION')\
.withColumnRenamed('TXT_COVER_DESCRIPTION', 'COVERAGE_DESC')

## gc_clmmst_mc_conveyence_view (gc_clmmst_mc_conveyence_view)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clmmst_mc_conveyence_view",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_mc_conveyence_view = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('ITEM_VALUE','ITEM_TEXT')

## gc_clmmst_mc_shipmenttype_view (gc_clmmst_mc_shipmenttype_view)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clmmst_mc_shipmenttype_view",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_mc_shipmenttype_view = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('ITEM_VALUE','ITEM_TEXT')

## gc_clmmst_generic_value (gc_clmmst_generic_value)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_gc_clmmst_generic_value",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_generic_value = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_MASTER_CD','TXT_INFO1','TXT_INFO2','TXT_INFO3','TXT_INFO5')

## cnfgtr_cover_grp_map (cnfgtr_cover_grp_map)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "policy_gc_cnfgtr_cover_grp_map",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
cnfgtr_cover_grp_map = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('COVERCODE','COVERGRPINDX')

## cnfgtr_cover_grp_mstr (cnfgtr_cover_grp_mstr)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "policy_gc_cnfgtr_cover_grp_mstr",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
cnfgtr_cover_grp_mstr = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('COVERGRPINDX','COVERGROUPNAME')

## gc_clmmst_workshop (gc_clmmst_workshop)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clmmst_workshop",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_workshop1 = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_WORKSHOP_CD','TXT_WORKSHOP_NAME','TXT_SKILL_AVAILABLE','TXT_PAN_NO')

## sub_product_mapper (sub_product_mapper)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": "mappers",
         "dbtable": "sub_product_mapper",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
sub_product_master_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('PRODUCT_CD','SUB_PRODUCT_NAME')

## product_type_mapper (product_type_mapper)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": "mappers",
         "dbtable": "product_type_mapper",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
product_type_master_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('PRODUCT_CD','PRODUCT_TYPE')

## gc_clmmst_resource (gc_clmmst_resource)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_gc_clmmst_resource",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_resource = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_RESOURCE_CD','UPDATENO','DAT_LICENCE_EXPIRY_DATE','NUM_RESOURCE_TYPE','TXT_LIECENCE_NO','TXT_PAN_NO','TXT_GST_NUMBER','NUM_CITYDISTRICT_CD')

##satya20190621
### genmst_citydistrict (genmst_citydistrict)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "customer_gc_genmst_citydistrict",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
genmst_citydistrict = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_STATE_CD','NUM_CITYDISTRICT_CD','TXT_CITYDISTRICT')

## country_master (country_master)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_country_master",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
country_master = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('COUNTRYCODE','COUNTRYNAME')\
.withColumn('COUNTRYCODE',col('COUNTRYCODE').cast(DecimalType(4,0)).cast(StringType()))

### product_details (product_details)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_product_details",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
prod_dtl = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('DEFVAL', 'NODEINDX')

###Index Declaration ends here

covercodemaster2.createOrReplaceTempView('covercodemaster2_view')

query = """select *,
cast(cast(NUM_COVER_CODE as decimal) as string) NUM_COVER_CODE_B
from covercodemaster2_view"""
covercodemaster = sqlContext.sql(query)
sqlContext.dropTempTable('covercodemaster2_view')

covercodemaster = covercodemaster\
                        .drop('NUM_COVER_CODE')

covercodemaster = covercodemaster\
                        .withColumnRenamed('NUM_COVER_CODE_B','NUM_COVER_CODE')

gc_clmmst_workshop1.createOrReplaceTempView('gc_clmmst_workshop_view')

query = """select *,
cast(cast(NUM_WORKSHOP_CD as decimal) as string) NUM_WORKSHOP_CD_B
from gc_clmmst_workshop_view"""
gc_clmmst_workshop = sqlContext.sql(query)
sqlContext.dropTempTable('gc_clmmst_workshop_view')

gc_clmmst_workshop = gc_clmmst_workshop\
                        .drop('NUM_WORKSHOP_CD')

gc_clmmst_workshop = gc_clmmst_workshop\
                        .withColumnRenamed('NUM_WORKSHOP_CD_B','NUM_WORKSHOP_CD')\
                        .withColumnRenamed('TXT_PAN_NO','WORKSHOP_PAN_NO')

###  gc_clm_activity_checklist
gc_clm_activity_checklist = gc_clm_activity_checklist_base\
                            .withColumn('DAT_INSERT_DATE', to_date(gc_clm_activity_checklist_base.DAT_INSERT_DATE, format='yyyy-MM-dd'))\
                            .withColumn('checklist_check', concat(gc_clm_activity_checklist_base.NUM_UPDATE_NO,gc_clm_activity_checklist_base.NUM_ACTIVITY_NUMBER).cast('integer'))\
                            .drop('NUM_UPDATE_NO','NUM_ACTIVITY_NUMBER')

###  risk_headers
risk_headers = risk_headers_base

###  cnfgtr_d_all_transactions
# cnfgtr_d_all_transactions = cnfgtr_d_all_transactions_base\
#                                 .withColumn('TRANS_ID', col('TRANS_ID').cast(DecimalType(15,0)).cast(StringType()))
cnfgtr_d_all_transactions = cnfgtr_d_all_transactions_base

#####   INNER JOIN WITH ALL TABLES POSSIBLE #####

ri_td_claim_details1 = ri_td_claim_details
ri_mm_claim_unit_details1 = ri_mm_claim_unit_details
gc_clm_activity_checklist1 = gc_clm_activity_checklist
gc_clm_mot_additional1 = gc_clm_mot_additional
gc_clm_mot_additional_dtls1 = gc_clm_mot_additional_dtls
gc_clm_assessment_summary1 = gc_clm_assessment_summary
gc_clm_assessment_info1 = gc_clm_assessment_info
gc_clm_claim_audit = gc_clm_claim_audit1
risk_headers1 = risk_headers
risk_details1 = risk_details
cnfgtr_d_all_transactions1 = cnfgtr_d_all_transactions


## join with gc_clm_gen_info_extra for Anat_Prop 20200110
gen_info_extra_anat_01 = gc_clm_gen_info_extra['NUM_CLAIM_NO','NUM_UPDATE_NO','ANAT_PROP']
gen_info_extra_anat_02 = gen_info_extra_anat_01.filter(~((gen_info_extra_anat_01.ANAT_PROP.isNull())|(gen_info_extra_anat_01.ANAT_PROP=='')|(lower(gen_info_extra_anat_01.ANAT_PROP)=='select...')))
gen_info_extra_anat_03 = gen_info_extra_anat_02\
                .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('NUM_UPDATE_NO').alias('NUM_UPDATE_NO'))
gen_info_extra_anat_04 = gen_info_extra_anat_03.join(gen_info_extra_anat_02,['NUM_CLAIM_NO','NUM_UPDATE_NO'],'inner')

gen_info_extra_anat_05 = gen_info_extra_anat_04\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('ANAT_PROP').alias('ANAT_PROP'))

anat_all = clm_00.join(gen_info_extra_anat_05,'NUM_CLAIM_NO','left_outer')

## Modification of Anat_Prop ended 20200110

# ## join with gc_clm_gen_info_extra for CATASTROPHE_TYPE 20200110
# gen_info_extra_cat_type_01 = gc_clm_gen_info_extra['NUM_CLAIM_NO','NUM_UPDATE_NO','CATASTROPHE_TYPE']
# gen_info_extra_cat_type_02 = gen_info_extra_cat_type_01.filter(~((gen_info_extra_cat_type_01.CATASTROPHE_TYPE.isNull())|(gen_info_extra_cat_type_01.CATASTROPHE_TYPE=='')|(lower(gen_info_extra_cat_type_01.CATASTROPHE_TYPE)=='select...')))
# gen_info_extra_cat_type_03 = gen_info_extra_cat_type_02\
#                 .groupBy('NUM_CLAIM_NO')\
#                         .agg(sf.max('NUM_UPDATE_NO').alias('NUM_UPDATE_NO'))
# gen_info_extra_cat_type_04 = gen_info_extra_cat_type_03.join(gen_info_extra_cat_type_02,['NUM_CLAIM_NO','NUM_UPDATE_NO'],'inner')

# gen_info_extra_cat_type_05 = gen_info_extra_cat_type_04\
#                     .groupBy('NUM_CLAIM_NO')\
#                     .agg(sf.max('CATASTROPHE_TYPE').alias('CATASTROPHE_TYPE'))

# cat_type_all = anat_all.join(gen_info_extra_cat_type_05,'NUM_CLAIM_NO','left_outer')
# ## Modification of CATASTROPHE_TYPE ended 20200110
cat_type_all = anat_all

## join with gc_clm_gen_info_extra for INJURY_DAMAGE/INGURY_DAMAGE 20200110
gen_info_extra_injury_01 = gc_clm_gen_info_extra['NUM_CLAIM_NO','NUM_UPDATE_NO','INGURY_DAMAGE']
gen_info_extra_injury_02 = gen_info_extra_injury_01.filter(~((gen_info_extra_injury_01.INGURY_DAMAGE.isNull())|(gen_info_extra_injury_01.INGURY_DAMAGE=='')|(lower(gen_info_extra_injury_01.INGURY_DAMAGE)=='select...')))
gen_info_extra_injury_03 = gen_info_extra_injury_02\
                .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('NUM_UPDATE_NO').alias('NUM_UPDATE_NO'))
gen_info_extra_injury_04 = gen_info_extra_injury_03.join(gen_info_extra_injury_02,['NUM_CLAIM_NO','NUM_UPDATE_NO'],'inner')

gen_info_extra_injury_05 = gen_info_extra_injury_04\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('INGURY_DAMAGE').alias('INJURY_DAMAGE'))

injury_all = cat_type_all.join(gen_info_extra_injury_05,'NUM_CLAIM_NO','left_outer')
## Modification of INJURY_DAMAGE/INGURY_DAMAGE ended 20200110

## join with gc_clm_gen_info_extra for CAUSE_OF_LOSS 20200110
gen_info_extra_cause_01 = gc_clm_gen_info_extra['NUM_CLAIM_NO','NUM_UPDATE_NO','CAUSE_OF_LOSS']
gen_info_extra_cause_02 = gen_info_extra_cause_01.filter(~((gen_info_extra_cause_01.CAUSE_OF_LOSS.isNull())|(gen_info_extra_cause_01.CAUSE_OF_LOSS=='')|(lower(gen_info_extra_cause_01.CAUSE_OF_LOSS)=='select...')))
gen_info_extra_cause_03 = gen_info_extra_cause_02\
                .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('NUM_UPDATE_NO').alias('NUM_UPDATE_NO'))
gen_info_extra_cause_04 = gen_info_extra_cause_03.join(gen_info_extra_cause_02,['NUM_CLAIM_NO','NUM_UPDATE_NO'],'inner')

gen_info_extra_cause_05 = gen_info_extra_cause_04\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('CAUSE_OF_LOSS').alias('CAUSE_OF_LOSS'))

cause_all = injury_all.join(gen_info_extra_cause_05,'NUM_CLAIM_NO','left_outer')
clm_00 = cause_all
## Modification of CAUSE_OF_LOSS ended 20200110

# clm_43_tmp = clm_43.select(col('CLAIM_NO').alias('CLAIM_NO'), 'TXT_CATASTROPHE_DESC', 'LCL_CATASTROPHE_NO')
# clm_43_tmp_01 = clm_43_tmp\
#                     .groupBy('CLAIM_NO')\
#                     .agg(sf.max('TXT_CATASTROPHE_DESC').alias('TXT_CATASTROPHE_DESC_TMP'),\
#                         sf.max('LCL_CATASTROPHE_NO').alias('LCL_CATASTROPHE_NO_TMP'))
# clm_43_tmp_02 = clm_43.join(clm_43_tmp_01, 'CLAIM_NO', 'left')
# clm_43 = clm_43_tmp_02\
#                 .withColumn('TXT_CATASTROPHE_DESC', when(((clm_43_tmp_02.TXT_CATASTROPHE_DESC.isNull())|(clm_43_tmp_02.TXT_CATASTROPHE_DESC=='')), clm_43_tmp_02.TXT_CATASTROPHE_DESC_TMP)
#                                 .otherwise(clm_43_tmp_02.TXT_CATASTROPHE_DESC))\
#                 .withColumn('LCL_CATASTROPHE_NO', when(((clm_43_tmp_02.LCL_CATASTROPHE_NO.isNull())|(clm_43_tmp_02.LCL_CATASTROPHE_NO=='')), clm_43_tmp_02.LCL_CATASTROPHE_NO_TMP)
#                                 .otherwise(clm_43_tmp_02.LCL_CATASTROPHE_NO))\
#                 .drop('TXT_CATASTROPHE_DESC_TMP')\
#                 .drop('LCL_CATASTROPHE_NO_TMP')

# #used gc_clmmst_catastrphy_typ_view
# clm_00 = clm_43\
#             .withColumn('CATASTROPHE_TYPE', \
#                         when(clm_43.LCL_CATASTROPHE_NO=='1', lit('Local'))\
#                         .when(clm_43.LCL_CATASTROPHE_NO=='2', lit('International'))\
#                              .otherwise(lit(None)))

# clm_00.count()

#below logic is written in better way - 20200421
clm_mot_add = gc_clm_mot_additional1\
                        .withColumn('check1', concat(trim(gc_clm_mot_additional1.NUM_UPDATE_NO).cast('integer'),\
                                                          trim(gc_clm_mot_additional1.NUM_SERIAL_NO).cast('integer')).cast('integer'))\
                        .drop('NUM_UPDATE_NO')\
                        .drop('NUM_SERIAL_NO')

clm_mot_add1 = clm_mot_add\
                    .groupBy('NUM_CLAIM_NO','NUM_ADDITIONAL_INFO_TYPE_CD')\
                    .agg(sf.max('check1').alias('check2'))

join_cond = [clm_mot_add.NUM_CLAIM_NO == clm_mot_add1.NUM_CLAIM_NO,\
             clm_mot_add.NUM_ADDITIONAL_INFO_TYPE_CD == clm_mot_add1.NUM_ADDITIONAL_INFO_TYPE_CD,\
             clm_mot_add.check1 == clm_mot_add1.check2]

clm_mot_add2 = clm_mot_add\
                        .join(clm_mot_add1, join_cond, "left_outer")\
                        .drop(clm_mot_add1.NUM_CLAIM_NO)\
                        .drop(clm_mot_add1.NUM_ADDITIONAL_INFO_TYPE_CD)\
                        .drop(clm_mot_add1.check2)\
                        .drop(clm_mot_add.check1)

clm_mot_add3 = clm_mot_add2\
                    .withColumn('WAYBILLDATE_AW',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.2, clm_mot_add2.TXT_INFO2)\
                                .otherwise(lit('')))\
                    .withColumn('WAYBILLDATE_RW',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.3, clm_mot_add2.TXT_INFO2)\
                                .otherwise(lit('')))\
                    .withColumn('WAYBILLDATE_TW',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.4, clm_mot_add2.TXT_INFO2)\
                                .otherwise(lit('')))\
                    .withColumn('WAYBILLDATE_TMW',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.1, clm_mot_add2.TXT_INFO41)\
                                .otherwise(lit('')))\
                    .withColumn('WAYBILLDATE_V',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.5, clm_mot_add2.TXT_INFO4))\
                    .withColumn('WAYBILLDATE_WH',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.6, clm_mot_add2.TXT_INFO5)\
                                .otherwise(lit('')))\
                    .withColumn('WAYBILLNO_AW',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.2, clm_mot_add2.TXT_INFO3)\
                                .otherwise(lit('')))\
                    .withColumn('WAYBILLNO_RW',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.3, clm_mot_add2.TXT_INFO3)\
                                .otherwise(lit('')))\
                    .withColumn('WAYBILLNO_TW',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.4, clm_mot_add2.TXT_INFO3)\
                                .otherwise(lit('')))\
                    .withColumn('WAYBILLNO_V',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.5, clm_mot_add2.TXT_INFO5))\
                    .withColumn('WAYBILLNO_WH',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.6, clm_mot_add2.TXT_INFO4)\
                                .otherwise(lit('')))\
                    .withColumn('CARRIER_AW',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.2, clm_mot_add2.TXT_INFO1)\
                                .otherwise(lit('')))\
                    .withColumn('CARRIER_RW',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.3, clm_mot_add2.TXT_INFO1)\
                                .otherwise(lit('')))\
                    .withColumn('CARRIER_TW',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.4, clm_mot_add2.TXT_INFO1)\
                                .otherwise(lit('')))\
                    .withColumn('CARRIER_V',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.5, clm_mot_add2.TXT_INFO3))\
                    .withColumn('CARRIER_WH',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.6, clm_mot_add2.TXT_INFO2)\
                                .otherwise(lit('')))\
                    .withColumn('EP_NUMBER',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 15, clm_mot_add2.TXT_INFO1)\
                                .otherwise(lit('')))\
                    .withColumn('SALARY_OF_INJURED',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 16, clm_mot_add2.TXT_INFO10)\
                                .otherwise(lit('')))\
                    .withColumn('NO_OF_DEPENDANTS',\
                                when((clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 16)|\
                                     (clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 10), clm_mot_add2.TXT_INFO15)\
                                .otherwise(lit('')))\
                    .withColumn('IS_ORDER_XXI_COMPLIED',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 15, clm_mot_add2.TXT_INFO17)\
                                .otherwise(lit('')))\
                    .withColumn('COMPLIANCE_DATE',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 15, clm_mot_add2.TXT_INFO18)\
                                .otherwise(lit('')))\
                    .withColumn('FROM_CITY',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.1, clm_mot_add2.TXT_INFO18)\
                                .otherwise(lit('')))\
                    .withColumn('FROM_COUNTRY_CD',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.1, clm_mot_add2.TXT_INFO16)\
                                .otherwise(lit('')))\
                    .withColumn('EP_YEAR',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 15, clm_mot_add2.TXT_INFO2)\
                                .otherwise(lit('')))\
                    .withColumn('AGE_OF_INJURED',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 7, clm_mot_add2.TXT_INFO2)\
                                .otherwise(lit('')))\
                    .withColumn('DATE_OF_ACCIDENT_AS_PER_FIR',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 3, clm_mot_add2.TXT_INFO2)\
                                .otherwise(lit('')))\
                    .withColumn('FIR_DATE',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 3, clm_mot_add2.TXT_INFO2)\
                                .otherwise(lit('')))\
                    .withColumn('REPAIRER_NAME',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 5, clm_mot_add2.TXT_INFO2)\
                                .otherwise(lit('')))\
                    .withColumn('PROPERTY_INSURED',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.1, clm_mot_add2.TXT_INFO2)\
                                .otherwise(lit('')))\
                    .withColumn('TO_CITY',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.1, clm_mot_add2.TXT_INFO25)\
                                .otherwise(lit('')))\
                    .withColumn('TO_COUNTRY_CD',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.1, clm_mot_add2.TXT_INFO23)\
                                .otherwise(lit('')))\
                    .withColumn('OTHER_VEHICLE_NO_AND_TYPE',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 11, clm_mot_add2.TXT_INFO28)\
                                .otherwise(lit('')))\
                    .withColumn('EXECUTION_CLOSURE',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 15, clm_mot_add2.TXT_INFO3)\
                                .otherwise(lit('')))\
                    .withColumn('CONVEYANCE_DESC_CD',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.1, clm_mot_add2.TXT_INFO3)\
                                .otherwise(lit('')))\
                    .withColumn('FIR_NO',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 3, clm_mot_add2.TXT_INFO1)\
                                .when((clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 15.1)|\
                                      (clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 47.1), clm_mot_add2.TXT_INFO3)\
                                .otherwise(lit('')))\
                    .withColumn('INVOICEAMOUNT',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.7, clm_mot_add2.TXT_INFO37)\
                                .otherwise(lit('')))\
                    .withColumn('DEMAND_AMOUNT',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 11, clm_mot_add2.TXT_INFO4)\
                                .otherwise(lit('')))\
                    .withColumn('SHIPMENT_TYPE_CD',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.1, clm_mot_add2.TXT_INFO4)\
                                .otherwise(lit('')))\
                    .withColumn('OCCUPATION_OF_INJURED',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 17, clm_mot_add2.TXT_INFO5)\
                                .otherwise(lit('')))\
                    .withColumn('PETITION_AMOUNT',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 15, clm_mot_add2.TXT_INFO6)\
                                .otherwise(lit('')))\
                    .withColumn('OTHER_VEHICLE_INSURANCE_CO',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 11, clm_mot_add2.TXT_INFO6)\
                                .otherwise(lit('')))\
                    .withColumn('OFFER_AMOUNT',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 15, clm_mot_add2.TXT_INFO7)\
                                .otherwise(lit('')))\
                    .withColumn('DATE_OF_OFFER',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 15, clm_mot_add2.TXT_INFO8)\
                                .otherwise(lit('')))\
                    .withColumn('DATE_OF_RECEIPT_OF_DAR',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 15.1, clm_mot_add2.TXT_INFO9)\
                                .otherwise(lit('')))\
                    .withColumn('DATEOFADMISSION',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 9, clm_mot_add2.TXT_INFO6)\
                                .otherwise(lit('')))\
                    .withColumn('DATEOFDISCHARGE',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 9, clm_mot_add2.TXT_INFO7)\
                                .otherwise(lit('')))\
                    .withColumn('DRIVER_NAME',\
								when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 2, clm_mot_add2.TXT_INFO19))\
                    .withColumn('DRIVER_NAME_extra',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 16, clm_mot_add2.TXT_INFO29)\
                                .otherwise(lit('')))\
                    .withColumn('DRIVING_LICENCE',\
								when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 2, clm_mot_add2.TXT_INFO28))\
                    .withColumn('DRIVING_LICENCE_extra',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 16, clm_mot_add2.TXT_INFO30)\
                                .otherwise(lit('')))\
                    .withColumn('DRIVING_LICENSE_EXPIRY_DATE',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 2, clm_mot_add2.TXT_INFO30)\
                                .otherwise(lit('')))\
                    .withColumn('PROPERTY_INSRD_CD',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 21.1, clm_mot_add2.TXT_INFO1)\
                                .otherwise(lit('')))\
                    .withColumn('CLAIMANT_NAME_MOT',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 16, clm_mot_add2.TXT_INFO1)\
                                .otherwise(lit('')))\
                    .withColumn('CLASS_CD',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 2, clm_mot_add2.TXT_INFO34)\
                                .otherwise(lit('')))\
                    .withColumn('GARAGE_NAME_CD',\
                                when(clm_mot_add2.NUM_ADDITIONAL_INFO_TYPE_CD == 5, clm_mot_add2.TXT_INFO1)\
                                .otherwise(lit('')))

                    
# sagar - 20190617 driver name and licence fixed in above and below code

clm_mot_add3 = clm_mot_add3\
                    .withColumn('DRIVER_NAME',\
								when(clm_mot_add3.DRIVER_NAME.isNull(), clm_mot_add3.DRIVER_NAME_extra)\
                                .otherwise(clm_mot_add3.DRIVER_NAME))\
                    .withColumn('DRIVING_LICENCE',\
								when(clm_mot_add3.DRIVING_LICENCE.isNull(), clm_mot_add3.DRIVING_LICENCE_extra)\
                                .otherwise(clm_mot_add3.DRIVING_LICENCE))
clm_mot_add4 = clm_mot_add3\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('WAYBILLDATE_AW').alias('WAYBILLDATE_AW'),\
                        sf.max('WAYBILLDATE_RW').alias('WAYBILLDATE_RW'),\
                        sf.max('WAYBILLDATE_TW').alias('WAYBILLDATE_TW'),\
                        sf.max('WAYBILLDATE_TMW').alias('WAYBILLDATE_TMW'),\
                        sf.max('WAYBILLDATE_V').alias('WAYBILLDATE_V'),\
                        sf.max('WAYBILLDATE_WH').alias('WAYBILLDATE_WH'),\
                        sf.max('WAYBILLNO_AW').alias('WAYBILLNO_AW'),\
                        sf.max('WAYBILLNO_RW').alias('WAYBILLNO_RW'),\
                        sf.max('WAYBILLNO_TW').alias('WAYBILLNO_TW'),\
                        sf.max('WAYBILLNO_V').alias('WAYBILLNO_V'),\
                        sf.max('WAYBILLNO_WH').alias('WAYBILLNO_WH'),\
                        sf.max('CARRIER_AW').alias('CARRIER_AW'),\
                        sf.max('CARRIER_RW').alias('CARRIER_RW'),\
                        sf.max('CARRIER_TW').alias('CARRIER_TW'),\
                        sf.max('CARRIER_V').alias('CARRIER_V'),\
                        sf.max('CARRIER_WH').alias('CARRIER_WH'),\
                        sf.max('EP_NUMBER').alias('EP_NUMBER'),\
                        sf.max('SALARY_OF_INJURED').alias('SALARY_OF_INJURED'),\
                        sf.max('NO_OF_DEPENDANTS').alias('NO_OF_DEPENDANTS'),\
                        sf.max('IS_ORDER_XXI_COMPLIED').alias('IS_ORDER_XXI_COMPLIED'),\
                        sf.max('COMPLIANCE_DATE').alias('COMPLIANCE_DATE'),\
                        sf.max('FROM_CITY').alias('FROM_CITY'),\
                        sf.max('EP_YEAR').alias('EP_YEAR'),\
                        sf.max('AGE_OF_INJURED').alias('AGE_OF_INJURED'),\
                        sf.max('DATE_OF_ACCIDENT_AS_PER_FIR').alias('DATE_OF_ACCIDENT_AS_PER_FIR'),\
                        sf.max('FIR_DATE').alias('FIR_DATE'),\
                        sf.max('REPAIRER_NAME').alias('REPAIRER_NAME'),\
                        sf.max('PROPERTY_INSURED').alias('PROPERTY_INSURED'),\
                        sf.max('TO_CITY').alias('TO_CITY'),\
                        sf.max('OTHER_VEHICLE_NO_AND_TYPE').alias('OTHER_VEHICLE_NO_AND_TYPE'),\
                        sf.max('EXECUTION_CLOSURE').alias('EXECUTION_CLOSURE'),\
                        sf.max('CONVEYANCE_DESC_CD').alias('CONVEYANCE_DESC_CD'),\
                        sf.max('FIR_NO').alias('FIR_NO'),\
                        sf.max('INVOICEAMOUNT').alias('INVOICEAMOUNT'),\
                        sf.max('DEMAND_AMOUNT').alias('DEMAND_AMOUNT'),\
                        sf.max('SHIPMENT_TYPE_CD').alias('SHIPMENT_TYPE_CD'),\
                        sf.max('OCCUPATION_OF_INJURED').alias('OCCUPATION_OF_INJURED'),\
                        sf.max('PETITION_AMOUNT').alias('PETITION_AMOUNT'),\
                        sf.max('OTHER_VEHICLE_INSURANCE_CO').alias('OTHER_VEHICLE_INSURANCE_CO'),\
                        sf.max('OFFER_AMOUNT').alias('OFFER_AMOUNT'),\
                        sf.max('DATE_OF_OFFER').alias('DATE_OF_OFFER'),\
                        sf.max('DATE_OF_RECEIPT_OF_DAR').alias('DATE_OF_RECEIPT_OF_DAR'),\
                        sf.max('DATEOFADMISSION').alias('DATEOFADMISSION'),\
                        sf.max('DATEOFDISCHARGE').alias('DATEOFDISCHARGE'),\
                        sf.max('DRIVER_NAME').alias('DRIVER_NAME'),\
                        sf.max('DRIVING_LICENCE').alias('DRIVING_LICENCE'),\
                        sf.max('DRIVING_LICENSE_EXPIRY_DATE').alias('DRIVING_LICENSE_EXPIRY_DATE'),\
                        sf.max('PROPERTY_INSRD_CD').alias('PROPERTY_INSRD_CD'),\
                        sf.max('CLAIMANT_NAME_MOT').alias('CLAIMANT_NAME_MOT'),\
                        sf.max('CLASS_CD').alias('CLASS_CD'),\
                        sf.max('GARAGE_NAME_CD').alias('GARAGE_NAME_CD'),\
                        sf.max('FROM_COUNTRY_CD').alias('FROM_COUNTRY_CD'),\
                        sf.max('TO_COUNTRY_CD').alias('TO_COUNTRY_CD'))


clm_mot_add4 = clm_mot_add4\
                    .withColumn('WAYBILLDATE', concat(lit('AIRWAY - '),\
                                                      coalesce(clm_mot_add4.WAYBILLDATE_AW, lit('')),\
                                                      lit(', RAILWAY - '),\
                                                      coalesce(clm_mot_add4.WAYBILLDATE_RW, lit('')),\
                                                      lit(', TRUCKWAY - '),\
                                                      coalesce(clm_mot_add4.WAYBILLDATE_TW, lit('')),\
                                                      lit(', TMWAY - '),\
                                                      coalesce(clm_mot_add4.WAYBILLDATE_TMW, lit('')),\
                                                      lit(', VESSEL - '),\
                                                      coalesce(clm_mot_add4.WAYBILLDATE_V, lit('')),\
													  lit(', WAREHOUSE - '),\
                                                      coalesce(clm_mot_add4.WAYBILLDATE_WH, lit(''))))\
                    .withColumn('WAYBILLNO', concat(lit('AIRWAY - '),\
                                                      coalesce(clm_mot_add4.WAYBILLNO_AW, lit('')),\
                                                      lit(', RAILWAY - '),\
                                                      coalesce(clm_mot_add4.WAYBILLNO_RW, lit('')),\
                                                      lit(', TRUCKWAY - '),\
                                                      coalesce(clm_mot_add4.WAYBILLNO_TW, lit('')),\
                                                      lit(', VESSEL - '),\
                                                      coalesce(clm_mot_add4.WAYBILLNO_V, lit('')),\
													  lit(', WAREHOUSE - '),\
                                                      coalesce(clm_mot_add4.WAYBILLNO_WH, lit(''))))\
                    .withColumn('CARRIER', concat(lit('AIRWAY - '),\
                                                      coalesce(clm_mot_add4.CARRIER_AW, lit('')),\
                                                      lit(', RAILWAY - '),\
                                                      coalesce(clm_mot_add4.CARRIER_RW, lit('')),\
                                                      lit(', TRUCKWAY - '),\
                                                      coalesce(clm_mot_add4.CARRIER_TW, lit('')),\
                                                      lit(', VESSEL - '),\
                                                      coalesce(clm_mot_add4.CARRIER_V, lit('')),\
													  lit(', WAREHOUSE - '),\
                                                      coalesce(clm_mot_add4.CARRIER_WH, lit(''))))



clm_mot_add4 = clm_mot_add4\
                    .drop('WAYBILLDATE_AW')\
                    .drop('WAYBILLDATE_RW')\
                    .drop('WAYBILLDATE_TW')\
                    .drop('WAYBILLDATE_TMW')\
					.drop('WAYBILLDATE_WH')\
                    .drop('WAYBILLNO_AW')\
                    .drop('WAYBILLNO_RW')\
                    .drop('WAYBILLNO_TW')\
                    .drop('WAYBILLNO_V')\
					.drop('WAYBILLNO_WH')\
                    .drop('CARRIER_AW')\
                    .drop('CARRIER_RW')\
                    .drop('CARRIER_TW')\
					.drop('CARRIER_WH')\
                    .drop('CARRIER_V')
country_master1 = country_master\
                        .withColumnRenamed('COUNTRYCODE','FROM_COUNTRY_CD')\
                        .withColumnRenamed('COUNTRYNAME','FROM_COUNTRY')
country_master2 = country_master\
                        .withColumnRenamed('COUNTRYCODE','TO_COUNTRY_CD')\
                        .withColumnRenamed('COUNTRYNAME','TO_COUNTRY')

clm_mot_add5 = clm_mot_add4.join(country_master1,'FROM_COUNTRY_CD','left_outer')
clm_mot_add6 = clm_mot_add5.join(country_master2,'TO_COUNTRY_CD','left_outer')\
                            .drop('FROM_COUNTRY_CD','TO_COUNTRY_CD')

## join for all columns above

join_cond = [clm_00.NUM_CLAIM_NO == clm_mot_add6.NUM_CLAIM_NO]

clm_01_1 = clm_00\
            .join(clm_mot_add6, join_cond, "left_outer")\
            .drop(clm_mot_add6.NUM_CLAIM_NO)

## join for CONVEYANCE_DESC

clmmst_conveyence1 = gc_clmmst_mc_conveyence_view\
                                .groupBy('ITEM_VALUE')\
                                .agg(sf.max('ITEM_TEXT').alias('CONVEYANCE_DESC'))

join_cond = [clm_01_1.CONVEYANCE_DESC_CD == clmmst_conveyence1.ITEM_VALUE]

clm_01_2 = clm_01_1\
            .join(clmmst_conveyence1, join_cond, "left_outer")\
            .drop(clmmst_conveyence1.ITEM_VALUE)\
            .drop(clm_01_1.CONVEYANCE_DESC_CD)


## join for SHIPMENT_TYPE

clmmst_shipmenttype1 = gc_clmmst_mc_shipmenttype_view\
                                .groupBy('ITEM_VALUE')\
                                .agg(sf.max('ITEM_TEXT').alias('SHIPMENT_TYPE'))

join_cond = [clm_01_2.SHIPMENT_TYPE_CD == clmmst_shipmenttype1.ITEM_VALUE]

clm_01_3 = clm_01_2\
            .join(clmmst_shipmenttype1, join_cond, "left_outer")\
            .drop(clmmst_shipmenttype1.ITEM_VALUE)\
            .drop(clm_01_2.SHIPMENT_TYPE_CD)

clm_01_3 = clm_01_3\
                .withColumn('FROM_CITY', when(((lower(trim(clm_01_3.SHIPMENT_TYPE))=='import')|\
                                              (lower(trim(clm_01_3.SHIPMENT_TYPE))=='export')), clm_01_3.FROM_COUNTRY)
                           .otherwise(clm_01_3.FROM_CITY))\
                .withColumn('TO_CITY', when(((lower(trim(clm_01_3.SHIPMENT_TYPE))=='import')|\
                                              (lower(trim(clm_01_3.SHIPMENT_TYPE))=='export')), clm_01_3.TO_COUNTRY)
                           .otherwise(clm_01_3.TO_CITY))\
                .drop('FROM_COUNTRY','TO_COUNTRY')

## join for CLASS

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 81)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('CLASS'))

join_cond = [trim(clm_01_3.CLASS_CD) == gc_genvalue2.TXT_INFO1]

clm_01_4 = clm_01_3\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)\
            .drop(clm_01_3.CLASS_CD)

## join for GARAGE_NAME

gc_workshop1 = gc_clmmst_workshop\
                    .withColumn('NUM_WORKSHOP_CD', trim(gc_clmmst_workshop.NUM_WORKSHOP_CD))

gc_workshop2 = gc_workshop1\
                    .groupBy('NUM_WORKSHOP_CD')\
                    .agg(sf.max('TXT_WORKSHOP_NAME').alias('GARAGE_NAME'),\
                         sf.max('TXT_SKILL_AVAILABLE').alias('TXT_SKILL_AVAILABLE'),\
                         sf.max('WORKSHOP_PAN_NO').alias('WORKSHOP_PAN_NO'))

join_cond = [trim(clm_01_4.GARAGE_NAME_CD) == gc_workshop2.NUM_WORKSHOP_CD]

clm_01_5A = clm_01_4\
            .join(gc_workshop2, join_cond, "left_outer")\
            .drop(gc_workshop2.NUM_WORKSHOP_CD)\


clm_01_5B = clm_01_5A.withColumn('PAYEE_PAN_NO', \
                     when((clm_01_5A.PAYEE_SURVEYORCODE==clm_01_5A.GARAGE_NAME_CD)&\
                          (clm_01_5A.PAYEE_PAN_NO.isNull()), clm_01_5A.WORKSHOP_PAN_NO)\
                                 .otherwise(clm_01_5A.PAYEE_PAN_NO))

#PAYEE_PAN_NO modified by satya on 20191015
clm_01_5C = clm_01_5B.withColumn('PAYEE_PAN_NO', \
                     when((clm_01_5B.PAYEE_PAN_NO.isNull()), clm_01_5B.PAN_NO_INSURED)\
                     .otherwise(clm_01_5B.PAYEE_PAN_NO))
                     
clm_01_5 = clm_01_5C.drop('GARAGE_NAME_CD')\
                    .drop('WORKSHOP_PAN_NO')\
                    .drop('PAN_NO_INSURED')

# # Sagar - 20210202 - for workshop category cd

# gc_genvalue_wrkshp_catgry = gc_clmmst_generic_value\
#                         .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 113)

# gc_genvalue_wrkshp_catgry_1 = gc_genvalue_wrkshp_catgry\
#                     .withColumn('TXT_INFO1', trim(col('TXT_INFO1')))

# gc_genvalue_wrkshp_catgry_2 = gc_genvalue_wrkshp_catgry_1\
#                     .groupBy('TXT_INFO1')\
#                     .agg(sf.max('TXT_INFO2').alias('WORKSHOP_CATEGORY'))

# # Sagar - 20210202 - below chnages are for workshop_category
# join_cond = [trim(clm_01_5D.TXT_SKILL_AVAILABLE) == gc_genvalue_wrkshp_catgry_2.TXT_INFO1]

# clm_01_5 = clm_01_5D\
#             .join(gc_genvalue_wrkshp_catgry_2, join_cond, "left_outer")\
#             .drop(gc_genvalue_wrkshp_catgry_2.TXT_INFO1)

####   KPG
#change by satya on 20191115 for KPG
clm_01 = clm_01_5\
            .withColumn('KPG', when(clm_01_5.TXT_SKILL_AVAILABLE.like('%007%'), lit('N'))\
                              .otherwise(lit('')))
clm_01 = clm_01\
            .withColumn('KPG', when((clm_01.KPG == 'N') & ((trim(lower(clm_01.PAYEE_TYPE))) == 'workshop') , lit('Y'))\
                              .otherwise(clm_01.KPG))

# clm_01.count()

## join with gc_clm_mot_additional_dtls
#below logic is written in better way - 20200421
clm_mot_dtls = gc_clm_mot_additional_dtls1\
                        .withColumn('check1', concat(trim(gc_clm_mot_additional_dtls1.NUM_UPDATE_NO).cast('integer'),\
                                                     trim(gc_clm_mot_additional_dtls1.NUM_SERIAL_NO).cast('integer'),\
                                                     trim(gc_clm_mot_additional_dtls1.NUM_DETAIL_SERIAL_NO)).cast('integer'))\
                        .drop('NUM_UPDATE_NO')\
                        .drop('NUM_SERIAL_NO')\
                        .drop('NUM_DETAIL_SERIAL_NO')

clm_mot_dtls1 = clm_mot_dtls\
                    .groupBy('NUM_CLAIM_NO','NUM_ADDITIONAL_INFO_TYPE_CD')\
                    .agg(sf.max('check1').alias('check2'))

join_cond = [clm_mot_dtls.NUM_CLAIM_NO == clm_mot_dtls1.NUM_CLAIM_NO,\
             clm_mot_dtls.NUM_ADDITIONAL_INFO_TYPE_CD == clm_mot_dtls1.NUM_ADDITIONAL_INFO_TYPE_CD,\
             clm_mot_dtls.check1 == clm_mot_dtls1.check2]

clm_mot_dtls2 = clm_mot_dtls\
                        .join(clm_mot_dtls1, join_cond, "inner")\
                        .drop(clm_mot_dtls1.NUM_CLAIM_NO)\
                        .drop(clm_mot_dtls1.NUM_ADDITIONAL_INFO_TYPE_CD)\
                        .drop(clm_mot_dtls1.check2)\
                        .drop(clm_mot_dtls.check1)


clm_mot_dtls3 = clm_mot_dtls2\
                    .withColumn('IS_CASE_FIT_FOR_COMPROMISE_IN_ALL_RESPECTS',\
                                when(clm_mot_dtls2.NUM_ADDITIONAL_INFO_TYPE_CD == 15.1, clm_mot_dtls2.TXT_INFO10)
                                .otherwise(lit('')))\
                    .withColumn('DEFENCE_ATTORNEY',\
                                when(clm_mot_dtls2.NUM_ADDITIONAL_INFO_TYPE_CD == 15.1, clm_mot_dtls2.TXT_INFO12)
                                .otherwise(lit('')))\
                    .withColumn('VENUE',\
                                when(clm_mot_dtls2.NUM_ADDITIONAL_INFO_TYPE_CD == 15.1, clm_mot_dtls2.TXT_INFO2)
                                .otherwise(lit('')))\
                    .withColumn('INTIMATION_US_156',\
                                when(clm_mot_dtls2.NUM_ADDITIONAL_INFO_TYPE_CD == 15.1, clm_mot_dtls2.TXT_INFO8)
                                .otherwise(lit('')))\
                    .withColumn('DAR',\
                                when(clm_mot_dtls2.NUM_ADDITIONAL_INFO_TYPE_CD == 15.1, clm_mot_dtls2.TXT_INFO9)
                                .otherwise(lit('')))\
                    .withColumn('DEFENDANT_NAME',\
                                when(clm_mot_dtls2.NUM_ADDITIONAL_INFO_TYPE_CD == 15.1, clm_mot_dtls2.TXT_INFO5)
                                .otherwise(lit('')))\
                    .withColumn('PLAINTIFF_ATTORNEY',\
                                when(clm_mot_dtls2.NUM_ADDITIONAL_INFO_TYPE_CD == 15.1, clm_mot_dtls2.TXT_INFO13)
                                .otherwise(lit('')))\
                    .withColumn('PETITIONER_NAME',\
                                when(clm_mot_dtls2.NUM_ADDITIONAL_INFO_TYPE_CD == 15.1, clm_mot_dtls2.TXT_INFO3)
                                .otherwise(lit('')))

clm_mot_dtls4 = clm_mot_dtls3\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('IS_CASE_FIT_FOR_COMPROMISE_IN_ALL_RESPECTS').alias('IS_CASE_FIT_FOR_COMPROMISE_IN_ALL_RESPECTS'),\
                        sf.max('DEFENCE_ATTORNEY').alias('DEFENCE_ATTORNEY'),\
                        sf.max('VENUE').alias('VENUE'),\
                        sf.max('INTIMATION_US_156').alias('INTIMATION_US_156'),\
                        sf.max('DEFENDANT_NAME').alias('DEFENDANT_NAME'),\
                        sf.max('PLAINTIFF_ATTORNEY').alias('PLAINTIFF_ATTORNEY'),\
                        sf.max('PETITIONER_NAME').alias('PETITIONER_NAME'),\
                        sf.max('DAR').alias('DAR'))

## join for all columns above

join_cond = [clm_01.NUM_CLAIM_NO == clm_mot_dtls4.NUM_CLAIM_NO]

clm_02 = clm_01\
            .join(clm_mot_dtls4, join_cond, "left_outer")\
            .drop(clm_mot_dtls4.NUM_CLAIM_NO)

## join with gc_clm_assessment_summary

#below logic is written in better way - 20200421
clm_asmnt_sum = gc_clm_assessment_summary1\
                        .withColumn('check1', concat(trim(gc_clm_assessment_summary1.NUM_UPDATE_NO).cast('integer'),\
                                                          trim(gc_clm_assessment_summary1.NUM_SERIAL_NO).cast('integer')).cast('integer'))\
                        .drop('NUM_UPDATE_NO')\
                        .drop('NUM_SERIAL_NO')


clm_asmnt_sum1 = clm_asmnt_sum\
                    .groupBy('NUM_CLAIM_NO','NUM_ASSESSMENT_TYPE_CD','NUM_PAYMENT_UPDATE_NO')\
                    .agg(sf.max('check1').alias('check2'))

join_cond = [clm_asmnt_sum.NUM_CLAIM_NO == clm_asmnt_sum1.NUM_CLAIM_NO,\
             clm_asmnt_sum.NUM_ASSESSMENT_TYPE_CD == clm_asmnt_sum1.NUM_ASSESSMENT_TYPE_CD,\
             clm_asmnt_sum.NUM_PAYMENT_UPDATE_NO == clm_asmnt_sum1.NUM_PAYMENT_UPDATE_NO,\
             clm_asmnt_sum.check1 == clm_asmnt_sum1.check2]

clm_asmnt_sum2 = clm_asmnt_sum\
                        .join(clm_asmnt_sum1, join_cond, "inner")\
                        .drop(clm_asmnt_sum1.NUM_CLAIM_NO)\
                        .drop(clm_asmnt_sum1.NUM_ASSESSMENT_TYPE_CD)\
                        .drop(clm_asmnt_sum.NUM_PAYMENT_UPDATE_NO)\
                        .drop(clm_asmnt_sum1.check2)\
                        .drop(clm_asmnt_sum.check1)

#TATAAIG_GSTN_NO modification start on 20191101 by Satya
clm_asmnt_sum3 = clm_asmnt_sum2\
                    .withColumn('IS_INVOICE_IN_FAVOR_OF_TATAAIG',\
                                when(trim(clm_asmnt_sum2.NUM_ASSESSMENT_TYPE_CD) == '1.3', clm_asmnt_sum2.TXT_INFO68)
                                .otherwise(lit('')))\
                    .withColumn('TATAAIG_GSTN_NO_CD',\
                                when(trim(clm_asmnt_sum2.NUM_ASSESSMENT_TYPE_CD) == '1.3', clm_asmnt_sum2.TXT_INFO69)
                                .otherwise(lit('')))\
					.withColumn('INVOICE_DATE_INDEMNITY', \
                                when(trim(clm_asmnt_sum2.NUM_ASSESSMENT_TYPE_CD) == '1.3', clm_asmnt_sum2.TXT_INFO72)
                                .otherwise(lit('')))\
					.withColumn('INVOICE_NO_INDEMNITY', \
                                when(trim(clm_asmnt_sum2.NUM_ASSESSMENT_TYPE_CD) == '1.3', clm_asmnt_sum2.TXT_INFO71)
                                .otherwise(lit('')))\
					.withColumn('WORKSHOPGSTN_INDEMNITY', \
                                when(trim(clm_asmnt_sum2.NUM_ASSESSMENT_TYPE_CD) == '1.3', clm_asmnt_sum2.TXT_INFO70)
                                .otherwise(lit('')))\

clm_asmnt_sum4 = clm_asmnt_sum3\
                    .groupBy('NUM_CLAIM_NO','NUM_PAYMENT_UPDATE_NO')\
                    .agg(sf.max('IS_INVOICE_IN_FAVOR_OF_TATAAIG').alias('IS_INVOICE_IN_FAVOR_OF_TATAAIG'),\
                         sf.max('TATAAIG_GSTN_NO_CD').alias('TATAAIG_GSTN_NO_CD'),\
                         sf.max('INVOICE_DATE_INDEMNITY').alias('INVOICE_DATE_INDEMNITY'),\
                         sf.max('INVOICE_NO_INDEMNITY').alias('INVOICE_NO_INDEMNITY'),\
                         sf.max('WORKSHOPGSTN_INDEMNITY').alias('WORKSHOPGSTN_INDEMNITY'))

clm_asmnt_sum4 = clm_asmnt_sum4.withColumn('IS_INVOICE_IN_FAVOR_OF_TATAAIG',\
                                when(trim(lower(clm_asmnt_sum4.IS_INVOICE_IN_FAVOR_OF_TATAAIG)) == 'true', lit('Yes'))\
                                .when(trim(lower(clm_asmnt_sum4.IS_INVOICE_IN_FAVOR_OF_TATAAIG)) == 'false', lit('No'))\
                                .otherwise(lit('')))

# old(p1) TATAAIG_GSTN_NO is considered only for Expense and rest additional logic for Indemnity 20191210
clm_02 = clm_02.withColumnRenamed('TATAAIG_GSTN_NO','TATAAIG_GSTN_NO_EXPENSE')

## join for all columns above

join_cond = [clm_02.NUM_CLAIM_NO == clm_asmnt_sum4.NUM_CLAIM_NO, \
             clm_02.NUM_PAYMENT_UPDATE_NO == clm_asmnt_sum4.NUM_PAYMENT_UPDATE_NO]

clm_03_01A = clm_02\
                .join(clm_asmnt_sum4, join_cond, "left_outer")\
                .drop(clm_asmnt_sum4.NUM_CLAIM_NO)\
				.drop(clm_asmnt_sum4.NUM_PAYMENT_UPDATE_NO)\


# 20191123 below code added to populate details against indemnity record.				
clm_03_01 = clm_03_01A.withColumn('PAYEE_GSTN_NO', \
                                    when((clm_03_01A.ACCT_LINE_CD=='50')&\
									     (clm_03_01A.PAYEE_TYPE=='WORKSHOP'), clm_03_01A.WORKSHOPGSTN_INDEMNITY)\
								    .otherwise(clm_03_01A.PAYEE_GSTN_NO))\
					  .withColumn('INVOICE_DATE', \
					                when(clm_03_01A.ACCT_LINE_CD=='50', clm_03_01A.INVOICE_DATE_INDEMNITY)\
									.otherwise(clm_03_01A.INVOICE_DATE))\
					  .withColumn('INVOICE_NO', \
					                when(clm_03_01A.ACCT_LINE_CD=='50', clm_03_01A.INVOICE_NO_INDEMNITY)\
									.otherwise(clm_03_01A.INVOICE_NO))

## genmst_tab_state (genmst_tab_state)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_genmst_tab_state",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
genmst_tab_state = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TXT_GST_STATE_CD','TXT_GSTIN_NO')

## join for TATAAIG_GSTN_NO

gst1 = genmst_tab_state\
            .withColumn('TXT_GST_STATE_CD', trim(genmst_tab_state.TXT_GST_STATE_CD))

gst2 = gst1\
        .groupBy('TXT_GST_STATE_CD')\
        .agg(sf.max('TXT_GSTIN_NO').alias('TATAAIG_GSTN_NO_INDEMNITY'))

join_cond = [trim(clm_03_01.TATAAIG_GSTN_NO_CD) == gst2.TXT_GST_STATE_CD]

clm_03_02 = clm_03_01\
            .join(gst2, join_cond, "left_outer")\
            .drop(gst2.TXT_GST_STATE_CD)\
            .drop(clm_03_01.TATAAIG_GSTN_NO_CD)

clm_03 = clm_03_02.withColumn('TATAAIG_GSTN_NO', when(clm_03_02.ACCT_LINE_CD=='50', clm_03_02.TATAAIG_GSTN_NO_INDEMNITY)\
                                                 .when(clm_03_02.ACCT_LINE_CD=='55', clm_03_02.TATAAIG_GSTN_NO_EXPENSE))         
#TATAAIG_GSTN_NO modification ended on 20191101 by Satya

#below logic is written in better way - 20200421
clm_asmnt_info = gc_clm_assessment_info1\
                        .withColumn('check1', concat(trim(gc_clm_assessment_info1.NUM_UPDATE_NO).cast('integer'),\
                                                          trim(gc_clm_assessment_info1.NUM_SERIAL_NO).cast('integer')).cast('integer'))\
                        .drop('NUM_UPDATE_NO')\
                        .drop('NUM_SERIAL_NO')


clm_asmnt_info1 = clm_asmnt_info\
                    .groupBy('NUM_CLAIM_NO','NUM_ASSESSMENT_TYPE_CD')\
                    .agg(sf.max('check1').alias('check2'))

join_cond = [clm_asmnt_info.NUM_CLAIM_NO == clm_asmnt_info1.NUM_CLAIM_NO,\
             clm_asmnt_info.NUM_ASSESSMENT_TYPE_CD == clm_asmnt_info1.NUM_ASSESSMENT_TYPE_CD,\
             clm_asmnt_info.check1 == clm_asmnt_info1.check2]

clm_asmnt_info2 = clm_asmnt_info\
                        .join(clm_asmnt_info1, join_cond, "inner")\
                        .drop(clm_asmnt_info1.NUM_CLAIM_NO)\
                        .drop(clm_asmnt_info1.NUM_ASSESSMENT_TYPE_CD)\
                        .drop(clm_asmnt_info1.check2)\
                        .drop(clm_asmnt_info.check1)

clm_asmnt_info3 = clm_asmnt_info2\
                    .withColumn('HANDLING_FEE_AMT',\
                                when(clm_asmnt_info2.NUM_ASSESSMENT_TYPE_CD == 1.2, clm_asmnt_info2.TXT_INFO34)
                                .otherwise(lit('')))\
                    .withColumn('SETTLEMENT_TYPE',\
                                when(clm_asmnt_info2.NUM_ASSESSMENT_TYPE_CD == 4.7, clm_asmnt_info2.TXT_INFO6)
                                .otherwise(lit('')))\
                    .withColumn('INVOICE_NO_AI',\
                                when(clm_asmnt_info2.NUM_ASSESSMENT_TYPE_CD == 7.1, clm_asmnt_info2.TXT_INFO16)
                                .otherwise(lit('')))\
                    .withColumn('SAC_CODE_OLD',\
                                when(clm_asmnt_info2.NUM_ASSESSMENT_TYPE_CD == 1.1, clm_asmnt_info2.TXT_INFO30)
                                .otherwise(lit('')))\
                    .withColumn('INVOICE_DATE_AI',\
                                when(clm_asmnt_info2.NUM_ASSESSMENT_TYPE_CD == 7.1, clm_asmnt_info2.TXT_INFO17)
                                .otherwise(lit('')))

#SAC_CODE new logic added in p1 by satya on 20191024

clm_asmnt_info4 = clm_asmnt_info3\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('HANDLING_FEE_AMT').alias('HANDLING_FEE_AMT'),\
                        sf.max('INVOICE_NO_AI').alias('INVOICE_NO_AI'),\
                        sf.max('INVOICE_DATE_AI').alias('INVOICE_DATE_AI'),\
                       sf.max('SAC_CODE_OLD').alias('SAC_CODE_OLD'),\
                        sf.max('SETTLEMENT_TYPE').alias('SETTLEMENT_TYPE'))

## join for all columns above

join_cond = [clm_03.NUM_CLAIM_NO == clm_asmnt_info4.NUM_CLAIM_NO]

clm_17 = clm_03\
            .join(clm_asmnt_info4, join_cond, "left_outer")\
            .drop(clm_asmnt_info4.NUM_CLAIM_NO)

clm_17 = clm_17\
            .withColumn('SAC_CODE', when(clm_17.ACCT_LINE_CD == '55', clm_17.SAC_CODE)\
                                     .otherwise(lit('')))

clm_17 = clm_17\
            .drop('INVOICE_NO_AI')\
            .drop('INVOICE_DATE_AI')\
            .drop('WORKSHOPGSTN_INDEMNITY')\
            .drop('INVOICE_NO_INDEMNITY')\
            .drop('INVOICE_DATE_INDEMNITY')

## join with gc_clm_activity_checklist

activity_temp0 = gc_clm_activity_checklist1\
                        .withColumn('TXT_ACTIVITY', lower(trim(gc_clm_activity_checklist1.TXT_ACTIVITY)))

activity_temp = activity_temp0\
                    .groupBy('NUM_CLAIM_NO', 'TXT_ACTIVITY')\
                    .agg(sf.max('DAT_INSERT_DATE').alias('DAT_INSERT_DATE'))

## Report Column - Feature Activity: First Contact

activity_temp1 = activity_temp\
                        .filter(activity_temp.TXT_ACTIVITY == 'first contact')

activity_temp2 = activity_temp1.drop('TXT_ACTIVITY')

join_cond = [clm_17.NUM_CLAIM_NO == activity_temp2.NUM_CLAIM_NO]

clm_18_1 = clm_17\
            .join(activity_temp2, join_cond, "left_outer")\
            .withColumnRenamed('DAT_INSERT_DATE', 'FEATURE_ACTIVITY_FIRST_CONTACT')\
            .drop(activity_temp2.NUM_CLAIM_NO)

## Report Column - FIRST_FOLLOW_UP

activity_temp1 = activity_temp\
                        .filter(activity_temp.TXT_ACTIVITY == '1st follow up')

activity_temp2 = activity_temp1.drop('TXT_ACTIVITY')

join_cond = [clm_18_1.NUM_CLAIM_NO == activity_temp2.NUM_CLAIM_NO]

clm_18_2 = clm_18_1\
            .join(activity_temp2, join_cond, "left_outer")\
            .withColumnRenamed('DAT_INSERT_DATE', 'FIRST_FOLLOW_UP')\
            .drop(activity_temp2.NUM_CLAIM_NO)

## Report Column - SECOND_FOLLOW_UP

activity_temp1 = activity_temp\
                        .filter(activity_temp.TXT_ACTIVITY == '2nd follow up')

activity_temp2 = activity_temp1.drop('TXT_ACTIVITY')

join_cond = [clm_18_2.NUM_CLAIM_NO == activity_temp2.NUM_CLAIM_NO]

clm_18_3 = clm_18_2\
            .join(activity_temp2, join_cond, "left_outer")\
            .withColumnRenamed('DAT_INSERT_DATE', 'SECOND_FOLLOW_UP')\
            .drop(activity_temp2.NUM_CLAIM_NO)

## Report Column - THIRD_FOLLOW_UP

activity_temp1 = activity_temp\
                        .filter(activity_temp.TXT_ACTIVITY == '3rd follow up')

activity_temp2 = activity_temp1.drop('TXT_ACTIVITY')

join_cond = [clm_18_3.NUM_CLAIM_NO == activity_temp2.NUM_CLAIM_NO]

clm_18 = clm_18_3\
            .join(activity_temp2, join_cond, "left_outer")\
            .withColumnRenamed('DAT_INSERT_DATE', 'THIRD_FOLLOW_UP')\
            .drop(activity_temp2.NUM_CLAIM_NO)

## Report Column - Feature Activity: All Doc Recd

activity_temp1 = activity_temp\
                        .filter(activity_temp.TXT_ACTIVITY == 'all documents received')

activity_temp_adr = activity_temp1.drop('TXT_ACTIVITY')

join_cond = [clm_18.NUM_CLAIM_NO == activity_temp_adr.NUM_CLAIM_NO]

clm_20_1 = clm_18\
            .join(activity_temp_adr, join_cond, "left_outer")\
            .withColumnRenamed('DAT_INSERT_DATE', 'FEATURE_ACTIVITY_ALL_DOC_RECD')\
            .drop(activity_temp_adr.NUM_CLAIM_NO)

## Report Column - CLAIM_FORM_GIVEN

## activity_temp1 = activity_temp\activity_temp
##                         .filter(activity_temp.TXT_ACTIVITY == 'claim form given')
##                         (activity_temp.SURVEYOR_APPOINTMENT_DATE <= report_date)   ## This filter was newly added
    
activity_temp1 = activity_temp\
                         .filter((lower(activity_temp.TXT_ACTIVITY) == 'claim form given')&\
                        (activity_temp.DAT_INSERT_DATE <= report_date))

activity_temp2 = activity_temp1.drop('TXT_ACTIVITY')

join_cond = [clm_20_1.NUM_CLAIM_NO == activity_temp2.NUM_CLAIM_NO]

clm_20_2 = clm_20_1\
            .join(activity_temp2, join_cond, "left_outer")\
            .withColumnRenamed('DAT_INSERT_DATE', 'CLAIM_FORM_GIVEN')\
            .drop(activity_temp2.NUM_CLAIM_NO)

## Report Column - Feature Activity (Yes/ No)

activity_templf = activity_temp\
                        .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('DAT_INSERT_DATE').alias('DAT_INSERT_DATE'))

activity_threshold_date = report_date - datetime.timedelta(days=60)

activity_temp2 = activity_templf\
                        .filter(activity_templf.DAT_INSERT_DATE >= activity_threshold_date)

join_cond = [clm_20_2.NUM_CLAIM_NO == activity_temp2.NUM_CLAIM_NO]

clm_20 = clm_20_2\
            .join(activity_temp2, join_cond, "left_outer")\
            .withColumnRenamed('DAT_INSERT_DATE', 'ANY_FEATURE_ACTIVITY')\
            .drop(activity_temp2.NUM_CLAIM_NO)

clm_21 = clm_20\
            .withColumn('ANY_FEATURE_ACTIVITY', when(clm_20.ANY_FEATURE_ACTIVITY.isNotNull(), lit('Y'))\
                                                .otherwise(lit('N')))

## Report Column - All_Docs_Received

activity_temp0 = gc_clm_activity_checklist1\
                        .filter(gc_clm_activity_checklist1.TXT_ACTIVITY == 'all documents received')

activity_temp00 = activity_temp0.drop('TXT_ACTIVITY')

join_cond = [activity_temp_adr.NUM_CLAIM_NO == activity_temp00.NUM_CLAIM_NO,\
             activity_temp_adr.DAT_INSERT_DATE == activity_temp00.DAT_INSERT_DATE]

activity_temp1 = activity_temp_adr\
                    .join(activity_temp00, join_cond, "inner")\
                    .drop(activity_temp00.NUM_CLAIM_NO)\
                    .drop(activity_temp00.DAT_INSERT_DATE)

activity_temp2 = activity_temp1\
                        .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('TXT_CHECK_STATUS').alias('ALL_DOCS_RECEIVED'))


clm_21.createOrReplaceTempView('clm_21_view')
activity_temp2.createOrReplaceTempView('activity_temp2_view')

query ="select a.*,b.ALL_DOCS_RECEIVED from clm_21_view a left join activity_temp2_view b on a.NUM_CLAIM_NO = b.NUM_CLAIM_NO"
clm_22 = sqlContext.sql(query)
sqlContext.dropTempTable('clm_21_view')
sqlContext.dropTempTable('activity_temp2_view')

## Report Column - LAST_FEATURE_CHANGE

activity_temp0 = gc_clm_activity_checklist1\
                        .withColumnRenamed('TXT_ACTIVITY', 'LAST_FEATURE_CHANGE')\
                        .drop('TXT_CHECK_STATUS')

join_cond = [activity_templf.NUM_CLAIM_NO == activity_temp0.NUM_CLAIM_NO,\
             activity_templf.DAT_INSERT_DATE == activity_temp0.DAT_INSERT_DATE]

activity_temp1 = activity_templf\
                    .join(activity_temp0, join_cond, "inner")\
                    .drop(activity_temp0.NUM_CLAIM_NO)\
                    .drop(activity_temp0.DAT_INSERT_DATE)

activity_temp2 = activity_temp1\
                        .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('LAST_FEATURE_CHANGE').alias('LAST_FEATURE_CHANGE'))

clm_22.createOrReplaceTempView('clm_22_view')
activity_temp2.createOrReplaceTempView('activity_temp2_view')

query ="select a.*,b.LAST_FEATURE_CHANGE from clm_22_view a left join activity_temp2_view b on a.NUM_CLAIM_NO = b.NUM_CLAIM_NO"
clm_23 = sqlContext.sql(query)
sqlContext.dropTempTable('clm_22_view')
sqlContext.dropTempTable('activity_temp2_view')
# clm_23 = clm_22\
#             .join(activity_temp2, join_cond, "left_outer")\
#             .drop(activity_temp2.NUM_CLAIM_NO)

## Report Column - CURRENT_STATUS
activity_temp3 = gc_clm_activity_checklist1\
                        .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('checklist_check').alias('checklist_check'))
activity_temp4 = activity_temp3.join(gc_clm_activity_checklist1,['NUM_CLAIM_NO','checklist_check'])
activity_temp5 = activity_temp4\
                        .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('TXT_ACTIVITY').alias('CURRENT_STATUS'))
clm_23 = clm_23.join(activity_temp5,'NUM_CLAIM_NO','left_outer')

## join with gc_clm_claim_audit

clm_claim_audit1 = gc_clm_claim_audit\
                    .withColumn('ACCORDANCE_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 2, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('SUBROGATION_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 1, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('FILE_NO',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 1, gc_clm_claim_audit.TXT_FILE_NO)
                                .otherwise(lit('')))\
                    .withColumn('ACCORDANCE_COMMENTS',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 1, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('COV_CONFIRMATION_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 4, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('RESERVE_APPROVAL_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 5, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('EXCESS_EXPOSURE_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 6, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('RESERVE_POSTING_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 7, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('COVRAGE_RESERV_COMNTS',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 8, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('FILE_INSUIT_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 9, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('LITIGATION_COSTS_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 10, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('ACTIVE_CASEMGT_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 11, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('COUNSEL_QUALITY_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 12, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('LITIGATION_COMMENTS',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 13, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('CLAIM_REIN_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 14, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('REIN_LOSSNOTICE_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 15, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('TIMELY_LOSSNOTIFY_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 15, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('REIN_DOCUMENTATION_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 16, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('REIN_PROOFOFLOSS_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 17, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('REINSURANCE_COMMENTS',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 18, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('DIARYDATES_UTIL_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 19, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('INVESTIGATION_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 20, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('TP_RECOG_NOTICE_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 22, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('REPORTING_DEVELOPMENTS_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 23, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('DOCUMENTATION_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 24, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('EXPERTS_UTIL_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 25, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('AGGRESSIVE_PURSUIT_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 26, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('PROMPT_CORRESPONDENCE_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 27, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('LOSS_ACKNOWLEDGED_DT',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 28, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('INDEMN_EXP_PAYMENT_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 29, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('EVIDENCE_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 30, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('QUALITY_INPUT_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 31, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('ACCOUNT_CLAIMBULLETIN_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 32, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('EVALUATION_COMMENTS',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 34, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('OVERALLEVAL_HANDLING_FG',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 34, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))\
                    .withColumn('GENERAL_REMARKS',\
                                when(gc_clm_claim_audit.NUM_CLM_AUD_QUSTN_ID == 35, gc_clm_claim_audit.TXT_CLM_AUD_QUSTN_ANSWR)
                                .otherwise(lit('')))


clm_claim_audit2 = clm_claim_audit1\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('ACCORDANCE_FG').alias('ACCORDANCE_FG'),\
                        sf.max('SUBROGATION_FG').alias('SUBROGATION_FG'),\
                        sf.max('FILE_NO').alias('FILE_NO'),\
                        sf.max('ACCORDANCE_COMMENTS').alias('ACCORDANCE_COMMENTS'),\
                        sf.max('COV_CONFIRMATION_FG').alias('COV_CONFIRMATION_FG'),\
                        sf.max('RESERVE_APPROVAL_FG').alias('RESERVE_APPROVAL_FG'),\
                        sf.max('EXCESS_EXPOSURE_FG').alias('EXCESS_EXPOSURE_FG'),\
                        sf.max('RESERVE_POSTING_FG').alias('RESERVE_POSTING_FG'),\
                        sf.max('COVRAGE_RESERV_COMNTS').alias('COVRAGE_RESERV_COMNTS'),\
                        sf.max('FILE_INSUIT_FG').alias('FILE_INSUIT_FG'),\
                        sf.max('LITIGATION_COSTS_FG').alias('LITIGATION_COSTS_FG'),\
                        sf.max('ACTIVE_CASEMGT_FG').alias('ACTIVE_CASEMGT_FG'),\
                        sf.max('COUNSEL_QUALITY_FG').alias('COUNSEL_QUALITY_FG'),\
                        sf.max('LITIGATION_COMMENTS').alias('LITIGATION_COMMENTS'),\
                        sf.max('CLAIM_REIN_FG').alias('CLAIM_REIN_FG'),\
                        sf.max('REIN_LOSSNOTICE_FG').alias('REIN_LOSSNOTICE_FG'),\
                        sf.max('TIMELY_LOSSNOTIFY_FG').alias('TIMELY_LOSSNOTIFY_FG'),\
                        sf.max('REIN_DOCUMENTATION_FG').alias('REIN_DOCUMENTATION_FG'),\
                        sf.max('REIN_PROOFOFLOSS_FG').alias('REIN_PROOFOFLOSS_FG'),\
                        sf.max('REINSURANCE_COMMENTS').alias('REINSURANCE_COMMENTS'),\
                        sf.max('DIARYDATES_UTIL_FG').alias('DIARYDATES_UTIL_FG'),\
                        sf.max('INVESTIGATION_FG').alias('INVESTIGATION_FG'),\
                        sf.max('TP_RECOG_NOTICE_FG').alias('TP_RECOG_NOTICE_FG'),\
                        sf.max('REPORTING_DEVELOPMENTS_FG').alias('REPORTING_DEVELOPMENTS_FG'),\
                        sf.max('DOCUMENTATION_FG').alias('DOCUMENTATION_FG'),\
                        sf.max('EXPERTS_UTIL_FG').alias('EXPERTS_UTIL_FG'),\
                        sf.max('AGGRESSIVE_PURSUIT_FG').alias('AGGRESSIVE_PURSUIT_FG'),\
                        sf.max('PROMPT_CORRESPONDENCE_FG').alias('PROMPT_CORRESPONDENCE_FG'),\
                        sf.max('LOSS_ACKNOWLEDGED_DT').alias('LOSS_ACKNOWLEDGED_DT'),\
                        sf.max('INDEMN_EXP_PAYMENT_FG').alias('INDEMN_EXP_PAYMENT_FG'),\
                        sf.max('EVIDENCE_FG').alias('EVIDENCE_FG'),\
                        sf.max('QUALITY_INPUT_FG').alias('QUALITY_INPUT_FG'),\
                        sf.max('ACCOUNT_CLAIMBULLETIN_FG').alias('ACCOUNT_CLAIMBULLETIN_FG'),\
                        sf.max('EVALUATION_COMMENTS').alias('EVALUATION_COMMENTS'),\
                        sf.max('OVERALLEVAL_HANDLING_FG').alias('OVERALLEVAL_HANDLING_FG'),\
                        sf.max('GENERAL_REMARKS').alias('GENERAL_REMARKS'))

## join for all columns above

join_cond = [clm_23.NUM_CLAIM_NO == clm_claim_audit2.NUM_CLAIM_NO]

clm_24 = clm_23\
            .join(clm_claim_audit2, join_cond, "left_outer")\
            .drop(clm_claim_audit2.NUM_CLAIM_NO)

### join with risk_headers

risk_h = risk_headers1\
            .groupBy('REFERENCE_NUM')\
            .agg(sf.max('INFORMATION49').alias('SUB_PRODUCT_TYPE'),\
                 sf.max('RISK1').alias('POLICY_TYPE'),\
                 sf.max('INFORMATION3').alias('INFORMATION3'),\
                 sf.max('INFORMATION4').alias('INFORMATION4'),\
                 sf.max('INFORMATION5').alias('INFORMATION5'),\
                 sf.max('INFORMATION6').alias('INFORMATION6'),\
                 sf.max('INFORMATION7').alias('INFORMATION7'),\
                 sf.max('INFORMATION8').alias('INFORMATION8'),\
                 sf.max('INFORMATION9').alias('INFORMATION9'),\
                 sf.max('INFORMATION10').alias('INFORMATION10'),\
                 sf.max('INFORMATION11').alias('INFORMATION11'),\
                 sf.max('INFORMATION12').alias('INFORMATION12'),\
                 sf.max('INFORMATION14').alias('INFORMATION14'),\
                 sf.max('INFORMATION15').alias('INFORMATION15'),\
                 sf.max('INFORMATION16').alias('INFORMATION16'),\
                 sf.max('INFORMATION17').alias('INFORMATION17'),\
                 sf.max('INFORMATION19').alias('INFORMATION19'),\
                 sf.max('INFORMATION21').alias('INFORMATION21'),\
                 sf.max('INFORMATION22').alias('INFORMATION22'),\
                 sf.max('INFORMATION23').alias('INFORMATION23'),\
                 sf.max('INFORMATION24').alias('INFORMATION24'),\
                 sf.max('INFORMATION33').alias('INFORMATION33'),\
                 sf.max('INFORMATION34').alias('INFORMATION34'),\
                 sf.max('INFORMATION35').alias('INFORMATION35'),\
                 sf.max('INFORMATION36').alias('INFORMATION36'),\
                 sf.max('INFORMATION37').alias('INFORMATION37'),\
                 sf.max('INFORMATION38').alias('INFORMATION38'),\
                 sf.max('INFORMATION47').alias('INFORMATION47'),\
                 sf.max('INFORMATION59').alias('INFORMATION59'),\
                 sf.max('INFORMATION65').alias('INFORMATION65'),\
                 sf.max('INFORMATION102').alias('INFORMATION102'),\
                 sf.max('INFORMATION104').alias('INFORMATION104'),\
                 sf.max('INFORMATION106').alias('INFORMATION106'),\
                 sf.max('INFORMATION114').alias('INFORMATION114'),\
                 sf.max('INFORMATION182').alias('INFORMATION182'),\
                 sf.max('INFORMATION225').alias('INFORMATION225'),\
                 sf.max('INFORMATION273').alias('SI_YEAR1'),\
                 sf.max('INFORMATION274').alias('SI_YEAR2'),\
                 sf.max('INFORMATION275').alias('SI_YEAR3'),\
                 sf.max('INFORMATION276').alias('SI_YEAR4'),\
                 sf.max('INFORMATION277').alias('SI_YEAR5'))

join_cond = [clm_24.NUM_REFERENCE_NO == risk_h.REFERENCE_NUM]

clm_25 = clm_24\
            .join(risk_h, join_cond, 'left_outer')\
            .drop(risk_h.REFERENCE_NUM)

clm_25 = clm_25\
            .withColumn('VEH_MAKE_NAME', when(clm_25.PRODUCT_CD == '3121', clm_25.INFORMATION38)\
                                        .when(clm_25.PRODUCT_CD == '3122', clm_25.INFORMATION22)\
                                        .when(clm_25.PRODUCT_CD == '3124', clm_25.INFORMATION36)\
                                        .when(clm_25.PRODUCT_CD == '3184', clm_25.INFORMATION3)\
                                        .otherwise(lit('')))\
            .withColumn('VEH_MDL_NAME',\
                            when(clm_25.PRODUCT_CD == '3121', concat(coalesce(clm_25.INFORMATION36, lit('')),\
                                                                     lit('-'),\
                                                                     coalesce(clm_25.INFORMATION114, lit(''))))\
                            .when(clm_25.PRODUCT_CD == '3122', concat(coalesce(clm_25.INFORMATION23, lit('')),\
                                                                     lit('-'),\
                                                                     coalesce(clm_25.INFORMATION24, lit(''))))\
                            .when(clm_25.PRODUCT_CD == '3124', concat(coalesce(clm_25.INFORMATION5, lit('')),\
                                                                     lit('-'),\
                                                                     coalesce(clm_25.INFORMATION182, lit(''))))\
                            .when(clm_25.PRODUCT_CD == '3184', clm_25.INFORMATION4)\
                            .otherwise(lit('')))\
            .withColumn('IDV', when(clm_25.PRODUCT_CD == '3121', clm_25.INFORMATION8)\
                                        .when(clm_25.PRODUCT_CD == '3122', clm_25.INFORMATION65)\
                                        .when(clm_25.PRODUCT_CD == '3124', clm_25.INFORMATION19)\
                                        .otherwise(lit('')))\
            .withColumn('CHASSIS_NUMBER', when(clm_25.PRODUCT_CD == '3121', clm_25.INFORMATION16)\
                                        .when(clm_25.PRODUCT_CD == '3122', clm_25.INFORMATION38)\
                                        .when(clm_25.PRODUCT_CD == '3124', clm_25.INFORMATION17)\
                                        .when(clm_25.PRODUCT_CD == '3184', clm_25.INFORMATION7)\
                                        .otherwise(lit('')))\
            .withColumn('ENGINE_NUMBER', when(clm_25.PRODUCT_CD == '3121', clm_25.INFORMATION14)\
                                        .when(clm_25.PRODUCT_CD == '3122', clm_25.INFORMATION37)\
                                        .when(clm_25.PRODUCT_CD == '3124', clm_25.INFORMATION16)\
                                        .when(clm_25.PRODUCT_CD == '3184', clm_25.INFORMATION6)\
                                        .otherwise(lit('')))\
            .withColumn('VEHICLE_REGISTRATION_NUMBER',\
                            when(clm_25.PRODUCT_CD == '3121', concat(coalesce(trim(clm_25.INFORMATION6), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION102), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION104), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION106), lit(''))))\
                            .when(clm_25.PRODUCT_CD == '3122', concat(coalesce(trim(clm_25.INFORMATION33), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION34), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION35), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION36), lit(''))))\
                            .when(clm_25.PRODUCT_CD == '3124', concat(coalesce(trim(clm_25.INFORMATION15), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION21), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION22), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION23), lit(''))))\
                            .when(clm_25.PRODUCT_CD == '3184', concat(coalesce(trim(clm_25.INFORMATION9), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION10), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION11), lit('')),\
                                                                     coalesce(trim(clm_25.INFORMATION12), lit(''))))\
                            .otherwise(lit('')))\
            .withColumn('SUB_PRODUCT_TYPE', when(clm_25.PRODUCT_CD == '3124', clm_25.SUB_PRODUCT_TYPE)\
                                           .otherwise(lit('')))\
            .withColumn('POLICY_TYPE', when(clm_25.NUM_DEPARTMENT_CODE == '31', clm_25.POLICY_TYPE)\
                                      .otherwise(clm_25.PRODUCT_NAME))

clm_26 = clm_25.withColumn('TYPE', clm_25.POLICY_TYPE)

clm_25 = clm_26\
            .drop('INFORMATION3')\
            .drop('INFORMATION4')\
            .drop('INFORMATION5')\
            .drop('INFORMATION6')\
            .drop('INFORMATION7')\
            .drop('INFORMATION8')\
            .drop('INFORMATION9')\
            .drop('INFORMATION10')\
            .drop('INFORMATION11')\
            .drop('INFORMATION12')\
            .drop('INFORMATION14')\
            .drop('INFORMATION15')\
            .drop('INFORMATION16')\
            .drop('INFORMATION17')\
            .drop('INFORMATION19')\
            .drop('INFORMATION21')\
            .drop('INFORMATION22')\
            .drop('INFORMATION23')\
            .drop('INFORMATION24')\
            .drop('INFORMATION33')\
            .drop('INFORMATION34')\
            .drop('INFORMATION35')\
            .drop('INFORMATION36')\
            .drop('INFORMATION37')\
            .drop('INFORMATION38')\
            .drop('INFORMATION47')\
            .drop('INFORMATION59')\
            .drop('INFORMATION65')\
            .drop('INFORMATION102')\
            .drop('INFORMATION104')\
            .drop('INFORMATION106')\
            .drop('INFORMATION114')\
            .drop('INFORMATION182')\
            .drop('INFORMATION225')

### join with risk_details

# to read only latest record based dat_insert_date. fixed on 20191123 by Sagar. for Advice May. Sample CFN 0821427134A
risk_details1.createOrReplaceTempView('risk_details1_view')

sql_risk = """
select REFERENCE_NUM, INFORMATION34,INFORMATION33,INFORMATION20,INFORMATION12,INFORMATION26,INFORMATION25,INFORMATION24
from 
(select REFERENCE_NUM, INFORMATION34,INFORMATION33,INFORMATION20,INFORMATION12,INFORMATION26,INFORMATION25,INFORMATION24,
row_number() over (partition by REFERENCE_NUM order by DAT_INSERT_DATE desc) as rnk
from risk_details1_view )
where rnk = 1

"""
risk_details2 = sqlContext.sql(sql_risk)
sqlContext.dropTempTable('risk_details1_view')

risk_d1 = risk_details2\
            .groupBy('REFERENCE_NUM')\
            .agg(sf.max('INFORMATION34').alias('SUB_SIC_DESC_R'),\
                 sf.max('INFORMATION33').alias('SIC_DESC_R'),\
                 sf.max('INFORMATION20').alias('SIC_CODE_R'),\
                 sf.max('INFORMATION12').alias('SUB_SIC_DESC_R1'),\
                 sf.max('INFORMATION26').alias('SIC_DESC_R1'),\
                 sf.max('INFORMATION25').alias('SIC_CODE_R1'),\
                 sf.max('INFORMATION24').alias('SIC_CODE_R2'))

risk_d = risk_d1\
            .withColumn('SUB_SIC_DESC_R', when(trim(risk_d1.SIC_CODE_R) == '0', risk_d1.SUB_SIC_DESC_R1)\
                                         .otherwise(risk_d1.SUB_SIC_DESC_R))\
            .withColumn('SIC_DESC_R', when(trim(risk_d1.SIC_CODE_R) == '0', risk_d1.SIC_DESC_R1)\
                                         .otherwise(risk_d1.SIC_DESC_R))\
            .withColumn('SIC_CODE_R', when(trim(risk_d1.SIC_CODE_R) == '0', risk_d1.SIC_CODE_R1)\
                                         .otherwise(risk_d1.SIC_CODE_R))


join_cond = [clm_25.NUM_REFERENCE_NO == risk_d.REFERENCE_NUM]

clm_46_1 = clm_25\
                .join(risk_d, join_cond, 'left_outer')\
                .drop(risk_d.REFERENCE_NUM)

clm_46_2 = clm_46_1\
                .withColumn('SIC_CODE_R', when(trim(clm_46_1.PRODUCT_CD) == '2159', clm_46_1.SIC_CODE_R2)\
                                         .otherwise(clm_46_1.SIC_CODE_R))

clm_46 = clm_46_2\
            .withColumn('SUB_SIC_CD_R', clm_46_2.SIC_CODE_R)

## Report Column - COVERAGE_DESC

join_cond = [clm_46.COVERAGE_CODE == covercodemaster.NUM_COVER_CODE]

clm_47_1 = clm_46\
            .join(covercodemaster, join_cond, 'left_outer')\
            .drop(covercodemaster.NUM_COVER_CODE)


# gc_genvalue1 = gc_clmmst_generic_value\
#                         .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 204)

# gc_genvalue1 = gc_genvalue1\
#                     .withColumn('TXT_INFO5', trim(gc_genvalue1.TXT_INFO5))

# gc_genvalue2 = gc_genvalue1\
#                     .groupBy('TXT_INFO5')\
#                     .agg(sf.max('TXT_INFO3').alias('COVERAGE_DESC_2'))

# join_cond = [trim(clm_47_1.COVERAGE_CODE) == gc_genvalue2.TXT_INFO5]

# clm_47_2 = clm_47_1\
#             .join(gc_genvalue2, join_cond, "left_outer")\
#             .drop(gc_genvalue2.TXT_INFO5)

# clm_47 = clm_47_2\
#             .withColumn('COVERAGE_DESC', when(clm_47_2.COVERAGE_DESC.isNull(), clm_47_2.COVERAGE_DESC_2)\
#                                         .otherwise(clm_47_2.COVERAGE_DESC))\
#             .drop('COVERAGE_DESC_2')


cover_grp_map1 = cnfgtr_cover_grp_map\
                    .withColumn('COVERCODE', trim(cnfgtr_cover_grp_map.COVERCODE))

cover_grp_map2 = cover_grp_map1\
                    .groupBy('COVERCODE')\
                    .agg(sf.max('COVERGRPINDX').alias('COVERGRPINDX'))

join_cond = [trim(clm_47_1.COVERAGE_CODE) == cover_grp_map2.COVERCODE]

clm_47_2 = clm_47_1\
            .join(cover_grp_map2, join_cond, "left_outer")\
            .drop(cover_grp_map2.COVERCODE)

cover_grp_mstr1 = cnfgtr_cover_grp_mstr\
                    .withColumn('COVERGRPINDX', trim(cnfgtr_cover_grp_mstr.COVERGRPINDX))

cover_grp_mstr2 = cover_grp_mstr1\
                    .groupBy('COVERGRPINDX')\
                    .agg(sf.max('COVERGROUPNAME').alias('COVERAGE_DESC_2'))

join_cond = [trim(clm_47_2.COVERGRPINDX) == cover_grp_mstr2.COVERGRPINDX]

clm_47_3 = clm_47_2\
            .join(cover_grp_mstr2, join_cond, "left_outer")\
            .drop(cover_grp_mstr2.COVERGRPINDX)

clm_47 = clm_47_3\
            .withColumn('COVERAGE_DESC', when(clm_47_3.COVERAGE_DESC.isNull(), clm_47_3.COVERAGE_DESC_2)\
                                        .otherwise(clm_47_3.COVERAGE_DESC))\
            .drop('COVERAGE_DESC_2')


clm_47 = clm_47\
            .withColumnRenamed('ACCT_LINE_CD','ACCOUNT_LINE_CD')

clm_47 = clm_47\
            .withColumn('ACCOUNT_LINE_TXT', when(trim(clm_47.ACCOUNT_LINE_CD) == '50', lit('INDEMNITY'))\
                                           .when(trim(clm_47.ACCOUNT_LINE_CD) == '55', lit('EXPENSE'))\
                                           .otherwise(lit('')))\
            .withColumn('CURRENCY_CD', lit('INR'))

## Report Column - SUBPRODUCT_CD

clm_47 = clm_47\
            .withColumn('pcd_length', LengthOfString(clm_47.PRODUCT_CD))

clm_47 = clm_47\
        .withColumn('SUBPRODUCT_CD', when((clm_47.pcd_length >= 2) & (clm_25.MAJOR_LINE_CD == '01')\
                                          , lit(firstTwoChar(clm_47.PRODUCT_CD)))\
                                       .otherwise(lit('')))

clm_47 = clm_47.drop('pcd_length')

## Report Column - product_type_subproduct_type
join_cond = [trim(clm_47.PRODUCT_CD) == sub_product_master_base.PRODUCT_CD]

clm_47 = clm_47\
            .join(sub_product_master_base, join_cond, "left_outer")\
            .drop(sub_product_master_base.PRODUCT_CD)
        

join_cond = [trim(clm_47.PRODUCT_CD) == product_type_master_base.PRODUCT_CD]

clm_47 = clm_47\
            .join(product_type_master_base, join_cond, "left_outer")\
            .drop(product_type_master_base.PRODUCT_CD)
        
##################################################################
# satya-20190618-product_type_subproduct_type ends here

clm_47 = clm_47\
            .withColumn('pol_length', LengthOfString(clm_47.POL_ISS_OFF_CD))

clm_47 = clm_47\
        .withColumn('POL_ISS_OFF_CD_WR', when(clm_47.pol_length > 1, lit(afterOneChar(clm_47.POL_ISS_OFF_CD)))\
                                         .otherwise(lit('')))

clm_47 = clm_47\
            .drop('pol_length')


clm_47 = clm_47\
            .withColumn('sof_length', LengthOfString(clm_47.SETTLING_OFF_CD))

clm_47 = clm_47\
        .withColumn('SETTLING_OFF_CD_WR', when(clm_47.sof_length > 2, lit(afterOneChar(clm_47.SETTLING_OFF_CD)))\
                                         .otherwise(lit('')))

clm_47 = clm_47\
            .drop('sof_length')

clm_47 = clm_47\
            .withColumn('AUTHORISED_BY', clm_47.APPROVER_ID)\
            .withColumn('PROD_OFF_CD', concat(firstTwoChar(clm_47.POL_ISS_OFF_CD_WR),lit('00')))\
            .withColumn('PROD_OFF_SUB_CD', afterOneChar(clm_47.POL_ISS_OFF_CD_WR))

clm_48 = clm_47
cnfgtr_d_at1 = cnfgtr_d_all_transactions1\
                    .groupBy('TRANS_ID')\
                    .agg(sf.max('STATUS').alias('CNFGTR_TRANS_STATUS'))

join_cond = [clm_48.NUM_REFERENCE_NO == cnfgtr_d_at1.TRANS_ID]

clm_49 = clm_48\
            .join(cnfgtr_d_at1, join_cond, "left_outer")\
            .drop(cnfgtr_d_at1.TRANS_ID)

# clm_49.count()

cnfgtr_grid_tab = cnfgtr_otherdtls_grid_tab\
                        .groupBy('NUM_REFERENCE_NUMBER','GRPID')\
                        .agg(sf.max('SEQ_NO').alias('check'))

join_cond = [cnfgtr_otherdtls_grid_tab.NUM_REFERENCE_NUMBER == cnfgtr_grid_tab.NUM_REFERENCE_NUMBER,\
             cnfgtr_otherdtls_grid_tab.GRPID == cnfgtr_grid_tab.GRPID,\
             cnfgtr_otherdtls_grid_tab.SEQ_NO == cnfgtr_grid_tab.check]

cnfgtr_grid_tab2 = cnfgtr_otherdtls_grid_tab\
                        .join(cnfgtr_grid_tab, join_cond, "inner")\
                        .drop(cnfgtr_grid_tab.NUM_REFERENCE_NUMBER)\
                        .drop(cnfgtr_grid_tab.check)\
                        .drop(cnfgtr_grid_tab.GRPID)\
                        .drop(cnfgtr_otherdtls_grid_tab.SEQ_NO)

cnfgtr_grid_tab3 = cnfgtr_grid_tab2\
                        .withColumn('ICD_MINOR',\
                                    when((lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp365')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp511')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp510')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp626')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp500')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp535')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp711')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp697')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp703')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp732'), cnfgtr_grid_tab2.INFORMATION5)\
                                    .otherwise(lit('')))\
                        .withColumn('ICD_MAJOR',\
                                    when((lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp365')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp511')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp510')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp626')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp500')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp535')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp711')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp697')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp703')|\
                                        (lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp732'), cnfgtr_grid_tab2.INFORMATION7)\
                                    .otherwise(lit('')))\
                        .withColumn('SIC_CODE_CN',\
                                    when(lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp868', cnfgtr_grid_tab2.INFORMATION20)\
                                    .otherwise(lit('')))\
                        .withColumn('SIC_DESC_CN',\
                                    when(lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp868', cnfgtr_grid_tab2.INFORMATION22)\
                                    .otherwise(lit('')))\
                        .withColumn('SUB_SIC_CD_CN',\
                                    when(lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp868', cnfgtr_grid_tab2.INFORMATION23)\
                                    .otherwise(lit('')))\
                        .withColumn('SUB_SIC_DESC_CN',\
                                    when(lower(trim(cnfgtr_grid_tab2.GRPID)) == 'grp868', cnfgtr_grid_tab2.INFORMATION24)\
                                    .otherwise(lit('')))

cnfgtr_grid_tab4 = cnfgtr_grid_tab3\
                        .groupBy('NUM_REFERENCE_NUMBER')\
                        .agg(sf.max('ICD_MINOR').alias('ICD_MINOR'),\
                             sf.max('ICD_MAJOR').alias('ICD_MAJOR'),\
                             sf.max('SIC_CODE_CN').alias('SIC_CODE_CN'),\
                             sf.max('SIC_DESC_CN').alias('SIC_DESC_CN'),\
                             sf.max('SUB_SIC_CD_CN').alias('SUB_SIC_CD_CN'),\
                             sf.max('SUB_SIC_DESC_CN').alias('SUB_SIC_DESC_CN'))

join_cond = [clm_49.NUM_REFERENCE_NO == cnfgtr_grid_tab4.NUM_REFERENCE_NUMBER]

clm_50 = clm_49\
            .join(cnfgtr_grid_tab4, join_cond, "left_outer")\
            .drop(cnfgtr_grid_tab4.NUM_REFERENCE_NUMBER)

clm_50 = clm_50\
            .withColumn('SIC_CODE', when((trim(clm_50.PRODUCT_CD) == '1703')|\
                                        (trim(clm_50.PRODUCT_CD) == '1704')|\
                                        (trim(clm_50.PRODUCT_CD) == '1706')|\
                                        (trim(clm_50.PRODUCT_CD) == '1707')|\
                                        (trim(clm_50.PRODUCT_CD) == '1710')|\
                                        (trim(clm_50.PRODUCT_CD) == '1712')|\
                                        (trim(clm_50.PRODUCT_CD) == '1713')|\
                                        (trim(clm_50.PRODUCT_CD) == '1714')|\
                                        (trim(clm_50.PRODUCT_CD) == '1715')|\
                                        (trim(clm_50.PRODUCT_CD) == '1716')|\
                                        (trim(clm_50.PRODUCT_CD) == '1717')|\
                                        (trim(clm_50.PRODUCT_CD) == '1718')|\
                                        (trim(clm_50.PRODUCT_CD) == '1719')|\
                                        (trim(clm_50.PRODUCT_CD) == '1720')|\
                                        (trim(clm_50.PRODUCT_CD) == '1721')|\
                                        (trim(clm_50.PRODUCT_CD) == '1722')|\
                                        (trim(clm_50.PRODUCT_CD) == '1723')|\
                                        (trim(clm_50.PRODUCT_CD) == '1724')|\
                                        (trim(clm_50.PRODUCT_CD) == '1726')|\
                                        (trim(clm_50.PRODUCT_CD) == '1727'), clm_50.SIC_CODE_CN)\
                                    .otherwise(lit('')))\
            .withColumn('SIC_DESC', when((trim(clm_50.PRODUCT_CD) == '1703')|\
                                        (trim(clm_50.PRODUCT_CD) == '1704')|\
                                        (trim(clm_50.PRODUCT_CD) == '1706')|\
                                        (trim(clm_50.PRODUCT_CD) == '1707')|\
                                        (trim(clm_50.PRODUCT_CD) == '1710')|\
                                        (trim(clm_50.PRODUCT_CD) == '1712')|\
                                        (trim(clm_50.PRODUCT_CD) == '1713')|\
                                        (trim(clm_50.PRODUCT_CD) == '1714')|\
                                        (trim(clm_50.PRODUCT_CD) == '1715')|\
                                        (trim(clm_50.PRODUCT_CD) == '1716')|\
                                        (trim(clm_50.PRODUCT_CD) == '1717')|\
                                        (trim(clm_50.PRODUCT_CD) == '1718')|\
                                        (trim(clm_50.PRODUCT_CD) == '1719')|\
                                        (trim(clm_50.PRODUCT_CD) == '1720')|\
                                        (trim(clm_50.PRODUCT_CD) == '1721')|\
                                        (trim(clm_50.PRODUCT_CD) == '1722')|\
                                        (trim(clm_50.PRODUCT_CD) == '1723')|\
                                        (trim(clm_50.PRODUCT_CD) == '1724')|\
                                        (trim(clm_50.PRODUCT_CD) == '1726')|\
                                        (trim(clm_50.PRODUCT_CD) == '1727'), clm_50.SIC_DESC_CN)\
                                    .otherwise(lit('')))\
            .withColumn('SUB_SIC_CD', when((trim(clm_50.PRODUCT_CD) == '1703')|\
                                        (trim(clm_50.PRODUCT_CD) == '1704')|\
                                        (trim(clm_50.PRODUCT_CD) == '1706')|\
                                        (trim(clm_50.PRODUCT_CD) == '1707')|\
                                        (trim(clm_50.PRODUCT_CD) == '1710')|\
                                        (trim(clm_50.PRODUCT_CD) == '1712')|\
                                        (trim(clm_50.PRODUCT_CD) == '1713')|\
                                        (trim(clm_50.PRODUCT_CD) == '1714')|\
                                        (trim(clm_50.PRODUCT_CD) == '1715')|\
                                        (trim(clm_50.PRODUCT_CD) == '1716')|\
                                        (trim(clm_50.PRODUCT_CD) == '1717')|\
                                        (trim(clm_50.PRODUCT_CD) == '1718')|\
                                        (trim(clm_50.PRODUCT_CD) == '1719')|\
                                        (trim(clm_50.PRODUCT_CD) == '1720')|\
                                        (trim(clm_50.PRODUCT_CD) == '1721')|\
                                        (trim(clm_50.PRODUCT_CD) == '1722')|\
                                        (trim(clm_50.PRODUCT_CD) == '1723')|\
                                        (trim(clm_50.PRODUCT_CD) == '1724')|\
                                        (trim(clm_50.PRODUCT_CD) == '1726')|\
                                        (trim(clm_50.PRODUCT_CD) == '1727'), clm_50.SUB_SIC_CD_CN)\
                                    .otherwise(lit('')))\
            .withColumn('SUB_SIC_DESC', when((trim(clm_50.PRODUCT_CD) == '1703')|\
                                        (trim(clm_50.PRODUCT_CD) == '1704')|\
                                        (trim(clm_50.PRODUCT_CD) == '1706')|\
                                        (trim(clm_50.PRODUCT_CD) == '1707')|\
                                        (trim(clm_50.PRODUCT_CD) == '1710')|\
                                        (trim(clm_50.PRODUCT_CD) == '1712')|\
                                        (trim(clm_50.PRODUCT_CD) == '1713')|\
                                        (trim(clm_50.PRODUCT_CD) == '1714')|\
                                        (trim(clm_50.PRODUCT_CD) == '1715')|\
                                        (trim(clm_50.PRODUCT_CD) == '1716')|\
                                        (trim(clm_50.PRODUCT_CD) == '1717')|\
                                        (trim(clm_50.PRODUCT_CD) == '1718')|\
                                        (trim(clm_50.PRODUCT_CD) == '1719')|\
                                        (trim(clm_50.PRODUCT_CD) == '1720')|\
                                        (trim(clm_50.PRODUCT_CD) == '1721')|\
                                        (trim(clm_50.PRODUCT_CD) == '1722')|\
                                        (trim(clm_50.PRODUCT_CD) == '1723')|\
                                        (trim(clm_50.PRODUCT_CD) == '1724')|\
                                        (trim(clm_50.PRODUCT_CD) == '1726')|\
                                        (trim(clm_50.PRODUCT_CD) == '1727'), clm_50.SUB_SIC_DESC_CN)\
                                    .otherwise(lit('')))

# removed losscountry when condition from below DF on 20191130 by Sagar. losscountry will be for 56 and 16
clm_50 = clm_50\
            .withColumn('RSRV_CURR_CD', clm_50.CURRENCY)\
            .withColumn('CURRENCY', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '56', clm_50.CURRENCY)\
                                    .otherwise(lit('')))\
            .withColumn('INVOICEAMOUNT', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '56', clm_50.INVOICEAMOUNT)\
                                    .otherwise(lit('')))\
            .withColumn('CARRIER', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.CARRIER)\
                                    .otherwise(lit('')))\
            .withColumn('WAYBILLDATE', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.WAYBILLDATE)\
                                    .otherwise(lit('')))\
            .withColumn('WAYBILLNO', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.WAYBILLNO)\
                                    .otherwise(lit('')))\
            .withColumn('FROM_CITY', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.FROM_CITY)\
                                    .otherwise(lit('')))\
            .withColumn('TO_CITY', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.TO_CITY)\
                                    .otherwise(lit('')))\
            .withColumn('CONVEYANCE_DESC', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.CONVEYANCE_DESC)\
                                    .otherwise(lit('')))\
            .withColumn('PROPERTY_INSURED', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.PROPERTY_INSURED)\
                                    .otherwise(lit('')))\
            .withColumn('SHIPMENT_TYPE', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.SHIPMENT_TYPE)\
                                    .otherwise(lit('')))\
            .withColumn('PROPERTY_INSRD_CD', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.PROPERTY_INSRD_CD)\
                                    .otherwise(lit('')))\
            .withColumn('SIC_CODE', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.SIC_CODE_R)\
                                    .otherwise(clm_50.SIC_CODE))\
            .withColumn('SIC_DESC', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.SIC_DESC_R)\
                                    .otherwise(clm_50.SIC_DESC))\
            .withColumn('SUB_SIC_CD', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.SUB_SIC_CD_R)\
                                    .otherwise(clm_50.SUB_SIC_CD))\
            .withColumn('SUB_SIC_DESC', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '21', clm_50.SUB_SIC_DESC_R)\
                                    .otherwise(clm_50.SUB_SIC_DESC))\
            .withColumn('ICD_MAJOR', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '28', clm_50.ICD_MAJOR)\
                                    .otherwise(lit('')))\
            .withColumn('ICD_MINOR', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '28', clm_50.ICD_MINOR)\
                                    .otherwise(lit('')))\
            .withColumn('CLAIMANT_NAME', when(trim(clm_50.NUM_DEPARTMENT_CODE) == '31', clm_50.CLAIMANT_NAME_MOT)\
                                            .otherwise(clm_50.CLAIMANT_NAME))\
            .withColumn('FLAG_CLAIM_FORM_CONTACT', when(clm_50.FEATURE_ACTIVITY_FIRST_CONTACT.isNull(), lit('N'))\
                                                .otherwise(lit('Y')))

# clm_50 = clm_50\
#             .withColumn('LOSS_DATE', StringToDateFunc(clm_50.LOSS_DATE))\
#             .withColumn('EFFECTIVE_DATE_OF_CANCELLATION', StringToDateFunc(clm_50.EFFECTIVE_DATE_OF_CANCELLATION))

clm_51 = clm_50\
            .withColumn('EFFECTIVE_DATE_OF_CANCELLATION',\
                        when((clm_50.EFFECTIVE_DATE_OF_CANCELLATION <= clm_50.LOSS_DATE)&\
                             ((lower(trim(clm_50.CNFGTR_TRANS_STATUS)) == 'apc')|\
                              (lower(trim(clm_50.CNFGTR_TRANS_STATUS)) == 'acdc')|\
                              (lower(trim(clm_50.CNFGTR_TRANS_STATUS)) == 'acde')|\
                              (lower(trim(clm_50.CNFGTR_TRANS_STATUS)) == 'erfc')), clm_50.EFFECTIVE_DATE_OF_CANCELLATION))


clm_52 = clm_51\
            .withColumn('CANCELLATION_REASON',\
                        when(clm_51.EFFECTIVE_DATE_OF_CANCELLATION.isNotNull(), clm_51.CANCELLATION_REASON))

#POLICY_CANCELLATION_DATE is in p1 corrected on 20191024 by satya
#            .withColumn('POLICY_CANCELLATION_DATE',\
#                        when(clm_51.EFFECTIVE_DATE_OF_CANCELLATION.isNotNull(), clm_51.POLICY_CANCELLATION_DATE))\

## join with ri_tmp_cession_unit_details

####   REINSURANCE_DESC

# join_cond = [clm_52.NUM_REFERENCE_NO == ri_tmp_cession_unit_details1.NUM_REFERENCE_NUMBER]

# clm_53 = clm_52\
#             .join(ri_tmp_cession_unit_details1, join_cond, "left_outer")

## changed on 20190608
## clm_53 = clm_53\
##            .withColumn('REINSURANCE_DESC', when(clm_53.NUM_REFERENCE_NUMBER.isNotNull(), lit('Y'))\
##                                            .otherwise(lit('N')))   

# clm_54 = clm_52\
#             .withColumn('REINSURANCE_DESC',  lit(''))

# clm_54 = clm_53\
#             .drop('NUM_REFERENCE_NUMBER')
clm_53 = clm_52\
            .withColumn('POLICY_CANCELLATION_DATE',\
                        when(((lower(trim(clm_52.CNFGTR_TRANS_STATUS)) == 'acdc')|\
                              (lower(trim(clm_52.CNFGTR_TRANS_STATUS)) == 'acde')), clm_52.POLICY_CANCELLATION_DATE)\
                       .otherwise(lit('')))
clm_54 = clm_53\
            .withColumn('POLICY_CANCELLATION_DATE', to_date(clm_53.POLICY_CANCELLATION_DATE, format='yyyy-MM-dd'))

#########################Changes made by Satya to fix NET_PCT on 20201110
ri_claims_0 = ri_td_claim_details1.join(ri_mm_claim_unit_details1,['NUM_CLAIM_NO','NUM_UPDATE_NO'])
ri_claims_2 = ri_claims_0.join(ri_tm_claim_cession,'NUM_CLAIM_UNIT_ID')
ri_claims_3 = ri_claims_2.join(ri_td_reinsurer_wise_claims,'NUM_CESSION_NUMBER')
ri_claims_4 = ri_claims_3.join(ri_mm_cession_types,'TXT_CESSION_TYPE')
ri_claims_5 = ri_claims_4\
                    .withColumn('RI_TYPE',concat(coalesce(trim(ri_claims_4.NUM_CESSION_TYPE_CD), lit('')),\
                                             coalesce(lit('-')),\
                                             coalesce(trim(ri_claims_4.TXT_CESSION_TYPE_DESCRIPTION))))\
                    .drop('NUM_CESSION_TYPE_CD','TXT_CESSION_TYPE_DESCRIPTION')
ri_claims_5 = ri_claims_5\
                    .withColumn('REIN_TYPE_CD', when(ri_claims_5.RI_TYPE=='75-Obligatory', lit('75'))\
                                                .when(ri_claims_5.RI_TYPE=='76-Facultative', lit('76'))\
                                                .when(ri_claims_5.RI_TYPE=='77-Quota Share', lit('77'))\
                                                .when(ri_claims_5.RI_TYPE=='90-Net Retention', lit('0')))
ri_claims_6 = ri_claims_5.filter(ri_claims_5.REIN_TYPE_CD.isin(['75','76','77','0']))

claim_base = clm_00[['NUM_CLAIM_NO','ACCT_LINE_CD']].distinct()
ri_claims_7 = ri_claims_6.join(claim_base,'NUM_CLAIM_NO')\
                            .withColumnRenamed('ACCT_LINE_CD','ACCOUNT_LINE_CD')
ri_claims_8 = ri_claims_7\
            .withColumn('NET_AMOUNT',\
                        when((ri_claims_7.ACCOUNT_LINE_CD == '50')&\
                             (abs(ri_claims_7.CUR_OS_RETAINED_AMOUNT_RWC - ri_claims_7.CUR_OS_RELEASED_AMOUNT_RWC) == 0),
                             abs(ri_claims_7.CUR_CLAIM_PAID_AMOUNT_RWC - ri_claims_7.CUR_CLAIM_RECOVERED_AMOUNT_RWC))\
                        .when((ri_claims_7.ACCOUNT_LINE_CD == '50')&\
                             (abs(ri_claims_7.CUR_OS_RETAINED_AMOUNT_RWC - ri_claims_7.CUR_OS_RELEASED_AMOUNT_RWC) != 0),
                             abs(ri_claims_7.CUR_OS_RETAINED_AMOUNT_RWC - ri_claims_7.CUR_OS_RELEASED_AMOUNT_RWC))\
                        .when((ri_claims_7.ACCOUNT_LINE_CD == '55')&\
                             (abs(ri_claims_7.NUM_EXP_OS_RETAINED_AMOUNT_RWC - ri_claims_7.NUM_EXP_OS_RELEASED_AMOUNT_RWC) == 0),
                             abs(ri_claims_7.CUR_EXPENSE_PAID_AMOUNT_RWC - ri_claims_7.CUR_EXPENSE_RECOVERED_AMOUNT_RWC))\
                        .when((ri_claims_7.ACCOUNT_LINE_CD == '55')&\
                             (abs(ri_claims_7.NUM_EXP_OS_RETAINED_AMOUNT_RWC - ri_claims_7.NUM_EXP_OS_RELEASED_AMOUNT_RWC) != 0),
                             abs(ri_claims_7.NUM_EXP_OS_RETAINED_AMOUNT_RWC - ri_claims_7.NUM_EXP_OS_RELEASED_AMOUNT_RWC)))\
            .withColumn('NET_PCT',\
                        when((ri_claims_7.ACCOUNT_LINE_CD == '50')&\
                             (ri_claims_7.CUR_OS_RETAINED_AMOUNT_CC - ri_claims_7.CUR_OS_RELEASED_AMOUNT_CC == 0),
                             (ri_claims_7.CUR_CLAIM_PAID_AMOUNT_RWC - ri_claims_7.CUR_CLAIM_RECOVERED_AMOUNT_RWC)*100/\
                             (ri_claims_7.CUR_CLAIM_PAID_AMOUNT_CC - ri_claims_7.CUR_CLAIM_RECOVERED_AMOUNT_CC))\
                        .when((ri_claims_7.ACCOUNT_LINE_CD == '50')&\
                             (ri_claims_7.CUR_OS_RETAINED_AMOUNT_CC - ri_claims_7.CUR_OS_RELEASED_AMOUNT_CC != 0),
                             (ri_claims_7.CUR_OS_RETAINED_AMOUNT_RWC - ri_claims_7.CUR_OS_RELEASED_AMOUNT_RWC)*100/\
                             (ri_claims_7.CUR_OS_RETAINED_AMOUNT_CC - ri_claims_7.CUR_OS_RELEASED_AMOUNT_CC))\
                        .when((ri_claims_7.ACCOUNT_LINE_CD == '55')&\
                             (ri_claims_7.NUM_EXP_OS_RETAINED_AMOUNT_CC - ri_claims_7.NUM_EXP_OS_RELEASED_AMOUNT_CC == 0),
                             (ri_claims_7.CUR_EXPENSE_PAID_AMOUNT_RWC - ri_claims_7.CUR_EXPENSE_RECOVERED_AMOUNT_RWC)*100/\
                             (ri_claims_7.CUR_EXPENSE_PAID_AMOUNT_CC - ri_claims_7.CUR_EXPENSE_RECOVERED_AMOUNT_CC))\
                        .when((ri_claims_7.ACCOUNT_LINE_CD == '55')&\
                             (ri_claims_7.NUM_EXP_OS_RETAINED_AMOUNT_CC - ri_claims_7.NUM_EXP_OS_RELEASED_AMOUNT_CC != 0),
                             (ri_claims_7.NUM_EXP_OS_RETAINED_AMOUNT_RWC - ri_claims_7.NUM_EXP_OS_RELEASED_AMOUNT_RWC)*100/\
                             (ri_claims_7.NUM_EXP_OS_RETAINED_AMOUNT_CC - ri_claims_7.NUM_EXP_OS_RELEASED_AMOUNT_CC)))\
            .drop('NUM_CESSION_NUMBER')\
            .drop('CUR_OS_RETAINED_AMOUNT_RWC')\
            .drop('CUR_OS_RELEASED_AMOUNT_RWC')\
            .drop('NUM_EXP_OS_RETAINED_AMOUNT_RWC')\
            .drop('NUM_EXP_OS_RELEASED_AMOUNT_RWC')\
            .drop('CUR_CLAIM_PAID_AMOUNT_RWC')\
            .drop('CUR_CLAIM_RECOVERED_AMOUNT_RWC')\
            .drop('CUR_EXPENSE_PAID_AMOUNT_RWC')\
            .drop('CUR_EXPENSE_RECOVERED_AMOUNT_RWC')\
            .drop('CUR_OS_RETAINED_AMOUNT_CC')\
            .drop('CUR_OS_RELEASED_AMOUNT_CC')\
            .drop('NUM_EXP_OS_RETAINED_AMOUNT_CC')\
            .drop('NUM_EXP_OS_RELEASED_AMOUNT_CC')\
            .drop('CUR_CLAIM_PAID_AMOUNT_CC')\
            .drop('CUR_CLAIM_RECOVERED_AMOUNT_CC')\
            .drop('CUR_EXPENSE_PAID_AMOUNT_CC')\
            .drop('CUR_EXPENSE_RECOVERED_AMOUNT_CC')\
            .drop('NUM_CLAIM_UNIT_ID','NUM_UPDATE_NO','TXT_CESSION_TYPE')

ri_claims_9 = ri_claims_8\
                    .groupBy('NUM_CLAIM_NO','REIN_TYPE_CD')\
                .agg(sf.max('NET_AMOUNT').alias('NET_AMOUNT'))
ri_claims_10 = ri_claims_8.join(ri_claims_9,['NUM_CLAIM_NO','REIN_TYPE_CD','NET_AMOUNT'],'inner')
ri_claims_10 = ri_claims_10\
                    .groupBy('NUM_CLAIM_NO','REIN_TYPE_CD')\
                .agg(sf.max('NET_PCT').alias('NET_PCT'))

# ri_td_claim_details1.show()

# ri_claims_9 = ri_claims_8.filter(ri_claims_8.NET_AMOUNT<>0)
# ri_claims_10 = ri_claims_9\
#                     .groupBy('NUM_CLAIM_NO','REIN_TYPE_CD')\
#                 .agg(sf.max('NET_PCT').alias('NET_PCT'))
ri_claims_11 = ri_claims_10.withColumn('NET_PCT',round(ri_claims_10.NET_PCT,2))
clm_55 = clm_54.join(ri_claims_11,['NUM_CLAIM_NO','REIN_TYPE_CD'],'left_outer')

clm_58 =  clm_55\
            .withColumn('NET_PCT', when((clm_55.NET_PCT.isNull())|(clm_55.NET_PCT==''), lit(0))\
                        .otherwise(clm_55.NET_PCT))

clm_all = clm_58\
            .withColumn('ITD_NET_PAID_AMOUNT' , (((clm_58.ITD_GROSS_PAID_AMOUNT)*(clm_58.NET_PCT))/100))\
            .withColumn('YTD_NET_PAID_AMOUNT' , (((clm_58.YTD_GROSS_PAID_AMOUNT)*(clm_58.NET_PCT))/100))\
            .withColumn('ITD_NET_PAID_AMOUNT_REOPEN' , (((clm_58.ITD_GROSS_PAID_AMOUNT_REOPEN)*(clm_58.NET_PCT))/100))

clm_all2 = clm_all\
                .drop('SUB_SIC_DESC_R')\
                .drop('SUB_SIC_DESC_CN')\
                .drop('SIC_CODE_CN')\
                .drop('SIC_CODE_R')\
                .drop('SUB_SIC_CD_CN')\
                .drop('SUB_SIC_CD_R')\
                .drop('SIC_CODE_R')\
                .drop('SIC_CODE_R')\
                .drop('SIC_DESC_R')\
                .drop('SIC_DESC_CN')\
                .drop('CLAIMANT_NAME_MOT')\
                .drop('PAN_NO_SI')

genmst_citydistrict.createOrReplaceTempView('genmst_citydistrict_view')
gc_clm_mot_additional_dtls.createOrReplaceTempView('gc_clm_mot_additional_dtls_view')
gc_clmmst_resource.createOrReplaceTempView('gc_clmmst_resource_view')
    
query="""
select k.NUM_CLAIM_NO,out.TXT_CITYDISTRICT as DEFENCE_ATTORNEY_CITY 
from genmst_citydistrict_view out,
(
select a.NUM_CLAIM_NO,b.NUM_CITYDISTRICT_CD from gc_clm_mot_additional_dtls_view a, gc_clmmst_resource_view b
where a.num_additional_info_type_cd=15.10 AND a.TXT_INFO6 = b.num_resource_cd
) k
where out.NUM_CITYDISTRICT_CD = k.num_citydistrict_cd
"""
clm_all_join = sqlContext.sql(query)
sqlContext.dropTempTable('genmst_citydistrict_view')
sqlContext.dropTempTable('gc_clm_mot_additional_dtls_view')
sqlContext.dropTempTable('gc_clmmst_resource_view')

clm_all_join = clm_all_join.groupBy('NUM_CLAIM_NO').agg(sf.max('DEFENCE_ATTORNEY_CITY').alias('DEFENCE_ATTORNEY_CITY'))
join_cond = [clm_all2.NUM_CLAIM_NO == clm_all_join.NUM_CLAIM_NO]

# part of main code

clm_all2 = clm_all2\
                .join(clm_all_join, 'NUM_CLAIM_NO', 'left_outer')

genmst_tab_office_off.createOrReplaceTempView('genmst_tab_office_off')

sql_1 = """
SELECT cast(A.NUM_OFFICE_CD as int) AS PROD_OFF_SUB_CD, A.TXT_OFFICE AS PROD_OFF_SUB_TXT, 
(case when (A.TXT_OFFICE_TYPE in ('RO', 'HO')) 
     then cast(A.NUM_OFFICE_CD as int) 
     else cast(B.NUM_OFFICE_CD as int) 
end) AS PROD_OFF_CD, 
(case when (A.TXT_OFFICE_TYPE in ('RO', 'HO')) 
     then A.TXT_OFFICE
     else B.TXT_OFFICE
end) AS PROD_OFF_TXT

FROM GENMST_TAB_OFFICE_OFF A left outer JOIN GENMST_TAB_OFFICE_OFF B
ON B.NUM_OFFICE_CD = A.NUM_PARENT_OFFICE_CD

"""

genmst_off = sqlContext.sql(sql_1)
sqlContext.dropTempTable('genmst_tab_office_off')

genmst_off_1 = genmst_off.groupBy('PROD_OFF_SUB_CD')\
                    .agg(sf.max('PROD_OFF_TXT').alias('PROD_OFF_TXT'),\
                         sf.max('PROD_OFF_CD').alias('PROD_OFF_CD'),\
                         sf.max('PROD_OFF_SUB_TXT').alias('PROD_OFF_SUB_TXT'))

clm_all3 = clm_all2.drop('PROD_OFF_SUB_CD')\
                   .drop('PROD_OFF_TXT')\
                   .drop('PROD_OFF_CD')\
                   .drop('PROD_OFF_SUB_TXT')\
                   .withColumnRenamed('TXT_ISSUE_OFFICE_CD', 'PROD_OFF_SUB_CD')

clm_all4 = clm_all3.join(genmst_off_1, 'PROD_OFF_SUB_CD', 'left')

clm_all5 = clm_all4.withColumn('PROD_OFF_SUB_CD', clm_all4['PROD_OFF_SUB_CD'].substr(2, 5))\
                   .withColumn('PROD_OFF_CD', clm_all4['PROD_OFF_CD'].substr(2, 5))

clm_all6 = clm_all5

clm_all6.persist()

## claim_report_gc_cvr_dtl (claim_report_gc_cvr_dtl)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": "staging",
         "dbtable": "claim_report_gc_cvr_dtl",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":2} 
cvr_dtl_3 = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REFERENCE_NO','COVERGRPINDX','SUM_INSURED_FOR_THE_COVERAGE_SELECTED','PRODUCT_INDEX','COVER_GROUP')\
.filter(col("REPORT_DATE").between(to_date(lit(start_date), format='yyyy-MM-dd'),to_date(lit(end_date), format='yyyy-MM-dd')))\

prod_dt2 = prod_dtl.withColumnRenamed('DEFVAL', 'PRODUCT_CD')\
                   .withColumnRenamed('NODEINDX', 'PRODUCT_INDEX')

prod_dt3 = prod_dt2\
                .groupBy('PRODUCT_INDEX')\
                .agg(sf.max('PRODUCT_CD').alias('PRODUCT_CD'))

cvr_prod = cvr_dtl_3.join(prod_dt3, ['PRODUCT_INDEX'],'inner')\
                  .drop(prod_dt3.PRODUCT_INDEX)

cover_grp_map2 = cover_grp_map1.withColumnRenamed('COVERCODE','COVERAGE_CODE')

cvr_prod_covindx = cvr_prod.join(cover_grp_map2,['COVERGRPINDX'],'inner')\
                            .drop(cover_grp_map2.COVERGRPINDX)

cvr_prod_covindx_01 = cvr_prod_covindx\
                .groupBy('NUM_REFERENCE_NO','PRODUCT_CD','COVERAGE_CODE')\
                .agg(sf.max('COVERGRPINDX').alias('COVERGRPINDX'),\
                     sf.max('PRODUCT_INDEX').alias('PRODUCT_INDEX'),\
                     sf.max('SUM_INSURED_FOR_THE_COVERAGE_SELECTED').alias('SUM_INSURED_FOR_THE_COVERAGE_SELECTED'),\
                     sf.max('COVER_GROUP').alias('COVER_GROUP'))                                            
            
clm_all6_01 = clm_all6.drop('COVERGRPINDX')
           
clm_all6A = clm_all6_01.join(cvr_prod_covindx_01, ['NUM_REFERENCE_NO', 'PRODUCT_CD', 'COVERAGE_CODE'], 'left')\
                        .drop(cvr_prod_covindx_01.NUM_REFERENCE_NO)\
                        .drop(cvr_prod_covindx_01.PRODUCT_CD)\
                        .drop(cvr_prod_covindx_01.COVERAGE_CODE)\
                        .drop('PRODUCT_INDEX')

### 201908013 - Satya - SUM_INSURED

clm_all6A.createOrReplaceTempView('clm_all6A_view')
query1="""SELECT distinct NUM_REFERENCE_NO, COVERAGE_CODE, NUM_DEPARTMENT_CODE
FROM clm_all6A_view 
WHERE COVERAGE_CODE = '9948'OR COVERAGE_CODE = '2304' AND NUM_DEPARTMENT_CODE = '31' AND lower(trim(COVER_GROUP)) = 'owndamage'"""

clm_all6A_mod = sqlContext.sql(query1)
sqlContext.dropTempTable('clm_all6A_view')

cvr_dtl_2_B = cvr_dtl_3.filter(cvr_dtl_3.COVER_GROUP == 'Own Damage')
cvr_dtl_2_C = cvr_dtl_2_B.groupBy('NUM_REFERENCE_NO')\
                       .agg(sf.max('SUM_INSURED_FOR_THE_COVERAGE_SELECTED').alias('SUM_INSURED_FOR_THE_COVERAGE_SELECTED_TMP'))
    
clm_all6B = clm_all6A_mod.join(cvr_dtl_2_C, ['NUM_REFERENCE_NO'], 'left_outer')

clm_all6C = clm_all6A.join(clm_all6B, ['NUM_REFERENCE_NO', 'COVERAGE_CODE', 'NUM_DEPARTMENT_CODE'], 'left')

clm_all6C = clm_all6C\
                .withColumn('SUM_INSURED_FOR_THE_COVERAGE_SELECTED',\
                   when((clm_all6C.NUM_DEPARTMENT_CODE == '31')&\
                       ((clm_all6C.COVERAGE_CODE == '9948')|(clm_all6C.COVERAGE_CODE == '2304')),\
                        clm_all6C.SUM_INSURED_FOR_THE_COVERAGE_SELECTED_TMP)\
                       .otherwise(clm_all6C.SUM_INSURED_FOR_THE_COVERAGE_SELECTED))
##New block
clm_all6D = clm_all6C\
                .withColumn('SUM_INSURED_FOR_THE_COVERAGE_SELECTED',\
                   when(((clm_all6C.NUM_DEPARTMENT_CODE == '31')&\
                       ((clm_all6C.COVERAGE_CODE == '24979')| (clm_all6C.COVERAGE_CODE == '2330'))&\
                       ((clm_all6C.PRODUCT_CD == '3121')| (clm_all6C.PRODUCT_CD == '3122')| (clm_all6C.PRODUCT_CD == '3184'))), lit('1500000'))\
                       .otherwise(clm_all6C.SUM_INSURED_FOR_THE_COVERAGE_SELECTED))

clm_all6E = clm_all6D\
                .withColumn('SUM_INSURED_FOR_THE_COVERAGE_SELECTED',\
                   when(((clm_all6D.NUM_DEPARTMENT_CODE == '31')&\
                       ((clm_all6D.COVERAGE_CODE == '24979')| (clm_all6D.COVERAGE_CODE == '2330'))&\
                       (clm_all6D.PRODUCT_CD == '3124')), lit('200000'))\
                       .otherwise(clm_all6D.SUM_INSURED_FOR_THE_COVERAGE_SELECTED))

# SI modification/addition by satya on 20200126
# For POLICYTERM=='1' or isNull() take old SI; 
# otherwise pick on the basis of in which tenure loss_date lies between POL_INCEPT_DATE and POL_EXP_DATE; 
# pick accordingly from risk_headers 5 columns.

clm_all6F = clm_all6E\
                .withColumn('SUM_INSURED_FOR_THE_COVERAGE_SELECTED',\
                            when(~((trim(clm_all6E.POLICYTERM).isNull())|(trim(clm_all6E.POLICYTERM)=='1'))&\
                                 (clm_all6E.LOSS_DATE.between(col("POL_INCEPT_DATE"),date_sub(add_months(col("POL_INCEPT_DATE"),12),1))),clm_all6E.SI_YEAR1)
                            .when(~((trim(clm_all6E.POLICYTERM).isNull())|(trim(clm_all6E.POLICYTERM)=='1'))&\
                                 (clm_all6E.LOSS_DATE.between(add_months(col("POL_INCEPT_DATE"),12),date_sub(add_months(col("POL_INCEPT_DATE"),24),1))),clm_all6E.SI_YEAR2)
                            .when(~((trim(clm_all6E.POLICYTERM).isNull())|(trim(clm_all6E.POLICYTERM)=='1'))&\
                                 (clm_all6E.LOSS_DATE.between(add_months(col("POL_INCEPT_DATE"),24),date_sub(add_months(col("POL_INCEPT_DATE"),36),1))),clm_all6E.SI_YEAR3)
                            .when(~((trim(clm_all6E.POLICYTERM).isNull())|(trim(clm_all6E.POLICYTERM)=='1'))&\
                                 (clm_all6E.LOSS_DATE.between(add_months(col("POL_INCEPT_DATE"),36),date_sub(add_months(col("POL_INCEPT_DATE"),48),1))),clm_all6E.SI_YEAR4)
                            .when(~((trim(clm_all6E.POLICYTERM).isNull())|(trim(clm_all6E.POLICYTERM)=='1'))&\
                                 (clm_all6E.LOSS_DATE.between(add_months(col("POL_INCEPT_DATE"),48),date_sub(add_months(col("POL_INCEPT_DATE"),60),1))),clm_all6E.SI_YEAR5)
                .otherwise(clm_all6E.SUM_INSURED_FOR_THE_COVERAGE_SELECTED))

# clm_all6F.show()

## claim_report_gc_cbc (claim_report_gc_cbc)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": "staging",
         "dbtable": "claim_report_gc_cbc",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":2} 
cbc_data = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('POLICY_NO','CBC_STATUS','CBC_RECEIPT_NO','CHEQUE_BOUNCE')\
.filter(col("REPORT_DATE").between(to_date(lit(start_date), format='yyyy-MM-dd'),to_date(lit(end_date), format='yyyy-MM-dd')))\
.withColumn('CBC_RECEIPT_NO', col('CBC_RECEIPT_NO').cast(DecimalType(20,0)).cast(StringType()))
# .filter(col("POLICY_NO").isin(pol_no_list))

clm_all7 = clm_all6F.join(cbc_data, ['POLICY_NO'], 'left')
    
gc_clm_neft0 = gc_clm_neft_upload_comp_base\
                        .withColumnRenamed('TXT_NEFTOUTWARDS_CHQ_NO','CHEQUE_NO')\
                        .withColumnRenamed('TXT_NEFTOUTWARDS_UTR_NO','UTR_NUMBER')\
                        .withColumn('ACCOUNT_LINE_CD', when(lower(trim(gc_clm_neft_upload_comp_base.TXT_INFO3)) == 'indemnity', lit('50'))\
                                        .otherwise(lit('55')))\
                        .withColumn('DAT_BOOK_DATE', to_date(gc_clm_neft_upload_comp_base.DAT_BOOK_DATE, format='yyyy-MM-dd'))\
                        .drop('TXT_INFO3')

gc_clm_neft1 = gc_clm_neft0\
                    .withColumn("ROW_NUM", sf.row_number()\
                                .over(Window.partitionBy('NUM_CLAIM_NO','ACCOUNT_LINE_CD','CHEQUE_NO')\
                                      .orderBy(gc_clm_neft0.DAT_DATE_OF_INSERT.desc())))

gc_clm_neft2 = gc_clm_neft1.filter((gc_clm_neft1.ROW_NUM==1))

gc_clm_neft3 = gc_clm_neft2\
                    .withColumn('UTR_DATE', gc_clm_neft2.DAT_BOOK_DATE)\
                    .withColumn('UTR_REJECTION_REASON',\
                                when(lower(trim(gc_clm_neft2.TXT_PROCESS_FLAG))=='r', gc_clm_neft2.TXT_TRANS_DETAIL_LINE4)\
                               .otherwise(lit(None)))\
                    .withColumn('UTR_REJECTION_DATE',\
                                when(lower(trim(gc_clm_neft2.TXT_PROCESS_FLAG))=='r', gc_clm_neft2.DAT_BOOK_DATE)\
                               .otherwise(lit(None)))\
                    .drop('ROW_NUM','DAT_BOOK_DATE','TXT_TRANS_DETAIL_LINE4','TXT_PROCESS_FLAG','DAT_DATE_OF_INSERT')

clm_all8 = clm_all7.join(gc_clm_neft3,['NUM_CLAIM_NO','ACCOUNT_LINE_CD','CHEQUE_NO'],'left_outer')

clm_all9 = clm_all8.withColumn('LOAD_DATE', lit(datetime.date.today()))\
                    .drop('CNFGTR_TRANS_STATUS','COVER_GROUP','COVERGRPINDX','POLICYTERM','SI_YEAR1','SI_YEAR2','SI_YEAR3','SI_YEAR4','SI_YEAR5','SIC_CODE_R1','SIC_CODE_R2','SIC_DESC_R1','SUB_SIC_DESC_R1','SUM_INSURED_FOR_THE_COVERAGE_SELECTED_TMP','TATAAIG_GSTN_NO_EXPENSE','TATAAIG_GSTN_NO_INDEMNITY','TXT_SKILL_AVAILABLE','WAYBILLDATE_V')

# clm_all9 = clm_all9[['ACCOUNT_LINE_CD','DEFENCE_ATTORNEY_CITY','ADDRESS_1','ANATOMY_CD','ANAT_PROP','APPROVER_ID',\
# 'BANK_ACCT_NO','BANK_NAME','BASIC_TP','CANCELLATION_REASON','CATASTROPHE_TYPE','CAUSE_LOSS_CD',\
# 'CAUSE_OF_LOSS','CBC_RECEIPT_NUMBER_STATUS','CERTIFICATE_NUMBER','CGST_AMOUNT','CHEQUE_DATE','CHEQUE_NO',\
# 'CLAIMANT_NAME','CLAIMSPROXIMITY','CLAIM_CATEGORY_CD','CLAIM_FACULTATIVE_76','CLAIM_FEATURE_CONCAT','CLAIM_FEATURE_NO',\
# 'CLAIM_GROSS','CLAIM_NET','CLAIM_NET_0','CLAIM_NO','CLAIM_OBLIGATORY_75','CLAIM_STATUS_CD',\
# 'CLAIM_TREATY_77','CLAIM_XOL_RECOVERY_78','CLASS_PERIL_CD','CLASS_PERIL_TEXT','CLM_CREATE_DATE','COINSURANCE_CD',\
# 'COINSURANCE_DESC','COINSURANCE_PCT','COSTS_PLAINTIFF','COURT_NAME','COVERAGE_CODE','CUSTOMER_CLAIM_NUMBER',\
# 'CUSTOMER_ID','CWP_REASON','DATEDIFFERENCE','DATE_OF_AWARD','DATE_OF_FILING_OF_PETITION','DATE_OF_HEARING',\
# 'DATE_OF_RECEIPT_OF_AWARD_BY_ADVOCATE','DATE_OF_RECEIPT_OF_AWARD_BY_TATAAIG','DAT_ENDORSEMENT_EFF_DATE',\
# 'DAT_POLICY_EFF_FROMDATE','DAT_REFERENCE_DATE','LCL_CATASTROPHE_NO',\
# 'DAT_TRANSACTION_DATE','DAT_TRANS_DATE','DAT_VOUCHER_DATE','DEFENCE_COST','DEFENDANT_NAME','EFFECTIVE_DATE_OF_CANCELLATION',\
# 'EFF_DT_SEQ_NO','EXAMINER_NAME','EXAMINER_USER_ID','EXONERATION','EXPENSES_STATUS_AT_FEATURE_LEVEL','FEES',\
# 'HOSPITAL_NAME','IFSC_CODE','IGST_AMOUNT','INDEMNITY_PARTS','INDEMNITY_STATUS_AT_FEATURE_LEVEL','INJURY_DAMAGE',\
# 'INJURY_CD','INSURED_NAME','INTEREST_AMOUNT','INTERIM_AWARD_DATE','INTERIM_AWARD_RECEIVED_BY_TAGIC_DATE','PAYEE_GSTN_NO',\
# 'ISSUING_COMAPNY','ISSUING_OFFICE','ITD_GROSS_PAID_AMOUNT','ITD_NET_PAID_AMOUNT','LAST_NOTE_UPDATED_DATE',\
# 'LITIGATION_TYPE','LOB','LOSSCITY','LOSSCOUNTRY','LOSSDISTRICT','LOSS_DATE',\
# 'LOSS_DESC_TXT','LOSS_LOC_TXT','LOSS_REPORTED_DATE','MAJOR_LINE_CD','MAJOR_LINE_TEXT','MARKET_SECMENTATION',\
# 'MINOR_LINE_CD','MINOR_LINE_TXT','MOTHER_BRANCH','MULTIPLE_PAY_IND','NCB','NETT_CHEQUE_AMOUNT',\
# 'NOTIONAL_SALVAGE_AMOUNT','NOTIONAL_SUBROGATION_AMOUNT','NUM_CLAIM_NO','NUM_DEPARTMENT_CODE',\
# 'NUM_REFERENCE_NO','NUM_RI_TREATY_NO','NUM_SURVEY_TYPE','NUM_TRANSACTION_CONTROL_NO','NUM_TYPE_OF_PARTY','OTHER_RECOVERY',\
# 'PAYEE_ADDRESS','PAYEE_BANK_CODE','PAYEE_NAME','PAYEE_TYPE','PAYMENT_CORRRECTION','PAYMENT_POSTING_DATE',\
# 'PAYMENT_STATUS','A&H_PAYMENT_TYPE','PAY_FORM_TEXT','POLICY_CANCELLATION_DATE','POLICY_NO','POL_EXP_DATE',\
# 'POL_INCEPT_DATE','POL_ISS_OFF_CD','POL_ISS_OFF_DESC','PRIORITY_CLIENT_ID','PRODUCER_CD','PRODUCER_NAME',\
# 'PRODUCT_CD','PRODUCT_NAME','PRODUCT_TYPE','PROD_OFF_SUB_TXT','REASON_FOR_REOPEN','RECEIPT_DETAILS',\
# 'REIN_TYPE_CD','RENL_CERT_NO','REOPEN_STATUS','REPORT','REPUDIATED_REASON','SALVAGE_AMOUNT',\
# 'SALVAGE_BUYER_ADDRESS','SALVAGE_BUYER_NAME','SALVAGE_BUYER_PAN','SALVAGE_BUYER_STATE','SETTLING_OFF_CD','SETTLING_OFF_CD_WR',\
# 'SETTLING_OFF_DESC','SGST_AMOUNT','STATE_OF_LOSS','SUBROGATION_AMT','SURVEYORCODE','SURVEYOR_APPOINTMENT_DATE',\
# 'APPOINTMENT_OF_INVESTIGATOR','SURVEYOR_NAME','SURVEYOR_REPORT_RECEIVED_DATE','TATAAIG_GSTN_NO','TDS_AMOUNT','TP_AWARD',\
# 'TRANS_TYPE_CD','TREATMENTTYPE','TXT_CATASTROPHE_DESC','TXT_MASTER_CLAIM_NO_NEW','TXT_MMCP_CODE',\
# 'TXT_USER_ID','TYPE_OF_CLAIM','TYPE_OF_SETTLEMENT','TYPE_OF_SURVEY','USER_NAME','YTD_GROSS_PAID_AMOUNT',\
# 'YTD_NET_PAID_AMOUNT','WAYBILLDATE','WAYBILLNO','CARRIER','EP_NUMBER','SALARY_OF_INJURED',\
# 'NO_OF_DEPENDANTS','IS_ORDER_XXI_COMPLIED','COMPLIANCE_DATE','FROM_CITY','PETITIONER_NAME','EP_YEAR',\
# 'AGE_OF_INJURED','DATE_OF_ACCIDENT_AS_PER_FIR','FIR_DATE','REPAIRER_NAME','PROPERTY_INSURED','TO_CITY',\
# 'OTHER_VEHICLE_NO_AND_TYPE','EXECUTION_CLOSURE','FIR_NO','RSRV_CURR_CD','INVOICEAMOUNT','CASE_NUMBER',\
# 'DEMAND_AMOUNT','CASE_YEAR','OCCUPATION_OF_INJURED','KPG','PETITION_AMOUNT','OTHER_VEHICLE_INSURANCE_CO',\
# 'OFFER_AMOUNT','DATE_OF_OFFER','DATE_OF_RECEIPT_OF_DAR','INVOICE_NO','INVOICE_DATE','RELATIONSHIP',\
# 'CURRENCY','DATEOFADMISSION','DATEOFDISCHARGE','DRIVER_NAME','GARAGE_NAME','DRIVING_LICENCE',\
# 'DRIVING_LICENSE_EXPIRY_DATE','PROPERTY_INSRD_CD','PLAINTIFF_ATTORNEY','CONVEYANCE_DESC','SHIPMENT_TYPE',\
# 'IS_CASE_FIT_FOR_COMPROMISE_IN_ALL_RESPECTS','DEFENCE_ATTORNEY','VENUE','INTIMATION_US_156','DAR',\
# 'IS_INVOICE_IN_FAVOR_OF_TATAAIG','SAC_CODE','SAC_CODE_OLD','INDEMNITY_PAINT','INDEMNITY_LABOR','INDEMNITY',\
# 'CASH_COLLECTION','HANDLING_FEE_AMT','SETTLEMENT_TYPE','FEATURE_ACTIVITY_FIRST_CONTACT','FIRST_FOLLOW_UP','SECOND_FOLLOW_UP',\
# 'THIRD_FOLLOW_UP','FEATURE_ACTIVITY_ALL_DOC_RECD','CLAIM_FORM_GIVEN','ANY_FEATURE_ACTIVITY','ALL_DOCS_RECEIVED',\
# 'LAST_FEATURE_CHANGE','ACCORDANCE_FG','SUBROGATION_FG','FILE_NO','ACCORDANCE_COMMENTS','COV_CONFIRMATION_FG',\
# 'RESERVE_APPROVAL_FG','EXCESS_EXPOSURE_FG','RESERVE_POSTING_FG','COVRAGE_RESERV_COMNTS','FILE_INSUIT_FG','LITIGATION_COSTS_FG',\
# 'ACTIVE_CASEMGT_FG','COUNSEL_QUALITY_FG','LITIGATION_COMMENTS','CLAIM_REIN_FG','REIN_LOSSNOTICE_FG','TIMELY_LOSSNOTIFY_FG',\
# 'REIN_DOCUMENTATION_FG','REIN_PROOFOFLOSS_FG','REINSURANCE_COMMENTS','DIARYDATES_UTIL_FG','INVESTIGATION_FG',\
# 'TP_RECOG_NOTICE_FG','REPORTING_DEVELOPMENTS_FG','DOCUMENTATION_FG','EXPERTS_UTIL_FG','AGGRESSIVE_PURSUIT_FG',\
# 'PROMPT_CORRESPONDENCE_FG','LOSS_ACKNOWLEDGED_DT','INDEMN_EXP_PAYMENT_FG','EVIDENCE_FG','QUALITY_INPUT_FG',\
# 'ACCOUNT_CLAIMBULLETIN_FG','EVALUATION_COMMENTS','OVERALLEVAL_HANDLING_FG','GENERAL_REMARKS','SUB_SIC_CD',\
# 'SUB_PRODUCT_NAME','VEH_MAKE_NAME','VEH_MDL_NAME','IDV','SUB_SIC_DESC','SIC_DESC',\
# 'SIC_CODE','COVERAGE_DESC','ACCOUNT_LINE_TXT','SOURCE','CURRENCY_CD',\
# 'SUBPRODUCT_CD','POL_ISS_OFF_CD_WR','AUTHORISED_BY','PROD_OFF_CD','PROD_OFF_SUB_CD','TYPE',\
# 'POLICY_TYPE','SUB_PRODUCT_TYPE','SURVEYOR_LICENSE_EXPIRY_DATE','SURVEYOR_LICENSE_NUMBER','PAYEE_PAN_NO',\
# 'POLICY_STATUS','CHASSIS_NUMBER','ENGINE_NUMBER','VEHICLE_REGISTRATION_NUMBER','CLASS','ICD_MINOR',\
# 'ICD_MAJOR','USER_EMP_CD','LITIGATION_TYPE_CD','FLAG_CLAIM_FORM_CONTACT','PROD_OFF_TXT','PROD_SOURCE_CD',\
# 'PAY_FORM_CD','SUBLIMIT','CHEQUE_BOUNCE','TYPE_OF_SURVEYOR','REOPEN_DATE_REF','REINSURANCE_DESC',\
# 'NET_PCT','EXPENSE_PAYMENT_TYPE','NUM_ISACTIVE','CLM_CREATE_DATE_REF','CWP_AMOUNT_REF',\
# 'CWP_REASON_REF','CWP_DATE_REF','EXAMINER_EMP_CODE','EXPENSES','RESERVE_RELEASE_PAYMENT','ICD_MAJOR_CD',\
# 'ICD_MAJOR_DESC','ICD_MINOR_CD','ICD_MINOR_DESC','CBC_STATUS','CBC_RECEIPT_NO','SUM_INSURED_FOR_THE_COVERAGE_SELECTED',\
# 'CWP_REOPEN_DATE','TRANSACTIONTYPE','LOAD_DATE','REPORT_DATE',\
# 'EXAMINER_USER_ID_CD','INTERMEDIARY_GSTN_NO','ITD_GROSS_PAID_AMOUNT_REOPEN','NUM_PAYMENT_UPDATE_NO',\
# 'NUM_RESOURCE_TYPE','PAN_NO','PAYEE_SURVEYORCODE','ITD_NET_PAID_AMOUNT_REOPEN','RI_INWARD_FLAG','PAYMENT_TYPE']]

# clm_all9.printSchema()

print ("Deleting data from Claim Report GC P2 with below query: ")
print ("""delete from datamarts.claim_report_gc_p2 where REPORT_DATE BETWEEN '""" + start_date + """' and '""" + end_date + "' ")

from pg import DB

db = DB(dbname='gpadmin', host='10.35.12.194', port=5432,user='gpspark', passwd='spark@456')
dlt_cnt = db.query("""delete from datamarts.claim_report_gc_p2 where REPORT_DATE BETWEEN '""" + start_date + """' and '""" + end_date + "' ")

print ("no of records deleted for "+report_date_str)
print (dlt_cnt)

clm_all9.write.format("greenplum").option("dbtable","claim_report_gc_p2").option('dbschema','datamarts').option("server.port","1104").option("url","jdbc:postgresql://10.35.12.194:5432/gpadmin").option("user", "gpspark").option("password","spark@456").mode('append').save()

clm_00.unpersist()
clm_all6.unpersist()

# spark.stop()
