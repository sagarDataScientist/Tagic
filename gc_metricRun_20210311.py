# Change log: Paid and Oslr together
# Change log: INDEMNITY_STATUS & EXPENSES_STATUS incorporated to filted closed claims in oslr (Optimization)
# Change log: Identify ICAN claims
# Change log: Implemented REPORT_DATE and LOAD_DATE
# Change log: Implemented POLICY_CANCELLATION_DATE from p1 to here (Optimization)
# Change log: CLAIM_CATEGORY_CD implemented in metric- to handle paid service claims
# Change log: Paid service claims to to derived from oslr transactions
# Change log: TXT_USERID.isNull() and NUM_ISACTIVE='1' added
# Change log: NUM_ISACTIVE=1 implemented for 4 history reports
# Change log: APPROVER_ID,TXT_USER_ID,TRANSACTIONTYPE,NUM_ISACTIVE,CWP_REASON,CHEQUE_NO,CHEQUE_DATE included in metric
# Change log: YTD_GROSS_PAID_AMOUNT,YTD_NET_PAID_AMOUNT removed
# Change log: paid/oslr transactions unnecessary transactions filtered out
# OSLR inactive transactions in history table will not be there in oslr report unlike F&A report- If to match with F&A report remove inactive filters from oslr report
# Change log: DISPLAY_PRODUCT_CD implemented

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
                app_name = "Claim_Report_GC_Metric_"+runtype+": "+report_date_str
        elif runtype == 'daily':
            app_name = "Claim_Report_GC_Metric_"+runtype

no_of_cpu = 8
max_cores = 16
executor_mem = '41g'
# runtype = 'monthly'
# report_date_str = '2020-06-30'

# automated_run = 'Y'
# if automated_run == 'Y':
#     if len(sys.argv) != 2:
#         print("if automated_run = 'Y' then this code need one parameter - report date")
#         sys.exit(-1)
#     else:
#         report_date_str = sys.argv[1]

# env = 'newprod'
# if env=='newprod':
#     mesos_ip = 'mesos://10.35.12.205:5050'
# else:
#     mesos_ip = 'mesos://10.35.12.5:5050'

from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import asc,lit
# warnings.filterwarnings('error')
import pyspark
from datetime import datetime,timedelta
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
conf = pyspark.SparkConf()
#import numpy
import calendar
import time
#import pandas as pd
#import simplejson as json
#import pandas as pd
import numpy as np
import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, StringType, DecimalType
from pyspark.sql.functions import lit
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

conf.set('spark.es.scroll.size','10000')
conf.set('spark.network.timeout','600s')
conf.set('spark.sql.crossJoin.enabled', 'true')
conf.set('spark.executor.heartbeatInterval','60s')
conf.set("spark.driver.cores","6")
conf.set("spark.driver.extraJavaOptions","-Xms6g -Xmx12g")
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
conf.set('es.http.retries', '5')
spark = pyspark.SparkContext(conf=conf)
sqlContext = SQLContext(spark)

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
# num_claim_no_str = ["30015731199058004002"]
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
# start_tsp = to_timestamp(str(start_date)+' 00:00:00')
# end_tsp = to_timestamp(str(end_date)+' 23:59:59')
# YYMM = str(report_date.year) + str('{:02d}'.format(report_date.month))
# YYMMDD = report_date.strftime('%Y%m%d')
# print(report_date_filter)
print("report_date_str",report_date_str)
print("report_date",report_date)
print("start_date",start_date)
print("end_date",end_date)

# url_uat = "jdbc:postgresql://10.33.195.103:5432/gpadmin"
url_prod = "jdbc:postgresql://10.35.12.194:5432/gpadmin"
# server_port = "1140-1160"
server_port = "1108"
user_prod="gpspark"
pwd_prod="spark@456"
dbschema_etl = "public"

paid_gl_list = ["7010001110","7010001120","7010002110","7010002120","7010003110"\
,"7010003120","7010004110","7010004120","7010005110","7010020110"\
,"7010020120","7010010110","7010010120","7010011110","7010011120"\
,"7010012110","7010012120","7010013110","7010013120","7010014110"\
,"7010014120","7010015110","7010015120","7010016110","7010016120"\
,"7010017110","7010017120","7010018110","7010018120","7010019110"\
,"7010011150","7010001190"]

oslr_gl_list = ["7010001140","7010002130","7010002140","7010002150","7010002160"\
,"7010003130","7010003140","7010004130","7010004140","7010005130"\
,"7010010130","7010010140","7010011130","7010011140","7010012130"\
,"7010012140","7010013130","7010013140","7010016130","7010016140"\
,"7010017130","7010017140","7010018130","7010018140","7010019130"\
,"7010020130","7010020140","7010001130"]

## acc_general_ledger (acc_general_ledger)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "account_gc_acc_general_ledger",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":16} 
acc_general_ledger_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TXT_LEDGER_ACCOUNT_CD','DAT_VOUCHER_DATE','DAT_TRANSACTION_DATE','TXT_TRANSACTION_CATEGORY_CD','NUM_TRANSACTION_CONTROL_NO','NUM_REFERENCE_NO','NUM_CLAIM_NO','NUM_RI_TREATY_NO','TXT_DR_CR','NUM_AMOUNT','TXT_DIMENSION_2_VALUE_CD','TXT_DIMENSION_3_VALUE_CD','TXT_DIMENSION_4_VALUE_CD','DAT_REFERENCE_DATE','DAT_TRANS_DATE','NUM_OFFICE_CD','TXT_DIMENSION_1_VALUE_CD','TXT_DIMENSION_7_VALUE_CD')\
.filter((col("TXT_LEDGER_ACCOUNT_CD").isin(paid_gl_list)&\
        ((col("DAT_VOUCHER_DATE")>=to_timestamp(lit(start_date), format='yyyy-MM-dd'))&\
         (col("DAT_VOUCHER_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))))|
        ((col("TXT_LEDGER_ACCOUNT_CD").isin(oslr_gl_list))&\
         (col("DAT_VOUCHER_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

acc_general_ledger_base = acc_general_ledger_base\
                                .withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))\
                                .withColumn('NUM_TRANSACTION_CONTROL_NO',col('NUM_TRANSACTION_CONTROL_NO').cast(DecimalType(20,0)).cast(StringType()))

acc_general_ledger_base.persist()

## gc_clm_transaction_history (gc_clm_transaction_history)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_transaction_history",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":2} 
gc_clm_transaction_history_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','TXT_ACCOUNTLINE','DAT_TRANSACTIONDATE','NUM_PAYMENTNUMBER','NUM_TRANSACTIONAMOUNT','TXT_USERID','NUM_SERIAL_NO','TXT_TRANSACTIONTYPE','NUM_ISACTIVE','TXT_APPROVERID','TXT_CHEQUENO','TXT_REASON')\
.filter(col("DAT_TRANSACTIONDATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_transaction_history_base = gc_clm_transaction_history_base\
                                        .withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))\
                                        .withColumn('NUM_PAYMENTNUMBER',col('NUM_PAYMENTNUMBER').cast(DecimalType(20,0)).cast(StringType()))

# gc_clm_claim_recovery (gc_clm_claim_recovery)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_claim_recovery",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clm_claim_recovery_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_FORWARD_VOUCHER_NO','TXT_STATUS','DAT_DATE_OF_RECOVERY','TXT_TYPE_OF_RECOVERY','CUR_NET_AMOUNT','CUR_RECOVERY_AMOUNT','NUM_RECEIPT_NO','TXT_PAN_NUMBER','TXT_PARTY_ADDRESS_1','TXT_PAYER_NAME','TXT_STATE')\
.filter(col("DAT_DATE_OF_RECOVERY")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))


gc_clm_claim_recovery_base = gc_clm_claim_recovery_base\
                                    .withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))\
                                    .withColumn('NUM_FORWARD_VOUCHER_NO',col('NUM_FORWARD_VOUCHER_NO').cast(DecimalType(20,0)).cast(StringType()))\

## co_insurance_details_tab (co_insurance_details_tab)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "policy_gc_co_insurance_details_tab",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
co_insurance_details_tab_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REFERENCE_NUMBER','SHARE_PCT','TXT_COMPANY_SHORT_DESC','LEADERNONLEADER','CO_INSURANCE_TYPE')\
.filter(col("TXT_COMPANY_SHORT_DESC")=='TATAAIG')

### gc_clm_gen_info (gc_clm_gen_info)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_gen_info",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":4} 
gc_clm_gen_info_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','TXT_MASTER_CLAIM_NO_NEW','NUM_REFERENCE_NO','TXT_POLICY_NO_CHAR')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))
# .filter((to_date(col("DAT_INSERT_DATE"), format='yyyy-MM-dd'))<=end_date)\

gc_clm_gen_info_base = gc_clm_gen_info_base.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

##Added by Satya on 20200828 for identifying ICAN claims
### claim_bpm_featuresummary (claim_bpm_featuresummary)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_bpm_featuresummary",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
feature_summary = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('CLAIMNUMBER')

###  acc_general_ledger
acc_general_ledger = acc_general_ledger_base\
                            .withColumnRenamed('TXT_DIMENSION_3_VALUE_CD','PRODUCT_CD')\
                            .withColumnRenamed('TXT_DIMENSION_4_VALUE_CD','TXT_MMCP_CODE')\
                            .withColumnRenamed('NUM_OFFICE_CD','PROD_OFF_SUB_CD')\
                            .withColumn('TXT_ISSUE_OFFICE_CD', acc_general_ledger_base.TXT_DIMENSION_2_VALUE_CD)\
                            .withColumn('POL_ISS_OFF_CD', acc_general_ledger_base.TXT_DIMENSION_2_VALUE_CD)\
                            .withColumn('DAT_VOUCHER_DATE', to_date(acc_general_ledger_base.DAT_VOUCHER_DATE, format='yyyy-MM-dd'))\
                            .withColumn('DAT_TRANS_DATE', to_date(acc_general_ledger_base.DAT_TRANS_DATE, format='yyyy-MM-dd'))\
                            .withColumn('DAT_TRANSACTION_DATE', to_date(acc_general_ledger_base.DAT_TRANSACTION_DATE, format='yyyy-MM-dd'))\
                            .withColumn('DAT_REFERENCE_DATE', to_date(acc_general_ledger_base.DAT_REFERENCE_DATE, format='yyyy-MM-dd'))\
                            .withColumn('DISPLAY_PRODUCT_CD', when (acc_general_ledger_base.TXT_DIMENSION_1_VALUE_CD.isNull(),\
                                   acc_general_ledger_base.TXT_DIMENSION_7_VALUE_CD)\
                                        .otherwise(acc_general_ledger_base.TXT_DIMENSION_1_VALUE_CD))\
                            .drop('TXT_DIMENSION_2_VALUE_CD','TXT_DIMENSION_1_VALUE_CD','TXT_DIMENSION_7_VALUE_CD')
#                             .withColumn('NUM_REFERENCE_NO',\
#                                         col('NUM_REFERENCE_NO').cast(DecimalType(15,0)).cast(StringType()))\

acc_general_ledger = acc_general_ledger\
                         .withColumn('DISPLAY_PRODUCT_CD', acc_general_ledger.DISPLAY_PRODUCT_CD.substr(1,6))

###  gc_clm_transaction_history
gc_clm_transaction_history = gc_clm_transaction_history_base\
                                .withColumnRenamed('NUM_PAYMENTNUMBER','NUM_TRANSACTION_CONTROL_NO')\
                                .withColumn('DAT_TRANS_DATE', to_date(gc_clm_transaction_history_base.DAT_TRANSACTIONDATE, format='yyyy-MM-dd'))\
                                .withColumn('ACCT_LINE_CD', when(lower(trim(gc_clm_transaction_history_base.TXT_ACCOUNTLINE)) == 'indemnity', lit('50'))\
                                        .otherwise(lit('55')))
###  gc_clm_claim_recovery
gc_clm_claim_recovery = gc_clm_claim_recovery_base\
                                .withColumnRenamed('NUM_FORWARD_VOUCHER_NO', 'NUM_TRANSACTION_CONTROL_NO')\
                                .withColumnRenamed('DAT_DATE_OF_RECOVERY', 'DAT_VOUCHER_DATE')\
                                .withColumnRenamed('TXT_TYPE_OF_RECOVERY', 'TXT_TRANSACTION_CATEGORY_CD')\
                                .withColumnRenamed('NUM_RECEIPT_NO', 'RECEIPT_DETAILS')\
                                .withColumnRenamed('TXT_PAN_NUMBER', 'SALVAGE_BUYER_PAN')\
                                .withColumnRenamed('TXT_PARTY_ADDRESS_1', 'SALVAGE_BUYER_ADDRESS')\
                                .withColumnRenamed('TXT_PAYER_NAME', 'SALVAGE_BUYER_NAME')\
                                .withColumnRenamed('TXT_STATE', 'SALVAGE_BUYER_STATE')

gc_clm_claim_recovery = gc_clm_claim_recovery.withColumn('DAT_VOUCHER_DATE', to_date(gc_clm_claim_recovery.DAT_VOUCHER_DATE, format='yyyy-MM-dd'))

###  co_insurance_details_tab_base
co_insurance_details_tab = co_insurance_details_tab_base\
                                .withColumnRenamed('SHARE_PCT', 'COINSURANCE_PCT')\
                                .withColumnRenamed('LEADERNONLEADER', 'COINSURANCE_CD')\
                                .withColumnRenamed('CO_INSURANCE_TYPE', 'COINSURANCE_DESC')

####   gc_clm_gen_info
# gc_clm_gen_info = gc_clm_gen_info_base\
#                         .withColumn('GEN_INFO_NUM_REFERENCE_NO',\
#                                         col('NUM_REFERENCE_NO').cast(DecimalType(15,0)).cast(StringType()))\
#                         .drop('NUM_REFERENCE_NO')
gc_clm_gen_info = gc_clm_gen_info_base\
                        .withColumnRenamed('NUM_REFERENCE_NO','GEN_INFO_NUM_REFERENCE_NO')\
                        .withColumnRenamed('TXT_POLICY_NO_CHAR', 'POLICY_NO')

# gc_clm_gen_info = gc_clm_gen_info\
#                         .withColumnRenamed('TXT_NAME_OF_INSURED', 'INSURED_NAME')\
#                         .withColumnRenamed('TXT_REMARKS', 'MOTHER_BRANCH')\
#                         .withColumnRenamed('TXT_SERVICING_OFFICE_CD', 'SETTLING_OFF_CD')\
#                         .withColumnRenamed('YN_CLOSE_PROXIMITY', 'CLAIMSPROXIMITY')\
#                         .withColumnRenamed('TXT_POLICY_NO_CHAR', 'POLICY_NO')\
#                         .withColumnRenamed('NUM_NATURE_OF_LOSS', 'COVERAGE_CODE')\
#                         .withColumn('CLM_CREATE_DATE', to_date(gc_clm_gen_info.DAT_REGISTRATION_DATE, format='yyyy-MM-dd'))\
#                         .withColumn('LOSS_DATE', to_date(gc_clm_gen_info.DAT_LOSS_DATE, format='yyyy-MM-dd'))\
#                         .withColumn('LOSS_REPORTED_DATE', to_date(gc_clm_gen_info.DAT_NOTIFICATION_DATE, format='yyyy-MM-dd'))

#################    Filter all required ledger codes   #########

# acc_gl = acc_general_ledger\
#                 .filter(~((acc_general_ledger.TXT_LEDGER_ACCOUNT_CD == '7010040110')|\
#                         (acc_general_ledger.TXT_LEDGER_ACCOUNT_CD == '6010001110')))

acc_gl = acc_general_ledger\
            .withColumn('NUM_AMOUNT_SIGN', when(lower(acc_general_ledger.TXT_DR_CR) == 'cr', acc_general_ledger.NUM_AMOUNT * -1)\
                                            .otherwise(acc_general_ledger.NUM_AMOUNT))

### Acct_line_cd
acc_gl2 = acc_gl\
            .withColumn('ACCT_LINE_CD', when((acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010001110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010002110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010003110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010004110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010005110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010020110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010010110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010011110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010012110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010013110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010014110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010015110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010016110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010017110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010018110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010019110')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010011150')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010002130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010002150')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010003130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010004130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010005130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010010130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010011130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010012130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010013130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010016130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010017130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010018130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010019130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010020130')|\
                                             (acc_gl.TXT_LEDGER_ACCOUNT_CD == '7010001130'), lit('50'))\
                                        .otherwise(lit('55')))

## REIN_TYPE_CD
acc_gl3 = acc_gl2\
            .withColumn('REIN_TYPE_CD', when((acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010010110')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010010120')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010010130')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010010140'), lit('75'))\
                                       .when((acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010018110')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010018120')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010018130')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010018140'), lit('76'))\
                                       .when((acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010020110')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010020120')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010011110')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010011120')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010012110')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010012120')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010013110')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010013120')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010014110')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010014120')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010015110')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010015120')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010016110')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010016120')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010017110')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010017120')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010020130')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010020140')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010011130')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010011140')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010012130')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010012140')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010013130')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010013140')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010016130')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010016140')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010017130')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010005130')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010017140'), lit('77'))\
                                       .when((acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010019110')|\
                                             (acc_gl2.TXT_LEDGER_ACCOUNT_CD == '7010019130'), lit('78'))\
                                       .otherwise(lit('0')))

## Report_Name
acc_gl3 = acc_gl3\
            .withColumn('REPORT', when((acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010001110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010001120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010002110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010002120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010003110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010003120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010004110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010004120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010005110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010020110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010020120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010010110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010010120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010011110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010011120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010012110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010012120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010013110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010013120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010014110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010014120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010015110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010015120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010016110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010016120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010017110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010017120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010018110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010018120')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010019110')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010011150')|\
                                       (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010001190'), lit('paid'))\
                            .when((acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010001130')|\
                                  (acc_gl3.TXT_LEDGER_ACCOUNT_CD == '7010001140'), lit('sc'))
                            .otherwise(lit('oslr')))

acc_gl4 = acc_gl3\
            .withColumn('TRANS_TYPE_CD', when(((acc_gl3.REPORT == 'paid')|(acc_gl3.REPORT == 'sc'))&\
                                              (lower(acc_gl3.TXT_TRANSACTION_CATEGORY_CD) == 't85'), lit('21'))\
                                        .when(((acc_gl3.REPORT == 'paid')|(acc_gl3.REPORT == 'sc'))&\
                                              (lower(acc_gl3.TXT_TRANSACTION_CATEGORY_CD) == 't86'), lit('23'))\
                                        .when(acc_gl3.REPORT == 'oslr', lit('19'))\
                                        .otherwise(lit('22')))
acc_gl5 = acc_gl4\
            .groupBy('NUM_CLAIM_NO','NUM_TRANSACTION_CONTROL_NO','NUM_REFERENCE_NO',\
                     'TRANS_TYPE_CD','DAT_VOUCHER_DATE','DAT_TRANS_DATE',\
                     'NUM_RI_TREATY_NO','ACCT_LINE_CD','REIN_TYPE_CD','REPORT')\
            .agg(sf.sum('NUM_AMOUNT_SIGN').alias('NUM_AMOUNT_SIGN'),\
                 sf.max('TXT_MMCP_CODE').alias('TXT_MMCP_CODE'),\
                 sf.max('DISPLAY_PRODUCT_CD').alias('DISPLAY_PRODUCT_CD'),\
                 sf.max('DAT_REFERENCE_DATE').alias('DAT_REFERENCE_DATE'),\
                 sf.max('DAT_TRANSACTION_DATE').alias('DAT_TRANSACTION_DATE'),\
                 sf.max('PROD_OFF_SUB_CD').alias('PROD_OFF_SUB_CD'),\
                 sf.max('PRODUCT_CD').alias('PRODUCT_CD'),\
                 sf.max('TXT_ISSUE_OFFICE_CD').alias('TXT_ISSUE_OFFICE_CD'),\
                 sf.max('POL_ISS_OFF_CD').alias('POL_ISS_OFF_CD'))

#### create required bifurcations of claim amounts based upon rein_type_cd

clm_rg = acc_gl5\
            .withColumn('CLAIM_OBLIGATORY_75', when(acc_gl5.REIN_TYPE_CD == '75', acc_gl5.NUM_AMOUNT_SIGN)\
                                       .otherwise(lit(0)))\
            .withColumn('CLAIM_FACULTATIVE_76', when(acc_gl5.REIN_TYPE_CD == '76', acc_gl5.NUM_AMOUNT_SIGN)\
                                       .otherwise(lit(0)))\
            .withColumn('CLAIM_TREATY_77', when(acc_gl5.REIN_TYPE_CD == '77', acc_gl5.NUM_AMOUNT_SIGN)\
                                       .otherwise(lit(0)))\
            .withColumn('CLAIM_XOL_RECOVERY_78', when(acc_gl5.REIN_TYPE_CD == '78', acc_gl5.NUM_AMOUNT_SIGN)\
                                       .otherwise(lit(0)))\
            .withColumn('CLAIM_GROSS', when(acc_gl5.REIN_TYPE_CD == '0', acc_gl5.NUM_AMOUNT_SIGN)\
                                       .otherwise(lit(0)))\
            .withColumn('CLAIM_NET', acc_gl5.NUM_AMOUNT_SIGN)

## filter required month data based on voucher date

clmpd_rg = clm_rg.filter(((clm_rg.REPORT == 'paid')|(clm_rg.REPORT == 'sc'))&\
                         ((clm_rg.DAT_VOUCHER_DATE >= first_day_of_month(report_date))&\
                          (clm_rg.DAT_VOUCHER_DATE <= last_day_of_month(report_date))))
# clmpd_g = clmpd_rg.filter(clmpd_rg.REIN_TYPE_CD == '0')
clmpd_g5 = clmpd_rg\
            .groupBy('NUM_CLAIM_NO','NUM_TRANSACTION_CONTROL_NO','NUM_REFERENCE_NO',\
                     'ACCT_LINE_CD','REIN_TYPE_CD',\
                     'TRANS_TYPE_CD','DAT_VOUCHER_DATE','DAT_TRANS_DATE','REPORT')\
            .agg(sf.max('TXT_MMCP_CODE').alias('TXT_MMCP_CODE'),\
                 sf.max('CLAIM_GROSS').alias('CLAIM_GROSS'),\
                 sf.max('CLAIM_NET').alias('CLAIM_NET'),\
                 sf.max('DAT_REFERENCE_DATE').alias('DAT_REFERENCE_DATE'),\
                 sf.max('DISPLAY_PRODUCT_CD').alias('DISPLAY_PRODUCT_CD'),\
                 sf.max('DAT_TRANSACTION_DATE').alias('DAT_TRANSACTION_DATE'),\
                 sf.max('PROD_OFF_SUB_CD').alias('PROD_OFF_SUB_CD'),\
                 sf.max('PRODUCT_CD').alias('PRODUCT_CD'),\
                 sf.max('TXT_ISSUE_OFFICE_CD').alias('TXT_ISSUE_OFFICE_CD'),\
                 sf.max('POL_ISS_OFF_CD').alias('POL_ISS_OFF_CD'))

clmpd_g6 = clmpd_g5\
            .withColumn('CLAIM_OBLIGATORY_75', lit(0))\
            .withColumn('CLAIM_NET_0', lit(0))\
            .withColumn('CLAIM_FACULTATIVE_76', lit(0))\
            .withColumn('CLAIM_TREATY_77', lit(0))\
            .withColumn('CLAIM_XOL_RECOVERY_78', lit(0))\
            .withColumn('NUM_RI_TREATY_NO', lit(''))\
#             .withColumn('REPORT', lit('paid'))

# clmpd_r = clmpd_rg.filter(clmpd_rg.REIN_TYPE_CD != '0')   
# clmpd_r = clmpd_r\
#             .withColumn('CLAIM_NET_0', lit(0))

clmos = clm_rg.filter((clm_rg.REPORT == 'oslr')|(clm_rg.REPORT == 'sc'))                   

clmos = clmos\
            .withColumn('CLAIM_NET_0', lit(0))\
            .withColumn('REPORT', lit('oslr'))\
            .withColumn('TRANS_TYPE_CD', lit('19'))

# Remove claims with zero outstanding value

clmos_g0 = clmos\
            .groupBy('NUM_CLAIM_NO')\
            .agg(sf.sum('CLAIM_GROSS').alias('CLAIM_GROSS_AGG'))

clmos_g0 = clmos_g0.withColumn('CLAIM_GROSS_AGG', round(clmos_g0.CLAIM_GROSS_AGG, 2))
clmos_g01 = clmos_g0\
                .filter(clmos_g0.CLAIM_GROSS_AGG != 0)

clmos_g02 = clmos_g01\
                .select('NUM_CLAIM_NO')

join_cond = [clmos.NUM_CLAIM_NO == clmos_g02.NUM_CLAIM_NO]

clmos_f = clmos\
            .join(clmos_g02, join_cond, "inner")\
            .drop(clmos_g02.NUM_CLAIM_NO)

# clm_00 = unionAll([clmpd_g6,clmpd_r,clmos_f])
clm_00 = unionAll([clmpd_g6,clmos_f])

clm_00.persist()

clm_h00 = gc_clm_transaction_history\
            .filter((gc_clm_transaction_history.DAT_TRANS_DATE >= first_day_of_month(report_date))&\
                    (gc_clm_transaction_history.DAT_TRANS_DATE <= report_date))

##TXT_USERID.isNull() and NUM_ISACTIVE='1' added.
clm_h = clm_h00\
            .filter((((lower(trim(clm_h00.TXT_USERID)) != 'system generated entry')|\
                   (trim(clm_h00.TXT_USERID).isNull()))&\
                   (clm_h00.NUM_ISACTIVE=='1')))

### filter required transaction types from gc_clm_transaction_history

clm_h0 = clm_h\
            .filter((lower(trim(clm_h.TXT_TRANSACTIONTYPE)) == 'advice')|\
                    (lower(trim(clm_h.TXT_TRANSACTIONTYPE)) == 'decrease in reserve')|\
                    (lower(trim(clm_h.TXT_TRANSACTIONTYPE)) == 'increase in reserve')|\
                    (lower(trim(clm_h.TXT_TRANSACTIONTYPE)) == 'mark off reserve')|\
                    (lower(trim(clm_h.TXT_TRANSACTIONTYPE)) == 'reopen'))
clm_h2 = clm_h0\
            .groupBy('NUM_CLAIM_NO','ACCT_LINE_CD','DAT_TRANS_DATE',\
                     'NUM_TRANSACTION_CONTROL_NO','TXT_TRANSACTIONTYPE')\
            .agg(sf.sum('NUM_TRANSACTIONAMOUNT').alias('CLAIM_GROSS'))

clm_h3 = clm_h2\
            .withColumn('TRANS_TYPE_CD', when(lower(trim(clm_h2.TXT_TRANSACTIONTYPE)) == 'advice', lit('11'))\
                                        .when(lower(trim(clm_h2.TXT_TRANSACTIONTYPE)) == 'reopen', lit('12'))\
                                        .when(lower(trim(clm_h2.TXT_TRANSACTIONTYPE)) == 'mark off reserve', lit('16'))\
                                        .when(lower(trim(clm_h2.TXT_TRANSACTIONTYPE)) == 'decrease in reserve', lit('14'))\
                                        .when(lower(trim(clm_h2.TXT_TRANSACTIONTYPE)) == 'increase in reserve', lit('13')))

clm_h4 = clm_h3\
            .withColumn('REPORT', when(clm_h3.TRANS_TYPE_CD == '11', lit('advice'))\
                                 .when(clm_h3.TRANS_TYPE_CD == '12', lit('reopen'))\
                                 .when(clm_h3.TRANS_TYPE_CD == '16', lit('cwp'))\
                                 .when((clm_h3.TRANS_TYPE_CD == '13')|\
                                       (clm_h3.TRANS_TYPE_CD == '14'), lit('reserve_revision')))

### join with acc_general_ledger for base columns

agl_temp1 = acc_general_ledger\
                    .select('NUM_CLAIM_NO','NUM_TRANSACTION_CONTROL_NO','NUM_REFERENCE_NO','PRODUCT_CD',\
                            'TXT_MMCP_CODE','PROD_OFF_SUB_CD','TXT_ISSUE_OFFICE_CD','POL_ISS_OFF_CD','DAT_REFERENCE_DATE','DISPLAY_PRODUCT_CD')

agl_temp2 = agl_temp1\
                .groupBy('NUM_CLAIM_NO','NUM_TRANSACTION_CONTROL_NO')\
                .agg(sf.max('NUM_REFERENCE_NO').alias('NUM_REFERENCE_NO'),\
                     sf.max('PRODUCT_CD').alias('PRODUCT_CD'),\
                     sf.max('TXT_MMCP_CODE').alias('TXT_MMCP_CODE'),\
                     sf.max('PROD_OFF_SUB_CD').alias('PROD_OFF_SUB_CD'),\
                     sf.max('DAT_REFERENCE_DATE').alias('DAT_REFERENCE_DATE'),\
                     sf.max('DISPLAY_PRODUCT_CD').alias('DISPLAY_PRODUCT_CD'),\
                     sf.max('TXT_ISSUE_OFFICE_CD').alias('TXT_ISSUE_OFFICE_CD'),\
                     sf.max('POL_ISS_OFF_CD').alias('POL_ISS_OFF_CD'))

join_cond = [clm_h4.NUM_CLAIM_NO == agl_temp2.NUM_CLAIM_NO,\
             clm_h4.NUM_TRANSACTION_CONTROL_NO == agl_temp2.NUM_TRANSACTION_CONTROL_NO]

clm_h5 = clm_h4\
            .join(agl_temp2, join_cond, 'left_outer')\
            .drop(agl_temp2.NUM_CLAIM_NO)\
            .drop(agl_temp2.NUM_TRANSACTION_CONTROL_NO)

clm_h6 = clm_h5\
            .withColumn('REIN_TYPE_CD', lit('0'))\
            .withColumn('NUM_RI_TREATY_NO', lit(''))\
            .withColumn('CLAIM_NET', lit(0))\
            .withColumn('CLAIM_NET_0', lit(0))\
            .withColumn('CLAIM_OBLIGATORY_75', lit(0))\
            .withColumn('CLAIM_FACULTATIVE_76', lit(0))\
            .withColumn('CLAIM_TREATY_77', lit(0))\
            .withColumn('CLAIM_XOL_RECOVERY_78', lit(0))\
            .withColumn('DAT_TRANSACTION_DATE', clm_h5.DAT_TRANS_DATE)\
            .withColumn('DAT_VOUCHER_DATE', clm_h5.DAT_TRANS_DATE)\
            .withColumn('RECEIPT_DETAILS', lit(''))\
            .withColumn('SALVAGE_BUYER_PAN', lit(''))\
            .withColumn('SALVAGE_BUYER_ADDRESS', lit(''))\
            .withColumn('SALVAGE_BUYER_NAME', lit(''))\
            .withColumn('SALVAGE_BUYER_STATE', lit(''))\
            .withColumn('NOTIONAL_SALVAGE_AMOUNT', lit(0))\
            .withColumn('NOTIONAL_SUBROGATION_AMOUNT', lit(0))\
            .withColumn('SALVAGE_AMOUNT', lit(0))\
            .withColumn('SUBROGATION_AMT', lit(0))\
            .withColumn('OTHER_RECOVERY', lit(0))

clm_00 = clm_00\
            .withColumn('RECEIPT_DETAILS', lit(''))\
            .withColumn('SALVAGE_BUYER_PAN', lit(''))\
            .withColumn('SALVAGE_BUYER_ADDRESS', lit(''))\
            .withColumn('SALVAGE_BUYER_NAME', lit(''))\
            .withColumn('SALVAGE_BUYER_STATE', lit(''))\
            .withColumn('NOTIONAL_SALVAGE_AMOUNT', lit(0))\
            .withColumn('NOTIONAL_SUBROGATION_AMOUNT', lit(0))\
            .withColumn('SALVAGE_AMOUNT', lit(0))\
            .withColumn('SUBROGATION_AMT', lit(0))\
            .withColumn('OTHER_RECOVERY', lit(0))

#####   ADD SALVAGE DETAILS
## acc_general_ledger (acc_general_ledger)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "account_gc_acc_general_ledger",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":2} 
salv1 = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('DAT_VOUCHER_DATE','DAT_TRANSACTION_DATE','NUM_TRANSACTION_CONTROL_NO','NUM_REFERENCE_NO','NUM_CLAIM_NO','TXT_DIMENSION_2_VALUE_CD','TXT_DIMENSION_3_VALUE_CD','TXT_DIMENSION_4_VALUE_CD','DAT_REFERENCE_DATE','DAT_TRANS_DATE','NUM_OFFICE_CD','TXT_DIMENSION_1_VALUE_CD','TXT_DIMENSION_7_VALUE_CD')\
.filter(col("TXT_LEDGER_ACCOUNT_CD")==lit('7010040110'))\
.filter(col("DAT_VOUCHER_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# salv1 = acc_general_ledger\
#             .filter(acc_general_ledger.TXT_LEDGER_ACCOUNT_CD == '7010040110')
salv1 = salv1\
            .withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))\
            .withColumn('NUM_TRANSACTION_CONTROL_NO',col('NUM_TRANSACTION_CONTROL_NO').cast(DecimalType(20,0)).cast(StringType()))\
            .withColumnRenamed('TXT_DIMENSION_3_VALUE_CD','PRODUCT_CD')\
            .withColumnRenamed('TXT_DIMENSION_4_VALUE_CD','TXT_MMCP_CODE')\
            .withColumnRenamed('NUM_OFFICE_CD','PROD_OFF_SUB_CD')\
            .withColumn('TXT_ISSUE_OFFICE_CD', salv1.TXT_DIMENSION_2_VALUE_CD)\
            .withColumn('POL_ISS_OFF_CD', salv1.TXT_DIMENSION_2_VALUE_CD)\
            .withColumn('DAT_VOUCHER_DATE', to_date(salv1.DAT_VOUCHER_DATE, format='yyyy-MM-dd'))\
            .withColumn('DAT_TRANS_DATE', to_date(salv1.DAT_TRANS_DATE, format='yyyy-MM-dd'))\
            .withColumn('DAT_TRANSACTION_DATE', to_date(salv1.DAT_TRANSACTION_DATE, format='yyyy-MM-dd'))\
            .withColumn('DAT_REFERENCE_DATE', to_date(salv1.DAT_REFERENCE_DATE, format='yyyy-MM-dd'))\
            .withColumn('DISPLAY_PRODUCT_CD', when (salv1.TXT_DIMENSION_1_VALUE_CD.isNull(),\
                                   salv1.TXT_DIMENSION_7_VALUE_CD)\
                                        .otherwise(salv1.TXT_DIMENSION_1_VALUE_CD))\
            .drop('TXT_DIMENSION_2_VALUE_CD','TXT_DIMENSION_1_VALUE_CD','TXT_DIMENSION_7_VALUE_CD')

salv1 = salv1\
           .withColumn('DISPLAY_PRODUCT_CD', salv1.DISPLAY_PRODUCT_CD.substr(1,6))
salv2 = salv1\
            .groupBy('NUM_CLAIM_NO', 'NUM_TRANSACTION_CONTROL_NO','DAT_VOUCHER_DATE')\
            .agg(sf.max('DAT_TRANSACTION_DATE').alias('DAT_TRANSACTION_DATE'),\
                 sf.max('DAT_TRANS_DATE').alias('DAT_TRANS_DATE'),\
                 sf.max('TXT_ISSUE_OFFICE_CD').alias('TXT_ISSUE_OFFICE_CD'),\
                 sf.max('POL_ISS_OFF_CD').alias('POL_ISS_OFF_CD'),\
                 sf.max('PROD_OFF_SUB_CD').alias('PROD_OFF_SUB_CD'),\
                 sf.max('PRODUCT_CD').alias('PRODUCT_CD'),\
                 sf.max('TXT_MMCP_CODE').alias('TXT_MMCP_CODE'),\
                 sf.max('DISPLAY_PRODUCT_CD').alias('DISPLAY_PRODUCT_CD'),\
                 sf.max('DAT_REFERENCE_DATE').alias('DAT_REFERENCE_DATE'),\
                 sf.max('NUM_REFERENCE_NO').alias('NUM_REFERENCE_NO'))

# salv2 = salv2\
#             .withColumn('NUM_REFERENCE_NO',col('NUM_REFERENCE_NO').cast(DecimalType(15,0)).cast(StringType()))

######    MONTH TO BE SPECIFIED HERE    #####
gc_clm_rec1 = gc_clm_claim_recovery.filter((gc_clm_claim_recovery.DAT_VOUCHER_DATE >= first_day_of_month(report_date))&\
                                           (gc_clm_claim_recovery.DAT_VOUCHER_DATE <= report_date))

gc_clm_rec1 = gc_clm_rec1.filter((lower(trim(gc_clm_rec1.TXT_STATUS)) == 'collected')|\
                                 (lower(trim(gc_clm_rec1.TXT_STATUS)) == 'active'))


join_cond = [gc_clm_rec1.NUM_CLAIM_NO == salv2.NUM_CLAIM_NO,\
             gc_clm_rec1.NUM_TRANSACTION_CONTROL_NO == salv2.NUM_TRANSACTION_CONTROL_NO,\
             gc_clm_rec1.DAT_VOUCHER_DATE == salv2.DAT_VOUCHER_DATE]

clm_rec2 = gc_clm_rec1\
                .join(salv2, join_cond, 'left_outer')\
                .drop(salv2.NUM_CLAIM_NO)\
                .drop(salv2.NUM_TRANSACTION_CONTROL_NO)\
                .drop(salv2.DAT_VOUCHER_DATE)


clm_rec3 = clm_rec2\
                .withColumn('TRANS_TYPE_CD',\
                            when((lower(trim(clm_rec2.TXT_TRANSACTION_CATEGORY_CD)) == 'sv')|\
                                 (lower(trim(clm_rec2.TXT_TRANSACTION_CATEGORY_CD)) == 'sl'), lit('25'))\
                            .when(lower(trim(clm_rec2.TXT_TRANSACTION_CATEGORY_CD)) == 'sr', lit('26'))\
                            .otherwise(lit('')))

clm_rec4 = clm_rec3\
            .withColumn('NOTIONAL_SALVAGE_AMOUNT', when(clm_rec3.TRANS_TYPE_CD == '25', clm_rec3.CUR_NET_AMOUNT)\
                                                  .otherwise(lit(0)))\
            .withColumn('NOTIONAL_SUBROGATION_AMOUNT', when(clm_rec3.TRANS_TYPE_CD == '26', clm_rec3.CUR_NET_AMOUNT)\
                                                      .otherwise(lit(0)))\
            .withColumn('SALVAGE_AMOUNT', when((clm_rec3.TRANS_TYPE_CD == '25')&\
                                               (lower(trim(clm_rec3.TXT_TRANSACTION_CATEGORY_CD)) != 'cc'),\
                                               clm_rec3.CUR_RECOVERY_AMOUNT)\
                                         .otherwise(lit(0)))\
            .withColumn('SUBROGATION_AMT', when((clm_rec3.TRANS_TYPE_CD == '26')&\
                                               (lower(trim(clm_rec3.TXT_TRANSACTION_CATEGORY_CD)) != 'cc'),\
                                               clm_rec3.CUR_RECOVERY_AMOUNT)\
                                         .otherwise(lit(0)))\
            .withColumn('OTHER_RECOVERY', when(clm_rec3.TRANS_TYPE_CD == '', clm_rec3.CUR_RECOVERY_AMOUNT)\
                                         .otherwise(lit(0)))


clm_rec4 = clm_rec4\
            .withColumn('ACCT_LINE_CD', lit('50'))\
            .withColumn('REIN_TYPE_CD', lit('0'))\
            .withColumn('CLAIM_GROSS', lit(0))\
            .withColumn('CLAIM_NET', lit(0))\
            .withColumn('CLAIM_NET_0', lit(0))\
            .withColumn('CLAIM_OBLIGATORY_75', lit(0))\
            .withColumn('CLAIM_FACULTATIVE_76', lit(0))\
            .withColumn('CLAIM_TREATY_77', lit(0))\
            .withColumn('CLAIM_XOL_RECOVERY_78', lit(0))\
            .withColumn('NUM_RI_TREATY_NO', lit(''))\
            .withColumn('REPORT', lit('salvage_subrogation'))

clm_all_01 = unionAll([clm_00,clm_h6,clm_rec4])

## join with gc_clm_gen_info

gc_clm_temp1 = gc_clm_gen_info\
                    .filter(gc_clm_gen_info.NUM_UPDATE_NO == 0)\
                    .drop('NUM_UPDATE_NO')                          

gc_clm_temp = gc_clm_temp1\
                    .select('NUM_CLAIM_NO','TXT_MASTER_CLAIM_NO_NEW')

join_cond = [clm_all_01.NUM_CLAIM_NO == gc_clm_temp.NUM_CLAIM_NO]

clm_all_01_01 = clm_all_01\
                .join(gc_clm_temp, join_cond, "left_outer")\
                .drop(gc_clm_temp.NUM_CLAIM_NO)

## join with gc_clm_gen_info

gc_clm_gen_info1 = gc_clm_gen_info\
                        .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('NUM_UPDATE_NO').alias('check'))

join_cond = [gc_clm_gen_info.NUM_CLAIM_NO == gc_clm_gen_info1.NUM_CLAIM_NO,\
             gc_clm_gen_info.NUM_UPDATE_NO == gc_clm_gen_info1.check]

gc_clm_gen_info2 = gc_clm_gen_info\
                        .join(gc_clm_gen_info1, join_cond, "inner")\
                        .drop(gc_clm_gen_info1.NUM_CLAIM_NO)\
                        .drop(gc_clm_gen_info1.check)\
                        .drop(gc_clm_gen_info.NUM_UPDATE_NO)

gc_clm_gen_info2 = gc_clm_gen_info2\
                    .select('NUM_CLAIM_NO','GEN_INFO_NUM_REFERENCE_NO','POLICY_NO')\
                    .withColumn('policy_length', LengthOfString(gc_clm_gen_info2.POLICY_NO))

join_cond = [clm_all_01_01.NUM_CLAIM_NO == gc_clm_gen_info2.NUM_CLAIM_NO]

clm_all_01_02 = clm_all_01_01\
            .join(gc_clm_gen_info2, join_cond, "left_outer")\
            .drop(gc_clm_gen_info2.NUM_CLAIM_NO)

clm_all_03 = clm_all_01_02\
                .withColumn('NUM_REFERENCE_NO',\
                            when((clm_all_01_02.NUM_REFERENCE_NO.isNull())|\
                                 (clm_all_01_02.NUM_REFERENCE_NO == ''), clm_all_01_02.GEN_INFO_NUM_REFERENCE_NO)\
                            .otherwise(clm_all_01_02.NUM_REFERENCE_NO))\
                .withColumn('POLICY_NO', when(clm_all_01_02.policy_length > 10, firstTenChar(clm_all_01_02.POLICY_NO))\
                                        .otherwise(clm_all_01_02.POLICY_NO))\
                .drop('GEN_INFO_NUM_REFERENCE_NO','policy_length')

#### Get co_insurer details
cin_tmp1 = co_insurance_details_tab
cin_tmp2 = cin_tmp1\
            .groupBy('NUM_REFERENCE_NUMBER')\
            .agg(sf.max('COINSURANCE_PCT').alias('COINSURANCE_PCT'),\
                 sf.max('COINSURANCE_CD').alias('COINSURANCE_CD'),\
                 sf.max('COINSURANCE_DESC').alias('COINSURANCE_DESC'))

### join with co_insurance_details_tab

join_cond = [clm_all_03.NUM_REFERENCE_NO == cin_tmp2.NUM_REFERENCE_NUMBER]

clm_all = clm_all_03\
                .join(cin_tmp2, join_cond, 'left_outer')\
                .drop(cin_tmp2.NUM_REFERENCE_NUMBER)

clm_all = clm_all\
                .withColumn('CLAIM_NO', firstTenChar(clm_all.TXT_MASTER_CLAIM_NO_NEW))\
                .withColumn('CLAIM_FEATURE_NO', afterTenChar(clm_all.TXT_MASTER_CLAIM_NO_NEW))\
                .withColumn('CLAIM_FEATURE_CONCAT', clm_all.TXT_MASTER_CLAIM_NO_NEW)\
#                 .withColumn('DAT_VOUCHER_DATE',col('DAT_VOUCHER_DATE').cast('String'))\
#                 .withColumn('DAT_TRANS_DATE',col('DAT_TRANS_DATE').cast('String'))\
#                 .withColumn('DAT_REFERENCE_DATE',col('DAT_REFERENCE_DATE').cast('String'))\
#                 .withColumn('DAT_TRANSACTION_DATE',col('DAT_TRANSACTION_DATE').cast('String'))

#Modified by Satya on 20190909 for YTD fields, to be populated across all reports.
# clm_all_01 = clm_all.drop('YTD_GROSS_PAID_AMOUNT')\
#                     .drop('YTD_NET_PAID_AMOUNT')

# clmpd_g6_ytd_01 = clmpd_g6.select('NUM_CLAIM_NO','ACCT_LINE_CD','YTD_GROSS_PAID_AMOUNT','YTD_NET_PAID_AMOUNT')
# clmpd_g6_ytd_02 = clmpd_g6_ytd_01.groupBy('NUM_CLAIM_NO','ACCT_LINE_CD')\
#                       .agg(sf.max('YTD_GROSS_PAID_AMOUNT').alias('YTD_GROSS_PAID_AMOUNT'),\
#                           sf.max('YTD_NET_PAID_AMOUNT').alias('YTD_NET_PAID_AMOUNT'))
    
# clm_all_02 = clm_all_01.join(clmpd_g6_ytd_02,['NUM_CLAIM_NO','ACCT_LINE_CD'],'left_outer')
clm_all_02 = clm_all.drop('TXT_MASTER_CLAIM_NO_NEW')

#####   ADD POLICY_CANCELLATION_DATE
## acc_general_ledger (acc_general_ledger)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "account_gc_acc_general_ledger",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":6} 
acc_pol_cancellation = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REFERENCE_NO','DAT_VOUCHER_DATE')\
.filter(col("TXT_LEDGER_ACCOUNT_CD")==lit('6010001110'))\
.filter(col("DAT_VOUCHER_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))

acc_pol_cancellation1 = acc_pol_cancellation\
                            .withColumn('DAT_VOUCHER_DATE', to_date(acc_pol_cancellation.DAT_VOUCHER_DATE, format='yyyy-MM-dd'))\
# acc_general_ledger1 = acc_general_ledger\
#                             .filter(acc_general_ledger.TXT_LEDGER_ACCOUNT_CD == '6010001110')
agl = acc_pol_cancellation1\
            .groupBy('NUM_REFERENCE_NO')\
            .agg(sf.max('DAT_VOUCHER_DATE').alias('POLICY_CANCELLATION_DATE'))
clm_all_02 = clm_all_02.join(agl,'NUM_REFERENCE_NO','left_outer')

###################################################################################################
## gc_clm_transaction_history (gc_clm_transaction_history)

#CHEQUE_DATE added on 20190909
gc_clm_th = gc_clm_transaction_history_base\
                    .withColumnRenamed('NUM_PAYMENTNUMBER','NUM_TRANSACTION_CONTROL_NO')\
                    .withColumnRenamed('TXT_APPROVERID','APPROVER_ID')\
                    .withColumnRenamed('TXT_USERID','TXT_USER_ID')\
                    .withColumnRenamed('TXT_CHEQUENO','CHEQUE_NO')\
                    .withColumn('CHEQUE_DATE',to_date(gc_clm_transaction_history_base.DAT_TRANSACTIONDATE, format='yyyy-MM-dd'))

gc_clm_th0 = gc_clm_th\
                .withColumn('ACCT_LINE_CD',\
                            when(lower(trim(gc_clm_th.TXT_ACCOUNTLINE)) == 'indemnity', '50')\
                            .when(lower(trim(gc_clm_th.TXT_ACCOUNTLINE)) == 'expense', '55'))


#CHEQUE_DATE added on 20190909
gc_clm_th1 = gc_clm_th0\
                .groupBy('NUM_CLAIM_NO','NUM_TRANSACTION_CONTROL_NO','ACCT_LINE_CD')\
                .agg(sf.max('APPROVER_ID').alias('APPROVER_ID'),\
                     sf.max('TXT_USER_ID').alias('TXT_USER_ID'),\
                     sf.max('TXT_TRANSACTIONTYPE').alias('TRANSACTIONTYPE'),\
                     sf.max('NUM_ISACTIVE').alias('NUM_ISACTIVE'),\
                     sf.max('TXT_REASON').alias('TXT_REASON'),\
                     sf.max('CHEQUE_NO').alias('CHEQUE_NO'),\
                     sf.max('CHEQUE_DATE').alias('CHEQUE_DATE'))

clm_00_3_0 = clm_all_02\
                .join(gc_clm_th1, ['NUM_CLAIM_NO','NUM_TRANSACTION_CONTROL_NO','ACCT_LINE_CD'], "left_outer")
                
clm_00_3_1 = clm_00_3_0\
                .withColumn('CWP_REASON', when(clm_00_3_0.REPORT == 'cwp', clm_00_3_0.TXT_REASON)\
                                     .otherwise(lit('')))
clm_all_02 = clm_00_3_1.drop('TXT_REASON')
###################################################################################################

#Code modification for taking latest values of Expense/Indemnity feature level by satya 20190819
# gc_clm_th1 = gc_clm_transaction_history.filter(gc_clm_transaction_history.DAT_TRANS_DATE <= report_date)
gc_clm_th1 = gc_clm_transaction_history\
                    .filter((gc_clm_transaction_history.DAT_TRANS_DATE <= report_date)&\
                            (gc_clm_transaction_history.NUM_ISACTIVE =='1'))
##20190827
gc_clm_th2 = gc_clm_th1.select('NUM_CLAIM_NO','ACCT_LINE_CD','NUM_SERIAL_NO','TXT_TRANSACTIONTYPE','TXT_USERID')

gc_clm_th3 = gc_clm_th2\
                      .groupBy('NUM_CLAIM_NO','ACCT_LINE_CD')\
                      .agg(sf.max('NUM_SERIAL_NO').alias('NUM_SERIAL_NO'))

gc_clm_th4 = gc_clm_th2.join(gc_clm_th3, ['NUM_CLAIM_NO','ACCT_LINE_CD','NUM_SERIAL_NO'], "inner")

gc_clm_th5 = gc_clm_th4.groupBy('NUM_CLAIM_NO','ACCT_LINE_CD')\
                      .agg(sf.max('TXT_TRANSACTIONTYPE').alias('TXT_TRANSACTIONTYPE_GTH'),\
                          sf.max('TXT_USERID').alias('TXT_USER_ID_TMP'))

clm_all_03 = clm_all_02.join(gc_clm_th5, ['NUM_CLAIM_NO','ACCT_LINE_CD'], "left_outer")


# Below code added by satya in order to insure indemnity and expense status codes are correct
##Incorporated from p1 to metric in order to filter out clodes oslr claims (optimization)
#####################################################################################################
clm_all_04 = clm_all_03\
                .withColumn('INDEMNITY_STATUS_AT_FEATURE_LEVEL',\
                            when((clm_all_03.ACCT_LINE_CD == '50')&\
                                ((lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'advice')|\
                                (lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'partial payment')|\
                                (lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'decrease in reserve')|\
                                (lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'reopen')|\
                                (lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'payment voiding')|\
                                (lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 're-open')|\
                                ((lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'increase in reserve')&\
                                 (lower(trim(clm_all_03.TXT_USER_ID_TMP)) != 'system generated entry'))|\
                                (lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'payment listing')), lit('Open'))\
                           .otherwise(lit('')))\
                .withColumn('EXPENSES_STATUS_AT_FEATURE_LEVEL',\
                            when((clm_all_03.ACCT_LINE_CD == '55')&\
                                 (((lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'increase in reserve') &\
                                  (lower(trim(clm_all_03.TXT_USER_ID_TMP)) != 'system generated entry'))|\
                                (lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'advice')|\
                                (lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'partial payment')|\
                                (lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'decrease in reserve')|\
                                (lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'reopen')|\
                                (lower(trim(clm_all_03.TXT_TRANSACTIONTYPE_GTH)) == 'payment listing')), lit('Open'))\
                           .otherwise(lit('')))

clm_all_05 = clm_all_04\
                .withColumn('INDEMNITY_STATUS_AT_FEATURE_LEVEL',\
                            when((clm_all_04.ACCT_LINE_CD == '50')&\
                                ((lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'payment after closing')|\
                                ((lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'reserves released - final payment') &\
                                 (lower(trim(clm_all_04.TXT_USER_ID_TMP)) == 'system generated entry'))|\
                                ((lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'increase in reserve')&\
                                   (lower(trim(clm_all_04.TXT_USER_ID_TMP)) == 'system generated entry'))|\
                                (lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'reserves released - markoff/cancel')|\
                                (lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'final payment-reinstatement deduction')|\
                                (lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'final payment')|\
                                (lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'mark off')|\
                                (lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'mark off reserve')|\
                                (lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'markoff')), lit('Close'))\
                                .otherwise(clm_all_04.INDEMNITY_STATUS_AT_FEATURE_LEVEL))\
                .withColumn('EXPENSES_STATUS_AT_FEATURE_LEVEL',\
                            when((clm_all_04.ACCT_LINE_CD == '55')&\
                                 (((lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'reserves released - final payment') &\
                                  (lower(trim(clm_all_04.TXT_USER_ID_TMP)) == 'system generated entry'))|\
                                 ((lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'increase in reserve') &\
                                  (lower(trim(clm_all_04.TXT_USER_ID_TMP)) == 'system generated entry'))|\
                                (lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'payment after closing')|\
                                (lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'reserve released')|\
                                (lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'final payment')|\
                                (lower(trim(clm_all_04.TXT_TRANSACTIONTYPE_GTH)) == 'mark off reserve')), lit('Close'))\
                                .otherwise(clm_all_04.EXPENSES_STATUS_AT_FEATURE_LEVEL))\
                .drop('TXT_USER_ID_TMP')

clm_all_06 = clm_all_05\
                .withColumn('INDEMNITY_STATUS_AT_FEATURE_LEVEL',\
                            when((clm_all_05.ACCT_LINE_CD == '50')&\
                                  ((lower(trim(clm_all_05.TXT_TRANSACTIONTYPE_GTH)) == 'subrogation')|\
                                  (lower(trim(clm_all_05.TXT_TRANSACTIONTYPE_GTH)) == 'salvage')), lit(''))\
                             .otherwise(clm_all_05.INDEMNITY_STATUS_AT_FEATURE_LEVEL))

feature_summary_01 = feature_summary.distinct()
feature_summary_02 = feature_summary_01\
                                .withColumnRenamed('CLAIMNUMBER','CLAIM_NO')\
                                .withColumn('SOURCE_TEMP', lit('BPM'))

clm_all_07 = clm_all_06.join(feature_summary_02,'CLAIM_NO','left_outer')
clm_all_08 = clm_all_07.withColumn('SOURCE',\
                                   when(clm_all_07.SOURCE_TEMP=='BPM',clm_all_07.SOURCE_TEMP)\
                                  .otherwise(lit('GC')))\
                        .withColumn('LOAD_DATE', lit(datetime.date.today()))\
                        .withColumn('REPORT_DATE', lit(report_date))\
                        .drop('SOURCE_TEMP','TXT_TRANSACTIONTYPE_GTH')

# #########################################Paid SERVICE_CLAIMS Processing from oslr transactions#######################################
# acc_gl4 = acc_gl3\
#                 .filter((acc_gl3.REPORT=='oslr')&\
#                         (acc_gl3.REIN_TYPE_CD=='0')&\
#                         (acc_gl3.DAT_VOUCHER_DATE >= first_day_of_month(report_date))&\
#                         (acc_gl3.DAT_VOUCHER_DATE <= last_day_of_month(report_date)))
# acc_gl5 = acc_gl4\
#             .withColumn('TRANS_TYPE_CD', when((lower(acc_gl4.TXT_TRANSACTION_CATEGORY_CD) == 't85'), lit('21'))\
#                                         .when((lower(acc_gl4.TXT_TRANSACTION_CATEGORY_CD) == 't86'), lit('23'))\
#                                         .otherwise(lit('22')))
# acc_gl6 = acc_gl5\
#             .groupBy('NUM_CLAIM_NO','NUM_TRANSACTION_CONTROL_NO')\
#             .agg(sf.max('TRANS_TYPE_CD').alias('TRANS_TYPE_CD'))

### gc_clm_gen_info_extra (gc_clm_gen_info_extra)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_gen_info_extra",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clm_gen_info_extra = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','TXT_INFO7')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
.withColumnRenamed('TXT_INFO7', 'CLAIM_CATEGORY_CD')\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))
# .filter((to_date(col("DAT_INSERT_DATE"), format='yyyy-MM-dd'))<=end_date)\

gc_clm_gen_info_extra = gc_clm_gen_info_extra.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## join with gc_clm_gen_info_extra for CLAIM_CATEGORY_CD

gc_clm_gen_info_extra1 = gc_clm_gen_info_extra\
                        .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('NUM_UPDATE_NO').alias('NUM_UPDATE_NO'))

gc_clm_gen_info_extra2 = gc_clm_gen_info_extra.join(gc_clm_gen_info_extra1,['NUM_CLAIM_NO','NUM_UPDATE_NO'] , "inner")

gc_clm_gen_info_extra2 = gc_clm_gen_info_extra2\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('CLAIM_CATEGORY_CD').alias('CLAIM_CATEGORY_CD'))

clm_all_09 = clm_all_08.join(gc_clm_gen_info_extra2, 'NUM_CLAIM_NO', "left_outer")

clm_all_10 = clm_all_09\
                    .withColumn('CLAIM_CATEGORY_CD',\
                                when(trim(clm_all_09.CLAIM_CATEGORY_CD) == '1', lit('AB'))\
                                .when(trim(clm_all_09.CLAIM_CATEGORY_CD) == '2', lit('SC'))\
                                .when(trim(clm_all_09.CLAIM_CATEGORY_CD) == '3', lit('HF'))\
                                .otherwise(lit('')))
clm_all_11 = clm_all_10
# #Taking out paid service claims from oslr for one month
# clm_paid_sc = clm_all_10\
#                 .filter((clm_all_10.REPORT=='oslr')&\
#                         (clm_all_10.REIN_TYPE_CD=='0')&\
#                         (clm_all_10.DAT_VOUCHER_DATE >= first_day_of_month(report_date))&\
#                         (clm_all_10.DAT_VOUCHER_DATE <= last_day_of_month(report_date))&\
#                         (clm_all_10.CLAIM_CATEGORY_CD=='SC'))

# clm_paid_sc_01 = clm_paid_sc.withColumn('REPORT',lit('paid'))\
#                             .drop('TRANS_TYPE_CD')

# clm_paid_sc_02 = clm_paid_sc_01.join(acc_gl6,['NUM_CLAIM_NO','NUM_TRANSACTION_CONTROL_NO'],'left_outer')

# clm_all_11 = unionAll([clm_all_10,clm_paid_sc_02])

# clm_all = clm_all_11\
#                 .filter((clm_all_11.REPORT <> 'oslr')|
#                        ((clm_all_11.REPORT == 'oslr')&\
#                       (((lower(clm_all_11.INDEMNITY_STATUS_AT_FEATURE_LEVEL) != 'close')&\
#                         ((lower(clm_all_11.EXPENSES_STATUS_AT_FEATURE_LEVEL).isNull())|\
#                       (lower(clm_all_11.EXPENSES_STATUS_AT_FEATURE_LEVEL)=='')))|\
#                      ((lower(clm_all_11.EXPENSES_STATUS_AT_FEATURE_LEVEL) != 'close')&\
#                         ((lower(clm_all_11.INDEMNITY_STATUS_AT_FEATURE_LEVEL).isNull())|\
#                       (lower(clm_all_11.INDEMNITY_STATUS_AT_FEATURE_LEVEL)==''))))))
clm_all_12 = clm_all_11\
                .filter((clm_all_11.REPORT != 'oslr')|\
                       ((clm_all_11.REPORT == 'oslr')&\
                        (((lower(clm_all_11.INDEMNITY_STATUS_AT_FEATURE_LEVEL) != 'close')&\
                          ((lower(clm_all_11.EXPENSES_STATUS_AT_FEATURE_LEVEL).isNull())|\
                           (lower(clm_all_11.EXPENSES_STATUS_AT_FEATURE_LEVEL)=='')))|\
                         ((lower(clm_all_11.EXPENSES_STATUS_AT_FEATURE_LEVEL) != 'close')&\
                          ((lower(clm_all_11.INDEMNITY_STATUS_AT_FEATURE_LEVEL).isNull())|\
                           (lower(clm_all_11.INDEMNITY_STATUS_AT_FEATURE_LEVEL)=='')))|\
                         ((lower(clm_all_11.INDEMNITY_STATUS_AT_FEATURE_LEVEL).isNull())&\
                          (lower(clm_all_11.EXPENSES_STATUS_AT_FEATURE_LEVEL).isNull()))|\
                         ((lower(clm_all_11.INDEMNITY_STATUS_AT_FEATURE_LEVEL)=='')&\
                          (lower(clm_all_11.EXPENSES_STATUS_AT_FEATURE_LEVEL)=='')))))

################################################# RI Claims Logic ##########################################################
## ri_mm_inward_claim_details (ri_mm_inward_claim_details)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "policy_gc_ri_mm_inward_claim_details",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
ri_claim_dtls = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NUMBER','TXT_MASTER_CLAIM_NO_NEW','TXT_POLICY_NO_CHAR')

ri_claim_dtls = ri_claim_dtls.withColumn('NUM_CLAIM_NUMBER',col('NUM_CLAIM_NUMBER').cast(DecimalType(20,0)).cast(StringType()))

ri_claim_dtls_01 = ri_claim_dtls\
                        .groupBy('NUM_CLAIM_NUMBER')\
                        .agg(sf.max('TXT_MASTER_CLAIM_NO_NEW').alias('TXT_MASTER_CLAIM_NO_NEW'),\
                             sf.max('TXT_POLICY_NO_CHAR').alias('TXT_POLICY_NO_CHAR'))

ri_claim_dtls_01 = ri_claim_dtls_01.withColumn('policy_length', LengthOfString(ri_claim_dtls_01.TXT_POLICY_NO_CHAR))

ri_claim_dtls_02 = ri_claim_dtls_01\
                        .withColumn('CLAIM_FEATURE_CONCAT_RI', ri_claim_dtls_01.TXT_MASTER_CLAIM_NO_NEW)\
                        .withColumn('CLAIM_NO_RI', firstTenChar(ri_claim_dtls_01.TXT_MASTER_CLAIM_NO_NEW))\
                        .withColumn('CLAIM_FEATURE_NO_RI', afterTenChar(ri_claim_dtls_01.TXT_MASTER_CLAIM_NO_NEW))\
                        .withColumnRenamed('NUM_CLAIM_NUMBER','NUM_CLAIM_NO')\
                        .withColumn('POLICY_NO_RI', \
                        when(ri_claim_dtls_01.policy_length > 10, firstTenChar(ri_claim_dtls_01.TXT_POLICY_NO_CHAR))\
                                        .otherwise(ri_claim_dtls_01.TXT_POLICY_NO_CHAR))\
                        .withColumn('RI_INWARD_FLAG', lit('Y'))\
                        .drop('TXT_MASTER_CLAIM_NO_NEW','policy_length','TXT_POLICY_NO_CHAR')

clm_all_2_tmp = clm_all_12.join(ri_claim_dtls_02,'NUM_CLAIM_NO','left_outer')
clm_all_2_tmp_01 = clm_all_2_tmp\
                        .withColumn('CLAIM_FEATURE_CONCAT', \
                                    when(((trim(clm_all_2_tmp.CLAIM_FEATURE_CONCAT).isNull())|\
                                              (trim(clm_all_2_tmp.CLAIM_FEATURE_CONCAT)=='')), clm_all_2_tmp.CLAIM_FEATURE_CONCAT_RI)\
                                    .otherwise(clm_all_2_tmp.CLAIM_FEATURE_CONCAT))\
                        .withColumn('POLICY_NO', \
                                    when(((trim(clm_all_2_tmp.POLICY_NO).isNull())|\
                                              (trim(clm_all_2_tmp.POLICY_NO)=='')), clm_all_2_tmp.POLICY_NO_RI)\
                                    .otherwise(clm_all_2_tmp.POLICY_NO))\
                        .withColumn('CLAIM_NO', \
                                    when(((trim(clm_all_2_tmp.CLAIM_NO).isNull())|\
                                              (trim(clm_all_2_tmp.CLAIM_NO)=='')), clm_all_2_tmp.CLAIM_NO_RI)\
                                    .otherwise(clm_all_2_tmp.CLAIM_NO))\
                        .withColumn('CLAIM_FEATURE_NO', \
                                    when(((trim(clm_all_2_tmp.CLAIM_FEATURE_NO).isNull())|\
                                              (trim(clm_all_2_tmp.CLAIM_FEATURE_NO)=='')), clm_all_2_tmp.CLAIM_FEATURE_NO_RI)\
                                    .otherwise(clm_all_2_tmp.CLAIM_FEATURE_NO))\
                        .withColumn('RI_INWARD_FLAG', \
                                    when(((trim(clm_all_2_tmp.RI_INWARD_FLAG).isNull())|\
                                              (trim(clm_all_2_tmp.RI_INWARD_FLAG)=='')), lit('N'))\
                                    .otherwise(clm_all_2_tmp.RI_INWARD_FLAG))\
                        .drop('CLAIM_FEATURE_CONCAT_RI','POLICY_NO_RI','CLAIM_NO_RI','CLAIM_FEATURE_NO_RI')
clm_all_13 = clm_all_2_tmp_01\
                        .filter(~((clm_all_2_tmp_01.CLAIM_FEATURE_CONCAT.isNull())|\
                                (trim(clm_all_2_tmp_01.CLAIM_FEATURE_CONCAT)=='')))
################################################# RI Claims Logic ############################################

#####################FilterOut unnecessary transactions###################################################
# clm_all_14 = clm_all_13\
#                    .filter(clm_all_13.REPORT.isin(['advice','cwp','reserve_revision','reopen'])|\
#                            (((clm_all_13.REPORT == 'paid')&\
#                              (clm_all_13.REIN_TYPE_CD == '0')&\
#                              ((clm_all_13.NUM_ISACTIVE == '1')|(clm_all_13.NUM_ISACTIVE.isNull())))|
#                             ((clm_all_13.REPORT == 'sc')&\
#                              (clm_all_13.REIN_TYPE_CD == '0')&\
#                              (clm_all_13.NUM_ISACTIVE == '1')&\
#                              (clm_all_13.CLAIM_CATEGORY_CD=='SC')&\
#                              ((clm_all_13.TRANSACTIONTYPE=='Final Payment')|
#                               (clm_all_13.TRANSACTIONTYPE=='Partial Payment')))|
#                             ((clm_all_13.REPORT == 'paid')&\
#                              (clm_all_13.REIN_TYPE_CD.isin(['75','76','77']))&\
#                              (clm_all_13.RI_INWARD_FLAG=='Y')))|
#                            (((clm_all_13.REPORT == 'oslr')&\
#                              (clm_all_13.REIN_TYPE_CD == '0')&\
#                              ((clm_all_13.NUM_ISACTIVE == '1')|\
#                               (clm_all_13.NUM_ISACTIVE.isNull())))|\
#                             ((clm_all_13.REPORT == 'oslr')&\
#                              (clm_all_13.REIN_TYPE_CD.isin(['75','76','77']))&\
#                              (clm_all_13.RI_INWARD_FLAG=='Y'))))

clm_all_14 = clm_all_13\
                   .filter(clm_all_13.REPORT.isin(['advice','cwp','reserve_revision','reopen','salvage_subrogation'])|\
                           (((clm_all_13.REPORT == 'paid')&\
                             (clm_all_13.REIN_TYPE_CD == '0')&\
                             ((clm_all_13.NUM_ISACTIVE == '1')|(clm_all_13.NUM_ISACTIVE.isNull())))|
                            ((clm_all_13.REPORT == 'sc')&\
                             (clm_all_13.REIN_TYPE_CD == '0')&\
                             (clm_all_13.NUM_ISACTIVE == '1')&\
                             (clm_all_13.CLAIM_CATEGORY_CD=='SC')&\
                             ((clm_all_13.TRANSACTIONTYPE=='Final Payment')|
                              (clm_all_13.TRANSACTIONTYPE=='Partial Payment')))|
                            ((clm_all_13.REPORT == 'paid')&\
                             (clm_all_13.REIN_TYPE_CD.isin(['75','76','77']))&\
                             (clm_all_13.RI_INWARD_FLAG=='Y')))|
                           (((clm_all_13.REPORT == 'oslr')&\
                             (clm_all_13.REIN_TYPE_CD == '0'))|\
                            ((clm_all_13.REPORT == 'oslr')&\
                             (clm_all_13.REIN_TYPE_CD.isin(['75','76','77']))&\
                             (clm_all_13.RI_INWARD_FLAG=='Y'))))

clm_all_15 = clm_all_14.withColumn('REPORT', when(clm_all_14.REPORT=='sc', lit('paid'))\
                                              .otherwise(clm_all_14.REPORT))
#####################FilterOut unnecessary transactions###################################################

s=-1
def ranged_numbers(anything): 
    global s 
    if s >= 47:
        s = 0
    else:
        s = s+1 
    return s

udf_ranged_numbers = udf(lambda x: ranged_numbers(x), IntegerType())
clm_all = clm_all_15.withColumn("ROW_NUM", lit(udf_ranged_numbers(lit(1))))

print ("Deleting data from Claims Report GC Metric with below query: ")
print ("""delete from datamarts.claim_report_gc_metric where REPORT_DATE BETWEEN '""" + start_date + """' and '""" + end_date + "' ")

from pg import DB

db = DB(dbname='gpadmin', host='10.35.12.194', port=5432,user='gpspark', passwd='spark@456')
dlt_cnt = db.query("""delete from datamarts.claim_report_gc_metric where REPORT_DATE BETWEEN '""" + start_date + """' and '""" + end_date + "' ")

print ("no of records deleted for "+report_date_str)
print (dlt_cnt)

clm_all.write.format("greenplum").option("dbtable","claim_report_gc_metric").option('dbschema','datamarts').option("server.port","1104").option("url","jdbc:postgresql://10.35.12.194:5432/gpadmin").option("user", "gpspark").option("password","spark@456").mode('append').save()

acc_general_ledger_base.unpersist()
clm_00.unpersist()
