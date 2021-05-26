# Change log: COURT_NAME, COSTS_PLAINTIFF, CATASTROPHE_TYPE, TXT_CATASTROPHE_DESC, LCL_CATASTROPHE_NO and SUM_INSURED fixed
# Change log: ITD_NET_PAID_AMOUNT calculated here - optimization
# Change log: INDEMNITY_STATUS & EXPENSES_STATUS incorporated in metric to filted closed claims in oslr (Optimization) 10-04-2020
# Change log: date filter applied while reading gc_clm_gen_info - optimization
# Change log: date filter applied while reading claim_gc_gc_clm_gen_info_extra - optimization
# Change log: date filter applied while reading gc_clm_payment_details - optimization
# Change log: date filter applied while reading acc_voucher - optimization
# Change log: date filter applied while reading gen_prop_information_tab - optimization
# Change log: date filter applied while reading gc_clm_gen_settlement_info - optimization
# Change log: date filter applied while reading gc_clm_surveyor/code modification for NUM_UPDATE_NO - optimization
# Change log: code modification for NUM_UPDATE_NO in gc_clm_claim_diary - optimization
# Change log: date filter applied while reading gc_clm_tp_award_dtls/code modification for NUM_UPDATE_NO - optimization
# Change log: date filter applied while reading gc_clm_tp_petition_dtls/code modification for NUM_UPDATE_NO - optimization
# Change log: date filter applied while reading gc_clm_reopen/NUM_REASON_FOR_REOPENING typecast/code modification for NUM_UPDATE_NO - optimization
# Change log: gl filter applied while reading acc_general_ledger - optimization
# Change log: LDDESC filter applied while reading cnfgtr_policy_ld_dtls - optimization - not done
# Change log: Reading REPORT_DATE and SOURCE from metric
# Change log: uw_proposal_addl_info applied uw_filter - optimization
# Change log: Introduced ri_mm_inward_claim_details table for RI related info
# Change log: Filter out claims having null claim_feature_concat
# Change log: POLICY_CANCELLATION_DATE moved to metric- optimization
# Change log: CLAIM_CATEGORY_CD moved to metric- to handle paid service claims
# Change log: APPROVER_ID,TXT_USER_ID,TRANSACTIONTYPE,NUM_ISACTIVE,CWP_REASON,CHEQUE_NO,CHEQUE_DATE moved to metric
# Change log: TXT_WITHHOLDINGTAX deduction applied in YTD_GROSS amount
# Change log: CUR_PROFFESIONAL_FEE removed from ITD_GROSS amount
# Change log: applied abs to all itd and ytd gross fields - reverted
# Change log: RI_INWARD_FLAG incorporated
# Change log: Fix/logic change for cgst,igst,sgst,net_cheq,pay_form,interest_amount
# Change log: TXT_CATASTROPHE_DESC/CATASTROPHE_TYPE fixed
# Change log: BANK_ACCT_NO,BANK_NAME,IFSC_CODE,TDS_AMOUNT,FEES,SURVEYORCODE and related fields.
# Change log: TYPE_OF_SURVEY fix for dept_cd 99
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
                app_name = "Claim_Report_GC_P1_"+runtype+": "+report_date_str
        elif runtype == 'daily':
            app_name = "Claim_Report_GC_P1_"+runtype

no_of_cpu = 8
max_cores = 16
executor_mem = '41g'
# runtype = 'monthly'
# report_date_str = '2020-09-30'

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

##P1
from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import asc,lit
#warnings.filterwarnings('error')
import pyspark
from datetime import datetime,timedelta
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
conf = pyspark.SparkConf()
#import numpy
import calendar
import time
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
# conf.set("spark.driver.maxResultSize","0")
conf.set("spark.app.name", app_name)
conf.set('spark.es.scroll.size','10000')
conf.set('spark.network.timeout','600s')
conf.set('spark.sql.crossJoin.enabled', 'true')
conf.set('spark.executor.heartbeatInterval','60s')
conf.set("spark.driver.cores","6")
conf.set("spark.driver.extraJavaOptions","-Xms6g -Xmx12g")
# conf.set("spark.memory.offHeap.enabled","true")
# conf.set("spark.memory.offHeap.size","32g")
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
# num_claim_no_str = ["30012031199055594001"]
# num_refer_no_list = [201806290596838]
# trans_control_no_str = ["20191016103102020207","20190625103100805947"]
# cust_cd_list = ["6021994738"]
# loc_cd_list = ["58153223"]
# num_claim_no_new = [Decimal(i) for i in num_claim_no_str]
# trans_control_no_list_new = [Decimal(i) for i in trans_control_no_str]

# select * from datamarts.claim_report_gc_metric
# where report_date='2020-06-30'
# and claim_feature_concat='0821814128A'
# select TXT_CUSTOMER_ID from policy_gc_gen_prop_information_tab
# where NUM_REFERENCE_NUMBER=201907100072474
# select distinct NUM_LOSS_LOCATION_CD from claim_gc_gc_clm_gen_info
# where num_claim_no='30043014209050005001'

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
YYMMDD = report_date.strftime('%Y%m%d')

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

####  NON-FINANCIAL PART START
## claim_report_gc_metric (claim_report_gc_metric)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": "datamarts",
         "dbtable": "claim_report_gc_metric",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":8} 
clm_00_1 = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('CLAIM_NO','CLAIM_FEATURE_NO','CLAIM_FEATURE_CONCAT','POLICY_NO','ACCT_LINE_CD','CLAIM_FACULTATIVE_76','CLAIM_GROSS','CLAIM_NET','CLAIM_NET_0','CLAIM_OBLIGATORY_75','CLAIM_TREATY_77','CLAIM_XOL_RECOVERY_78','COINSURANCE_CD','COINSURANCE_DESC','COINSURANCE_PCT','DAT_REFERENCE_DATE','DAT_TRANSACTION_DATE','DAT_TRANS_DATE','DAT_VOUCHER_DATE','NUM_CLAIM_NO','NUM_REFERENCE_NO','NUM_RI_TREATY_NO','NUM_TRANSACTION_CONTROL_NO','OTHER_RECOVERY','POL_ISS_OFF_CD','PRODUCT_CD','RECEIPT_DETAILS','REIN_TYPE_CD','REPORT','SALVAGE_AMOUNT','SALVAGE_BUYER_ADDRESS','SALVAGE_BUYER_NAME','SALVAGE_BUYER_PAN','SALVAGE_BUYER_STATE','SUBROGATION_AMT','TRANS_TYPE_CD','TXT_ISSUE_OFFICE_CD','TXT_MMCP_CODE','INDEMNITY_STATUS_AT_FEATURE_LEVEL','EXPENSES_STATUS_AT_FEATURE_LEVEL','SOURCE','REPORT_DATE','POLICY_CANCELLATION_DATE','CLAIM_CATEGORY_CD','APPROVER_ID','TXT_USER_ID','TRANSACTIONTYPE','NUM_ISACTIVE','CWP_REASON','CHEQUE_NO','CHEQUE_DATE','RI_INWARD_FLAG','DISPLAY_PRODUCT_CD','ROW_NUM')\
.filter(col("REPORT_DATE").between(to_date(lit(start_date), format='yyyy-MM-dd'),to_date(lit(end_date), format='yyyy-MM-dd')))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

clm_00_1.persist()

# clm_00_1.count()

# ################################################# RI Claims Logic ##########################################################
# ## ri_mm_inward_claim_details (ri_mm_inward_claim_details)
# gscPythonOptions = {
#          "url": url_prod,
#          "user": user_prod,
#          "password": pwd_prod,
#          "dbschema": dbschema_etl,
#          "dbtable": "policy_gc_ri_mm_inward_claim_details",
#          "partitionColumn":"row_num",
#          "server.port":server_port,
#          "partitions":1} 
# ri_claim_dtls = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
# .select('NUM_CLAIM_NUMBER','TXT_MASTER_CLAIM_NO_NEW','TXT_POLICY_NO_CHAR')

# ri_claim_dtls = ri_claim_dtls.withColumn('NUM_CLAIM_NUMBER',col('NUM_CLAIM_NUMBER').cast(DecimalType(20,0)).cast(StringType()))

# ri_claim_dtls_01 = ri_claim_dtls\
#                         .groupBy('NUM_CLAIM_NUMBER')\
#                         .agg(sf.max('TXT_MASTER_CLAIM_NO_NEW').alias('TXT_MASTER_CLAIM_NO_NEW'),\
#                              sf.max('TXT_POLICY_NO_CHAR').alias('TXT_POLICY_NO_CHAR'))

# ri_claim_dtls_01 = ri_claim_dtls_01.withColumn('policy_length', LengthOfString(ri_claim_dtls_01.TXT_POLICY_NO_CHAR))

# ri_claim_dtls_02 = ri_claim_dtls_01\
#                         .withColumn('CLAIM_FEATURE_CONCAT_RI', ri_claim_dtls_01.TXT_MASTER_CLAIM_NO_NEW)\
#                         .withColumn('CLAIM_NO_RI', firstTenChar(ri_claim_dtls_01.TXT_MASTER_CLAIM_NO_NEW))\
#                         .withColumn('CLAIM_FEATURE_NO_RI', afterTenChar(ri_claim_dtls_01.TXT_MASTER_CLAIM_NO_NEW))\
#                         .withColumnRenamed('NUM_CLAIM_NUMBER','NUM_CLAIM_NO')\
#                         .withColumn('POLICY_NO_RI', \
#                         when(ri_claim_dtls_01.policy_length > 10, firstTenChar(ri_claim_dtls_01.TXT_POLICY_NO_CHAR))\
#                                         .otherwise(ri_claim_dtls_01.TXT_POLICY_NO_CHAR))\
#                         .withColumn('RI_INWARD_FLAG', lit('Y'))\
#                         .drop('TXT_MASTER_CLAIM_NO_NEW','policy_length','TXT_POLICY_NO_CHAR')

# clm_all_2_tmp = clm_00_1.join(ri_claim_dtls_02,'NUM_CLAIM_NO','left_outer')
# clm_all_2_tmp_01 = clm_all_2_tmp\
#                         .withColumn('CLAIM_FEATURE_CONCAT', \
#                                     when(((trim(clm_all_2_tmp.CLAIM_FEATURE_CONCAT).isNull())|\
#                                               (trim(clm_all_2_tmp.CLAIM_FEATURE_CONCAT)=='')), clm_all_2_tmp.CLAIM_FEATURE_CONCAT_RI)\
#                                     .otherwise(clm_all_2_tmp.CLAIM_FEATURE_CONCAT))\
#                         .withColumn('POLICY_NO', \
#                                     when(((trim(clm_all_2_tmp.POLICY_NO).isNull())|\
#                                               (trim(clm_all_2_tmp.POLICY_NO)=='')), clm_all_2_tmp.POLICY_NO_RI)\
#                                     .otherwise(clm_all_2_tmp.POLICY_NO))\
#                         .withColumn('CLAIM_NO', \
#                                     when(((trim(clm_all_2_tmp.CLAIM_NO).isNull())|\
#                                               (trim(clm_all_2_tmp.CLAIM_NO)=='')), clm_all_2_tmp.CLAIM_NO_RI)\
#                                     .otherwise(clm_all_2_tmp.CLAIM_NO))\
#                         .withColumn('CLAIM_FEATURE_NO', \
#                                     when(((trim(clm_all_2_tmp.CLAIM_FEATURE_NO).isNull())|\
#                                               (trim(clm_all_2_tmp.CLAIM_FEATURE_NO)=='')), clm_all_2_tmp.CLAIM_FEATURE_NO_RI)\
#                                     .otherwise(clm_all_2_tmp.CLAIM_FEATURE_NO))\
#                         .withColumn('RI_INWARD_FLAG', \
#                                     when(((trim(clm_all_2_tmp.RI_INWARD_FLAG).isNull())|\
#                                               (trim(clm_all_2_tmp.RI_INWARD_FLAG)=='')), lit('N'))\
#                                     .otherwise(clm_all_2_tmp.RI_INWARD_FLAG))\
#                         .drop('CLAIM_FEATURE_CONCAT_RI','POLICY_NO_RI','CLAIM_NO_RI','CLAIM_FEATURE_NO_RI')
# clm_all_2_tmp_02 = clm_all_2_tmp_01\
#                         .filter(~((clm_all_2_tmp_01.CLAIM_FEATURE_CONCAT.isNull())|\
#                                 (trim(clm_all_2_tmp_01.CLAIM_FEATURE_CONCAT)=='')))
clm_00_1_02 = clm_00_1\
                    .withColumn('INDEMNITY_STATUS_AT_FEATURE_LEVEL',\
                                   when((trim(clm_00_1.INDEMNITY_STATUS_AT_FEATURE_LEVEL).isNull()),lit(''))\
                                       .otherwise(clm_00_1.INDEMNITY_STATUS_AT_FEATURE_LEVEL))\
                    .withColumn('EXPENSES_STATUS_AT_FEATURE_LEVEL',\
                                   when((trim(clm_00_1.EXPENSES_STATUS_AT_FEATURE_LEVEL).isNull()),lit(''))\
                                       .otherwise(clm_00_1.EXPENSES_STATUS_AT_FEATURE_LEVEL))
################################################# RI Claims Logic ##########################################################

# clm_ncn = clm_all_2_tmp_02[['NUM_CLAIM_NO']].distinct()
# clm_nrn = clm_all_2_tmp_02[['NUM_REFERENCE_NO']].distinct()
# clm_ntn = clm_all_2_tmp_02[['NUM_TRANSACTION_CONTROL_NO']].distinct()
# s=-1
# def ranged_numbers(anything): 
#     global s 
#     if s >= 47:
#         s = 0
#     else:
#         s = s+1 
#     return s

# udf_ranged_numbers = udf(lambda x: ranged_numbers(x), IntegerType())
# clm_ncn = clm_ncn.withColumn("ROW_NUM", lit(udf_ranged_numbers(lit(1))))
# clm_nrn = clm_nrn.withColumn("ROW_NUM", lit(udf_ranged_numbers(lit(1))))
# clm_ntn = clm_ntn.withColumn("ROW_NUM", lit(udf_ranged_numbers(lit(1))))

# from pg import DB
# db = DB(dbname='gpadmin', host='10.35.12.194', port=5432,user='gpspark', passwd='spark@456')
# db.query("""delete from staging.claim_report_ncn""")
# db.query("""delete from staging.claim_report_nrn""")
# db.query("""delete from staging.claim_report_ntn""")

# clm_ncn.write.format("greenplum").option("dbtable","claim_report_ncn").option('dbschema','staging').option("server.port","1104").option("url","jdbc:postgresql://10.35.12.194:5432/gpadmin").option("user", "gpspark").option("password","spark@456").mode('append').save()
# clm_nrn.write.format("greenplum").option("dbtable","claim_report_nrn").option('dbschema','staging').option("server.port","1104").option("url","jdbc:postgresql://10.35.12.194:5432/gpadmin").option("user", "gpspark").option("password","spark@456").mode('append').save()
# clm_ntn.write.format("greenplum").option("dbtable","claim_report_ntn").option('dbschema','staging').option("server.port","1104").option("url","jdbc:postgresql://10.35.12.194:5432/gpadmin").option("user", "gpspark").option("password","spark@456").mode('append').save()

# gscPythonOptions = {
#          "url": url_prod,
#          "user": user_prod,
#          "password": pwd_prod,
#          "dbschema": "staging",
#          "dbtable": "claim_report_ncn",
#          "partitionColumn":"row_num",
#          "server.port":server_port,
#          "partitions":1} 
# clm_ncn = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
# .select('NUM_CLAIM_NO')

# gscPythonOptions = {
#          "url": url_prod,
#          "user": user_prod,
#          "password": pwd_prod,
#          "dbschema": "staging",
#          "dbtable": "claim_report_nrn",
#          "partitionColumn":"row_num",
#          "server.port":server_port,
#          "partitions":1} 
# clm_nrn = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
# .select('NUM_REFERENCE_NO')

# gscPythonOptions = {
#          "url": url_prod,
#          "user": user_prod,
#          "password": pwd_prod,
#          "dbschema": "staging",
#          "dbtable": "claim_report_ntn",
#          "partitionColumn":"row_num",
#          "server.port":server_port,
#          "partitions":1} 
# clm_ntn = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
# .select('NUM_TRANSACTION_CONTROL_NO')

# clm_00_1_02.count()

### gc_clm_gen_info (gc_clm_gen_info)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_gen_info",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":8} 
gc_clm_gen_info_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','DAT_LOSS_DATE','DAT_NOTIFICATION_DATE','DAT_REGISTRATION_DATE','NUM_CATASTROPHE_CD','NUM_PROXIMITY_DAYS','TXT_NAME_OF_INSURED','TXT_REMARKS','TXT_SERVICING_OFFICE_CD','YN_CLOSE_PROXIMITY','DAT_TRANS_DATE','NUM_LOSS_LOCATION_CD','NUM_NATURE_OF_LOSS','NUM_PIN_CD')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))
# .filter((to_date(col("DAT_INSERT_DATE"), format='yyyy-MM-dd'))<=end_date)\

gc_clm_gen_info_base = gc_clm_gen_info_base.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

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
gc_clm_gen_info_extra_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','TXT_INFO10','TXT_INFO12','TXT_INFO14','TXT_INFO16','TXT_INFO17','TXT_INFO18','TXT_INFO19','TXT_INFO29','TXT_INFO3','TXT_INFO31','TXT_INFO35','TXT_INFO36','TXT_INFO37','TXT_INFO39','TXT_INFO4','TXT_INFO43','TXT_INFO44','TXT_INFO45','TXT_INFO46','TXT_INFO47','TXT_INFO49','TXT_INFO50','TXT_INFO51','TXT_INFO52','TXT_INFO53','TXT_INFO55','TXT_INFO6','TXT_INFO72','TXT_INFO76','TXT_INFO80')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_gen_info_extra_base = gc_clm_gen_info_extra_base.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## gc_clm_transaction_history (gc_clm_transaction_history)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_transaction_history",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":12} 
gc_clm_transaction_history_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_PAYMENTNUMBER','TXT_APPROVERID','TXT_USERID','DAT_TRANSACTIONDATE','NUM_SERIAL_NO','TXT_TRANSACTIONTYPE','TXT_CHEQUENO','TXT_ACCOUNTLINE','NUM_ISACTIVE','TXT_REASON','NUM_TRANSACTIONAMOUNT','TXT_WITHHOLDINGTAX')\
.filter(col("DAT_TRANSACTIONDATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))
# .filter(col("NUM_ISACTIVE")=='1')\

gc_clm_transaction_history_base = gc_clm_transaction_history_base\
                                    .withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))\
                                    .withColumn('NUM_PAYMENTNUMBER',col('NUM_PAYMENTNUMBER').cast(DecimalType(20,0)).cast(StringType()))

## gc_clm_payment_details (gc_clm_payment_details)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_payment_details",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":2} 
gc_clm_payment_details_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_CLAIM_RECORD_ID','NUM_UPDATE_NO','APPROVEDUSERID','CUR_AMOUNT','CUR_PROFFESIONAL_FEE','CUR_TDS','DAT_POSTING_DATE','NUM_CGST_AMOUNT','NUM_IGST_AMOUNT','NUM_SGST_UGST_AMOUNT','NUM_TYPE_OF_PARTY','TXT_BENEFICIARY_ACCOUNT_NO','TXT_BENEFICIARY_BANK_CD','TXT_BENEFICIARY_BANK_NAME','TXT_IFSC_CODE','TXT_INSTRUMENT_NO','TXT_MULTIPLE_STATUS','TXT_PAYEE_CUSTOMER_VENDOR_CD','TXT_PYMNT_FORM','TXT_STATUS','TXT_TATAAIG_GST_NUMBER','TXT_PAYEE_CUSTOMER_VENDOR_NAME','CUR_EXPENSE_AMOUNT','CUR_APPROVER_RECOM_AMOUNT','CUR_TOT_EXP_CLAIMED_AMT')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_payment_details_base = gc_clm_payment_details_base\
                                .withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))\
                                .withColumn('NUM_CLAIM_RECORD_ID',col('NUM_CLAIM_RECORD_ID').cast(DecimalType(20,0)).cast(StringType()))

## genmst_customer (genmst_customer)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "customer_gc_genmst_customer",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":16}
genmst_customer_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('CUSTOMER_CODE','YN_VIP')\
# .filter(col("CUSTOMER_CODE").isin(cust_cd_list))

## genmst_location (genmst_location)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "customer_gc_genmst_location",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":8}
genmst_location_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_LOCATION_CD','TXT_ADDRESS_LINE_1','TXT_ADDRESS_LINE_2','TXT_ADDRESS_LINE_3','NUM_STATE_CD','NUM_CITYDISTRICT_CD','TXT_STATE')\
# .filter(col("NUM_LOCATION_CD").isin(loc_cd_list))

## gc_clm_surveyor (gc_clm_surveyor)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_surveyor",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":5}
gc_clm_surveyor_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_SERIAL_NO','NUM_INTERMEDIARY_CD','DAT_DATE_OF_APPOINTMENT','DAT_REPORT_SUBMISSION_DATE','TXT_INTERMEDIARY_NAME','NUM_SURVEY_TYPE', 'DAT_DATE_OF_LICENSE_EXPIRY')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_surveyor_base = gc_clm_surveyor_base.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

# ## acc_voucher (acc_voucher)
# gscPythonOptions = {
#          "url": url_prod,
#          "user": user_prod,
#          "password": pwd_prod,
#          "dbschema": dbschema_etl,
#          "dbtable": "account_gc_acc_voucher",
#          "partitionColumn":"row_num",
#          "server.port":server_port,
#          "partitions":14}
# acc_voucher_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
# .select('NUM_TRANSACTION_CONTROL_NO','TXT_PAYEE_ADDRESS')\
# .filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# # .filter(col("NUM_TRANSACTION_CONTROL_NO").isin(trans_control_no_list_new))

## uw_proposal_addl_info (uw_proposal_addl_info)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "underwriting_gc_uw_proposal_addl_info",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":6}
uw_proposal_addl_info_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REFERERANCE_NO','TXT_RENL_CERT_NO','TXT_EFF_DT_SEQ_NO')\
.filter((col("TXT_EFF_DT_SEQ_NO").isNotNull())|(col("TXT_RENL_CERT_NO").isNotNull()))\
# .filter(col("NUM_REFERERANCE_NO").isin(num_refer_no_list))

## distribution_channel_tab (distribution_channel_tab)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "underwriting_gc_distribution_channel_tab",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":10}
distribution_channel_tab_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REFERENCE_NUMBER','TXT_INTERMEDIARY_CODE','TXT_INTERMEDIARY_NAME','TXT_TYPE_OF_ENDORSEMENT')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_REFERENCE_NUMBER").isin(num_refer_no_list))

## gen_prop_information_tab (gen_prop_information_tab)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "policy_gc_gen_prop_information_tab",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":10}
gen_prop_information_tab_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REFERENCE_NUMBER','TXT_REASON_FOR_CANCELLATION','DAT_DBLINS_POLICY_ISSUEDATE','DAT_POLICY_EFF_TODATE','DAT_ENDORSEMENT_EFF_DATE','DAT_POLICY_EFF_FROMDATE','NUM_DEPARTMENT_CODE','TXT_CUSTOMER_ID','TXT_INTERNAL_CERTIFICATE_NO','TXT_RE_INSURANCE_INWARD','TXT_MODEOFOPERATION','TXT_POLICY_NO_CHAR','DAT_REFERENCE_DATE','NUM_TRANS_REF_NUMBER','TXT_BUSINESS_TYPE','TXT_POLICYTERM')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_REFERENCE_NUMBER").isin(num_refer_no_list))

## gc_clm_claim_diary (gc_clm_claim_diary)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_claim_diary",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":16} 
gc_clm_claim_diary_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','TXT_INFO1')\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_claim_diary_base = gc_clm_claim_diary_base.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## gc_clm_gen_settlement_info (gc_clm_gen_settlement_info)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_gen_settlement_info",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":2} 
gc_clm_gen_settlement_info_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_SERIAL_NO','NUM_SETTLEMENT_TYPE_CD','TXT_INFO1','TXT_INFO6','TXT_INFO10','TXT_INFO11','TXT_INFO12','TXT_INFO13','TXT_INFO19','TXT_INFO2','TXT_INFO23','TXT_INFO24','TXT_INFO25','TXT_INFO26','TXT_INFO27','TXT_INFO28','TXT_INFO3', 'TXT_INFO30','TXT_INFO31', 'TXT_INFO32', 'TXT_INFO44','TXT_INFO45','TXT_INFO46','TXT_INFO47','TXT_INFO48','TXT_INFO5','TXT_INFO7','DAT_INSERT_DATE')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_gen_settlement_info_base = gc_clm_gen_settlement_info_base.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## gc_clm_tp_award_dtls (gc_clm_tp_award_dtls)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_tp_award_dtls",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clm_tp_award_dtls_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_SERIAL_NO','NUM_AWARD_TYPE_CD','TXT_INFO1','TXT_INFO13','TXT_INFO14','TXT_INFO23','TXT_INFO25','TXT_INFO27','TXT_INFO3')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_tp_award_dtls_base = gc_clm_tp_award_dtls_base.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## gc_clm_tp_petition_dtls (gc_clm_tp_petition_dtls)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_tp_petition_dtls",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clm_tp_petition_dtls_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_SERIAL_NO','DAT_DATE_OF_FILING_OF_PETITION','DAT_DATE_OF_HEARING','TXT_VENUE_CODE','TXT_CASE_NO','NUM_CASE_YEAR','NUM_COURT_LOCATION','NUM_COURT_CODE')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))\
# .filter(col("NUM_CLAIM_NO").isin(num_claim_no_new))

gc_clm_tp_petition_dtls_base = gc_clm_tp_petition_dtls_base.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## cnfgtr_policy_ld_dtls (cnfgtr_policy_ld_dtls)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_cnfgtr_policy_ld_dtls",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
cnfgtr_policy_ld_dtls_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('REFERENCE_NUM','LDDESC','LD_RATE')\
# .filter(col("REFERENCE_NUM").isin(num_refer_no_list))

## claimmst_court (claimmst_court)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_claimmst_court",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
claimmst_court_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TXT_COURT_NAME','NUM_COURT_CODE')

## gc_clm_reopen (gc_clm_reopen)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clm_reopen",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clm_reopen_base = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_SERIAL_NO','NUM_REASON_FOR_REOPENING')\
.filter(col("DAT_INSERT_DATE")<to_timestamp(lit(report_date_filter), format='yyyy-MM-dd'))

gc_clm_reopen_base = gc_clm_reopen_base.withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))

## dw_mst_mmcp_code_map (dw_mst_mmcp_code_map)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_dw_mst_mmcp_code_map",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_mmcp_code_map = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TXT_MAJOR_LINE_DESC','TXT_MAJOR_LINE_CD','TXT_MINOR_LINE_DESC','TXT_MINOR_LINE_CD','TXT_CLASS_PERIL_DESC','TXT_CLASS_PERIL_CD')

## uw_product_master (uw_product_master)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "underwriting_gc_uw_product_master",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
uw_product_master = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('PRODUCTCODE','PRODUCTNAME')\
.withColumn('PRODUCTCODE',col('PRODUCTCODE').cast(DecimalType(4,0)).cast(StringType()))\
.withColumnRenamed('PRODUCTNAME', 'PRODUCT_NAME')

## uw_department_master (uw_department_master)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_uw_department_master",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
uw_department_master = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('DEPARTMENTCODE','DEPARTMENTNAME')

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
genmst_tab_office = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TXT_OFFICE','NUM_OFFICE_CD','NUM_PARENT_OFFICE_CD','TXT_OFFICE_TYPE')

## gc_clmmst_resource_type (gc_clmmst_resource_type)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_gc_clmmst_resource_type",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_resource_type = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_RESOURCE_TYPE_CD','TXT_RESOURCE_TYPE')

## gc_clmmst_repudiation_reason (gc_clmmst_repudiation_reason)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clmmst_repudiation_reason",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_repudiation_reason = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REPUDIATION_REASON_ID','TXT_REPUDIATION_REASON_DESC','TXT_INFO1')

## genmst_employee (genmst_employee)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "corporate_gc_genmst_employee",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
genmst_employee = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TXT_USERID','TXT_EMPLOYEE_NAME','TXT_HR_REF_NO','NUM_EMPLOYEE_CD')

## gc_clmmst_settlement_type_view (gc_clmmst_settlement_type_view)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clmmst_settlement_type_view",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_settlement_type_view = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('ITEM_VALUE','ITEM_TEXT')

## gc_clmmst_pet_venue_forum_view (gc_clmmst_pet_venue_forum_view)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clmmst_pet_venue_forum_view",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_pet_venue_forum_view = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('ITEM_VALUE','ITEM_TEXT')

## gc_clmmst_surveytype (gc_clmmst_surveytype)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clmmst_surveytype",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_surveytype2 = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_SURVEY_TYPE','NUM_DEPARTMENT_CODE','NUM_PRODUCT_CODE','TXT_SUVEY_TYPE_DESC')

## gc_clmmst_court_type (gc_clmmst_court_type)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clmmst_court_type",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_court_type = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_COURT_TYPE','TXT_DESCRIPTION')

# ## gc_clmmst_catestrophy_view (gc_clmmst_catestrophy_view)
# gscPythonOptions = {
#          "url": url_prod,
#          "user": user_prod,
#          "password": pwd_prod,
#          "dbschema": dbschema_etl,
#          "dbtable": "claim_gc_gc_clmmst_catestrophy_view",
#          "partitionColumn":"row_num",
#          "server.port":server_port,
#          "partitions":1} 
# gc_clmmst_catestrophy_view = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
# .select('ITEM_VALUE','ITEM_TEXT')

## clmmst_catestrophy (clmmst_catestrophy)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_clmmst_catestrophy",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_catestrophy = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CATESTROPHY_CD','TXT_CATESTROPHY_CD','NUM_CATASTROPHY_TYP_CD')\
.withColumnRenamed('NUM_CATESTROPHY_CD','LCL_CATASTROPHE_NO')\
.withColumnRenamed('TXT_CATESTROPHY_CD','TXT_CATASTROPHE_DESC')\
.withColumnRenamed('NUM_CATASTROPHY_TYP_CD','CATASTROPHE_TYPE_CD')

gc_clmmst_catestrophy = gc_clmmst_catestrophy.withColumn('LCL_CATASTROPHE_NO',(col('LCL_CATASTROPHE_NO')).cast('string'))

## gc_clm_claim_recovery (gc_clm_claim_recovery)
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
.select('NUM_CLAIM_NO','NUM_FORWARD_VOUCHER_NO','TXT_TYPE_OF_RECOVERY','CUR_RECOVERY_AMOUNT')

gc_clm_claim_recovery_base = gc_clm_claim_recovery_base\
                                .withColumn('NUM_CLAIM_NO',col('NUM_CLAIM_NO').cast(DecimalType(20,0)).cast(StringType()))\
                                .withColumn('NUM_FORWARD_VOUCHER_NO',col('NUM_FORWARD_VOUCHER_NO').cast(DecimalType(20,0)).cast(StringType()))
## genmst_state (genmst_state)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_genmst_state",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
genmst_state = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_STATE_CD','TXT_STATE')\
.withColumnRenamed('TXT_STATE','STATE_OF_LOSS')

## genmst_citydistrict (genmst_citydistrict)
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
.select('NUM_STATE_CD','NUM_CITYDISTRICT_CD','TXT_CITYDISTRICT').distinct()\
.withColumnRenamed('TXT_CITYDISTRICT','LOSSCITY_CIDS')

## genmst_pincode (genmst_pincode)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "customer_gc_genmst_pincode",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
genmst_pincode = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_STATE_CD','NUM_CITYDISTRICT_CD','NUM_PINCODE','TXT_PINCODE_LOCALITY','DAT_INSERT_DATE')

genmst_pincode_ss1 = genmst_pincode
genmst_pincode1 = genmst_pincode\
                        .withColumnRenamed('TXT_PINCODE_LOCALITY','LOSSCITY_PC')\
                        .withColumnRenamed('NUM_STATE_CD','NUM_STATE_CD_PC')


genmst_pincode1.createOrReplaceTempView('genmst_pincode_view')

query = """select *,
cast(cast(NUM_PINCODE as decimal) as string) NUM_PINCODE_B
from genmst_pincode_view"""

genmst_pincode = sqlContext.sql(query)
sqlContext.dropTempTable('genmst_pincode_view')
genmst_pincode = genmst_pincode\
                        .drop('NUM_PINCODE')

genmst_pincode = genmst_pincode\
                        .withColumnRenamed('NUM_PINCODE_B','NUM_PINCODE')

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

## cnfgtr_user_dtls (cnfgtr_user_dtls)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_cnfgtr_user_dtls",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
cnfgtr_user_dtls = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('CODE','USERID','USERNAME')

## gc_clmmst_currency_vw_travel (gc_clmmst_currency_vw_travel)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clmmst_currency_vw_travel",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_currency_vw_travel = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('ITEM_VALUE','ITEM_TEXT')

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
.select('COUNTRYCODE','COUNTRYNAME')

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

## gc_clmmst_paymenttype_e (gc_clmmst_paymenttype_e)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "claim_gc_gc_clmmst_paymenttype_e",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
gc_clmmst_paymenttype_e = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('ITEM_VALUE','ITEM_TEXT')

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
.select('NUM_RESOURCE_CD','UPDATENO','NUM_RESOURCE_TYPE','TXT_LIECENCE_NO','TXT_PAN_NO','TXT_GST_NUMBER','NUM_CITYDISTRICT_CD')\
.withColumnRenamed('TXT_LIECENCE_NO', 'SURVEYOR_LICENSE_NUMBER')\
.withColumnRenamed('TXT_PAN_NO', 'PAN_NO')\
.withColumnRenamed('TXT_GST_NUMBER', 'INTERMEDIARY_GSTN_NO')

#####   INNER JOIN WITH ALL TABLES POSSIBLE #####

gc_clm_gen_info1 = gc_clm_gen_info_base
gc_clm_gen_info_extra1 = gc_clm_gen_info_extra_base
gc_clm_transaction_history1 = gc_clm_transaction_history_base\
                                .withColumn('DAT_TRANSACTIONDATE', to_date(gc_clm_transaction_history_base.DAT_TRANSACTIONDATE, format='yyyy-MM-dd'))
gc_clm_transaction_history1.persist()
# gc_clm_transaction_history1.count()
gc_clm_payment_details1 = gc_clm_payment_details_base
gc_clm_surveyor1 = gc_clm_surveyor_base
gc_clm_claim_diary1 = gc_clm_claim_diary_base
gc_clm_tp_petition_dtls1 = gc_clm_tp_petition_dtls_base
gc_clm_reopen1 = gc_clm_reopen_base
gc_clm_tp_award_dtls1 = gc_clm_tp_award_dtls_base
gc_clm_gen_settlement_info1 = gc_clm_gen_settlement_info_base
## join with uw_proposal_addl_info
uw_proposal_addl_info1 = uw_proposal_addl_info_base

## join with distribution_channel_tab
distribution_channel_tab1 = distribution_channel_tab_base

## join with gen_prop_information_tab
gen_prop_information_tab1 = gen_prop_information_tab_base

## join with cnfgtr_policy_ld_dtls_base
cnfgtr_policy_ld_dtls = cnfgtr_policy_ld_dtls_base
# acc_voucher1 = acc_voucher_base

# #####   INNER JOIN WITH ALL TABLES POSSIBLE #####

# gc_clm_gen_info1 = gc_clm_gen_info_base.join(clm_ncn,'NUM_CLAIM_NO','inner')
# gc_clm_gen_info_extra1 = gc_clm_gen_info_extra_base.join(clm_ncn,'NUM_CLAIM_NO','inner')
# gc_clm_transaction_history1 = gc_clm_transaction_history_base.join(clm_ncn,'NUM_CLAIM_NO','inner')\
#                                 .withColumn('DAT_TRANSACTIONDATE', to_date(gc_clm_transaction_history_base.DAT_TRANSACTIONDATE, format='yyyy-MM-dd'))
# gc_clm_transaction_history1.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
# gc_clm_payment_details1 = gc_clm_payment_details_base.join(clm_ncn,'NUM_CLAIM_NO','inner')
# gc_clm_surveyor1 = gc_clm_surveyor_base.join(clm_ncn,'NUM_CLAIM_NO','inner')
# gc_clm_claim_diary1 = gc_clm_claim_diary_base.join(clm_ncn,'NUM_CLAIM_NO','inner')
# gc_clm_tp_petition_dtls1 = gc_clm_tp_petition_dtls_base.join(clm_ncn,'NUM_CLAIM_NO','inner')
# gc_clm_reopen1 = gc_clm_reopen_base.join(clm_ncn,'NUM_CLAIM_NO','inner')
# gc_clm_tp_award_dtls1 = gc_clm_tp_award_dtls_base.join(clm_ncn,'NUM_CLAIM_NO','inner')
# gc_clm_gen_settlement_info1 = gc_clm_gen_settlement_info_base.join(clm_ncn,'NUM_CLAIM_NO','inner')
# ## join with uw_proposal_addl_info
# join_cond = [uw_proposal_addl_info_base.NUM_REFERERANCE_NO == clm_nrn.NUM_REFERENCE_NO]
# uw_proposal_addl_info1 = uw_proposal_addl_info_base\
#                                     .join(clm_nrn, join_cond, "inner")\
#                                     .drop(clm_nrn.NUM_REFERENCE_NO)

# ## join with distribution_channel_tab
# join_cond = [distribution_channel_tab_base.NUM_REFERENCE_NUMBER == clm_nrn.NUM_REFERENCE_NO]
# distribution_channel_tab1 = distribution_channel_tab_base\
#                                     .join(clm_nrn, join_cond, "inner")\
#                                     .drop(clm_nrn.NUM_REFERENCE_NO)

# ## join with gen_prop_information_tab
# join_cond = [gen_prop_information_tab_base.NUM_REFERENCE_NUMBER == clm_nrn.NUM_REFERENCE_NO]
# gen_prop_information_tab1 = gen_prop_information_tab_base\
#                                     .join(clm_nrn, join_cond, "inner")\
#                                     .drop(clm_nrn.NUM_REFERENCE_NO)

# ## join with cnfgtr_policy_ld_dtls_base
# join_cond = [cnfgtr_policy_ld_dtls_base.REFERENCE_NUM == clm_nrn.NUM_REFERENCE_NO]
# cnfgtr_policy_ld_dtls = cnfgtr_policy_ld_dtls_base\
#                             .join(clm_nrn, join_cond, "inner")\
#                             .drop(clm_nrn.NUM_REFERENCE_NO)
# # acc_voucher1 = acc_voucher_base

## gc_clm_payment_details (gc_clm_payment_details)

gc_clm_payment_details = gc_clm_payment_details1\
                                .withColumnRenamed('APPROVEDUSERID', 'APPROVER_ID_PO')\
                                .withColumnRenamed('CUR_AMOUNT', 'NETT_CHEQUE_AMOUNT')\
                                .withColumnRenamed('CUR_PROFFESIONAL_FEE', 'FEES')\
                                .withColumnRenamed('CUR_TDS', 'TDS_AMOUNT')\
                                .withColumnRenamed('DAT_POSTING_DATE', 'PAYMENT_POSTING_DATE')\
                                .withColumnRenamed('NUM_CGST_AMOUNT', 'CGST_AMOUNT')\
                                .withColumnRenamed('NUM_IGST_AMOUNT', 'IGST_AMOUNT')\
                                .withColumnRenamed('NUM_SGST_UGST_AMOUNT', 'SGST_AMOUNT')\
                                .withColumnRenamed('TXT_BENEFICIARY_ACCOUNT_NO', 'BANK_ACCT_NO')\
                                .withColumnRenamed('TXT_BENEFICIARY_BANK_CD', 'PAYEE_BANK_CODE')\
                                .withColumnRenamed('TXT_BENEFICIARY_BANK_NAME', 'BANK_NAME')\
                                .withColumnRenamed('TXT_IFSC_CODE', 'IFSC_CODE')\
                                .withColumnRenamed('TXT_MULTIPLE_STATUS', 'MULTIPLE_PAY_IND')\
                                .withColumnRenamed('TXT_STATUS', 'PAYMENT_STATUS')\
                                .withColumnRenamed('TXT_PAYEE_CUSTOMER_VENDOR_NAME', 'PAYEE_NAME')\
                                .withColumnRenamed('CUR_EXPENSE_AMOUNT', 'EXPENSES')\
                                .withColumnRenamed('NUM_UPDATE_NO','NUM_PAYMENT_UPDATE_NO')\
                                .withColumn('PAY_FORM_TEXT', gc_clm_payment_details1.TXT_PYMNT_FORM)\
                                .withColumn('MANUAL_CHEQUE', gc_clm_payment_details1.TXT_INSTRUMENT_NO)\
                                .drop('TXT_PYMNT_FORM')\
                                .drop('TXT_INSTRUMENT_NO')

gc_clm_payment_details = gc_clm_payment_details\
                            .withColumn('PAYMENT_POSTING_DATE', to_date(gc_clm_payment_details.PAYMENT_POSTING_DATE, format='yyyy-MM-dd'))

## genmst_customer (genmst_customer)

genmst_customer = genmst_customer_base\
                        .groupBy('CUSTOMER_CODE')\
                        .agg(sf.max('YN_VIP').alias('PRIORITY_CLIENT_ID'))

## genmst_location (genmst_location)
genmst_location_base = genmst_location_base\
                                .groupBy('NUM_LOCATION_CD')\
                                .agg(sf.max('TXT_ADDRESS_LINE_1').alias('TXT_ADDRESS_LINE_1'),\
                                    sf.max('TXT_ADDRESS_LINE_2').alias('TXT_ADDRESS_LINE_2'),\
                                    sf.max('TXT_ADDRESS_LINE_3').alias('TXT_ADDRESS_LINE_3'),\
                                    sf.max('NUM_STATE_CD').alias('NUM_STATE_CD'),\
                                    sf.max('NUM_CITYDISTRICT_CD').alias('NUM_CITYDISTRICT_CD'),\
                                    sf.max('TXT_STATE').alias('TXT_STATE'))

genmst_location = genmst_location_base\
                        .withColumn('LOSS_LOC_TXT', concat(coalesce(genmst_location_base.TXT_ADDRESS_LINE_1, lit('')),\
                                                           coalesce(genmst_location_base.TXT_ADDRESS_LINE_2, lit('')),\
                                                           coalesce(genmst_location_base.TXT_ADDRESS_LINE_3, lit(''))))

## gc_clm_surveyor (gc_clm_surveyor)

gc_clm_surveyor = gc_clm_surveyor1\
                    .withColumn('DAT_DATE_OF_APPOINTMENT', to_date(gc_clm_surveyor1.DAT_DATE_OF_APPOINTMENT, format='yyyy-MM-dd'))

# gc_clm_surveyor = gc_clm_surveyor\
#                         .withColumn('DAT_DATE_OF_APPOINTMENT', to_date(gc_clm_surveyor.DAT_DATE_OF_APPOINTMENT, format='yyyy-MM-dd'))

# 20191130 reading SURVEYOR_LICENSE_EXPIRY_DATE from gc_clm_surveyor instead of surveyor master- sagar
gc_clm_surveyor = gc_clm_surveyor\
                        .withColumnRenamed('DAT_DATE_OF_APPOINTMENT', 'SURVEYOR_APPOINTMENT_DATE')\
                        .withColumnRenamed('DAT_REPORT_SUBMISSION_DATE', 'SURVEYOR_REPORT_RECEIVED_DATE')\
                        .withColumnRenamed('NUM_INTERMEDIARY_CD', 'SURVEYORCODE')\
                        .withColumnRenamed('TXT_INTERMEDIARY_NAME', 'SURVEYOR_NAME')\
                        .withColumnRenamed('DAT_DATE_OF_LICENSE_EXPIRY', 'SURVEYOR_LICENSE_EXPIRY_DATE')\

gc_clm_surveyor = gc_clm_surveyor\
                    .withColumn('SURVEYOR_REPORT_RECEIVED_DATE', to_date(gc_clm_surveyor.SURVEYOR_REPORT_RECEIVED_DATE, format='yyyy-MM-dd'))

##########################   Part 1 Ends here ########################

## acc_voucher (acc_voucher)

# acc_voucher = acc_voucher1\
#                     .withColumnRenamed('TXT_PAYEE_ADDRESS', 'PAYEE_ADDRESS')


## uw_proposal_addl_info (uw_proposal_addl_info)

uw_proposal_addl_info = uw_proposal_addl_info1\
                                .withColumnRenamed('TXT_EFF_DT_SEQ_NO', 'EFF_DT_SEQ_NO')\
                                .withColumnRenamed('TXT_RENL_CERT_NO', 'RENL_CERT_NO')


## distribution_channel_tab (distribution_channel_tab)

distribution_channel_tab = distribution_channel_tab1\
                                .withColumnRenamed('TXT_INTERMEDIARY_CODE', 'PRODUCER_CD')\
                                .withColumnRenamed('TXT_INTERMEDIARY_NAME', 'PRODUCER_NAME')\
                                .withColumnRenamed('TXT_TYPE_OF_ENDORSEMENT', 'CANCELLATION_REASON')

## gen_prop_information_tab (gen_prop_information_tab)

# gen_prop_information_tab = gen_prop_information_tab1\
#                                 .withColumn('NUM_REFERENCE_NUMBER',\
#                                             col('NUM_REFERENCE_NUMBER').cast(DecimalType(15,0)).cast(StringType()))
gen_prop_information_tab = gen_prop_information_tab1

gen_prop_information_tab = gen_prop_information_tab\
                            .withColumn('DAT_ENDORSEMENT_EFF_DATE',\
                                        to_date(gen_prop_information_tab.DAT_ENDORSEMENT_EFF_DATE, format='yyyy-MM-dd'))\
                            .withColumn('DAT_POLICY_EFF_FROMDATE',\
                                        to_date(gen_prop_information_tab.DAT_POLICY_EFF_FROMDATE, format='yyyy-MM-dd'))\
                            .withColumn('DAT_POLICY_EFF_TODATE',\
                                        to_date(gen_prop_information_tab.DAT_POLICY_EFF_TODATE, format='yyyy-MM-dd'))\
                            .withColumn('DAT_DBLINS_POLICY_ISSUEDATE',\
                                        to_date(gen_prop_information_tab.DAT_DBLINS_POLICY_ISSUEDATE, format='yyyy-MM-dd'))

gen_prop_information_tab = gen_prop_information_tab\
                                .withColumnRenamed('DAT_POLICY_EFF_TODATE', 'POL_EXP_DATE')\
                                .withColumnRenamed('TXT_INTERNAL_CERTIFICATE_NO', 'CERTIFICATE_NUMBER')\
                                .withColumnRenamed('TXT_CUSTOMER_ID', 'CUSTOMER_ID')\
                                .withColumnRenamed('TXT_POLICY_NO_CHAR', 'POLICY_NO_GP')\
                                .withColumn('EFFECTIVE_DATE_OF_CANCELLATION',\
                                            gen_prop_information_tab.DAT_ENDORSEMENT_EFF_DATE)

gen_prop_information_tab = gen_prop_information_tab\
                                .withColumn('POLICY_STATUS', \
                                     when ((((trim(lower(gen_prop_information_tab.TXT_BUSINESS_TYPE))) == 's3 roll over')|
                                           ((trim(lower(gen_prop_information_tab.TXT_BUSINESS_TYPE))) == 's3 rollover')|
                                          ((trim(lower(gen_prop_information_tab.TXT_BUSINESS_TYPE))) == 's3 roll over new')|
                                          ((trim(lower(gen_prop_information_tab.TXT_BUSINESS_TYPE))) == 's3 rollover new')|
                                           ((trim(lower(gen_prop_information_tab.TXT_BUSINESS_TYPE))) == 'roll over')), lit('Roll Over'))\
                                            .when((((trim(lower(gen_prop_information_tab.TXT_BUSINESS_TYPE))) == 'new business')|
                                                 ((trim(lower(gen_prop_information_tab.TXT_BUSINESS_TYPE))) == 'used vehicle')),lit('New Business'))\
                                            .when(((trim(lower(gen_prop_information_tab.TXT_BUSINESS_TYPE))) == 'renewal business'),lit('Renewal Business'))\
                                           .otherwise(lit('')))

## gc_clm_tp_petition_dtls (gc_clm_tp_petition_dtls)
#satya20190621 for CASE_YEAR
gc_clm_tp_petition_dtls = gc_clm_tp_petition_dtls1\
                                .withColumnRenamed('TXT_VENUE_CODE','LITIGATION_TYPE_CD')\
                                .withColumnRenamed('TXT_CASE_NO','CASE_NUMBER')\
                                .withColumnRenamed('NUM_COURT_CODE','NUM_COURT_CODE')\
                                .withColumnRenamed('NUM_CASE_YEAR','CASE_YEAR')\
                                .withColumn('CASE_YEAR',col('CASE_YEAR').cast(IntegerType()))\
                                .withColumn('DATE_OF_FILING_OF_PETITION',\
                                            to_date(gc_clm_tp_petition_dtls1.DAT_DATE_OF_FILING_OF_PETITION, format='yyyy-MM-dd'))\
                                .withColumn('DATE_OF_HEARING',\
                                            to_date(gc_clm_tp_petition_dtls1.DAT_DATE_OF_HEARING, format='yyyy-MM-dd'))

## gc_clm_reopen (gc_clm_reopen)
#below logic written in single step by satya - optimization 20200504
gc_clm_reopen = gc_clm_reopen1\
                        .withColumn('NUM_REASON_FOR_REOPENING',(gc_clm_reopen1.NUM_REASON_FOR_REOPENING.cast('integer')).cast('string'))


## gc_clmmst_surveytype (gc_clmmst_surveytype)

gc_clmmst_surveytype2.createOrReplaceTempView('gc_clmmst_surveytype2_view')

query = """select *,
cast(cast(NUM_PRODUCT_CODE as decimal) as string) NUM_PRODUCT_CODE_B
from gc_clmmst_surveytype2_view"""
gc_clmmst_surveytype = sqlContext.sql(query)
sqlContext.dropTempTable('gc_clmmst_surveytype2_view')
gc_clmmst_surveytype = gc_clmmst_surveytype\
                            .drop('NUM_PRODUCT_CODE')

gc_clmmst_surveytype = gc_clmmst_surveytype\
                            .withColumnRenamed('NUM_PRODUCT_CODE_B','NUM_PRODUCT_CODE')


## country_master (country_master)

country_master.createOrReplaceTempView('country_master_view')

query = """select *,
cast(cast(COUNTRYCODE as decimal) as string) COUNTRYCODE_B
from country_master_view"""

country_master1 = sqlContext.sql(query)
sqlContext.dropTempTable('country_master_view')

country_master1 = country_master1\
                            .drop('COUNTRYCODE')

country_master1 = country_master1\
                            .withColumnRenamed('COUNTRYCODE_B','COUNTRYCODE')

## genmst_tab_office (genmst_tab_office)

genmst_tab_office.createOrReplaceTempView('genmst_tab_office_view')

query = """select *,
cast(cast(NUM_PARENT_OFFICE_CD as decimal) as string) NUM_PARENT_OFFICE_CD_B
from genmst_tab_office_view"""

genmst_tab_office = sqlContext.sql(query)
sqlContext.dropTempTable('genmst_tab_office_view')

genmst_tab_office = genmst_tab_office\
                            .drop('NUM_PARENT_OFFICE_CD')

genmst_tab_office = genmst_tab_office\
                            .withColumnRenamed('NUM_PARENT_OFFICE_CD_B','NUM_PARENT_OFFICE_CD')

####   gc_clm_gen_info
gc_clm_gen_info = gc_clm_gen_info1\
                        .withColumnRenamed('DAT_TRANS_DATE', 'CLM_CREATE_DATE')\
                        .withColumnRenamed('DAT_LOSS_DATE', 'LOSS_DATE')\
                        .withColumnRenamed('DAT_NOTIFICATION_DATE', 'LOSS_REPORTED_DATE')\
                        .withColumnRenamed('TXT_NAME_OF_INSURED', 'INSURED_NAME')\
                        .withColumnRenamed('TXT_REMARKS', 'MOTHER_BRANCH')\
                        .withColumnRenamed('TXT_SERVICING_OFFICE_CD', 'SETTLING_OFF_CD')\
                        .withColumnRenamed('YN_CLOSE_PROXIMITY', 'CLAIMSPROXIMITY')\
                        .withColumnRenamed('NUM_NATURE_OF_LOSS', 'COVERAGE_CODE')\
                        .withColumnRenamed('NUM_CATASTROPHE_CD', 'NUM_CATASTROPHE_CD_TMP')
#                         .withColumnRenamed('TXT_POLICY_NO_CHAR', 'POLICY_NO')\
gc_clm_gen_info = gc_clm_gen_info\
                        .withColumn('LOSS_DATE', to_date(gc_clm_gen_info.LOSS_DATE, format='yyyy-MM-dd'))\
                        .withColumn('LOSS_REPORTED_DATE', to_date(gc_clm_gen_info.LOSS_REPORTED_DATE, format='yyyy-MM-dd'))\
                        .withColumn('CLM_CREATE_DATE', to_date(gc_clm_gen_info.CLM_CREATE_DATE, format='yyyy-MM-dd'))\
                        .withColumn('NUM_CATASTROPHE_CD', \
                                    when(((gc_clm_gen_info.NUM_CATASTROPHE_CD_TMP.isNull())|(trim(gc_clm_gen_info.NUM_CATASTROPHE_CD_TMP)=='0')), lit(''))
                                .otherwise(gc_clm_gen_info.NUM_CATASTROPHE_CD_TMP))\
                        .drop('NUM_CATASTROPHE_CD_TMP')

### gc_clm_gen_info_extra
gc_clm_gen_info_extra = gc_clm_gen_info_extra1\
                                .withColumnRenamed('TXT_INFO10', 'CUSTOMER_CLAIM_NUMBER')\
                                .withColumnRenamed('TXT_INFO12', 'CAUSE_LOSS_CD')\
                                .withColumnRenamed('TXT_INFO14', 'ANATOMY_CD')\
                                .withColumnRenamed('TXT_INFO16', 'INJURY_CD')\
                                .withColumnRenamed('TXT_INFO17', 'EXAMINER_NAME')\
                                .withColumnRenamed('TXT_INFO18', 'EXAMINER_USER_ID_CD')\
                                .withColumnRenamed('TXT_INFO19', 'DATEDIFFERENCE')\
                                .withColumnRenamed('TXT_INFO29', 'ISSUING_OFFICE_CD')\
                                .withColumnRenamed('TXT_INFO35', 'MARKET_SECMENTATION_CD')\
                                .withColumnRenamed('TXT_INFO36', 'ISSUING_COMAPNY_CD')\
                                .withColumnRenamed('TXT_INFO4', 'CLAIM_STATUS_CD')\
                                .withColumnRenamed('TXT_INFO6', 'TYPE_OF_CLAIM')\
                                .withColumnRenamed('TXT_INFO72', 'HOSPITAL_NAME')\
                                .withColumnRenamed('TXT_INFO76', 'ADDRESS_1')\
                                .withColumnRenamed('TXT_INFO31', 'GEN_TXT_INFO31')\
                                .withColumnRenamed('TXT_INFO37', 'GEN_TXT_INFO37')\
                                .withColumnRenamed('TXT_INFO39', 'GEN_TXT_INFO39')\
                                .withColumnRenamed('TXT_INFO43', 'GEN_TXT_INFO43')\
                                .withColumnRenamed('TXT_INFO44', 'GEN_TXT_INFO44')\
                                .withColumnRenamed('TXT_INFO45', 'GEN_TXT_INFO45')\
                                .withColumnRenamed('TXT_INFO46', 'GEN_TXT_INFO46')\
                                .withColumnRenamed('TXT_INFO49', 'GEN_TXT_INFO49')\
                                .withColumnRenamed('TXT_INFO50', 'GEN_TXT_INFO50')\
                                .withColumnRenamed('TXT_INFO51', 'GEN_TXT_INFO51')\
                                .withColumnRenamed('TXT_INFO52', 'GEN_TXT_INFO52')\
                                .withColumnRenamed('TXT_INFO53', 'GEN_TXT_INFO53')\
                                .withColumnRenamed('TXT_INFO55', 'GEN_TXT_INFO55')\
                                .withColumnRenamed('TXT_INFO80', 'GEN_TXT_INFO80')

# gc_clm_gen_info_extra.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
genmst_pincode = genmst_pincode\
                            .withColumn('DAT_INSERT_DATE', to_date(genmst_pincode.DAT_INSERT_DATE, format='yyyy-MM-dd'))

## Moved to metric
# ####   CLAIM_NO PATCH
# ## join with gc_clm_gen_info

# gc_clm_temp01 = gc_clm_gen_info\
#                     .filter(gc_clm_gen_info.NUM_UPDATE_NO == 0)\
#                     .drop('NUM_UPDATE_NO')

# gc_clm_temp1 = gc_clm_temp01\
#                     .drop('NUM_UPDATE_NO')

# gc_clm_temp = gc_clm_temp1\
#                     .select('NUM_CLAIM_NO','TXT_MASTER_CLAIM_NO_NEW')

# join_cond = [clm_00_1.NUM_CLAIM_NO == gc_clm_temp.NUM_CLAIM_NO]

# clm_00_1_01 = clm_00_1\
#                 .join(gc_clm_temp, join_cond, "left_outer")\
#                 .drop(gc_clm_temp.NUM_CLAIM_NO)

# clm_00_1_001 = clm_00_1_01\
#                 .withColumn('INDEMNITY_STATUS_AT_FEATURE_LEVEL',\
#                                    when((trim(clm_00_1_01.INDEMNITY_STATUS_AT_FEATURE_LEVEL).isNull()),lit(''))\
#                                        .otherwise(clm_00_1_01.INDEMNITY_STATUS_AT_FEATURE_LEVEL))\
#                 .withColumn('EXPENSES_STATUS_AT_FEATURE_LEVEL',\
#                                    when((trim(clm_00_1_01.EXPENSES_STATUS_AT_FEATURE_LEVEL).isNull()),lit(''))\
#                                        .otherwise(clm_00_1_01.EXPENSES_STATUS_AT_FEATURE_LEVEL))
# #                 .withColumn('CLAIM_NO', firstTenChar(clm_00_1_01.TXT_MASTER_CLAIM_NO_NEW))\
# #                 .withColumn('CLAIM_FEATURE_NO', afterTenChar(clm_00_1_01.TXT_MASTER_CLAIM_NO_NEW))\
# #                 .withColumn('CLAIM_FEATURE_CONCAT', clm_00_1_01.TXT_MASTER_CLAIM_NO_NEW)\
# clm_00_1_02 = clm_00_1_001\
#                     .drop('TXT_MASTER_CLAIM_NO_NEW')

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


join_cond = [clm_00_1_02.NUM_CLAIM_NO == gc_clm_gen_info2.NUM_CLAIM_NO]

clm_00_2_1 = clm_00_1_02\
            .join(gc_clm_gen_info2, join_cond, "left_outer")\
            .drop(gc_clm_gen_info2.NUM_CLAIM_NO)\
            .drop('NUM_CATASTROPHE_CD')

## join with gc_clm_gen_info for NUM_CATASTROPHE_CD 20200108
gc_cat_01 = gc_clm_gen_info['NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_CATASTROPHE_CD']
gen_cat_02 = gc_cat_01.filter(~((gc_cat_01.NUM_CATASTROPHE_CD.isNull())|(gc_cat_01.NUM_CATASTROPHE_CD=='')))
gen_cat_03 = gen_cat_02\
                .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('NUM_UPDATE_NO').alias('NUM_UPDATE_NO'))
gen_cat_04 = gen_cat_03.join(gen_cat_02,['NUM_CLAIM_NO','NUM_UPDATE_NO'],'inner')

gen_cat_05 = gen_cat_04\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('NUM_CATASTROPHE_CD').alias('NUM_CATASTROPHE_CD'))

cat_all = clm_00_2_1.join(gen_cat_05,'NUM_CLAIM_NO','left_outer')
clm_00_2 = cat_all

# clm_00_2_1 = clm_00_2_1\
#                 .withColumn('policy_length', LengthOfString(clm_00_2_1.POLICY_NO))

# clm_00_2 = clm_00_2_1\
#                 .withColumn('POLICY_NO', when(clm_00_2_1.policy_length > 10, firstTenChar(clm_00_2_1.POLICY_NO))\
#                                         .otherwise(clm_00_2_1.POLICY_NO))

## join with gc_clm_gen_info_extra

gc_clm_gen_info_extra1 = gc_clm_gen_info_extra\
                        .groupBy('NUM_CLAIM_NO')\
                        .agg(sf.max('NUM_UPDATE_NO').alias('check'))

join_cond = [gc_clm_gen_info_extra.NUM_CLAIM_NO == gc_clm_gen_info_extra1.NUM_CLAIM_NO,\
             gc_clm_gen_info_extra.NUM_UPDATE_NO == gc_clm_gen_info_extra1.check]

gc_clm_gen_info_extra2 = gc_clm_gen_info_extra\
                                .join(gc_clm_gen_info_extra1, join_cond, "inner")\
                                .drop(gc_clm_gen_info_extra1.NUM_CLAIM_NO)\
                                .drop(gc_clm_gen_info_extra1.check)\
                                .drop(gc_clm_gen_info_extra.NUM_UPDATE_NO)

# join_cond = [clm_00_2.NUM_CLAIM_NO == gc_clm_gen_info_extra2.NUM_CLAIM_NO]

clm_00_3 = clm_00_2.join(gc_clm_gen_info_extra2, 'NUM_CLAIM_NO', "left_outer")

# ## join with gc_clm_gen_info_extra for Anat_Prop 20200110
# gen_info_extra_anat_01 = gc_clm_gen_info_extra['NUM_CLAIM_NO','NUM_UPDATE_NO','ANAT_PROP']
# gen_info_extra_anat_02 = gen_info_extra_anat_01.filter(~((gen_info_extra_anat_01.ANAT_PROP.isNull())|(gen_info_extra_anat_01.ANAT_PROP=='')|(lower(gen_info_extra_anat_01.ANAT_PROP)=='select...')))
# gen_info_extra_anat_03 = gen_info_extra_anat_02\
#                 .groupBy('NUM_CLAIM_NO')\
#                         .agg(sf.max('NUM_UPDATE_NO').alias('NUM_UPDATE_NO'))
# gen_info_extra_anat_04 = gen_info_extra_anat_03.join(gen_info_extra_anat_02,['NUM_CLAIM_NO','NUM_UPDATE_NO'],'inner')

# gen_info_extra_anat_05 = gen_info_extra_anat_04\
#                     .groupBy('NUM_CLAIM_NO')\
#                     .agg(sf.max('ANAT_PROP').alias('ANAT_PROP'))

# anat_all = clm_00_3.join(gen_info_extra_anat_05,'NUM_CLAIM_NO','left_outer')

# ## Modification of Anat_Prop ended 20200110

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

# cat_type_all = clm_00_3.join(gen_info_extra_cat_type_05,'NUM_CLAIM_NO','left_outer')
# clm_00_3 = cat_type_all
# ## Modification of CATASTROPHE_TYPE ended 20200110

# ## join with gc_clm_gen_info_extra for INJURY_DAMAGE/INGURY_DAMAGE 20200110
# gen_info_extra_injury_01 = gc_clm_gen_info_extra['NUM_CLAIM_NO','NUM_UPDATE_NO','INGURY_DAMAGE']
# gen_info_extra_injury_02 = gen_info_extra_injury_01.filter(~((gen_info_extra_injury_01.INGURY_DAMAGE.isNull())|(gen_info_extra_injury_01.INGURY_DAMAGE=='')|(lower(gen_info_extra_injury_01.INGURY_DAMAGE)=='select...')))
# gen_info_extra_injury_03 = gen_info_extra_injury_02\
#                 .groupBy('NUM_CLAIM_NO')\
#                         .agg(sf.max('NUM_UPDATE_NO').alias('NUM_UPDATE_NO'))
# gen_info_extra_injury_04 = gen_info_extra_injury_03.join(gen_info_extra_injury_02,['NUM_CLAIM_NO','NUM_UPDATE_NO'],'inner')

# gen_info_extra_injury_05 = gen_info_extra_injury_04\
#                     .groupBy('NUM_CLAIM_NO')\
#                     .agg(sf.max('INGURY_DAMAGE').alias('INJURY_DAMAGE'))

# injury_all = cat_type_all.join(gen_info_extra_injury_05,'NUM_CLAIM_NO','left_outer')
# ## Modification of INJURY_DAMAGE/INGURY_DAMAGE ended 20200110

# ## join with gc_clm_gen_info_extra for CAUSE_OF_LOSS 20200110
# gen_info_extra_cause_01 = gc_clm_gen_info_extra['NUM_CLAIM_NO','NUM_UPDATE_NO','CAUSE_OF_LOSS']
# gen_info_extra_cause_02 = gen_info_extra_cause_01.filter(~((gen_info_extra_cause_01.CAUSE_OF_LOSS.isNull())|(gen_info_extra_cause_01.CAUSE_OF_LOSS=='')|(lower(gen_info_extra_cause_01.CAUSE_OF_LOSS)=='select...')))
# gen_info_extra_cause_03 = gen_info_extra_cause_02\
#                 .groupBy('NUM_CLAIM_NO')\
#                         .agg(sf.max('NUM_UPDATE_NO').alias('NUM_UPDATE_NO'))
# gen_info_extra_cause_04 = gen_info_extra_cause_03.join(gen_info_extra_cause_02,['NUM_CLAIM_NO','NUM_UPDATE_NO'],'inner')

# gen_info_extra_cause_05 = gen_info_extra_cause_04\
#                     .groupBy('NUM_CLAIM_NO')\
#                     .agg(sf.max('CAUSE_OF_LOSS').alias('CAUSE_OF_LOSS'))

# cause_all = injury_all.join(gen_info_extra_cause_05,'NUM_CLAIM_NO','left_outer')
# clm_00_3 = cause_all
# ## Modification of CAUSE_OF_LOSS ended 20200110


clm_00_3_6 = clm_00_3.withColumnRenamed('CLAIM_STATUS_CD', 'CLAIM_STATUS_CD_TMP')

clm_00_3_6_tmp = clm_00_3_6.groupBy('NUM_CLAIM_NO')\
                            .agg(sf.max('INDEMNITY_STATUS_AT_FEATURE_LEVEL').alias('INDEMNITY_STATUS_AT_FEATURE_LEVEL'),\
                                sf.max('EXPENSES_STATUS_AT_FEATURE_LEVEL').alias('EXPENSES_STATUS_AT_FEATURE_LEVEL'))

clm_00_3_6_tmp_01 = clm_00_3_6_tmp.withColumn('CLAIM_STATUS_CD',\
                                  when((lower(clm_00_3_6_tmp.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'open')&\
                                  (lower(clm_00_3_6_tmp.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'open'), 'Open')\
                                   .when((lower(clm_00_3_6_tmp.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'close')&\
                                  (lower(clm_00_3_6_tmp.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'open'), 'Open')\
                                  .when((lower(clm_00_3_6_tmp.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'close')&\
                                  (lower(clm_00_3_6_tmp.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'close'), 'Close')\
                                  .when((lower(clm_00_3_6_tmp.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'open')&\
                                  (lower(clm_00_3_6_tmp.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'close'), 'Open')
                                  .when((lower(clm_00_3_6_tmp.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'open')&\
                                  (trim(clm_00_3_6_tmp.EXPENSES_STATUS_AT_FEATURE_LEVEL)==''), 'Open')
                                  .when((trim(clm_00_3_6_tmp.INDEMNITY_STATUS_AT_FEATURE_LEVEL)=='')&\
                                  (lower(clm_00_3_6_tmp.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'open'), 'Open')
                                  .when((lower(clm_00_3_6_tmp.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'close')&\
                                  (trim(clm_00_3_6_tmp.EXPENSES_STATUS_AT_FEATURE_LEVEL)==''), 'Close')
                                  .when((trim(clm_00_3_6_tmp.INDEMNITY_STATUS_AT_FEATURE_LEVEL)=='')&\
                                  (lower(clm_00_3_6_tmp.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'close'), 'Close'))

clm_00_3_7 = clm_00_3_6.join(clm_00_3_6_tmp_01, ['NUM_CLAIM_NO'],'left_outer')\
                        .drop(clm_00_3_6_tmp_01.INDEMNITY_STATUS_AT_FEATURE_LEVEL)\
                        .drop(clm_00_3_6_tmp_01.EXPENSES_STATUS_AT_FEATURE_LEVEL)

clm_00_3_8= clm_00_3_7.withColumn('CLAIM_STATUS_CD',\
                                  when(lower(trim(clm_00_3_7.CLAIM_STATUS_CD_TMP)) == 'suspended', 'Suspended')\
                                  .when(lower(trim(clm_00_3_7.CLAIM_STATUS_CD_TMP)) == 'cancelled', 'Cancel')\
                                  .when(lower(trim(clm_00_3_7.CLAIM_STATUS_CD_TMP)) == 'on hold', '')\
                                 .otherwise(clm_00_3_7.CLAIM_STATUS_CD))\
                                  .drop('CLAIM_STATUS_CD_TMP')
    
########################################################################################################

# clm_00_3.count()
#678093 (right)
#678385 (wrong)

###  REPORT COLUMN - REOPEN_DATE_REF (for Paid and OSLR)
## As on date filter applied by satya on 20190905

gc_clm_th2 = gc_clm_transaction_history1\
                    .filter((lower(trim(gc_clm_transaction_history1.TXT_TRANSACTIONTYPE)) == 'reopen'))

gc_clm_th_temp1 = gc_clm_th2\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('NUM_SERIAL_NO').alias('check2'))

join_cond = [gc_clm_th2.NUM_CLAIM_NO == gc_clm_th_temp1.NUM_CLAIM_NO,\
             gc_clm_th2.NUM_SERIAL_NO == gc_clm_th_temp1.check2]

gc_clm_th3 = gc_clm_th2\
                .join(gc_clm_th_temp1, join_cond, "inner")\
                .drop(gc_clm_th_temp1.NUM_CLAIM_NO)\
                .drop(gc_clm_th_temp1.check2)\
                .drop(gc_clm_th2.NUM_SERIAL_NO)

gc_clm_th4 = gc_clm_th3\
                .withColumn('REOPEN_DATE_REF', to_date(gc_clm_th3.DAT_TRANSACTIONDATE, format='yyyy-MM-dd'))

gc_clm_th5 = gc_clm_th4\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('REOPEN_DATE_REF').alias('REOPEN_DATE_REF'))

join_cond = [clm_00_3_8.NUM_CLAIM_NO == gc_clm_th5.NUM_CLAIM_NO]

clm_00_3_2 = clm_00_3_8\
            .join(gc_clm_th5, join_cond, "left_outer")\
            .drop(gc_clm_th5.NUM_CLAIM_NO)

###  REPORT COLUMN - CLM_CREATE_DATE_REF (CLM_CREATE_DATE Advice & RR Column)

# gc_clm_th2 = gc_clm_transaction_history1\
#                     .filter(gc_clm_transaction_history1.DAT_TRANSACTIONDATE <= report_date)
gc_clm_th2 = gc_clm_transaction_history1
gc_clm_th2 = gc_clm_th2\
                    .filter(lower(trim(gc_clm_transaction_history1.TXT_TRANSACTIONTYPE)) == 'advice')

#groupby changed-added TXT_ACCOUNTLINE for diff values of Indemnity & Expense-satya
gc_clm_th_temp1 = gc_clm_th2\
                    .groupBy('NUM_CLAIM_NO','TXT_ACCOUNTLINE')\
                    .agg(sf.max('NUM_SERIAL_NO').alias('check2'))

join_cond = [gc_clm_th2.NUM_CLAIM_NO == gc_clm_th_temp1.NUM_CLAIM_NO,\
             gc_clm_th2.NUM_SERIAL_NO == gc_clm_th_temp1.check2]

gc_clm_th3 = gc_clm_th2\
                .join(gc_clm_th_temp1, join_cond, "inner")\
                .drop(gc_clm_th_temp1.NUM_CLAIM_NO)\
                .drop(gc_clm_th_temp1.check2)\
                .drop(gc_clm_th2.NUM_SERIAL_NO)\
                .drop(gc_clm_th2.TXT_ACCOUNTLINE)

gc_clm_th4 = gc_clm_th3\
                .withColumn('CLM_CREATE_DATE_REF', to_date(gc_clm_th3.DAT_TRANSACTIONDATE, format='yyyy-MM-dd'))

gc_clm_th5 = gc_clm_th4\
                    .groupBy('NUM_CLAIM_NO','TXT_ACCOUNTLINE')\
                    .agg(sf.max('CLM_CREATE_DATE_REF').alias('CLM_CREATE_DATE_REF'))
gc_clm_th6 = gc_clm_th5\
            .withColumn('ACCT_CD', when(lower(trim(gc_clm_th5.TXT_ACCOUNTLINE)) == 'indemnity', lit('50'))\
                                        .otherwise(lit('55')))

join_cond = [clm_00_3_2.NUM_CLAIM_NO == gc_clm_th6.NUM_CLAIM_NO,\
             clm_00_3_2.ACCT_LINE_CD == gc_clm_th6.ACCT_CD]

clm_00_3_3 = clm_00_3_2\
            .join(gc_clm_th6, join_cond, "left_outer")\
            .drop(gc_clm_th6.NUM_CLAIM_NO)\
        
gc_clm_th6 = gc_clm_th6.drop(gc_clm_th6.ACCT_CD)

###  REPORT COLUMN - cwp columns for other reports
####  REPORT COLUMN - CWP_DATE_REF CWP_AMOUNT_REF (Reopen report) change done by satya on 20190828
# gc_clm_th2 = gc_clm_transaction_history1\
#                     .filter(gc_clm_transaction_history1.DAT_TRANSACTIONDATE <= report_date)
gc_clm_th2 = gc_clm_transaction_history1
gc_clm_th2.createOrReplaceTempView('gc_clm_transaction_history_view')
query_1="""
SELECT K6.NUM_CLAIM_NO,K6.NUM_SERIAL_NO,K6.TXT_ACCOUNTLINE,
K6.DAT_TRANSACTIONDATE CWP_DATE_REF,K6.NUM_TRANSACTIONAMOUNT CWP_AMOUNT_REF,
K6.TXT_REASON CWP_REASON_REF,K6.TXT_TRANSACTIONTYPE
FROM gc_clm_transaction_history_view K6,
(
SELECT K3.*,
K4.TXT_TRANSACTIONTYPE
FROM gc_clm_transaction_history_view K4,
(
SELECT K2.NUM_CLAIM_NO,MAX(K2.NUM_SERIAL_NO) NUM_SERIAL_NO,K2.TXT_ACCOUNTLINE
FROM gc_clm_transaction_history_view K2,
(
SELECT NUM_CLAIM_NO,MAX(NUM_SERIAL_NO) NUM_SERIAL_NO,MAX(DAT_TRANSACTIONDATE) DAT_TRANSACTIONDATE,TXT_ACCOUNTLINE
FROM gc_clm_transaction_history_view
WHERE lower(trim(TXT_TRANSACTIONTYPE)) = 'reopen'
GROUP BY NUM_CLAIM_NO,TXT_ACCOUNTLINE
)K1
WHERE K1.NUM_CLAIM_NO = K2.NUM_CLAIM_NO
AND lower(trim(K1.TXT_ACCOUNTLINE)) = lower(trim(K2.TXT_ACCOUNTLINE))
AND K2.DAT_TRANSACTIONDATE < K1.DAT_TRANSACTIONDATE
AND K2.NUM_SERIAL_NO < K1.NUM_SERIAL_NO
GROUP BY K2.NUM_CLAIM_NO,K2.TXT_ACCOUNTLINE
)K3
WHERE K3.NUM_CLAIM_NO = K4.NUM_CLAIM_NO
AND K3.NUM_SERIAL_NO = K4.NUM_SERIAL_NO
AND lower(trim(K3.TXT_ACCOUNTLINE)) = lower(trim(K4.TXT_ACCOUNTLINE))
)K5
WHERE lower(trim(K5.TXT_TRANSACTIONTYPE))='mark off reserve'
AND K5.NUM_CLAIM_NO = K6.NUM_CLAIM_NO
AND K5.NUM_SERIAL_NO = K6.NUM_SERIAL_NO
AND lower(trim(K5.TXT_ACCOUNTLINE)) = lower(trim(K6.TXT_ACCOUNTLINE))
"""
reopen_cwp_date = sqlContext.sql(query_1)
sqlContext.dropTempTable('gc_clm_transaction_history_view')

reopen_cwp_date_01 = reopen_cwp_date\
            .withColumn('ACCT_LINE_CD', when(lower(trim(reopen_cwp_date.TXT_ACCOUNTLINE)) == 'indemnity', lit('50'))\
                                        .otherwise(lit('55')))\
                                        .drop('TXT_ACCOUNTLINE')\
                                        .drop('NUM_SERIAL_NO')
                                        
reopen_cwp_date_02 = reopen_cwp_date_01\
                                .groupBy('NUM_CLAIM_NO','ACCT_LINE_CD')\
                                .agg(sf.max('CWP_DATE_REF').alias('CWP_DATE_REF'),\
                                    sf.max('CWP_AMOUNT_REF').alias('CWP_AMOUNT_REF'),\
                                    sf.max('CWP_REASON_REF').alias('CWP_REASON_REF'))
                                    
clm_00_3_5 = clm_00_3_3.join(reopen_cwp_date_02,['NUM_CLAIM_NO','ACCT_LINE_CD'],'left_outer')\
                        .drop(reopen_cwp_date_02.NUM_CLAIM_NO)\
                        .drop(reopen_cwp_date_02.ACCT_LINE_CD)

## REOPEN_DATE/CWP_REOPEN_DATE
#Added on 20190709 by Satya for REOPEN_DATE (CWP)
# gc_clm_th2 = gc_clm_transaction_history1\
#                     .filter(gc_clm_transaction_history1.DAT_TRANSACTIONDATE <= report_date)
gc_clm_th2 = gc_clm_transaction_history1
gc_clm_th2.createOrReplaceTempView('gc_clm_transaction_history_view')
query1="""
select k2.NUM_CLAIM_NO,max(k1.DAT_TRANSACTIONDATE) CWP_REOPEN_DATE from gc_clm_transaction_history_view k1,
(select NUM_CLAIM_NO,DAT_TRANSACTIONDATE from gc_clm_transaction_history_view
where TXT_TRANSACTIONTYPE = 'Mark Off Reserve'
)k2
where k1.NUM_CLAIM_NO=k2.NUM_CLAIM_NO
and k1.TXT_TRANSACTIONTYPE='Reopen'
and k1.DAT_TRANSACTIONDATE < k2.DAT_TRANSACTIONDATE
group by k2.NUM_CLAIM_NO
"""
cwp_rd = sqlContext.sql(query1)
clm_00_3_5 = clm_00_3_5.join(cwp_rd, ['NUM_CLAIM_NO'], 'left')

###  REPORT COLUMN - paid specific column
#change start on 20191210 for RESERVE_RELEASE_PAYMENT

gc_clm_th2_0 = gc_clm_transaction_history1\
                    .filter(lower(trim(gc_clm_transaction_history1.TXT_TRANSACTIONTYPE)) == 'reserves released - final payment')

gc_clm_th2_0 = gc_clm_th2_0\
                    .withColumn('ACCT_LINE_CD', when(lower(trim(gc_clm_th2_0.TXT_ACCOUNTLINE)) == 'indemnity', lit('50'))\
                                .otherwise(lit('55')))
gc_clm_th2_1 = gc_clm_th2_0\
                .withColumn('DAT_TRANSACTIONDATE', to_date(gc_clm_th2_0.DAT_TRANSACTIONDATE, format='yyyy-MM-dd'))

# gc_clm_th2 = gc_clm_th2_1\
#                 .filter(gc_clm_th2_1.DAT_TRANSACTIONDATE <= report_date)
gc_clm_th2 = gc_clm_th2_1

gc_clm_th_temp1 = gc_clm_th2\
                    .groupBy('NUM_CLAIM_NO','ACCT_LINE_CD')\
                    .agg(sf.max('NUM_SERIAL_NO').alias('check2'))

join_cond = [gc_clm_th2.NUM_CLAIM_NO == gc_clm_th_temp1.NUM_CLAIM_NO,\
             gc_clm_th2.ACCT_LINE_CD == gc_clm_th_temp1.ACCT_LINE_CD,\
             gc_clm_th2.NUM_SERIAL_NO == gc_clm_th_temp1.check2]

gc_clm_th3 = gc_clm_th2\
                .join(gc_clm_th_temp1, join_cond, "inner")\
                .drop(gc_clm_th_temp1.NUM_CLAIM_NO)\
                .drop(gc_clm_th_temp1.ACCT_LINE_CD)\
                .drop(gc_clm_th_temp1.check2)\
                .drop(gc_clm_th2.NUM_SERIAL_NO)

gc_clm_th5 = gc_clm_th3\
                    .groupBy('NUM_CLAIM_NO','ACCT_LINE_CD')\
                    .agg(sf.max('NUM_TRANSACTIONAMOUNT').alias('RESERVE_RELEASE_PAYMENT'))

clm_00 = clm_00_3_5.join(gc_clm_th5, ['NUM_CLAIM_NO','ACCT_LINE_CD'], "left_outer")

#change end on 20191210 for RESERVE_RELEASE_PAYMENT

##  TAX DETAILS (metric for paid, rest for all)

# clm_00.count()

## join with gc_clm_payment_details

gc_clm_paym = gc_clm_payment_details\
                    .groupBy('NUM_CLAIM_NO', 'NUM_CLAIM_RECORD_ID')\
                    .agg(sf.max('PAY_FORM_TEXT').alias('PAY_FORM_TEXT'),\
                         sf.sum('NETT_CHEQUE_AMOUNT').alias('NETT_CHEQUE_AMOUNT'),\
                         sf.sum('TDS_AMOUNT').alias('TDS_AMOUNT'),\
                         sf.max('MULTIPLE_PAY_IND').alias('MULTIPLE_PAY_IND'),\
                         sf.sum('SGST_AMOUNT').alias('SGST_AMOUNT'),\
                         sf.sum('CGST_AMOUNT').alias('CGST_AMOUNT'),\
                         sf.sum('IGST_AMOUNT').alias('IGST_AMOUNT'),\
                         sf.max('NUM_TYPE_OF_PARTY').alias('NUM_TYPE_OF_PARTY'),\
                         sf.max('BANK_ACCT_NO').alias('BANK_ACCT_NO'),\
                         sf.max('BANK_NAME').alias('BANK_NAME'),\
                         sf.max('IFSC_CODE').alias('IFSC_CODE'),\
                         sf.max('PAYEE_BANK_CODE').alias('PAYEE_BANK_CODE'),\
                         sf.sum('FEES').alias('FEES'),\
                         sf.max('PAYMENT_STATUS').alias('PAYMENT_STATUS'),\
                         sf.max('PAYEE_NAME').alias('PAYEE_NAME'),\
                         sf.max('EXPENSES').alias('EXPENSES'),\
                         sf.max('PAYMENT_POSTING_DATE').alias('PAYMENT_POSTING_DATE'),\
                         sf.max('NUM_PAYMENT_UPDATE_NO').alias('NUM_PAYMENT_UPDATE_NO'),\
                         sf.max('TXT_PAYEE_CUSTOMER_VENDOR_CD').alias('PAYEE_SURVEYORCODE'))

#old CHEQUE_DATE dropped 20190909
# gc_clm_paym = gc_clm_paym.drop('CHEQUE_DATE')

join_cond = [clm_00.NUM_CLAIM_NO == gc_clm_paym.NUM_CLAIM_NO,\
             clm_00.NUM_TRANSACTION_CONTROL_NO == gc_clm_paym.NUM_CLAIM_RECORD_ID]

clm_02_01 = clm_00.join(gc_clm_paym, join_cond, "left_outer")\
                 .drop(gc_clm_paym.NUM_CLAIM_NO)\
                 .drop(gc_clm_paym.NUM_CLAIM_RECORD_ID)

gc_clm_paym1 = gc_clm_payment_details\
                    .groupBy('NUM_CLAIM_NO', 'MANUAL_CHEQUE')\
                    .agg(sf.max('PAY_FORM_TEXT').alias('PAY_FORM_TEXT_TEMP'),\
                         sf.max('NETT_CHEQUE_AMOUNT').alias('NETT_CHEQUE_AMOUNT_TEMP'),\
                         sf.max('SGST_AMOUNT').alias('SGST_AMOUNT_TEMP'),\
                         sf.max('CGST_AMOUNT').alias('CGST_AMOUNT_TEMP'),\
                         sf.max('IGST_AMOUNT').alias('IGST_AMOUNT_TEMP'),\
                         sf.max('TDS_AMOUNT').alias('TDS_AMOUNT_TEMP'),\
                         sf.max('FEES').alias('FEES_TEMP'),\
                         sf.max('BANK_ACCT_NO').alias('BANK_ACCT_NO_TEMP'),\
                         sf.max('BANK_NAME').alias('BANK_NAME_TEMP'),\
                         sf.max('IFSC_CODE').alias('IFSC_CODE_TEMP'))
gc_clm_paym2 = gc_clm_paym1.withColumnRenamed('MANUAL_CHEQUE','CHEQUE_NO')

clm_02_02 = clm_02_01.join(gc_clm_paym2,['NUM_CLAIM_NO','CHEQUE_NO'],'left_outer')
clm_02_03 = clm_02_02\
                        .withColumn('PAY_FORM_TEXT', \
                                    when(((trim(clm_02_02.PAY_FORM_TEXT).isNull())|\
                                              (trim(clm_02_02.PAY_FORM_TEXT)=='')), clm_02_02.PAY_FORM_TEXT_TEMP)\
                                    .otherwise(clm_02_02.PAY_FORM_TEXT))\
                        .withColumn('NETT_CHEQUE_AMOUNT', \
                                    when(((trim(clm_02_02.NETT_CHEQUE_AMOUNT).isNull())|\
                                              (trim(clm_02_02.NETT_CHEQUE_AMOUNT)=='')), clm_02_02.NETT_CHEQUE_AMOUNT_TEMP)\
                                    .otherwise(clm_02_02.NETT_CHEQUE_AMOUNT))\
                        .withColumn('SGST_AMOUNT', \
                                    when(((trim(clm_02_02.SGST_AMOUNT).isNull())|\
                                              (trim(clm_02_02.SGST_AMOUNT)=='')), clm_02_02.SGST_AMOUNT_TEMP)\
                                    .otherwise(clm_02_02.SGST_AMOUNT))\
                        .withColumn('CGST_AMOUNT', \
                                    when(((trim(clm_02_02.CGST_AMOUNT).isNull())|\
                                              (trim(clm_02_02.CGST_AMOUNT)=='')), clm_02_02.CGST_AMOUNT_TEMP)\
                                    .otherwise(clm_02_02.CGST_AMOUNT))\
                        .withColumn('IGST_AMOUNT', \
                                    when(((trim(clm_02_02.IGST_AMOUNT).isNull())|\
                                              (trim(clm_02_02.IGST_AMOUNT)=='')), clm_02_02.IGST_AMOUNT_TEMP)\
                                    .otherwise(clm_02_02.IGST_AMOUNT))\
                        .withColumn('TDS_AMOUNT', \
                                    when(((trim(clm_02_02.TDS_AMOUNT).isNull())|\
                                              (trim(clm_02_02.TDS_AMOUNT)=='')), clm_02_02.TDS_AMOUNT_TEMP)\
                                    .otherwise(clm_02_02.TDS_AMOUNT))\
                        .withColumn('FEES', \
                                    when(((trim(clm_02_02.FEES).isNull())|\
                                              (trim(clm_02_02.FEES)=='')), clm_02_02.FEES_TEMP)\
                                    .otherwise(clm_02_02.FEES))\
                        .withColumn('BANK_ACCT_NO', \
                                    when(((trim(clm_02_02.BANK_ACCT_NO).isNull())|\
                                              (trim(clm_02_02.BANK_ACCT_NO)=='')), clm_02_02.BANK_ACCT_NO_TEMP)\
                                    .otherwise(clm_02_02.BANK_ACCT_NO))\
                        .withColumn('BANK_NAME', \
                                    when(((trim(clm_02_02.BANK_NAME).isNull())|\
                                              (trim(clm_02_02.BANK_NAME)=='')), clm_02_02.BANK_NAME_TEMP)\
                                    .otherwise(clm_02_02.BANK_NAME))\
                        .withColumn('IFSC_CODE', \
                                    when(((trim(clm_02_02.IFSC_CODE).isNull())|\
                                              (trim(clm_02_02.IFSC_CODE)=='')), clm_02_02.IFSC_CODE_TEMP)\
                                    .otherwise(clm_02_02.IFSC_CODE))\
                        .drop('PAY_FORM_TEXT_TEMP','NETT_CHEQUE_AMOUNT_TEMP','SGST_AMOUNT_TEMP','CGST_AMOUNT_TEMP','IGST_AMOUNT_TEMP','TDS_AMOUNT_TEMP','FEES_TEMP','BANK_ACCT_NO_TEMP','BANK_NAME_TEMP','IFSC_CODE_TEMP')

# clm_02_02 = clm_02_01
# clm_02_03 = clm_02_02
## join for PAY_FORM_CD

# gc_genvalue1 = gc_clmmst_generic_value\
#                         .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 138)

# gc_genvalue1 = gc_genvalue1\
#                     .withColumn('TXT_INFO3', lower(trim(gc_genvalue1.TXT_INFO3)))

# gc_genvalue2 = gc_genvalue1\
#                     .groupBy('TXT_INFO3')\
#                     .agg(sf.max('TXT_INFO1').alias('PAY_FORM_CD'))

# join_cond = [lower(trim(clm_02_01.PAY_FORM_TEXT)) == gc_genvalue2.TXT_INFO3]

# clm_02_1 = clm_02_01\
#             .join(gc_genvalue2, join_cond, "left_outer")\
#             .drop(gc_genvalue2.TXT_INFO3)


##Changes done on join level for PAY_FORM_CD by satya on 20190723
#Taking PAY_FORM_TEXT & PAY_FORM_CD for latest record by satya on 20190916
#clm_02_02 = clm_02_01.drop('PAY_FORM_TEXT')

#gc_clm_paym_tmp_01 = gc_clm_payment_details1\
#                         .withColumn('max_update_serial_no', concat(trim(gc_clm_payment_details.NUM_UPDATE_NO).cast('integer'),\
#                                                           trim(gc_clm_payment_details.NUM_SERIAL_NO).cast('integer')).cast('integer'))
#gc_clm_paym_tmp_02 = gc_clm_paym_tmp_01\
#                    .groupBy('NUM_CLAIM_NO')\
#                    .agg(sf.max('max_update_serial_no').alias('max_update_serial_no'))
#
#gc_clm_paym_tmp_03 = gc_clm_paym_tmp_02.join(gc_clm_paym_tmp_01,['NUM_CLAIM_NO','max_update_serial_no'],'inner')
#
#gc_clm_paym_tmp_04 = gc_clm_paym_tmp_03.groupby('NUM_CLAIM_NO')\
#                                       .agg(sf.max('TXT_PYMNT_FORM').alias('PAY_FORM_TEXT'))
#
#join_cond = [clm_02_02.NUM_CLAIM_NO == gc_clm_paym_tmp_04.NUM_CLAIM_NO]
#
#clm_02_03 = clm_02_02.join(gc_clm_paym_tmp_04, join_cond, "left_outer")\
#                 .drop(gc_clm_paym_tmp_04.NUM_CLAIM_NO)

#Logic added for PAY_FORM_CD 52 (Void)
clm_02_04 = clm_02_03.withColumn('PAY_FORM_TEXT',when((lower(trim(clm_02_03.TRANSACTIONTYPE)) == 'payment listing')|\
                                                     (lower(trim(clm_02_03.TRANSACTIONTYPE)) == 'payment voiding'),lit('Void'))\
                                .otherwise(clm_02_03.PAY_FORM_TEXT))
clm_02_05 = clm_02_04\
            .withColumn('PAY_FORM_CD', when(lower(trim(clm_02_04.PAY_FORM_TEXT)) == 'mc', lit('03'))\
                                      .when(lower(trim(clm_02_04.PAY_FORM_TEXT)) == 'sc', lit('02'))\
                                      .when(lower(trim(clm_02_04.PAY_FORM_TEXT)) == 'neft', lit('06'))\
                                      .when(lower(trim(clm_02_04.PAY_FORM_TEXT)) == 'void', lit('52'))\
                                      .otherwise(lit('')))
######################Logic added/modified for PAYMENT_TYPE######################################
clm_02_06 = clm_02_05\
                .withColumn('PAYMENT_TYPE', when(clm_02_05.TRANS_TYPE_CD == '21', lit('Partial Payment'))\
                                        .when(clm_02_05.TRANS_TYPE_CD == '22', lit('Final Payment'))\
                                        .when(clm_02_05.TRANS_TYPE_CD == '23', lit('Payment After Closing'))\
                                        .otherwise(lit('')))
payment_void = clm_02_05\
                    .filter(((clm_02_05.PAY_FORM_CD=='52')&(~((clm_02_05.CHEQUE_NO.isNull())|(trim(clm_02_05.CHEQUE_NO)=='')))))\
                    .select('NUM_CLAIM_NO','ACCT_LINE_CD','CHEQUE_NO').distinct()
payment_void_01 = payment_void\
                    .groupBy('NUM_CLAIM_NO', 'ACCT_LINE_CD')\
                    .agg(sf.max('CHEQUE_NO').alias('CHEQUE_NO'))

# history_01 = gc_clm_transaction_history1\
#                     .filter((gc_clm_transaction_history1.DAT_TRANSACTIONDATE <= report_date)&\
#                             (~((lower(trim(gc_clm_transaction_history1.TXT_TRANSACTIONTYPE)) == 'payment listing')|\
#                              (lower(trim(gc_clm_transaction_history1.TXT_TRANSACTIONTYPE)) == 'payment voiding'))))\
#                     .select('NUM_CLAIM_NO','TXT_ACCOUNTLINE','TXT_TRANSACTIONTYPE','TXT_CHEQUENO')
history_01 = gc_clm_transaction_history1\
                    .filter((~((lower(trim(gc_clm_transaction_history1.TXT_TRANSACTIONTYPE)) == 'payment listing')|\
                               (lower(trim(gc_clm_transaction_history1.TXT_TRANSACTIONTYPE)) == 'payment voiding'))))\
                    .select('NUM_CLAIM_NO','TXT_ACCOUNTLINE','TXT_TRANSACTIONTYPE','TXT_CHEQUENO')

history_02 = history_01\
                    .withColumn('ACCT_LINE_CD', when(lower(trim(history_01.TXT_ACCOUNTLINE)) == 'indemnity', lit('50'))\
                                .otherwise(lit('55')))\
                    .withColumnRenamed('TXT_CHEQUENO','CHEQUE_NO')\
                    .withColumnRenamed('TXT_TRANSACTIONTYPE','PAYMENT_TYPE_VOID')\
                    .drop('TXT_ACCOUNTLINE')
history_03 = history_02\
                    .groupBy('NUM_CLAIM_NO', 'ACCT_LINE_CD','CHEQUE_NO')\
                    .agg(sf.max('PAYMENT_TYPE_VOID').alias('PAYMENT_TYPE_VOID'))
payment_void_02 = payment_void_01.join(history_03,['NUM_CLAIM_NO','ACCT_LINE_CD','CHEQUE_NO'],'inner')
payment_void_03 = payment_void_02\
                    .withColumn('TRANS_TYPE_CD_VOID', \
                                when((lower(trim(payment_void_02.PAYMENT_TYPE_VOID)) == 'partial payment'), lit('21'))\
                                .when((lower(trim(payment_void_02.PAYMENT_TYPE_VOID)) == 'final payment'), lit('22'))\
                                .when((lower(trim(payment_void_02.PAYMENT_TYPE_VOID)) == 'payment after closing'), lit('23')))

clm_02_07 = clm_02_06.join(payment_void_03,['NUM_CLAIM_NO','ACCT_LINE_CD','CHEQUE_NO'],'left')
clm_02_08 = clm_02_07\
                .withColumn('PAYMENT_TYPE', when(clm_02_07.PAY_FORM_CD == '52', clm_02_07.PAYMENT_TYPE_VOID)\
                                        .otherwise(clm_02_07.PAYMENT_TYPE))\
                .withColumn('TRANS_TYPE_CD', when(clm_02_07.PAY_FORM_CD == '52', clm_02_07.TRANS_TYPE_CD_VOID)\
                                        .otherwise(clm_02_07.TRANS_TYPE_CD))

######################Logic end for PAYMENT_TYPE######################################

# clm_02_08.count()

## join with gc_clmmst_resource_type

join_cond = [clm_02_08.NUM_TYPE_OF_PARTY == gc_clmmst_resource_type.NUM_RESOURCE_TYPE_CD]

clm_02 = clm_02_08\
            .join(gc_clmmst_resource_type, join_cond, "left_outer")\
            .drop(gc_clmmst_resource_type.NUM_RESOURCE_TYPE_CD)

clm_03 = clm_02\
            .withColumnRenamed('TXT_RESOURCE_TYPE','PAYEE_TYPE')

## join with gc_clm_surveyor

#below logic is written in better way by satya - 20200504
clm_surveyor_temp = gc_clm_surveyor\
                        .withColumn('surv_check1', concat(trim(gc_clm_surveyor.NUM_UPDATE_NO).cast('integer'),\
                                                          trim(gc_clm_surveyor.NUM_SERIAL_NO).cast('integer')).cast('integer'))\
                        .drop('NUM_UPDATE_NO')\
                        .drop('NUM_SERIAL_NO')

clm_surveyor_temp1 = clm_surveyor_temp\
                    .groupBy('NUM_CLAIM_NO', 'SURVEYORCODE')\
                    .agg(sf.max('surv_check1').alias('surv_check2'))

join_cond = [clm_surveyor_temp.NUM_CLAIM_NO == clm_surveyor_temp1.NUM_CLAIM_NO,\
             clm_surveyor_temp.SURVEYORCODE == clm_surveyor_temp1.SURVEYORCODE,\
             clm_surveyor_temp.surv_check1 == clm_surveyor_temp1.surv_check2]

clm_surveyor_temp2 = clm_surveyor_temp\
                            .join(clm_surveyor_temp1, join_cond, "inner")\
                            .drop(clm_surveyor_temp1.NUM_CLAIM_NO)\
                            .drop(clm_surveyor_temp1.SURVEYORCODE)\
                            .drop(clm_surveyor_temp.surv_check1)
# .drop(clm_surveyor_temp1.surv_check2)

gc_clmmst_resource1 = gc_clmmst_resource\
                        .groupBy('NUM_RESOURCE_CD')\
                        .agg(sf.max('UPDATENO').alias('check'))

join_cond = [gc_clmmst_resource.NUM_RESOURCE_CD == gc_clmmst_resource1.NUM_RESOURCE_CD,\
             gc_clmmst_resource.UPDATENO == gc_clmmst_resource1.check]

###############################################
# Below code added by sagar on 20190621 for surveyor and investigator related all fields
###############################################
gc_clmmst_resource1A = gc_clmmst_resource\
                        .join(gc_clmmst_resource1, join_cond, "inner")\
                        .drop(gc_clmmst_resource1.NUM_RESOURCE_CD)\
                        .drop(gc_clmmst_resource1.check)\
                        .drop(gc_clmmst_resource.UPDATENO)

#########################################################################################
# below snippet added on 2019-09-19 05:05 PM by sagar to read INTERMEDIARY_GSTN_NO and PAN_NO related to PAYEE
gc_clmmst_resource_payee = gc_clmmst_resource1A[['NUM_RESOURCE_CD', 'PAN_NO', 'INTERMEDIARY_GSTN_NO']]
gc_clmmst_resource_payee_1 = gc_clmmst_resource_payee.withColumnRenamed('PAN_NO', 'PAYEE_PAN_NO')\
                                                     .withColumnRenamed('INTERMEDIARY_GSTN_NO', 'PAYEE_GSTN_NO')\
													 .withColumnRenamed('NUM_RESOURCE_CD', 'PAYEE_SURVEYORCODE')\
													 
gc_clmmst_resource_payee_2 = gc_clmmst_resource_payee_1.groupBy('PAYEE_SURVEYORCODE')\
                                                       .agg(sf.max('PAYEE_PAN_NO').alias('PAYEE_PAN_NO'),\
													        sf.max('PAYEE_GSTN_NO').alias('PAYEE_GSTN_NO'))
#########################################################################################
# below code added by sagar on 20190621
#########################################################################################

gc_clmmst_resource2 = gc_clmmst_resource1A\
                        .filter((gc_clmmst_resource1A.NUM_RESOURCE_TYPE == 6) |\
                                (gc_clmmst_resource1A.NUM_RESOURCE_TYPE == 7) |\
                                (gc_clmmst_resource1A.NUM_RESOURCE_TYPE == 33))	

gc_clmmst_resource2 = gc_clmmst_resource2\
                           .withColumnRenamed('NUM_RESOURCE_CD', 'SURVEYORCODE')

gc_clmmst_resource2 = gc_clmmst_resource2\
                         .withColumn('TYPE_OF_SURVEYOR', when(gc_clmmst_resource2.NUM_RESOURCE_TYPE == 6, lit('EXTERNAL'))\
						                              .otherwise(lit('INTERNAL')))
													  
gc_clmmst_resource2 = gc_clmmst_resource2\
                         .withColumn('SURVEYOR_INVESTIGATOR', when(gc_clmmst_resource2.NUM_RESOURCE_TYPE == 7, lit('INVESTIGATOR'))\
						                              .otherwise(lit('SURVEYOR')))

gc_clmmst_resource2 = gc_clmmst_resource2.withColumn('SURVEYORCODE', \
                                                    trim(gc_clmmst_resource2.SURVEYORCODE))

clm_surveyor_temp2 = clm_surveyor_temp2.withColumn('SURVEYORCODE', \
                                                    trim(clm_surveyor_temp2.SURVEYORCODE))

surveyor_df = clm_surveyor_temp2.join(gc_clmmst_resource2, 'SURVEYORCODE', 'inner')

surveyor_df.createOrReplaceTempView('surveyor_table')

sql_1 = """select NUM_CLAIM_NO, SURVEYORCODE, SURVEYOR_APPOINTMENT_DATE, SURVEYOR_REPORT_RECEIVED_DATE, 
                  INTERMEDIARY_GSTN_NO, PAN_NO, NUM_SURVEY_TYPE, NUM_RESOURCE_TYPE, 
                  SURVEYOR_NAME, TYPE_OF_SURVEYOR, SURVEYOR_LICENSE_EXPIRY_DATE, SURVEYOR_LICENSE_NUMBER, SURVEYOR_INVESTIGATOR
from 
(select NUM_CLAIM_NO, SURVEYORCODE, SURVEYOR_APPOINTMENT_DATE, SURVEYOR_REPORT_RECEIVED_DATE,
                  INTERMEDIARY_GSTN_NO, PAN_NO, NUM_SURVEY_TYPE, NUM_RESOURCE_TYPE, 
                  SURVEYOR_NAME, TYPE_OF_SURVEYOR, SURVEYOR_LICENSE_EXPIRY_DATE, SURVEYOR_LICENSE_NUMBER, SURVEYOR_INVESTIGATOR,
				  row_number() over (partition by NUM_CLAIM_NO, SURVEYOR_INVESTIGATOR order by SURVEYOR_APPOINTMENT_DATE,surv_check2 asc) as ranking
from surveyor_table)
where ranking = 1"""

surveyor_df_1 = sqlContext.sql(sql_1)
sqlContext.dropTempTable('surveyor_table')

surveyor_only = surveyor_df_1.filter(surveyor_df_1.SURVEYOR_INVESTIGATOR == 'SURVEYOR')
investigator = surveyor_df_1.filter(surveyor_df_1.SURVEYOR_INVESTIGATOR == 'INVESTIGATOR')
investigator_1 = investigator[['NUM_CLAIM_NO', 'SURVEYOR_APPOINTMENT_DATE']]
investigator_2 = investigator_1.withColumnRenamed('SURVEYOR_APPOINTMENT_DATE', 'APPOINTMENT_OF_INVESTIGATOR')

clm_03_1 = clm_03.join(surveyor_only, 'NUM_CLAIM_NO', 'left')
clm_03_2 = clm_03_1.join(investigator_2, 'NUM_CLAIM_NO', 'left')

clm_04 = clm_03_2.join(gc_clmmst_resource_payee_2, 'PAYEE_SURVEYORCODE', 'left')

## join with acc_voucher
# acc_voucher_temp = acc_voucher\
#                         .groupBy('NUM_TRANSACTION_CONTROL_NO')\
#                         .agg(sf.max('PAYEE_ADDRESS').alias('PAYEE_ADDRESS'))

# join_cond = [clm_04.NUM_TRANSACTION_CONTROL_NO == acc_voucher_temp.NUM_TRANSACTION_CONTROL_NO]

# clm_06 = clm_04\
#             .join(acc_voucher_temp, join_cond, "left_outer")\
#             .drop(acc_voucher_temp.NUM_TRANSACTION_CONTROL_NO)

clm_06 = clm_04

### join with uw_proposal_addl_info

uw_pai = uw_proposal_addl_info\
            .groupBy('NUM_REFERERANCE_NO')\
            .agg((sf.max('RENL_CERT_NO').alias('RENL_CERT_NO')),\
                 (sf.max('EFF_DT_SEQ_NO').alias('EFF_DT_SEQ_NO')))

join_cond = [clm_06.NUM_REFERENCE_NO == uw_pai.NUM_REFERERANCE_NO]

clm_08 = clm_06\
            .join(uw_pai, join_cond, 'left_outer')\
            .drop(uw_pai.NUM_REFERERANCE_NO)

### join with distribution_channel_tab (PRODUCER_CD)

dct = distribution_channel_tab\
            .groupBy('NUM_REFERENCE_NUMBER')\
            .agg(sf.max('PRODUCER_CD').alias('PRODUCER_CD'),\
                 sf.max('PRODUCER_NAME').alias('PRODUCER_NAME'),\
                 sf.max('CANCELLATION_REASON').alias('CANCELLATION_REASON'))

join_cond = [clm_08.NUM_REFERENCE_NO == dct.NUM_REFERENCE_NUMBER]

clm_10 = clm_08\
            .join(dct, join_cond, 'left_outer')\
            .drop(dct.NUM_REFERENCE_NUMBER)

### join with gen_prop_information_tab

gpit = gen_prop_information_tab\
            .groupBy('NUM_REFERENCE_NUMBER')\
            .agg(sf.max('POL_EXP_DATE').alias('POL_EXP_DATE'),\
                 sf.max('EFFECTIVE_DATE_OF_CANCELLATION').alias('EFFECTIVE_DATE_OF_CANCELLATION'),\
                 sf.max('DAT_POLICY_EFF_FROMDATE').alias('DAT_POLICY_EFF_FROMDATE'),\
                 sf.max('DAT_ENDORSEMENT_EFF_DATE').alias('DAT_ENDORSEMENT_EFF_DATE'),\
                 sf.max('NUM_DEPARTMENT_CODE').alias('NUM_DEPARTMENT_CODE'),\
                 sf.max('CUSTOMER_ID').alias('CUSTOMER_ID'),\
                 sf.max('CERTIFICATE_NUMBER').alias('CERTIFICATE_NUMBER'),\
                 sf.max('TXT_RE_INSURANCE_INWARD').alias('TXT_RE_INSURANCE_INWARD'),\
                 sf.max('POLICY_NO_GP').alias('POLICY_NO_GP'),\
                 sf.max('POLICY_STATUS').alias('POLICY_STATUS'),\
                 sf.max('TXT_POLICYTERM').alias('POLICYTERM'))

join_cond = [clm_10.NUM_REFERENCE_NO == gpit.NUM_REFERENCE_NUMBER]

clm_11_1 = clm_10\
            .join(gpit, join_cond, 'left_outer')\
            .drop(gpit.NUM_REFERENCE_NUMBER)

clm_11_1.persist()

# clm_11_1.count()
# 678340

### join with uw_department_master - LOB

dep_master = uw_department_master\
            .groupBy('DEPARTMENTCODE')\
            .agg(sf.max('DEPARTMENTNAME').alias('LOB'))

join_cond = [clm_11_1.NUM_DEPARTMENT_CODE == dep_master.DEPARTMENTCODE]

clm_11_2 = clm_11_1\
            .join(dep_master, join_cond, 'left_outer')\
            .drop(dep_master.DEPARTMENTCODE)

### join with uw_department_master - POL_INCEPT_DATE

clm_11_3 = clm_11_2\
            .withColumn('POL_INCEPT_DATE', clm_11_2.DAT_POLICY_EFF_FROMDATE)

## join with genmst_customer

join_cond = [clm_11_3.CUSTOMER_ID == genmst_customer.CUSTOMER_CODE]

clm_11_4 = clm_11_3\
            .join(genmst_customer, join_cond, "left_outer")\
            .drop(genmst_customer.CUSTOMER_CODE)

## join with genmst_location

join_cond = [clm_11_4.NUM_LOSS_LOCATION_CD == genmst_location.NUM_LOCATION_CD]

clm_11_5_1 = clm_11_4\
            .join(genmst_location, join_cond, "left_outer")\
            .drop(genmst_location.NUM_LOCATION_CD)


## join with genmst_state

genmst_state_01 = genmst_state.groupBy('NUM_STATE_CD')\
                              .agg(sf.max('STATE_OF_LOSS').alias('STATE_OF_LOSS'))

join_cond = [clm_11_5_1.NUM_STATE_CD == genmst_state_01.NUM_STATE_CD]

clm_11_5_02 = clm_11_5_1\
                .join(genmst_state_01, join_cond, "left_outer")\
                .drop(genmst_state_01.NUM_STATE_CD)

# clm_11_5_2 = clm_11_5_02\
#                     .withColumn('STATE_OF_LOSS', when(clm_11_5_02.STATE_OF_LOSS.isNull(), clm_11_5_02.TXT_STATE)\
#                                                 .otherwise(clm_11_5_02.STATE_OF_LOSS))
clm_11_5_2 = clm_11_5_02\
                    .withColumn('STATE_OF_LOSS', when(clm_11_5_02.TXT_STATE.isNull(), clm_11_5_02.STATE_OF_LOSS)\
                                                .otherwise(clm_11_5_02.TXT_STATE))

## join with genmst_citydistrict

join_cond = [clm_11_5_2.NUM_STATE_CD == genmst_citydistrict.NUM_STATE_CD,\
             clm_11_5_2.NUM_CITYDISTRICT_CD == genmst_citydistrict.NUM_CITYDISTRICT_CD]

clm_11_5_3 = clm_11_5_2\
                .join(genmst_citydistrict, join_cond, "left_outer")\
                .drop(genmst_citydistrict.NUM_CITYDISTRICT_CD)\
                .drop(genmst_citydistrict.NUM_STATE_CD)

## join with genmst_pincode

genmst_pincode2 = genmst_pincode\
                        .groupBy('NUM_PINCODE')\
                        .agg(sf.max('DAT_INSERT_DATE').alias('check'))

join_cond = [genmst_pincode.NUM_PINCODE == genmst_pincode2.NUM_PINCODE,\
             genmst_pincode.DAT_INSERT_DATE == genmst_pincode2.check]

genmst_pincode3 = genmst_pincode\
                    .join(genmst_pincode2, join_cond, "inner")\
                    .drop(genmst_pincode2.NUM_PINCODE)\
                    .drop(genmst_pincode2.check)\
                    .drop(genmst_pincode.DAT_INSERT_DATE)


pincode1 = genmst_pincode3\
            .groupBy('NUM_PINCODE')\
            .agg(sf.max('LOSSCITY_PC').alias('LOSSCITY_PC'),\
                sf.max('NUM_STATE_CD_PC').alias('NUM_STATE_CD_PC'))

join_cond = [clm_11_5_3.NUM_PIN_CD == pincode1.NUM_PINCODE]

clm_11_5_4 = clm_11_5_3\
                .join(pincode1, join_cond, "left_outer")\
                .drop(pincode1.NUM_PINCODE)

## genmst_state (genmst_state)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_genmst_state",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
genmst_state = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_STATE_CD','TXT_STATE')\
.withColumnRenamed('NUM_STATE_CD','NUM_STATE_CD_PC')

genmst_state_01 = genmst_state.groupBy('NUM_STATE_CD_PC')\
                              .agg(sf.max('TXT_STATE').alias('STATE_OF_LOSS_PC'))

clm_11_5_5 = clm_11_5_4.join(genmst_state_01,['NUM_STATE_CD_PC'],'left_outer')
clm_11_5_6 = clm_11_5_5.withColumn('STATE_OF_LOSS', when(clm_11_5_5.STATE_OF_LOSS.isNull(), clm_11_5_5.STATE_OF_LOSS_PC)\
                                                .otherwise(clm_11_5_5.STATE_OF_LOSS))

clm_12 = clm_11_5_6

# new changes - sagar/snehal - 20190609
genmst_pincode_ss1 = genmst_pincode
genmst_pincode_ss1 = genmst_pincode_ss1\
                         .withColumnRenamed('NUM_PINCODE','NUM_PIN_CD')

genmst_citydistrict_ss1 = genmst_citydistrict.withColumnRenamed('LOSSCITY_CIDS', 'LOSS_LOC_TXT_PINCODE')


pincode_citydistrict = genmst_pincode_ss1.join(genmst_citydistrict_ss1, 'NUM_CITYDISTRICT_CD', 'left')

clm_12 = clm_12\
                .withColumn('mmcp_length', LengthOfString(clm_12.TXT_MMCP_CODE))

clm_12 = clm_12\
            .withColumn('MAJOR_LINE_CD', when(clm_12.mmcp_length == 7, lit(firstTwoChar(clm_12.TXT_MMCP_CODE)))\
                                           .otherwise(lit('')))\
            .withColumn('MINOR_LINE_CD', when(clm_12.mmcp_length == 7, lit(ThirdFourthChar(clm_12.TXT_MMCP_CODE)))\
                                           .otherwise(lit('')))\
            .withColumn('CLASS_PERIL_CD', when(clm_12.mmcp_length == 7, lit(afterFourChar(clm_12.TXT_MMCP_CODE)))\
                                           .otherwise(lit('')))

clm_12 = clm_12\
            .withColumn('MAJOR_LINE_CD', when(clm_12.MAJOR_LINE_CD == '', lit('01'))\
                                           .otherwise(clm_12.MAJOR_LINE_CD))

clm_12 = clm_12\
            .drop('mmcp_length')

## join with gc_clmmst_mmcp_code_map  --  Report Column - MAJOR_LINE_TEXT

mmcp_map_temp = gc_clmmst_mmcp_code_map\
                        .groupBy('TXT_MAJOR_LINE_CD')\
                        .agg(sf.max('TXT_MAJOR_LINE_DESC').alias('TXT_MAJOR_LINE_DESC'))

join_cond = [clm_12.MAJOR_LINE_CD == mmcp_map_temp.TXT_MAJOR_LINE_CD]

clm_13 = clm_12\
            .join(mmcp_map_temp, join_cond, "left_outer")\
            .withColumnRenamed('TXT_MAJOR_LINE_DESC', 'MAJOR_LINE_TEXT')\
            .drop(mmcp_map_temp.TXT_MAJOR_LINE_CD)

## join with gc_clmmst_mmcp_code_map  --  Report Column - MINOR_LINE_TXT

mmcp_map_temp = gc_clmmst_mmcp_code_map\
                            .groupBy('TXT_MAJOR_LINE_CD', 'TXT_MINOR_LINE_CD')\
                            .agg(sf.min('TXT_MINOR_LINE_DESC').alias('TXT_MINOR_LINE_DESC'))

join_cond = [clm_13.MAJOR_LINE_CD == mmcp_map_temp.TXT_MAJOR_LINE_CD,\
             clm_13.MINOR_LINE_CD == mmcp_map_temp.TXT_MINOR_LINE_CD]

clm_14 = clm_13\
            .join(mmcp_map_temp, join_cond, "left_outer")\
            .withColumnRenamed('TXT_MINOR_LINE_DESC', 'MINOR_LINE_TXT')\
            .drop(mmcp_map_temp.TXT_MAJOR_LINE_CD)\
            .drop(mmcp_map_temp.TXT_MINOR_LINE_CD)

## join with gc_clmmst_mmcp_code_map  --  Report Column - CLASS_PERIL_TEXT

mmcp_map_temp = gc_clmmst_mmcp_code_map\
                            .groupBy('TXT_MAJOR_LINE_CD', 'TXT_MINOR_LINE_CD', 'TXT_CLASS_PERIL_CD')\
                            .agg(sf.min('TXT_CLASS_PERIL_DESC').alias('TXT_CLASS_PERIL_DESC'))

join_cond = [clm_14.MAJOR_LINE_CD == mmcp_map_temp.TXT_MAJOR_LINE_CD,\
             clm_14.MINOR_LINE_CD == mmcp_map_temp.TXT_MINOR_LINE_CD,\
             clm_14.CLASS_PERIL_CD == mmcp_map_temp.TXT_CLASS_PERIL_CD]

clm_15 = clm_14\
            .join(mmcp_map_temp, join_cond, "left_outer")\
            .withColumnRenamed('TXT_CLASS_PERIL_DESC', 'CLASS_PERIL_TEXT')\
            .drop(mmcp_map_temp.TXT_MAJOR_LINE_CD)\
            .drop(mmcp_map_temp.TXT_MINOR_LINE_CD)\
            .drop(mmcp_map_temp.TXT_CLASS_PERIL_CD)

### Report Column - PRODUCTNAME  (join with uw_product_master)

join_cond = [clm_15.PRODUCT_CD == uw_product_master.PRODUCTCODE]

clm_16 = clm_15\
            .join(uw_product_master, join_cond, 'left_outer')\
            .drop(uw_product_master.PRODUCTCODE)

clm_dairy = gc_clm_claim_diary1.withColumnRenamed('TXT_INFO1', 'LAST_NOTE_UPDATED_DATE')

clm_dairy = clm_dairy.withColumn('LAST_NOTE_UPDATED_DATE_CLEAN', clm_dairy['LAST_NOTE_UPDATED_DATE'].substr(1, 10))

clm_dairy = clm_dairy[['NUM_CLAIM_NO', 'LAST_NOTE_UPDATED_DATE_CLEAN']]
clm_dairy.createOrReplaceTempView('clm_dairy_temp_view')

sql_1 = """
select NUM_CLAIM_NO, (case when (LAST_NOTE_UPDATED_DATE_CLEAN is not null) then 
concat(substring(LAST_NOTE_UPDATED_DATE_CLEAN,7,4),'-', substring(LAST_NOTE_UPDATED_DATE_CLEAN,4,2),'-', substring(LAST_NOTE_UPDATED_DATE_CLEAN,1,2)) 
else null end) as LAST_NOTE_UPDATED_DATE, LAST_NOTE_UPDATED_DATE_CLEAN
from clm_dairy_temp_view

"""

clm_dairy_1 = sqlContext.sql(sql_1)
sqlContext.dropTempTable('clm_dairy_temp_view')

#parameterize filter added by satya on 20191031
clm_dairy_1 = clm_dairy_1.filter(to_date(clm_dairy_1.LAST_NOTE_UPDATED_DATE, format='yyyy-MM-dd') <= report_date)
clm_dairy_1.createOrReplaceTempView('clm_dairy_1_temp_view')

sql_1 = """
select NUM_CLAIM_NO, (case when ((LAST_NOTE_UPDATED_DATE is not null)) then 
to_date(LAST_NOTE_UPDATED_DATE) 
else null end) as LAST_NOTE_UPDATED_DATE, LAST_NOTE_UPDATED_DATE_CLEAN
from clm_dairy_1_temp_view

"""

clm_dairy = sqlContext.sql(sql_1)
sqlContext.dropTempTable('clm_dairy_1_temp_view')

clm_dairy =  clm_dairy.withColumn('LAST_NOTE_UPDATED_DATE', to_date(clm_dairy.LAST_NOTE_UPDATED_DATE, format='yyyy-MM-dd'))

# change 20190614A  
clm_dairy = clm_dairy.groupBy('NUM_CLAIM_NO')\
                     .agg(sf.max('LAST_NOTE_UPDATED_DATE').alias('LAST_NOTE_UPDATED_DATE'))

join_cond = [clm_16.NUM_CLAIM_NO == clm_dairy.NUM_CLAIM_NO]

clm_18 = clm_16\
            .join(clm_dairy, join_cond, 'left_outer')\
            .drop(clm_dairy.NUM_CLAIM_NO)

# ## join with gc_clm_gen_settlement_info

# gc_clm_gen_settlement_info1.createOrReplaceTempView('clm_sett_info_temp_view')

# query = """select *,
# cast(NUM_UPDATE_NO as decimal) NUM_UPDATE_NO_B,
# cast(NUM_SERIAL_NO as decimal) NUM_SERIAL_NO_B
# from clm_sett_info_temp_view"""

# gc_clm_gen_settlement_info = sqlContext.sql(query)
# sqlContext.dropTempTable('clm_sett_info_temp_view')

# clm_sett_info_00 = gc_clm_gen_settlement_info\
#                         .withColumn('check1', concat(trim(gc_clm_gen_settlement_info.NUM_UPDATE_NO_B),\
#                                                           trim(gc_clm_gen_settlement_info.NUM_SERIAL_NO_B)))\
#                         .drop('NUM_UPDATE_NO')\
#                         .drop('NUM_SERIAL_NO')\
#                         .drop('NUM_UPDATE_NO_B')\
#                         .drop('NUM_SERIAL_NO_B')

# ## clm_sett_info (clm_sett_info)

# clm_sett_info_00.createOrReplaceTempView('clm_sett_info_view')

# query = """select *,
# cast(check1 as decimal) check1_B
# from clm_sett_info_view"""

# clm_sett_info = sqlContext.sql(query)
# sqlContext.dropTempTable('clm_sett_info_view')

# clm_sett_info = clm_sett_info\
#                     .drop('check1')

# clm_sett_info = clm_sett_info\
#                 .withColumnRenamed('check1_B','check1')

clm_sett_info = gc_clm_gen_settlement_info1\
                        .withColumn('check1', concat(trim(gc_clm_gen_settlement_info1.NUM_UPDATE_NO).cast('integer'),\
                                                          trim(gc_clm_gen_settlement_info1.NUM_SERIAL_NO).cast('integer')).cast('integer'))\
                        .drop('NUM_UPDATE_NO')\
                        .drop('NUM_SERIAL_NO')

# clm_11_3.count()

# clm_11_4.count()

# clm_16.count()

# clm_18.count()

clm_sett_info_A = clm_sett_info

clm_sett_info_A2 = clm_sett_info_A.groupBy('NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD')\
                                   .agg(sf.max('check1').alias('check1'))
    
# for group 2
clm_sett_info_B = clm_sett_info.filter((clm_sett_info.NUM_SETTLEMENT_TYPE_CD == '16')|\
                                       (clm_sett_info.NUM_SETTLEMENT_TYPE_CD == '56'))

clm_sett_info_B2 = clm_sett_info_B.groupBy('NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','TXT_INFO24','TXT_INFO25')\
                                   .agg(sf.max('check1').alias('check1'))
    
# for group 3

clm_sett_info_C1 = clm_sett_info_B[['NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','check1','NUM_SETTLEMENT_TYPE_CD','TXT_INFO26']]
clm_sett_info_C2 = clm_sett_info_C1.groupBy('NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','TXT_INFO26')\
                                   .agg(sf.max('check1').alias('check1'))
    
# for group 4

clm_sett_info_D1 = clm_sett_info_B[['NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','check1','NUM_SETTLEMENT_TYPE_CD','TXT_INFO27']]
clm_sett_info_D2 = clm_sett_info_D1.groupBy('NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','TXT_INFO27')\
                                   .agg(sf.max('check1').alias('check1'))
    
# for group 5

clm_sett_info_E1 = clm_sett_info_B[['NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','check1','NUM_SETTLEMENT_TYPE_CD','TXT_INFO28']]
clm_sett_info_E2 = clm_sett_info_E1.groupBy('NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','TXT_INFO28')\
                                   .agg(sf.max('check1').alias('check1'))
    
#This group modified by satya for monetory fields on 20191217
# for group 6
#clm_sett_info_F = clm_sett_info.filter((clm_sett_info.NUM_SETTLEMENT_TYPE_CD == '8')|\
#                                       (clm_sett_info.NUM_SETTLEMENT_TYPE_CD == '12'))
#
#clm_sett_info_F1 = clm_sett_info_F[['NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','check1','TXT_INFO3','TXT_INFO5']]
#clm_sett_info_F2 = clm_sett_info_F1.groupBy('NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD', 'TXT_INFO3')\
#                                   .agg(sf.max('check1').alias('check1'))

clm_sett_info_F = gc_clm_gen_settlement_info1.filter((gc_clm_gen_settlement_info1.NUM_SETTLEMENT_TYPE_CD == '8')|\
                                                    (gc_clm_gen_settlement_info1.NUM_SETTLEMENT_TYPE_CD == '12'))

clm_sett_info_1 = clm_sett_info.join(clm_sett_info_A2, ['NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD', 'check1'])
clm_sett_info_2 = clm_sett_info.join(clm_sett_info_B2, ['NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','TXT_INFO24','TXT_INFO25','check1'])
clm_sett_info_3 = clm_sett_info.join(clm_sett_info_C2, ['NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','TXT_INFO26', 'check1'])
clm_sett_info_4 = clm_sett_info.join(clm_sett_info_D2, ['NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','TXT_INFO27', 'check1'])
clm_sett_info_5 = clm_sett_info.join(clm_sett_info_E2, ['NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','TXT_INFO28', 'check1'])
clm_sett_info_6 = clm_sett_info_F
#modified by satya for monetory fields on 20191217
#clm_sett_info_6 = clm_sett_info.join(clm_sett_info_F2, ['NUM_CLAIM_NO','NUM_SETTLEMENT_TYPE_CD','TXT_INFO3', 'check1'])

clm_sett_info_1A = clm_sett_info_1\
                    .withColumn('TATAAIG_GSTN_NO_CD', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='9', \
                                                           clm_sett_info_1.TXT_INFO44).otherwise(lit('')))\
                    .withColumn('REPUDIATION_CD', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='1', \
                                                       clm_sett_info_1.TXT_INFO2).otherwise(lit('')))\
                    .withColumn('TYPE_OF_SETTLEMENT_CD', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='21', \
                                                              clm_sett_info_1.TXT_INFO1).otherwise(lit('')))\
                    .withColumn('TYPE_OF_SETTLEMENT_CD_HEALTH', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='3', \
                                                              clm_sett_info_1.TXT_INFO1).otherwise(lit('')))\
                    .withColumn('TYPE_OF_SETTLEMENT_CD_MOTOR', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='1', \
                                                              clm_sett_info_1.TXT_INFO2).otherwise(lit('')))\
                    .withColumn('TYPE_OF_SETTLEMENT_CD_PA', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='15', \
                                                              clm_sett_info_1.TXT_INFO1).otherwise(lit('')))\
                    .withColumn('TYPE_OF_SETTLEMENT_CD_TRAVEL', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='56', \
                                                              clm_sett_info_1.TXT_INFO6).otherwise(lit('')))\
                    .withColumn('TYPE_OF_SETTLEMENT_CD_LTA', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='16', \
                                                              clm_sett_info_1.TXT_INFO6).otherwise(lit('')))\
                    .withColumn('INVOICE_NO', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='9', \
                                                   clm_sett_info_1.TXT_INFO46).otherwise(lit('')))\
                    .withColumn('INVOICE_DATE', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='9', \
                                                     clm_sett_info_1.TXT_INFO47).otherwise(lit('')))\
                    .withColumn('CURRENCY_SET_CD', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='56', \
                                                        clm_sett_info_1.TXT_INFO11).otherwise(lit('')))\
                    .withColumn('LOSSCOUNTRY_CD_56', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='56', \
                                                       clm_sett_info_1.TXT_INFO12).otherwise(lit('')))\
				    .withColumn('LOSSCOUNTRY_CD_16', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='16', \
                                                       clm_sett_info_1.TXT_INFO12).otherwise(lit('')))\
                    .withColumn('LOSSCITY_CD_56', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='56', \
                                                    clm_sett_info_1.TXT_INFO13).otherwise(lit('')))\
                    .withColumn('LOSSCITY_CD_16', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='16', \
                                                    clm_sett_info_1.TXT_INFO13).otherwise(lit('')))\
                    .withColumn('EXPENSE_PAYMENT_TYPE_CD', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='9', \
                                                                clm_sett_info_1.TXT_INFO23).otherwise(lit('')))\
                    .withColumn('NOTIONAL_SALVAGE_AMOUNT', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='21', \
                                                                clm_sett_info_1.TXT_INFO5).otherwise(lit('')))\
                    .withColumn('NOTIONAL_SUBROGATION_AMOUNT', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='21', \
                                                                    clm_sett_info_1.TXT_INFO7).otherwise(lit('')))\
                    .withColumn('PAN_NO_SI', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='9', \
                                                  clm_sett_info_1.TXT_INFO2).otherwise(lit('')))\
                    .withColumn('PAN_NO_INSURED', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='7', \
                                                      clm_sett_info_1.TXT_INFO2).otherwise(lit('')))\
                    .withColumn('RELATIONSHIP_CD', when((clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='16')|\
                                                        (clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='56'),clm_sett_info_1.TXT_INFO10)
                                                        .otherwise(lit('')))\
                    .withColumn('SAC_CODE', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='9', \
                                                    clm_sett_info_1.TXT_INFO48).otherwise(lit('')))\
                    .withColumn('TXT_INFO10', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='4', \
                                                    clm_sett_info_1.TXT_INFO10).otherwise(lit('')))\
                    .withColumn('TXT_INFO11', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='4', \
                                                    clm_sett_info_1.TXT_INFO11).otherwise(lit('')))\
                    .withColumn('TXT_INFO30', when(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='3', \
                                                    clm_sett_info_1.TXT_INFO30).otherwise(lit('')))\
                    .withColumn('TXT_INFO31', when((clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='16')|(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='56'), \
                                                    clm_sett_info_1.TXT_INFO31).otherwise(lit('')))\
                    .withColumn('TXT_INFO32', when((clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='16')|(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='56')|(clm_sett_info_1.NUM_SETTLEMENT_TYPE_CD=='3'), \
                                                    clm_sett_info_1.TXT_INFO32).otherwise(lit('')))\
#RELATIONSHIP_CD added by Satya on 20191009 
#TYPE_OF_SETTLEMENT_CD_HEALTH added by Satya on 20191014 
#PAN_NO_INSURED added by Satya on 20191014 
#SAC_CODE added by Satya on 20191022

clm_sett_info_1B = clm_sett_info_1A.groupBy('NUM_CLAIM_NO')\
                                   .agg(sf.max('TATAAIG_GSTN_NO_CD').alias('TATAAIG_GSTN_NO_CD'),\
                                        sf.max('REPUDIATION_CD').alias('REPUDIATION_CD'),\
                                        sf.max('TYPE_OF_SETTLEMENT_CD').alias('TYPE_OF_SETTLEMENT_CD'),\
                                        sf.max('TYPE_OF_SETTLEMENT_CD_HEALTH').alias('TYPE_OF_SETTLEMENT_CD_HEALTH'),\
                                        sf.max('TYPE_OF_SETTLEMENT_CD_MOTOR').alias('TYPE_OF_SETTLEMENT_CD_MOTOR'),\
                                        sf.max('TYPE_OF_SETTLEMENT_CD_PA').alias('TYPE_OF_SETTLEMENT_CD_PA'),\
                                        sf.max('TYPE_OF_SETTLEMENT_CD_TRAVEL').alias('TYPE_OF_SETTLEMENT_CD_TRAVEL'),\
                                        sf.max('TYPE_OF_SETTLEMENT_CD_LTA').alias('TYPE_OF_SETTLEMENT_CD_LTA'),\
                                        sf.max('INVOICE_NO').alias('INVOICE_NO'),\
                                        sf.max('INVOICE_DATE').alias('INVOICE_DATE'),\
                                        sf.max('CURRENCY_SET_CD').alias('CURRENCY_SET_CD'),\
                                        sf.max('LOSSCOUNTRY_CD_56').alias('LOSSCOUNTRY_CD_56'),\
                                        sf.max('LOSSCOUNTRY_CD_16').alias('LOSSCOUNTRY_CD_16'),\
                                        sf.max('LOSSCITY_CD_16').alias('LOSSCITY_CD_16'),\
                                        sf.max('LOSSCITY_CD_56').alias('LOSSCITY_CD_56'),\
                                        sf.max('EXPENSE_PAYMENT_TYPE_CD').alias('EXPENSE_PAYMENT_TYPE_CD'),\
                                        sf.max('NOTIONAL_SALVAGE_AMOUNT').alias('NOTIONAL_SALVAGE_AMOUNT'),\
                                        sf.max('NOTIONAL_SUBROGATION_AMOUNT').alias('NOTIONAL_SUBROGATION_AMOUNT'),\
                                        sf.max('PAN_NO_SI').alias('PAN_NO_SI'),\
                                        sf.max('PAN_NO_INSURED').alias('PAN_NO_INSURED'),\
                                        sf.max('RELATIONSHIP_CD').alias('RELATIONSHIP_CD'),\
                                        sf.max('TXT_INFO10').alias('TXT_INFO10'),\
                                        sf.max('TXT_INFO11').alias('TXT_INFO11'),\
                                        sf.max('TXT_INFO30').alias('TXT_INFO30'),\
                                        sf.max('TXT_INFO31').alias('TXT_INFO31'),\
                                        sf.max('TXT_INFO32').alias('TXT_INFO32'),\
                                        sf.max('SAC_CODE').alias('SAC_CODE'))

#RELATIONSHIP added by Satya on 20191009  
## join for RELATIONSHIP

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 49)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('RELATIONSHIP_CD', lower(trim(gc_genvalue1.TXT_INFO1)))\
                    .drop('TXT_INFO1')

gc_genvalue2 = gc_genvalue1\
                    .groupBy('RELATIONSHIP_CD')\
                    .agg(sf.max('TXT_INFO2').alias('RELATIONSHIP'))

join_cond = [lower(trim(clm_sett_info_1B.RELATIONSHIP_CD)) == gc_genvalue2.RELATIONSHIP_CD]

clm_sett_info_1B = clm_sett_info_1B.join(gc_genvalue2, 'RELATIONSHIP_CD', "left_outer")\
                                    .drop(clm_sett_info_1B.RELATIONSHIP_CD)

###########RELATIONSHIP modification ended

clm_sett_info_2A = clm_sett_info_2\
                   .withColumn('PAYMENT_TYPE16', when((clm_sett_info_2.NUM_SETTLEMENT_TYPE_CD=='16')&\
                                                     (lower(trim(clm_sett_info_2.TXT_INFO24)) == 'true'), lit('Reimbursement'))\
                                                .when((clm_sett_info_2.NUM_SETTLEMENT_TYPE_CD == '16')&\
                                                      (lower(trim(clm_sett_info_2.TXT_INFO25)) == 'true'), lit('Cashless'))\
                                                .otherwise(lit('')))\
                   .withColumn('PAYMENT_TYPE56', when((clm_sett_info_2.NUM_SETTLEMENT_TYPE_CD=='56')&\
                                                     (lower(trim(clm_sett_info_2.TXT_INFO24)) == 'true'), lit('Reimbursement'))\
                                                .when((clm_sett_info_2.NUM_SETTLEMENT_TYPE_CD == '56')&\
                                                     (lower(trim(clm_sett_info_2.TXT_INFO25)) == 'true'), lit('Cashless'))\
                                                .otherwise(lit('')))

clm_sett_info_2B = clm_sett_info_2A.groupBy('NUM_CLAIM_NO')\
                                   .agg(sf.max('PAYMENT_TYPE16').alias('PAYMENT_TYPE16'),\
                                        sf.max('PAYMENT_TYPE56').alias('PAYMENT_TYPE56'))

clm_sett_info_3A = clm_sett_info_3\
                   .withColumn('OPD_TXT16', when((clm_sett_info_3.NUM_SETTLEMENT_TYPE_CD=='16') &\
                                                (lower(trim(clm_sett_info_3.TXT_INFO26)) == 'true'), lit('OPD'))\
                                            .otherwise(lit('')))\
                   .withColumn('OPD_TXT56', when((clm_sett_info_3.NUM_SETTLEMENT_TYPE_CD=='56') &\
                                                (lower(trim(clm_sett_info_3.TXT_INFO26)) == 'true'), lit('OPD'))\
                                            .otherwise(lit('')))

clm_sett_info_3B = clm_sett_info_3A.groupBy('NUM_CLAIM_NO')\
                                   .agg(sf.max('OPD_TXT16').alias('OPD_TXT16'),\
                                        sf.max('OPD_TXT56').alias('OPD_TXT56'))

clm_sett_info_4A = clm_sett_info_4\
                   .withColumn('IPD_TXT16', when((clm_sett_info_4.NUM_SETTLEMENT_TYPE_CD=='16') &\
                                                (lower(trim(clm_sett_info_4.TXT_INFO27)) == 'true'), lit('IPD'))\
                                            .otherwise(lit('')))\
                   .withColumn('IPD_TXT56', when((clm_sett_info_4.NUM_SETTLEMENT_TYPE_CD=='56') &\
                                                (lower(trim(clm_sett_info_4.TXT_INFO27)) == 'true'), lit('IPD'))\
                                            .otherwise(lit('')))
        
clm_sett_info_4B = clm_sett_info_4A.groupBy('NUM_CLAIM_NO')\
                                   .agg(sf.max('IPD_TXT16').alias('IPD_TXT16'),\
                                        sf.max('IPD_TXT56').alias('IPD_TXT56'))

clm_sett_info_5A = clm_sett_info_5\
                   .withColumn('SUBLIMIT', when((clm_sett_info_5.NUM_SETTLEMENT_TYPE_CD=='56') &\
                                                (lower(trim(clm_sett_info_5.TXT_INFO28)) == 'false'), lit('No'))\
                               .when((clm_sett_info_5.NUM_SETTLEMENT_TYPE_CD=='56') &\
                                                (lower(trim(clm_sett_info_5.TXT_INFO28)) == 'true'), lit('Yes'))\
                                           .otherwise(lit('')))\
                   .withColumn('TRAVEL_TXT16', when((clm_sett_info_5.NUM_SETTLEMENT_TYPE_CD=='16') &\
                                                (lower(trim(clm_sett_info_5.TXT_INFO28)) == 'true'), lit('Travel'))\
                                               .otherwise(lit('')))\
                   .withColumn('TRAVEL_TXT56', when((clm_sett_info_5.NUM_SETTLEMENT_TYPE_CD=='56') &\
                                                (lower(trim(clm_sett_info_5.TXT_INFO28)) == 'true'), lit('Travel'))\
                                               .otherwise(lit('')))

clm_sett_info_5B = clm_sett_info_5A.groupBy('NUM_CLAIM_NO')\
                                   .agg(sf.max('SUBLIMIT').alias('SUBLIMIT'),\
                                        sf.max('TRAVEL_TXT16').alias('TRAVEL_TXT16'),\
                                        sf.max('TRAVEL_TXT56').alias('TRAVEL_TXT56'))

#Added tp_award - new mapping - satya 20191211
clm_sett_info_6A = clm_sett_info_6\
                    .withColumn('DEFENCE_COST', when((clm_sett_info_6.NUM_SETTLEMENT_TYPE_CD=='8')&\
                                                     (clm_sett_info_6.TXT_INFO3=='12'), clm_sett_info_6.TXT_INFO5)\
                                                .otherwise(lit('')))\
                    .withColumn('PAYMENT_CORRRECTION', when((clm_sett_info_6.NUM_SETTLEMENT_TYPE_CD=='12')&\
                                                                (clm_sett_info_6.TXT_INFO3 =='19'), clm_sett_info_6.TXT_INFO5)\
                                                       .otherwise(lit('')))\
                    .withColumn('INDEMNITY_PARTS', when((clm_sett_info_6.NUM_SETTLEMENT_TYPE_CD=='8')&\
                                                            (clm_sett_info_6.TXT_INFO3 =='2'), clm_sett_info_6.TXT_INFO5)\
                                                   .otherwise(lit('')))\
                    .withColumn('INDEMNITY_PAINT', when((clm_sett_info_6.NUM_SETTLEMENT_TYPE_CD=='8')&\
                                                            (clm_sett_info_6.TXT_INFO3 =='15'), clm_sett_info_6.TXT_INFO5)\
                                                   .otherwise(lit('')))\
                    .withColumn('INDEMNITY_LABOR', when((clm_sett_info_6.NUM_SETTLEMENT_TYPE_CD=='8')&\
                                                            (clm_sett_info_6.TXT_INFO3 =='14'), clm_sett_info_6.TXT_INFO5)\
                                                   .otherwise(lit('')))\
                    .withColumn('INDEMNITY', when((clm_sett_info_6.NUM_SETTLEMENT_TYPE_CD=='8')&\
                                                      (clm_sett_info_6.TXT_INFO3 =='13')&\
                                                      ((to_date(clm_sett_info_6.DAT_INSERT_DATE, format='yyyy-MM-dd') >= first_day_of_month(report_date))&\
                                                        (to_date(clm_sett_info_6.DAT_INSERT_DATE, format='yyyy-MM-dd') <= report_date)), clm_sett_info_6.TXT_INFO5)\
                                             .otherwise(lit('')))\
                    .withColumn('BASIC_TP', when((clm_sett_info_6.NUM_SETTLEMENT_TYPE_CD=='8')&\
                                                     (clm_sett_info_6.TXT_INFO3 =='8'), clm_sett_info_6.TXT_INFO5)\
                                            .otherwise(lit('')))\
                    .withColumn('TP_AWARD', when((clm_sett_info_6.NUM_SETTLEMENT_TYPE_CD=='8')&\
                                                     (clm_sett_info_6.TXT_INFO3 =='7'), clm_sett_info_6.TXT_INFO5)\
                                            .otherwise(lit('')))\
                    .withColumn('COSTS_PLAINTIFF', when((clm_sett_info_6.NUM_SETTLEMENT_TYPE_CD=='8')&\
                                                     (clm_sett_info_6.TXT_INFO3 =='10'), clm_sett_info_6.TXT_INFO5)\
                                            .otherwise(lit('')))\
                    .withColumn('INTEREST_AMOUNT', when((clm_sett_info_6.NUM_SETTLEMENT_TYPE_CD=='8')&\
                                                     (clm_sett_info_6.TXT_INFO3=='17'), clm_sett_info_6.TXT_INFO5)\
                                                .otherwise(lit('')))\
                    .withColumnRenamed('NUM_UPDATE_NO','NUM_PAYMENT_UPDATE_NO')

clm_sett_info_6B = clm_sett_info_6A.groupBy('NUM_CLAIM_NO','NUM_PAYMENT_UPDATE_NO')\
                                   .agg(sf.sum('DEFENCE_COST').alias('DEFENCE_COST'),\
                                        sf.sum('PAYMENT_CORRRECTION').alias('PAYMENT_CORRRECTION'),\
                                        sf.sum('INDEMNITY_PARTS').alias('INDEMNITY_PARTS'),\
                                        sf.sum('INDEMNITY_PAINT').alias('INDEMNITY_PAINT'),\
                                        sf.sum('INDEMNITY_LABOR').alias('INDEMNITY_LABOR'),\
                                        sf.sum('INDEMNITY').alias('INDEMNITY'),\
                                        sf.sum('BASIC_TP').alias('BASIC_TP'),\
                                        sf.sum('TP_AWARD').alias('TP_AWARD'),\
                                        sf.sum('COSTS_PLAINTIFF').alias('COSTS_PLAINTIFF'),\
                                        sf.sum('INTEREST_AMOUNT').alias('INTEREST_AMOUNT'))

clm_18A = clm_18.join( clm_sett_info_1B, 'NUM_CLAIM_NO', "left_outer")
clm_18B = clm_18A.join(clm_sett_info_2B, 'NUM_CLAIM_NO', "left_outer")
clm_18C = clm_18B.join(clm_sett_info_3B, 'NUM_CLAIM_NO', "left_outer")
clm_18D = clm_18C.join(clm_sett_info_4B, 'NUM_CLAIM_NO', "left_outer")
clm_18E = clm_18D.join(clm_sett_info_5B, 'NUM_CLAIM_NO', "left_outer")
clm_18F = clm_18E.join(clm_sett_info_6B, ['NUM_CLAIM_NO','NUM_PAYMENT_UPDATE_NO'], "left_outer")

# fixed losscountry in below dataframe on 20191118 by sagar
clm_18G = clm_18F\
                    .withColumn('TREATMENTTYPE16',\
                                        when((clm_18F.OPD_TXT16 == 'OPD')&\
                                             (clm_18F.IPD_TXT16 == 'IPD')&\
                                             (clm_18F.TRAVEL_TXT16 == 'Travel'), lit('OPD & IPD & Travel'))\
                                        .when((clm_18F.OPD_TXT16 == 'OPD')&\
                                             (clm_18F.IPD_TXT16 == 'IPD')&\
                                             (clm_18F.TRAVEL_TXT16 == ''), lit('OPD & IPD'))\
                                        .when((clm_18F.OPD_TXT16 == 'OPD')&\
                                             (clm_18F.IPD_TXT16 == '')&\
                                             (clm_18F.TRAVEL_TXT16 == 'Travel'), lit('OPD & Travel'))\
                                        .when((clm_18F.OPD_TXT16 == '')&\
                                             (clm_18F.IPD_TXT16 == 'IPD')&\
                                             (clm_18F.TRAVEL_TXT16 == 'Travel'), lit('IPD & Travel'))\
                                        .when((clm_18F.OPD_TXT16 == 'OPD')&\
                                             (clm_18F.IPD_TXT16 == '')&\
                                             (clm_18F.TRAVEL_TXT16 == ''), lit('OPD'))\
                                        .when((clm_18F.OPD_TXT16 == '')&\
                                             (clm_18F.IPD_TXT16 == 'IPD')&\
                                             (clm_18F.TRAVEL_TXT16 == ''), lit('IPD'))\
                                        .when((clm_18F.OPD_TXT16 == '')&\
                                             (clm_18F.IPD_TXT16 == '')&\
                                             (clm_18F.TRAVEL_TXT16 == 'Travel'), lit('Travel'))\
                                        .otherwise(lit('')))\
                    .withColumn('TREATMENTTYPE56',\
                                        when((clm_18F.OPD_TXT56 == 'OPD')&\
                                             (clm_18F.IPD_TXT56 == 'IPD')&\
                                             (clm_18F.TRAVEL_TXT56 == 'Travel'), lit('OPD & IPD & Travel'))\
                                        .when((clm_18F.OPD_TXT56 == 'OPD')&\
                                             (clm_18F.IPD_TXT56 == 'IPD')&\
                                             (clm_18F.TRAVEL_TXT56 == ''), lit('OPD & IPD'))\
                                        .when((clm_18F.OPD_TXT56 == 'OPD')&\
                                             (clm_18F.IPD_TXT56 == '')&\
                                             (clm_18F.TRAVEL_TXT56 == 'Travel'), lit('OPD & Travel'))\
                                        .when((clm_18F.OPD_TXT56 == '')&\
                                             (clm_18F.IPD_TXT56 == 'IPD')&\
                                             (clm_18F.TRAVEL_TXT56 == 'Travel'), lit('IPD & Travel'))\
                                        .when((clm_18F.OPD_TXT56 == 'OPD')&\
                                             (clm_18F.IPD_TXT56 == '')&\
                                             (clm_18F.TRAVEL_TXT56 == ''), lit('OPD'))\
                                        .when((clm_18F.OPD_TXT56 == '')&\
                                             (clm_18F.IPD_TXT56 == 'IPD')&\
                                             (clm_18F.TRAVEL_TXT56 == ''), lit('IPD'))\
                                        .when((clm_18F.OPD_TXT56 == '')&\
                                             (clm_18F.IPD_TXT56 == '')&\
                                             (clm_18F.TRAVEL_TXT56 == 'Travel'), lit('Travel'))\
                                        .otherwise(lit('')))\
					.withColumn('LOSSCOUNTRY_CD', when(clm_18F.NUM_DEPARTMENT_CODE=='16', clm_18F.LOSSCOUNTRY_CD_16)\
					                             .when(clm_18F.NUM_DEPARTMENT_CODE=='56', clm_18F.LOSSCOUNTRY_CD_56)\
												 .otherwise(lit('')))\
					.withColumn('LOSSCITY_CD', when(clm_18F.NUM_DEPARTMENT_CODE=='16', clm_18F.LOSSCITY_CD_16)\
					                           .when(clm_18F.NUM_DEPARTMENT_CODE=='56', clm_18F.LOSSCITY_CD_56)\
											   .otherwise(lit('')))\
                    .withColumn('ICD_MAJOR_DATA', \
					                    when((clm_18F.NUM_DEPARTMENT_CODE=='16')|(clm_18F.NUM_DEPARTMENT_CODE=='56'),\
										clm_18F.TXT_INFO31)\
										.when(clm_18F.NUM_DEPARTMENT_CODE=='28', clm_18F.TXT_INFO30)\
										.when(clm_18F.NUM_DEPARTMENT_CODE=='42', clm_18F.TXT_INFO10)\
										.otherwise(lit('')))\
			        .withColumn('ICD_MINOR_DATA', \
					                    when((clm_18F.NUM_DEPARTMENT_CODE=='16')|(clm_18F.NUM_DEPARTMENT_CODE=='56')|(clm_18F.NUM_DEPARTMENT_CODE=='28'),\
										clm_18F.TXT_INFO32)\
										.when(clm_18F.NUM_DEPARTMENT_CODE=='42', clm_18F.TXT_INFO11)\
										.otherwise(lit('')))\
										
split_col = pyspark.sql.functions.split(clm_18G['ICD_MAJOR_DATA'], '\(')
clm_18G = clm_18G.withColumn('ICD_MAJOR_CD', split_col.getItem(0))
clm_18G = clm_18G.withColumn('ICD_MAJOR_DESC', split_col.getItem(1))

split_col = pyspark.sql.functions.split(clm_18G['ICD_MINOR_DATA'], '\(')
clm_18G = clm_18G.withColumn('ICD_MINOR_CD', split_col.getItem(0))
clm_18G = clm_18G.withColumn('ICD_MINOR_DESC', split_col.getItem(1))

clm_18G = clm_18G\
                 .withColumn('ICD_MINOR_CD', trim(clm_18G.ICD_MINOR_CD))\
                 .withColumn('ICD_MAJOR_CD', trim(clm_18G.ICD_MAJOR_CD))\
                 .withColumn('ICD_MINOR_DESC', regexp_replace('ICD_MINOR_DESC', '\)', ''))\
                 .withColumn('ICD_MAJOR_DESC', regexp_replace('ICD_MAJOR_DESC', '\)', ''))\

clm_20_1_1 = clm_18G\
                    .drop('OPD_TXT56')\
                    .drop('IPD_TXT56')\
                    .drop('TRAVEL_TXT56')\
                    .drop('OPD_TXT16')\
                    .drop('IPD_TXT16')\
                    .drop('TRAVEL_TXT16')\
					.drop('LOSSCITY_CD_16')\
					.drop('LOSSCITY_CD_56')\
					.drop('TXT_INFO10')\
					.drop('TXT_INFO11')\
					.drop('TXT_INFO30')\
					.drop('TXT_INFO31')\
					.drop('TXT_INFO32')\
                    
# New code to read attributes from gc_clm_gen_settlement_info table ends here

# small logic change done for ICD_MAJOR and ICD_MINOR - sagar - 20190620
clm_20_1_1 = clm_20_1_1.withColumn('ICD_MAJOR_CD', when(clm_20_1_1.MAJOR_LINE_CD=='02', clm_20_1_1.ICD_MAJOR_CD)\
                               .otherwise(lit('')))\
                   .withColumn('ICD_MAJOR_DESC', when(clm_20_1_1.MAJOR_LINE_CD=='02', clm_20_1_1.ICD_MAJOR_DESC)\
                               .otherwise(lit('')))\
                   .withColumn('ICD_MINOR_CD', when(clm_20_1_1.MAJOR_LINE_CD=='02', clm_20_1_1.ICD_MINOR_CD)\
                               .otherwise(lit('')))\
                   .withColumn('ICD_MINOR_DESC', when(clm_20_1_1.MAJOR_LINE_CD=='02', clm_20_1_1.ICD_MINOR_DESC)\
                               .otherwise(lit('')))
            
# Indemnity related changes for paid report by sagar - 20190723
clm_20_1_1 = clm_20_1_1.withColumn('INDEMNITY', when(clm_20_1_1.ACCT_LINE_CD == 50, \
                                                     clm_20_1_1.INDEMNITY))\
                       .withColumn('INDEMNITY_LABOR', when(clm_20_1_1.ACCT_LINE_CD == 50, \
                                                     clm_20_1_1.INDEMNITY_LABOR))\
                       .withColumn('INDEMNITY_PAINT', when(clm_20_1_1.ACCT_LINE_CD == 50, \
                                                     clm_20_1_1.INDEMNITY_PAINT))\
                       .withColumn('INDEMNITY_PARTS', when(clm_20_1_1.ACCT_LINE_CD == 50, \
                                                     clm_20_1_1.INDEMNITY_PARTS))

# clm_sett_info.printSchema()
# clm_sett_info4.select('NUM_CLAIM_NO', 'ICD_MAJOR_CD', 'ICD_MAJOR_DESC', 'ICD_MINOR_CD', 'ICD_MINOR_DESC').show(20, False)
# clm_20_1_1.select('NUM_CLAIM_NO', 'ICD_MAJOR_CD', 'ICD_MAJOR_DESC', 'ICD_MINOR_CD', 'ICD_MINOR_DESC', 'LAST_NOTE_UPDATED_DATE').show(20, False)
## A&H_PAYMENT_TYPE, TREATMENTTYPE56

clm_20_1 = clm_20_1_1\
                .withColumn('ANH_PAYMENT_TYPE', when(trim(clm_20_1_1.NUM_DEPARTMENT_CODE) == 16, clm_20_1_1.PAYMENT_TYPE16)\
                                           .when(trim(clm_20_1_1.NUM_DEPARTMENT_CODE) == 56, clm_20_1_1.PAYMENT_TYPE56)\
                                           .otherwise(lit('')))\
                .withColumn('TREATMENTTYPE', when(trim(clm_20_1_1.NUM_DEPARTMENT_CODE) == 16, clm_20_1_1.TREATMENTTYPE16)\
                                           .when(trim(clm_20_1_1.NUM_DEPARTMENT_CODE) == 56, clm_20_1_1.TREATMENTTYPE56)\
                                           .otherwise(lit('')))

## join for LOSSCOUNTRY

country_master2 = country_master1\
                            .withColumn('COUNTRYCODE', lower(trim(country_master1.COUNTRYCODE)))

country_master3 = country_master2\
                        .groupBy('COUNTRYCODE')\
                        .agg(sf.max('COUNTRYNAME').alias('LOSSCOUNTRY'))

join_cond = [lower(trim(clm_20_1.LOSSCOUNTRY_CD)) == country_master3.COUNTRYCODE]

clm_20_1_1 = clm_20_1\
                .join(country_master3, join_cond, "left_outer")\
                .drop(country_master3.COUNTRYCODE)\
                .drop(clm_20_1.LOSSCOUNTRY_CD)


## join with genmst_citydistrict

citydistrict1 = genmst_citydistrict\
                    .groupBy('NUM_CITYDISTRICT_CD')\
                    .agg(sf.max('NUM_STATE_CD').alias('check1'))

join_cond = [genmst_citydistrict.NUM_CITYDISTRICT_CD == citydistrict1.NUM_CITYDISTRICT_CD,\
             genmst_citydistrict.NUM_STATE_CD == citydistrict1.check1]

citydistrict2 = genmst_citydistrict\
                        .join(citydistrict1, join_cond, "inner")\
                        .drop(citydistrict1.NUM_CITYDISTRICT_CD)\
                        .drop(citydistrict1.check1)\
                        .drop(genmst_citydistrict.NUM_STATE_CD)

citydistrict3 = citydistrict2\
                    .groupBy('NUM_CITYDISTRICT_CD')\
                    .agg(sf.max('LOSSCITY_CIDS').alias('LOSSCITY_SI'))

citydistrict3.createOrReplaceTempView('citydistrict_view')

query = """select *,
cast(cast(NUM_CITYDISTRICT_CD as decimal) as string) NUM_CITYDISTRICT_CD_B
from citydistrict_view"""

citydistrict = sqlContext.sql(query)
sqlContext.dropTempTable('citydistrict_view')

citydistrict = citydistrict\
                        .drop('NUM_CITYDISTRICT_CD')

citydistrict = citydistrict\
                        .withColumnRenamed('NUM_CITYDISTRICT_CD_B','NUM_CITYDISTRICT_CD')


join_cond = [clm_20_1_1.LOSSCITY_CD == citydistrict.NUM_CITYDISTRICT_CD]

clm_20_1_01 = clm_20_1_1\
                .join(citydistrict, join_cond, "left_outer")\
                .drop(citydistrict.NUM_CITYDISTRICT_CD)
#20190612start
# clm_20_1_01 = clm_20_1_01\
#                     .withColumn('LOSSCITY_CD',\
#                                  when(clm_20_1_01.MAJOR_LINE_CD == '02', clm_20_1_01.LOSSCITY_CD)
#                                   .otherwise(lit('')))
#20190612end
# clm_20_1_02 = clm_20_1_01\
#                     .withColumn('LOSSCITY', when((clm_20_1_01.NUM_LOSS_LOCATION_CD.isNull())|\
#                                                  (trim(clm_20_1_01.NUM_LOSS_LOCATION_CD) == '0'),clm_20_1_01.LOSSCITY_PC)\
#                                             .otherwise(lit(clm_20_1_01.LOSSCITY_CIDS)))

# clm_20_1_02 = clm_20_1_01\
#                     .withColumn('LOSSCITY', when(clm_20_1_01.LOSSCITY_CIDS.isNull(),clm_20_1_01.LOSSCITY_PC)\
#                                             .otherwise(lit(clm_20_1_01.LOSSCITY_CIDS)))

# Modified on 20191118 by Sagar to read losscity based on pincode available in gc_clm_gen_info
clm_20_1_02 = clm_20_1_01\
                    .withColumn('LOSSCITY', when(clm_20_1_01.LOSSCITY_PC.isNotNull(),clm_20_1_01.LOSSCITY_PC)\
                                            .otherwise(lit(clm_20_1_01.LOSSCITY_SI)))

# clm_20_1_2 = clm_20_1_02\
#                     .withColumn('LOSSCITY', when((clm_20_1_02.NUM_PIN_CD.isNull())|\
#                                                  (trim(clm_20_1_02.NUM_PIN_CD) == '0'),clm_20_1_02.LOSSCITY_SI)\
#                                             .otherwise(lit(clm_20_1_02.LOSSCITY)))

# clm_20_1_2 = clm_20_1_02\
#                     .withColumn('LOSSCITY', when((clm_20_1_02.LOSSCITY.isNull()),clm_20_1_02.LOSSCITY_SI)\
#                                             .otherwise(lit(clm_20_1_02.LOSSCITY)))

clm_20_1_2 = clm_20_1_02\
                    .withColumn('LOSSCITY', when((clm_20_1_02.LOSSCITY.isNull()),clm_20_1_02.LOSSCITY_CIDS)\
                                            .otherwise(lit(clm_20_1_02.LOSSCITY)))

clm_20_1_2 = clm_20_1_2\
                    .withColumn('LOSS_LOC_TXT', clm_20_1_2.LOSSCITY)\
                    .withColumn('LOSSCITY', when(clm_20_1_2.MAJOR_LINE_CD == '02', clm_20_1_2.LOSSCITY)
                               .otherwise(lit('')))

## join for CURRENCY

clmmst_currency1 = gc_clmmst_currency_vw_travel\
                            .withColumn('ITEM_VALUE', lower(trim(gc_clmmst_currency_vw_travel.ITEM_VALUE)))

clmmst_currency2 = clmmst_currency1\
                                .groupBy('ITEM_VALUE')\
                                .agg(sf.max('ITEM_TEXT').alias('CURRENCY'))

join_cond = [lower(trim(clm_20_1_2.CURRENCY_SET_CD)) == clmmst_currency2.ITEM_VALUE]

clm_20_3 = clm_20_1_2\
            .join(clmmst_currency2, join_cond, "left_outer")\
            .drop(clmmst_currency2.ITEM_VALUE)\
            .drop(clm_20_1_2.CURRENCY_SET_CD)
##########################   Part 2 Ends here ########################

## join for TATAAIG_GSTN_NO

gst1 = genmst_tab_state\
            .withColumn('TXT_GST_STATE_CD', trim(genmst_tab_state.TXT_GST_STATE_CD))

gst2 = gst1\
        .groupBy('TXT_GST_STATE_CD')\
        .agg(sf.max('TXT_GSTIN_NO').alias('TATAAIG_GSTN_NO'))

join_cond = [trim(clm_20_3.TATAAIG_GSTN_NO_CD) == gst2.TXT_GST_STATE_CD]

clm_20_4 = clm_20_3\
            .join(gst2, join_cond, "left_outer")\
            .drop(gst2.TXT_GST_STATE_CD)\
            .drop(clm_20_3.TATAAIG_GSTN_NO_CD)


## join for EXPENSE_PAYMENT_TYPE_CD

paymenttype_e1 = gc_clmmst_paymenttype_e\
                            .withColumn('ITEM_VALUE', trim(gc_clmmst_paymenttype_e.ITEM_VALUE))

paymenttype_e2 = paymenttype_e1\
                    .groupBy('ITEM_VALUE')\
                    .agg(sf.max('ITEM_TEXT').alias('EXPENSE_PAYMENT_TYPE'))

join_cond = [trim(clm_20_4.EXPENSE_PAYMENT_TYPE_CD) == paymenttype_e2.ITEM_VALUE]

clm_20 = clm_20_4\
            .join(paymenttype_e2, join_cond, "left_outer")\
            .drop(paymenttype_e2.ITEM_VALUE)\
            .drop(clm_20_4.EXPENSE_PAYMENT_TYPE_CD)
            
# 20190730 - Sagar EXPENSE_PAYMENT_TYPE should have value only for expense records. otherwise null.
clm_20 = clm_20.withColumn('EXPENSE_PAYMENT_TYPE', when(clm_20.ACCT_LINE_CD == 55, \
                            clm_20.EXPENSE_PAYMENT_TYPE))


## join with gc_clmmst_repudiation_reason

join_cond = [clm_20.REPUDIATION_CD == gc_clmmst_repudiation_reason.NUM_REPUDIATION_REASON_ID]

clm_21 = clm_20\
            .join(gc_clmmst_repudiation_reason, join_cond, "left_outer")\
            .drop(gc_clmmst_repudiation_reason.NUM_REPUDIATION_REASON_ID)

clm_21_1 = clm_21\
            .withColumn('REPUDIATED_REASON', when(lower(trim(clm_21.TXT_INFO1)) == 'repudiation',\
                                                  clm_21.TXT_REPUDIATION_REASON_DESC)\
                                            .otherwise(lit('')))\
            .withColumn('EXONERATION', when(lower(trim(clm_21.TXT_INFO1)) == 'cwp',\
                                                  clm_21.TXT_REPUDIATION_REASON_DESC)\
                                            .otherwise(lit('')))\
            .drop('TXT_REPUDIATION_REASON_DESC')\
            .drop('TXT_INFO1')\
            .drop('REPUDIATION_CD')


## join for TYPE_OF_SETTLEMENT

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 172)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('TYPE_OF_SETTLEMENT'))

join_cond = [trim(clm_21_1.TYPE_OF_SETTLEMENT_CD) == gc_genvalue2.TXT_INFO1]

clm_22_1 = clm_21_1\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)\
            .drop(clm_21_1.TYPE_OF_SETTLEMENT_CD)

##chages made by satya on 20191014 for TYPE_OF_SETTLEMENT_HEALTH
## join for TYPE_OF_SETTLEMENT_CD_HEALTH

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 345)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('TYPE_OF_SETTLEMENT_HEALTH'))

join_cond = [trim(clm_22_1.TYPE_OF_SETTLEMENT_CD_HEALTH) == gc_genvalue2.TXT_INFO1]

clm_22_2 = clm_22_1\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)

##chages made by satya on 20191210 for TYPE_OF_SETTLEMENT_MOTOR
## join for TYPE_OF_SETTLEMENT_CD_MOTOR

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 1)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('TYPE_OF_SETTLEMENT_MOTOR'))

join_cond = [trim(clm_22_2.TYPE_OF_SETTLEMENT_CD_MOTOR) == gc_genvalue2.TXT_INFO1]

clm_22_3 = clm_22_2\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)

##chages made by satya on 20210319 for TYPE_OF_SETTLEMENT_PA
## join for TYPE_OF_SETTLEMENT_PA

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 131)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('TYPE_OF_SETTLEMENT_PA'))

gc_genvalue3 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('TYPE_OF_SETTLEMENT_TRAVEL'))

gc_genvalue4 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('TYPE_OF_SETTLEMENT_LTA'))

join_cond = [trim(clm_22_3.TYPE_OF_SETTLEMENT_CD_PA) == gc_genvalue2.TXT_INFO1]

clm_22_4 = clm_22_3\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)

join_cond = [trim(clm_22_4.TYPE_OF_SETTLEMENT_CD_TRAVEL) == gc_genvalue3.TXT_INFO1]

clm_22_5 = clm_22_4\
            .join(gc_genvalue3, join_cond, "left_outer")\
            .drop(gc_genvalue3.TXT_INFO1)

join_cond = [trim(clm_22_5.TYPE_OF_SETTLEMENT_CD_LTA) == gc_genvalue4.TXT_INFO1]

clm_22_4 = clm_22_5\
            .join(gc_genvalue4, join_cond, "left_outer")\
            .drop(gc_genvalue4.TXT_INFO1)

clm_22_5 = clm_22_4\
                .withColumn('TYPE_OF_SETTLEMENT', \
                            when(clm_22_4.NUM_DEPARTMENT_CODE==28, clm_22_4.TYPE_OF_SETTLEMENT_HEALTH)\
                            .when(clm_22_4.NUM_DEPARTMENT_CODE==31, clm_22_4.TYPE_OF_SETTLEMENT_MOTOR)\
                            .when(clm_22_4.NUM_DEPARTMENT_CODE==15, clm_22_4.TYPE_OF_SETTLEMENT_PA)\
                            .when(clm_22_4.NUM_DEPARTMENT_CODE==56, clm_22_4.TYPE_OF_SETTLEMENT_TRAVEL)\
                            .when(clm_22_4.NUM_DEPARTMENT_CODE==16, clm_22_4.TYPE_OF_SETTLEMENT_LTA)\
                            .otherwise(clm_22_4.TYPE_OF_SETTLEMENT))

clm_22 = clm_22_5.drop('TYPE_OF_SETTLEMENT_CD_HEALTH','TYPE_OF_SETTLEMENT_CD_MOTOR','TYPE_OF_SETTLEMENT_CD_PA')\
                 .drop('TYPE_OF_SETTLEMENT_HEALTH','TYPE_OF_SETTLEMENT_MOTOR','TYPE_OF_SETTLEMENT_PA')\
                 .drop('TYPE_OF_SETTLEMENT_CD_LTA','TYPE_OF_SETTLEMENT_CD_TRAVEL','TYPE_OF_SETTLEMENT_TRAVEL','TYPE_OF_SETTLEMENT_LTA')

## join with gc_clm_tp_award_dtls

#below logic is written in better way by satya - 20200504
clm_tp_awd = gc_clm_tp_award_dtls1\
                        .withColumn('check1', concat(trim(gc_clm_tp_award_dtls1.NUM_UPDATE_NO).cast('integer'),\
                                                          trim(gc_clm_tp_award_dtls1.NUM_SERIAL_NO).cast('integer')).cast('integer'))\
                        .drop('NUM_UPDATE_NO')\
                        .drop('NUM_SERIAL_NO')

clm_tp_awd1 = clm_tp_awd\
                    .groupBy('NUM_CLAIM_NO','NUM_AWARD_TYPE_CD')\
                    .agg(sf.max('check1').alias('check2'))

join_cond = [clm_tp_awd.NUM_CLAIM_NO == clm_tp_awd1.NUM_CLAIM_NO,\
             clm_tp_awd.NUM_AWARD_TYPE_CD == clm_tp_awd1.NUM_AWARD_TYPE_CD,\
             clm_tp_awd.check1 == clm_tp_awd1.check2]

clm_tp_awd2 = clm_tp_awd\
                        .join(clm_tp_awd1, join_cond, "inner")\
                        .drop(clm_tp_awd1.NUM_CLAIM_NO)\
                        .drop(clm_tp_awd1.NUM_AWARD_TYPE_CD)\
                        .drop(clm_tp_awd1.check2)\
                        .drop(clm_tp_awd.check1)

#TP_AWARD/COSTS_PLAINTIFF removed mapped to gc_clm_gen_settlement_info
clm_tp_awd3 = clm_tp_awd2\
                    .withColumn('INTERIM_AWARD_DATE',\
                                when(clm_tp_awd2.NUM_AWARD_TYPE_CD == 1, clm_tp_awd2.TXT_INFO1)
                                .otherwise(lit('')))\
                    .withColumn('DATE_OF_RECEIPT_OF_AWARD_BY_TATAAIG',\
                                when(clm_tp_awd2.NUM_AWARD_TYPE_CD == 3, clm_tp_awd2.TXT_INFO13)
                                .otherwise(lit('')))\
                    .withColumn('INTERIM_AWARD_RECEIVED_BY_TAGIC_DATE',\
                                when(clm_tp_awd2.NUM_AWARD_TYPE_CD == 6, clm_tp_awd2.TXT_INFO13)
                                .otherwise(lit('')))\
                    .withColumn('DATE_OF_RECEIPT_OF_AWARD_BY_ADVOCATE',\
                                when(clm_tp_awd2.NUM_AWARD_TYPE_CD == 3, clm_tp_awd2.TXT_INFO14)
                                .otherwise(lit('')))\
                    .withColumn('DATE_OF_AWARD',\
                                when(clm_tp_awd2.NUM_AWARD_TYPE_CD == 1, clm_tp_awd2.TXT_INFO3)
                                .otherwise(lit('')))

#.withColumn('TP_AWARD',\
#            when(clm_tp_awd2.NUM_AWARD_TYPE_CD == 4, clm_tp_awd2.TXT_INFO25)
#            .otherwise(lit('')))\

#.withColumn('COSTS_PLAINTIFF',\
#            when(clm_tp_awd2.NUM_AWARD_TYPE_CD == 4, clm_tp_awd2.TXT_INFO27)
#            .otherwise(lit('')))\

clm_tp_awd4 = clm_tp_awd3\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('INTERIM_AWARD_DATE').alias('INTERIM_AWARD_DATE'),\
                        sf.max('DATE_OF_RECEIPT_OF_AWARD_BY_TATAAIG').alias('DATE_OF_RECEIPT_OF_AWARD_BY_TATAAIG'),\
                        sf.max('INTERIM_AWARD_RECEIVED_BY_TAGIC_DATE').alias('INTERIM_AWARD_RECEIVED_BY_TAGIC_DATE'),\
                        sf.max('DATE_OF_RECEIPT_OF_AWARD_BY_ADVOCATE').alias('DATE_OF_RECEIPT_OF_AWARD_BY_ADVOCATE'),\
                        sf.max('DATE_OF_AWARD').alias('DATE_OF_AWARD'))

#sf.max('TP_AWARD').alias('TP_AWARD'),\
#sf.max('COSTS_PLAINTIFF').alias('COSTS_PLAINTIFF'),\

## join for all columns above

join_cond = [clm_22.NUM_CLAIM_NO == clm_tp_awd4.NUM_CLAIM_NO]

clm_38 = clm_22\
            .join(clm_tp_awd4, join_cond, "left_outer")\
            .drop(clm_tp_awd4.NUM_CLAIM_NO)

## join with gc_clm_tp_petition_dtls
#below logic is written in better way by satya - 20200504
clm_petition_temp = gc_clm_tp_petition_dtls\
                        .withColumn('check1', concat(trim(gc_clm_tp_petition_dtls.NUM_UPDATE_NO).cast('integer'),\
                                                    trim(gc_clm_tp_petition_dtls.NUM_SERIAL_NO).cast('integer')).cast('integer'))\
                        .drop('NUM_UPDATE_NO')\
                        .drop('NUM_SERIAL_NO')

clm_petition_temp1 = clm_petition_temp\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('check1').alias('check2'))

join_cond = [clm_petition_temp.NUM_CLAIM_NO == clm_petition_temp1.NUM_CLAIM_NO,\
             clm_petition_temp.check1 == clm_petition_temp1.check2]

clm_petition_temp2 = clm_petition_temp\
                        .join(clm_petition_temp1, join_cond, "inner")\
                        .drop(clm_petition_temp1.NUM_CLAIM_NO)\
                        .drop(clm_petition_temp1.check2)\
                        .drop(clm_petition_temp.check1)

join_cond = [clm_38.NUM_CLAIM_NO == clm_petition_temp2.NUM_CLAIM_NO]

clm_39_1 = clm_38\
            .join(clm_petition_temp2, join_cond, 'left_outer')\
            .drop(clm_petition_temp2.NUM_CLAIM_NO)

## join for LITIGATION_TYPE

clmmst_pet_venue1 = gc_clmmst_pet_venue_forum_view\
                                .groupBy('ITEM_VALUE')\
                                .agg(sf.max('ITEM_TEXT').alias('LITIGATION_TYPE'))

join_cond = [clm_39_1.LITIGATION_TYPE_CD == clmmst_pet_venue1.ITEM_VALUE]

clm_39_2 = clm_39_1\
            .join(clmmst_pet_venue1, join_cond, "left_outer")\
            .drop(clmmst_pet_venue1.ITEM_VALUE)


## join for COURT_NAME

claimmst_court = claimmst_court_base\
                                .groupBy('NUM_COURT_CODE')\
                                .agg(sf.max('TXT_COURT_NAME').alias('COURT_NAME'))

clm_39 = clm_39_2.join(claimmst_court, 'NUM_COURT_CODE', "left_outer")\
                 .drop('NUM_COURT_CODE')

# gc_clm_reopen.printSchema()

## join with gc_clm_reopen
#below logic is written in better way by satya - 20200504
clm_reopen_temp = gc_clm_reopen\
                        .withColumn('check1', concat(trim(gc_clm_reopen.NUM_UPDATE_NO).cast('integer'),\
                                                    trim(gc_clm_reopen.NUM_SERIAL_NO).cast('integer')).cast('integer'))\
                        .drop('NUM_UPDATE_NO')\
                        .drop('NUM_SERIAL_NO')

clm_reopen_temp1 = clm_reopen_temp\
                    .groupBy('NUM_CLAIM_NO')\
                    .agg(sf.max('check1').alias('check2'))

join_cond = [clm_reopen_temp.NUM_CLAIM_NO == clm_reopen_temp1.NUM_CLAIM_NO,\
             clm_reopen_temp.check1 == clm_reopen_temp1.check2]

clm_reopen_temp2 = clm_reopen_temp\
                        .join(clm_reopen_temp1, join_cond, "inner")\
                        .drop(clm_reopen_temp1.NUM_CLAIM_NO)\
                        .drop(clm_reopen_temp1.check2)\
                        .drop(clm_reopen_temp.check1)

join_cond = [clm_39.NUM_CLAIM_NO == clm_reopen_temp2.NUM_CLAIM_NO]

clm_40_1 = clm_39\
            .join(clm_reopen_temp2, join_cond, 'left_outer')\
            .drop(clm_reopen_temp2.NUM_CLAIM_NO)


# ## join with gc_clmmst_repudiation_reason

# join_cond = [clm_40_1.NUM_REASON_FOR_REOPENING == gc_clmmst_repudiation_reason.NUM_REPUDIATION_REASON_ID]

# clm_40 = clm_40_1\
#             .join(gc_clmmst_repudiation_reason, join_cond, "left_outer")\
#             .drop(gc_clmmst_repudiation_reason.NUM_REPUDIATION_REASON_ID)\
#             .drop(clm_40_1.NUM_REASON_FOR_REOPENING)

# clm_40 = clm_40\
#             .withColumnRenamed('TXT_REPUDIATION_REASON_DESC','REASON_FOR_REOPEN')


### join with gc_clmmst_generic_value

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 117)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('REASON_FOR_REOPEN'))

join_cond = [trim(clm_40_1.NUM_REASON_FOR_REOPENING) == gc_genvalue2.TXT_INFO1]

clm_40 = clm_40_1\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)


## join with genmst_tab_office -- Report Column - pol_iss_off_desc

join_cond = [clm_40.TXT_ISSUE_OFFICE_CD == genmst_tab_office.NUM_OFFICE_CD]

clm_41 = clm_40\
            .join(genmst_tab_office, join_cond, "left_outer")\
            .drop(genmst_tab_office.NUM_OFFICE_CD)\
            .withColumnRenamed('TXT_OFFICE', 'POL_ISS_OFF_DESC')

clm_41_1_1 = clm_41\
            .withColumn('PROD_OFF_SUB_TXT', clm_41.POL_ISS_OFF_DESC)

clm_41_1 = clm_41_1_1\
            .withColumn('NUM_PARENT_OFFICE_CD', when(lower(trim(clm_41_1_1.TXT_OFFICE_TYPE)) == 'bo', clm_41_1_1.NUM_PARENT_OFFICE_CD)\
                                                .otherwise(clm_41_1_1.TXT_ISSUE_OFFICE_CD))

clm_41_1.createOrReplaceTempView('clm_41_1_view')
genmst_tab_office.createOrReplaceTempView('genmst_tab_office_view')

query ="select a.*,b.TXT_OFFICE as PROD_OFF_TXT from clm_41_1_view a left join genmst_tab_office_view b on a.NUM_PARENT_OFFICE_CD = b.NUM_OFFICE_CD"

clm_41 = sqlContext.sql(query)
sqlContext.dropTempTable('clm_41_1_view')

clm_41.createOrReplaceTempView('clm_41_view')
genmst_tab_office.createOrReplaceTempView('genmst_tab_office_view')

query ="select a.*,b.TXT_OFFICE as SETTLING_OFF_DESC from clm_41_view a left join genmst_tab_office_view b on a.SETTLING_OFF_CD = b.NUM_OFFICE_CD"

clm_42_1 = sqlContext.sql(query)
sqlContext.dropTempTable('clm_41_view')

### join with gc_clmmst_generic_value

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 157)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('LOSS_DESC_42_55'))

join_cond = [trim(clm_42_1.GEN_TXT_INFO31) == gc_genvalue2.TXT_INFO1]

clm_42_2 = clm_42_1\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)
    
### join with gc_clmmst_generic_value

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 212)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('LOSS_DESC_21'))

join_cond = [trim(clm_42_2.GEN_TXT_INFO31) == gc_genvalue2.TXT_INFO1]

clm_42_3 = clm_42_2\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)


### join with gc_clmmst_generic_value

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 346)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('LOSS_DESC_61'))

join_cond = [trim(clm_42_3.GEN_TXT_INFO31) == gc_genvalue2.TXT_INFO1]

clm_42_4 = clm_42_3\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)

### join with gc_clmmst_generic_value

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 75)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('LOSS_DESC_ELSE'))

join_cond = [trim(clm_42_4.GEN_TXT_INFO31) == gc_genvalue2.TXT_INFO1]

clm_42_5 = clm_42_4\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)
##########################################################################################
# New code added for department code 62. Sample Claim No:0821239872A CWP May. added by Satya on 20191121
### join with gc_clmmst_generic_value

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 391)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('LOSS_DESC_62'))

join_cond = [trim(clm_42_5.GEN_TXT_INFO31) == gc_genvalue2.TXT_INFO1]

clm_42_6 = clm_42_5\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)
##########################################################################################
##########################################################################################
# New code added for department code 15. Sample Claim No:0821817070A CWP/Reopen
### join with gc_clmmst_generic_value

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 300)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('LOSS_DESC_15'))

join_cond = [trim(clm_42_6.GEN_TXT_INFO31) == gc_genvalue2.TXT_INFO1]

clm_42 = clm_42_6\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)
##########################################################################################

# clm_42.count()
# not checked

########
# added new column LOSS_DESC_62 to derive loss_desc for department code 62
#########
clm_42 = clm_42\
            .withColumn('LOSSDISTRICT', when((clm_42.NUM_DEPARTMENT_CODE == '14')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '15')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '17')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '27')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '43')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '47')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '55'), clm_42.GEN_TXT_INFO45)\
                                        .when(clm_42.NUM_DEPARTMENT_CODE == '42', clm_42.GEN_TXT_INFO49)\
                                        .when((clm_42.NUM_DEPARTMENT_CODE == '16')|\
                                              (clm_42.NUM_DEPARTMENT_CODE == '21')|\
                                              (clm_42.NUM_DEPARTMENT_CODE == '28')|\
                                              (clm_42.NUM_DEPARTMENT_CODE == '56'), clm_42.GEN_TXT_INFO51)\
                                        .otherwise(lit('')))\
            .withColumn('CLAIMANT_NAME', when((clm_42.NUM_DEPARTMENT_CODE == '14')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '15')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '17')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '27')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '43')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '47')|\
                                             (clm_42.NUM_DEPARTMENT_CODE == '55'), clm_42.GEN_TXT_INFO39)\
                                        .when(clm_42.NUM_DEPARTMENT_CODE == '42', clm_42.GEN_TXT_INFO43)\
                                        .when((clm_42.NUM_DEPARTMENT_CODE == '16')|\
                                              (clm_42.NUM_DEPARTMENT_CODE == '56'), clm_42.GEN_TXT_INFO44)\
                                        .when((clm_42.NUM_DEPARTMENT_CODE == '21')|\
                                              (clm_42.NUM_DEPARTMENT_CODE == '28'), clm_42.GEN_TXT_INFO45)\
                                        .when(clm_42.NUM_DEPARTMENT_CODE == '62', clm_42.GEN_TXT_INFO53)\
                                        .when(clm_42.NUM_DEPARTMENT_CODE == '61', clm_42.GEN_TXT_INFO55)\
                                        .otherwise(lit('')))\
            .withColumn('LOSS_DESC_TXT16', when(((clm_42.NUM_DEPARTMENT_CODE == '16')|\
                                               (clm_42.NUM_DEPARTMENT_CODE == '55')|\
                                               (clm_42.NUM_DEPARTMENT_CODE == '56')|\
                                               (clm_42.NUM_DEPARTMENT_CODE == '42'))&\
                                              (clm_42.GEN_TXT_INFO31 == '999')&\
                                              (clm_42.GEN_TXT_INFO37.isNotNull()), clm_42.GEN_TXT_INFO37)\
                                        .otherwise(clm_42.LOSS_DESC_42_55))\
            .withColumn('LOSS_DESC_TXT21', when((clm_42.NUM_DEPARTMENT_CODE == '21')&\
                                              (clm_42.GEN_TXT_INFO31 == '999')&\
                                              (clm_42.GEN_TXT_INFO37.isNotNull()), clm_42.GEN_TXT_INFO37)\
                                        .otherwise(clm_42.LOSS_DESC_21))\
            .withColumn('LOSS_DESC_TXT61', when((clm_42.NUM_DEPARTMENT_CODE == '61')&\
                                              (clm_42.GEN_TXT_INFO31 == '999')&\
                                              (clm_42.GEN_TXT_INFO37.isNotNull()), clm_42.GEN_TXT_INFO37)\
                                        .otherwise(clm_42.LOSS_DESC_61))\
            .withColumn('LOSS_DESC_TXT28', when((clm_42.NUM_DEPARTMENT_CODE == '28')&\
                                              (clm_42.GEN_TXT_INFO31 == '999')&\
                                              (clm_42.GEN_TXT_INFO37.isNotNull()), clm_42.GEN_TXT_INFO37)\
                                        .otherwise(clm_42.GEN_TXT_INFO31))\
            .withColumn('LOSS_DESC_TXT62', when((clm_42.NUM_DEPARTMENT_CODE == '62'), clm_42.LOSS_DESC_62)\
                                        .otherwise(lit('')))\
            .withColumn('LOSS_DESC_TXTELSE', when((clm_42.GEN_TXT_INFO31 == '999')&\
                                              (clm_42.GEN_TXT_INFO37.isNotNull()), clm_42.GEN_TXT_INFO37)\
                                        .otherwise(clm_42.LOSS_DESC_ELSE))\
            .withColumn('ADDRESS_1', when((clm_42.NUM_DEPARTMENT_CODE == '28'), clm_42.ADDRESS_1)\
                                    .otherwise(lit('')))\
            .withColumn('CERTIFICATE_NUMBER', when(clm_42.CERTIFICATE_NUMBER.isNull(), lit('000000'))\
                                            .otherwise(clm_42.CERTIFICATE_NUMBER))\
            .withColumn('PROD_SOURCE_CD', when(lower(trim(clm_42.TXT_RE_INSURANCE_INWARD)) == 'yes', lit('14'))\
                                            .when(lower(trim(clm_42.TXT_RE_INSURANCE_INWARD)) == 'no', lit('12'))\
                                            .otherwise(lit('')))
##Change on 20210108
clm_42 = clm_42\
            .withColumn('CLAIMANT_NAME', when(((clm_42.NUM_DEPARTMENT_CODE == '28')&\
                                               ((clm_42.CLAIMANT_NAME.isNull())|\
                                                (trim(clm_42.CLAIMANT_NAME)==''))), clm_42.GEN_TXT_INFO80)\
                        .otherwise(clm_42.CLAIMANT_NAME))\
            .withColumnRenamed('TXT_RE_INSURANCE_INWARD','REINSURANCE_DESC')

##above when condition for 12 was added on 20190609
## gc_clmmst_hospital (gc_clmmst_hospital)
gscPythonOptions = {
         "url": url_prod,
         "user": user_prod,
         "password": pwd_prod,
         "dbschema": dbschema_etl,
         "dbtable": "reference_gc_gc_clmmst_hospital",
         "partitionColumn":"row_num",
         "server.port":server_port,
         "partitions":1} 
clm_hospital = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TXT_HOSPITAL_NAME','TXT_HOSPITAL_ADDRESS')

clm_hospital_1 = clm_hospital.groupBy('TXT_HOSPITAL_NAME').agg(sf.max('TXT_HOSPITAL_ADDRESS').alias('TXT_HOSPITAL_ADDRESS'))
clm_hospital_1 = clm_hospital_1.withColumnRenamed('TXT_HOSPITAL_NAME', 'HOSPITAL_NAME')

clm_42 = clm_42.join(clm_hospital_1, 'HOSPITAL_NAME', 'left')
clm_42 = clm_42.drop('ADDRESS_1')
clm_42 = clm_42.withColumn('ADDRESS_1', when((clm_42.NUM_DEPARTMENT_CODE == '28'), clm_42.TXT_HOSPITAL_ADDRESS)\
                                    .otherwise(lit('')))\
                .drop('TXT_HOSPITAL_ADDRESS')
########################################################

clm_42 = clm_42\
            .withColumn('LOSS_DESC_TXT', when((clm_42.NUM_DEPARTMENT_CODE == '16')|\
                                               (clm_42.NUM_DEPARTMENT_CODE == '55')|\
                                               (clm_42.NUM_DEPARTMENT_CODE == '56')|\
                                               (clm_42.NUM_DEPARTMENT_CODE == '42'), clm_42.LOSS_DESC_TXT16)\
                                        .when(clm_42.NUM_DEPARTMENT_CODE == '21', clm_42.LOSS_DESC_TXT21)\
                                        .when(clm_42.NUM_DEPARTMENT_CODE == '61', clm_42.LOSS_DESC_TXT61)\
                                        .when(clm_42.NUM_DEPARTMENT_CODE == '28', clm_42.LOSS_DESC_TXT28)\
                                        .when((clm_42.NUM_DEPARTMENT_CODE == '27')|\
                                               (clm_42.NUM_DEPARTMENT_CODE == '17'), clm_42.GEN_TXT_INFO31)\
                                        .when(clm_42.NUM_DEPARTMENT_CODE == '14', clm_42.GEN_TXT_INFO51)\
                                        .when(clm_42.NUM_DEPARTMENT_CODE == '62', clm_42.LOSS_DESC_TXT62)\
                                        .when(clm_42.NUM_DEPARTMENT_CODE == '15', clm_42.LOSS_DESC_15)\
                                        .otherwise(clm_42.LOSS_DESC_TXTELSE))

clm_42 = clm_42\
            .drop('LOSS_DESC_TXT16')\
            .drop('LOSS_DESC_TXT21')\
            .drop('LOSS_DESC_TXT61')\
            .drop('LOSS_DESC_TXT28')\
            .drop('LOSS_DESC_TXT31')\
            .drop('LOSS_DESC_TXT51')\
            .drop('LOSS_DESC_TXT62','LOSS_DESC_15')\
            .drop('LOSS_DESC_TXTELSE')

### join with cnfgtr_user_dtls

user_dtls1 = cnfgtr_user_dtls\
                .withColumn('USERID', trim(cnfgtr_user_dtls.USERID))

user_dtls2 = user_dtls1\
            .groupBy('USERID')\
            .agg(sf.max('USERNAME').alias('USER_NAME'),\
                 sf.max('CODE').alias('CODE'))

join_cond = [clm_42.TXT_USER_ID == user_dtls2.USERID]

clm_42_101 = clm_42\
            .join(user_dtls2, join_cond, "left_outer")\
            .drop(user_dtls2.USERID)

### join with genmst_employee

genmst_employee1 = genmst_employee\
                            .groupBy('NUM_EMPLOYEE_CD')\
                            .agg(sf.max('TXT_HR_REF_NO').alias('USER_EMP_CD'))

join_cond = [clm_42_101.CODE == genmst_employee1.NUM_EMPLOYEE_CD]

clm_43 = clm_42_101\
            .join(genmst_employee1, join_cond, "left_outer")\
            .drop(genmst_employee1.NUM_EMPLOYEE_CD)\
            .drop(clm_42_101.CODE)

## to populate catastrophe against each claim irrespective of feature
clm_43_tmp = clm_43.select(col('CLAIM_NO').alias('CLAIM_NO'),'NUM_CATASTROPHE_CD')
clm_43_tmp_01 = clm_43_tmp\
                    .groupBy('CLAIM_NO')\
                    .agg(sf.max('NUM_CATASTROPHE_CD').alias('NUM_CATASTROPHE_CD_TMP'))

clm_43_tmp_02 = clm_43.join(clm_43_tmp_01, 'CLAIM_NO', 'left')
clm_43 = clm_43_tmp_02\
                .withColumn('NUM_CATASTROPHE_CD', \
                            when(((clm_43_tmp_02.NUM_CATASTROPHE_CD.isNull())|(clm_43_tmp_02.NUM_CATASTROPHE_CD=='')), clm_43_tmp_02.NUM_CATASTROPHE_CD_TMP)
                                .otherwise(clm_43_tmp_02.NUM_CATASTROPHE_CD))\
                .drop('NUM_CATASTROPHE_CD_TMP')

clm_43 = clm_43\
            .withColumnRenamed('NUM_CATASTROPHE_CD','LCL_CATASTROPHE_NO')

## join for TXT_CATASTROPHE_DESC, CATASTROPHE_TYPE
clmmst_catestrophy1 = gc_clmmst_catestrophy\
                                .groupBy('LCL_CATASTROPHE_NO')\
                                .agg(sf.max('TXT_CATASTROPHE_DESC').alias('TXT_CATASTROPHE_DESC'),\
                                     sf.max('CATASTROPHE_TYPE_CD').alias('CATASTROPHE_TYPE_CD'))

clm_43_01 = clm_43.join(clmmst_catestrophy1, ['LCL_CATASTROPHE_NO'], "left_outer")
clm_43 = clm_43_01\
            .withColumn('CATASTROPHE_TYPE', \
                        when(clm_43_01.CATASTROPHE_TYPE_CD=='1', lit('Local'))\
                        .when(clm_43_01.CATASTROPHE_TYPE_CD=='2', lit('International'))\
                             .otherwise(lit(None)))\
            .drop('CATASTROPHE_TYPE_CD')

### join with gc_clmmst_surveytype

gc_surveytype1 = gc_clmmst_surveytype\
                            .groupBy('NUM_SURVEY_TYPE','NUM_DEPARTMENT_CODE','NUM_PRODUCT_CODE')\
                            .agg(sf.max('TXT_SUVEY_TYPE_DESC').alias('TYPE_OF_SURVEY'))

join_cond = [clm_43.NUM_SURVEY_TYPE == gc_surveytype1.NUM_SURVEY_TYPE,\
             clm_43.NUM_DEPARTMENT_CODE == gc_surveytype1.NUM_DEPARTMENT_CODE,\
             clm_43.PRODUCT_CD == gc_surveytype1.NUM_PRODUCT_CODE]

clm_44 = clm_43\
            .join(gc_surveytype1, join_cond, "left_outer")\
            .drop(gc_surveytype1.NUM_SURVEY_TYPE)\
            .drop(gc_surveytype1.NUM_DEPARTMENT_CODE)\
            .drop(gc_surveytype1.NUM_PRODUCT_CODE)

clm_44_A = clm_44\
            .withColumn('NUM_DEPARTMENT_CODE_ELSE', when((clm_44.TYPE_OF_SURVEY.isNull())|(clm_44.TYPE_OF_SURVEY==''), lit("99"))\
                        .otherwise(clm_44.NUM_DEPARTMENT_CODE))\
            .withColumn('PRODUCT_CD_ELSE', when((clm_44.TYPE_OF_SURVEY.isNull())|(clm_44.TYPE_OF_SURVEY==''), lit("99"))\
                        .otherwise(clm_44.PRODUCT_CD))

gc_surveytype1 = gc_surveytype1.withColumnRenamed("TYPE_OF_SURVEY","TYPE_OF_SURVEY_ELSE")

join_cond = [clm_44_A.NUM_SURVEY_TYPE == gc_surveytype1.NUM_SURVEY_TYPE,\
             clm_44_A.NUM_DEPARTMENT_CODE_ELSE == gc_surveytype1.NUM_DEPARTMENT_CODE,\
             clm_44_A.PRODUCT_CD_ELSE == gc_surveytype1.NUM_PRODUCT_CODE]

clm_44_B = clm_44_A\
            .join(gc_surveytype1, join_cond, "left_outer")\
            .drop(gc_surveytype1.NUM_SURVEY_TYPE)\
            .drop(gc_surveytype1.NUM_DEPARTMENT_CODE)\
            .drop(gc_surveytype1.NUM_PRODUCT_CODE)\
            .drop(clm_44_A.NUM_DEPARTMENT_CODE_ELSE)\
            .drop(clm_44_A.PRODUCT_CD_ELSE)

clm_44 = clm_44_B\
            .withColumn('TYPE_OF_SURVEY', when((clm_44_B.TYPE_OF_SURVEY.isNull())|(clm_44_B.TYPE_OF_SURVEY==''), clm_44_B.TYPE_OF_SURVEY_ELSE)\
                        .otherwise(clm_44_B.TYPE_OF_SURVEY))

### join with gc_clmmst_generic_value

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 77)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO2').alias('MARKET_SECMENTATION'))

join_cond = [trim(clm_44.MARKET_SECMENTATION_CD) == gc_genvalue2.TXT_INFO1]

clm_45_1 = clm_44\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)\
            .drop(clm_44.MARKET_SECMENTATION_CD)

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 350)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO5', trim(gc_genvalue1.TXT_INFO5))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO5')\
                    .agg(sf.max('TXT_INFO3').alias('ISSUING_OFFICE'))

join_cond = [trim(clm_45_1.ISSUING_OFFICE_CD) == gc_genvalue2.TXT_INFO5]

clm_45_2 = clm_45_1\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO5)\
            .drop(clm_45_1.ISSUING_OFFICE_CD)



gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 349)

gc_genvalue1 = gc_genvalue1\
                    .withColumn('TXT_INFO1', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1\
                    .groupBy('TXT_INFO1')\
                    .agg(sf.max('TXT_INFO3').alias('ISSUING_COMAPNY'))

join_cond = [trim(clm_45_2.ISSUING_COMAPNY_CD) == gc_genvalue2.TXT_INFO1]

clm_45 = clm_45_2\
            .join(gc_genvalue2, join_cond, "left_outer")\
            .drop(gc_genvalue2.TXT_INFO1)\
            .drop(clm_45_2.ISSUING_COMAPNY_CD)

clm_46 = clm_45

### join with cnfgtr_user_dtls

user_dtls1 = cnfgtr_user_dtls\
                .withColumn('CODE', trim(cnfgtr_user_dtls.CODE))

user_dtls2 = user_dtls1\
            .groupBy('CODE')\
            .agg(sf.max('USERID').alias('EXAMINER_USER_ID'))

join_cond = [trim(clm_46.EXAMINER_USER_ID_CD) == user_dtls2.CODE]

clm_47_1 = clm_46\
            .join(user_dtls2, join_cond, "left_outer")\
            .drop(user_dtls2.CODE)


### join with genmst_employee

genmst_employee1 = genmst_employee\
                            .groupBy('NUM_EMPLOYEE_CD')\
                            .agg(sf.max('TXT_HR_REF_NO').alias('EXAMINER_EMP_CODE'))

join_cond = [clm_47_1.EXAMINER_USER_ID_CD == genmst_employee1.NUM_EMPLOYEE_CD]

clm_47 = clm_47_1\
            .join(genmst_employee1, join_cond, "left_outer")\
            .drop(genmst_employee1.NUM_EMPLOYEE_CD)

### join with cnfgtr_policy_ld_dtls

policy_ld_dtls1 = cnfgtr_policy_ld_dtls\
                        .filter(lower(trim(cnfgtr_policy_ld_dtls.LDDESC)) == 'no claim bonus')

policy_ld_dtls2 = policy_ld_dtls1\
                        .groupBy('REFERENCE_NUM')\
                        .agg(sf.max('LD_RATE').alias('NCB'))

join_cond = [clm_47.NUM_REFERENCE_NO == policy_ld_dtls2.REFERENCE_NUM]

clm_48 = clm_47\
            .join(policy_ld_dtls2, join_cond, "left_outer")\
            .drop(policy_ld_dtls2.REFERENCE_NUM)

### join with gc_clm_claim_recovery

claim_recovery = gc_clm_claim_recovery_base\
                        .filter((lower(trim(gc_clm_claim_recovery_base.TXT_TYPE_OF_RECOVERY)) == 'cc')&\
                                (gc_clm_claim_recovery_base.NUM_FORWARD_VOUCHER_NO.isNotNull()))

claim_recovery1 = claim_recovery\
                        .groupBy('NUM_CLAIM_NO','NUM_FORWARD_VOUCHER_NO')\
                        .agg(sf.max('CUR_RECOVERY_AMOUNT').alias('CASH_COLLECTION'))


join_cond = [clm_48.NUM_CLAIM_NO == claim_recovery1.NUM_CLAIM_NO,\
             clm_48.NUM_TRANSACTION_CONTROL_NO == claim_recovery1.NUM_FORWARD_VOUCHER_NO]

clm_49 = clm_48\
            .join(claim_recovery1, join_cond, 'left_outer')\
            .drop(claim_recovery1.NUM_CLAIM_NO)\
            .drop(claim_recovery1.NUM_FORWARD_VOUCHER_NO)

clm_all_1 = clm_49\
                .withColumn('CWP_DATE_REF',to_date(clm_49.CWP_DATE_REF, format='yyyy-MM-dd'))\
                .withColumn('CWP_REOPEN_DATE',to_date(clm_49.CWP_REOPEN_DATE, format='yyyy-MM-dd'))\
                .withColumn('DAT_REGISTRATION_DATE',to_date(clm_49.DAT_REGISTRATION_DATE, format='yyyy-MM-dd'))\
                .withColumn('SURVEYOR_LICENSE_EXPIRY_DATE',to_date(clm_49.SURVEYOR_LICENSE_EXPIRY_DATE, format='yyyy-MM-dd'))\
                .withColumn('DAT_DATE_OF_FILING_OF_PETITION',to_date(clm_49.DAT_DATE_OF_FILING_OF_PETITION, format='yyyy-MM-dd'))\
                .withColumn('DAT_DATE_OF_HEARING',to_date(clm_49.DAT_DATE_OF_HEARING, format='yyyy-MM-dd'))

clm_all_1.createOrReplaceTempView('clm_all_1_view')

query = """select *,
cast(cast(NUM_TYPE_OF_PARTY as decimal) as string) NUM_TYPE_OF_PARTY_B,
cast(cast(SURVEYORCODE as decimal) as string) SURVEYORCODE_B,
cast(cast(NUM_SURVEY_TYPE as decimal) as string) NUM_SURVEY_TYPE_B,
cast(cast(NUM_DEPARTMENT_CODE as decimal) as string) NUM_DEPARTMENT_CODE_B,
cast(cast(COVERAGE_CODE as decimal) as string) COVERAGE_CODE_B
from clm_all_1_view"""
clm_all_2 = sqlContext.sql(query)
sqlContext.dropTempTable('clm_all_1_view')

clm_all_2 = clm_all_2\
            .drop('NUM_TYPE_OF_PARTY')\
            .drop('SURVEYORCODE')\
            .drop('NUM_SURVEY_TYPE')\
            .drop('NUM_DEPARTMENT_CODE')\
            .drop('COVERAGE_CODE')

clm_all_2 = clm_all_2\
            .withColumnRenamed('NUM_TYPE_OF_PARTY_B','NUM_TYPE_OF_PARTY')\
            .withColumnRenamed('SURVEYORCODE_B','SURVEYORCODE')\
            .withColumnRenamed('NUM_SURVEY_TYPE_B','NUM_SURVEY_TYPE')\
            .withColumnRenamed('NUM_DEPARTMENT_CODE_B','NUM_DEPARTMENT_CODE')\
            .withColumnRenamed('COVERAGE_CODE_B','COVERAGE_CODE')

clm_all_2 = clm_all_2\
                .withColumn('REOPEN_STATUS',\
                            when(clm_all_2.REPORT == 'reopen', clm_all_2.CLAIM_STATUS_CD)\
                            .otherwise(lit('')))\
                .withColumn('BASIC_TP',\
                            when(clm_all_2.ACCT_LINE_CD == '50', clm_all_2.BASIC_TP)\
                            .otherwise(lit('')))\
                .withColumn('TP_AWARD',\
                            when(clm_all_2.ACCT_LINE_CD == '50', clm_all_2.TP_AWARD)\
                            .otherwise(lit('')))\
                .withColumn('COSTS_PLAINTIFF',\
                            when(clm_all_2.ACCT_LINE_CD == '50', clm_all_2.COSTS_PLAINTIFF)\
                            .otherwise(lit('')))\
                .withColumn('CLAIMSPROXIMITY',\
                            when((trim(lower(clm_all_2.CLAIMSPROXIMITY)) != 'y')|\
                                 (clm_all_2.CLAIMSPROXIMITY.isNull()), lit('N'))\
                            .otherwise(clm_all_2.CLAIMSPROXIMITY))
#                 .withColumn('CHEQUE_NO',\
#                             when((clm_all_2.TRANS_TYPE_CD == '21')|\
#                                  (clm_all_2.TRANS_TYPE_CD == '22')|\
#                                  (clm_all_2.TRANS_TYPE_CD == '23'), clm_all_2.CHEQUE_NO)\
#                             .otherwise(lit('')))
#                 .withColumn('CBC_RECEIPT_NUMBER_STATUS', lit(''))\
#                 .withColumn('CBC_RECEIPT_CREATION_DATE', lit(''))\
#                 .withColumn('CHEQUE_BOUNCE', lit(''))

# 20190723 - updated cheque_no condition above - previously if TRANS_TYPE_CD =22 then cheque_no. now if TRANS_TYPE_CD = 21,22,23 then cheque_no.
# ################################################# RI Claims Logic ##########################################################
# ## ri_mm_inward_claim_details (ri_mm_inward_claim_details)
# gscPythonOptions = {
#          "url": url_prod,
#          "user": user_prod,
#          "password": pwd_prod,
#          "dbschema": dbschema_etl,
#          "dbtable": "policy_gc_ri_mm_inward_claim_details",
#          "partitionColumn":"row_num",
#          "server.port":server_port,
#          "partitions":1} 
# ri_claim_dtls = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
# .select('NUM_CLAIM_NUMBER','TXT_MASTER_CLAIM_NO_NEW','TXT_POLICY_NO_CHAR')

# ri_claim_dtls_01 = ri_claim_dtls\
#                         .groupBy('NUM_CLAIM_NUMBER')\
#                         .agg(sf.max('TXT_MASTER_CLAIM_NO_NEW').alias('TXT_MASTER_CLAIM_NO_NEW'),\
#                              sf.max('TXT_POLICY_NO_CHAR').alias('TXT_POLICY_NO_CHAR'))

# ri_claim_dtls_01 = ri_claim_dtls_01.withColumn('policy_length', LengthOfString(ri_claim_dtls_01.TXT_POLICY_NO_CHAR))

# ri_claim_dtls_02 = ri_claim_dtls_01\
#                         .withColumn('CLAIM_FEATURE_CONCAT_RI', ri_claim_dtls_01.TXT_MASTER_CLAIM_NO_NEW)\
#                         .withColumn('CLAIM_NO_RI', firstTenChar(ri_claim_dtls_01.TXT_MASTER_CLAIM_NO_NEW))\
#                         .withColumn('CLAIM_FEATURE_NO_RI', afterTenChar(ri_claim_dtls_01.TXT_MASTER_CLAIM_NO_NEW))\
#                         .withColumnRenamed('NUM_CLAIM_NUMBER','NUM_CLAIM_NO')\
#                         .withColumn('POLICY_NO_RI', \
#                         when(ri_claim_dtls_01.policy_length > 10, firstTenChar(ri_claim_dtls_01.TXT_POLICY_NO_CHAR))\
#                                         .otherwise(ri_claim_dtls_01.TXT_POLICY_NO_CHAR))\
#                         .withColumn('RI_INWARD_FLAG', lit('Y'))\
#                         .drop('TXT_MASTER_CLAIM_NO_NEW','policy_length','TXT_POLICY_NO_CHAR')

# clm_all_2_tmp = clm_all_2.join(ri_claim_dtls_02,'NUM_CLAIM_NO','left_outer')
# clm_all_2_tmp_01 = clm_all_2_tmp\
#                         .withColumn('CLAIM_FEATURE_CONCAT', \
#                                     when(((trim(clm_all_2_tmp.CLAIM_FEATURE_CONCAT).isNull())|\
#                                               (trim(clm_all_2_tmp.CLAIM_FEATURE_CONCAT)=='')), clm_all_2_tmp.CLAIM_FEATURE_CONCAT_RI)\
#                                     .otherwise(clm_all_2_tmp.CLAIM_FEATURE_CONCAT))\
#                         .withColumn('POLICY_NO', \
#                                     when(((trim(clm_all_2_tmp.POLICY_NO).isNull())|\
#                                               (trim(clm_all_2_tmp.POLICY_NO)=='')), clm_all_2_tmp.POLICY_NO_RI)\
#                                     .otherwise(clm_all_2_tmp.POLICY_NO))\
#                         .withColumn('CLAIM_NO', \
#                                     when(((trim(clm_all_2_tmp.CLAIM_NO).isNull())|\
#                                               (trim(clm_all_2_tmp.CLAIM_NO)=='')), clm_all_2_tmp.CLAIM_NO_RI)\
#                                     .otherwise(clm_all_2_tmp.CLAIM_NO))\
#                         .withColumn('CLAIM_FEATURE_NO', \
#                                     when(((trim(clm_all_2_tmp.CLAIM_FEATURE_NO).isNull())|\
#                                               (trim(clm_all_2_tmp.CLAIM_FEATURE_NO)=='')), clm_all_2_tmp.CLAIM_FEATURE_NO_RI)\
#                                     .otherwise(clm_all_2_tmp.CLAIM_FEATURE_NO))\
#                         .withColumn('RI_INWARD_FLAG', \
#                                     when(((trim(clm_all_2_tmp.RI_INWARD_FLAG).isNull())|\
#                                               (trim(clm_all_2_tmp.RI_INWARD_FLAG)=='')), lit('N'))\
#                                     .otherwise(clm_all_2_tmp.RI_INWARD_FLAG))\
#                         .drop('CLAIM_FEATURE_CONCAT_RI','POLICY_NO_RI','CLAIM_NO_RI','CLAIM_FEATURE_NO_RI')
# clm_all_2_tmp_02 = clm_all_2_tmp_01\
#                         .filter(~((clm_all_2_tmp_01.CLAIM_FEATURE_CONCAT.isNull())|\
#                                 (trim(clm_all_2_tmp_01.CLAIM_FEATURE_CONCAT)=='')))
# clm_all_2 = clm_all_2_tmp_02
# ################################################# RI Claims Logic ##########################################################

# sqlContext.dropTempTable('gc_clmmst_surveytype2_view')
# sqlContext.dropTempTable('country_master_view')
# sqlContext.dropTempTable('genmst_tab_office_view')
# sqlContext.dropTempTable('gc_clm_transaction_history_view')
# sqlContext.dropTempTable('surveyor_table')
# sqlContext.dropTempTable('clm_dairy_temp_view')
# sqlContext.dropTempTable('clm_dairy_1_temp_view')
# sqlContext.dropTempTable('clm_sett_info_temp_view')
# sqlContext.dropTempTable('clm_sett_info_view')
# sqlContext.dropTempTable('citydistrict_view')
# sqlContext.dropTempTable('clm_41_1_view')
# sqlContext.dropTempTable('clm_41_view')
# sqlContext.dropTempTable('clm_all_1_view')

# clm_all_2.count()

# clm_all_2.show()

# clm_all_2 = clm_all_2.drop('CHEQUE_BOUNCE')

#######
# ITD_GROSS_PAID_AMOUNT_REOPEN new column for REOPEN report to show ITD paid amount before reopen. by Sagar on 20191121
# read required columns from transaction_history table and filter payment records for calculation which qualifies till report_date
gc_clm_transaction_history_reopen = gc_clm_transaction_history1[['NUM_CLAIM_NO','DAT_TRANSACTIONDATE','TXT_TRANSACTIONTYPE','TXT_ACCOUNTLINE','NUM_TRANSACTIONAMOUNT','NUM_ISACTIVE','TXT_WITHHOLDINGTAX']]
trans_hist_reopen_fltr = gc_clm_transaction_history_reopen\
                               .filter(((gc_clm_transaction_history_reopen.TXT_TRANSACTIONTYPE=='Final Payment')|\
                                        (gc_clm_transaction_history_reopen.TXT_TRANSACTIONTYPE=='Partial Payment')|\
                                        (gc_clm_transaction_history_reopen.TXT_TRANSACTIONTYPE=='Payment After Closing')|\
                                        (gc_clm_transaction_history_reopen.TXT_TRANSACTIONTYPE=='Payment Voiding')|\
                                        (gc_clm_transaction_history_reopen.TXT_TRANSACTIONTYPE=='Payment Listing'))&\
                                       (gc_clm_transaction_history_reopen.NUM_ISACTIVE == '1')&\
                                       (gc_clm_transaction_history_reopen.DAT_TRANSACTIONDATE <= report_date))

trans_hist_reopen_fltr_1 = trans_hist_reopen_fltr\
                                .withColumn('ACCT_LINE_CD', \
                                             when(upper(trim(trans_hist_reopen_fltr.TXT_ACCOUNTLINE))== 'INDEMNITY', '50')\
                                             .when(upper(trim(trans_hist_reopen_fltr.TXT_ACCOUNTLINE))== 'EXPENSE', '55')\
                                             .otherwise(lit('')))

clm_all_reopen = clm_all_2.filter(clm_all_2.REPORT == 'reopen').select('NUM_CLAIM_NO', 'DAT_TRANS_DATE', 'ACCT_LINE_CD')

trans_hist_reopen_1 = clm_all_reopen\
                            .join(trans_hist_reopen_fltr_1, ['NUM_CLAIM_NO','ACCT_LINE_CD'])\
                            .drop('TXT_WITHHOLDINGTAX')

# filter paid transactions done before reopening of the claim
trans_hist_reopen_2 = trans_hist_reopen_1\
                            .withColumn('TRANSACTION_BEFORE_REOPEN', \
                                        when(trans_hist_reopen_1.DAT_TRANSACTIONDATE < trans_hist_reopen_1.DAT_TRANS_DATE, 'Y')\
                                        .otherwise('N'))\

trans_hist_reopen_3 = trans_hist_reopen_2.filter(trans_hist_reopen_2.TRANSACTION_BEFORE_REOPEN=='Y')
trans_hist_reopen_4 = trans_hist_reopen_3.groupBy('NUM_CLAIM_NO', 'ACCT_LINE_CD')\
                                         .agg(sf.sum(trans_hist_reopen_3.NUM_TRANSACTIONAMOUNT).alias('ITD_GROSS_PAID_AMOUNT_REOPEN'))

clm_all_3 = clm_all_2.join(trans_hist_reopen_4, ['NUM_CLAIM_NO','ACCT_LINE_CD'], 'left')

##YTD_GROSS
if report_date.month < 4:
    fy_start_date = report_date.replace(year = report_date.year - 1, month = 4, day = 1)
else:
    fy_start_date = report_date.replace(month = 4, day = 1)

trans_hist_ytd = trans_hist_reopen_fltr_1.filter(trans_hist_reopen_fltr_1.DAT_TRANSACTIONDATE >= fy_start_date)
trans_hist_ytd_01 = trans_hist_ytd.groupBy('NUM_CLAIM_NO', 'ACCT_LINE_CD')\
                                  .agg(sf.sum(trans_hist_ytd.NUM_TRANSACTIONAMOUNT).alias('YTD_GROSS_PAID_AMOUNT'),\
                                       sf.sum(trans_hist_ytd.TXT_WITHHOLDINGTAX).alias('TAX'))
trans_hist_ytd_01 = trans_hist_ytd_01\
                        .withColumn('YTD_GROSS_PAID_AMOUNT', trans_hist_ytd_01.YTD_GROSS_PAID_AMOUNT-trans_hist_ytd_01.TAX)\
                        .drop('TAX')
clm_all_4 = clm_all_3.join(trans_hist_ytd_01, ['NUM_CLAIM_NO','ACCT_LINE_CD'], 'left')

##ITD_GROSS Logic by satya on 20190830
##ITD_NET logic in p2 as NET_PCT derived in p2

# # gc_clm_payment_details1 = gc_clm_payment_details1.filter((StringToDateFunc(gc_clm_payment_details1.DAT_INSERT_DATE)) <= report_date)
# gc_clm_payment_details_01 = gc_clm_payment_details1.filter(lower(trim(gc_clm_payment_details1.TXT_STATUS))=='paid')

# itd_gross_01 = gc_clm_payment_details_01.groupBy('NUM_CLAIM_NO')\
#                                         .agg(sf.sum('CUR_APPROVER_RECOM_AMOUNT').alias('INDEMNITY_ITD_GROSS_TMP'),\
#                                             sf.sum('CUR_TOT_EXP_CLAIMED_AMT').alias('EXPENSE_ITD_GROSS_TMP'),\
#                                             sf.sum('CUR_PROFFESIONAL_FEE').alias('PROFFESIONAL_FEE'))

# clm_all_5 = clm_all_4.join(itd_gross_01,['NUM_CLAIM_NO'],'left_outer')

# clm_all_6 = clm_all_5.withColumn('ITD_GROSS_PAID_AMOUNT',\
#                                 when(clm_all_5.ACCT_LINE_CD == '50', clm_all_5.INDEMNITY_ITD_GROSS_TMP)\
#                                 .when(clm_all_5.ACCT_LINE_CD == '55', clm_all_5.EXPENSE_ITD_GROSS_TMP))\
#                                 .drop('INDEMNITY_ITD_GROSS_TMP','EXPENSE_ITD_GROSS_TMP','PROFFESIONAL_FEE')\
#                                 .withColumn('LOAD_DATE', lit(datetime.date.today()))

##ITD_GROSS Logic by satya on 20210108
trans_hist_itd_01 = trans_hist_reopen_fltr_1.groupBy('NUM_CLAIM_NO', 'ACCT_LINE_CD')\
                                  .agg(sf.sum(trans_hist_reopen_fltr_1.NUM_TRANSACTIONAMOUNT).alias('ITD_GROSS_PAID_AMOUNT'),\
                                       sf.sum(trans_hist_ytd.TXT_WITHHOLDINGTAX).alias('TAX'))
trans_hist_itd_01 = trans_hist_itd_01\
                        .withColumn('ITD_GROSS_PAID_AMOUNT', trans_hist_itd_01.ITD_GROSS_PAID_AMOUNT-trans_hist_itd_01.TAX)\
                        .drop('TAX')
clm_all_5 = clm_all_4.join(trans_hist_itd_01, ['NUM_CLAIM_NO','ACCT_LINE_CD'], 'left')
clm_all_6 = clm_all_5.withColumn('LOAD_DATE', lit(datetime.date.today()))

clm_all = clm_all_6.drop('NUM_STATE_CD_PC','DAT_REGISTRATION_DATE','NUM_PROXIMITY_DAYS','NUM_LOSS_LOCATION_CD','NUM_PIN_CD','TXT_INFO3','GEN_TXT_INFO31','GEN_TXT_INFO37','GEN_TXT_INFO39','GEN_TXT_INFO43','GEN_TXT_INFO44','GEN_TXT_INFO45','GEN_TXT_INFO46','TXT_INFO47','GEN_TXT_INFO49','GEN_TXT_INFO50','GEN_TXT_INFO51','GEN_TXT_INFO52','GEN_TXT_INFO53','GEN_TXT_INFO55','GEN_TXT_INFO80','TXT_ACCOUNTLINE','ACCT_CD','PAYMENT_TYPE_VOID','TRANS_TYPE_CD_VOID','SURVEYOR_INVESTIGATOR','POLICY_NO_GP','TXT_ADDRESS_LINE_1','TXT_ADDRESS_LINE_2','TXT_ADDRESS_LINE_3','NUM_STATE_CD','NUM_CITYDISTRICT_CD','TXT_STATE','LOSSCITY_CIDS','LOSSCITY_PC','STATE_OF_LOSS_PC','LOSSCOUNTRY_CD_56','LOSSCOUNTRY_CD_16','PAYMENT_TYPE16','PAYMENT_TYPE56','TREATMENTTYPE16','TREATMENTTYPE56','LOSSCITY_CD','ICD_MAJOR_DATA','ICD_MINOR_DATA','LOSSCITY_SI','DAT_DATE_OF_FILING_OF_PETITION','DAT_DATE_OF_HEARING','NUM_COURT_LOCATION','NUM_REASON_FOR_REOPENING','TXT_OFFICE_TYPE','NUM_PARENT_OFFICE_CD','LOSS_DESC_42_55','LOSS_DESC_21','LOSS_DESC_61','LOSS_DESC_ELSE','LOSS_DESC_62')
## Total 218 columns in printSchema() and 219 in p1 base table.

## Below block taking from metric,, thus not required here
# s=-1
# def ranged_numbers(anything): 
#     global s 
#     if s >= 47:
#         s = 0
#     else:
#         s = s+1 
#     return s

# udf_ranged_numbers = udf(lambda x: ranged_numbers(x), IntegerType())
# clm_all_6 = clm_all_6.withColumn("ROW_NUM", lit(udf_ranged_numbers(lit(1))))

# clm_all_6.printSchema()

# clm_all = clm_all_6[['ACCT_LINE_CD','CLAIM_FACULTATIVE_76','CLAIM_FEATURE_CONCAT','CLAIM_FEATURE_NO','CLAIM_GROSS','CLAIM_NET',\
# 'CLAIM_NET_0','CLAIM_NO','CLAIM_OBLIGATORY_75','CLAIM_TREATY_77','CLAIM_XOL_RECOVERY_78','COINSURANCE_CD',\
# 'COINSURANCE_DESC','COINSURANCE_PCT','DAT_REFERENCE_DATE','DAT_TRANSACTION_DATE','DAT_TRANS_DATE','DAT_VOUCHER_DATE',\
# 'ITD_GROSS_PAID_AMOUNT','NUM_CLAIM_NO','NUM_REFERENCE_NO','NUM_RI_TREATY_NO','NUM_TRANSACTION_CONTROL_NO',\
# 'OTHER_RECOVERY','POL_ISS_OFF_CD','PRODUCT_CD','RECEIPT_DETAILS','REIN_TYPE_CD','REPORT','SALVAGE_AMOUNT',\
# 'SALVAGE_BUYER_ADDRESS','SALVAGE_BUYER_NAME','SALVAGE_BUYER_PAN','SALVAGE_BUYER_STATE','SUBROGATION_AMT',\
# 'TRANS_TYPE_CD','TXT_ISSUE_OFFICE_CD','TXT_MMCP_CODE','YTD_GROSS_PAID_AMOUNT',\
# 'LOSS_DATE','LOSS_REPORTED_DATE','CLM_CREATE_DATE','CATASTROPHE_TYPE','DATEDIFFERENCE','INSURED_NAME',\
# 'MOTHER_BRANCH','SETTLING_OFF_CD','TXT_USER_ID','CLAIMSPROXIMITY','CUSTOMER_CLAIM_NUMBER','CAUSE_OF_LOSS',\
# 'CAUSE_LOSS_CD','ANAT_PROP','ANATOMY_CD','INJURY_DAMAGE','INJURY_CD','EXAMINER_NAME','EXAMINER_USER_ID',\
# 'EXAMINER_USER_ID_CD','ISSUING_OFFICE','MARKET_SECMENTATION','ISSUING_COMAPNY','LOSS_DESC_TXT','LOSSCITY',\
# 'CLAIM_STATUS_CD','LOSSCOUNTRY','ADDRESS_1','LOSS_LOC_TXT','STATE_OF_LOSS','COVERAGE_CODE','TYPE_OF_CLAIM',\
# 'CLAIM_CATEGORY_CD','HOSPITAL_NAME','TXT_CATASTROPHE_DESC','LCL_CATASTROPHE_NO','CLAIMANT_NAME','APPROVER_ID',\
# 'PAY_FORM_TEXT','INTEREST_AMOUNT','CHEQUE_DATE','CHEQUE_NO','NETT_CHEQUE_AMOUNT','TDS_AMOUNT','MULTIPLE_PAY_IND',\
# 'SGST_AMOUNT','CGST_AMOUNT','IGST_AMOUNT','NUM_TYPE_OF_PARTY','BANK_ACCT_NO','BANK_NAME','IFSC_CODE','PAYEE_BANK_CODE',\
# 'FEES','PAYMENT_STATUS','ANH_PAYMENT_TYPE','PAYMENT_POSTING_DATE','PAYEE_TYPE','SURVEYORCODE','SURVEYOR_APPOINTMENT_DATE',\
# 'SURVEYOR_REPORT_RECEIVED_DATE','SURVEYOR_NAME','NUM_SURVEY_TYPE','PAYEE_NAME','CERTIFICATE_NUMBER',\
# 'RENL_CERT_NO','EFF_DT_SEQ_NO','POLICY_NO','PRODUCER_CD','PRODUCER_NAME','CANCELLATION_REASON','POL_EXP_DATE',\
# 'EFFECTIVE_DATE_OF_CANCELLATION','POLICY_CANCELLATION_DATE','DAT_POLICY_EFF_FROMDATE','DAT_ENDORSEMENT_EFF_DATE',\
# 'NUM_DEPARTMENT_CODE','CUSTOMER_ID','NCB','LOB','POL_INCEPT_DATE','MAJOR_LINE_CD',\
# 'MINOR_LINE_CD','CLASS_PERIL_CD','MAJOR_LINE_TEXT','MINOR_LINE_TXT','CLASS_PERIL_TEXT','PRODUCT_NAME',\
# 'LAST_NOTE_UPDATED_DATE','TATAAIG_GSTN_NO','DEFENCE_COST','PAYMENT_CORRRECTION','TREATMENTTYPE','REPUDIATED_REASON',\
# 'EXONERATION','CWP_REASON','TYPE_OF_SETTLEMENT','INTERIM_AWARD_DATE','DATE_OF_RECEIPT_OF_AWARD_BY_TATAAIG',\
# 'INTERIM_AWARD_RECEIVED_BY_TAGIC_DATE','DATE_OF_RECEIPT_OF_AWARD_BY_ADVOCATE','BASIC_TP',\
# 'TP_AWARD','COSTS_PLAINTIFF','DATE_OF_AWARD','DATE_OF_FILING_OF_PETITION','DATE_OF_HEARING',\
# 'LITIGATION_TYPE','COURT_NAME','REASON_FOR_REOPEN','POL_ISS_OFF_DESC','PROD_OFF_SUB_TXT',\
# 'SETTLING_OFF_DESC','LOSSDISTRICT','USER_NAME','TYPE_OF_SURVEY','INDEMNITY_STATUS_AT_FEATURE_LEVEL',\
# 'EXPENSES_STATUS_AT_FEATURE_LEVEL','REOPEN_STATUS','USER_EMP_CD','LITIGATION_TYPE_CD',\
# 'PROD_OFF_TXT','INVOICE_NO','INVOICE_DATE','PROD_SOURCE_CD','PRIORITY_CLIENT_ID','PAY_FORM_CD','POLICY_STATUS',\
# 'SUBLIMIT','CURRENCY','CASE_NUMBER','CASE_YEAR','REOPEN_DATE_REF','EXPENSE_PAYMENT_TYPE','NOTIONAL_SALVAGE_AMOUNT',\
# 'NOTIONAL_SUBROGATION_AMOUNT','INDEMNITY_PARTS','INDEMNITY_PAINT','INDEMNITY_LABOR','INDEMNITY','PAN_NO_SI',\
# 'NUM_ISACTIVE','CASH_COLLECTION','CLM_CREATE_DATE_REF','CWP_DATE_REF','CWP_AMOUNT_REF','CWP_REASON_REF',\
# 'EXAMINER_EMP_CODE','EXPENSES','RESERVE_RELEASE_PAYMENT', 'ICD_MAJOR_CD','ICD_MAJOR_DESC', 'ICD_MINOR_CD',\
# 'ICD_MINOR_DESC', 'INTERMEDIARY_GSTN_NO', 'PAN_NO', 'NUM_RESOURCE_TYPE', 'TYPE_OF_SURVEYOR', 'SURVEYOR_LICENSE_EXPIRY_DATE',\
# 'SURVEYOR_LICENSE_NUMBER', 'APPOINTMENT_OF_INVESTIGATOR','CWP_REOPEN_DATE', 'PAYEE_SURVEYORCODE', 'PAYEE_GSTN_NO',\
# 'PAYEE_PAN_NO', 'RELATIONSHIP','PAN_NO_INSURED', 'SAC_CODE','TRANSACTIONTYPE','ITD_GROSS_PAID_AMOUNT_REOPEN',\
# 'NUM_PAYMENT_UPDATE_NO', 'POLICYTERM', 'SOURCE', 'REPORT_DATE', 'RI_INWARD_FLAG', 'PAYMENT_TYPE','REINSURANCE_DESC','LOAD_DATE','ROW_NUM']]

# clm_all.count()

# clm_all.printSchema()

print ("Deleting data from Claim Report GC P1 with below query: ")
print ("""delete from datamarts.claim_report_gc_p1 where REPORT_DATE BETWEEN '""" + start_date + """' and '""" + end_date + "' ")

from pg import DB

db = DB(dbname='gpadmin', host='10.35.12.194', port=5432,user='gpspark', passwd='spark@456')
dlt_cnt = db.query("""delete from datamarts.claim_report_gc_p1 where REPORT_DATE BETWEEN '""" + start_date + """' and '""" + end_date + "' ")

print ("no of records deleted for "+report_date_str)
print (dlt_cnt)

clm_all.write.format("greenplum").option("dbtable","claim_report_gc_p1").option('dbschema','datamarts').option("server.port","1104").option("url","jdbc:postgresql://10.35.12.194:5432/gpadmin").option("user", "gpspark").option("password","spark@456").mode('append').save()

# import sys
# try:
#     clm_all.write.format("greenplum").option("dbtable","claim_report_gc_p1").option('dbschema','datamarts').option("server.port","1104").option("url","jdbc:postgresql://10.35.12.194:5432/gpadmin").option("user", "gpspark").option("password","spark@456").mode('append').save()
# except Exception as e :
#     x = e
# else:
#     x = 200 #success

clm_00_1.unpersist()
gc_clm_transaction_history1.unpersist()
# gc_clm_gen_info_extra.unpersist()
# clm_00_3.unpersist()
clm_11_1.unpersist()

# spark.stop()
