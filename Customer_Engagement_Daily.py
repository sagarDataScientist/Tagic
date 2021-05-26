#Testing changes- Blank filter uncomment - Filter change at 2 places- 2 index names change - history date fix to 1
#Daily changes - filter in dump and filter in .ksh while counting records
#Change log: for T30 null mobile_nos are filtered out on 20200809
#Change log: 3dummuy users added for T30 template on 20200909
#Change log: changed dump from xlsx to csv on 20200915
#Change log: T30 frequency changed to daily (for T+7 days) on 20210119
#Change log: FNOL templates on 20210218

#import sys
# changes done by amar 20201127 GPDB Migration
# envir='prod'
# if envir == 'prod':
#     if len(sys.argv) != 3:
#         print("Usage: need 2 arguments 1.Run Type \n2.Report Date/End Date")
#         sys.exit(-1)

# runtype = sys.argv[1]
# report_date = sys.argv[2]
import datetime
from datetime import datetime as dt
import pytz
runtype = 'daily'
report_date_str = dt.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d")
# report_date_str = str(datetime.date.today())
# runtype = 'adhoc'
# report_date_str = '2021-02-28'

no_of_cpu = 8
max_cores = 16
executor_mem = '41g'

print ("No_of_cpu="+str(no_of_cpu))
print ("Max_cores="+str(max_cores))
print ("Executor_mem="+str(executor_mem))
print ("Runtype="+str(runtype))
print ("Report_date="+str(report_date_str))

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
conf.set('spark.sql.shuffle.partitions','24')
conf.set('spark.driver.memory','6g')
conf.set("spark.ui.port","4048")
#conf.set('spark.memory.fraction','.2')
conf.set("spark.driver.maxResultSize","0")

conf.set('spark.es.scroll.size','10000')
conf.set('spark.network.timeout','600s')
conf.set('spark.sql.crossJoin.enabled', 'true')

conf.set('spark.executor.heartbeatInterval','60s')
conf.set("spark.driver.cores","4")
conf.set("spark.driver.extraJavaOptions","-Xmx4g -Xms4g")

#conf.set("spark.shuffle.blockTransferService", "nio"); 
conf.set("spark.files.overwrite","true");
conf.set("spark.kryoserializer.buffer", "70");
conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC");
conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC");
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"); 
conf.set("spark.broadcast.compress", "true");
conf.set("spark.shuffle.compress", "true"); 
conf.set("spark.shuffle.spill.compress", "true");
conf.set("spark.app.name", "Customer_Engagement-"+runtype+": "+report_date_str)

from pyspark.sql.functions import broadcast

conf.set('es.nodes.wan.only','true')

conf.set('spark.es.mapping.date.rich','false')
conf.set('es.http.timeout', '5m')
conf.set('es.http.retries', '5')
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
StringToDateFunc = udf(lambda x: string_to_date(x), DateType())

import functools 
from pyspark.sql import DataFrame
# Function row-wise joining of dataframes
def unionAll(dfs):
    return functools.reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), dfs)

report_date = string_to_date(report_date_str).date()
print (report_date)

# from decimal import *
# num_refer_no_list = [202011270076516]
# cust_cd_list = ["6061838694"]
# agent_cd_list = ["0042996000"]

# url_uat = "jdbc:postgresql://10.33.195.103:5432/gpadmin"
url_prod = "jdbc:postgresql://10.35.12.194:5432/gpadmin"
server_port = "1176"
user_prod="gpspark"
pwd_prod="spark@456"
dbschema_etl = "public"

def day_month(any_day):
    if any_day != None:
        return str('{:02d}'.format(any_day.day)) + str('{:02d}'.format(any_day.month))
    else:
        return any_day
DayMonth = udf(lambda x: day_month(x), StringType())

print (time.gmtime())
  
gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "registers","dbtable": "premium_register",
         "partitionColumn":"row_num","partitions":18,
         "server.port":server_port} 
prem_reg_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('POLICY_NO','CERTIFICATE_NO','TIMESTAMP','CLIENT_NAME','POL_INCEPT_DATE','POL_EXP_DATE','PRODUCER_CD','PRODUCER_NAME','PRODUCER_TYPE','CHANNEL','PORTAL_FLAG','PREMIUM_WITH_TAX','CUST_RES_STATE','NCB_PERCENT','PRODUCT_CD','NUM_REFERENCE_NO','CUSTOMER_ID')\
# .filter(col("NUM_REFERENCE_NO").isin(num_refer_no_list))\

prem_reg_base = prem_reg_base\
                    .withColumn('TIMESTAMP', to_date(prem_reg_base.TIMESTAMP, format='yyyy-MM-dd'))\
                    .withColumn('POL_INCEPT_DATE', to_date(prem_reg_base.POL_INCEPT_DATE, format='yyyy-MM-dd'))\
                    .withColumn('POL_EXP_DATE', to_date(prem_reg_base.POL_EXP_DATE, format='yyyy-MM-dd'))\
                    .withColumn('NUM_REFERENCE_NO', col('NUM_REFERENCE_NO').cast(DecimalType(15,0)).cast(StringType()))

prem_reg_base_01 = prem_reg_base\
                .groupBy('POLICY_NO','CERTIFICATE_NO')\
                .agg(sf.sum('PREMIUM_WITH_TAX').alias('TOTAL_PREMIUM'),\
                     sf.max('TIMESTAMP').alias('POLICY_BINDING_DATE'),\
                     sf.max('CLIENT_NAME').alias('CUSTOMER_NAME'),\
                     sf.max('POL_INCEPT_DATE').alias('POL_INCEPT_DATE'),\
                     sf.max('POL_EXP_DATE').alias('POL_EXP_DATE'),\
                     sf.max('PRODUCER_CD').alias('PRODUCER_CD'),\
                     sf.max('PRODUCER_NAME').alias('PRODUCER_NAME'),\
                     sf.max('PRODUCER_TYPE').alias('PRODUCER_TYPE'),\
                     sf.max('CHANNEL').alias('CHANNEL'),\
                     sf.max('PORTAL_FLAG').alias('PORTAL_FLAG'),\
                     sf.max('CUST_RES_STATE').alias('CUSTOMER_STATE'),\
                     sf.max('NCB_PERCENT').alias('NCB_PERCENT'),\
                     sf.max('PRODUCT_CD').alias('PRODUCTCODE'),\
                     sf.max('NUM_REFERENCE_NO').alias('NUM_REFERENCE_NO'),\
                     sf.max('CUSTOMER_ID').alias('CUSTOMER_ID'))

prem_reg_base_01 = prem_reg_base_01.withColumn('TOTAL_PREMIUM', round(prem_reg_base_01.TOTAL_PREMIUM, 2))\
                                   .withColumn('CERTIFICATE_NO', trim(prem_reg_base.CERTIFICATE_NO).cast('integer'))

gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "underwriting_gc_uw_product_master",
         "partitionColumn":"row_num","partitions":1,
         "server.port":server_port} 
uw_product_master =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('DEPARTMENTCODE','PRODUCTCODE','PRODUCTNAME')\

uw_product_master = uw_product_master\
                        .withColumn('PRODUCTCODE',col('PRODUCTCODE').cast(DecimalType(4,0)).cast(StringType()))\
                        .withColumn('DEPARTMENTCODE',col('DEPARTMENTCODE').cast(DecimalType(2,0)).cast(StringType()))
uw_product_master = uw_product_master\
                            .withColumnRenamed('PRODUCTNAME', 'PRODUCT_NAME')\
                            .withColumnRenamed('DEPARTMENTCODE', 'LOBCODE')
uw_product_master = uw_product_master\
                            .groupBy('PRODUCTCODE')\
                            .agg(sf.max('LOBCODE').alias('LOBCODE'),\
                                 sf.max('PRODUCT_NAME').alias('PRODUCT_NAME'))

gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "mappers","dbtable": "product_category_mapper",
         "partitionColumn":"row_num","partitions":1,
         "server.port":server_port} 
product_category_mapper =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('PRODUCTCODE','PRODUCT_CATEGORY')

product_category_mapper = product_category_mapper\
                                        .withColumn('PRODUCTCODE',col('PRODUCTCODE').cast(DecimalType(4,0)).cast(StringType()))
product_category_mapper = product_category_mapper.groupBy('PRODUCTCODE').agg(sf.max('PRODUCT_CATEGORY').alias('PRODUCT_CATEGORY'))

product_master = uw_product_master.join(product_category_mapper,'PRODUCTCODE','left_outer')

gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "reference_gc_uw_department_master",
         "partitionColumn":"row_num","partitions":1,
         "server.port":server_port} 
uw_department_master =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('DEPARTMENTCODE','DEPARTMENTNAME')\

#uw_department_master.show(500)
#15

uw_department_master = uw_department_master.withColumn('LOBCODE',col('DEPARTMENTCODE').cast(DecimalType(2,0)).cast(StringType()))\
                                           .drop('DEPARTMENTCODE')

dep_master = uw_department_master\
            .groupBy('LOBCODE')\
            .agg(sf.max('DEPARTMENTNAME').alias('LOB_NAME'))

lob_product_master = dep_master.join(product_master,'LOBCODE','inner')
prem_reg_base_02 = prem_reg_base_01.join(lob_product_master,'PRODUCTCODE','left_outer')

prem_reg_base_03 = prem_reg_base_02.filter((prem_reg_base_02.LOBCODE == '31')|\
                                           (prem_reg_base_02.LOBCODE == '28')|\
                                           (prem_reg_base_02.LOBCODE == '55')|\
                                           (prem_reg_base_02.LOBCODE == '56')|\
                                           (prem_reg_base_02.LOBCODE == '42')|
                                           (prem_reg_base_02.LOBCODE == '16'))

prem_reg_base_04 = prem_reg_base_03\
                        .withColumn('DATE_COMPARE',\
                                    when(prem_reg_base_03.POL_INCEPT_DATE>=prem_reg_base_03.POLICY_BINDING_DATE,prem_reg_base_03.POL_INCEPT_DATE)\
                                    .otherwise(prem_reg_base_03.POLICY_BINDING_DATE))

#prem_reg_base_04.show(500)
#4923494

###########################################################################################################
#Below DF for Template T1 & T2
prem_reg_base_05 = prem_reg_base_04.filter(prem_reg_base_04.DATE_COMPARE == (report_date - datetime.timedelta(days=11)))
prem_reg_base_06 = prem_reg_base_04.filter(prem_reg_base_04.POL_INCEPT_DATE == (report_date - datetime.timedelta(days=11)))

prem_reg_T1_cv = prem_reg_base_05.filter((prem_reg_base_05.PRODUCTCODE=='3124')&\
                                         (prem_reg_base_05.TOTAL_PREMIUM > 10000))
prem_reg_T1_pc = prem_reg_base_05.filter((prem_reg_base_05.PRODUCTCODE.isin(['3121','3184']))&\
                                         (prem_reg_base_05.TOTAL_PREMIUM > 10000))
prem_reg_T1_tw = prem_reg_base_05.filter((prem_reg_base_05.PRODUCTCODE=='3122')&\
                                         (prem_reg_base_05.TOTAL_PREMIUM > 6000))
# prem_reg_T1_tvl = prem_reg_base_06.filter((prem_reg_base_06.PRODUCTCODE=='5601')&\
#                                           (prem_reg_base_06.TOTAL_PREMIUM > 3000))
prem_reg_T1_tvl = prem_reg_base_06.filter((prem_reg_base_06.PRODUCTCODE.isin(['5601','1601','1602','1603','1604','1605','1606']))&\
                                          (prem_reg_base_06.TOTAL_PREMIUM > 3000))
prem_reg_T1_hlt = prem_reg_base_05.filter(prem_reg_base_05.PRODUCTCODE.isin(['2807','2861','2862','2863','2864','2865','2866','2868','2871','2874','2875','2888','2896']))
prem_reg_T1_hme = prem_reg_base_05.filter((prem_reg_base_05.LOBCODE=='55')&\
                                         (prem_reg_base_05.TOTAL_PREMIUM > 7000))
prem_reg_T1_ipa = prem_reg_base_05.filter((prem_reg_base_05.PRODUCTCODE.isin(['4251','4252','4253','4254','4255','4256','4257','4258','4259','4260','4261','4276','4277']))&\
                                          (prem_reg_base_05.TOTAL_PREMIUM > 10000))

prem_reg_T1 = unionAll([prem_reg_T1_cv,prem_reg_T1_pc,prem_reg_T1_tw,prem_reg_T1_tvl,prem_reg_T1_hlt,prem_reg_T1_hme,prem_reg_T1_ipa])
prem_reg_T1 = prem_reg_T1.withColumn('TEMPLATE_ID',lit('T1'))

#Below DF for Template T2
prem_reg_T2_tvl = prem_reg_base_06.filter(((prem_reg_base_06.PRODUCTCODE.isin(['5602','5604']))&\
                                            (prem_reg_base_06.CERTIFICATE_NO!=0))&\
                                          (prem_reg_base_06.TOTAL_PREMIUM > 3000))
prem_reg_T2_hlt = prem_reg_base_05.filter((prem_reg_base_05.PRODUCTCODE.isin(['2860','2869','2876','2877','2878','2889','2890','2895','2897']))&\
                                            (prem_reg_base_05.CERTIFICATE_NO!=0))
prem_reg_T2_ipa = prem_reg_base_05.filter(((prem_reg_base_05.PRODUCTCODE.isin(['4266','4269','4270','4273']))&\
                                            (prem_reg_base_05.CERTIFICATE_NO!=0))&\
                                          (prem_reg_base_05.TOTAL_PREMIUM > 10000))
prem_reg_T2 = unionAll([prem_reg_T2_tvl,prem_reg_T2_hlt,prem_reg_T2_ipa])
prem_reg_T2 = prem_reg_T2.withColumn('TEMPLATE_ID',lit('T2'))
###########################################################################################################
#Below DF for Template T4
prem_reg_base_07 = prem_reg_base_04.filter(prem_reg_base_04.POL_INCEPT_DATE == (report_date - datetime.timedelta(days=91)))

prem_reg_T4_cv = prem_reg_base_07.filter((prem_reg_base_07.PRODUCTCODE=='3124')&\
                                         (prem_reg_base_07.TOTAL_PREMIUM > 10000))
prem_reg_T4_pc = prem_reg_base_07.filter((prem_reg_base_07.PRODUCTCODE.isin(['3121','3184']))&\
                                         (prem_reg_base_07.TOTAL_PREMIUM > 10000))
prem_reg_T4_tw = prem_reg_base_07.filter((prem_reg_base_07.PRODUCTCODE=='3122')&\
                                         (prem_reg_base_07.TOTAL_PREMIUM > 6000))
# prem_reg_T4_tvl = prem_reg_base_07.filter(((prem_reg_base_07.PRODUCTCODE=='5601')|\
#                                            ((prem_reg_base_07.PRODUCTCODE.isin(['5602','5604']))&\
#                                             (prem_reg_base_07.CERTIFICATE_NO!=0)))&\
#                                           (prem_reg_base_07.TOTAL_PREMIUM > 3000))
prem_reg_T4_tvl = prem_reg_base_07.filter(((prem_reg_base_07.PRODUCTCODE.isin(['5601','1601','1602','1603','1604','1605','1606']))|\
                                           ((prem_reg_base_07.PRODUCTCODE.isin(['5602','5604']))&\
                                            (prem_reg_base_07.CERTIFICATE_NO!=0)))&\
                                          (prem_reg_base_07.TOTAL_PREMIUM > 3000))
prem_reg_T4_hlt = prem_reg_base_07.filter((prem_reg_base_07.PRODUCTCODE.isin(['2807','2861','2862','2863','2864','2865','2866','2868','2871','2874','2875','2888','2896']))|\
                                           ((prem_reg_base_07.PRODUCTCODE.isin(['2860','2869','2876','2877','2878','2889','2890','2895','2897']))&\
                                            (prem_reg_base_07.CERTIFICATE_NO!=0)))
prem_reg_T4_hme = prem_reg_base_07.filter((prem_reg_base_07.LOBCODE=='55')&\
                                         (prem_reg_base_07.TOTAL_PREMIUM > 7000))
prem_reg_T4_ipa = prem_reg_base_07.filter(((prem_reg_base_07.PRODUCTCODE.isin(['4251','4252','4253','4254','4255','4256','4257','4258','4259','4260','4261','4276','4277']))|\
                                           ((prem_reg_base_07.PRODUCTCODE.isin(['4266','4269','4270','4273']))&\
                                            (prem_reg_base_07.CERTIFICATE_NO!=0)))&\
                                          (prem_reg_base_07.TOTAL_PREMIUM > 10000))
prem_reg_T4 = unionAll([prem_reg_T4_cv,prem_reg_T4_pc,prem_reg_T4_tw,prem_reg_T4_tvl,prem_reg_T4_hlt,prem_reg_T4_hme,prem_reg_T4_ipa])
prem_reg_T4 = prem_reg_T4.withColumn('TEMPLATE_ID',lit('T4'))

#Below DF for Template T12
prem_reg_base_08 = prem_reg_base_04.filter(prem_reg_base_04.POL_INCEPT_DATE == (report_date + datetime.timedelta(days=2)))
prem_reg_T12_tvl = prem_reg_base_08.filter((prem_reg_base_08.PRODUCTCODE.isin(['5601','1601','1602','1603','1604','1605','1606']))|\
                                           ((prem_reg_base_08.PRODUCTCODE.isin(['5602','5604']))&\
                                            (prem_reg_base_08.CERTIFICATE_NO!=0)))
prem_reg_T12 = prem_reg_T12_tvl.withColumn('TEMPLATE_ID',lit('T12'))

#Below DF for Template T13
prem_reg_base_09 = prem_reg_base_04.filter(prem_reg_base_04.POL_EXP_DATE == (report_date - datetime.timedelta(days=1)))
prem_reg_T13_tvl = prem_reg_base_09.filter((prem_reg_base_09.PRODUCTCODE.isin(['5601','1601','1602','1603','1604','1605','1606']))|\
                                           ((prem_reg_base_09.PRODUCTCODE.isin(['5602','5604']))&\
                                            (prem_reg_base_09.CERTIFICATE_NO!=0)))
prem_reg_T13 = prem_reg_T13_tvl.withColumn('TEMPLATE_ID',lit('T13'))

#Below DF for Template T14
#Pending for logic (and Vehicle Registration No is null)
prem_reg_base_10 = prem_reg_base_04.filter(prem_reg_base_04.POL_INCEPT_DATE == (report_date - datetime.timedelta(days=46)))
prem_reg_T14_mtr = prem_reg_base_10.filter(prem_reg_base_10.PRODUCTCODE.isin(['3121','3122','3124','3184']))
prem_reg_T14 = prem_reg_T14_mtr.withColumn('TEMPLATE_ID',lit('T14'))

#Below DF for Template T15
prem_reg_base_11 = prem_reg_base_04.filter(prem_reg_base_04.POL_INCEPT_DATE == (report_date - datetime.timedelta(days=121)))
prem_reg_T15_ncb = prem_reg_base_11.filter((prem_reg_base_11.PRODUCTCODE.isin(['3121','3122','3124','3184']))&\
                                           (prem_reg_base_11.NCB_PERCENT>0))

# add product filter
list_product_codes=['3121','3122','3124','3184']
gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "policy_gc_gen_prop_information_tab",
         "partitionColumn":"row_num","partitions":4,
         "server.port":server_port} 
gen_prop_information_tab_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REFERENCE_NUMBER','TXT_BUSINESS_TYPE')\
.filter(col("TXT_BUSINESS_TYPE")==lit('Roll Over'))\
.filter(col("NUM_PRODUCT_CODE").isin(list_product_codes))\
# .filter(col("NUM_REFERENCE_NUMBER").isin(num_refer_no_list))\

gen_prop_info_tab_01 = gen_prop_information_tab_base\
                            .withColumn('NUM_REFERENCE_NO',\
                                        col('NUM_REFERENCE_NUMBER').cast(DecimalType(15,0)).cast(StringType()))\
                            .drop('NUM_REFERENCE_NUMBER')

# gen_prop_info_tab_01 = gen_prop_information_tab_base.withColumnRenamed('NUM_REFERENCE_NUMBER', 'NUM_REFERENCE_NO')
gen_prop_info_tab_02 = gen_prop_info_tab_01\
                                    .groupBy('NUM_REFERENCE_NO')\
                                    .agg(sf.max('TXT_BUSINESS_TYPE').alias('TXT_BUSINESS_TYPE'))
prem_reg_T15_ncb_01 = prem_reg_T15_ncb.join(gen_prop_info_tab_02,'NUM_REFERENCE_NO','left_outer')
prem_reg_T15_ncb_02 = prem_reg_T15_ncb_01.filter(lower(trim(prem_reg_T15_ncb_01.TXT_BUSINESS_TYPE))=='roll over')
prem_reg_T15 = prem_reg_T15_ncb_02.withColumn('TEMPLATE_ID',lit('T15'))\
                                   .drop('TXT_BUSINESS_TYPE')

#Below DF for Template T16
prem_reg_base_11 = prem_reg_base_04.filter(prem_reg_base_04.POL_INCEPT_DATE == (report_date - datetime.timedelta(days=181)))
prem_reg_T16_cv = prem_reg_base_11.filter((prem_reg_base_11.PRODUCTCODE=='3124')&\
                                         (prem_reg_base_11.TOTAL_PREMIUM > 10000))
prem_reg_T16_pc = prem_reg_base_11.filter((prem_reg_base_11.PRODUCTCODE.isin(['3121','3184']))&\
                                         (prem_reg_base_11.TOTAL_PREMIUM > 10000))
prem_reg_T16_tw = prem_reg_base_11.filter((prem_reg_base_11.PRODUCTCODE=='3122')&\
                                         (prem_reg_base_11.TOTAL_PREMIUM > 6000))
prem_reg_T16 = unionAll([prem_reg_T16_cv,prem_reg_T16_pc,prem_reg_T16_tw])
prem_reg_T16 = prem_reg_T16.withColumn('TEMPLATE_ID',lit('T16'))

#Below DF for Template T18
#Pending for logic (and DOB is sysdate)
prem_reg_base_T18 = prem_reg_base_04.filter(prem_reg_base_04.POL_EXP_DATE >= report_date)
prem_reg_T18_hlt = prem_reg_base_T18.filter((prem_reg_base_T18.PRODUCTCODE.isin(['2807','2861','2862','2863','2864','2865','2866','2868','2871','2874','2875','2888','2896']))|\
                                           ((prem_reg_base_T18.PRODUCTCODE.isin(['2860','2869','2876','2877','2878','2889','2890','2895','2897']))&\
                                            (prem_reg_base_T18.CERTIFICATE_NO!=0)))
prem_reg_T18 = prem_reg_T18_hlt.withColumn('TEMPLATE_ID',lit('T18'))

#prem_reg_T18.show(500)
#855135

#Below DF for Template T23
prem_reg_base_13 = prem_reg_base_04.filter(prem_reg_base_04.POL_INCEPT_DATE == (report_date - datetime.timedelta(days=46)))
prem_reg_T23_hlt = prem_reg_base_13.filter((prem_reg_base_13.PRODUCTCODE.isin(['2807','2861','2862','2863','2864','2865','2866','2868','2871','2874','2875','2888','2896']))|\
                                           ((prem_reg_base_13.PRODUCTCODE.isin(['2860','2869','2876','2877','2878','2889','2890','2895','2897']))&\
                                            (prem_reg_base_13.CERTIFICATE_NO!=0)))
prem_reg_T23 = prem_reg_T23_hlt.withColumn('TEMPLATE_ID',lit('T23'))

#prem_reg_T23.show(500)
#1919

#Below DF for Template T29
#Pending for logic (and MobNo is not null)
prem_reg_base_12 = prem_reg_base_04.filter(prem_reg_base_04.POL_INCEPT_DATE == (report_date - datetime.timedelta(days=271)))
prem_reg_T29_cv = prem_reg_base_12.filter((prem_reg_base_12.PRODUCTCODE=='3124')&\
                                         (prem_reg_base_12.TOTAL_PREMIUM > 10000))
prem_reg_T29_pc = prem_reg_base_12.filter((prem_reg_base_12.PRODUCTCODE.isin(['3121','3184']))&\
                                         (prem_reg_base_12.TOTAL_PREMIUM > 10000))
prem_reg_T29_tw = prem_reg_base_12.filter((prem_reg_base_12.PRODUCTCODE=='3122')&\
                                         (prem_reg_base_12.TOTAL_PREMIUM > 6000))
prem_reg_T29_hlt = prem_reg_base_12.filter((prem_reg_base_12.PRODUCTCODE.isin(['2807','2861','2862','2863','2864','2865','2866','2868','2871','2874','2875','2888','2896']))|\
                                           ((prem_reg_base_12.PRODUCTCODE.isin(['2860','2869','2876','2877','2878','2889','2890','2895','2897']))&\
                                            (prem_reg_base_12.CERTIFICATE_NO!=0)))
prem_reg_T29_hme = prem_reg_base_12.filter((prem_reg_base_12.LOBCODE=='55')&\
                                         (prem_reg_base_12.TOTAL_PREMIUM > 7000))
prem_reg_T29_ipa = prem_reg_base_12.filter(((prem_reg_base_12.PRODUCTCODE.isin(['4251','4252','4253','4254','4255','4256','4257','4258','4259','4260','4261','4276','4277']))|\
                                           ((prem_reg_base_12.PRODUCTCODE.isin(['4266','4269','4270','4273']))&\
                                            (prem_reg_base_12.CERTIFICATE_NO!=0)))&\
                                          (prem_reg_base_12.TOTAL_PREMIUM > 10000))

prem_reg_T29 = unionAll([prem_reg_T29_cv,prem_reg_T29_pc,prem_reg_T29_tw,prem_reg_T29_hlt,prem_reg_T29_hme,prem_reg_T29_ipa])
prem_reg_T29 = prem_reg_T29.withColumn('TEMPLATE_ID',lit('T29'))

template_all = unionAll([prem_reg_T1,prem_reg_T2,prem_reg_T4,prem_reg_T12,prem_reg_T13,prem_reg_T14,prem_reg_T15,prem_reg_T16,prem_reg_T18,prem_reg_T23,prem_reg_T29])

#Below DF for Template T30
# if (report_date.weekday() == 5):
# prem_reg_base_14 = prem_reg_base_04\
#                         .filter((prem_reg_base_04.POLICY_BINDING_DATE >= (report_date - datetime.timedelta(days=2)))&\
#                                (prem_reg_base_04.POLICY_BINDING_DATE < report_date))
# print ("Saturday")
prem_reg_base_14 = prem_reg_base_04.filter(prem_reg_base_04.POLICY_BINDING_DATE == (report_date - datetime.timedelta(days=7)))

prem_reg_T30 = prem_reg_base_14.withColumn('TEMPLATE_ID',lit('T30'))
template_all = unionAll([template_all,prem_reg_T30])
    
#Below DF for Template T17
YYYY = report_date.year
december = day_month(last_day_of_month(datetime.date(2020,12,11))) #Can be any day of December
january = day_month(last_day_of_month(datetime.date(2020,1,11))) #Can be any day of January
febuary = day_month(last_day_of_month(datetime.date(YYYY,2,11))) #Can be any day of February

if ((report_date.strftime("%d%m") == december) | (report_date.strftime("%d%m") == january) | (report_date.strftime("%d%m") == febuary)):
    prem_reg_base_15 = prem_reg_base_04.filter(prem_reg_base_04.POL_EXP_DATE >= report_date)
    prem_reg_T17_hlt = prem_reg_base_15.filter((prem_reg_base_15.PRODUCTCODE.isin(['2807','2861','2862','2863','2864','2865','2866','2868','2871','2874','2875','2888','2896']))|\
                                           ((prem_reg_base_15.PRODUCTCODE.isin(['2860','2869','2876','2877','2878','2889','2890','2895','2897']))&\
                                            (prem_reg_base_15.CERTIFICATE_NO!=0)))
    prem_reg_T17 = prem_reg_T17_hlt.withColumn('TEMPLATE_ID',lit('T17'))
    template_all = unionAll([template_all,prem_reg_T17])
    print ("Tax Benefit Letter")

#Below DF for Template T19,T20,T21,T22
summer = day_month(first_day_of_month(datetime.date(2020,4,11))) #Can be any day of April
rainy = day_month(first_day_of_month(datetime.date(2020,6,11))) #Can be any day of June
autumn = day_month(first_day_of_month(datetime.date(2020,10,11))) #Can be any day of October
# winter = day_month(first_day_of_month(datetime.date(2020,12,11))) #Can be nay day of December
winter = day_month(datetime.date(2020,12,3)) #Third day of December as per user change

if ((report_date.strftime("%d%m") == summer) | (report_date.strftime("%d%m") == rainy) | (report_date.strftime("%d%m") == autumn) | (report_date.strftime("%d%m") == winter)):
    prem_reg_base_16 = prem_reg_base_04.filter(prem_reg_base_04.POL_EXP_DATE >= report_date)
    prem_reg_season_hlt = prem_reg_base_16.filter((prem_reg_base_16.PRODUCTCODE.isin(['2807','2861','2862','2863','2864','2865','2866','2868','2871','2874','2875','2888','2896']))|\
                                           ((prem_reg_base_16.PRODUCTCODE.isin(['2860','2869','2876','2877','2878','2889','2890','2895','2897']))&\
                                            (prem_reg_base_16.CERTIFICATE_NO!=0)))
    if (report_date.strftime("%d%m") == summer):
        prem_reg_season = prem_reg_season_hlt.withColumn('TEMPLATE_ID',lit('T19'))
        print ("summer")
    elif (report_date.strftime("%d%m") == rainy):
        prem_reg_season = prem_reg_season_hlt.withColumn('TEMPLATE_ID',lit('T20'))
        print ("rainy")
    elif (report_date.strftime("%d%m") == autumn):
        prem_reg_season = prem_reg_season_hlt.withColumn('TEMPLATE_ID',lit('T21'))
        print ("autumn")
    elif (report_date.strftime("%d%m") == winter):
        prem_reg_season = prem_reg_season_hlt.withColumn('TEMPLATE_ID',lit('T22'))
        print ("winter")
    template_all = unionAll([template_all,prem_reg_season])

#Below DF for Template T24,T25,T26,T27,T28
cancer_day = day_month(datetime.date(2020,2,4)) #Hardcoded as per given health awareness calender
health_day = day_month(datetime.date(2020,4,7)) #Hardcoded as per given health awareness calender
yoga_day = day_month(datetime.date(2020,6,21)) #Hardcoded as per given health awareness calender
diabetes_day = day_month(datetime.date(2020,11,14)) #Hardcoded as per health awareness given calender
aids_day = day_month(datetime.date(2020,12,1)) #Hardcoded as per given health awareness calender
# print cancer_day
# print health_day
# print yoga_day
# print diabetes_day
# print aids_day

# check here amar 20201125
if ((report_date.strftime("%d%m") == cancer_day) | (report_date.strftime("%d%m") == health_day) | (report_date.strftime("%d%m") == yoga_day) | (report_date.strftime("%d%m") == diabetes_day) | (report_date.strftime("%d%m") == aids_day)):
    prem_reg_base_17 = prem_reg_base_04.filter(prem_reg_base_04.POL_EXP_DATE >= report_date)
    prem_reg_awarness_hlt = prem_reg_base_17.filter((prem_reg_base_17.PRODUCTCODE.isin(['2807','2861','2862','2863','2864','2865','2866','2868','2871','2874','2875','2888','2896']))|\
                                           ((prem_reg_base_17.PRODUCTCODE.isin(['2860','2869','2876','2877','2878','2889','2890','2895','2897']))&\
                                            (prem_reg_base_17.CERTIFICATE_NO!=0)))
    if (report_date.strftime("%d%m") == cancer_day):
        prem_reg_awarness = prem_reg_awarness_hlt.withColumn('TEMPLATE_ID',lit('T24'))
        print ("cancer_day")
    elif (report_date.strftime("%d%m") == health_day):
        prem_reg_awarness = prem_reg_awarness_hlt.withColumn('TEMPLATE_ID',lit('T25'))
        print ("health_day")
    elif (report_date.strftime("%d%m") == yoga_day):
        prem_reg_awarness = prem_reg_awarness_hlt.withColumn('TEMPLATE_ID',lit('T28'))
        print ("yoga_day")
    elif (report_date.strftime("%d%m") == diabetes_day):
        prem_reg_awarness = prem_reg_awarness_hlt.withColumn('TEMPLATE_ID',lit('T26'))
        print ("diabetes_day")
    elif (report_date.strftime("%d%m") == aids_day):
        prem_reg_awarness = prem_reg_awarness_hlt.withColumn('TEMPLATE_ID',lit('T27'))
        print ("aids_day")   
    template_all = unionAll([template_all,prem_reg_awarness])

gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "mappers","dbtable": "festival_mapper",
         "partitionColumn":"row_num","partitions":2,
         "server.port":server_port} 
festival_mapper =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TEMPLATE_ID','FESTIVAL_DATE')\

festival_mapper = festival_mapper.groupBy('TEMPLATE_ID').agg(sf.max('FESTIVAL_DATE').alias('FESTIVAL_DATE'))

festival_mapper_T7 = festival_mapper.filter(festival_mapper.TEMPLATE_ID=='T7').select('FESTIVAL_DATE').rdd.collect()
festival_mapper_T9 = festival_mapper.filter(festival_mapper.TEMPLATE_ID=='T9').select('FESTIVAL_DATE').rdd.collect()
festival_mapper_T10 = festival_mapper.filter(festival_mapper.TEMPLATE_ID=='T10').select('FESTIVAL_DATE').rdd.collect()

new_year = day_month(datetime.date(2020,12,31)) #Hardcoded for system generated
republic_day = day_month(datetime.date(2020,1,26)) #Hardcoded for system generated
holi = festival_mapper_T7[0].FESTIVAL_DATE
independence_day = day_month(datetime.date(2020,8,15)) #Hardcoded for system generated
dussehra = festival_mapper_T9[0].FESTIVAL_DATE
diwali = festival_mapper_T10[0].FESTIVAL_DATE
christmas = day_month(datetime.date(2020,12,24)) #Hardcoded for system generated
# print new_year
# print republic_day
# print holi
# print independence_day
# print dussehra
# print diwali
# print christmas

if ((report_date.strftime("%d%m") == new_year) | (report_date.strftime("%d%m") == republic_day) | (report_date_str == holi) | (report_date.strftime("%d%m") == independence_day) | (report_date_str == dussehra) | (report_date_str == diwali) | (report_date.strftime("%d%m") == christmas)):
    prem_reg_base_18 = prem_reg_base_04.filter(prem_reg_base_04.POL_EXP_DATE >= report_date)
    prem_reg_festival_cv = prem_reg_base_18.filter((prem_reg_base_18.PRODUCTCODE=='3124')&\
                                         (prem_reg_base_18.TOTAL_PREMIUM > 10000))
    prem_reg_festival_pc = prem_reg_base_18.filter((prem_reg_base_18.PRODUCTCODE.isin(['3121','3184']))&\
                                            (prem_reg_base_18.TOTAL_PREMIUM > 10000))
    prem_reg_festival_tw = prem_reg_base_18.filter((prem_reg_base_18.PRODUCTCODE=='3122')&\
                                            (prem_reg_base_18.TOTAL_PREMIUM > 6000))
    prem_reg_festival_tvl = prem_reg_base_18.filter(((prem_reg_base_18.PRODUCTCODE.isin(['5601','1601','1602','1603','1604','1605','1606']))|\
                                            ((prem_reg_base_18.PRODUCTCODE.isin(['5602','5604']))&\
                                                (prem_reg_base_18.CERTIFICATE_NO!=0)))&\
                                            (prem_reg_base_18.TOTAL_PREMIUM > 3000))
    prem_reg_festival_hlt = prem_reg_base_18.filter((prem_reg_base_18.PRODUCTCODE.isin(['2807','2861','2862','2863','2864','2865','2866','2868','2871','2874','2875','2888','2896']))|\
                                            ((prem_reg_base_18.PRODUCTCODE.isin(['2860','2869','2876','2877','2878','2889','2890','2895','2897']))&\
                                                (prem_reg_base_18.CERTIFICATE_NO!=0)))
    prem_reg_festival_hme = prem_reg_base_18.filter((prem_reg_base_18.LOBCODE=='55')&\
                                            (prem_reg_base_18.TOTAL_PREMIUM > 7000))
    prem_reg_festival_ipa = prem_reg_base_18.filter(((prem_reg_base_18.PRODUCTCODE.isin(['4251','4252','4253','4254','4255','4256','4257','4258','4259','4260','4261','4276','4277']))|\
                                            ((prem_reg_base_18.PRODUCTCODE.isin(['4266','4269','4270','4273']))&\
                                                (prem_reg_base_18.CERTIFICATE_NO!=0)))&\
                                            (prem_reg_base_18.TOTAL_PREMIUM > 10000))
    
    prem_reg_festival_all = unionAll([prem_reg_festival_cv,prem_reg_festival_pc,prem_reg_festival_tw,prem_reg_festival_tvl,prem_reg_festival_hlt,prem_reg_festival_hme,prem_reg_festival_ipa])
    
    if (report_date.strftime("%d%m") == new_year):
        prem_reg_festival = prem_reg_festival_all.withColumn('TEMPLATE_ID',lit('T5'))
        print ("new_year")
    elif (report_date.strftime("%d%m") == republic_day):
        prem_reg_festival = prem_reg_festival_all.withColumn('TEMPLATE_ID',lit('T6'))
        print ("republic_day")
    elif (report_date_str == holi):
        prem_reg_festival = prem_reg_festival_all.withColumn('TEMPLATE_ID',lit('T7'))
        print ("holi")
    elif (report_date.strftime("%d%m") == independence_day):
        prem_reg_festival = prem_reg_festival_all.withColumn('TEMPLATE_ID',lit('T8'))
        print ("independence_day")
    elif (report_date_str == dussehra):
        prem_reg_festival = prem_reg_festival_all.withColumn('TEMPLATE_ID',lit('T9'))
        print ("dussehra")
    elif (report_date_str == diwali):
        prem_reg_festival = prem_reg_festival_all.withColumn('TEMPLATE_ID',lit('T10'))
        print ("diwali")
    elif (report_date.strftime("%d%m") == christmas):
        prem_reg_festival = prem_reg_festival_all.withColumn('TEMPLATE_ID',lit('T11'))
        print ("christmas")
    template_all = unionAll([template_all,prem_reg_festival])

status_filter=['RC','NC','DC']
gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "underwriting_gc_cnfgtr_d_all_transactions",
         "partitionColumn":"row_num","partitions":6,
         "server.port":server_port} 
cnfgtr_d_all_transactions_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TRANS_ID','STATUS','ACTIVE_FLAG')\
.filter(col("STATUS").isin(status_filter))\
.filter(col("ACTIVE_FLAG")==lit('A'))\
# .filter(col("TRANS_ID").isin(num_refer_no_list))\

#cnfgtr_d_all_transactions_base.count()

cnfgtr_d_all_transactions = cnfgtr_d_all_transactions_base\
                                .withColumn('NUM_REFERENCE_NO', col('TRANS_ID').cast(DecimalType(15,0)).cast(StringType()))\
                                .drop('TRANS_ID')

# cnfgtr_d_all_transactions = cnfgtr_d_all_transactions_base.withColumnRenamed('TRANS_ID', 'NUM_REFERENCE_NO')

cnfgtr_d_all_transactions = cnfgtr_d_all_transactions\
                                            .filter((trim(lower(cnfgtr_d_all_transactions.ACTIVE_FLAG))=='a')&\
                                                    (trim(lower(cnfgtr_d_all_transactions.STATUS)).isin(['rc','nc','dc'])))

cnfgtr_d_at1 = cnfgtr_d_all_transactions\
                    .groupBy('NUM_REFERENCE_NO')\
                    .agg(sf.max('ACTIVE_FLAG').alias('ACTIVE_FLAG'),\
                        sf.max('STATUS').alias('STATUS'))

template_all_01 = template_all.join(cnfgtr_d_at1,'NUM_REFERENCE_NO','left_outer')
template_all_02 = template_all_01\
                        .filter((trim(lower(template_all_01.ACTIVE_FLAG))=='a')&\
                                       (trim(lower(template_all_01.STATUS)).isin(['rc','nc','dc'])))\
                        .drop('DATE_COMPARE')

##############################8 FNOL templates included#########################################
gpdb_url = "jdbc:postgresql://10.35.12.194:5432/gpadmin"
gpdb_user = "gpspark"
gpdb_pass = "spark@456"

t_30_date = datetime.date.today() - datetime.timedelta(days=31)
t_60_date = datetime.date.today() - datetime.timedelta(days=61)
t_90_date = datetime.date.today() - datetime.timedelta(days=91)
t_150_date = datetime.date.today() - datetime.timedelta(days=151)
t_210_date = datetime.date.today() - datetime.timedelta(days=211)
t_240_date = datetime.date.today() - datetime.timedelta(days=241)
t_270_date = datetime.date.today() - datetime.timedelta(days=271)
t_300_date = datetime.date.today() - datetime.timedelta(days=301)
# t_30_date = '2020-12-20'
print('t_30_date',t_30_date)
print('t_60_date',t_60_date)
print('t_90_date',t_90_date)
print('t_150_date',t_150_date)
print('t_210_date',t_210_date)
print('t_240_date',t_240_date)
print('t_270_date',t_270_date)
print('t_300_date',t_300_date)

branch_list = ['AHMEDABAD','BANGALORE','HYDERABAD','DELHI','MUMBAI']
vehicle_manfctr_list = ['MARUTI', 'MAHINDRA', 'TATA', 'HYUNDAI']
channel_list = ['AFFINITY','AGENCY','AXIS','BRANCH','BROKER','D2C','DIRECT','E-BUSINESS','KPG- BANK OF BARODA','KPG- FINANCIAL INSTITUTION- B1','KPG- FINANCIAL INSTITUTION- B2','KPG -FINANCIAL INSTITUTION- B4','KPG-CANARA BANK','KPG-INDUSIND BANK','P-AGENCY','T-AGENCY']

gscPythonOptions = {"url": gpdb_url,"user": gpdb_user,"password": gpdb_pass,
                    "dbschema": "registers","dbtable": "premium_register",
                    "partitionColumn":"row_num","server.port":"1106","partitions":2} 

prem_reg_src = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('POLICY_NO','PRODUCER_CD','POL_INCEPT_DATE','CUSTOMER_ID','NUM_REFERENCE_NO')\
.filter(col("POL_INCEPT_DATE").isin([t_30_date,t_60_date,t_90_date,t_150_date,t_210_date,t_240_date,t_270_date,t_300_date]))\
.filter(col("SOURCE_SYSTEM").isin(["GC"]))\
.filter(col("PRODUCT_CD").isin(["3121","3184"]))\
.filter(col("BRANCH").isin(branch_list)|col("BRANCH").like('MUMBAI%'))\
.filter(col('CHANNEL').isin(channel_list)|col('CHANNEL').like('AGENCY%'))\
.filter(col('RECORD_TYPE_DESC').isin(["NEW BUSINESS", "RENEWAL BUSINESS"]))\
.filter(col("POL_INCEPT_DATE")>=(to_date(lit('2020-12-15'), format='yyyy-MM-dd')))\
.distinct()
# .filter(col("POL_INCEPT_DATE")==(to_date(lit(t_30_date), format='yyyy-MM-dd')))\
# .filter(upper(col('MOTOR_MANUFACTURER_NAME')).isin(vehicle_manfctr_list))

# template_all_FNOL = prem_reg_src\
#                         .withColumn('NUM_REFERENCE_NO', col('NUM_REFERENCE_NO').cast(DecimalType(15,0)).cast(StringType()))\
#                         .withColumn('TEMPLATE_ID', \
#                                     when(col('POL_INCEPT_DATE')== t_30_date,lit('FNOL_T_30'))\
#                                     .when(col('POL_INCEPT_DATE')== t_60_date,lit('FNOL_T_60'))\
#                                     .when(col('POL_INCEPT_DATE')== t_90_date,lit('FNOL_T_90'))\
#                                     .when(col('POL_INCEPT_DATE')== t_150_date,lit('FNOL_T_150'))\
#                                     .when(col('POL_INCEPT_DATE')== t_210_date,lit('FNOL_T_210'))\
#                                     .when(col('POL_INCEPT_DATE')== t_240_date,lit('FNOL_T_240'))\
#                                     .when(col('POL_INCEPT_DATE')== t_270_date,lit('FNOL_T_270'))\
#                                     .when(col('POL_INCEPT_DATE')== t_300_date,lit('FNOL_T_300')))\
#                         .withColumn('LOBCODE', lit(None))\
#                         .withColumn('LOB_NAME', lit(None))\
#                         .withColumn('PRODUCT_NAME', lit(None))\
#                         .withColumn('CUSTOMER_NAME', lit(None))\
#                         .withColumn('PRODUCER_NAME', lit(None))\
#                         .withColumn('PRODUCER_TYPE', lit(None))\
#                         .withColumn('CHANNEL', lit(None))\
#                         .withColumn('PORTAL_FLAG', lit(None))\
#                         .withColumn('TOTAL_PREMIUM', lit(None))\
#                         .withColumn('CUSTOMER_STATE', lit(None))\
#                         .withColumn('POLICY_BINDING_DATE', lit(None))\
#                         .withColumn('NCB_PERCENT', lit(None))\
#                         .withColumn('CERTIFICATE_NO', lit(None))\
#                         .withColumn('PRODUCTCODE', lit(None))\
#                         .withColumn('POL_EXP_DATE', lit(None))\
#                         .withColumn('PRODUCT_CATEGORY', lit(None))\
#                         .withColumn('ACTIVE_FLAG', lit(None))\
#                         .withColumn('STATUS', lit(None))

prem_reg_src_01 = prem_reg_src\
                        .withColumn('NUM_REFERENCE_NO', col('NUM_REFERENCE_NO').cast(DecimalType(15,0)).cast(StringType()))\
                        .withColumn('TEMPLATE_ID', \
                                    when(col('POL_INCEPT_DATE')== t_30_date,lit('FNOL_T_30'))\
                                    .when(col('POL_INCEPT_DATE')== t_60_date,lit('FNOL_T_60'))\
                                    .when(col('POL_INCEPT_DATE')== t_90_date,lit('FNOL_T_90'))\
                                    .when(col('POL_INCEPT_DATE')== t_150_date,lit('FNOL_T_150'))\
                                    .when(col('POL_INCEPT_DATE')== t_210_date,lit('FNOL_T_210'))\
                                    .when(col('POL_INCEPT_DATE')== t_240_date,lit('FNOL_T_240'))\
                                    .when(col('POL_INCEPT_DATE')== t_270_date,lit('FNOL_T_270'))\
                                    .when(col('POL_INCEPT_DATE')== t_300_date,lit('FNOL_T_300')))
prem_reg_src_02 = prem_reg_src_01\
                        .groupBy('POLICY_NO','TEMPLATE_ID')\
                        .agg(sf.max('PRODUCER_CD').alias('PRODUCER_CD'),\
                             sf.max('POL_INCEPT_DATE').alias('POL_INCEPT_DATE'),\
                             sf.max('CUSTOMER_ID').alias('CUSTOMER_ID'),\
                             sf.max('NUM_REFERENCE_NO').alias('NUM_REFERENCE_NO'))

template_all_FNOL = prem_reg_src_02\
                        .withColumn('LOBCODE', lit(None))\
                        .withColumn('LOB_NAME', lit(None))\
                        .withColumn('PRODUCT_NAME', lit(None))\
                        .withColumn('CUSTOMER_NAME', lit(None))\
                        .withColumn('PRODUCER_NAME', lit(None))\
                        .withColumn('PRODUCER_TYPE', lit(None))\
                        .withColumn('CHANNEL', lit(None))\
                        .withColumn('PORTAL_FLAG', lit(None))\
                        .withColumn('TOTAL_PREMIUM', lit(None))\
                        .withColumn('CUSTOMER_STATE', lit(None))\
                        .withColumn('POLICY_BINDING_DATE', lit(None))\
                        .withColumn('NCB_PERCENT', lit(None))\
                        .withColumn('CERTIFICATE_NO', lit(None))\
                        .withColumn('PRODUCTCODE', lit(None))\
                        .withColumn('POL_EXP_DATE', lit(None))\
                        .withColumn('PRODUCT_CATEGORY', lit(None))\
                        .withColumn('ACTIVE_FLAG', lit(None))\
                        .withColumn('STATUS', lit(None))

template_all_FNOL = template_all_FNOL[['NUM_REFERENCE_NO','PRODUCTCODE','POLICY_NO','CERTIFICATE_NO','TOTAL_PREMIUM','POLICY_BINDING_DATE','CUSTOMER_NAME','POL_INCEPT_DATE','POL_EXP_DATE','PRODUCER_CD','PRODUCER_NAME','PRODUCER_TYPE','CHANNEL','PORTAL_FLAG','CUSTOMER_STATE','NCB_PERCENT','CUSTOMER_ID','LOBCODE','LOB_NAME','PRODUCT_NAME','PRODUCT_CATEGORY','TEMPLATE_ID','ACTIVE_FLAG','STATUS']]

template_all_02 = unionAll([template_all_02,template_all_FNOL])

##############################8 FNOL templates included#########################################

gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "customer_gc_genmst_customer",
         "partitionColumn":"row_num","partitions":16,
         "server.port":server_port} 
genmst_customer_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('CUSTOMER_CODE','BIRTH_DT','EMAIL','EMAIL_2','MOBILE','MOBILE2')\
.withColumnRenamed('CUSTOMER_CODE','CUSTOMER_ID')\
# .filter(col("CUSTOMER_CODE").isin(cust_cd_list))

gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "reference_gc_genmst_intermediary",
         "partitionColumn":"row_num","partitions":1,
         "server.port":server_port} 
genmst_intermediary_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TXT_INTERMEDIARY_CD','TXT_POLICY_PHONE')\
.withColumnRenamed('TXT_INTERMEDIARY_CD','PRODUCER_CD')\
# .filter(col("TXT_INTERMEDIARY_CD").isin(agent_cd_list))

# genmst_intermediary_base = genmst_intermediary_base.withColumnRenamed('TXT_INTERMEDIARY_CD','PRODUCER_CD')

genmst_customer = genmst_customer_base\
                                .withColumn('CUSTOMER_DOB', to_date(genmst_customer_base.BIRTH_DT, format='yyyy-MM-dd'))\
                                .withColumn('MOBILE_NO',\
                                            when(trim(genmst_customer_base.MOBILE).isNull(),trim(genmst_customer_base.MOBILE2))\
                                            .otherwise(trim(genmst_customer_base.MOBILE)))\
                                .withColumn('EMAIL',\
                                            when(trim(genmst_customer_base.EMAIL).isNull(),trim(genmst_customer_base.EMAIL_2))\
                                            .otherwise(trim(genmst_customer_base.EMAIL)))\
                                .drop('MOBILE','MOBILE2','BIRTH_DT','EMAIL_2')
genmst_customer = genmst_customer\
                    .groupBy('CUSTOMER_ID')\
                    .agg(sf.max('CUSTOMER_DOB').alias('CUSTOMER_DOB'),\
                        sf.max('MOBILE_NO').alias('MOBILE_NO'),\
                        sf.max('EMAIL').alias('EMAIL'))

template_all_03 = template_all_02.join(genmst_customer,'CUSTOMER_ID','left_outer')\
                                   .drop('CUSTOMER_ID')

#genmst_intermediary.count()

#template_all_03.count()
#785126

genmst_intermediary = genmst_intermediary_base\
                                        .withColumn('AGENT_MOBILE_NO',trim(genmst_intermediary_base.TXT_POLICY_PHONE))\
                                        .drop('TXT_POLICY_PHONE')
genmst_intermediary = genmst_intermediary\
                                .groupBy('PRODUCER_CD')\
                                .agg(sf.max('AGENT_MOBILE_NO').alias('AGENT_MOBILE_NO'))

template_all_04 = template_all_03.join(genmst_intermediary,'PRODUCER_CD','left_outer')
template_all_05 = template_all_04.filter((~(template_all_04.MOBILE_NO == template_all_04.AGENT_MOBILE_NO))|\
                                        template_all_04.MOBILE_NO.isNull())\
                                   .drop('AGENT_MOBILE_NO')

gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "policy_dh_risk_headers_mot",
         "partitionColumn":"row_num","partitions":14,
         "server.port":server_port} 
risk_headers_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select( 'REFERENCE_NUM','RGSNO1','RGSNO2','RGSNO3','RGSNO4','REGISTRATION_NUMBER','REGISTRATIONNUMBER2','REGISTRATIONNUMBER3','REGISTRATIONNUMBER4','REGISTRATIONNUMBERSECTION1','REGISTRATIONNUMBERSECTION2','REGISTRATIONNUMBERSECTION3','REGISTRATIONNUMBERSECTION4','MANUFACTURER')\
# .filter(col("REFERENCE_NUM").isin(num_refer_no_list))

risk_headers1 = risk_headers_base\
                        .withColumn('NUM_REFERENCE_NO', col('REFERENCE_NUM').cast(DecimalType(15,0)).cast(StringType()))\
                        .drop('REFERENCE_NUM')

# risk_headers1 = risk_headers1.join(pol_nrn,'NUM_REFERENCE_NO','inner')

risk_headers2 = risk_headers1\
                    .withColumn('VEHICLE_REGISTRATION_NO_1',\
                                concat(coalesce(trim(risk_headers1.RGSNO1), lit('')),\
                                       coalesce(trim(risk_headers1.RGSNO2), lit('')),\
                                       coalesce(trim(risk_headers1.RGSNO3), lit('')),\
                                       coalesce(trim(risk_headers1.RGSNO4), lit(''))))\
                    .withColumn('VEHICLE_REGISTRATION_NO_2',\
                                concat(coalesce(trim(risk_headers1.REGISTRATION_NUMBER), lit('')),\
                                       coalesce(trim(risk_headers1.REGISTRATIONNUMBER2), lit('')),\
                                       coalesce(trim(risk_headers1.REGISTRATIONNUMBER3), lit('')),\
                                       coalesce(trim(risk_headers1.REGISTRATIONNUMBER4), lit(''))))\
                    .withColumn('VEHICLE_REGISTRATION_NO_3',\
                                concat(coalesce(trim(risk_headers1.REGISTRATIONNUMBERSECTION1), lit('')),\
                                       coalesce(trim(risk_headers1.REGISTRATIONNUMBERSECTION2), lit('')),\
                                       coalesce(trim(risk_headers1.REGISTRATIONNUMBERSECTION3), lit('')),\
                                       coalesce(trim(risk_headers1.REGISTRATIONNUMBERSECTION4), lit(''))))
risk_headers3 = risk_headers2\
                    .withColumn('VEHICLE_REGISTRATION_NO',\
                                when((trim(risk_headers2.VEHICLE_REGISTRATION_NO_1).isNull())|\
                                     (trim(risk_headers2.VEHICLE_REGISTRATION_NO_1) == ''),\
                                     trim(risk_headers2.VEHICLE_REGISTRATION_NO_2))\
                                .otherwise(trim(risk_headers2.VEHICLE_REGISTRATION_NO_1)))\
                    .drop('VEHICLE_REGISTRATION_NO_1','VEHICLE_REGISTRATION_NO_2')

risk_headers4 = risk_headers3\
                    .withColumn('VEHICLE_REGISTRATION_NO',\
                                when((trim(risk_headers3.VEHICLE_REGISTRATION_NO).isNull())|\
                                     (trim(risk_headers3.VEHICLE_REGISTRATION_NO) == ''),\
                                     trim(risk_headers3.VEHICLE_REGISTRATION_NO_3))\
                                .otherwise(trim(risk_headers3.VEHICLE_REGISTRATION_NO)))\
                    .drop('VEHICLE_REGISTRATION_NO_3')

risk_headers4 = risk_headers4\
                    .groupBy('NUM_REFERENCE_NO')\
                    .agg(sf.max('VEHICLE_REGISTRATION_NO').alias('VEHICLE_REGISTRATION_NO'),\
                         sf.max('MANUFACTURER').alias('MANUFACTURER'))
template_all_06 = template_all_05.join(risk_headers4,'NUM_REFERENCE_NO','left_outer')

# template_all_07 = template_all_06\
#                         .filter((template_all_06.TEMPLATE_ID.isin(['T1','T2','T4','T12','T13','T15','T16','T23','T17','T19','T20','T21','T22','T24','T25','T26','T27','T28','T5','T6','T7','T8','T9','T10','T11']))|\
#                                 ((template_all_06.TEMPLATE_ID == 'T14')&\
#                                  ((trim(template_all_06.VEHICLE_REGISTRATION_NO).isNull())|\
#                                   (trim(template_all_06.VEHICLE_REGISTRATION_NO)=='')|
#                                   (lower(trim(template_all_06.VEHICLE_REGISTRATION_NO))=='new')))|\
#                                ((template_all_06.TEMPLATE_ID == 'T18')&\
#                                (DayMonth(template_all_06.CUSTOMER_DOB) == report_date.strftime("%d%m")))|\
#                                ((template_all_06.TEMPLATE_ID == 'T29')&\
#                                ((~(trim(template_all_06.MOBILE_NO).isNull()))|(trim(template_all_06.MOBILE_NO)!='')))|\
#                                ((template_all_06.TEMPLATE_ID == 'T30')&\
#                                 (trim(template_all_06.STATUS) =='RC')&\
#                                 ((~(trim(template_all_06.MOBILE_NO).isNull()))|(trim(template_all_06.MOBILE_NO)!=''))))

# template_all_07 = template_all_06\
#                         .filter((template_all_06.TEMPLATE_ID.isin(['T1','T2','T12','T13','T15','T16','T17','T19','T20','T22','T23','T24','T25','T26','T27','T28']))|\
#                                 ((template_all_06.TEMPLATE_ID.isin(['T5','T6','T7','T8','T9','T10','T11']))&\
#                                ((~(trim(template_all_06.MOBILE_NO).isNull()))|(trim(template_all_06.MOBILE_NO)!='')))|\
#                                 ((template_all_06.TEMPLATE_ID == 'T14')&\
#                                  ((trim(template_all_06.VEHICLE_REGISTRATION_NO).isNull())|\
#                                   (trim(template_all_06.VEHICLE_REGISTRATION_NO)=='')|\
#                                   (lower(trim(template_all_06.VEHICLE_REGISTRATION_NO))=='new')))|\
#                                ((template_all_06.TEMPLATE_ID == 'T18')&\
#                                (DayMonth(template_all_06.CUSTOMER_DOB) == report_date.strftime("%d%m")))|\
#                                ((template_all_06.TEMPLATE_ID == 'T30')&\
#                                 (trim(template_all_06.STATUS) =='RC')&\
#                                 ((~(trim(template_all_06.MOBILE_NO).isNull()))|(trim(template_all_06.MOBILE_NO)!=''))))

template_all_07 = template_all_06\
                        .filter((template_all_06.TEMPLATE_ID.isin(['T1','T2','T12','T13','T15','T16','T17','T19','T20','T22','T23','T24','T25','T26','T27','T28']))|\
                                ((template_all_06.TEMPLATE_ID.isin(['T5','T6','T7','T8','T9','T10','T11']))&\
                               ((~(trim(template_all_06.MOBILE_NO).isNull()))|(trim(template_all_06.MOBILE_NO)!='')))|\
                                ((template_all_06.TEMPLATE_ID == 'T14')&\
                                 ((trim(template_all_06.VEHICLE_REGISTRATION_NO).isNull())|\
                                  (trim(template_all_06.VEHICLE_REGISTRATION_NO)=='')|\
                                  (lower(trim(template_all_06.VEHICLE_REGISTRATION_NO))=='new')))|\
                               ((template_all_06.TEMPLATE_ID == 'T18')&\
                               (DayMonth(template_all_06.CUSTOMER_DOB) == report_date.strftime("%d%m")))|\
                               ((template_all_06.TEMPLATE_ID == 'T30')&\
                                (trim(template_all_06.STATUS) =='RC')&\
                                ((~(trim(template_all_06.MOBILE_NO).isNull()))|(trim(template_all_06.MOBILE_NO)!='')))|\
                               ((template_all_06.TEMPLATE_ID.isin(['FNOL_T_30','FNOL_T_60','FNOL_T_90','FNOL_T_150','FNOL_T_210','FNOL_T_240','FNOL_T_270','FNOL_T_300']))&\
                               ((~(((trim(template_all_06.MOBILE_NO).isNull())|(trim(template_all_06.MOBILE_NO)==''))&\
                                   ((trim(template_all_06.EMAIL).isNull())|(trim(template_all_06.EMAIL)=='')))))&\
                                ((template_all_06.MANUFACTURER.isin(['MARUTI', 'HYUNDAI']))|\
                                 (template_all_06.MANUFACTURER.like('MAHINDRA%'))|\
                                 (template_all_06.MANUFACTURER.like('TATA%')))))

template_all_07 = template_all_07\
                            .filter(((template_all_07.CUSTOMER_DOB >= '1753-01-01')&\
                                   (template_all_07.CUSTOMER_DOB <= '9999-12-31'))|\
                                   template_all_07.CUSTOMER_DOB.isNull())

template_all_07 = template_all_07\
                            .withColumn('LOAD_DATE', lit(str(datetime.date.today().strftime("%Y-%m-%d"))))\
                            .withColumn('REPORT_DATE', lit(report_date))

template_all_08 = template_all_07\
                            .withColumn('LOAD_DATE', to_date(template_all_07.LOAD_DATE,format='yyyy-MM-dd'))\
                            .withColumn('REPORT_DATE',  to_date(template_all_07.REPORT_DATE,format='yyyy-MM-dd'))\
                            .withColumn('NUM_REFERENCE_NO',col('NUM_REFERENCE_NO').cast(DecimalType(15,0)))
                          
template_all_08 = template_all_08[['NUM_REFERENCE_NO','POLICY_NO','CERTIFICATE_NO','POLICY_BINDING_DATE','LOB_NAME','PRODUCT_NAME',\
                                  'ACTIVE_FLAG','STATUS','PRODUCER_CD','CUSTOMER_NAME','POL_INCEPT_DATE','POL_EXP_DATE','PRODUCER_NAME',\
                                  'PRODUCER_TYPE','CHANNEL','PORTAL_FLAG','TOTAL_PREMIUM','CUSTOMER_STATE','NCB_PERCENT','TEMPLATE_ID',\
                                  'CUSTOMER_DOB','EMAIL','MOBILE_NO','VEHICLE_REGISTRATION_NO','PRODUCTCODE','LOBCODE',\
                                   'PRODUCT_CATEGORY','REPORT_DATE','LOAD_DATE']]
s=-1
def ranged_numbers(anything): 
    global s 
    if s >= 47:
        s = 0
    else:
        s = s+1 
    return s

udf_ranged_numbers = udf(lambda x: ranged_numbers(x), IntegerType())
template_all = template_all_08.withColumn("ROW_NUM", lit(udf_ranged_numbers(lit(1))))

# add columns as per data frame and add row_num
template_all1 = template_all[['NUM_REFERENCE_NO','POLICY_NO','CERTIFICATE_NO','POLICY_BINDING_DATE','LOB_NAME','PRODUCT_NAME','ACTIVE_FLAG','STATUS','PRODUCER_CD','CUSTOMER_NAME','POL_INCEPT_DATE','POL_EXP_DATE','PRODUCER_NAME','PRODUCER_TYPE','CHANNEL','PORTAL_FLAG','TOTAL_PREMIUM','CUSTOMER_STATE','NCB_PERCENT','TEMPLATE_ID','CUSTOMER_DOB','EMAIL','MOBILE_NO','VEHICLE_REGISTRATION_NO','PRODUCTCODE','LOBCODE','PRODUCT_CATEGORY','REPORT_DATE','LOAD_DATE','ROW_NUM']]

print ("Deleting data from customer_engagement_daily with below query: ")
print ("""delete from datamarts.customer_engagement_daily""" )

from pg import DB
db = DB(dbname='gpadmin', host='10.35.12.194', port=5432,user='gpspark', passwd='spark@456')
db.query("""delete from datamarts.customer_engagement_daily""" )

template_all1.write.format("greenplum").option("dbtable","customer_engagement_daily").option('dbschema','datamarts').option("server.port","1104").option("url","jdbc:postgresql://10.35.12.194:5432/gpadmin").option("user", "gpspark").option("password","spark@456").mode('append').save()

## History Data maintain
gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "datamarts","dbtable": "customer_engagement_daily",
         "partitionColumn":"row_num","partitions":1,
         "server.port":server_port} 
daily_base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()

print ("Deleting data from customer_engagement_history with below query: ")
print ("""delete from datamarts.customer_engagement_history where REPORT_DATE = '""" + report_date_str +  """' """)

from pg import DB
db = DB(dbname='gpadmin', host='10.35.12.194', port=5432,user='gpspark', passwd='spark@456')
db.query("""delete from datamarts.customer_engagement_history where REPORT_DATE = '""" + report_date_str +  """' """)

daily_base.write.format("greenplum").option("dbtable","customer_engagement_history").option('dbschema','datamarts').option("server.port","1104").option("url","jdbc:postgresql://10.35.12.194:5432/gpadmin").option("user", "gpspark").option("password","spark@456").mode('append').save()

gscPythonOptions = {
         "url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "datamarts","dbtable": "customer_engagement_daily",
         "partitionColumn":"row_num","partitions":1,
         "server.port":server_port} 
base =sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select( 'TEMPLATE_ID','POLICY_NO','CERTIFICATE_NO','POLICY_BINDING_DATE','LOB_NAME','PRODUCT_NAME','PRODUCER_CD','CUSTOMER_NAME','POL_INCEPT_DATE','POL_EXP_DATE','PRODUCER_NAME','PRODUCER_TYPE','CHANNEL','PORTAL_FLAG','TOTAL_PREMIUM','CUSTOMER_STATE','NCB_PERCENT','CUSTOMER_DOB','EMAIL','MOBILE_NO','VEHICLE_REGISTRATION_NO','PRODUCT_CATEGORY')

base_01 = base\
        .groupBy('POLICY_NO','CERTIFICATE_NO','TEMPLATE_ID')\
        .agg(sf.max('POLICY_BINDING_DATE').alias('POLICY_BINDING_DATE'),\
             sf.max('LOB_NAME').alias('LOB_NAME'),\
             sf.max('PRODUCT_NAME').alias('PRODUCT_NAME'),\
             sf.max('CUSTOMER_NAME').alias('CUSTOMER_NAME'),\
             sf.max('POL_INCEPT_DATE').alias('POL_INCEPT_DATE'),\
             sf.max('POL_EXP_DATE').alias('POL_EXP_DATE'),\
             sf.max('PRODUCER_CD').alias('PRODUCER_CD'),\
             sf.max('PRODUCER_NAME').alias('PRODUCER_NAME'),\
             sf.max('PRODUCER_TYPE').alias('PRODUCER_TYPE'),\
             sf.max('CHANNEL').alias('CHANNEL'),\
             sf.max('PORTAL_FLAG').alias('PORTAL_FLAG'),\
             sf.max('TOTAL_PREMIUM').alias('TOTAL_PREMIUM'),\
             sf.max('CUSTOMER_STATE').alias('CUSTOMER_STATE'),\
             sf.max('NCB_PERCENT').alias('NCB_PERCENT'),\
             sf.max('CUSTOMER_DOB').alias('CUSTOMER_DOB'),\
             sf.max('EMAIL').alias('EMAIL'),\
             sf.max('MOBILE_NO').alias('MOBILE_NO'),\
            sf.max('VEHICLE_REGISTRATION_NO').alias('VEHICLE_REGISTRATION_NO'))
base_02 = base_01\
            .withColumnRenamed('POLICY_NO','POLICY_NUMBER')\
            .withColumnRenamed('EMAIL','EMAIL_ID')\
            .withColumnRenamed('POL_INCEPT_DATE','POLICY_INCEPTION_DATE')\
            .withColumnRenamed('POL_EXP_DATE','POLICY_END_DATE')\
            .withColumnRenamed('PRODUCER_CD','PRODUCER_CODE')\
            .withColumnRenamed('MOBILE_NO','MOBILE_NUMBER')\
            .withColumnRenamed('NCB_PERCENT','NCB')
data_count = base_02.count()

if (data_count!=0):
    base_03 = base_02\
                .withColumn('TEMPLATE_ID', regexp_replace('TEMPLATE_ID', '\n', ' '))\
                .withColumn('POLICY_NUMBER', regexp_replace('POLICY_NUMBER', '\n', ' '))\
                .withColumn('CERTIFICATE_NO', regexp_replace('CERTIFICATE_NO', '\n', ' '))\
                .withColumn('CHANNEL', regexp_replace('CHANNEL', '\n', ' '))\
                .withColumn('CUSTOMER_DOB', regexp_replace('CUSTOMER_DOB', '\n', ' '))\
                .withColumn('CUSTOMER_NAME', regexp_replace('CUSTOMER_NAME', '\n', ' '))\
                .withColumn('CUSTOMER_STATE', regexp_replace('CUSTOMER_STATE', '\n', ' '))\
                .withColumn('EMAIL_ID', regexp_replace('EMAIL_ID', '\n', ' '))\
                .withColumn('LOB_NAME', regexp_replace('LOB_NAME', '\n', ' '))\
                .withColumn('MOBILE_NUMBER', regexp_replace('MOBILE_NUMBER', '\n', ' '))\
                .withColumn('NCB', regexp_replace('NCB', '\n', ' '))\
                .withColumn('POLICY_BINDING_DATE', regexp_replace('POLICY_BINDING_DATE', '\n', ' '))\
                .withColumn('POLICY_END_DATE', regexp_replace('POLICY_END_DATE', '\n', ' '))\
                .withColumn('POLICY_INCEPTION_DATE', regexp_replace('POLICY_INCEPTION_DATE', '\n', ' '))\
                .withColumn('PORTAL_FLAG', regexp_replace('PORTAL_FLAG', '\n', ' '))\
                .withColumn('PRODUCER_CODE', regexp_replace('PRODUCER_CODE', '\n', ' '))\
                .withColumn('PRODUCER_NAME', regexp_replace('PRODUCER_NAME', '\n', ' '))\
                .withColumn('PRODUCER_TYPE', regexp_replace('PRODUCER_TYPE', '\n', ' '))\
                .withColumn('PRODUCT_NAME', regexp_replace('PRODUCT_NAME', '\n', ' '))\
                .withColumn('TOTAL_PREMIUM', regexp_replace('TOTAL_PREMIUM', '\n', ' '))\
                .withColumn('VEHICLE_REGISTRATION_NO', regexp_replace('VEHICLE_REGISTRATION_NO', '\n', ' '))
    
    base_04 = base_03\
                .withColumn('TEMPLATE_ID', regexp_replace('TEMPLATE_ID', '\r', ' '))\
                .withColumn('POLICY_NUMBER', regexp_replace('POLICY_NUMBER', '\r', ' '))\
                .withColumn('CERTIFICATE_NO', regexp_replace('CERTIFICATE_NO', '\r', ' '))\
                .withColumn('CHANNEL', regexp_replace('CHANNEL', '\r', ' '))\
                .withColumn('CUSTOMER_DOB', regexp_replace('CUSTOMER_DOB', '\r', ' '))\
                .withColumn('CUSTOMER_NAME', regexp_replace('CUSTOMER_NAME', '\r', ' '))\
                .withColumn('CUSTOMER_STATE', regexp_replace('CUSTOMER_STATE', '\r', ' '))\
                .withColumn('EMAIL_ID', regexp_replace('EMAIL_ID', '\r', ' '))\
                .withColumn('LOB_NAME', regexp_replace('LOB_NAME', '\r', ' '))\
                .withColumn('MOBILE_NUMBER', regexp_replace('MOBILE_NUMBER', '\r', ' '))\
                .withColumn('NCB', regexp_replace('NCB', '\r', ' '))\
                .withColumn('POLICY_BINDING_DATE', regexp_replace('POLICY_BINDING_DATE', '\r', ' '))\
                .withColumn('POLICY_END_DATE', regexp_replace('POLICY_END_DATE', '\r', ' '))\
                .withColumn('POLICY_INCEPTION_DATE', regexp_replace('POLICY_INCEPTION_DATE', '\r', ' '))\
                .withColumn('PORTAL_FLAG', regexp_replace('PORTAL_FLAG', '\r', ' '))\
                .withColumn('PRODUCER_CODE', regexp_replace('PRODUCER_CODE', '\r', ' '))\
                .withColumn('PRODUCER_NAME', regexp_replace('PRODUCER_NAME', '\r', ' '))\
                .withColumn('PRODUCER_TYPE', regexp_replace('PRODUCER_TYPE', '\r', ' '))\
                .withColumn('PRODUCT_NAME', regexp_replace('PRODUCT_NAME', '\r', ' '))\
                .withColumn('TOTAL_PREMIUM', regexp_replace('TOTAL_PREMIUM', '\r', ' '))\
                .withColumn('VEHICLE_REGISTRATION_NO', regexp_replace('VEHICLE_REGISTRATION_NO', '\r', ' '))
    
    base_05 = base_04\
                .withColumn('TEMPLATE_ID', regexp_replace('TEMPLATE_ID', '\t', ' '))\
                .withColumn('POLICY_NUMBER', regexp_replace('POLICY_NUMBER', '\t', ' '))\
                .withColumn('CERTIFICATE_NO', regexp_replace('CERTIFICATE_NO', '\t', ' '))\
                .withColumn('CHANNEL', regexp_replace('CHANNEL', '\t', ' '))\
                .withColumn('CUSTOMER_DOB', regexp_replace('CUSTOMER_DOB', '\t', ' '))\
                .withColumn('CUSTOMER_NAME', regexp_replace('CUSTOMER_NAME', '\t', ' '))\
                .withColumn('CUSTOMER_STATE', regexp_replace('CUSTOMER_STATE', '\t', ' '))\
                .withColumn('EMAIL_ID', regexp_replace('EMAIL_ID', '\t', ' '))\
                .withColumn('LOB_NAME', regexp_replace('LOB_NAME', '\t', ' '))\
                .withColumn('MOBILE_NUMBER', regexp_replace('MOBILE_NUMBER', '\t', ' '))\
                .withColumn('NCB', regexp_replace('NCB', '\t', ' '))\
                .withColumn('POLICY_BINDING_DATE', regexp_replace('POLICY_BINDING_DATE', '\t', ' '))\
                .withColumn('POLICY_END_DATE', regexp_replace('POLICY_END_DATE', '\t', ' '))\
                .withColumn('POLICY_INCEPTION_DATE', regexp_replace('POLICY_INCEPTION_DATE', '\t', ' '))\
                .withColumn('PORTAL_FLAG', regexp_replace('PORTAL_FLAG', '\t', ' '))\
                .withColumn('PRODUCER_CODE', regexp_replace('PRODUCER_CODE', '\t', ' '))\
                .withColumn('PRODUCER_NAME', regexp_replace('PRODUCER_NAME', '\t', ' '))\
                .withColumn('PRODUCER_TYPE', regexp_replace('PRODUCER_TYPE', '\t', ' '))\
                .withColumn('PRODUCT_NAME', regexp_replace('PRODUCT_NAME', '\t', ' '))\
                .withColumn('TOTAL_PREMIUM', regexp_replace('TOTAL_PREMIUM', '\t', ' '))\
                .withColumn('VEHICLE_REGISTRATION_NO', regexp_replace('VEHICLE_REGISTRATION_NO', '\t', ' '))
    
    base_06 = base_05\
                .withColumn('TEMPLATE_ID', regexp_replace('TEMPLATE_ID', '"', '``'))\
                .withColumn('POLICY_NUMBER', regexp_replace('POLICY_NUMBER', '"', '``'))\
                .withColumn('CERTIFICATE_NO', regexp_replace('CERTIFICATE_NO', '"', '``'))\
                .withColumn('CHANNEL', regexp_replace('CHANNEL', '"', '``'))\
                .withColumn('CUSTOMER_DOB', regexp_replace('CUSTOMER_DOB', '"', '``'))\
                .withColumn('CUSTOMER_NAME', regexp_replace('CUSTOMER_NAME', '"', '``'))\
                .withColumn('CUSTOMER_STATE', regexp_replace('CUSTOMER_STATE', '"', '``'))\
                .withColumn('EMAIL_ID', regexp_replace('EMAIL_ID', '"', '``'))\
                .withColumn('LOB_NAME', regexp_replace('LOB_NAME', '"', '``'))\
                .withColumn('MOBILE_NUMBER', regexp_replace('MOBILE_NUMBER', '"', '``'))\
                .withColumn('NCB', regexp_replace('NCB', '"', '``'))\
                .withColumn('POLICY_BINDING_DATE', regexp_replace('POLICY_BINDING_DATE', '"', '``'))\
                .withColumn('POLICY_END_DATE', regexp_replace('POLICY_END_DATE', '"', '``'))\
                .withColumn('POLICY_INCEPTION_DATE', regexp_replace('POLICY_INCEPTION_DATE', '"', '``'))\
                .withColumn('PORTAL_FLAG', regexp_replace('PORTAL_FLAG', '"', '``'))\
                .withColumn('PRODUCER_CODE', regexp_replace('PRODUCER_CODE', '"', '``'))\
                .withColumn('PRODUCER_NAME', regexp_replace('PRODUCER_NAME', '"', '``'))\
                .withColumn('PRODUCER_TYPE', regexp_replace('PRODUCER_TYPE', '"', '``'))\
                .withColumn('PRODUCT_NAME', regexp_replace('PRODUCT_NAME', '"', '``'))\
                .withColumn('TOTAL_PREMIUM', regexp_replace('TOTAL_PREMIUM', '"', '``'))\
                .withColumn('VEHICLE_REGISTRATION_NO', regexp_replace('VEHICLE_REGISTRATION_NO', '"', '``'))
    base_all = base_06\
                    .filter(~((base_06.TEMPLATE_ID=='T3')|\
                           (base_06.TEMPLATE_ID=='T4')|\
                           (base_06.TEMPLATE_ID=='T21')|\
                           (base_06.TEMPLATE_ID=='T29')))
    #######################Dummy records for T30########################################################################
    # if (report_date.weekday() == 5):
    dummy = sqlContext.createDataFrame(
    [('T30','200000005','200000012','','','Pooja Singh','','9769479142','Pooja2.singh@tataaig.com','','','','','','','','','','','',''), # create your data here, be consistent in the types.
        ('T30','200000005','200000012','','','Tehsin Laxmidhar','','8454999352','Tehsin.Laxmidhar@tataaig.com','','','','','','','','','','','',''),
        ('T30','200000005','200000012','','','Anith Paul','','9004674000','Anith.Paul@tataaig.com','','','','','','','','','','','',''),
    ],
    ['TEMPLATE_ID','POLICY_NUMBER','CERTIFICATE_NO','CHANNEL','CUSTOMER_DOB','CUSTOMER_NAME',\
     'CUSTOMER_STATE','MOBILE_NUMBER','EMAIL_ID','PRODUCT_NAME','LOB_NAME',\
     'NCB','POLICY_BINDING_DATE','POLICY_INCEPTION_DATE','POLICY_END_DATE','TOTAL_PREMIUM',\
     'PORTAL_FLAG','PRODUCER_CODE','PRODUCER_NAME','PRODUCER_TYPE','VEHICLE_REGISTRATION_NO'] # add your columns label here
    )
    base_all = base_all[['TEMPLATE_ID','POLICY_NUMBER','CERTIFICATE_NO','CHANNEL','CUSTOMER_DOB','CUSTOMER_NAME',\
         'CUSTOMER_STATE','MOBILE_NUMBER','EMAIL_ID','PRODUCT_NAME','LOB_NAME',\
         'NCB','POLICY_BINDING_DATE','POLICY_INCEPTION_DATE','POLICY_END_DATE','TOTAL_PREMIUM',\
         'PORTAL_FLAG','PRODUCER_CODE','PRODUCER_NAME','PRODUCER_TYPE','VEHICLE_REGISTRATION_NO']]
    
    base_all = unionAll([base_all,dummy])
    #######################Dummy records for T30########################################################################
    
    base_all=base_all[['TEMPLATE_ID','POLICY_NUMBER','CERTIFICATE_NO','CHANNEL','CUSTOMER_DOB','CUSTOMER_NAME',\
              'CUSTOMER_STATE','MOBILE_NUMBER','EMAIL_ID','PRODUCT_NAME','LOB_NAME',\
              'NCB','POLICY_BINDING_DATE','POLICY_INCEPTION_DATE','POLICY_END_DATE','TOTAL_PREMIUM',\
              'PORTAL_FLAG','PRODUCER_CODE','PRODUCER_NAME','PRODUCER_TYPE','VEHICLE_REGISTRATION_NO']]
    
    YYMMDD = report_date.strftime('%Y%m%d')
    ts = str(int(time.time()))
    file_name = 'customer_engagement_'+YYMMDD+'_'+ts
    #base_all\
    #    .coalesce(1).write.option("header","true")\
    #    .options(delimiter='^').option("quoteAll",True).option('escape','"')\
    #    .option('escape','\x0D').option('escape','\x0A').csv("file:///tmp/" + file_name + ".csv")
    base_all\
        .coalesce(1).write.option("header","true")\
        .options(delimiter='^').option("quoteAll",True).csv("file:///tmp/" + file_name + ".csv")
    # file_name1 = 'customer_engagement_renewal_'+YYMMDD+'_'+ts
    # file_name2 = 'customer_engagement_uat_'+YYMMDD+'_'+ts
    # print file_name1
    # print file_name2
    # if (renewal_count!=0):
    #     base_all_renewal\
    #         .coalesce(1).write.option("header","true")\
    #         .options(delimiter='^').option("quoteAll",True).option('escape','"')\
    #         .option('escape','\x0D').option('escape','\x0A').csv("file:///tmp/" + file_name1 + ".csv")
    # if (remain_count!=0):
    #     base_all_remain\
    #         .coalesce(1).write.option("header","true")\
    #         .options(delimiter='^').option("quoteAll",True).option('escape','"')\
    #         .option('escape','\x0D').option('escape','\x0A').csv("file:///tmp/" + file_name2 + ".csv")
    
    #print (time.gmtime())