# differencce with prod code on 20210107
# new columns - VEHICLE_REGISTRATION_NO, POLICY_TYPE, RECORD_TYPE_DESC
# logic change - POLICY_COUNTER for BTA policies
# Sagar - 20210205 new column addition - Policy Issuance Date,Proposal Date,Instrument_Date,Mode of payment
# and change in logic for MOTOR_MANUFACTURER_NAME and MOTOR_MODEL_NAME
# 20210218 - sagar - product_type mapping changes
# 20210419 - sagar - SI for Motor LOB
# 20210423 - sagar - SI for Extended warranty LOB
# 20210512 - sagar - no of lives - mail - FW: No. Of lives covered A&H LOB
# 20210517 - sagar - SI for Health LOB - refer doc - Mcube:\TCG\BRD\SUM INSURED\Sum Insured _TATA AIG_Requirements_Health_Specifications_v2.docx

import sys
runtype = 'prod' #runtype = 'nonprod'
gpdb_env = 'prod'

#runtype = 'manual'

# if arguments are not passed to this file then uncomment below parameter section

no_of_cpu = 8
max_cores = 16
executr_mem = '43g'

start_date = '2021-04-01'
end_date = '2021-04-30'
start_job_from = '1'
# recon with GC system within this job
recon = 'N'

if runtype == 'prod':
    if len(sys.argv) != 5:
        print("Usage: need 4 arguments 1.start_date \n2.end_date \n3.start_job_from \4.recon_parameter(Y/N)")
        sys.exit(-1)
    else:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        start_job_from = sys.argv[3]
        # recon with GC system within this job
        recon = sys.argv[4]

if gpdb_env=='uat':
    mesos_ip = 'mesos://10.35.12.5:5050'
    gpdb_server = '10.33.195.103'
    gpdb_port = '5432'
    gpdb_dbname = 'gpadmin'
    gpdb_user = "gpspark"
    gpdb_pass = "spark@456"
else:
    mesos_ip = 'mesos://10.33.195.18:5050'
    gpdb_server = '10.35.12.194'
    gpdb_port = '5432'
    gpdb_dbname = 'gpadmin'
    gpdb_user = "gpspark"
    gpdb_pass = "spark@456"
    
gpdb_url = "jdbc:postgresql://"+gpdb_server+":"+gpdb_port+"/"+gpdb_dbname
gpdb_server_uat = '10.33.195.103'

gpdb_url_uat = "jdbc:postgresql://"+gpdb_server_uat+":"+gpdb_port+"/"+gpdb_dbname

print("gpdb_url",gpdb_url)
print(no_of_cpu)
print(max_cores)
print(executr_mem)
print(start_date)
print(end_date)
print(start_job_from)
print(recon)

from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import asc,lit
#warnings.filterwarnings('error')
import pyspark
from datetime import datetime,timedelta
import time;
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
conf = pyspark.SparkConf()
#import numpy
import calendar
#import pandas as pd
#import simplejson as json
#import pandas as pd
import numpy as np
import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import lit
import json, pprint, requests
# es_nodes = '10.35.12.9'
# es_port = '9201'
# es_user = 'elastic'
# es_pwd = 'bEiilauM3es'
# mesos_ip = 'mesos://10.35.12.205:5050'
# spark.stop()
conf.setMaster(mesos_ip)

conf.set('spark.executor.cores', no_of_cpu)
#conf.set('spark.memory.fraction','.2')
conf.set('spark.executor.memory',executr_mem)
conf.set('spark.driver.memory','20g')
conf.set('spark.cores.max',max_cores)
conf.set('spark.sql.shuffle.partitions','300')
conf.set('spark.default.parallelism','23')
conf.set('spark.es.scroll.size','10000')
conf.set('spark.network.timeout','120m')
conf.set('spark.sql.crossJoin.enabled', 'true')

conf.set('spark.executor.heartbeatInterval','120s')
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
#conf.set("spark.app.name", "Premium Register-GPDB")
conf.set("spark.app.name", "gpdb_prem_reg : GC "+"_"+start_date+"_"+end_date)
# Premium Register-GPDB
#conf.set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec");
#conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true"); 
from pyspark.sql.functions import broadcast
from pyspark.sql.types import DateType, StringType, DecimalType
#conf.set('spark.sql.crossJoin.enabled', 'true')
# conf.set('es.nodes',es_nodes)
# conf.set('es.port',es_port)
conf.set('es.nodes.wan.only','true')
conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

#conf.set('spark.num.executors','5')

# conf.set('spark.es.net.http.auth.user', es_user)
# conf.set('spark.es.net.http.auth.pass', es_pwd)
conf.set('spark.ui.port', 4048)

conf.set('spark.es.mapping.date.rich','false')
spark = pyspark.SparkContext(conf=conf)
sqlContext = SQLContext(spark)

# Load Data into PySpark DataFrames
# Prodcom Data Frame
import json, pprint, requests
import pyspark.sql.functions as sf

report_date=end_date

start_YYMM = start_date[:4] + start_date[5:7]
end_YYMM = end_date[:4] + end_date[5:7]

current_date = datetime.date.today().strftime("%Y-%m-%d")
# #if start_job_from == '1':
# print "first job started @"
# print(time.strftime("%Y-%m-%d %H:%M:%S", ts))
# ###########################################
# Below code added on 20190803

if start_job_from == '1':
    print("First Job Started...")
    gscPythonOptions = {
         "url": gpdb_url,
         "user": gpdb_user,
         "password": gpdb_pass,
         "dbschema": "public",
         "dbtable": "reference_pr_gc_gl_cd_list"} 
    df_gl_code=sqlContext.read.format("jdbc").options(**gscPythonOptions).load()
    
    # input_indices_name = 'reference_pr_gc_gl_cd_list'
    # input_doc_type_name = input_indices_name+'/pr_gc_gl_cd_list'
    # document_count =documentcount(input_doc_type_name)
    # 
    # df_gl_code = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    # df_gl_code.printSchema()
    
    df_gl_code = df_gl_code[['TXT_LEDGER_ACCOUNT_CD', 'DESCRIPTION']]
    
    df_gl_code = df_gl_code.withColumn('TXT_LEDGER_ACCOUNT_CD', \
                                                 df_gl_code.TXT_LEDGER_ACCOUNT_CD.cast(DecimalType()).cast(StringType()))
    
    df_gl_code_prem = df_gl_code.filter(df_gl_code.DESCRIPTION == 'PREMIUM_AMOUNT_INR_WITHOUT_TAX')
    df_gl_code_comm = df_gl_code.filter(df_gl_code.DESCRIPTION == 'COMMISSION_INR')
    df_gl_code_igst = df_gl_code.filter(df_gl_code.DESCRIPTION == 'IGST')
    df_gl_code_cgst = df_gl_code.filter(df_gl_code.DESCRIPTION == 'CGST')
    df_gl_code_sgst = df_gl_code.filter(df_gl_code.DESCRIPTION == 'SGST')
    df_gl_code_tds =  df_gl_code.filter(df_gl_code.DESCRIPTION == 'TDS_ON_COMM')
    df_gl_code_srvc_tax = df_gl_code.filter(df_gl_code.DESCRIPTION == 'SERVICE_TAX')
    df_gl_code_pol_stmp_duty = df_gl_code.filter(df_gl_code.DESCRIPTION == 'POL_STAMP_DUTY_AMT')
    df_gl_code_pobl = df_gl_code.filter(df_gl_code.DESCRIPTION == 'PREM_OBL')
    df_gl_code_pvqs = df_gl_code.filter(df_gl_code.DESCRIPTION == 'PREM_CEEDED_VQST')
    df_gl_code_psur = df_gl_code.filter(df_gl_code.DESCRIPTION == 'PREM_CEEDED_SURPLUS')
    df_gl_code_pter = df_gl_code.filter(df_gl_code.DESCRIPTION == 'PREM_CEEDED_TERRISIOM')
    df_gl_code_pcom = df_gl_code.filter(df_gl_code.DESCRIPTION == 'PREM_CEEDED_AIG_COMBINED')
    df_gl_code_pfac = df_gl_code.filter(df_gl_code.DESCRIPTION == 'PREM_CEEDED_FAC')
    df_gl_code_pxol = df_gl_code.filter(df_gl_code.DESCRIPTION == 'PREM_CEEDED_XOL')
    df_gl_code_ptty = df_gl_code.filter(df_gl_code.DESCRIPTION == 'PREM_CEEDED_TTY')
    df_gl_code_cobl = df_gl_code.filter(df_gl_code.DESCRIPTION == 'COMM_OBL')
    df_gl_code_cvqs = df_gl_code.filter(df_gl_code.DESCRIPTION == 'COMM_CEEDED_VQST')
    df_gl_code_csur = df_gl_code.filter(df_gl_code.DESCRIPTION == 'COMM_CEEDED_SURPLUS')
    df_gl_code_cter = df_gl_code.filter(df_gl_code.DESCRIPTION == 'COMM_CEEDED_TERRISIOM')
    df_gl_code_ccom = df_gl_code.filter(df_gl_code.DESCRIPTION == 'COMM_CEEDED_AIG_COMBINED')
    df_gl_code_cfac = df_gl_code.filter(df_gl_code.DESCRIPTION == 'COMM_CEEDED_FAC')
    df_gl_code_ctty = df_gl_code.filter(df_gl_code.DESCRIPTION == 'COMM_UNEARNED_TTY')
    
    list_gl_code_prem = df_gl_code_prem[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_comm = df_gl_code_comm[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_igst = df_gl_code_igst[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_cgst = df_gl_code_cgst[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_sgst = df_gl_code_sgst[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_tds =  df_gl_code_tds[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_srvc_tax = df_gl_code_srvc_tax[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_pol_stmp_duty = df_gl_code_pol_stmp_duty[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_pobl = df_gl_code_pobl[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_pvqs = df_gl_code_pvqs[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_psur = df_gl_code_psur[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_pter = df_gl_code_pter[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_pcom = df_gl_code_pcom[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_pfac = df_gl_code_pfac[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_pxol = df_gl_code_pxol[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_ptty = df_gl_code_ptty[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_cobl = df_gl_code_cobl[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_cvqs = df_gl_code_cvqs[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_csur = df_gl_code_csur[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_cter = df_gl_code_cter[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_ccom = df_gl_code_ccom[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_cfac = df_gl_code_cfac[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    list_gl_code_ctty = df_gl_code_ctty[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    
    str_gl_code_prem = ''
    str_gl_code_comm = ''
    str_gl_code_igst = ''
    str_gl_code_cgst = ''
    str_gl_code_sgst = ''
    str_gl_code_tds = ''
    str_gl_code_srvc_tax = ''
    str_gl_code_pol_stmp_duty = ''
    str_gl_code_pobl = ''
    str_gl_code_pvqs = ''
    str_gl_code_psur = ''
    str_gl_code_pter = ''
    str_gl_code_pcom = ''
    str_gl_code_pfac = ''
    str_gl_code_pxol = ''
    str_gl_code_ptty = ''
    str_gl_code_cobl = ''
    str_gl_code_cvqs = ''
    str_gl_code_csur = ''
    str_gl_code_cter = ''
    str_gl_code_ccom = ''
    str_gl_code_cfac = ''
    str_gl_code_ctty = ''
    
    # str_gl_code_prem
    i = 0
    for index in range(len(list_gl_code_prem)):
        if (i==0):
            str_gl_code_prem = list_gl_code_prem[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_prem = str_gl_code_prem+','+list_gl_code_prem[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_prem
    
    # str_gl_code_comm
    i = 0
    for index in range(len(list_gl_code_comm)):
        if (i==0):
            str_gl_code_comm = list_gl_code_comm[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_comm = str_gl_code_comm+','+list_gl_code_comm[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_comm
    
    # str_gl_code_igst
    i = 0
    for index in range(len(list_gl_code_igst)):
        if (i==0):
            str_gl_code_igst = list_gl_code_igst[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_igst = str_gl_code_igst+','+list_gl_code_igst[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_igst
    
    # str_gl_code_cgst
    i = 0
    for index in range(len(list_gl_code_cgst)):
        if (i==0):
            str_gl_code_cgst = list_gl_code_cgst[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_cgst = str_gl_code_cgst+','+list_gl_code_cgst[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_cgst
    
    # str_gl_code_sgst
    i = 0
    for index in range(len(list_gl_code_sgst)):
        if (i==0):
            str_gl_code_sgst = list_gl_code_sgst[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_sgst = str_gl_code_sgst+','+list_gl_code_sgst[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_sgst
    
    # str_gl_code_tds
    i = 0
    for index in range(len(list_gl_code_tds)):
        if (i==0):
            str_gl_code_tds = list_gl_code_tds[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_tds = str_gl_code_tds+','+list_gl_code_tds[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_tds
    
    # str_gl_code_srvc_tax
    i = 0
    for index in range(len(list_gl_code_srvc_tax)):
        if (i==0):
            str_gl_code_srvc_tax = list_gl_code_srvc_tax[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_srvc_tax = str_gl_code_srvc_tax+','+list_gl_code_srvc_tax[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_srvc_tax
    
    # str_gl_code_pol_stmp_duty
    i = 0
    for index in range(len(list_gl_code_pol_stmp_duty)):
        if (i==0):
            str_gl_code_pol_stmp_duty = list_gl_code_pol_stmp_duty[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_pol_stmp_duty = str_gl_code_pol_stmp_duty+','+list_gl_code_pol_stmp_duty[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_pol_stmp_duty
    
    # str_gl_code_pobl
    i = 0
    for index in range(len(list_gl_code_pobl)):
        if (i==0):
            str_gl_code_pobl = list_gl_code_pobl[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_pobl = str_gl_code_pobl+','+list_gl_code_pobl[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_pobl
    
    # str_gl_code_pvqs
    i = 0
    for index in range(len(list_gl_code_pvqs)):
        if (i==0):
            str_gl_code_pvqs = list_gl_code_pvqs[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_pvqs = str_gl_code_pvqs+','+list_gl_code_pvqs[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_pvqs
    
    # str_gl_code_psur
    i = 0
    for index in range(len(list_gl_code_psur)):
        if (i==0):
            str_gl_code_psur = list_gl_code_psur[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_psur = str_gl_code_psur+','+list_gl_code_psur[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_psur
    
    # str_gl_code_pter
    i = 0
    for index in range(len(list_gl_code_pter)):
        if (i==0):
            str_gl_code_pter = list_gl_code_pter[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_pter = str_gl_code_pter+','+list_gl_code_pter[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_pter
    
    # str_gl_code_pcom
    i = 0
    for index in range(len(list_gl_code_pcom)):
        if (i==0):
            str_gl_code_pcom = list_gl_code_pcom[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_pcom = str_gl_code_pcom+','+list_gl_code_pcom[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_pcom
    
    # str_gl_code_pfac
    i = 0
    for index in range(len(list_gl_code_pfac)):
        if (i==0):
            str_gl_code_pfac = list_gl_code_pfac[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_pfac = str_gl_code_pfac+','+list_gl_code_pfac[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_pfac
    
    # str_gl_code_pxol
    i = 0
    for index in range(len(list_gl_code_pxol)):
        if (i==0):
            str_gl_code_pxol = list_gl_code_pxol[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_pxol = str_gl_code_pxol+','+list_gl_code_pxol[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_pxol
    
    # str_gl_code_ptty
    i = 0
    for index in range(len(list_gl_code_ptty)):
        if (i==0):
            str_gl_code_ptty = list_gl_code_ptty[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_ptty = str_gl_code_ptty+','+list_gl_code_ptty[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_ptty
    
    # ###################
    
    # str_gl_code_cobl
    i = 0
    for index in range(len(list_gl_code_cobl)):
        if (i==0):
            str_gl_code_cobl = list_gl_code_cobl[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_cobl = str_gl_code_cobl+','+list_gl_code_cobl[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_cobl
    
    # str_gl_code_cvqs
    i = 0
    for index in range(len(list_gl_code_cvqs)):
        if (i==0):
            str_gl_code_cvqs = list_gl_code_cvqs[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_cvqs = str_gl_code_cvqs+','+list_gl_code_cvqs[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_cvqs
    
    # str_gl_code_csur
    i = 0
    for index in range(len(list_gl_code_csur)):
        if (i==0):
            str_gl_code_csur = list_gl_code_csur[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_csur = str_gl_code_csur+','+list_gl_code_csur[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_csur
    
    # str_gl_code_cter
    i = 0
    for index in range(len(list_gl_code_cter)):
        if (i==0):
            str_gl_code_cter = list_gl_code_cter[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_cter = str_gl_code_cter+','+list_gl_code_cter[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_cter
    
    # str_gl_code_ccom
    i = 0
    for index in range(len(list_gl_code_ccom)):
        if (i==0):
            str_gl_code_ccom = list_gl_code_ccom[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_ccom = str_gl_code_ccom+','+list_gl_code_ccom[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_ccom
    
    # str_gl_code_cfac
    i = 0
    for index in range(len(list_gl_code_cfac)):
        if (i==0):
            str_gl_code_cfac = list_gl_code_cfac[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_cfac = str_gl_code_cfac+','+list_gl_code_cfac[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_cfac
    
    # str_gl_code_ctty
    i = 0
    for index in range(len(list_gl_code_ctty)):
        if (i==0):
            str_gl_code_ctty = list_gl_code_ctty[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_ctty = str_gl_code_ctty+','+list_gl_code_ctty[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    #print str_gl_code_ctty
    
    
    str_gl_code_all =  str_gl_code_prem+','+str_gl_code_comm+','+str_gl_code_igst+','+str_gl_code_cgst+','+str_gl_code_sgst+','+str_gl_code_tds+','+str_gl_code_srvc_tax+','\
    +str_gl_code_pobl+','+str_gl_code_pvqs+','+str_gl_code_psur+','+str_gl_code_pter+','+str_gl_code_pcom+','\
    +str_gl_code_pfac+','+str_gl_code_pxol+','+str_gl_code_ptty+','+str_gl_code_cobl+','+str_gl_code_cvqs+','\
    +str_gl_code_csur+','+str_gl_code_cter+','+str_gl_code_ccom+','+str_gl_code_cfac+','+str_gl_code_ctty+','+str_gl_code_pol_stmp_duty
    
    # optimization
    # sc.broadcast(str_gl_code_all)
    
    str_gl_code_ri = str_gl_code_pobl+','+str_gl_code_pvqs+','+str_gl_code_psur+','+str_gl_code_pter+','+str_gl_code_pcom+','\
    +str_gl_code_pfac+','+str_gl_code_pxol+','+str_gl_code_ptty+','+str_gl_code_cobl+','+str_gl_code_cvqs+','\
    +str_gl_code_csur+','+str_gl_code_cter+','+str_gl_code_ccom+','+str_gl_code_cfac+','+str_gl_code_ctty
    
    # # optimization
    # # sc.broadcast(str_gl_code_ri)
    # esq2 = """{"query": {"bool": {"must": {"terms": {"TXT_LEDGER_ACCOUNT_CD": ["""+str_gl_code_all+"""]
    # }},"filter": {"range": {"DAT_VOUCHER_DATE": {"lte": \""""+end_date+"""\", "gte":\""""+start_date+"""\"}}}}}}"""
    # 
    # # optimization
    # esq2 = """{"query": {"bool": {"must": {"terms": {"TXT_LEDGER_ACCOUNT_CD": ["""+str_gl_code_all+"""]
    # }},"filter": [{"range": {"DAT_VOUCHER_DATE": {"lte": \""""+end_date+"""\", "gte":\""""+start_date+"""\"}}},
    #               {"exists":{"field": "NUM_REFERENCE_NO"}},
    #               {"exists":{"field": "DAT_REFERENCE_DATE"}}
    #               ],
    #               "must_not": {"term":{"TXT_TRANSACTION_TYPE_CD":"AP"}}}}}}"""
    # ############################################
    
    
    
    # #input_indices_name = 'account_gc_acc_general_ledger_2018'
    # input_indices_name = 'account_gc_acc_general_ledger_alias'
    # input_doc_type_name = input_indices_name+'/acc_general_ledger'
    # document_count =documentcount(input_doc_type_name)
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # # hardcoded to max limit of integer as no of records of acc_general_ledger are 2150594436.
    # document_count = 2147483647
    # 
    # # added service tax GL codes in below filter 
    # # added GST GL codes in below filter - 2019-04-27
    
    list_name = list(str_gl_code_all.split(","))
    
    # gc_acc_general_ledger
    # account_gc_acc_general_ledger
    gscPythonOptions = {
             "url": gpdb_url,
             "user": gpdb_user,
             "password": gpdb_pass,
             "dbschema": "public",
             "dbtable": "account_gc_acc_general_ledger",
             "partitionColumn":"row_num",
             "server.port":"1160-1180",
             "partitions":6} 
    # "partitionColumn":"num_reference_no", "num_office_cd"
    acc_general_ledger=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_REFERENCE_NO','NUM_TRANSACTION_CONTROL_NO','DAT_VOUCHER_DATE','TXT_TRANSACTION_TYPE_CD','TXT_DIMENSION_4_VALUE_CD'\
            ,'TXT_DR_CR','NUM_AMOUNT','NUM_FIN_YEAR','NUM_FIN_PERIOD','DAT_REFERENCE_DATE','TXT_LEDGER_ACCOUNT_CD'\
            ,'TXT_DIMENSION_1_VALUE_CD','TXT_DIMENSION_7_VALUE_CD')\
    .filter(col("NUM_REFERENCE_NO").isNotNull())\
    .filter(col("DAT_REFERENCE_DATE").isNotNull())\
    .filter(col("TXT_LEDGER_ACCOUNT_CD").isin(list_name))\
    .filter(col("DAT_VOUCHER_DATE").between(to_timestamp(lit(start_date), format='yyyy-MM-dd'),to_timestamp(lit(end_date), format='yyyy-MM-dd')))\
    .filter(col("TXT_TRANSACTION_TYPE_CD")!=lit('AP'))
    # \
    # .filter(col('NUM_REFERENCE_NO')==lit('202001170097684'))
    # acc_general_ledger = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_REFERENCE_NO,NUM_TRANSACTION_CONTROL_NO,DAT_VOUCHER_DATE,TXT_TRANSACTION_TYPE_CD,TXT_DIMENSION_4_VALUE_CD,TXT_DR_CR,NUM_AMOUNT,NUM_FIN_YEAR,NUM_FIN_PERIOD,DAT_REFERENCE_DATE,TXT_LEDGER_ACCOUNT_CD,TXT_DIMENSION_1_VALUE_CD,TXT_DIMENSION_7_VALUE_CD').\
    # option('es.query', esq2).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    # print list_name
    
    # from pg import DB
    
    # acc_general_ledger.count()
    
    acc_general_ledger = acc_general_ledger\
                        .withColumn('NUM_REFERENCE_NO',col('NUM_REFERENCE_NO').cast(DecimalType(15,0)).cast(StringType()))
    
    acc_general_ledger = acc_general_ledger\
                        .withColumn('NUM_FIN_YEAR',col('NUM_FIN_YEAR').cast(DecimalType(4,0)).cast(StringType()))
    acc_general_ledger = acc_general_ledger\
                        .withColumn('NUM_FIN_PERIOD',col('NUM_FIN_PERIOD').cast(DecimalType(2,0)).cast(StringType()))
    
    # acc_general_ledger.createOrReplaceTempView('acc_general_ledger0')
    # acc_general_ledger.repartition(2000)
    acc_general_ledger.createOrReplaceTempView('acc_general_ledger1')
    
    ######################################################
    
    # optimization
    # distinct num_reference_no
    
    # nm_rfrnc_no_distinct = acc_general_ledger.select('NUM_REFERENCE_NO').distinct()
    # 
    # # acc_general_ledger.count()
    # 
    # # nm_rfrnc_no_distinct.persist()
    # 
    # es_nodes = '10.35.12.9'
    # es_port = '9200'
    # es_user = 'elastic'
    # es_pwd = 'bEiilauM3es'
    # output_index = "pr_gc_nm_rfrnc_no_distinct"
    # doc_type = "nm_rfrnc_no_distinct"
    # print output_index
    # 
    # ###delete index if already exists########################################################
    # headers = {'Content-Type': 'application/json'}
    # query = {"query":{"match_all":{}}}
    # URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index
    # r = requests.delete(URL, data=json.dumps(query), headers=headers)
    # 
    # ###create index ######################################################
    # headers = {'Content-Type': 'application/json'}
    # query_create= {"settings": { "index.number_of_shards": "1" , "index.number_of_replicas":"0"}}
    # URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index
    # r = requests.put(URL, data=json.dumps(query_create), headers=headers)
    # 
    # ### create mapping######################################################################################
    # 
    # mapping_output = {doc_type:{
    # 'properties': {
    # 'NUM_REFERENCE_NO': {'type': 'keyword'}}}}
    # 
    # URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index+'/_mapping/'+doc_type
    # headers = {'Content-Type': 'application/json'}
    # r = requests.put(URL, data=json.dumps(mapping_output), headers=headers)
    # 
    # final_spark_data_new = nm_rfrnc_no_distinct
    # 
    # try:
    #     final_spark_data_new.write.format('org.elasticsearch.spark.sql').mode('append').option('es.index.auto.create', 'true').\
    #     option('es.nodes' , es_nodes).option('es.port', es_port).option('es.resource',output_index+"/"+doc_type ).save() 
    # except Exception as e:
    #     x = e
    # else: 
    #     x = 200 # success
    # print x
    # 
    # input_indices_name = 'pr_gc_nm_rfrnc_no_distinct'
    # input_doc_type_name = input_indices_name+'/nm_rfrnc_no_distinct'
    # document_count =documentcount(input_doc_type_name)
    # 
    # nm_rfrnc_no_distinct = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.input.max.docs.per.partition',document_count).load()
    # 
    # ######################################################
    # # nm_rfrnc_no_distinct.persist()
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "policy_gc_gen_prop_information_tab",
        "partitionColumn":"row_num",
        "server.port":"1160-1180",
        "partitions":10} 
    #     "partitionColumn":"num_reference_number"
    gen_prop_information_tab_src=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_PRODUCT_CODE', 'NUM_REFERENCE_NUMBER', 'TXT_POLICY_NO_CHAR', 'TXT_MODEOFOPERATION', 'TXT_ILPOS_POLICY_NUMBER', \
    'TXT_BRANCH_OFFICE_CODE', 'DAT_POLICY_EFF_TODATE', 'DAT_ENDORSEMENT_EFF_DATE', 'DAT_POLICY_EFF_FROMDATE', 'DAT_POLICY_TO', 'DAT_POLICY_FROM', 'DAT_INSERT_DATE', 'DAT_MODIFY_DATE',\
    'TXT_OFFICE_NAME','TXT_SECTOR', 'TXT_CUSTOMER_ID', 'TXT_CUSTOMER_NAME','NUM_COVERNOTE_NO','TXT_BUSINESS_TYPE', \
    'TXT_PRODUCT_INDEX', 'DAT_COVER_NOTE_DATE','TXT_RELATIONSHIP_TYPE', 'NUM_NET_PREMIUM_CURR', \
    'NUM_NCBPER', 'TXT_DISPLAYOFFICECODE', 'TXT_REASON_FOR_NAME_TRANSFER', 'NUM_DEPARTMENT_CODE', 'NUM_STAMP_DUTY', 'TXT_COVERNOTE_NO', \
    'TXT_RE_INSURANCE_INWARD', 'NUM_SERVICE_TAX', 'NUM_ENDORSEMENT_SERVICE_TAX', 'TXT_SERV_TAX_EXCEMPT_CATEGORY','TGT_COMMIT_TIMESTAMP',\
    'DAT_CUSTOMER_REFERENCE_DATE')
    
    gen_prop_information_tab_src = gen_prop_information_tab_src\
    .withColumn('RANKING', sf.row_number()\
                           .over(Window.partitionBy('NUM_REFERENCE_NUMBER')\
                           .orderBy(col('TGT_COMMIT_TIMESTAMP').desc())))
    
    gen_prop_information_tab_src = gen_prop_information_tab_src\
    .filter(col('RANKING')==1)\
    .drop('RANKING').drop('TGT_COMMIT_TIMESTAMP')
    # \
    # .filter(col('NUM_REFERENCE_NUMBER')==lit('202001170097684'))
    # input_indices_name = 'policy_gc_gen_prop_information_tab_alias'
    # input_doc_type_name = input_indices_name+'/gen_prop_information_tab'
    # document_count =documentcount(input_doc_type_name)
    # 
    # gen_prop_information_tab_src = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_PRODUCT_CODE, NUM_REFERENCE_NUMBER, TXT_POLICY_NO_CHAR, TXT_MODEOFOPERATION, TXT_ILPOS_POLICY_NUMBER, \
    # TXT_BRANCH_OFFICE_CODE, DAT_POLICY_EFF_TODATE, DAT_ENDORSEMENT_EFF_DATE, DAT_POLICY_EFF_FROMDATE, DAT_POLICY_TO, DAT_POLICY_FROM, DAT_INSERT_DATE, DAT_MODIFY_DATE,\
    # TXT_OFFICE_NAME,TXT_SECTOR, TXT_CUSTOMER_ID, TXT_CUSTOMER_NAME,NUM_COVERNOTE_NO,TXT_BUSINESS_TYPE, \
    # TXT_PRODUCT_INDEX, DAT_COVER_NOTE_DATE,TXT_RELATIONSHIP_TYPE, NUM_NET_PREMIUM_CURR, \
    # NUM_NCBPER, TXT_DISPLAYOFFICECODE, TXT_REASON_FOR_NAME_TRANSFER, NUM_DEPARTMENT_CODE, NUM_STAMP_DUTY, TXT_COVERNOTE_NO, \
    # TXT_RE_INSURANCE_INWARD, NUM_SERVICE_TAX, NUM_ENDORSEMENT_SERVICE_TAX, TXT_SERV_TAX_EXCEMPT_CATEGORY').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    
    gen_prop_information_tab_src = gen_prop_information_tab_src\
                        .withColumn('NUM_REFERENCE_NUMBER',col('NUM_REFERENCE_NUMBER').cast(DecimalType(15,0)).cast(StringType()))
    
    # join_cond = [nm_rfrnc_no_distinct.NUM_REFERENCE_NO==gen_prop_information_tab_src.NUM_REFERENCE_NUMBER]
    # gen_prop_information_tab = nm_rfrnc_no_distinct.join(gen_prop_information_tab_src, join_cond)\
    #                                                .drop(nm_rfrnc_no_distinct.NUM_REFERENCE_NO)
    
    gen_prop_information_tab = gen_prop_information_tab_src.withColumn('TXT_MODEOFOPERATION', \
                                    when(((upper(trim(gen_prop_information_tab_src.TXT_BUSINESS_TYPE)) == 'S3 ROLL OVER'))&\
                                         ((upper(trim(gen_prop_information_tab_src.TXT_MODEOFOPERATION))=='NEWPOLICY')|\
                                          (upper(trim(gen_prop_information_tab_src.TXT_MODEOFOPERATION))=='PROPOSALMODIFICATION')),\
                                                 lit('RENEWPOLICY'))\
                                            .otherwise(gen_prop_information_tab_src.TXT_MODEOFOPERATION))
    gen_prop_information_tab.createOrReplaceTempView('gen_prop_information_tab0')
    
    # gen_prop_information_tab_src.count()
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "underwriting_gc_uw_proposal_addl_info",
        "partitionColumn":"row_num",
        "server.port":"1160-1180",
        "partitions":6}  
    uw_proposal_addl_info_src=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('TXT_CERTIFICATE_NO', 'TXT_RENL_CERT_NO', 'TXT_EFF_DT_SEQ_NO', 'NUM_REFERERANCE_NO')
    
    # \
    # .filter(col('NUM_REFERERANCE_NO')==lit('202001170097684'))
    # input_indices_name = 'underwriting_gc_uw_proposal_addl_info_alias'
    # input_doc_type_name = input_indices_name+'/uw_proposal_addl_info'
    # document_count =documentcount(input_doc_type_name)
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # uw_proposal_addl_info_src = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','TXT_CERTIFICATE_NO, TXT_RENL_CERT_NO, TXT_EFF_DT_SEQ_NO, NUM_REFERERANCE_NO').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    # optimization
    # join_cond = [nm_rfrnc_no_distinct.NUM_REFERENCE_NO==uw_proposal_addl_info_src.NUM_REFERERANCE_NO]
    # uw_proposal_addl_info = nm_rfrnc_no_distinct.join(uw_proposal_addl_info_src, join_cond)\
    #                                             .drop(nm_rfrnc_no_distinct.NUM_REFERENCE_NO)
    
    uw_proposal_addl_info = uw_proposal_addl_info_src
    uw_proposal_addl_info.createOrReplaceTempView('uw_proposal_addl_info0')
    
    # uw_proposal_addl_info_src.count()
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "underwriting_gc_distribution_channel_tab",
        "partitionColumn":"row_num",
        "server.port":"1160-1180",
        "partitions":8} 
    distribution_channel_tab_src=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_REFERENCE_NUMBER','TXT_TERTIARY_VERTICAL_NAME','TXT_POLICY_CURRENCY','TXT_INTERMEDIARY_TYPE','TXT_INTERMEDIARY_NAME',\
    'TXT_INTERMEDIARY_CODE','TXT_BUSINESS_SERVICING_CHANNEL','NUM_APPLICATION_NUMBER','TXT_TERTIARY_MO_CODE', \
    'TXT_BUSINESS_CHANNEL_TYPE','DAT_APPLICATION_DATE')
    # \
    # .filter(col('NUM_REFERENCE_NUMBER')==lit('202001170097684'))
    # input_indices_name = 'underwriting_gc_distribution_channel_tab_alias'
    #   
    # input_doc_type_name = input_indices_name+'/distribution_channel_tab'
    # 
    # document_count =documentcount(input_doc_type_name)
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # #esq5 = """{"query": {"bool":{"filter":{"terms" : {"NUM_REFERENCE_NUMBER":[201706230064229, 201803310241649, 201803310187910, 201804040274522, 201704270085806]}}}}}"""
    # #option('es.query', esq5).\
    # 
    # distribution_channel_tab_src = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_REFERENCE_NUMBER,TXT_TERTIARY_VERTICAL_NAME,TXT_POLICY_CURRENCY,TXT_INTERMEDIARY_TYPE,TXT_INTERMEDIARY_NAME,\
    # TXT_INTERMEDIARY_CODE,TXT_BUSINESS_SERVICING_CHANNEL,NUM_APPLICATION_NUMBER,TXT_TERTIARY_MO_CODE, \
    # TXT_BUSINESS_CHANNEL_TYPE').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    # optimization
    # join_cond = [nm_rfrnc_no_distinct.NUM_REFERENCE_NO==distribution_channel_tab_src.NUM_REFERENCE_NUMBER]
    # distribution_channel_tab = nm_rfrnc_no_distinct.join(distribution_channel_tab_src, join_cond)\
    #                                                .drop(nm_rfrnc_no_distinct.NUM_REFERENCE_NO)
    
    distribution_channel_tab = distribution_channel_tab_src
    distribution_channel_tab.createOrReplaceTempView('distribution_channel_tab')
    
    # distribution_channel_tab.count()
    
    ###############################
    dist_channel_1 = distribution_channel_tab.withColumnRenamed("NUM_REFERENCE_NUMBER", "NUM_REFERENCE_NO")\
    .withColumnRenamed("TXT_INTERMEDIARY_NAME", "PRODUCER_NAME")\
    .withColumnRenamed("TXT_INTERMEDIARY_CODE", "PRODUCER_CD")\
    .withColumnRenamed("NUM_APPLICATION_NUMBER", "APPLICATION_NO")\
    .withColumnRenamed("TXT_TERTIARY_MO_CODE", "PORTAL_FLAG")\
    .withColumnRenamed("TXT_BUSINESS_CHANNEL_TYPE", "PRODUCT_TYPE")\
    .withColumn('TXT_TERTIARY_VERTICAL_NAME', when((distribution_channel_tab.TXT_TERTIARY_VERTICAL_NAME.isNull())|\
                                                   (distribution_channel_tab.TXT_TERTIARY_VERTICAL_NAME==0), lit(1))\
    										  .otherwise(distribution_channel_tab.TXT_TERTIARY_VERTICAL_NAME))\
    .withColumnRenamed('DAT_APPLICATION_DATE','PROPOSAL_DATE')
    # .withColumnRenamed("TXT_BUSINESS_SERVICING_CHANNEL", "CHANNEL")\
    
    # dist_channel_2 = df_src_nm_rfrnce_no_distnct.join(dist_channel_1, 'NUM_REFERENCE_NO', 'left')
    dist_channel_2 = dist_channel_1.groupBy('NUM_REFERENCE_NO').agg(\
                                           sf.max('PRODUCER_NAME').alias('PRODUCER_NAME'),\
                                           sf.max('PRODUCER_CD').alias('PRODUCER_CD'),\
                                           sf.max('APPLICATION_NO').alias('APPLICATION_NO'),\
                                           sf.max('PORTAL_FLAG').alias('PORTAL_FLAG'),\
    									   sf.max('TXT_TERTIARY_VERTICAL_NAME').alias('TXT_TERTIARY_VERTICAL_NAME'),\
                                           sf.max('PRODUCT_TYPE').alias('PRODUCT_TYPE'),\
                                           sf.max('PROPOSAL_DATE').alias('PROPOSAL_DATE'))
    
    # dist_channel_3 = dist_channel_3.withColumn('NUM_REFERENCE_NO', \
    #                                            dist_channel_3.NUM_REFERENCE_NO.cast(DecimalType(15,0)).cast(StringType()))
    
    
    dist_channel_3 = dist_channel_2\
                           .withColumn('PORTAL_FLAG_tmp',\
                                 when(dist_channel_2.PORTAL_FLAG=="A_PORTAL",lit("P"))\
                                .when(dist_channel_2.PORTAL_FLAG=="C_PORTAL",lit("S"))\
                                .when(dist_channel_2.PORTAL_FLAG=="OFFLINE",lit("O"))\
                                .when((dist_channel_2.PORTAL_FLAG=="CORE")|(dist_channel_2.PORTAL_FLAG.isNull()),lit("C"))\
                                .otherwise("N"))\
                                .drop('PORTAL_FLAG')\
                                .withColumnRenamed('PORTAL_FLAG_tmp', 'PORTAL_FLAG')
    dist_channel_3.createOrReplaceTempView('distribution_channel_tab_1')
    ###############################
    acctrim = """select NUM_REFERENCE_NO, NUM_TRANSACTION_CONTROL_NO, trim(TXT_LEDGER_ACCOUNT_CD)  TXT_LEDGER_ACCOUNT_CD,
                        trim(TXT_DR_CR)  TXT_DR_CR, NUM_AMOUNT, trim(DAT_VOUCHER_DATE)  DAT_VOUCHER_DATE, trim(TXT_TRANSACTION_TYPE_CD)  TXT_TRANSACTION_TYPE_CD, 
                        trim(TXT_DIMENSION_4_VALUE_CD)  TXT_DIMENSION_4_VALUE_CD,
                        trim(TXT_DIMENSION_1_VALUE_CD)  TXT_DIMENSION_1_VALUE_CD,
                        trim(TXT_DIMENSION_7_VALUE_CD)  TXT_DIMENSION_7_VALUE_CD
                        --,NUM_FIN_YEAR,NUM_FIN_PERIOD,CONCAT(NUM_FIN_YEAR,'-',LPAD(NUM_FIN_PERIOD,2,'0'),'-01') AS FIN_YEAR_MONTH
                  from acc_general_ledger1"""
    
    flt_acc1 = sqlContext.sql(acctrim)
    flt_acc1.createOrReplaceTempView('acc_general_ledger2')
    
    
    acctrim = """select NUM_REFERENCE_NO, NUM_TRANSACTION_CONTROL_NO, TXT_LEDGER_ACCOUNT_CD,TXT_DR_CR, NUM_AMOUNT, 
                        DAT_VOUCHER_DATE, TXT_TRANSACTION_TYPE_CD, 
                        TXT_DIMENSION_4_VALUE_CD,
                        substr(ifnull(TXT_DIMENSION_1_VALUE_CD,TXT_DIMENSION_7_VALUE_CD), 1,6) as DISPLAY_PRODUCT_CD
                        --,NUM_FIN_YEAR,NUM_FIN_PERIOD,LAST_DAY(ADD_MONTHS(FIN_YEAR_MONTH,3)) AS YEAR_MONTH
                        , cast(LAST_DAY(DAT_VOUCHER_DATE) as string) AS ACCT_PERIOD_DATE
              from acc_general_ledger2"""
    
    flt_acc11 = sqlContext.sql(acctrim)
    flt_acc11.createOrReplaceTempView('acc_general_ledger')
    
    sqlContext.dropTempTable('acc_general_ledger0')
    sqlContext.dropTempTable('acc_general_ledger1')
    sqlContext.dropTempTable('acc_general_ledger2')
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_genprop_recordtype_mapper"} 
    recordtype_mapper=sqlContext.read.format("jdbc").options(**gscPythonOptions).load()
    
    # input_indices_name = 'reference_genprop_recordtype_mapper'
    # input_doc_type_name = input_indices_name+'/genprop_recordtype_mapper'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # recordtype_mapper = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    ################################################################
    gen_prop_src = gen_prop_information_tab.withColumn('TXT_MODEOFOPERATION',upper(gen_prop_information_tab.TXT_MODEOFOPERATION))
    gen_prop = gen_prop_src.join(recordtype_mapper, 'TXT_MODEOFOPERATION', 'left')
    gen_prop_1 = gen_prop\
                        .withColumn('PREM_EFF_DATE', when(gen_prop.DAT_ENDORSEMENT_EFF_DATE.isNull(), gen_prop.DAT_POLICY_EFF_FROMDATE)\
                                                     .otherwise(gen_prop.DAT_ENDORSEMENT_EFF_DATE))\
                        .withColumnRenamed("TXT_OFFICE_NAME", "BRANCH")\
                        .withColumnRenamed("TXT_CUSTOMER_ID", "CUSTOMER_ID")\
                        .withColumnRenamed("TXT_CUSTOMER_NAME", "CLIENT_NAME")\
                        .withColumnRenamed("TXT_COVERNOTE_NO", "COVERNOTENUMBER")\
                        .withColumnRenamed("NUM_PRODUCT_CODE", "PRODUCT_CD")\
                        .withColumnRenamed("TXT_ILPOS_POLICY_NUMBER", "WATTS_POLICY_NO")\
                        .withColumnRenamed("TXT_RELATIONSHIP_TYPE", "PARTNER_APPL_NO")\
                        .withColumnRenamed("NUM_REFERENCE_NUMBER", "NUM_REFERENCE_NO")\
                        .withColumnRenamed("TXT_PRODUCT_INDEX", "PRODUCT_INDEX")\
                        .withColumnRenamed("NUM_NET_PREMIUM_CURR", "ORG_CURR_COMMISSION")\
                        .withColumnRenamed("TXT_DISPLAYOFFICECODE", "POL_OFFICE_CD")\
                        .withColumnRenamed("TXT_REASON_FOR_NAME_TRANSFER", "TXT_LOB")\
                        .withColumnRenamed("NUM_DEPARTMENT_CODE", "DEPARTMENTCODE")\
                        .withColumnRenamed("DAT_POLICY_EFF_FROMDATE", "CERT_START_DATE")\
                        .withColumnRenamed("DAT_POLICY_EFF_TODATE", "CERT_END_DATE")\
                        .withColumnRenamed("DAT_ENDORSEMENT_EFF_DATE", "ENDORSEMENT_EFF_DATE")\
                        .withColumnRenamed("TXT_RE_INSURANCE_INWARD", "PROD_SOURCE_CD")\
                        .withColumnRenamed("TXT_SERV_TAX_EXCEMPT_CATEGORY", "SERVICE_TAX_EXEMPATION_FLAG")\
                        .withColumnRenamed("DAT_CUSTOMER_REFERENCE_DATE", "POLICY_ISSUANCE_DATE")\
                        .withColumn("COVER_NOTE_DATE", when(gen_prop.DAT_COVER_NOTE_DATE.isNull(), '1900-01-01')\
                                                       .otherwise(gen_prop.DAT_COVER_NOTE_DATE))\
                        .withColumn("PRDR_BRANCH_SUB_CD", substring(col("TXT_BRANCH_OFFICE_CODE"), -2, 2))\
                        .withColumn("BRANCH_OFF_CD", substring(col("TXT_BRANCH_OFFICE_CODE"), 2, 2))\
                        .withColumn("RURAL_FG", when(upper(gen_prop.TXT_SECTOR) == "RURAL","R").otherwise("U"))\
                        .withColumn("POLICY_COUNTER", \
                                    when((upper(gen_prop.TXT_MODEOFOPERATION) == "NEWPOLICY")|\
                                         (upper(gen_prop.TXT_MODEOFOPERATION) == "RENEWPOLICY"), 1).\
                                    when((upper(gen_prop.TXT_MODEOFOPERATION) == "NOTNILENDORSEMENT")|\
                                         (upper(gen_prop.TXT_MODEOFOPERATION) == "NILENDORSEMENT"),0).\
                                    when((upper(gen_prop.TXT_MODEOFOPERATION) == "CANCELLATIONENDORSEMENT"),-1)\
                                    .otherwise(0))\
                        .drop("TXT_SECTOR")\
                        .drop('TXT_MODEOFOPERATION')\
                        .drop('DAT_COVER_NOTE_DATE')
        
    gen_prop_2 = gen_prop_1.withColumn('ENDORSEMENT_END_DATE', when(gen_prop_1.ENDORSEMENT_EFF_DATE.isNull(), lit(None))\
                                       .otherwise(gen_prop_1.CERT_END_DATE))\
                           .withColumn('POL_INCEPT_DATE', gen_prop_1.CERT_START_DATE)\
                           .withColumn('POL_EXP_DATE', gen_prop_1.CERT_END_DATE)\
                           .withColumn('COVERNOTENUMBER', when((gen_prop_1.PRODUCT_INDEX == "N093891")|\
                                                               (gen_prop_1.PRODUCT_INDEX == "N162253")|\
                                                               (gen_prop_1.PRODUCT_INDEX == "N155266"),\
                                                                gen_prop_1.COVERNOTENUMBER))\
                           .withColumn('RI_INWARD_FLAG', when(upper(trim(gen_prop_1.PROD_SOURCE_CD))=='YES',lit('Y'))\
                                                         .otherwise(lit('N')))
            
    gen_prop_2 = gen_prop_2.withColumn('NUM_REFERENCE_NO', \
                                               gen_prop_2.NUM_REFERENCE_NO.cast(DecimalType(15,0)).cast(StringType()))
    										   
    gen_prop_2.createOrReplaceTempView('gen_prop_information_tab')
    ################################################################
    
    # gen_prop_information_tab.printSchema()
    
    propaddtrim = """select trim(TXT_CERTIFICATE_NO)  TXT_CERTIFICATE_NO, trim(TXT_RENL_CERT_NO)  TXT_RENL_CERT_NO, TXT_EFF_DT_SEQ_NO, NUM_REFERERANCE_NO 
                     from uw_proposal_addl_info0 
                     GROUP BY trim(TXT_CERTIFICATE_NO), trim(TXT_RENL_CERT_NO), TXT_EFF_DT_SEQ_NO, NUM_REFERERANCE_NO"""
    
    flt_prop = sqlContext.sql(propaddtrim)
    flt_prop.createOrReplaceTempView('uw_proposal_addl_info')
    
    #--- Fetched the Premium and commission amount - acc_general_ledger
    
    query_1_changed = """select     
    gl.NUM_REFERENCE_NO
    ,max((case when (gl.txt_ledger_account_cd in ("""+str_gl_code_ri+""")) 
    then 0 else gl.NUM_TRANSACTION_CONTROL_NO 
    end)) as NUM_TRANSACTION_CONTROL_NO
    --,gl.NUM_TRANSACTION_CONTROL_NO  as NUM_TRANSACTION_CONTROL_NO
    ,gpt.TXT_POLICY_NO_CHAR       as POLICY_NO
    ,uw.TXT_CERTIFICATE_NO as CERTIFICATE_NO 
    ,uw.txt_renl_cert_no AS  RENL_CERT_NO 
    ,uw.txt_eff_dt_seq_no AS EFF_DT_SEQ_NO
    ,substr(gl.txt_dimension_4_value_cd, 1, 2)      as MAJOR_LINE_CD
    ,substr(gl.txt_dimension_4_value_cd, 3, 2)      as MINOR_LINE_CD
    ,substr(gl.txt_dimension_4_value_cd, 5, 3)      as CLASS_PERIL_CD
    ,gl.DISPLAY_PRODUCT_CD
    ,gl.txt_dimension_4_value_cd -- added this column to use it in join with dw_policy_information in next tab
    
    ,gl.ACCT_PERIOD_DATE         as ACCT_PERIOD_DATE
    --,substr(P.txt_currency_cd, 1, 3)   as ORIG_CURR_CD
    ,gl.dat_voucher_date AS  DAT_VOUCHER_DATE
    ,gpt.PREM_EFF_DATE as PREM_EFF_DATE
    ,cast(last_day(CASE when gpt.PREM_EFF_DATE > gl.ACCT_PERIOD_DATE then gpt.PREM_EFF_DATE
          else gl.ACCT_PERIOD_DATE end) as string) as REPORT_ACCOUNTING_PERIOD
    ,(SUm(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_prem+"""))
                  and gl.txt_dr_cr = 'CR' then 
                  IFNULL(gl.Num_amount,0) 
                else 0  
                END)  -
      SUm(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_prem+"""))
                                and gl.txt_dr_cr = 'DR' then 
                                        IFNULL(gl.Num_amount,0) 
                            else 0  
                            END ))  AS PREMIUM_AMOUNT_INR_WITHOUT_TAX
    
    ,(SUm(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_comm+""")) 
    			  and gl.txt_dr_cr = 'DR' then 
    			  IFNULL(gl.Num_amount,0) 
    			  else 0  
    			  END)  -
      SUm(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_comm+"""))  
    						  and gl.txt_dr_cr = 'CR' then 
    						  IFNULL(gl.Num_amount,0) 
    						  else 0  
    						  END)) as COMMISSION_INR 
    						  
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pobl+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END )  -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pobl+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END )) As PREM_OBL
    
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pvqs+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END ) - 
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pvqs+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END )) AS PREM_CEEDED_VQST
    
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_psur+""") and gl.txt_dr_cr = 'DR'  
                       then (gl.Num_amount) else 0  END ) -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_psur+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END )) AS PREM_CEEDED_SURPLUS
    
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pter+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END )  - 
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pter+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END )) AS PREM_CEEDED_TERRISIOM
     
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pcom+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END ) -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pcom+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END )) AS PREM_CEEDED_AIG_COMBINED
     
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pfac+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END )  -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pfac+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END )) AS PREM_CEEDED_FAC
    
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pxol+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END ) - 
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_pxol+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END )) AS PREM_CEEDED_XOL
    
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_ptty+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END ) -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_ptty+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END )) AS PREM_CEEDED_TTY
                      
                      
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_cobl+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END ) -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_cobl+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END )) As COMM_OBL
    
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_cvqs+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END ) -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_cvqs+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END )) AS COMM_CEEDED_VQST
    
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_csur+""") and gl.txt_dr_cr = 'CR'  
                       then (gl.Num_amount) else 0  END ) -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_csur+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END )) AS COMM_CEEDED_SURPLUS
    
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_cter+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END ) -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_cter+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END )) AS COMM_CEEDED_TERRISIOM
    
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_ccom+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END ) -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_ccom+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END )) AS COMM_CEEDED_AIG_COMBINED
    
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_cfac+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END ) -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_cfac+""") and gl.txt_dr_cr = 'DR' 
             then (gl.Num_amount) else 0  END )) AS COMM_CEEDED_FAC
    
           ,(SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_ctty+""") and gl.txt_dr_cr = 'CR' 
                       then (gl.Num_amount) else 0  END ) -
             SUm (case when gl.txt_ledger_account_cd in ("""+str_gl_code_ctty+""") and gl.txt_dr_cr = 'DR' 
                       then (gl.Num_amount) else 0  END )) AS COMM_UNEARNED_TTY
    
    ,(SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_srvc_tax+""")) 
                  and gl.txt_dr_cr = 'CR' then 									
                                        IFNULL(gl.Num_amount,0) 
    									else 0 
    									END) - 
      SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_srvc_tax+""")) 
                                        and gl.txt_dr_cr = 'DR' then 									
                                        IFNULL(gl.Num_amount,0) 
    									else 0 
    									END)) as SERVICE_TAX
    
    									
    ---------------------------------- GST logic starts here
    ,(SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_igst+"""))  
    			 and gl.txt_dr_cr = 'CR' then 
                 IFNULL(gl.Num_amount,0) else 0 END) - 
      SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_igst+"""))  
    			 and gl.txt_dr_cr = 'DR' then 
                 IFNULL(gl.Num_amount,0) else 0 END)) as IGST_AMOUNT
    
    ,(SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_sgst+"""))  
    			 and gl.txt_dr_cr = 'CR' then 
                 IFNULL(gl.Num_amount,0) else 0 END) - 
      SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_sgst+""")) 
    			 and gl.txt_dr_cr = 'DR' then 
                 IFNULL(gl.Num_amount,0) else 0 END)) as SGST_AMOUNT
    
    ,(SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_cgst+"""))
                 and gl.txt_dr_cr = 'CR' then 
                 IFNULL(gl.Num_amount,0) else 0 END) -
      SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_cgst+"""))
                 and gl.txt_dr_cr = 'DR' then 
                 IFNULL(gl.Num_amount,0) else 0 END)) as CGST_AMOUNT
    
    ,0 as UTGST_AMOUNT
    ----------------------------------gst logic ended
    
    --------------- NEW TDS_ON_COMM logic shared by KISHAN on 2019-05-20
    ,(SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_tds+""")) and gl.txt_dr_cr = 'CR'
                                        then IFNULL(gl.Num_amount,0) else 0 END) -
      SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_tds+""")) and gl.txt_dr_cr = 'DR'
                                        then IFNULL(gl.Num_amount,0) else 0 END)) as TDS_ON_COMM
    ,(SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_pol_stmp_duty+""")) and gl.txt_dr_cr = 'DR'
                                        then IFNULL(gl.Num_amount,0) else 0 END) -
      SUM(case when (gl.txt_ledger_account_cd in ("""+str_gl_code_pol_stmp_duty+""")) and gl.txt_dr_cr = 'CR'
                                        then IFNULL(gl.Num_amount,0) else 0 END)) as POL_STAMP_DUTY_AMT
    
    ,0 AS COMMISSIONABLE_PREMIUM
    ,max(dist.txt_tertiary_vertical_name) as TXT_TERTIARY_VERTICAL_NAME
    ,max(dist.PRODUCER_NAME) as PRODUCER_NAME
    ,max(dist.PRODUCER_CD) as PRODUCER_CD
    ,max(dist.APPLICATION_NO) as APPLICATION_NO
    ,max(dist.PORTAL_FLAG) as PORTAL_FLAG
    ,max(dist.PRODUCT_TYPE) as PRODUCT_TYPE
    ,max(gpt.BRANCH) as BRANCH
    ,max(gpt.CUSTOMER_ID) as CUSTOMER_ID
    ,max(gpt.CLIENT_NAME) as CLIENT_NAME
    ,max(gpt.RECORD_TYPE_DESC) as RECORD_TYPE_DESC
    ,max(gpt.PRODUCT_CD) as PRODUCT_CD
    ,max(gpt.WATTS_POLICY_NO) as WATTS_POLICY_NO
    ,max(gpt.PARTNER_APPL_NO) as PARTNER_APPL_NO
    ,max(gpt.PRODUCT_INDEX) as PRODUCT_INDEX
    ,max(gpt.ORG_CURR_COMMISSION) as ORG_CURR_COMMISSION
    ,max(gpt.POL_OFFICE_CD) as POL_OFFICE_CD
    ,max(gpt.TXT_LOB) as TXT_LOB
    ,max(gpt.DEPARTMENTCODE) as DEPARTMENTCODE
    ,max(gpt.CERT_START_DATE) as CERT_START_DATE
    ,max(gpt.CERT_END_DATE) as CERT_END_DATE
    ,max(gpt.ENDORSEMENT_EFF_DATE) as ENDORSEMENT_EFF_DATE
    ,max(gpt.RI_INWARD_FLAG) as RI_INWARD_FLAG
    ,max(gpt.SERVICE_TAX_EXEMPATION_FLAG) as SERVICE_TAX_EXEMPATION_FLAG
    ,max(gpt.COVER_NOTE_DATE) as COVER_NOTE_DATE
    ,max(gpt.PRDR_BRANCH_SUB_CD) as PRDR_BRANCH_SUB_CD
    ,max(gpt.TXT_BRANCH_OFFICE_CODE) as TXT_BRANCH_OFFICE_CODE
    ,max(gpt.BRANCH_OFF_CD) as BRANCH_OFF_CD
    ,max(gpt.RURAL_FG) as RURAL_FG
    ,max(gpt.POLICY_COUNTER) as POLICY_COUNTER
    ,max(gpt.RECORD_TYPE_CD) as RECORD_TYPE_CD
    ,max(gpt.ENDORSEMENT_END_DATE) as ENDORSEMENT_END_DATE
    ,max(gpt.POL_INCEPT_DATE) as POL_INCEPT_DATE
    ,max(gpt.POL_EXP_DATE) as POL_EXP_DATE
    ,max(gpt.COVERNOTENUMBER) as COVERNOTENUMBER
    ,max(gpt.POLICY_ISSUANCE_DATE) as POLICY_ISSUANCE_DATE
    ,max(dist.PROPOSAL_DATE) as PROPOSAL_DATE
    
    from acc_general_ledger gl
    inner join gen_prop_information_tab gpt
    on gl.NUM_REFERENCE_NO = gpt.NUM_REFERENCE_NO
    left join distribution_channel_tab_1 dist
    on  gl.NUM_REFERENCE_NO = dist.NUM_REFERENCE_NO
    left join uw_proposal_addl_info uw
    on  gpt.NUM_REFERENCE_NO =  uw.NUM_REFERERANCE_NO 
    group by       gl.NUM_REFERENCE_NO
                  ,gl.NUM_TRANSACTION_CONTROL_NO
                  ,gpt.TXT_POLICY_NO_CHAR
                  ,uw.TXT_CERTIFICATE_NO
                  ,uw.txt_renl_cert_no
                  ,uw.txt_eff_dt_seq_no
                  ,gl.txt_dimension_4_value_cd
                  ,gl.DISPLAY_PRODUCT_CD
                  ,gl.ACCT_PERIOD_DATE 
                  --,P.txt_currency_cd
                  ,gl.dat_voucher_date
                  ,PREM_EFF_DATE
                  ,REPORT_ACCOUNTING_PERIOD
    			  
    """
          
    
    premium_transaction_1_c = sqlContext.sql(query_1_changed)
    
    premium_transaction_1_c11 = premium_transaction_1_c.withColumn('ORG_PREM_AMT_WITHOUT_TAX', \
                     premium_transaction_1_c.PREMIUM_AMOUNT_INR_WITHOUT_TAX*premium_transaction_1_c.TXT_TERTIARY_VERTICAL_NAME)\
    			  .drop('TXT_TERTIARY_VERTICAL_NAME')\
                   .withColumn('ORIG_CURR_CD', lit('INR'))\
                   .withColumn('CLASS_PERIL_CD', when((premium_transaction_1_c.ACCT_PERIOD_DATE >= '2020-04-30') &\
    			                                     (premium_transaction_1_c.MAJOR_LINE_CD == '01') &\
    			                                     ((premium_transaction_1_c.CLASS_PERIL_CD == '251')|\
    												  (premium_transaction_1_c.CLASS_PERIL_CD == '253')),lit('251'))\
    										   .when((premium_transaction_1_c.ACCT_PERIOD_DATE >= '2020-04-30') &\
    			                                     (premium_transaction_1_c.MAJOR_LINE_CD=='01') &\
    			                                     ((premium_transaction_1_c.CLASS_PERIL_CD == '257')|\
    												  (premium_transaction_1_c.CLASS_PERIL_CD == '258')),lit('257'))\
    			                               .otherwise(premium_transaction_1_c.CLASS_PERIL_CD))
    
    premium_transaction_1_c1 = premium_transaction_1_c11.withColumn('CLASS_PERIL_CD', \
                                                                    when((col('ACCT_PERIOD_DATE') >= '2020-04-30') &\
    		                                                             (col('MAJOR_LINE_CD') != '01'),lit(''))\
    		                                                        .otherwise(col('CLASS_PERIL_CD')))
    
    # premium_transaction_1_c1 = premium_transaction_1_c.withColumn('ORG_PREM_AMT_WITHOUT_TAX', \
    #                  premium_transaction_1_c.PREMIUM_AMOUNT_INR_WITHOUT_TAX*premium_transaction_1_c.TXT_TERTIARY_VERTICAL_NAME)\
    #                .drop('TXT_TERTIARY_VERTICAL_NAME')\
    #                .withColumn('ORIG_CURR_CD', lit('INR'))\
    
    #premium_transaction_1_c.repartition('POLICY_NO', 'CERTIFICATE_NO', 'RENL_CERT_NO', \
    #                                     'EFF_DT_SEQ_NO', 'NUM_REFERENCE_NO', 'txt_dimension_4_value_cd')
    premium_transaction_1_c1.createOrReplaceTempView('premium_transaction_1_c1')
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "policy_gc_uw_gst_dtls_tab",
        "partitionColumn":"row_num",
        "server.port":"1160-1180",
        "partitions":8} 
    gst_tab_src=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_REFER_NO', 'NUM_IGST_RATE', 'NUM_SGST_RATE', 'NUM_CGST_RATE')
    
    # \
    # .filter(col('NUM_REFER_NO')==lit('202001170097684'))
    
    # gst_tab_src.count()
    
    # #esq_gst = """{"query": {"term":{"NUM_REFER_NO": "202004220000842"}}}"""
    # esq_gst = ''
    # 
    # input_indices_name = 'policy_gc_uw_gst_dtls_tab_alias'
    # input_doc_type_name = input_indices_name+'/uw_gst_dtls_tab'
    # 
    # document_count =documentcount(input_doc_type_name)
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # gst_tab_src = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_REFER_NO, NUM_IGST_RATE, NUM_SGST_RATE, NUM_CGST_RATE').\
    # option('es.query', esq_gst).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    # optimization
    # join_cond = [nm_rfrnc_no_distinct.NUM_REFERENCE_NO==gst_tab_src.NUM_REFER_NO]
    # gst_tab = nm_rfrnc_no_distinct.join(gst_tab_src, join_cond)
    
    gst_tab = gst_tab_src
    gst_tab_1 = gst_tab.groupBy('NUM_REFER_NO').agg(\
                                 sf.max('NUM_IGST_RATE').alias('IGST_RATE'),\
    							 sf.max('NUM_SGST_RATE').alias('SGST_RATE'),\
    							 sf.max('NUM_CGST_RATE').alias('CGST_RATE'))
    						 
    gst_tab_2 = gst_tab_1.withColumnRenamed('NUM_REFER_NO', 'NUM_REFERENCE_NO')\
                         .withColumn('IGST_RATE', when(gst_tab_1.IGST_RATE.isNull(), 18).otherwise(gst_tab_1.IGST_RATE))\
    					 .withColumn('SGST_RATE', when(gst_tab_1.SGST_RATE.isNull(), 9).otherwise(gst_tab_1.SGST_RATE))\
    					 .withColumn('CGST_RATE', when(gst_tab_1.CGST_RATE.isNull(), 9).otherwise(gst_tab_1.CGST_RATE))
    				 
    gst_tab_3 = gst_tab_2.withColumn('UTGST_RATE', lit(0))
    
    # below join code commented and gst fields are hardcoded to avoid processing 212 cr records of gst table. it can be processed in monthly job
    premium_transaction_1_e = premium_transaction_1_c1.join(gst_tab_3, 'NUM_REFERENCE_NO', 'left')
    #premium_transaction_1_e = premium_transaction_1_c1.withColumn('IGST_RATE', lit(18))\
    #                                                  .withColumn('SGST_RATE', lit(9))\
    #                                                  .withColumn('CGST_RATE', lit(9))
    premium_transaction_1_e.createOrReplaceTempView('premium_transaction_1_e')
    
    sqlContext.dropTempTable('acc_general_ledger')
    sqlContext.dropTempTable('gen_prop_information_tab')
    sqlContext.dropTempTable('uw_proposal_addl_info')
    
    #--- part 2
    
    query_1 = """select 
             V1.NUM_REFERENCE_NO
            ,MAX(v1.NUM_TRANSACTION_CONTROL_NO) as NUM_TRANSACTION_CONTROL_NO
            ,V1.POLICY_NO
            ,V1.CERTIFICATE_NO
            ,V1.RENL_CERT_NO
            ,V1.EFF_DT_SEQ_NO
            ,V1.MAJOR_LINE_CD
            ,V1.MINOR_LINE_CD
            ,V1.CLASS_PERIL_CD
            ,V1.ACCT_PERIOD_DATE
            ,V1.PREM_EFF_DATE
            ,V1.REPORT_ACCOUNTING_PERIOD
            ,V1.WATTS_POLICY_NO
            ,MAX(V1.ORIG_CURR_CD) as ORIG_CURR_CD
            ,V1.DAT_VOUCHER_DATE as TIMESTAMP
            ,V1.DISPLAY_PRODUCT_CD
            ,SUM(V1.PREMIUM_AMOUNT_INR_WITHOUT_TAX) as PREMIUM_AMOUNT_INR_WITHOUT_TAX
            ,SUM(V1.COMMISSION_INR) AS COMMISSION_INR
            ,SUM(V1.SERVICE_TAX) AS SERVICE_TAX
            ,SUM(V1.COMMISSIONABLE_PREMIUM) AS COMMISSIONABLE_PREMIUM
            ,SUM(V1.ORG_CURR_COMMISSION) AS ORG_CURR_COMMISSION
            --,SUM(V1.CONVERT_RATE) AS CONVERT_RATE
            ,SUM(V1.ORG_PREM_AMT_WITHOUT_TAX) AS ORG_PREM_AMT_WITHOUT_TAX
            ,SUM(V1.PREM_OBL) AS OBLIGATORY_PREM
            ,SUM(V1.PREM_CEEDED_VQST) AS CEDED_VQST_PREM
            ,SUM(V1.PREM_CEEDED_SURPLUS) AS CEDED_SURPLUS_TREATY_PREM
            ,SUM(V1.PREM_CEEDED_TERRISIOM) AS CEDED_TERRORISM_POOL_PREM
            ,SUM(V1.PREM_CEEDED_AIG_COMBINED) AS  CEDED_AIG_COMBINED_PREM
            ,SUM(V1.PREM_CEEDED_FAC) AS  FAC_PREM
            ,SUM(V1.PREM_CEEDED_XOL) AS XOL_PREM
            ,SUM(V1.PREM_CEEDED_TTY) AS  TTY_PREM
            ,SUM(V1.COMM_OBL) AS OBLIGATORY_COMM
            ,SUM(V1.COMM_CEEDED_VQST) AS CEDED_VQST_COMM
            ,SUM(V1.COMM_CEEDED_SURPLUS) AS CEDED_SURPLUS_TREATY_COMM
            ,SUM(V1.COMM_CEEDED_TERRISIOM) AS CEDED_TERRORISM_POOL_COMM
            ,SUM(V1.COMM_CEEDED_AIG_COMBINED) AS CEDED_AIG_COMBINED_COMM
            ,SUM(V1.COMM_CEEDED_FAC) AS FAC_COMM
            ,SUM(V1.COMM_UNEARNED_TTY) AS TTY_COMM
            ,sum(V1.IGST_AMOUNT) as IGST_AMOUNT
            ,sum(V1.SGST_AMOUNT) as SGST_AMOUNT
            ,sum(V1.CGST_AMOUNT) as CGST_AMOUNT
            ,sum(V1.UTGST_AMOUNT) as UTGST_AMOUNT
            ,sum(V1.TDS_ON_COMM) as TDS_ON_COMM
            ,sum(V1.POL_STAMP_DUTY_AMT) as POL_STAMP_DUTY_AMT
            ,max(V1.IGST_RATE) as IGST_RATE
            ,max(V1.SGST_RATE) as SGST_RATE
            ,max(V1.CGST_RATE) as CGST_RATE
            ,max(V1.PRODUCER_NAME) as PRODUCER_NAME
            ,max(V1.PRODUCER_CD) as PRODUCER_CD
            ,max(V1.APPLICATION_NO) as APPLICATION_NO
            ,max(V1.PORTAL_FLAG) as PORTAL_FLAG
            ,max(V1.PRODUCT_TYPE) as PRODUCT_TYPE
            ,max(V1.BRANCH) as BRANCH
            ,max(V1.CUSTOMER_ID) as CUSTOMER_ID
            ,max(V1.CLIENT_NAME) as CLIENT_NAME
            ,max(V1.RECORD_TYPE_DESC) as RECORD_TYPE_DESC
            ,max(V1.PRODUCT_CD) as PRODUCT_CD
            ,max(V1.PARTNER_APPL_NO) as PARTNER_APPL_NO
            ,max(V1.PRODUCT_INDEX) as PRODUCT_INDEX
            ,max(V1.POL_OFFICE_CD) as POL_OFFICE_CD
            ,max(V1.TXT_LOB) as TXT_LOB
            ,max(V1.DEPARTMENTCODE) as DEPARTMENTCODE
            ,max(V1.CERT_START_DATE) as CERT_START_DATE
            ,max(V1.CERT_END_DATE) as CERT_END_DATE
            ,max(V1.ENDORSEMENT_EFF_DATE) as ENDORSEMENT_EFF_DATE
            ,max(V1.RI_INWARD_FLAG) as RI_INWARD_FLAG
            ,max(V1.SERVICE_TAX_EXEMPATION_FLAG) as SERVICE_TAX_EXEMPATION_FLAG
            ,max(V1.COVER_NOTE_DATE) as COVER_NOTE_DATE
            ,max(V1.PRDR_BRANCH_SUB_CD) as PRDR_BRANCH_SUB_CD
            ,max(V1.TXT_BRANCH_OFFICE_CODE) as TXT_BRANCH_OFFICE_CODE
            ,max(V1.BRANCH_OFF_CD) as BRANCH_OFF_CD
            ,max(V1.RURAL_FG) as RURAL_FG
            ,max(V1.POLICY_COUNTER) as POLICY_COUNTER
            ,max(V1.RECORD_TYPE_CD) as RECORD_TYPE_CD
            ,max(V1.ENDORSEMENT_END_DATE) as ENDORSEMENT_END_DATE
            ,max(V1.POL_INCEPT_DATE) as POL_INCEPT_DATE
            ,max(V1.POL_EXP_DATE) as POL_EXP_DATE
            ,max(V1.COVERNOTENUMBER) as COVERNOTENUMBER
            ,max(V1.POLICY_ISSUANCE_DATE) as POLICY_ISSUANCE_DATE
            ,max(V1.PROPOSAL_DATE) as PROPOSAL_DATE
    
    from premium_transaction_1_e V1
    GROUP BY V1.NUM_REFERENCE_NO
            ,V1.POLICY_NO
            ,V1.CERTIFICATE_NO 
            ,V1.RENL_CERT_NO 
            ,V1.EFF_DT_SEQ_NO
            ,V1.MAJOR_LINE_CD
            ,V1.MINOR_LINE_CD
            ,V1.CLASS_PERIL_CD
            ,V1.DISPLAY_PRODUCT_CD
            ,V1.ACCT_PERIOD_DATE
            ,V1.DAT_VOUCHER_DATE
            ,V1.PREM_EFF_DATE
            ,V1.REPORT_ACCOUNTING_PERIOD
            ,V1.WATTS_POLICY_NO
                  """
          
    
    prem_reg_d = sqlContext.sql(query_1)
    
    prem_reg_e = prem_reg_d.withColumn('NPW', (prem_reg_d.PREMIUM_AMOUNT_INR_WITHOUT_TAX - \
                                              (prem_reg_d.OBLIGATORY_PREM + prem_reg_d.CEDED_VQST_PREM + prem_reg_d.CEDED_SURPLUS_TREATY_PREM + \
                                               prem_reg_d.CEDED_TERRORISM_POOL_PREM + prem_reg_d.CEDED_AIG_COMBINED_PREM + prem_reg_d.FAC_PREM + \
                                               prem_reg_d.XOL_PREM + prem_reg_d.TTY_PREM)))\
                           .withColumn('TOTAL_COMMISSION', (prem_reg_d.OBLIGATORY_COMM +  prem_reg_d.CEDED_VQST_COMM + prem_reg_d.CEDED_SURPLUS_TREATY_COMM + prem_reg_d.CEDED_TERRORISM_POOL_COMM +\
                                                            prem_reg_d.CEDED_AIG_COMBINED_COMM + prem_reg_d.FAC_COMM + prem_reg_d.TTY_COMM))\
                           .withColumn('PREMIUM_WITH_TAX', prem_reg_d.PREMIUM_AMOUNT_INR_WITHOUT_TAX+\
                                                           prem_reg_d.CGST_AMOUNT+\
                                                           prem_reg_d.SGST_AMOUNT+\
                                                           prem_reg_d.IGST_AMOUNT+\
                                                           prem_reg_d.SERVICE_TAX)
    
    
    #####################################################################
    
    prem_reg_6 = prem_reg_e
    prem_reg_7 = prem_reg_6.drop('NUM_ENDORSEMENT_SERVICE_TAX')\
                           .drop('NUM_SERVICE_TAX')\
                           .withColumnRenamed('SERVICE_TAX','SERVICE_TAX_AMOUNT')\
                           .withColumn('REPORT_DATE', lit(report_date))
    
    # added on 2019-09-24 to give IRDA related fields in Premium register summary report
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_pr_irda_mapper"} 
    irda_mpr=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('UIN_NO', 'IRDA_LOB', 'IIB_LOB', 'DISPLAY_PRODUCT_CD')
    
    # input_indices_name = 'reference_pr_irda_mapper'
    # input_doc_type_name = input_indices_name+'/pr_irda_mapper'
    # 
    # document_count =documentcount(input_doc_type_name)
    # 
    # irda_mpr = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','UIN_NO, IRDA_LOB, IIB_LOB, DISPLAY_PRODUCT_CD').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    irda_mpr_1A = irda_mpr.groupBy('DISPLAY_PRODUCT_CD')\
                         .agg(sf.max('UIN_NO').alias('UIN_NO'),\
                              sf.max('IRDA_LOB').alias('IRDA_LOB'),\
                              sf.max('IIB_LOB').alias('IIB_LOB'))
    
    irda_mpr_1 = irda_mpr_1A.withColumn('UIN_NO', regexp_replace('UIN_NO', '\n', ' '))\
                            .withColumn('IRDA_LOB', regexp_replace('IRDA_LOB', '\n', ' '))\
                            .withColumn('IIB_LOB', regexp_replace('IIB_LOB', '\n', ' '))\
    
    prem_reg_8 = prem_reg_7.join(irda_mpr_1, 'DISPLAY_PRODUCT_CD', 'left')
    
    prem_reg_9A = prem_reg_8.withColumn('RECORD_TYPE_DESC', when((((prem_reg_8.RECORD_TYPE_DESC=='NEW BUSINESS') |\
                                                                (prem_reg_8.RECORD_TYPE_DESC=='RENEWAL BUSINESS')) &\
                                                                ((prem_reg_8.PREMIUM_AMOUNT_INR_WITHOUT_TAX < 0)|\
                                                                 ((prem_reg_8.PREMIUM_AMOUNT_INR_WITHOUT_TAX==0)&\
                                                                  (col('CGST_AMOUNT')+col('IGST_AMOUNT')+col('SGST_AMOUNT')<0)))),\
                                                                  lit('CANCELLATION'))\
                                                           .otherwise(prem_reg_8.RECORD_TYPE_DESC))\
                           .drop('POLICY_COUNTER')
    
    prem_reg_9B = prem_reg_9A.withColumn('RECORD_TYPE_CD', when(prem_reg_9A.RECORD_TYPE_DESC=='CANCELLATION', lit('X'))\
                                                           .otherwise(prem_reg_9A.RECORD_TYPE_CD))\
    
    # separate out records having premium amount 0 and then rank the data 
    # so that we can assign policy counter as 0 to records having 0 premium and proper ranking to non zero premium records.
    prem_reg_9C = prem_reg_9B.filter(prem_reg_9B.PREMIUM_AMOUNT_INR_WITHOUT_TAX == 0)
    prem_reg_9D = prem_reg_9B.filter(prem_reg_9B.PREMIUM_AMOUNT_INR_WITHOUT_TAX != 0)
    
    prem_reg_10A = prem_reg_9C.withColumn('SL_NO', lit(9999))\
                              .withColumn('POLICY_COUNTER', lit('0'))\
    
    prem_reg_10B = prem_reg_9D.withColumn('SL_NO', sf.row_number()\
                               .over(Window.partitionBy('NUM_REFERENCE_NO','TIMESTAMP','RECORD_TYPE_CD')\
                               .orderBy(prem_reg_9D.DISPLAY_PRODUCT_CD.asc())))
    
    prem_reg_10C = prem_reg_10B.withColumn('POLICY_COUNTER', when((prem_reg_10B.SL_NO==1)& \
                                                                (prem_reg_10B.RECORD_TYPE_CD=='E'), lit('0'))\
                                                          .when((prem_reg_10B.SL_NO==1)& \
                                                               (prem_reg_10B.RECORD_TYPE_CD == 'X'), lit('-1'))\
                                                          .when((prem_reg_10B.SL_NO==1)& \
                                                               ((prem_reg_10B.RECORD_TYPE_CD=='N')|\
                                                                (prem_reg_10B.RECORD_TYPE_CD=='R')), lit('1'))\
                                                         .otherwise(lit('0')))
    
    prem_reg_10D = prem_reg_10C.withColumn('POLICY_COUNTER', when((prem_reg_10C.PRODUCT_CD.cast(IntegerType())==1502)&\
                                                                (prem_reg_10C.CERTIFICATE_NO != '000000'), lit('0'))\
                                                          .otherwise(prem_reg_10C.POLICY_COUNTER))
    
    prem_reg_10E = prem_reg_10D.withColumn('POLICY_COUNTER', when(((prem_reg_10D.PRODUCT_CD.cast(IntegerType())==4269)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==4270)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==4266)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==5602)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==5601)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2869)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2877)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2876)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2876)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2878)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2878)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2889)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2895))&\
                                                                (prem_reg_10D.CERTIFICATE_NO.cast(IntegerType()) == 1) &\
                                                                (prem_reg_10D.RECORD_TYPE_DESC == 'NEW BUSINESS') &\
                                                                (prem_reg_10D.PREMIUM_AMOUNT_INR_WITHOUT_TAX > 0)  , lit('1'))\
                                                              .when(((prem_reg_10D.PRODUCT_CD.cast(IntegerType())==4269)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==4270)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==4266)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==5602)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==5601)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2869)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2877)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2876)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2876)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2878)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2878)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2889)|\
                                                                   (prem_reg_10D.PRODUCT_CD.cast(IntegerType())==2895)), lit('0'))
                                                          .otherwise(prem_reg_10D.POLICY_COUNTER))
    
    prem_reg_11 = prem_reg_10E.unionAll(prem_reg_10A)

    # reading product_name
    
    gscPythonOptions = {"url": gpdb_url,"user": gpdb_user,"password": gpdb_pass,
        "dbschema": "public","dbtable": "underwriting_gc_uw_product_master"} 
    prod_master=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('PRODUCTCODE','PRODUCTNAME','DEPARTMENTCODE')
    
    prod_master_1 = prod_master\
                        .withColumnRenamed("PRODUCTCODE", "PRODUCT_CD")\
                        .withColumnRenamed("PRODUCTNAME", "PRODUCT_NAME")
    
    prod_master_2 = prod_master_1.groupBy('PRODUCT_CD')\
                                 .agg(sf.max('PRODUCT_NAME').alias('PRODUCT_NAME'),\
                                      sf.max('DEPARTMENTCODE').alias('DEPARTMENTCODE'))
    prem_reg_11A = prem_reg_11.drop('DEPARTMENTCODE')
    prem_reg_11B = prem_reg_11A.join(prod_master_2, 'PRODUCT_CD', 'left')

    prem_reg_11C = prem_reg_11B\
    .withColumn('SUM_INSURED', lit(None))\
    .withColumn("MMCP", (sf.concat(sf.col('MAJOR_LINE_CD'),sf.col('MINOR_LINE_CD'), sf.col('CLASS_PERIL_CD'))))\
    .withColumn('COMMISSIONPERCENT', lit(None))
    prem_reg_B = prem_reg_11C

    prem_reg_B.printSchema()

    # prem_reg_B.count()
    
    #####################################################################    
    
    sqlContext.dropTempTable('acc_general_ledger')
    sqlContext.dropTempTable('gen_prop_information_tab')
    sqlContext.dropTempTable('uw_proposal_addl_info')
    sqlContext.dropTempTable('premium_transaction_1_c')
    sqlContext.dropTempTable('distribution_channel_tab')
    #prem_reg_B.printSchema()
    try:
        prem_reg_B.write.option("truncate", "true").format("greenplum").option("dbtable","premium_register_part_1").option('dbschema','staging').option("server.port","1160-1180").option("url",gpdb_url_uat).option("user", gpdb_user).option("password",gpdb_pass).mode('overwrite').save()
    except Exception as e:
        x = e
        print(x)
        print("Spark code : first job aborted @")
        ts = time.gmtime()
        print(time.strftime("%Y-%m-%d %H:%M:%S", ts))
        sys.exit(-1)
    else: 
        x = 200
    print(x)
    print("first job ended @")
    ts = time.gmtime()
    print(time.strftime("%Y-%m-%d %H:%M:%S", ts))

    # 20210520 - sagar - SUM_INSURED and commissionPercent part
    # 2861
    nrn_list = [202001240101477,202001300626724,202002050103637]
    #2869
    nrn_list = [202104100037148]
    #2875
    nrn_list = [202104010030802,202104010035185,202104090117195]
    #2876
    nrn_list = [202103310211364,202103310214453,202104010011572]
    #2878
    nrn_list = [202009100164652,202011090186524,202011090188150]
    #2889
    nrn_list = [202002190384138,202008310131803,202011300067118,202101040210237]
    #2895
    nrn_list = [202103060000459,202103130067086,202104140066774,202104120090497]
    #2895
    nrn_list = [202009220189764]
    #2888
    nrn_list = [202103190048601,202103230044877,202104090011961,202104140039050,202104190043725,202104080061280,202104090021771,202104190081322,202104260119430]

    gscPythonOptions = {"url": gpdb_url_uat,"user": gpdb_user,"password": gpdb_pass,
        "dbschema": "staging","dbtable": "premium_register_part_1",
        "partitionColumn":"sl_no","server.port":"1160-1180","partitions":4} 
    
    pr_part_1 = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('DISPLAY_PRODUCT_CD','PRODUCT_CD','NUM_REFERENCE_NO','NUM_TRANSACTION_CONTROL_NO','POLICY_NO','CERTIFICATE_NO','RENL_CERT_NO','EFF_DT_SEQ_NO','MAJOR_LINE_CD','MINOR_LINE_CD','CLASS_PERIL_CD','ACCT_PERIOD_DATE','PREM_EFF_DATE','REPORT_ACCOUNTING_PERIOD','WATTS_POLICY_NO','ORIG_CURR_CD','TIMESTAMP','PREMIUM_AMOUNT_INR_WITHOUT_TAX','COMMISSION_INR','SERVICE_TAX_AMOUNT','COMMISSIONABLE_PREMIUM','ORG_CURR_COMMISSION','ORG_PREM_AMT_WITHOUT_TAX','OBLIGATORY_PREM','CEDED_VQST_PREM','CEDED_SURPLUS_TREATY_PREM','CEDED_TERRORISM_POOL_PREM','CEDED_AIG_COMBINED_PREM','FAC_PREM','XOL_PREM','TTY_PREM','OBLIGATORY_COMM','CEDED_VQST_COMM','CEDED_SURPLUS_TREATY_COMM','CEDED_TERRORISM_POOL_COMM','CEDED_AIG_COMBINED_COMM','FAC_COMM','TTY_COMM','IGST_AMOUNT','SGST_AMOUNT','CGST_AMOUNT','UTGST_AMOUNT','TDS_ON_COMM','POL_STAMP_DUTY_AMT','IGST_RATE','SGST_RATE','CGST_RATE','PRODUCER_NAME','PRODUCER_CD','APPLICATION_NO','PORTAL_FLAG','PRODUCT_TYPE','BRANCH','CUSTOMER_ID','CLIENT_NAME','RECORD_TYPE_DESC','PARTNER_APPL_NO','PRODUCT_INDEX','POL_OFFICE_CD','TXT_LOB','DEPARTMENTCODE','CERT_START_DATE','CERT_END_DATE','ENDORSEMENT_EFF_DATE','RI_INWARD_FLAG','SERVICE_TAX_EXEMPATION_FLAG','COVER_NOTE_DATE','PRDR_BRANCH_SUB_CD','TXT_BRANCH_OFFICE_CODE','BRANCH_OFF_CD','RURAL_FG','RECORD_TYPE_CD','ENDORSEMENT_END_DATE','POL_INCEPT_DATE','POL_EXP_DATE','COVERNOTENUMBER','NPW','TOTAL_COMMISSION','PREMIUM_WITH_TAX','MMCP','COMMISSIONPERCENT','SUM_INSURED','PRODUCT_NAME','REPORT_DATE','UIN_NO','IRDA_LOB','IIB_LOB','SL_NO','POLICY_COUNTER','POLICY_ISSUANCE_DATE','PROPOSAL_DATE')\
    #.filter(col('NUM_REFERENCE_NO').isin(nrn_list))
    
    prem_reg_11B = pr_part_1\
    .drop('SUM_INSURED').drop('COMMISSIONPERCENT')

    #reference index - for commissionPercent, SUM_INSURED - original code, commented on 2019-04-17 after adding new SI logic
    ####################################################################################################
    #20210419 - Sagar - Sum_insured for motor LOB. 
    #please refer document Mcube:\TCG\BRD\SUM INSURED\Sum Insured Motor_TATA AIG_Requirements_Specifications_v1.docx
    ####################################################################################################
    gscPythonOptions = {"url": gpdb_url,"user": gpdb_user,"password": gpdb_pass,
        "dbschema": "public","dbtable": "policy_gc_covers_details","partitionColumn":"row_num",
        "server.port":"1160-1180","partitions":32}
    
    cover_src=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('REFERENCE_NUM', 'INFORMATION1', 'SI', 'DIFF_SI', 'COVER_GROUP')\
    #.filter(col('REFERENCE_NUM').isin(nrn_list))
    #.filter(col("ROWSTATUS")!=lit('True'))
    
    cover = cover_src.withColumnRenamed('INFORMATION1', 'COMMISSIONPERCENT')\
                     .withColumnRenamed('SI', 'CVR_DTL_SI')\
                     .withColumnRenamed('DIFF_SI', 'CVR_DTL_DIFF_SI')\
                     .withColumnRenamed('REFERENCE_NUM', 'NUM_REFERENCE_NO')\
    
    #20210512 commented below aggregation to process group data where sum of SI is expected
    cover_1 = cover
              #.groupBy('NUM_REFERENCE_NO','COVER_GROUP').agg(\
              #sf.max('COMMISSIONPERCENT').alias('COMMISSIONPERCENT'),\
              #sf.max('CVR_DTL_SI').alias('CVR_DTL_SI'),\
              #sf.max('CVR_DTL_DIFF_SI').alias('CVR_DTL_DIFF_SI'))
    
    gscPythonOptions = {"url": gpdb_url,"user": gpdb_user,"password": gpdb_pass,
        "dbschema": "public","dbtable": "policy_gc_risk_headers","partitionColumn":"row_num",
        "server.port":"1160-1180","partitions":11}
    
    risk_hdr=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('REFERENCE_NUM','INFORMATION1','INFORMATION2','INFORMATION4','INFORMATION5','INFORMATION15','INFORMATION16','INFORMATION21','INFORMATION42',\
            'INFORMATION53','INFORMATION92','INFORMATION93','INFORMATION109','INFORMATION115','RISK1')\
    #.filter(col('REFERENCE_NUM').isin(nrn_list))
    
    risk_hdr_1 = risk_hdr\
    .withColumnRenamed('REFERENCE_NUM', 'NUM_REFERENCE_NO')\
    .withColumn('INFORMATION5',when(col("INFORMATION5").cast("double").isNotNull()==True, col('INFORMATION5')).otherwise(lit(0)))\
    .withColumn('INFORMATION92',when(col("INFORMATION92").cast("double").isNotNull()==True, col('INFORMATION92')).otherwise(lit(0)))\
    .withColumn('INFORMATION93',when(col("INFORMATION93").cast("double").isNotNull()==True, col('INFORMATION93')).otherwise(lit(0)))

    risk_hdr_2 = risk_hdr_1.groupBy('NUM_REFERENCE_NO','RISK1').agg(\
                            sf.max('INFORMATION1').alias('INFORMATION1'),\
                            sf.max('INFORMATION2').alias('INFORMATION2'),\
                            sf.max('INFORMATION5').alias('INFORMATION5'),\
                            sf.max('INFORMATION15').alias('INFORMATION15'),\
                            sf.max('INFORMATION92').alias('INFORMATION92'),\
                            sf.max('INFORMATION93').alias('INFORMATION93'),\
                            sf.max('INFORMATION109').alias('INFORMATION109'),\
                            sf.max('INFORMATION115').alias('INFORMATION115'),\
                            sf.max('INFORMATION4').alias('RH_SI_2875'),\
                            sf.max('INFORMATION16').alias('RH_FLOTR_SI_2888'),\
                            sf.max('INFORMATION21').alias('RH_FLOTR_SI_2861'),\
                            sf.max('INFORMATION42').alias('RH_SI_2876_2878'),\
                            sf.max('INFORMATION53').alias('RH_FLOTR_SI_2862_2896'))

    prem_reg_nrn = prem_reg_11B.select('NUM_REFERENCE_NO','PRODUCT_CD','DEPARTMENTCODE','RECORD_TYPE_CD').distinct()
    prem_reg_si = prem_reg_nrn.join(risk_hdr_2, 'NUM_REFERENCE_NO', 'left')
    

    gscPythonOptions = {"url": gpdb_url,"user": gpdb_user,"password": gpdb_pass,
        "dbschema": "public","dbtable": "underwriting_gc_cnfgtr_risk_grid_detail_tab",
        "server.port":"1160-1180","partitionColumn":"row_num","partitions":8}
    
    cnfgtr=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_REFERENCE_NUMBER','INFORMATION6','INFORMATION7','INFORMATION13','INFORMATION15','FLAGSTATUS')\
    #.filter(col('NUM_REFERENCE_NUMBER').isin(nrn_list))
    
    cnfgtr_1 = cnfgtr\
    .withColumnRenamed("NUM_REFERENCE_NUMBER", "NUM_REFERENCE_NO")\
    .withColumn("CNFGTR_SI_2869_2877", when(col("FLAGSTATUS")=='A', col('INFORMATION13'))\
                                      .when(col("FLAGSTATUS")=='D', col('INFORMATION13')*-1)\
                                      .otherwise(lit(0)))\
    .withColumn("CNFGTR_SI_2889", when(col("FLAGSTATUS")=='A', col('INFORMATION15'))\
                                 .when(col("FLAGSTATUS")=='D', col('INFORMATION15')*-1)\
                                 .otherwise(lit(0)))\
    
    cnfgtr_SI = cnfgtr_1.groupBy('NUM_REFERENCE_NO')\
                        .agg(sf.sum('CNFGTR_SI_2869_2877').alias('CNFGTR_SI_SUM_2869_2877'),\
                             sf.sum('CNFGTR_SI_2889').alias('CNFGTR_SI_SUM_2889'),\
                             sf.max('CNFGTR_SI_2869_2877').alias('CNFGTR_SI_MAX_2869_2877'),\
                             sf.max('CNFGTR_SI_2889').alias('CNFGTR_SI_MAX_2889'))
    
    prem_reg_tmp_1A = prem_reg_nrn[["NUM_REFERENCE_NO"]]
    prem_reg_tmp_1 = prem_reg_tmp_1A.distinct()
    cnfgtr_2 = cnfgtr_1.join(prem_reg_tmp_1, "NUM_REFERENCE_NO")
    #cnfgtr_3 = cnfgtr_2.groupBy("NUM_REFERENCE_NO").count().withColumnRenamed("count", "CNFGTR_RISK_COUNT")

    # sum_insured for products family floater(2861,2888,2862,2896) and 2875,2878,2876
    
    prem_reg_si_A = prem_reg_si\
    .withColumn('FLOATER_OR_INDVDL_FG', \
                        when((col('PRODUCT_CD').isin(['2861','2888'])) &\
                             (lower(col('INFORMATION2')).like('%individual%')), lit('I'))\
                       .when((col('PRODUCT_CD').isin(['2861','2888'])) &\
                             (lower(col('INFORMATION2')).like('%floater%')), lit('F'))
                       .when((col('PRODUCT_CD').isin(['2862','2896'])) &\
                             (lower(col('INFORMATION15')).like('%individual%')), lit('I'))\
                       .when((col('PRODUCT_CD').isin(['2862','2896'])) &\
                             (lower(col('INFORMATION15')).like('%floater%')), lit('F'))\
                       .when((col('PRODUCT_CD').isin(['2869','2877'])) &\
                             (lower(col('INFORMATION1')).like('%individual%')), lit('I'))\
                       .when((col('PRODUCT_CD').isin(['2869','2877'])) &\
                             (lower(col('INFORMATION1')).like('%floater%')), lit('F'))\
                       .when((col('PRODUCT_CD')=='2895') &\
                             (lower(col('INFORMATION109')).like('%individual%')), lit('I'))\
                       .when((col('PRODUCT_CD')=='2895') &\
                             (lower(col('INFORMATION115')).like('%floater%')), lit('F'))\
                       .when((col('PRODUCT_CD')=='2889') &\
                             (lower(col('INFORMATION1')).like('%individual%')), lit('I'))\
                       .when((col('PRODUCT_CD')=='2889') &\
                             (lower(col('INFORMATION1')).like('%floater%')), lit('F'))\
                       .otherwise(lit(None)))\
    .drop('INFORMATION1').drop('INFORMATION2').drop('INFORMATION15').drop('INFORMATION109')\
    .drop('INFORMATION115')
                       
    rh_si = prem_reg_si_A\
    .withColumn('RH_SI', when((col('FLOATER_OR_INDVDL_FG')=='F')&\
                              (col('PRODUCT_CD')=='2861'), col('RH_FLOTR_SI_2861'))\
                        .when((col('FLOATER_OR_INDVDL_FG')=='F')&\
                              (col('PRODUCT_CD').isin(['2862','2896'])), col('RH_FLOTR_SI_2862_2896'))\
                        .when((col('FLOATER_OR_INDVDL_FG')=='F')&\
                              (col('PRODUCT_CD')=='2888'), col('RH_FLOTR_SI_2888'))\
                        .when((col('PRODUCT_CD')=='2875'), col('RH_SI_2875'))\
                        .when((col('PRODUCT_CD').isin(['2876','2878'])), col('RH_SI_2876_2878'))\
                        .otherwise(lit(0)))
                         
    rh_si_1 = rh_si.groupBy('NUM_REFERENCE_NO','PRODUCT_CD').agg(sf.max('RH_SI').alias('RH_SI'))

    prem_reg_si_1 = prem_reg_si_A.join(cover_1, 'NUM_REFERENCE_NO', 'left')
    
    # 20210517 2869 and 2877, 2889
    pr_cnfgtr_si = prem_reg_si_A.join(cnfgtr_SI, 'NUM_REFERENCE_NO', 'left')
    
    pr_cnfgtr_si_1 = pr_cnfgtr_si\
    .withColumn('CNFGTR_SI',\
                  when((col('PRODUCT_CD').isin(['2869','2877']))&\
                       ((col('FLOATER_OR_INDVDL_FG')) == 'I'),\
                        col('CNFGTR_SI_SUM_2869_2877'))\
                 .when((col('PRODUCT_CD').isin(['2869','2877']))&\
                       ((col('FLOATER_OR_INDVDL_FG')) == 'F'),\
                        col('CNFGTR_SI_MAX_2869_2877'))\
                 .when((col('PRODUCT_CD').isin(['2889']))&\
                       ((col('FLOATER_OR_INDVDL_FG')) == 'I'),\
                        col('CNFGTR_SI_SUM_2889'))\
                 .when((col('PRODUCT_CD').isin(['2889']))&\
                       ((col('FLOATER_OR_INDVDL_FG')) == 'F'),\
                        col('CNFGTR_SI_MAX_2889'))\
                 .otherwise(lit(0)))
                 
    pr_cnfgtr_si_2 = pr_cnfgtr_si_1\
    .groupBy('NUM_REFERENCE_NO','PRODUCT_CD').agg(sf.max('CNFGTR_SI').alias('CNFGTR_SI'))
    

    prem_reg_si_1A = prem_reg_si_1\
    .withColumn('SUM_INSURED',\
                  when((lower(col('RISK1')).isin(['packagepolicy','packagecomprehensive']))&\
                       (col('DEPARTMENTCODE')=='31')&(col('PRODUCT_CD')!='3184')&\
                       (lower(col('COVER_GROUP')).isin(['owndamage','own damage'])), col('CVR_DTL_SI')+col('INFORMATION93'))\
                 .when((lower(col('RISK1')).isin(['packagepolicy','packagecomprehensive']))&\
                       (col('DEPARTMENTCODE')=='31')&(col('PRODUCT_CD')=='3184')&\
                       (lower(col('COVER_GROUP')).isin(['owndamage','own damage'])), col('CVR_DTL_SI')+col('INFORMATION93')+col('INFORMATION92'))\
                 .when((lower(col('RISK1')).isin(['standaloneod','standalone od']))&\
                       (col('DEPARTMENTCODE')=='31')&\
                       (lower(col('COVER_GROUP')).isin(['owndamage','own damage'])), col('CVR_DTL_SI'))\
                 .when((lower(col('RISK1')).isin(['autosecurecpa']))&\
                       (col('DEPARTMENTCODE')=='31')&\
                       (lower(col('COVER_GROUP')).isin(['personal accident cover','personalaccidentcover'])), col('INFORMATION5'))\
                 .when((lower(col('RISK1')).isin(['firetheftliabilityonly']))&\
                       (col('DEPARTMENTCODE')=='31')&\
                       (lower(col('COVER_GROUP'))=='fire and theft basic od'), col('CVR_DTL_SI'))\
                 .when((lower(col('RISK1')).isin(['firetheft']))&\
                       (col('DEPARTMENTCODE')=='31')&\
                       (lower(col('COVER_GROUP'))=='fire and theft'), col('CVR_DTL_SI'))\
                 .when((lower(col('RISK1')).isin(['twowheelerextendedwarranty']))&\
                       (col('DEPARTMENTCODE')=='31')&\
                       (lower(col('COVER_GROUP'))=='extended warranty'), col('CVR_DTL_SI'))\
                 .when((lower(col('RISK1')).isin(['fireliabilityonly']))&\
                       (col('DEPARTMENTCODE')=='31')&\
                       (lower(col('COVER_GROUP'))=='fire basic od'), col('CVR_DTL_SI'))\
                 .when((lower(col('RISK1')).isin(['theftliabilityonly']))&\
                       (col('DEPARTMENTCODE')=='31')&\
                       (lower(col('COVER_GROUP'))=='theft-od'), col('CVR_DTL_SI'))\
                 .when((col('PRODUCT_CD').isin(['5501','5502']))&\
                       (lower(col('COVER_GROUP')).isin(['fire and spl perils building','fire and spl perils content','jewellery and valuables','plate glass'])),\
                        col('CVR_DTL_SI'))\
                 .when((col('PRODUCT_CD')=='5504')&\
                       (lower(col('COVER_GROUP')).isin(['fire and spl perils building','fire and spl perils content'])),\
                        col('CVR_DTL_SI'))\
                 .otherwise(lit(0))) #LiabilityOnly,Standalone TP and remaining lob
    

    # 20210512 health lob individual and other - '2861','2862','2888','2896'
    prem_reg_si_2 = prem_reg_si_1A\
    .withColumn('SUM_INSURED',\
                  when((col('PRODUCT_CD').isin(['2861','2862','2888','2896']))&\
                       ((col('FLOATER_OR_INDVDL_FG')) == 'I') &\
                       (upper(col('RECORD_TYPE_CD')).isin(['N','R','X'])) &\
                       (lower(col('COVER_GROUP')).isin(['inpatient treatment','inpatienttreatment','in patient treatment'])),\
                        col('CVR_DTL_SI'))\
                 .when((col('PRODUCT_CD').isin(['2861','2862','2888','2896']))&\
                       ((col('FLOATER_OR_INDVDL_FG')) == 'I') &\
                       (upper(col('RECORD_TYPE_CD'))=='E') &\
                       (lower(col('COVER_GROUP')).isin(['inpatient treatment','inpatienttreatment','in patient treatment'])),\
                        col('CVR_DTL_DIFF_SI'))\
                 .when((col('PRODUCT_CD').isin(['2863','2864']))&\
                       (upper(col('RECORD_TYPE_CD')).isin(['N','R','X'])) &\
                       (lower(col('COVER_GROUP')).isin(['inpatient treatment','inpatienttreatment','in patient treatment'])),\
                        col('CVR_DTL_SI'))\
                 .when((col('PRODUCT_CD').isin(['2863','2864']))&\
                       (upper(col('RECORD_TYPE_CD'))=='E') &\
                       (lower(col('COVER_GROUP')).isin(['inpatient treatment','inpatienttreatment','in patient treatment'])),\
                        col('CVR_DTL_DIFF_SI'))\
                 .when((col('PRODUCT_CD').isin(['2871']))&\
                       (upper(col('RECORD_TYPE_CD')).isin(['N','R','X'])) &\
                       (lower(col('COVER_GROUP')).isin(['coma'])),\
                        col('CVR_DTL_SI'))\
                 .when((col('PRODUCT_CD').isin(['2871']))&\
                       (upper(col('RECORD_TYPE_CD'))=='E') &\
                       (lower(col('COVER_GROUP')).isin(['coma'])),\
                        col('CVR_DTL_DIFF_SI'))\
                 .otherwise(col('SUM_INSURED')))
    
    prem_reg_si_2A = prem_reg_si_2\
    .groupBy('NUM_REFERENCE_NO','PRODUCT_CD','RECORD_TYPE_CD').agg(\
    sf.max('SUM_INSURED').alias('SUM_INSURED_MAX'),\
    sf.sum('SUM_INSURED').alias('SUM_INSURED_SUM'),\
    sf.max('FLOATER_OR_INDVDL_FG').alias('FLOATER_OR_INDVDL_FG'),\
    sf.max('COMMISSIONPERCENT').alias('COMMISSIONPERCENT'))

    prem_reg_si_2B = prem_reg_si_2A\
    .withColumn('SUM_INSURED', when(col('PRODUCT_CD').isin(['5501','5502','5504','2861','2888','2862','2896']),\
                                    col('SUM_INSURED_SUM'))\
                               .otherwise(col('SUM_INSURED_MAX')))

    prem_reg_si_3 = prem_reg_si_2B.drop('SUM_INSURED_MAX').drop('SUM_INSURED_SUM')

    prem_reg_11C = prem_reg_11B.join(prem_reg_si_3, ['NUM_REFERENCE_NO','PRODUCT_CD','RECORD_TYPE_CD'],'left')\
    
    # joining with rh_si_1 to get SI for products that are directly coming from risk_headers
    prem_reg_11D = prem_reg_11C.join(rh_si_1,['NUM_REFERENCE_NO','PRODUCT_CD'],'left')
    prem_reg_11E = prem_reg_11D\
    .withColumn('SUM_INSURED', \
                   when((col('PRODUCT_CD').isin(['2861','2888','2862','2896','2875','2878','2876']))&\
                        ((col('FLOATER_OR_INDVDL_FG')=='F')|col('FLOATER_OR_INDVDL_FG').isNull()),col('RH_SI'))\
                  .otherwise(col('SUM_INSURED')))\
    .drop('RH_SI')
    
    prem_reg_11F = prem_reg_11E.join(pr_cnfgtr_si_2,['NUM_REFERENCE_NO','PRODUCT_CD'],'left')
    prem_reg_11G = prem_reg_11F\
    .withColumn('SUM_INSURED', \
                   when(col('PRODUCT_CD').isin(['2869','2877','2889']),col('CNFGTR_SI'))\
                  .otherwise(col('SUM_INSURED')))\
    .drop('CNFGTR_SI')
    

    #prem_reg_si_1.persist()

    #prem_reg_si_1A.show()

    prem_reg_11G\
    .select('NUM_REFERENCE_NO','PRODUCT_CD','RECORD_TYPE_CD','SUM_INSURED','FLOATER_OR_INDVDL_FG','COMMISSIONPERCENT').show()

    ####################################################################################################
    #20210423 - Sagar - Sum_insured for extended warranty product. 
    #please refer document Mcube:\TCG\BRD\SUM INSURED\Sum Insured _TATA AIG_Requirements_ES_Specifications_v1.docx
    ####################################################################################################
    
    gscPythonOptions = {"url": gpdb_url,"user": gpdb_user,"password": gpdb_pass,
        "dbschema": "public","server.port":"1160-1180","dbtable": "policy_gc_risk_details",
        "partitionColumn":"row_num","partitions":10}
    risk_d_si=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('REFERENCE_NUM','SUM_INSURED_COMPONENT','ENDT_AMT')\
    #.filter(col('REFERENCE_NUM').isin(nrn_list))
    
    risk_d_si_1 = risk_d_si.groupBy('REFERENCE_NUM')\
                           .agg(sf.sum('SUM_INSURED_COMPONENT').alias('RD_SI_COMPONENT_SUM'),\
                                sf.max('SUM_INSURED_COMPONENT').alias('RD_SI_COMPONENT_MAX'),\
                                sf.max('ENDT_AMT').alias('END_AMOUNT'))
    
    risk_d_si_2 = risk_d_si_1\
    .withColumnRenamed('REFERENCE_NUM','NUM_REFERENCE_NO')\
    .withColumn('RD_SI_COMPONENT_MAX', \
                      when(col('END_AMOUNT') > 0, col('RD_SI_COMPONENT_MAX'))\
                     .otherwise(col('RD_SI_COMPONENT_MAX')*-1))\
    .drop('END_AMOUNT')

    
    rd_si_product_extded_wrnty = ['1504','1506','5505']
    rd_si_product_health = ['2874','2865','2668','2866']
    rd_si_product = rd_si_product_extded_wrnty + rd_si_product_health
    print("rd_si_product", rd_si_product)
    
    prem_reg_11H = prem_reg_11G.join(risk_d_si_2, 'NUM_REFERENCE_NO', 'left')
    
    prem_reg_11I = prem_reg_11H\
    .withColumn('SUM_INSURED', when(col('PRODUCT_CD').isin(rd_si_product), col('RD_SI_COMPONENT_SUM'))\
                              .when((col('PRODUCT_CD').isin(['2895'])) &\
                                    (col('FLOATER_OR_INDVDL_FG')=='I'), col('RD_SI_COMPONENT_SUM'))\
                              .when((col('PRODUCT_CD').isin(['2895'])) &\
                                    (col('FLOATER_OR_INDVDL_FG')=='F') &\
                                    (col('RECORD_TYPE_CD').isin(['N','R','X'])), abs(col('RD_SI_COMPONENT_MAX')))\
                              .when((col('PRODUCT_CD').isin(['2895'])) &\
                                    (col('FLOATER_OR_INDVDL_FG')=='F') &\
                                    (col('RECORD_TYPE_CD').isin(['E'])), col('RD_SI_COMPONENT_MAX'))\
                              .otherwise(col('SUM_INSURED')))
    
    prem_reg_11J = prem_reg_11I\
    .drop('SUM_INSURED_COMPONENT').drop('RH_SI').drop('RD_SI_COMPONENT_SUM').drop('RD_SI_COMPONENT_MAX')\
    .drop('FLOATER_OR_INDVDL_FG')
        
    prem_reg_11K = prem_reg_11J\
    .withColumn('SUM_INSURED', \
                    when(col('RECORD_TYPE_CD')=='X',col('SUM_INSURED')*-1)\
                   .otherwise(col('SUM_INSURED')))
    
    prem_reg_C = prem_reg_11K

    prem_reg_C.select('NUM_REFERENCE_NO','PRODUCT_CD','RECORD_TYPE_CD','SUM_INSURED', 'COMMISSIONPERCENT').show()

    prem_reg_11H.select('FLOATER_OR_INDVDL_FG','PRODUCT_CD','RECORD_TYPE_CD','SUM_INSURED','RD_SI_COMPONENT_SUM','RD_SI_COMPONENT_MAX').show()

    try:
        prem_reg_C.write.option("truncate", "true").format("greenplum").option("dbtable","premium_register_part_2").option('dbschema','staging').option("server.port","1160-1180").option("url",gpdb_url_uat).option("user", gpdb_user).option("password",gpdb_pass).mode('overwrite').save()
    except Exception as e:
        x = e
        print(x)
        print("Spark code : first job SI part aborted @")
        ts = time.gmtime()
        print(time.strftime("%Y-%m-%d %H:%M:%S", ts))
        sys.exit(-1)
    else: 
        x = 200
    print(x)
    print("first job SI part ended @")
    ts = time.gmtime()
    print(time.strftime("%Y-%m-%d %H:%M:%S", ts))
    
else:
    x = 200
    print("First job skipped.")

# Part 1 ends for good.
if x == 200:
    #Job 2
    # # schema: registers: as per Vaibhav's input: Read is having problem: : org.postgresql.util.PSQLException: ERROR: relation "prem_reg_final_gc_1_optmzd_test_april" does not exist
    # # Changed schema back to public
    # gscPythonOptions = {
    #     "url": gpdb_url,
    #     "user": gpdb_user,
    #     "password": gpdb_pass,
    #     "dbtable": "registers.premium_register_part_1"} 
    
    # prem_reg_final_1=sqlContext.read.format("jdbc").options(**gscPythonOptions).load()
    
    print("Now starting second part...")
    
    gscPythonOptions = {
        "url": gpdb_url_uat,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "staging",
        "dbtable": "premium_register_part_2",
        "partitionColumn":"sl_no",
        "server.port":"1160-1180",
        "partitions":4} 
    
    prem_reg_final_1=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('DISPLAY_PRODUCT_CD','PRODUCT_CD','NUM_REFERENCE_NO','NUM_TRANSACTION_CONTROL_NO','POLICY_NO','CERTIFICATE_NO','RENL_CERT_NO','EFF_DT_SEQ_NO','MAJOR_LINE_CD','MINOR_LINE_CD','CLASS_PERIL_CD','ACCT_PERIOD_DATE','PREM_EFF_DATE','REPORT_ACCOUNTING_PERIOD','WATTS_POLICY_NO','ORIG_CURR_CD','TIMESTAMP','PREMIUM_AMOUNT_INR_WITHOUT_TAX','COMMISSION_INR','SERVICE_TAX_AMOUNT','COMMISSIONABLE_PREMIUM','ORG_CURR_COMMISSION','ORG_PREM_AMT_WITHOUT_TAX','OBLIGATORY_PREM','CEDED_VQST_PREM','CEDED_SURPLUS_TREATY_PREM','CEDED_TERRORISM_POOL_PREM','CEDED_AIG_COMBINED_PREM','FAC_PREM','XOL_PREM','TTY_PREM','OBLIGATORY_COMM','CEDED_VQST_COMM','CEDED_SURPLUS_TREATY_COMM','CEDED_TERRORISM_POOL_COMM','CEDED_AIG_COMBINED_COMM','FAC_COMM','TTY_COMM','IGST_AMOUNT','SGST_AMOUNT','CGST_AMOUNT','UTGST_AMOUNT','TDS_ON_COMM','POL_STAMP_DUTY_AMT','IGST_RATE','SGST_RATE','CGST_RATE','PRODUCER_NAME','PRODUCER_CD','APPLICATION_NO','PORTAL_FLAG','PRODUCT_TYPE','BRANCH','CUSTOMER_ID','CLIENT_NAME','RECORD_TYPE_DESC','PARTNER_APPL_NO','PRODUCT_INDEX','POL_OFFICE_CD','TXT_LOB','DEPARTMENTCODE','CERT_START_DATE','CERT_END_DATE','ENDORSEMENT_EFF_DATE','RI_INWARD_FLAG','SERVICE_TAX_EXEMPATION_FLAG','COVER_NOTE_DATE','PRDR_BRANCH_SUB_CD','TXT_BRANCH_OFFICE_CODE','BRANCH_OFF_CD','RURAL_FG','RECORD_TYPE_CD','ENDORSEMENT_END_DATE','POL_INCEPT_DATE','POL_EXP_DATE','COVERNOTENUMBER','NPW','TOTAL_COMMISSION','PREMIUM_WITH_TAX','MMCP','COMMISSIONPERCENT','SUM_INSURED','PRODUCT_NAME','REPORT_DATE','UIN_NO','IRDA_LOB','IIB_LOB','SL_NO','POLICY_COUNTER','POLICY_ISSUANCE_DATE','PROPOSAL_DATE')
   
    prem_reg_final_1.persist()
    #prem_reg_final_1.count()
    # spark.stop()
    
    # prem_reg_final_1.count()
    
    # input_indices_name = 'prem_reg_final_gc_1_optmzd'
    # input_doc_type_name = "prem_reg_final"
    # 
    # input_doc_type_name = input_indices_name +'/'+ input_doc_type_name
    # print input_doc_type_name
    # 
    # document_count =documentcount(input_doc_type_name)
    # print document_count
    # 
    # prem_reg_final_1 = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.query', esq1).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    # prem_reg_3 = prem_reg_final_1.withColumn('NUM_REFERENCE_NO', \
    #                                         prem_reg_final_1.NUM_REFERENCE_NO.cast(DecimalType(15,0)).cast(StringType()))
    prem_reg_3 = prem_reg_final_1
    
    prem_reg_nm_rfrnc_no_distnct = prem_reg_3.select('NUM_REFERENCE_NO').distinct()
    
    # input_indices_name = 'pr_gc_nm_rfrnc_no_distinct'
    # input_doc_type_name = input_indices_name+'/nm_rfrnc_no_distinct'
    # document_count =documentcount(input_doc_type_name)
    # 
    # prem_reg_nm_rfrnc_no_distnct = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.input.max.docs.per.partition',document_count).load()
    # 
    # ######################################################
    # # nm_rfrnc_no_distinct.persist()
    
    prem_reg_cust_id_distnct = prem_reg_3.select('CUSTOMER_ID').distinct()
    
    # prem_reg_3.count()
    
    # Reference datasets for CITY/District and state
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_gc_genmst_tab_state"} 
    genmst_state=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('NUM_STATE_CD', 'TXT_STATE', 'TXT_GSTIN_NO')
    
    # Code was changed in production
    # gscPythonOptions = {
    #     "url": gpdb_url,
    #     "user": gpdb_user,
    #     "password": gpdb_pass,
    #     "dbschema": "public",
    #     "dbtable": "reference_gc_genmst_state"} 
    # genmst_state=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('NUM_STATE_CD', 'TXT_STATE', 'TXT_GSTIN_NO')
    
    # input_indices_name = 'reference_gc_genmst_state_alias'
    # input_doc_type_name = input_indices_name+'/genmst_state'
    # 
    # document_count =documentcount(input_doc_type_name)
    # 
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # genmst_state = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_STATE_CD, TXT_STATE, TXT_GSTIN_NO').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    ###############################################################
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "customer_gc_genmst_citydistrict"} 
    genmst_city=sqlContext.read.format("jdbc").options(**gscPythonOptions).load()\
    	.select('NUM_CITYDISTRICT_CD', 'TXT_CITYDISTRICT', 'TXT_CITY_DISTRICT_FLAG', 'NUM_STATE_CD')
    
    # input_indices_name = 'customer_gc_genmst_citydistrict_alias'
    # input_doc_type_name = input_indices_name+'/genmst_citydistrict'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # genmst_city = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_CITYDISTRICT_CD, TXT_CITYDISTRICT, TXT_CITY_DISTRICT_FLAG, NUM_STATE_CD').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    genmst_city_1 = genmst_city.withColumnRenamed('NUM_CITYDISTRICT_CD', 'NUM_CITY_CD')
    
    ####################################################################
    ##############################################
    # updated logic for receipt_no and crs_receipt_date on 27-03-2019
    #############################################
    
    # acc_map_instrproposal added on 2019-05-20 after discussing with Pankaj
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "account_gc_acc_map_instrproposal",
        "partitionColumn":"row_num",
        "server.port":"1160-1180",
        "partitions":8} 
    
    receipt=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_INSTRUMENT_ID', 'NUM_PROPOSAL_NO')
    
    # .filter(col('NUM_PROPOSAL_NO')==lit('202001170097684'))
    # input_indices_name = 'account_gc_acc_map_instrproposal'
    # input_doc_type_name = input_indices_name+'/acc_map_instrproposal'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # receipt = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_INSTRUMENT_ID, NUM_PROPOSAL_NO').\
    # option('es.query', esq2).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    receipt_1A = receipt.withColumnRenamed("NUM_PROPOSAL_NO", "NUM_REFERENCE_NO")
    
    # optimization
    receipt_1 = receipt_1A.join(prem_reg_nm_rfrnc_no_distnct, 'NUM_REFERENCE_NO')
        
    receipt_2 = receipt_1.groupBy("NUM_REFERENCE_NO").agg(sf.max("NUM_INSTRUMENT_ID").alias("NUM_INSTRUMENT_ID"))  
    # receipt_2 = receipt_2.withColumn('NUM_REFERENCE_NO', receipt_2.NUM_REFERENCE_NO.cast(DecimalType(15,0)).cast(StringType()))
    
    receipt_nm_instru_id_distnct = receipt_2.select('NUM_INSTRUMENT_ID').distinct()
    
    prem_reg_4A = prem_reg_3.join(receipt_2, 'NUM_REFERENCE_NO', 'left') 
    
    # receipt.show()
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "server.port":"1160-1180",
        "dbtable": "account_gc_acc_sub_receipt_info",
        "partitionColumn":"row_num",
        "partitions":6}
    
    receipt=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_INSTRUMENT_ID', 'NUM_REF_INSTRUMENT_ID')
    
    # .filter(col('NUM_INSTRUMENT_ID')==lit('702001038651841'))
    #input_indices_name = 'account_gc_acc_sub_receipt_info_alias'
    #input_doc_type_name = input_indices_name+'/acc_sub_receipt_info'
    #
    #document_count =documentcount(input_doc_type_name)
    #print "record count - "+input_indices_name+" : "+str(document_count)
    #
    #receipt = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    #option('es.resource',input_doc_type_name).\
    #option('es.read.field.include','NUM_INSTRUMENT_ID, NUM_REF_INSTRUMENT_ID').\
    #option('es.query', esq3).\
    #option('es.input.max.docs.per.partition',document_count).load()
    
    receipt_1B = receipt.withColumnRenamed("NUM_REF_INSTRUMENT_ID", "RECEIPT_NO")  
    
    # optimization
    receipt_1 = receipt_1B.join(receipt_nm_instru_id_distnct, 'NUM_INSTRUMENT_ID')
    
    receipt_2 = receipt_1.groupBy('NUM_INSTRUMENT_ID').agg(sf.max("RECEIPT_NO").alias("RECEIPT_NO")) 
    
    rcpt_no_distinct = receipt_2.select('RECEIPT_NO').distinct()
    
    prem_reg_4 = prem_reg_4A.join(receipt_2, 'NUM_INSTRUMENT_ID', 'left')  
    
    prem_reg_6 = prem_reg_4
    
    prem_reg_6 = prem_reg_6.drop('NCB_PERCENT')  
    #######################
    
    # New logic for NCB - given by Ankita on 2019-04-15
    #######################
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "server.port":"1160-1180",
        "dbtable": "reference_gc_cnfgtr_policy_ld_dtls",
        "partitionColumn":"row_num",
        "partitions":6}
    #     "server.port":"1160-1180",
    ncb_disc=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('REFERENCE_NUM', 'LD_RATE', 'LDDESC', 'LD_FLAG', 'ENDT_AMT')
    
    # .filter(col('REFERENCE_NUM')==lit('202001170097684'))
    # input_indices_name = 'reference_gc_cnfgtr_policy_ld_dtls'
    # input_doc_type_name = input_indices_name+'/cnfgtr_policy_ld_dtls'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # ncb_disc = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','REFERENCE_NUM, LD_RATE, LDDESC, LD_FLAG, ENDT_AMT').\
    # option('es.query', esq4).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    ncb_disc_1 = ncb_disc.withColumnRenamed('REFERENCE_NUM', 'NUM_REFERENCE_NO')
    # ncb_disc_2 = ncb_disc_1.withColumn('NUM_REFERENCE_NO', \
    #                                        ncb_disc_1.NUM_REFERENCE_NO.cast(DecimalType(15,0)).cast(StringType()))
    
    # optimization
    prem_ncb = ncb_disc_1.join(prem_reg_nm_rfrnc_no_distnct, 'NUM_REFERENCE_NO')
    
    prem_ncb.createOrReplaceTempView('prem_ncb_view')
    
    sql_1 = """select NUM_REFERENCE_NO,
    sum(case when((LDDESC in 
    ('Any other discount','Any Other Discount','Automobile Association Discount','Confined to Own sites OD'
    ,'Confined to Own sites TP','Discount for Anti-Theft Devices','Employee Discount','Family Discount','Group Discount'
    ,'Handicapped Discount','Long term Discount','Long Term Discount','NCB protection Discount for rewenal only','No Claim Bonus'
    ,'Other Policy Discount','Own Premises Discount OD','Own Premises Discount TP','SharedCategory Discount','Side Car Discount'
    ,'Special Discount','TPPD Restriction','Underwriting Discount','Vintage Car Discount','Voluntary Excess')) and (LD_FLAG='D'))
    then ifnull(ENDT_AMT, 0) else 0 end) as UW_DISCOUNT_AMT,
    
    sum(case when((LDDESC in 
    ('Any other loading','Cover for Hire or reward Liability IMT-44, 45 OD','Cover for Hire or reward Liability IMT-44, 45 TP',
    'Driving Tuitions','Driving Tution - TP','Embassy Loading / Imported without custom duty','Fiber Glass Fuel Tank',
    'Fibre Glass Fuel Tank','Other Policy Loading','Underwriting Loading','Use Of Commercial Type Vehicle OD',
    'Use Of Commercial Type Vehicle TP')) and (LD_FLAG='L'))
    then ifnull(ENDT_AMT, 0) else 0 end) as UW_LOADING_AMT,
    
    max(case when(lower(LDDESC) = 'no claim bonus')
    then ifnull(LD_RATE, 0) else 0 end) as NCB_PERCENT
    
    from prem_ncb_view
    group by NUM_REFERENCE_NO
    """
    prem_ncb_1 = sqlContext.sql(sql_1)
    
    prem_reg_6A = prem_reg_6.join(prem_ncb_1, 'NUM_REFERENCE_NO', 'left')
    
    # ncb_disc.show()
    
    # prem_reg_6A.printSchema()
    
    ############################
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "policy_gc_co_insurance_details_tab"} 
    co_insurnc_detl=sqlContext.read.format("jdbc").options(**gscPythonOptions).load()\
    	.select('NUM_REFERENCE_NUMBER','LEADERNONLEADER','SHARE_PCT','TXT_COMPANY_SHORT_DESC')
    
    # .filter(col('NUM_REFERENCE_NUMBER')==lit('202001170097684'))
    # input_indices_name = 'policy_gc_co_insurance_details_tab_alias'
    # input_doc_type_name = input_indices_name+'/co_insurance_details_tab'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # co_insurnc_detl = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_REFERENCE_NUMBER,LEADERNONLEADER,SHARE_PCT,TXT_COMPANY_SHORT_DESC').\
    # option('es.query', esq5).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    co_insurnc_detl_1 = co_insurnc_detl.filter(co_insurnc_detl.TXT_COMPANY_SHORT_DESC == "TATAAIG")\
                                       .withColumnRenamed('NUM_REFERENCE_NUMBER', 'NUM_REFERENCE_NO')\
                                       .withColumn('POL_CO_INSURER_PERCENTAGE', 100-co_insurnc_detl.SHARE_PCT)
        
    co_insurnc_detl_2 = co_insurnc_detl_1.groupBy('NUM_REFERENCE_NO').agg(\
                                             sf.max('LEADERNONLEADER').alias('LEADERNONLEADER'),\
                                             sf.max('SHARE_PCT').alias('OUR_SHARE_POLICY_PERCENTAGE'),
                                             sf.max('POL_CO_INSURER_PERCENTAGE').alias('POL_CO_INSURER_PERCENTAGE'))
    
    co_insurnc_detl_3 = co_insurnc_detl_2.withColumn('COINSURANCE_CD',\
                                              when(trim(co_insurnc_detl_2.LEADERNONLEADER) == "L", '01').\
                                              when(trim(co_insurnc_detl_2.LEADERNONLEADER) == "N", '04').\
                                              otherwise('05'))\
                                         .withColumn('POL_CO_INSURER',\
                                              when(trim(co_insurnc_detl_2.LEADERNONLEADER) == "L", 'Leader').\
                                              when(trim(co_insurnc_detl_2.LEADERNONLEADER) == "N", 'Follower').\
                                              otherwise('No-Coinsurance'))\
                                         .drop('LEADERNONLEADER')
    
    co_insurnc_detl_4 = co_insurnc_detl_3
    # co_insurnc_detl_4 = co_insurnc_detl_3.withColumn('NUM_REFERENCE_NO', \
    #                                        co_insurnc_detl_3.NUM_REFERENCE_NO.cast(DecimalType(15,0)).cast(StringType()))
    
    prem_reg_7 = prem_reg_6A.join(co_insurnc_detl_4, 'NUM_REFERENCE_NO', 'left')
    
    # prem_reg_7.printSchema()
    
    prem_reg_8 = prem_reg_7.withColumn('OUR_SHARE_SUMINSURED',\
                                      ((prem_reg_7.OUR_SHARE_POLICY_PERCENTAGE * prem_reg_7.SUM_INSURED)/100))
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "underwriting_gc_cnfgtr_risk_grid_detail_tab",
        "server.port":"1160-1180",
        "partitionColumn":"row_num",
        "partitions":8}
    #     "server.port":"1160-1180",
    cnfgtr=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_REFERENCE_NUMBER','INFORMATION6','INFORMATION7')\
    # .select('NUM_REFERENCE_NUMBER')
    # .filter(col('NUM_REFERENCE_NUMBER')==lit('202001170097684')) 
    
    # input_indices_name = 'underwriting_gc_cnfgtr_risk_grid_detail_tab_alias'
    # input_doc_type_name = input_indices_name+'/cnfgtr_risk_grid_detail_tab'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # cnfgtr = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).option('es.read.field.include','NUM_REFERENCE_NUMBER').\
    # option('es.query', esq5).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    cnfgtr_1 = cnfgtr.withColumnRenamed("NUM_REFERENCE_NUMBER", "NUM_REFERENCE_NO")
    
    # cnfgtr_1 = cnfgtr_1.withColumn('NUM_REFERENCE_NO', \
    #                                        cnfgtr_1.NUM_REFERENCE_NO.cast(DecimalType(15,0)).cast(StringType()))
    
    prem_reg_tmp = prem_reg_8.filter((prem_reg_8.PRODUCT_CD == 2869)|(prem_reg_8.PRODUCT_CD== 2878))
    
    #prem_reg_tmp_1A = prem_reg_tmp[["NUM_REFERENCE_NO"]]
    #prem_reg_tmp_1 = prem_reg_tmp_1A.distinct()
    #cnfgtr_2 = cnfgtr_1.join(prem_reg_tmp_1, "NUM_REFERENCE_NO")
    #cnfgtr_3 = cnfgtr_2.groupBy("NUM_REFERENCE_NO").count().withColumnRenamed("count", "CNFGTR_RISK_COUNT")
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "server.port":"1160-1180",
        "dbtable": "policy_gc_risk_details",
        "partitionColumn":"row_num",
        "partitions":6}
    #     "server.port":"1160-1180",
    risk_d=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('REFERENCE_NUM')
    
    # .filter(col('REFERENCE_NUM')==lit('202001170097684'))
    # input_indices_name = 'policy_gc_risk_details_alias_alias'
    # input_doc_type_name = input_indices_name+'/risk_details'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # risk_d = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).option('es.read.field.include','REFERENCE_NUM').\
    # option('es.query', esq4).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    risk_d_1 = risk_d.withColumnRenamed("REFERENCE_NUM", "NUM_REFERENCE_NO")
    
    # risk_d_1A = risk_d_1.withColumn('NUM_REFERENCE_NO', \
    #                                        risk_d_1.NUM_REFERENCE_NO.cast(DecimalType(15,0)).cast(StringType()))
    risk_d_1A = risk_d_1
    
    # 20210512 - sagar - no of life logic modified mail - FW: No. Of lives covered A&H LOB
    NO_OF_LIFE_PRODUCT_LIST = [2861,2862,2863,2864,2888,2896,2807,2871,2874,2865,2866,2868,2875,1603,1602,1604,1606,\
                               1605,1601,4251,4254,4256,4253,4252,4260,4255,4258,4277,4257]
    prem_reg_risk = prem_reg_8.filter(col('PRODUCT_CD').isin())
    
    prem_reg_risk_1A = prem_reg_risk[["NUM_REFERENCE_NO"]]
    
    prem_reg_risk_1 = prem_reg_risk_1A.distinct()
    
    risk_d_2 = risk_d_1A.join(prem_reg_risk_1, "NUM_REFERENCE_NO")
    risk_d_3 = risk_d_2.groupBy("NUM_REFERENCE_NO").count().withColumnRenamed("count","RISK_DETAILS_COUNT")
    
    prem_reg_9 = prem_reg_8 #.join(cnfgtr_3, "NUM_REFERENCE_NO", "left")
    prem_reg_9A = prem_reg_9.join(risk_d_3, "NUM_REFERENCE_NO", "left")
    prem_reg_10 = prem_reg_9A.withColumnRenamed('RISK_DETAILS_COUNT','NO_OF_LIFE_COVERED')
    
    #prem_reg_11 = prem_reg_10.withColumn("NO_OF_LIFE_COVERED", \
    #                                    when(((prem_reg_10.PRODUCT_CD == 2861)|(prem_reg_10.PRODUCT_CD == 2862)|\
    #                                          (prem_reg_10.PRODUCT_CD == 2863)|(prem_reg_10.PRODUCT_CD == 2864)|\
    #                                          (prem_reg_10.PRODUCT_CD == 2871)|(prem_reg_10.PRODUCT_CD == 2874)|\
    #                                          (prem_reg_10.PRODUCT_CD == 2865)|(prem_reg_10.PRODUCT_CD == 2866)|\
    #                                          (prem_reg_10.PRODUCT_CD == 2868)),prem_reg_10.RISK_DETAILS_COUNT)\
    #                                   .when(((prem_reg_10.PRODUCT_CD == 2861)|(prem_reg_10.PRODUCT_CD == 2862))\
    #                                          ,prem_reg_10.CNFGTR_RISK_COUNT)
    #                                     .when(prem_reg_10.PRODUCT_CD == 2875, 1)
    #                                     .otherwise(0))\
    #                         .drop("RISK_DETAILS_COUNT")\
    #                         .drop("CNFGTR_RISK_COUNT")
    
    prem_reg_10A = prem_reg_10.withColumn("NO_OF_LIFE_COVERED", \
                                            when(col('SL_NO')==1, col('NO_OF_LIFE_COVERED'))\
                                            .otherwise(lit(0)))
    
    prem_reg_11 = prem_reg_10A.withColumn("NO_OF_LIFE_COVERED", \
                                            when(col('RECORD_TYPE_DESC')=='CANCELLATION', col('NO_OF_LIFE_COVERED')*-1)\
                                            .otherwise(col('NO_OF_LIFE_COVERED')))
    
    #reference index - for vehicle related columns
    # policy_gc_risk_headers_alias
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "server.port":"1160-1180",
        "dbtable": "policy_gc_risk_headers",
        "partitionColumn":"row_num",
        "partitions":6}
    #     "server.port":"1210",
    risk_source=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('REFERENCE_NUM', 'PRODUCT_INDEX', 'INFORMATION5', 'INFORMATION22', 'INFORMATION23', 'INFORMATION33',\
    'INFORMATION36', 'INFORMATION38', 'INFORMATION125', 'INFORMATION6', 'INFORMATION15')
    # .filter(col('REFERENCE_NUM')==lit(202001170097684))
    
    # risk_source.show()
    
    # gscPythonOptions = {
    #     "url": gpdb_url,
    #     "user": gpdb_user,
    #     "password": gpdb_pass,
    #     "dbschema": "public",
    #     "dbtable": "policy_gc_risk_headers"} 
    # risk_test=sqlContext.read.format("jdbc").options(**gscPythonOptions).load()\
    # .select('REFERENCE_NUM', 'PRODUCT_INDEX', 'INFORMATION5', 'INFORMATION22', 'INFORMATION23', 'INFORMATION33',\
    # 'INFORMATION36', 'INFORMATION38', 'INFORMATION125', 'INFORMATION6', 'INFORMATION15')
    
    # risk_test.show(5)
    
    # from pg import DB
    
    # db = DB(dbname='gpadmin', host='10.35.12.194', port=5432,user='gpspark', passwd='spark@456')
    # myvar = db.query("""select * from public.policy_gc_risk_headers limit 5""")
    
    # print myvar
    
    # input_indices_name = 'policy_gc_risk_headers_alias'
    # input_doc_type_name = input_indices_name+'/risk_headers'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # risk_source = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','REFERENCE_NUM, PRODUCT_INDEX, INFORMATION5, INFORMATION22, INFORMATION23, INFORMATION33, \
    # INFORMATION36, INFORMATION38, INFORMATION125, INFORMATION6, INFORMATION15').\
    # option('es.query', esq4).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    risk_A = risk_source\
    					.withColumn('NUM_REFERENCE_NO', \
                                           risk_source.REFERENCE_NUM.cast(DecimalType(15,0)).cast(StringType()))\
    									   
    risk = risk_A.join(prem_reg_nm_rfrnc_no_distnct,'NUM_REFERENCE_NO')
    
    # commented below code as we are taking MOTOR_MANUFACTURER_NAME, MOTOR_MODEL_NAME and NEW_PURCHASE_FG from risk_headers_mot
    # risk_1 = risk\
    #              .withColumn("MOTOR_MANUFACTURER_NAME",\
    #                           when(risk.PRODUCT_INDEX == "N155266", risk.INFORMATION36)\
    #                          .when(risk.PRODUCT_INDEX == "N162253", risk.INFORMATION22)\
    #                          .when(risk.PRODUCT_INDEX == "N093891", risk.INFORMATION38)\
    #                          .otherwise("NA"))\
    #              .withColumn("MOTOR_MODEL_NAME", \
    #                           when(risk.PRODUCT_INDEX == "N155266", risk.INFORMATION5)\
    #                          .when(risk.PRODUCT_INDEX == "N162253", risk.INFORMATION23)\
    #                          .when(risk.PRODUCT_INDEX == "N093891", risk.INFORMATION36)\
    #                          .otherwise("NA"))\
    #              .withColumn("VEHICLE_AGE", \
    #                           when(risk.PRODUCT_INDEX == "N093891", risk.INFORMATION6)\
    #                          .when(risk.PRODUCT_INDEX == "N162253", risk.INFORMATION33)\
    #                          .when(risk.PRODUCT_INDEX == "N155266", risk.INFORMATION15)\
    #                          .otherwise("NA"))\
    #             .drop('INFORMATION5')\
    #             .drop('INFORMATION22')\
    #             .drop('INFORMATION23')\
    #             .drop('INFORMATION33')\
    #             .drop('INFORMATION36')\
    #             .drop('INFORMATION38')\
    # 			.drop('REFERENCE_NUM')
    #             
    #             
    # #              .withColumnRenamed("INFORMATION125", "DISCOUNT")\            
    # 
    # risk_2 = risk_1.groupBy('NUM_REFERENCE_NO', 'PRODUCT_INDEX').\
    #                                             agg(sf.max('MOTOR_MANUFACTURER_NAME').alias('MOTOR_MANUFACTURER_NAME'),\
    #                                                 sf.max('MOTOR_MODEL_NAME').alias('MOTOR_MODEL_NAME'),\
    #                                                 sf.max('VEHICLE_AGE').alias('VEHICLE_AGE'))
    # 
    # risk_2 = risk_2.withColumn('NUM_REFERENCE_NO', \
    #                                        risk_2.NUM_REFERENCE_NO.cast(DecimalType(15,0)).cast(StringType()))\
    #                .withColumn('NEW_PURCHASE_FG_tmp', when((risk_2.VEHICLE_AGE.cast(IntegerType()) > 0) &\
    #                                                    (risk_2.VEHICLE_AGE.cast(IntegerType()) < 2), "Y").otherwise("N"))\
    #                .drop("VEHICLE_AGE")
    #         
    # risk_3 = risk_2.withColumn('NEW_PURCHASE_FG', when((risk_2.PRODUCT_INDEX == "N093891")|\
    #                                                    (risk_2.PRODUCT_INDEX == "N162253")|\
    #                                                    (risk_2.PRODUCT_INDEX == "N155266") , risk_2.NEW_PURCHASE_FG_tmp))
    # 
    # prem_reg_11A = prem_reg_11.join(risk_3, ["NUM_REFERENCE_NO","PRODUCT_INDEX"], "left")
    
    # prem_reg_12 = prem_reg_11A.drop('CLIENT_NAME') --before new purchase fg
    # prem_reg_12_nfg = prem_reg_11A.drop('CLIENT_NAME')
    prem_reg_12_nfg = prem_reg_11.drop('CLIENT_NAME')
    
    # Added this snippet for new purchase fg column logic change
    prem_reg_12_nfg1 = prem_reg_12_nfg.drop('NEW_PURCHASE_FG')
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "server.port":"1109",
        "dbtable": "policy_dh_risk_headers_mot",
        "partitionColumn":"row_num",
        "partitions":10}

    risk_hdr_mot = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('REFERENCE_NUM','RISK1','REGISTRATION_NO_AS_OLD_LOGIC','REGISTRATION_NUMBER', 'REGISTRATIONNUMBER2','REGISTRATIONNUMBER3','REGISTRATIONNUMBER4','REGISTRATIONNUMBERSECTION1','REGISTRATIONNUMBERSECTION2','REGISTRATIONNUMBERSECTION3',\
            'REGISTRATIONNUMBERSECTION4','VEHICLE_AGE','MANUFACTURER','MODEL')\
    # .filter(col("REFERENCE_NUM").isin(list_nrn))
    
    risk_hdr_mot_1A = risk_hdr_mot.withColumn('VEHICLE_REGISTRATION_NO_OLD',(sf.concat(sf.col('REGISTRATIONNUMBERSECTION1'),\
                                                                        sf.col('REGISTRATIONNUMBERSECTION2'), \
                                                                        sf.col('REGISTRATIONNUMBERSECTION3'), \
                                                                        sf.col('REGISTRATIONNUMBERSECTION4'))))\
                                 .withColumn('VEHICLE_REGISTRATION_NO_NEW',(sf.concat(sf.col('REGISTRATION_NUMBER'),\
                                                                        sf.col('REGISTRATIONNUMBER2'), \
                                                                        sf.col('REGISTRATIONNUMBER3'), \
                                                                        sf.col('REGISTRATIONNUMBER4'))))\
                                 .withColumnRenamed('REFERENCE_NUM', 'NUM_REFERENCE_NO')
    
    risk_hdr_mot_1 = risk_hdr_mot_1A.withColumn('VEHICLE_REGISTRATION_NO', \
                                         when(upper(col('REGISTRATION_NO_AS_OLD_LOGIC'))=='TRUE',col('VEHICLE_REGISTRATION_NO_OLD'))\
                                         .otherwise(col('VEHICLE_REGISTRATION_NO_NEW')))
        
    risk_hdr_mot_2 = risk_hdr_mot_1.groupBy('NUM_REFERENCE_NO')\
                                   .agg(sf.max('VEHICLE_REGISTRATION_NO').alias('VEHICLE_REGISTRATION_NO'),\
                                        sf.max('VEHICLE_AGE').alias('VEHICLE_AGE'),\
                                        sf.max('RISK1').alias('POLICY_TYPE'),\
                                        sf.max('MANUFACTURER').alias('MOTOR_MANUFACTURER_NAME'),\
                                        sf.max('MODEL').alias('MOTOR_MODEL_NAME'))
        
    risk_hdr_mot_3 = risk_hdr_mot_2.withColumn('NEW_PURCHASE_FG', when(risk_hdr_mot_2.VEHICLE_AGE < 1, 'Y')\
                                                                  .otherwise(lit('N')))
    
    risk_hdr_mot_4 = risk_hdr_mot_3.drop('VEHICLE_AGE')
        
    prem_reg_12 = prem_reg_12_nfg1.join(risk_hdr_mot_4, 'NUM_REFERENCE_NO', 'left')
    
    # new purchase fg change ends here
    
    
    # reading few columns from customer 
    # gc_genmst_customer
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "server.port":"1160-1180",
        "dbtable": "customer_gc_genmst_customer",
        "partitionColumn":"row_num",
        "partitions":8}
    #     "server.port":"1211",
    # adr_1_pincode
    customer=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('ADR_2_CITY_TOWN_NAME', 'AFFILIATION_FLAG', 'ADR_2_PINCODE', 'ADR_1_PINCODE', 'CUSTOMER_CODE',\
    'ADR_1_CITY_TOWN_NAME', 'ADR_1_STATE', 'ADR_1_STATE_CODE', 'CUST_GSTIN', 'IND_CORP_FLAG', 'MAIL_LOCATION_CD',\
    'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CUSTOMER_NAME')
    # .filter(col('CUSTOMER_CODE')==lit('6006212705'))
    
    # input_indices_name = 'customer_gc_genmst_customer_alias'
    # input_doc_type_name = input_indices_name+'/genmst_customer'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # customer = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','ADR_2_CITY_TOWN_NAME, AFFILIATION_FLAG,ADR_2_PINCODE,ADR_1_PINCODE,CUSTOMER_CODE,\
    # ADR_1_CITY_TOWN_NAME, ADR_1_STATE, ADR_1_STATE_CODE, CUST_GSTIN, IND_CORP_FLAG, NUM_MAIL_LOCATION_CD,\
    # FIRST_NAME , MIDDLE_NAME, LAST_NAME, CUSTOMER_NAME').\
    # option('es.query', esq6).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    customer_1AB = customer\
                        .withColumnRenamed("CUSTOMER_CODE", "CUSTOMER_ID")\
                        .withColumnRenamed("IND_CORP_FLAG", "CUST_TYPE")\
                        .withColumn("PIN", \
                                   when(customer.ADR_2_PINCODE.isNull(),customer.ADR_1_PINCODE)\
                                    .otherwise(customer.ADR_2_PINCODE))\
                        .withColumn("MAP_FG", \
                                   when(customer.AFFILIATION_FLAG.isNull(),lit("N"))\
                                    .otherwise(customer.AFFILIATION_FLAG))\
                        .drop("ADR_2_PINCODE")\
                        .drop("ADR_1_PINCODE")\
                        .drop("AFFILIATION_FLAG")
    
    # optimization
    customer_1A = customer_1AB.join(prem_reg_cust_id_distnct,'CUSTOMER_ID')
    
    customer_1 = customer_1A.withColumn('CLIENT_NAME', when(upper(trim(customer_1A.CUST_TYPE))=='C', customer_1A.CUSTOMER_NAME)\
                                                       .otherwise(concat_ws(' ', customer_1A.FIRST_NAME,customer_1A.MIDDLE_NAME,customer_1A.LAST_NAME)))\
                            .withColumnRenamed('MAIL_LOCATION_CD', 'NUM_MAIL_LOCATION_CD')\
                            .withColumn('PIN', customer_1A.PIN.cast(DecimalType(10,0)).cast(StringType()))\
    
    # altered the code because ETL team took MAIL_LOCATION_CD instead of NUM_MAIL_LOCATION_CD during ETL of above index
    #                         .withColumn('NUM_MAIL_LOCATION_CD', customer_1A.NUM_MAIL_LOCATION_CD.cast(DecimalType(20,0)).cast(StringType()))\
    
            
    customer_2 = customer_1.groupBy('CUSTOMER_ID').agg(sf.max('PIN').alias('PIN'),\
                                                       sf.max('CUST_TYPE').alias('CUST_TYPE'),\
                                                       sf.max('MAP_FG').alias('MAP_FG'),\
                                                       sf.max('NUM_MAIL_LOCATION_CD').alias('NUM_MAIL_LOCATION_CD'),\
                                                       sf.max('CUST_GSTIN').alias('CUSTOMER_GSTIN'),\
                                                       sf.max('CLIENT_NAME').alias('CLIENT_NAME'))
    
    prem_reg_12A = prem_reg_12.join(customer_2, "CUSTOMER_ID", "left")
    
    cust_loc_distinct = customer_2.select('NUM_MAIL_LOCATION_CD').distinct()
    
    # prem_reg_13 = prem_reg_13.withColumn('DEPARTMENTCODE', lit(0))
    
    # CUST_RES_CITY and CUST_RES_DIST new logic - 2019-05-20
    
    # prem_reg_12A.printSchema()
    
    # reading few columns from customer 
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "server.port":"1160-1180",
        "dbtable": "customer_gc_genmst_location",
        "partitionColumn":"row_num",
        "partitions":8}
    #     "server.port":"1205",
    location=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_LOCATION_CD', 'NUM_CITYDISTRICT_CD','NUM_STATE_CD','NUM_CITY_CD','TXT_ADDRESS_LINE_1','TXT_ADDRESS_LINE_2')
    # .filter(col('NUM_LOCATION_CD')==lit('10000334961'))
    
    # input_indices_name = 'customer_gc_genmst_location_alias'
    # input_doc_type_name = input_indices_name+'/genmst_location'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # location = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_LOCATION_CD, NUM_CITYDISTRICT_CD, NUM_STATE_CD, NUM_CITY_CD').\
    # option('es.query', esq7).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    location_1A1 = location.withColumnRenamed('NUM_LOCATION_CD', 'NUM_MAIL_LOCATION_CD')\
                           .withColumn('TXT_ADDRESS_LINE_1', when(col('TXT_ADDRESS_LINE_1').isNull(), lit(''))\
                                                             .otherwise(col('TXT_ADDRESS_LINE_1')))\
                           .withColumn('TXT_ADDRESS_LINE_2', when(col('TXT_ADDRESS_LINE_2').isNull(), lit(''))\
                                                             .otherwise(col('TXT_ADDRESS_LINE_2')))
    
    location_1AB = location_1A1.withColumn('CUST_ADDRESS', \
                                           sf.concat(sf.col('TXT_ADDRESS_LINE_1'),lit(' '),sf.col('TXT_ADDRESS_LINE_2')))
    
    # optimization
    location_1A = location_1AB.join(cust_loc_distinct, 'NUM_MAIL_LOCATION_CD')
    
    location_1 = location_1A.groupBy('NUM_MAIL_LOCATION_CD').agg(sf.max('NUM_STATE_CD').alias('CUST_STATE_CD'),\
                                                                 sf.max('NUM_CITYDISTRICT_CD').alias('CUST_DIST_CD'),\
                                                                 sf.max('NUM_CITY_CD').alias('CUST_CITY_CD'),\
                                                                 sf.max('CUST_ADDRESS').alias('CUST_ADDRESS'))
    
    
    
    prem_reg_12B = prem_reg_12A.join(location_1, "NUM_MAIL_LOCATION_CD", "left")
    
    cust_genmst_state = genmst_state.withColumnRenamed('NUM_STATE_CD', 'CUST_STATE_CD')\
                                    .withColumnRenamed('TXT_STATE', 'CUST_RES_STATE')
    
    
        
    cust_genmst_state_1 = cust_genmst_state.groupBy('CUST_STATE_CD')\
                                           .agg(sf.max('CUST_RES_STATE').alias('CUST_RES_STATE'))
        
    prem_reg_12C = prem_reg_12B.join(cust_genmst_state_1, 'CUST_STATE_CD', 'left')
    cust_genmst_district = genmst_city.filter(upper(genmst_city.TXT_CITY_DISTRICT_FLAG) == "DISTRICT")\
                                  .withColumnRenamed('NUM_CITYDISTRICT_CD', 'CUST_DIST_CD')\
                                  .withColumnRenamed('TXT_CITYDISTRICT', 'CUST_RES_DIST')\
                                  .withColumnRenamed('NUM_STATE_CD', 'CUST_STATE_CD')\
                                  .drop('TXT_CITY_DISTRICT_FLAG')
                
    cust_genmst_district = cust_genmst_district.groupBy('CUST_DIST_CD','CUST_STATE_CD').agg(sf.max('CUST_RES_DIST').alias('CUST_RES_DIST'))
    
    prem_reg_12D = prem_reg_12C.join(cust_genmst_district, ['CUST_DIST_CD', 'CUST_STATE_CD'], 'left')
    
    # not applying TXT_CITY_DISTRICT_FLAG = "CITY" filter on city dataframe as no record exist for few cities like DELHI with TXT_CITY_DISTRICT_FLAG = "CITY". instead sorting data on TXT_CITY_DISTRICT_FLAG, so if record with city flag available fine if not then take district record.
    
    cust_genmst_city = genmst_city.withColumnRenamed('NUM_CITYDISTRICT_CD', 'CUST_CITY_CD')\
                                  .withColumnRenamed('TXT_CITYDISTRICT', 'CUST_RES_CITY')\
                                  .withColumnRenamed('NUM_STATE_CD', 'CUST_STATE_CD')\
                
                
    cust_genmst_city_1 = cust_genmst_city\
                             .withColumn("ROW_NUM", sf.row_number()\
                                   .over(Window.partitionBy('CUST_CITY_CD', 'CUST_STATE_CD')\
                                         .orderBy(cust_genmst_city.TXT_CITY_DISTRICT_FLAG.asc())))
        
    cust_genmst_city_2 = cust_genmst_city_1.filter(cust_genmst_city_1.ROW_NUM==1)
    cust_genmst_city_3 = cust_genmst_city_2.groupBy('CUST_CITY_CD', 'CUST_STATE_CD').agg(sf.max('CUST_RES_CITY').alias('CUST_RES_CITY'))
    
    
    prem_reg_12E = prem_reg_12D.join(cust_genmst_city_3, ['CUST_CITY_CD','CUST_STATE_CD'], 'left')
    prem_reg_13 = prem_reg_12E.withColumnRenamed('CUST_ADDRESS', 'CUSTOMER_ADDRESS')
    # withColumn('CUSTOMER_ADDRESS', sf.concat(sf.col('CUST_ADDRESS'),lit(' '),sf.col('CUST_RES_CITY'))).drop('CUST_ADDRESS')
    
    # prem_reg_12D.select('CUST_CITY_CD', 'CUST_STATE_CD').printSchema()
    
    # cust_genmst_city_1.printSchema()
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_gc_uw_department_master"} 
    lob=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('DEPARTMENTCODE', 'DEPARTMENTNAME')
    
    # input_indices_name = 'reference_gc_uw_department_master'
    # input_doc_type_name = input_indices_name+'/uw_department_master'
    # 
    # document_count =documentcount(input_doc_type_name)
    # 
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # #esq2 = """{  "query": {"bool": {"must": {"terms": {"policy_no": [  "0189303306" ]}}}}}"""
    # lob = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','DEPARTMENTCODE, DEPARTMENTNAME').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    lob_1 = lob.withColumnRenamed("DEPARTMENTNAME", "PRODUCT_TYPE_1")
    
    prem_reg_13A = prem_reg_13.join(lob_1, "DEPARTMENTCODE", "left")
    
    #reading zone - 121
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "mappers",
        "dbtable": "prodcomm_branch_zone_mapper"} 
    zone=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('REGBRANCH','BRANCH')
    
    #     "dbschema": "public",
    #     "dbtable": "reference_zone_master"} 
    # Sandeep: Changed as per Ankita's input: Sep 18, 2020
    # input_indices_name = 'reference'
    # input_doc_type_name = input_indices_name+'/zone_master'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # #esq2 = """{  "query": {"bool": {"must": {"terms": {"policy_no": [  "0189303306" ]}}}}}"""
    # zone = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','RegBranch,Branch').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    zone_1 = zone\
                 .withColumnRenamed("REGBRANCH", "ZONE")\
                 .withColumn('BRANCH', trim(upper(zone.BRANCH)))
    zone_2 = zone_1.groupBy('BRANCH').agg(sf.max('ZONE').alias('ZONE'))       
    
    prem_reg_13B = prem_reg_13.withColumn('BRANCH', trim(upper(prem_reg_13.BRANCH)))  
    prem_reg_14 = prem_reg_13B.join(zone_2, 'BRANCH', "left")
    # prem_reg_14 = prem_reg_13
    
    # FOR PRODUCER_TYPE,agent.TXT_PAN_NO as AGENT_IT_PAN_NUM,agent.TXT_LICENSE_NO as AGENT_LICENSE_CODE,
    # loc.TXT_STATE as AGENT_STATE,loc.TXT_CITYDISTRICT as AGENT_CITY and CHANNEL
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_gc_cnfgtr_intermediary_cat_master"} 
    producer=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('NUM_CATEGORY_ID', 'TXT_CATEGORY')
    
    # input_indices_name = 'reference_gc_cnfgtr_intermediary_cat_master'
    # input_doc_type_name = input_indices_name+'/cnfgtr_intermediary_cat_master'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # #esq2 = """{  "query": {"bool": {"must": {"terms": {"policy_no": [  "0189303306" ]}}}}}"""
    # producer = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_CATEGORY_ID, TXT_CATEGORY').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    producer_1 = producer\
                 .withColumnRenamed("TXT_CATEGORY", "PRODUCER_TYPE")
        
    producer_2 = producer_1.groupBy('PRODUCER_TYPE').agg(sf.max('NUM_CATEGORY_ID').alias('NUM_CATEGORY_ID'))
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_gc_genmst_intermediary"} 
    genmst_intermediary=sqlContext.read.format("jdbc").options(**gscPythonOptions).load()\
    	.select('TXT_INTERMEDIARY_CD','TXT_PAN_NO','TXT_LICENSE_NO','NUM_LOCATION_CD','NUM_CATEGORY_ID','NUM_CHANNEL_ID')
    # .filter(trim(col('TXT_INTERMEDIARY_CD'))==lit('0012274000'))
    
    # input_indices_name = 'reference_gc_genmst_intermediary_alias'
    # input_doc_type_name = input_indices_name+'/genmst_intermediary'
    # 
    # document_count =documentcount(input_doc_type_name)
    # 
    # genmst_intermediary = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','TXT_INTERMEDIARY_CD,TXT_PAN_NO,TXT_LICENSE_NO,NUM_LOCATION_CD,NUM_CATEGORY_ID,NUM_CHANNEL_ID').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    genmst_intermediary = genmst_intermediary.withColumn("TXT_INTERMEDIARY_CD", trim(genmst_intermediary.TXT_INTERMEDIARY_CD))
    genmst_intermediary_1 = genmst_intermediary\
                                               .withColumnRenamed("TXT_INTERMEDIARY_CD", "PRODUCER_CD")\
                                               .withColumnRenamed("TXT_PAN_NO", "AGENT_IT_PAN_NUM")\
                                               .withColumnRenamed("TXT_LICENSE_NO", "AGENT_LICENSE_CODE")\
                                               .withColumnRenamed("NUM_CHANNEL_ID", "NUM_CHANNEL_CODE")\
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "server.port":"1160-1180",
        "dbtable": "customer_gc_genmst_location",
        "partitionColumn":"row_num",
        "partitions":8}
    #     "server.port":"1205",
    location=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_LOCATION_CD','NUM_STATE_CD', 'NUM_CITY_CD')
    
    # gscPythonOptions = {
    #     "url": gpdb_url,
    #     "user": gpdb_user,
    #     "password": gpdb_pass,
    #     "dbschema": "public",
    #     "dbtable": "customer_gc_genmst_location"} 
    # location=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('NUM_LOCATION_CD','NUM_STATE_CD', 'NUM_CITY_CD')
    # .filter(col('NUM_LOCATION_CD')==lit('10000828947'))
    # input_indices_name = 'customer_gc_genmst_location_alias'
    # input_doc_type_name = input_indices_name+'/genmst_location'
    
    # document_count =documentcount(input_doc_type_name)
    
    # genmst_location = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_LOCATION_CD,NUM_STATE_CD, NUM_CITY_CD').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    genmst_location = location.groupBy('NUM_LOCATION_CD')\
                              .agg(sf.max('NUM_STATE_CD').alias('NUM_STATE_CD'), \
                                   sf.max('NUM_CITY_CD').alias('NUM_CITY_CD'))
    
    ###############################################################
    
    # for agent_city only city data required from genmst_citydistrict. hence applying TXT_CITY_DISTRICT_FLAG filter.
    
    genmst_city_2 = genmst_city_1.filter(upper(genmst_city_1.TXT_CITY_DISTRICT_FLAG) == "CITY")
    location_city = genmst_location.join(genmst_city_2, 'NUM_CITY_CD', 'left')
    location_city_state = location_city.join(genmst_state, 'NUM_STATE_CD', 'left')
    
    agent_city_state = location_city_state.withColumnRenamed('TXT_CITYDISTRICT', 'AGENT_CITY')\
                                          .withColumnRenamed('TXT_STATE', 'AGENT_STATE')
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_gc_genmst_producer_channel"} 
    channel=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('NUM_CHANNEL_CODE','TXT_CHANNEL_NAME')
    
    # input_indices_name = 'reference_gc_genmst_producer_channel'
    # input_doc_type_name = input_indices_name+'/genmst_producer_channel'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # channel = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_CHANNEL_CODE,TXT_CHANNEL_NAME').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    channel_1 = channel.withColumnRenamed("TXT_CHANNEL_NAME", "CHANNEL")    
        
    
    attribute_1 = genmst_intermediary_1.join(producer_1, "NUM_CATEGORY_ID", "left")
    attribute_2 = attribute_1.join(agent_city_state, "NUM_LOCATION_CD", "left")
    attribute_3 = attribute_2.join(channel_1, "NUM_CHANNEL_CODE", "left")
    
    
    attribute_4 = attribute_3.groupBy('PRODUCER_CD').agg(sf.max('PRODUCER_TYPE').alias('PRODUCER_TYPE'),\
                                                         sf.max('AGENT_LICENSE_CODE').alias('AGENT_LICENSE_CODE'),\
                                                         sf.max('AGENT_IT_PAN_NUM').alias('AGENT_IT_PAN_NUM'),\
                                                         sf.max('AGENT_CITY').alias('AGENT_CITY'),\
                                                         sf.max('AGENT_STATE').alias('AGENT_STATE'),\
                                                         sf.max('CHANNEL').alias('CHANNEL'))   
    prem_reg_15 = prem_reg_14.join(attribute_4, "PRODUCER_CD", "left")
    
    # prem_reg_new = prem_reg_3.join(attribute_4, "PRODUCER_CD", "left")
    
    # prem_reg_new.select('POLICY_NO','PRODUCER_CD','CHANNEL').show()
    
    # prem_reg_15 = prem_reg_14.join(attribute_4, "PRODUCER_CD", "left")
    
    # for devel_name - new 
    
    # input_indices_name = 'reference'
    # input_doc_type_name = input_indices_name+'/rpt_developer_map'
    
    # document_count =documentcount(input_doc_type_name)
    
    # devel = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).option('es.read.field.include','producer_cd, developer_name').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    # devel_1 = devel.withColumn('PRODUCER_CD', trim(devel.producer_cd))
            
    # devel_2 = devel_1.groupBy('PRODUCER_CD').agg(sf.max('developer_name').alias('DEVEL_DET'))
            
    # prem_reg_15 = prem_reg_15.withColumn('BRANCH', trim(upper(prem_reg_15.BRANCH)))\
    #                          .withColumn('PRODUCER_CD', trim(prem_reg_15.PRODUCER_CD))
    
    # prem_reg_15A = prem_reg_15.join(devel_2, ['PRODUCER_CD'], "left")
    
    # for devel_det new mapping - 2019-05-20 please refer email - "FW: require Devel_det and Devel_CD logic/mapping"
    
    #######################################
    # devel_code new logic - 2019-04-17
    #######################################
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_prodcuer_branch_cdm_mapping"} 
    cdm_mapping=sqlContext.read.format("jdbc").options(**gscPythonOptions).load()\
    	.select('branch_cd', 'Sub_branch_cd', 'VerticalName', 'CDMName', 'producer_cd', 'CDMCode')
    
    # input_indices_name = 'reference'
    # input_doc_type_name = input_indices_name+'/prodcuer_branch_cdm_mapping'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # #esq2 = """{  "query": {"bool": {"must": {"terms": {"policy_no": [  "0189303306" ]}}}}}"""
    # cdm_mapping = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','branch_cd, Sub_branch_cd, VerticalName, CDMName, producer_cd,CDMCode').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    cdm_mapping_1 = cdm_mapping.withColumnRenamed("CDMCode", "DEVEL_CODE")\
                               .withColumnRenamed("CDMName", "DEVEL_DET")\
                               .withColumnRenamed("branch_cd", "BRANCH_OFF_CD")\
                               .withColumnRenamed("Sub_branch_cd", "PRDR_BRANCH_SUB_CD")\
                               .withColumnRenamed("producer_cd", "PRODUCER_CD")
    
    cdm_mapping_2 = cdm_mapping_1.groupBy('PRODUCER_CD', 'BRANCH_OFF_CD', 'PRDR_BRANCH_SUB_CD')\
                                 .agg(sf.max('DEVEL_CODE').alias('DEVEL_CODE'),\
                                      sf.max('DEVEL_DET').alias('DEVEL_DET'))
        
    cdm_mapping_3 = cdm_mapping_1.groupBy('PRODUCER_CD')\
                                 .agg(sf.max('DEVEL_CODE').alias('DEVEL_CODE_BY_PRODUCER_CD'),\
                                      sf.max('DEVEL_DET').alias('DEVEL_DET_BY_PRODUCER_CD'))
    
    prem_reg_15A = prem_reg_15.join(cdm_mapping_2, ['PRODUCER_CD', 'BRANCH_OFF_CD', 'PRDR_BRANCH_SUB_CD'], 'left')
    
    # testing purpose
    prem_reg_16 = prem_reg_15A.join(cdm_mapping_3, 'PRODUCER_CD', 'left')
    
    # optimization
    
    # FOR CRS RECEIPT DATE
    # account_gc_acc_payment_entry_alias
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "server.port":"1160-1180",
        "dbtable": "account_gc_acc_payment_entry",
        "partitionColumn":"row_num",
        "partitions":6}
    #     "server.port":"1207",
    receipt_date=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_INSTRUMENT_ID', 'DAT_ACCEPTANCE_DATE','DAT_INSTRUMENT_DATE','TXT_PAYMENT_MODE_CD')
    
    # .filter(col("NUM_INSTRUMENT_ID")==lit(102001013902200))
    # input_indices_name = 'account_gc_acc_payment_entry_alias'
    # input_doc_type_name = input_indices_name+'/acc_payment_entry'
    # 
    # document_count =documentcount(input_doc_type_name)
    # 
    # receipt_date = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
    # option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_INSTRUMENT_ID, DAT_ACCEPTANCE_DATE').\
    # option('es.query', esq9).\
    # option('es.input.max.docs.per.partition',document_count).load()
    receipt_date_1A = receipt_date.withColumnRenamed("DAT_ACCEPTANCE_DATE", "CRS_RCPT_DATE")\
                                 .withColumnRenamed("NUM_INSTRUMENT_ID", "RECEIPT_NO")\
                                 .withColumnRenamed("DAT_INSTRUMENT_DATE", "INSTRUMENT_DATE")\
    
    # optimization
    receipt_date_1 = receipt_date_1A.join(rcpt_no_distinct, 'RECEIPT_NO')
    
    receipt_date_2 =  receipt_date_1.groupBy('RECEIPT_NO')\
                                    .agg(sf.max('CRS_RCPT_DATE').alias('CRS_RCPT_DATE'),\
                                         sf.max('INSTRUMENT_DATE').alias('INSTRUMENT_DATE'),\
                                         sf.max('TXT_PAYMENT_MODE_CD').alias('TXT_PAYMENT_MODE_CD'))       
    
    prem_reg_17 = prem_reg_16.join(receipt_date_2, "RECEIPT_NO", "left")    
    
    # null handling for CRS_RCPT_DATE 
    prem_reg_17 = prem_reg_17.withColumn('CRS_RCPT_DATE_tmp', when(prem_reg_17.CRS_RCPT_DATE.isNull(), lit('1900-01-01'))\
                                                              .otherwise(prem_reg_17.CRS_RCPT_DATE))\
                             
    
    prem_reg_17 = prem_reg_17.drop('CRS_RCPT_DATE')\
                             .withColumnRenamed('CRS_RCPT_DATE_tmp', 'CRS_RCPT_DATE')
    						 
    prem_reg_18 = prem_reg_17.withColumn('INSTRUMENT_DATE', when(col('INSTRUMENT_DATE').isNull(), col('CRS_RCPT_DATE'))\
                                                            .otherwise(col('INSTRUMENT_DATE')))\
                             .withColumn('PROPOSAL_DATE',when(col('PROPOSAL_DATE').isNull(), col('TIMESTAMP'))\
                                                         .otherwise(col('PROPOSAL_DATE')))\
                                                         
    gscPythonOptions = {"url": gpdb_url,"user": gpdb_user,"password": gpdb_pass,
        "dbschema": "public", "dbtable": "reference_gc_accmst_payment_mode_desc"} 
    accmst_pymnt_mode = sqlContext.read.format("jdbc").options(**gscPythonOptions).load()\
    .select('TXT_PAYMENT_MODE_CD', 'TXT_PAYMENT_MODE_DESC')
    
    accmst_pymnt_mode_1 = accmst_pymnt_mode\
    .groupBy('TXT_PAYMENT_MODE_CD')\
    .agg(sf.max('TXT_PAYMENT_MODE_DESC').alias('MODE_OF_PAYMENT'))
    
    prem_reg_23 = prem_reg_18.join(accmst_pymnt_mode_1, 'TXT_PAYMENT_MODE_CD', 'left')\
                             .drop('TXT_PAYMENT_MODE_CD')
    
    # BRANCH_STATE_CODE, BRANCH_STATE_NAME new mapping - 2019-05-20
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_gc_genmst_tab_office"} 
    genmst_office=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('NUM_OFFICE_CD', 'NUM_STATE_CD', 'TXT_OFFICE')
    
    # input_indices_name = 'reference_gc_genmst_tab_office_alias'
    # input_doc_type_name = input_indices_name+'/genmst_tab_office'
    # 
    # document_count =documentcount(input_doc_type_name)
    # 
    # genmst_office = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_OFFICE_CD, NUM_STATE_CD, TXT_OFFICE').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    genmst_office_1 = genmst_office.withColumnRenamed('NUM_OFFICE_CD', 'TXT_BRANCH_OFFICE_CODE')\
                             .withColumn('NUM_STATE_CD', genmst_office.NUM_STATE_CD.cast(DecimalType(10,0)).cast(StringType()))
        
    genmst_office_2 = genmst_office_1.groupBy('TXT_BRANCH_OFFICE_CODE').agg(\
                                sf.max('NUM_STATE_CD').alias('BRANCH_STATE_CD'))
    
    prem_reg_23 = prem_reg_23.join(genmst_office_2, 'TXT_BRANCH_OFFICE_CODE', 'left')
    
    branch_genmst_state = genmst_state.withColumnRenamed('NUM_STATE_CD', 'BRANCH_STATE_CD')\
                                      .withColumnRenamed('TXT_GSTIN_NO', 'TAGIC_GSTIN')
        
    branch_genmst_state_1 = branch_genmst_state.groupBy('BRANCH_STATE_CD')\
                                               .agg(sf.max('TAGIC_GSTIN').alias('TAGIC_GSTIN'),\
                                                    sf.max('TXT_STATE').alias('TXT_STATE'))
    
    prem_reg_23A = prem_reg_23.join(branch_genmst_state_1, 'BRANCH_STATE_CD', 'left')
    
    prem_reg_24 = prem_reg_23A.withColumnRenamed('BRANCH_STATE_CD', 'BRANCH_STATE_CODE')\
                              .withColumnRenamed('TXT_STATE', 'BRANCH_STATE_NAME')
    						  
    # for MAIN_LOB below three mapper used
    # 1. zone_new_commercial_mapper
    # 2. channel_mapping_1
    # 3. direct_cons_comm_segment
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "direct_cons_comm_segment"} 
    direct_cons_comm_segment=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('Product_Name', 'Minor_Line', 'New_Direct', 'Min_Line_3')
    
    # input_indices_name = 'direct_cons_comm_segment'
    # input_doc_type_name = input_indices_name+'/direct_cons_comm_segment'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # direct_cons_comm_segment = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes',es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','Product_Name, Minor_Line, New_Direct, Min_Line_3').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    comm_segment = direct_cons_comm_segment.groupBy('Product_Name').agg(\
                            sf.max('Minor_Line').alias('MINOR_LINE'),\
                            sf.max('Min_Line_3').alias('MIN_LINE_3'),\
                            sf.max('New_Direct').alias('DIRECT_CHANNEL'))
    
    comm_segment = comm_segment.withColumnRenamed('Product_Name', 'PRODUCT_NAME')
                    
    prem_reg_40 = prem_reg_24.join(comm_segment, 'PRODUCT_NAME', 'left')
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "channel_mapping_1"} 
    channel_mapper=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('Channel', 'Channel_New_2', 'Line')
    
    # input_indices_name = 'channel_mapping_1'
    # input_doc_type_name = input_indices_name+'/channel_mapping_1'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # channel_mapper = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes',es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','Channel, Channel_New_2, Line').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    channel_mapper_1 = channel_mapper.groupBy('Channel').agg(\
                            sf.max('Channel_New_2').alias('CHANNEL_NEW_2'), 
                            sf.max('Line').alias('LINE'))
    
    channel_mapper_1 = channel_mapper_1.withColumnRenamed('channel', 'CHANNEL')
    
    prem_reg_41 = prem_reg_40.join(channel_mapper_1, 'CHANNEL', 'left')
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "mappers",
        "dbtable": "zone_new_commercial_mapper_new"} 
    zone_new_mapper=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('Location_Type_Commerical', 'City')
    
    #     "dbschema": "public",
    #     "dbtable": "zone_new_commercial_mapper"} 
    # input_indices_name = 'zone_new_commercial_mapper'
    # input_doc_type_name = input_indices_name+'/zone_new_commercial_mapper'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # zone_new_mapper = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes',es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','Location_Type_Commerical, City').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    zone_new_mapper = zone_new_mapper.withColumn('City', upper(trim(col('City'))))
    zone_new_mapper_1 = zone_new_mapper.groupBy('City').agg(sf.max('Location_Type_Commerical').alias('ZONE_NEW_COMMERCIAL'))
    
    zone_new_mapper_1 = zone_new_mapper_1.withColumn('BRANCH', upper(trim(zone_new_mapper_1.City))).drop('City')
    
    prem_reg_42 = prem_reg_41.join(zone_new_mapper_1, 'BRANCH', 'left')
    
    prem_reg_43 = prem_reg_42.withColumn('LINE_TEMP', when(prem_reg_42.ZONE_NEW_COMMERCIAL == "HOM", lit("CONSUMER LINES"))\
                                                     .when((prem_reg_42.ZONE_NEW_COMMERCIAL == "HOM")&\
                                                           (prem_reg_42.CHANNEL_NEW_2 == "BROKERS"), lit('COMMERCIAL LINES'))
                                                     .when(prem_reg_42.CHANNEL_NEW_2 == "DIRECT", \
                                                           trim(upper(prem_reg_42.DIRECT_CHANNEL))))
    
    prem_reg_44 = prem_reg_43.withColumn('LINE_TEMP', when(prem_reg_43.LINE_TEMP.isNull(), trim(upper(prem_reg_43.LINE)))\
                                         .otherwise(trim(upper(prem_reg_43.LINE_TEMP))))
    
    prem_reg_44 = prem_reg_44.drop("LINE").withColumnRenamed("LINE_TEMP","LINE")
    
    prem_reg_45 =prem_reg_44.withColumn('MAIN_LOB',when(upper(trim(prem_reg_44.MIN_LINE_3))=="GOVT-BUSINESS",lit("Govt-Business"))\
                                                     .otherwise(prem_reg_44.LINE))
    
    # Channel & LOB mapping for channels in (Agency, E-Franchisee and Life Source Distribution)
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_pr_tproduct_to_channel"} 
    channel_lob_agency_mpr=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('Product_cd', 'Channel', 'main_lob')
    
    # input_indices_name = 'reference_pr_tproduct_to_channel'
    # input_doc_type_name = input_indices_name+'/reference_pr_tproduct_to_channel'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # channel_lob_agency_mpr = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes',es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','Product_cd, Channel, main_lob').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    channel_lob_agency_mpr_1 = channel_lob_agency_mpr.groupBy('Product_cd')\
                                                     .agg(sf.max('Channel').alias('CHANNEL'),\
                                                          sf.max('main_lob').alias('MAIN_LOB'))
    
    channel_lob_agency_mpr_2 = channel_lob_agency_mpr_1.filter(upper(trim(channel_lob_agency_mpr_1.CHANNEL)).like('AGENCY%'))
    channel_lob_agency_mpr_3 = channel_lob_agency_mpr_2.withColumnRenamed('Product_cd', 'PRODUCT_CD')\
                                                       .withColumnRenamed('CHANNEL', 'CHANNEL_AGENCY')\
                                                       .withColumnRenamed('MAIN_LOB', 'MAIN_LOB_AGENCY')\
    
    # Sagar - 20210315 : below code changes/addition done to match channel of agencies on the basis of display_product_cd as well.	
    channel_lob_agency_mpr_3A = channel_lob_agency_mpr_2.withColumnRenamed('Product_cd', 'DISPLAY_PRODUCT_CD')\
                                                        .withColumnRenamed('CHANNEL', 'DPC_CHANNEL_AGENCY')\
                                                        .withColumnRenamed('MAIN_LOB', 'DPC_MAIN_LOB_AGENCY')\
    
    prem_reg_45A1 = prem_reg_45.join(channel_lob_agency_mpr_3, 'PRODUCT_CD', 'left')
    prem_reg_45A2 = prem_reg_45A1.join(channel_lob_agency_mpr_3A, 'DISPLAY_PRODUCT_CD', 'left')
    prem_reg_45A3 = prem_reg_45A2.withColumn('CHANNEL_AGENCY', \
                                                when(col('CHANNEL_AGENCY').isNull(), col('DPC_CHANNEL_AGENCY'))\
                                               .otherwise(col('CHANNEL_AGENCY')))\
        #							 .withColumn('MAIN_LOB_AGENCY', \
        #                                            when(col('MAIN_LOB_AGENCY').isNull(), col('DPC_MAIN_LOB_AGENCY'))\
        #											.otherwise(col('MAIN_LOB_AGENCY')))
    prem_reg_45A = prem_reg_45A3.drop('DPC_CHANNEL_AGENCY').drop('DPC_MAIN_LOB_AGENCY')
    
    prem_reg_45B = prem_reg_45A.withColumn('MAIN_LOB', when((upper(trim(prem_reg_45A.CHANNEL)).like('AGENCY%'))&\
                                                            (upper(trim(prem_reg_45A.MAIN_LOB_AGENCY)).isNotNull())
                                                            ,upper(trim(prem_reg_45A.MAIN_LOB_AGENCY)))\
                                                      .otherwise(prem_reg_45A.MAIN_LOB))
    
    prem_reg_45C = prem_reg_45B\
                               .withColumn('MAIN_LOB', when(((upper(trim(prem_reg_45B.CHANNEL)) == 'E-FRANCHISEE')|\
                                                             (upper(trim(prem_reg_45B.CHANNEL)) == 'LIFE SOURCE DISTRIBUTION'))&\
                                                             (upper(trim(prem_reg_45B.MAIN_LOB_AGENCY)).isNotNull())\
                                                            ,upper(trim(prem_reg_45B.MAIN_LOB_AGENCY)))\
                                                      .otherwise(prem_reg_45B.MAIN_LOB))\
    
    #CHANNEL FOUND
    prem_reg_45D = prem_reg_45C.withColumn('CHANNEL', when((upper(trim(prem_reg_45C.CHANNEL)).like('AGENCY%') &\
                                                           (upper(trim(prem_reg_45C.CHANNEL_AGENCY)).isNotNull()))\
                                                           ,upper(trim(prem_reg_45C.CHANNEL_AGENCY)))\
                                                      .otherwise(prem_reg_45C.CHANNEL))
    
    prem_reg_45E = prem_reg_45D\
                               .withColumn('CHANNEL',  when((((upper(trim(prem_reg_45D.CHANNEL)) == 'E-FRANCHISEE')|\
                                                             (upper(trim(prem_reg_45D.CHANNEL)) == 'LIFE SOURCE DISTRIBUTION')) & 
                                                             (upper(trim(prem_reg_45D.CHANNEL_AGENCY)).isNotNull()))\
                                                             ,upper(trim(prem_reg_45D.CHANNEL_AGENCY)))\
                                                      .otherwise(prem_reg_45D.CHANNEL))\
    
    
    prem_reg_45F = prem_reg_45E.withColumn('BRANCH_STATE_NAME', upper(trim(prem_reg_45E.BRANCH_STATE_NAME)))
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "mappers",
        "dbtable": "zone_new_commercial_mapper_new"} 
    zone_new_mapper=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('CITY','STATE')
    
    #     "dbschema": "public",
    #     "dbtable": "zone_new_commercial_mapper"} 
    # zone_new_mapper=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('City','Zone_1','State')
    
    # input_indices_name = 'zone_new_commercial_mapper'
    # input_doc_type_name = input_indices_name+'/zone_new_commercial_mapper'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # zone_new_mapper = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes',es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','City,Zone_1,State').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    # zone_new_mapper_1 = zone_new_mapper.withColumn('BRANCH', upper(trim(zone_new_mapper.City)))\
    #                                      .withColumn('BIU_BRANCH_STATE_NAME', upper(trim(zone_new_mapper.State)))\
    #                                      .withColumn('ZONE', upper(trim(zone_new_mapper.Zone_1)))
    
    zone_new_mapper_1 = zone_new_mapper.withColumn('BRANCH', upper(trim(zone_new_mapper.CITY)))\
                                       .withColumn('BIU_BRANCH_STATE_NAME', upper(trim(zone_new_mapper.STATE)))
    
    zone_new_mapper_2 = zone_new_mapper_1.groupBy('BRANCH').agg(sf.max('BIU_BRANCH_STATE_NAME').alias('BIU_BRANCH_STATE_NAME'))
    # sf.max('ZONE').alias('ZONE'), \
                                                            
    # Removed Zone from here as it is already coming from previous mapper: Change suggested by Ankita, Sep 18, 2020.
    prem_reg_45F1 = prem_reg_45F.join(zone_new_mapper_2, 'BRANCH', 'left')
    
    # BRANCH_STATE_CODE
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_pr_statecodemaster"} 
    statecodemaster_mpr=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('State_UT','State_Code')
    
    # input_indices_name = 'reference_pr_statecodemaster'
    # input_doc_type_name = input_indices_name+'/reference_pr_statecodemaster'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # statecodemaster_mpr = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes',es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','State_UT,State_Code').\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    statecodemaster_mpr_1 = statecodemaster_mpr.withColumn('BIU_BRANCH_STATE_NAME', upper(trim(statecodemaster_mpr.State_UT)))\
                                               .withColumn('BIU_BRANCH_STATE_CODE', upper(trim(statecodemaster_mpr.State_Code)))
        
    statecodemaster_mpr_2 = statecodemaster_mpr_1.groupBy('BIU_BRANCH_STATE_NAME')\
                                                 .agg(sf.max('BIU_BRANCH_STATE_CODE').alias('BIU_BRANCH_STATE_CODE'))
        
    prem_reg_45G = prem_reg_45F1.join(statecodemaster_mpr_2, 'BIU_BRANCH_STATE_NAME', 'left')
    
    # prem_reg_45G.select('CGST_RATE', 'SGST_RATE', 'IGST_RATE').printSchema()
    # prem_reg_45G.printSchema()
    
    # fetched GL code of premium for SAP data
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "dbtable": "reference_pr_gc_gl_cd_list"} 
    df_gl_code=sqlContext.read.format("jdbc").options(**gscPythonOptions).load().select('TXT_LEDGER_ACCOUNT_CD', 'DESCRIPTION')
    
    # input_indices_name = 'reference_pr_gc_gl_cd_list'
    # input_doc_type_name = input_indices_name+'/pr_gc_gl_cd_list'
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # df_gl_code = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    df_gl_code = df_gl_code.withColumn('TXT_LEDGER_ACCOUNT_CD', \
                                                 df_gl_code.TXT_LEDGER_ACCOUNT_CD.cast(DecimalType()).cast(StringType()))
    
    # bifurcate GL codes in dataframe for respective columns 
    df_gl_code_prem = df_gl_code.filter(df_gl_code.DESCRIPTION == 'PREMIUM_AMOUNT_INR_WITHOUT_TAX')
    
    # create list from dataframe
    list_gl_code_prem = df_gl_code_prem[['TXT_LEDGER_ACCOUNT_CD']].rdd.collect()
    
    # load above list in string variable
    
    str_gl_code_prem = ''
    
    # str_gl_code_prem
    i = 0
    for index in range(len(list_gl_code_prem)):
        if (i==0):
            str_gl_code_prem = list_gl_code_prem[i].TXT_LEDGER_ACCOUNT_CD
        else:
            str_gl_code_prem = str_gl_code_prem+','+list_gl_code_prem[i].TXT_LEDGER_ACCOUNT_CD
        i = i+1
    # print str_gl_code_prem
    
    # #################################
    # esq11 = """{  "query": {"bool": {"must": {"terms": {"NUM_REFERENCE_NO": [201911130070313]}}}}}"""
    # # esq21 = """{"query": {"bool": {"must": {"terms": {"TXT_GL_CODE": [6010001110,6010002110,6010004110,6010003110,6010005110,6010008110,6010001130]}}
    # #                          ,
    # # filter": [{"range": {"DAT_VOUCHER_DATE": {"lte": \""""+end_date+"""\", "gte":\""""+start_date+"""\"}}}]}}}"""
    # 
    # esq21 = """{"query": {"bool": {"must": {"terms": {"TXT_GL_CODE": ["""+str_gl_code_prem+"""]
    # }},"filter": {"range": {"DAT_VOUCHER_DATE": {"lte": \""""+end_date+"""\", "gte":\""""+start_date+"""\"}}}}}}"""
    # 
    # # ,{"terms":{"NUM_REFERENCE_NO": [201911130070313]}}
    # input_indices_name = 'receipt_gc_accext_gl_sap_data_arch_'+start_YYMM
    # 
    # # uncommented below line on 20200204
    
    list_sap = list(str_gl_code_prem.split(','))
    
    gscPythonOptions = {
        "url": gpdb_url,
        "user": gpdb_user,
        "password": gpdb_pass,
        "dbschema": "public",
        "server.port":"1160-1180",
        "dbtable": "receipt_gc_accext_gl_sap_data_arch",
        "partitionColumn":"row_num",
        "partitions":10}
    
    #     "server.port":"1160-1180" ,
    prem_reg_sap_source=sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('NUM_REFERENCE_NO', 'NUM_TRANSACTION_CONTROL_NO', 'TXT_DIMENSION_1_VALUE_CD', 'TXT_DIMENSION_7_VALUE_CD',\
    'TXT_DOCUMENT_SEQ_NO', 'DAT_POSTING_DATE', 'TXT_GL_CODE')\
    .filter(col("DAT_VOUCHER_DATE").between(to_timestamp(lit(start_date), format='yyyy-MM-dd'),to_timestamp(lit(end_date), format='yyyy-MM-dd')))\
    .filter(col("TXT_GL_CODE").isin(list_sap))
    # .filter(col('NUM_REFERENCE_NO')==lit('202001170097684'))
    
    # if int(end_date[8:10])< 8:
    #     input_indices_name = 'receipt_gc_accext_gl_sap_data_arch_alias'
    # 
    # input_doc_type_name = input_indices_name+'/accext_gl_sap_data_arch'
    # 
    # document_count =documentcount(input_doc_type_name)
    # print "record count - "+input_indices_name+" : "+str(document_count)
    # 
    # # below code is commented for testing
    # #prem_reg_sap_source = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # #option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # #option('es.read.field.include','NUM_REFERENCE_NO, NUM_TRANSACTION_CONTROL_NO, TXT_DIMENSION_1_VALUE_CD, TXT_DIMENSION_7_VALUE_CD, TXT_DOCUMENT_SEQ_NO,\
    # #                                DAT_POSTING_DATE, TXT_GL_CODE').\
    # #option('es.query', esq2).\
    # #option('es.input.max.docs.per.partition',document_count).load()
    # 
    # # do not use below code for normal run. it is for testing
    # prem_reg_sap_source = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
    # option('es.port',es_port).option('es.resource',input_doc_type_name).\
    # option('es.read.field.include','NUM_REFERENCE_NO, NUM_TRANSACTION_CONTROL_NO, TXT_DIMENSION_1_VALUE_CD, TXT_DIMENSION_7_VALUE_CD, TXT_DOCUMENT_SEQ_NO,\
    #                                 DAT_POSTING_DATE, TXT_GL_CODE').\
    # option('es.query', esq21).\
    # option('es.input.max.docs.per.partition',document_count).load()
    
    # optimization
    prem_reg_sap = prem_reg_sap_source.join(prem_reg_nm_rfrnc_no_distnct, 'NUM_REFERENCE_NO')
    
    prem_reg_sap_1 = prem_reg_sap.withColumn('DISPLAY_PRODUCT_CD_RAW', \
                          when(prem_reg_sap.TXT_DIMENSION_1_VALUE_CD.isNull(), prem_reg_sap.TXT_DIMENSION_7_VALUE_CD)\
    					  .otherwise(prem_reg_sap.TXT_DIMENSION_1_VALUE_CD))
    					  
    prem_reg_sap_2 = prem_reg_sap_1.withColumn('DISPLAY_PRODUCT_CD', substring(col("DISPLAY_PRODUCT_CD_RAW"), 1, 6))\
                                   .drop('DISPLAY_PRODUCT_CD_RAW')\
    							   .drop('TXT_DIMENSION_1_VALUE_CD')\
    							   .drop('TXT_DIMENSION_7_VALUE_CD')\
    								  
    								  
    prem_reg_sap_2.createOrReplaceTempView('sap_data')
    
    
    sap_sql = """select * from (select NUM_REFERENCE_NO, NUM_TRANSACTION_CONTROL_NO, DISPLAY_PRODUCT_CD, TXT_DOCUMENT_SEQ_NO, 
    row_number() over (partition by NUM_REFERENCE_NO, NUM_TRANSACTION_CONTROL_NO, DISPLAY_PRODUCT_CD, TXT_DOCUMENT_SEQ_NO 
    order by TXT_DOCUMENT_SEQ_NO) as ranking
    from sap_data 
    where
    --where TXT_GL_CODE in ("""+str_gl_code_prem+""")
    --where (TXT_GL_CODE='6010001110' or TXT_GL_CODE='6010001130'
    --     or TXT_GL_CODE='6010002110' or TXT_GL_CODE = '6010004110' 
    --     or TXT_GL_CODE='6010003110' or TXT_GL_CODE='6010005110' 
    --     or TXT_GL_CODE='6010008110')
    --and 
    dat_posting_date >= '"""+start_date+"""' and dat_posting_date <= '"""+end_date+"""'
    --and TXT_DOCUMENT_SEQ_NO = 'XAA7905260' 
    --and NUM_REFERENCE_NO = '201812300152464'
    ) where ranking = 1"""
    
    sap_df = sqlContext.sql(sap_sql)
    sap_df.createOrReplaceTempView('sap_data_unique')
    #df.withColumn("colD", collect_list("colC").over(Window.partitionBy("colA").orderBy("colB"))).show(false)
     #
    sap_sql_2 = """select cast(cast(NUM_REFERENCE_NO as decimal(15,0)) as string) as NUM_REFERENCE_NO, cast(cast(NUM_TRANSACTION_CONTROL_NO as decimal(20,0)) as string) as NUM_TRANSACTION_CONTROL_NO, DISPLAY_PRODUCT_CD, 
    collect_list(TXT_DOCUMENT_SEQ_NO) as SAP_DOC_NO
    from sap_data_unique
    group by NUM_REFERENCE_NO, NUM_TRANSACTION_CONTROL_NO, DISPLAY_PRODUCT_CD"""
    sap_df_2 = sqlContext.sql(sap_sql_2)
    sap_df_3 = sap_df_2.withColumn('SAP_DOC_NO',concat_ws(',', 'SAP_DOC_NO'))
    
    # sap_df_2.createOrReplaceTempView('sap_data_final')
    
    prem_reg_45H = prem_reg_45G.join(sap_df_3, ['NUM_REFERENCE_NO', 'NUM_TRANSACTION_CONTROL_NO', 'DISPLAY_PRODUCT_CD'], "left")

    cnfgtr_2A = cnfgtr_1.groupBy('NUM_REFERENCE_NO')\
                       .agg(sf.max('INFORMATION6').alias('POL_INCEPT_DATE_5602'),\
                            sf.max('INFORMATION7').alias('POL_EXP_DATE_5602'))
    
    prem_reg_45I = prem_reg_45H.join(cnfgtr_2A, 'NUM_REFERENCE_NO', 'left')
    
    prem_reg_45J = prem_reg_45I.withColumn('POL_INCEPT_DATE', \
                                           when((col('PRODUCT_CD') == 5602)&\
                                                (col('RECORD_TYPE_DESC') == 'ENDORSEMENT'), \
                                                to_date(from_unixtime(unix_timestamp(col('POL_INCEPT_DATE_5602'), 'dd/MM/yyyy'))))\
                                           .otherwise(prem_reg_45I.POL_INCEPT_DATE))\
                               .withColumn('POL_EXP_DATE', \
                                           when((col('PRODUCT_CD') == 5602)&\
                                                (col('RECORD_TYPE_DESC') == 'ENDORSEMENT'), \
                                                to_date(from_unixtime(unix_timestamp(col('POL_EXP_DATE_5602'), 'dd/MM/yyyy'))))\
                                           .otherwise(col('POL_EXP_DATE')))\
        
    prem_reg_45K = prem_reg_45J.drop('POL_INCEPT_DATE_5602').drop('POL_EXP_DATE_5602')
        
    prem_reg_36 = prem_reg_45K
    # .join(risk_hdr_mot_4, 'NUM_REFERENCE_NO', 'left')
    
    prem_reg_36A = prem_reg_36\
                        .drop('NUM_ENDORSEMENT_SERVICE_TAX')\
                        .drop('NUM_SERVICE_TAX')\
                        .withColumn('LOAD_DATE',lit(current_date))\
                        .withColumn('REPORT_DATE',lit(report_date))\
                        .withColumnRenamed('SERVICE_TAX','SERVICE_TAX_AMOUNT')\
                        .withColumn('SERVICE_TAX_PERCENTAGE', lit(0))\
                        .withColumn('PRODUCT_CD', prem_reg_36.PRODUCT_CD.cast(DecimalType(10,0)).cast(StringType()))\
                        .withColumn('SOURCE_SYSTEM', lit('GC'))\
           
    
    # updated report accounting period for dpc 001506 where accounting period is before Dec 2020 set report accounting period to Dec 2020
    # where accounting period is greater than Nov 2020 set report accounting period to accounting period
    prem_reg_36B = prem_reg_36A.withColumn('REPORT_ACCOUNTING_PERIOD', \
                                             when((((prem_reg_36A.DISPLAY_PRODUCT_CD == '001504')|\
                                                    (prem_reg_36A.DISPLAY_PRODUCT_CD == '164002'))&\
                                                    (prem_reg_36A.ACCT_PERIOD_DATE >= '2019-09-01')), prem_reg_36A.ACCT_PERIOD_DATE)\
                                          .when(((prem_reg_36A.DISPLAY_PRODUCT_CD== '001506')&\
                                                    (prem_reg_36A.ACCT_PERIOD_DATE >= '2020-11-01')), prem_reg_36A.ACCT_PERIOD_DATE)\
                                          .when(((prem_reg_36A.DISPLAY_PRODUCT_CD== '001506')&\
                                                    (prem_reg_36A.ACCT_PERIOD_DATE <= '2020-11-30')), '2020-12-31')\
                                             .otherwise(prem_reg_36A.REPORT_ACCOUNTING_PERIOD))
                                                                    
    gscPythonOptions = {"url": gpdb_url,"user": gpdb_user,"password": gpdb_pass,
        "dbschema": "public", "server.port":"1109", "dbtable": "policy_dh_risk_details_hlt",
        "partitionColumn":"reference_num", "partitions":10}
    
    #     "server.port":"1105" ,
    dh_risk_hdr = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
    .select('REFERENCE_NUM', 'PORTABILITY_REQUIRED')
    
    dh_risk_hdr_1 = dh_risk_hdr.groupBy('REFERENCE_NUM')\
                               .agg(sf.max('PORTABILITY_REQUIRED').alias('PORTABILITY_REQUIRED'))
    
    dh_risk_hdr_2 = dh_risk_hdr_1.withColumnRenamed('REFERENCE_NUM', 'NUM_REFERENCE_NO')
    
    prem_reg_36C = prem_reg_36B.join(dh_risk_hdr_2, 'NUM_REFERENCE_NO' , 'left')
    
    prem_reg_36D = prem_reg_36C.withColumn('RECORD_TYPE_DESC_BIU', \
                                               when((prem_reg_36C.MAJOR_LINE_CD == '01')&\
    										        (prem_reg_36C.RENL_CERT_NO.cast(IntegerType()) != 0), 'RENEWAL BUSINESS')
    										  .when((prem_reg_36C.MAJOR_LINE_CD == '01')&\
    										        (prem_reg_36C.RENL_CERT_NO.cast(IntegerType()) == 0)&\
    										        (prem_reg_36C.NEW_PURCHASE_FG == 'Y'), 'NEW BUSINESS')
    										  .when((prem_reg_36C.MAJOR_LINE_CD == '01')&\
    										        (prem_reg_36C.RENL_CERT_NO.cast(IntegerType()) == 0)&\
    										        (prem_reg_36C.NEW_PURCHASE_FG =='N' ), 'ROLLOVER')\
                                              .otherwise(lit('')))
    
    prem_reg_36E = prem_reg_36D.withColumn('RECORD_TYPE_DESC_BIU', \
                                               when((prem_reg_36D.MAJOR_LINE_CD == '02')&\
    										        (prem_reg_36D.RECORD_TYPE_DESC == 'RENEWAL BUSINESS')&\
    												(prem_reg_36D.PRODUCT_CD.cast(IntegerType()) == 2889), 'NEW BUSINESS')
                                              .when((prem_reg_36D.MAJOR_LINE_CD == '02')&\
    										        (prem_reg_36D.RECORD_TYPE_DESC == 'RENEWAL BUSINESS')&\
    												(prem_reg_36D.PRODUCT_CD.cast(IntegerType()) != 2889), 'RENEWAL BUSINESS')
    										  .when((prem_reg_36D.MAJOR_LINE_CD == '02')&\
    										        (prem_reg_36D.RENL_CERT_NO.cast(IntegerType()) != 0)&\
    										        (prem_reg_36D.RECORD_TYPE_DESC == 'RENEWAL BUSINESS'), 'RENEWAL BUSINESS')
    										  .when((prem_reg_36D.MAJOR_LINE_CD == '02')&\
    										        (prem_reg_36D.RENL_CERT_NO.cast(IntegerType()) == 0)&\
    										        (prem_reg_36D.RECORD_TYPE_DESC == 'NEW BUSINESS')&\
    												(prem_reg_36D.PORTABILITY_REQUIRED == 'False'), 'NEW BUSINESS')
    										  .when((prem_reg_36D.MAJOR_LINE_CD == '02')&\
    										        (prem_reg_36D.RENL_CERT_NO.cast(IntegerType()) == 0)&\
    										        (prem_reg_36D.RECORD_TYPE_DESC == 'NEW BUSINESS')&\
    												(prem_reg_36D.PORTABILITY_REQUIRED == 'True'), 'PORTABILITY')
                                              .otherwise(prem_reg_36D.RECORD_TYPE_DESC_BIU))
    
    prem_reg_36F = prem_reg_36E.withColumn('RECORD_TYPE_DESC_BIU', \
                                              when(((prem_reg_36E.MAJOR_LINE_CD != '01')&\
                                                    (prem_reg_36E.MAJOR_LINE_CD != '02')&\
                                                    (prem_reg_36E.MAJOR_LINE_CD != '0'))&\
                                                    (prem_reg_36C.RENL_CERT_NO.cast(IntegerType()) == 0), 'NEW BUSINESS')
                                             .when(((prem_reg_36E.MAJOR_LINE_CD != '01')&\
                                                    (prem_reg_36E.MAJOR_LINE_CD != '02')&\
                                                    (prem_reg_36E.MAJOR_LINE_CD != '0'))&\
                                                    (prem_reg_36E.RENL_CERT_NO.cast(IntegerType()) != 0), 'RENEWAL BUSINESS')
                                             .otherwise(prem_reg_36E.RECORD_TYPE_DESC_BIU))\
                               .drop('PRODUCT_TYPE')
    
    # 20210218 - Sagar - New mapping/requirement received from Pankaj for Product_type. overwriting existing product_type
    
    # below is mapper file prepared by Pragati after discussion with Pankaj and Vaibhav on 17-02-2021.
    gscPythonOptions = {"url": gpdb_url,"user": gpdb_user,"password": gpdb_pass,
        "dbschema": "mappers", "dbtable": "product_type_final","server.port":"1109"}

    prdct_type_mapper = sqlContext.read.format("jdbc").options(**gscPythonOptions).load()\
    .select('PRODUCTCODE','PRODUCT_TYPE')\
    .filter(col('SOURCE_SYSTEM')==lit('GC'))
    
    prdct_type_1 = prdct_type_mapper.withColumn('PRODUCT_CD', col('PRODUCTCODE').cast(IntegerType()))
    prdct_type_2 = prdct_type_1.groupBy('PRODUCT_CD').agg(sf.max('PRODUCT_TYPE').alias('PRODUCT_TYPE'))
    
    prem_reg_37 = prem_reg_36F.join(prdct_type_2, 'PRODUCT_CD', 'left')    
    
    prem_reg_37A = prem_reg_37\
                            .withColumn('AGENT_CITY', trim(upper(prem_reg_37.AGENT_CITY)))\
                            .withColumn('AGENT_IT_PAN_NUM', trim(upper(prem_reg_37.AGENT_IT_PAN_NUM)))\
                            .withColumn('AGENT_LICENSE_CODE', trim(upper(prem_reg_37.AGENT_LICENSE_CODE)))\
                            .withColumn('AGENT_STATE', trim(upper(prem_reg_37.AGENT_STATE)))\
                            .withColumn('APPLICATION_NO', trim(upper(prem_reg_37.APPLICATION_NO)))\
                            .withColumn('BRANCH', trim(upper(prem_reg_37.BRANCH)))\
                            .withColumn('BRANCH_OFF_CD', trim(upper(prem_reg_37.BRANCH_OFF_CD)))\
                            .withColumn('BRANCH_STATE_CODE', trim(upper(prem_reg_37.BRANCH_STATE_CODE)))\
                            .withColumn('BRANCH_STATE_NAME', trim(upper(prem_reg_37.BRANCH_STATE_NAME)))\
                            .withColumn('CERTIFICATE_NO', trim(upper(prem_reg_37.CERTIFICATE_NO)))\
                            .withColumn('CHANNEL', trim(upper(prem_reg_37.CHANNEL)))\
                            .withColumn('CLASS_PERIL_CD', trim(upper(prem_reg_37.CLASS_PERIL_CD)))\
                            .withColumn('CLIENT_NAME', trim(upper(prem_reg_37.CLIENT_NAME)))\
                            .withColumn('COINSURANCE_CD', trim(upper(prem_reg_37.COINSURANCE_CD)))\
                            .withColumn('CUST_RES_CITY', trim(upper(prem_reg_37.CUST_RES_CITY)))\
                            .withColumn('CUST_RES_DIST', trim(upper(prem_reg_37.CUST_RES_DIST)))\
                            .withColumn('CUST_RES_STATE', trim(upper(prem_reg_37.CUST_RES_STATE)))\
                            .withColumn('CUST_TYPE', trim(upper(prem_reg_37.CUST_TYPE)))\
                            .withColumn('CUSTOMER_GSTIN', trim(upper(prem_reg_37.CUSTOMER_GSTIN)))\
                            .withColumn('CUSTOMER_ID', trim(upper(prem_reg_37.CUSTOMER_ID)))\
                            .withColumn('DEVEL_CODE', trim(upper(prem_reg_37.DEVEL_CODE)))\
                            .withColumn('DEVEL_DET', trim(upper(prem_reg_37.DEVEL_DET)))\
                            .withColumn('IIB_LOB', trim(upper(prem_reg_37.IIB_LOB)))\
                            .withColumn('IRDA_LOB', trim(upper(prem_reg_37.IRDA_LOB)))\
                            .withColumn('MAIN_LOB', trim(upper(prem_reg_37.MAIN_LOB)))\
                            .withColumn('MAP_FG', trim(upper(prem_reg_37.MAP_FG)))\
                            .withColumn('MOTOR_MANUFACTURER_NAME', trim(upper(prem_reg_37.MOTOR_MANUFACTURER_NAME)))\
                            .withColumn('MOTOR_MODEL_NAME', trim(upper(prem_reg_37.MOTOR_MODEL_NAME)))\
                            .withColumn('NEW_PURCHASE_FG', trim(upper(prem_reg_37.NEW_PURCHASE_FG)))\
                            .withColumn('PARTNER_APPL_NO', trim(upper(prem_reg_37.PARTNER_APPL_NO)))\
                            .withColumn('PIN', trim(upper(prem_reg_37.PIN)))\
                            .withColumn('POL_CO_INSURER', trim(upper(prem_reg_37.POL_CO_INSURER)))\
                            .withColumn('PRODUCER_NAME', trim(upper(prem_reg_37.PRODUCER_NAME)))\
                            .withColumn('PRODUCER_TYPE', trim(upper(prem_reg_37.PRODUCER_TYPE)))\
                            .withColumn('PRODUCT_CD', trim(upper(prem_reg_37.PRODUCT_CD)))\
                            .withColumn('PRODUCT_NAME', trim(upper(prem_reg_37.PRODUCT_NAME)))\
                            .withColumn('PRODUCT_TYPE', trim(upper(prem_reg_37.PRODUCT_TYPE)))\
                            .withColumn('RECEIPT_NO', trim(upper(prem_reg_37.RECEIPT_NO)))\
                            .withColumn('RECORD_TYPE_CD', trim(upper(prem_reg_37.RECORD_TYPE_CD)))\
                            .withColumn('RECORD_TYPE_DESC', trim(upper(prem_reg_37.RECORD_TYPE_DESC)))\
                            .withColumn('RI_INWARD_FLAG', trim(upper(prem_reg_37.RI_INWARD_FLAG)))\
                            .withColumn('SAP_DOC_NO', trim(upper(prem_reg_37.SAP_DOC_NO)))\
                            .withColumn('SERVICE_TAX_EXEMPATION_FLAG', trim(upper(prem_reg_37.SERVICE_TAX_EXEMPATION_FLAG)))\
                            .withColumn('TAGIC_GSTIN', trim(upper(prem_reg_37.TAGIC_GSTIN)))\
                            .withColumn('UIN_NO', trim(upper(prem_reg_37.UIN_NO)))\
                            .withColumn('WATTS_POLICY_NO', trim(upper(prem_reg_37.WATTS_POLICY_NO)))\
                            .withColumn('ZONE', trim(upper(prem_reg_37.ZONE)))\
                            .withColumn('COVERNOTENUMBER', trim(upper(prem_reg_37.COVERNOTENUMBER)))\
                            .withColumn('NO_OF_LIFE_COVERED', trim(upper(prem_reg_37.NO_OF_LIFE_COVERED)))\
                            .withColumn('NUM_REFERENCE_NO', trim(upper(prem_reg_37.NUM_REFERENCE_NO)))\
                            .withColumn('NUM_TRANSACTION_CONTROL_NO', trim(upper(prem_reg_37.NUM_TRANSACTION_CONTROL_NO)))\
                            .withColumn('ORG_CURR_COMMISSION', trim(upper(prem_reg_37.ORG_CURR_COMMISSION)))\
                            .withColumn('DISPLAY_PRODUCT_CD', trim(upper(prem_reg_37.DISPLAY_PRODUCT_CD)))
    
    # function for row_num
    s=-1
    def ranged_numbers(argument): 
        global s 
        if s >= 47:
            s = 0
        else:
            s = s+1 
        return s
    
    udf_ranged_numbers = udf(lambda x: ranged_numbers(x), IntegerType())
    
    prem_reg_37B = prem_reg_37A.withColumn('CGST_RATE', prem_reg_37A.CGST_RATE.cast(DoubleType()))\
    .withColumn('CRS_RCPT_DATE', to_date(prem_reg_37A.CRS_RCPT_DATE, format='yyyy-MM-dd'))\
    .withColumn('IGST_RATE', prem_reg_37A.IGST_RATE.cast(DoubleType()))\
    .withColumn('LOAD_DATE', to_date(prem_reg_37A.LOAD_DATE, format='yyyy-MM-dd'))\
    .withColumn('NUM_REFERENCE_NO', prem_reg_37A.NUM_REFERENCE_NO.cast(LongType()))\
    .withColumn('OUR_SHARE_POLICY_PERCENTAGE', prem_reg_37A.OUR_SHARE_POLICY_PERCENTAGE.cast(DoubleType()))\
    .withColumn('POL_CO_INSURER_PERCENTAGE', prem_reg_37A.POL_CO_INSURER_PERCENTAGE.cast(DoubleType()))\
    .withColumn('POLICY_COUNTER', prem_reg_37A.POLICY_COUNTER.cast(DoubleType()))\
    .withColumn('REPORT_ACCOUNTING_PERIOD', to_date(prem_reg_37A.REPORT_ACCOUNTING_PERIOD, format='yyyy-MM-dd'))\
    .withColumn('REPORT_DATE', to_date(prem_reg_37A.REPORT_DATE, format='yyyy-MM-dd'))\
    .withColumn('SERVICE_TAX_PERCENTAGE', prem_reg_37A.SERVICE_TAX_PERCENTAGE.cast(DoubleType()))\
    .withColumn('SGST_RATE', prem_reg_37A.SGST_RATE.cast(DoubleType()))\
    .withColumn('SL_NO', prem_reg_37A.SL_NO.cast(IntegerType()))\
    .withColumn('UW_DISCOUNT_AMT', prem_reg_37A.UW_DISCOUNT_AMT.cast(DoubleType()))\
    .withColumn('UW_LOADING_AMT', prem_reg_37A.UW_LOADING_AMT.cast(DoubleType()))\
    .withColumn('POLICY_ISSUANCE_DATE', to_date(col('POLICY_ISSUANCE_DATE'), format='yyyy-MM-dd'))\
    .withColumn('PROPOSAL_DATE', to_date(col('PROPOSAL_DATE'), format='yyyy-MM-dd'))\
    .withColumn('INSTRUMENT_DATE', to_date(col('INSTRUMENT_DATE'), format='yyyy-MM-dd'))\
    .withColumn("row_num", lit(udf_ranged_numbers(lit(1))))\
    .withColumn("RURAL_NON_RURAL", lit(None))\
    .withColumn("VILLAGE_CODE", lit(None))\
    .withColumn("VILLAGE_NAME", lit(None))\
    .withColumn("CONFIDENCE", lit(None))
    
    prem_reg_37B.printSchema()
    
    prem_reg_final_df = prem_reg_37B
                                
    prem_reg_final_col = prem_reg_final_df[['ACCT_PERIOD_DATE','AGENT_CITY','AGENT_IT_PAN_NUM','AGENT_LICENSE_CODE','AGENT_STATE','APPLICATION_NO','BRANCH','BRANCH_OFF_CD','BRANCH_STATE_CODE','BRANCH_STATE_NAME','CEDED_AIG_COMBINED_COMM','CEDED_AIG_COMBINED_PREM','CEDED_SURPLUS_TREATY_COMM','CEDED_SURPLUS_TREATY_PREM','CEDED_TERRORISM_POOL_COMM','CEDED_TERRORISM_POOL_PREM','CEDED_VQST_COMM','CEDED_VQST_PREM','CERT_END_DATE','CERT_START_DATE','CERTIFICATE_NO','CGST_AMOUNT','CGST_RATE','CHANNEL','CLASS_PERIL_CD','CLIENT_NAME','COINSURANCE_CD','COMMISSION_INR','COMMISSIONABLE_PREMIUM','CRS_RCPT_DATE','CUST_RES_CITY','CUST_RES_DIST','CUST_RES_STATE','CUST_TYPE','CUSTOMER_GSTIN','CUSTOMER_ID','DEVEL_CODE','DEVEL_DET','EFF_DT_SEQ_NO','ENDORSEMENT_EFF_DATE','ENDORSEMENT_END_DATE','FAC_COMM','FAC_PREM','IGST_AMOUNT','IGST_RATE','IIB_LOB','IRDA_LOB','LOAD_DATE','MAIN_LOB','MAJOR_LINE_CD','MAP_FG','MINOR_LINE_CD','MOTOR_MANUFACTURER_NAME','MOTOR_MODEL_NAME','NEW_PURCHASE_FG','NPW','OBLIGATORY_COMM','OBLIGATORY_PREM','ORG_PREM_AMT_WITHOUT_TAX','ORIG_CURR_CD','PARTNER_APPL_NO','PIN','POL_CO_INSURER','POL_EXP_DATE','POL_INCEPT_DATE','POL_OFFICE_CD','POLICY_NO','PORTAL_FLAG','PRDR_BRANCH_SUB_CD','PREMIUM_AMOUNT_INR_WITHOUT_TAX','PREMIUM_WITH_TAX','PRODUCER_CD','PRODUCER_NAME','PRODUCER_TYPE','PRODUCT_CD','PRODUCT_NAME','PRODUCT_TYPE','RECEIPT_NO','RECORD_TYPE_CD','RECORD_TYPE_DESC','RECORD_TYPE_DESC_BIU','RENL_CERT_NO','REPORT_ACCOUNTING_PERIOD','RI_INWARD_FLAG','RURAL_FG','SAP_DOC_NO','SERVICE_TAX_AMOUNT','SERVICE_TAX_EXEMPATION_FLAG','SGST_AMOUNT','SGST_RATE','SL_NO','SUM_INSURED','TAGIC_GSTIN','TDS_ON_COMM','TOTAL_COMMISSION','TTY_COMM','TTY_PREM','UIN_NO','WATTS_POLICY_NO','XOL_PREM','ZONE','COMMISSIONPERCENT','COVER_NOTE_DATE','COVERNOTENUMBER','UW_DISCOUNT_AMT','UW_LOADING_AMT','NO_OF_LIFE_COVERED','NUM_REFERENCE_NO','NUM_TRANSACTION_CONTROL_NO','ORG_CURR_COMMISSION','OUR_SHARE_POLICY_PERCENTAGE','OUR_SHARE_SUMINSURED','POL_CO_INSURER_PERCENTAGE','POL_STAMP_DUTY_AMT','POLICY_COUNTER','PREM_EFF_DATE','SERVICE_TAX_PERCENTAGE','NCB_PERCENT','TIMESTAMP','DISPLAY_PRODUCT_CD','SOURCE_SYSTEM','REPORT_DATE','VEHICLE_REGISTRATION_NO','POLICY_TYPE','POLICY_ISSUANCE_DATE','PROPOSAL_DATE','INSTRUMENT_DATE','MODE_OF_PAYMENT','CUSTOMER_ADDRESS','RURAL_NON_RURAL','VILLAGE_CODE','VILLAGE_NAME','CONFIDENCE','row_num']]
    
    final_spark_data_new = prem_reg_final_col
        
    print("Deleting data from premium premium_register_testing with below query: ")
    print("""delete from registers.premium_register_testing where SOURCE_SYSTEM = 'GC' and TIMESTAMP BETWEEN '""" + start_date + """' and '""" + end_date + "' ")
    # print("""WITH deleted AS (delete from registers.premium_register_testing where SOURCE_SYSTEM = 'GC' and TIMESTAMP BETWEEN '""" + start_date + """' and '""" + end_date + """'   RETURNING *) SELECT count(*) as delete_cnt FROM deleted""")
    
    from pg import DB
    
    print("deleting reocrds")
    db = DB(dbname=gpdb_dbname, host=gpdb_server_uat, port=5432,user='gpspark', passwd='spark@456')
    #db_op = db.query("""WITH deleted AS (delete from registers.premium_register_testing where SOURCE_SYSTEM = 'GC' and TIMESTAMP BETWEEN '""" + start_date + """' and '""" + end_date + """'   RETURNING *) SELECT count(*) as delete_cnt FROM deleted""")
    #print("no of records deleted :",db)
    #record_cnt = 0
    #for row in db_op.namedresult():
    #    record_cnt = row.record_count
    #print("deleted record_cnt",record_cnt)
    db_op = db.query("""delete from registers.premium_register_testing where SOURCE_SYSTEM = 'GC' and TIMESTAMP BETWEEN '""" + start_date + """' and '""" + end_date + "'")
    
    try:
        final_spark_data_new.write.format("greenplum").option("dbtable","premium_register_testing").option('dbschema','registers')\
        .option("url",gpdb_url_uat).option("server.port","1160-1180")\
        .option("user",gpdb_user).option("password",gpdb_pass).mode('append').save()
    except Exception as e:
        x = e
        print("Spark code : second job aborted @")
        ts = time.gmtime()
        print(time.strftime("%Y-%m-%d %H:%M:%S", ts))
    else: 
        x = 200 # success
print("status",x)    
print("second job ended @")
ts = time.gmtime()
print(time.strftime("%Y-%m-%d %H:%M:%S", ts))
print(start_date[8:10])
