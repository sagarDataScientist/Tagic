# testing

# process level parameters
import sys

no_of_cpu = 8
max_cores = 16
executr_mem = '41g'

from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import asc,lit
#warnings.filterwarnings('error')
import pyspark
from datetime import datetime,timedelta
import time;
import pytz
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
import simplejson as json
import json, pprint, requests
es_nodes = '10.35.12.212'
es_port = '9200'
es_user = 'elastic'
es_pwd = 'bEiilauM3es'
mesos_ip = 'mesos://10.35.12.205:5050'
# spark.stop()
conf.setMaster(mesos_ip)
conf.set('spark.executor.cores', no_of_cpu)
#conf.set('spark.memory.fraction','.2')
conf.set('spark.executor.memory',executr_mem)
conf.set('spark.driver.memory','4g')
conf.set('spark.cores.max',max_cores)
conf.set('spark.sql.shuffle.partitions','300')
conf.set('spark.default.parallelism','23')
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
conf.set("spark.app.name", "Custpmer Summary")
#conf.set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec");
#conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true"); 
from pyspark.sql.functions import broadcast
from pyspark.sql.types import DateType, StringType, DecimalType
#conf.set('spark.sql.crossJoin.enabled', 'true')
conf.set('es.nodes',es_nodes)
conf.set('es.port',es_port)
# conf.set('es.nodes.wan.only','true')
conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
#conf.set('spark.es.net.http.auth.user','Spark')
#conf.set('spark.es.net.http.auth.pass','Jarkpet1Sap3')
#conf.set('spark.num.executors','5')
conf.set('spark.es.net.http.auth.user', es_user)
conf.set('spark.es.net.http.auth.pass', es_pwd)
conf.set('spark.ui.port', 4048)
conf.set('spark.es.mapping.date.rich','false')
spark = pyspark.SparkContext(conf=conf)
sqlContext = SQLContext(spark)
import json, pprint, requests
import pyspark.sql.functions as sf

def documentcount(index_name):
    headers = {'Content-Type': 'application/json'}
    query = {'query':{'match_all':{}}}
    URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+index_name+'/_count'
    r = requests.get(URL, headers=headers)
    document_count =r.json().get('count')
    if document_count==0:
        document_count=1
    return document_count

def createindex(index_name,number_of_shards,number_of_replicas):
####   Create a Index     #######
    headers = {'Content-Type': 'application/json'}
    index_config= {"settings": { "index.number_of_shards": number_of_shards , "index.number_of_replicas":number_of_replicas}}
    URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+index_name
    r = requests.put(URL, data=json.dumps(index_config), headers=headers)
    return r

def createmapping(index_name,doc_type,mappings):
### Create a doc-type Mappings    
    URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+index_name+'/_mappings/'+doc_type+'/'
    headers = {'Content-Type': 'application/json'}
    r = requests.put(URL, data=json.dumps(mappings), headers=headers)
    return r

def deleteindex(index_name):
###delete index if already exists########################################################
    headers = {'Content-Type': 'application/json'}
    query = {"query":{"match_all":{}}}
    URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+index_name
    r = requests.delete(URL, data=json.dumps(query), headers=headers)
    return r

IST = pytz.timezone('Asia/Kolkata') 
current_date = datetime.datetime.now(IST).strftime("%Y-%m-%d")
currentdate = datetime.datetime.now(IST).strftime("%Y%m%d")
yesterday = (datetime.datetime.now(IST) - datetime.timedelta(days=1)).strftime("%Y%m%d")
dayBeforeYesterday = (datetime.datetime.now(IST) - datetime.timedelta(days=2)).strftime("%Y%m%d")

pol_load_flg = -1
clm_load_flg = -1
crm_load_flg = -1


print (current_date)

ts = time.gmtime()

# derive important parameters
# derive policy expiry date ie. greater than 90 days from run date
today = datetime.datetime.now(IST).strftime("%Y-%m-%d")
# today = datetime.datetime.strptime('2021-03-26', '%Y-%m-%d')
ninty_day = datetime.timedelta(days=90)

trans_end_date = (datetime.datetime.now(IST) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
# trans_start_date = (datetime.datetime.now(IST) - datetime.timedelta(days=61)).strftime("%Y-%m-%d")
trans_start_date = (datetime.datetime.now(IST) - datetime.timedelta(days=121)).strftime("%Y-%m-%d")
# pol_exp_date_limit = (datetime.datetime.now(IST) - datetime.timedelta(days=150)).strftime("%Y-%m-%d")
pol_exp_date_limit = (datetime.datetime.now(IST) - datetime.timedelta(days=210)).strftime("%Y-%m-%d")

start_date = (datetime.datetime.now(IST) - datetime.timedelta(days=1600)).strftime("%Y-%m-%d")
end_date = trans_end_date

trans_end_date_str = str(trans_end_date)
trans_start_date_str = str(trans_start_date)
str_pol_exp_date_limit = str(pol_exp_date_limit)

print("trans_end_date_str : "+trans_end_date_str)
print("trans_start_date_str : "+trans_start_date_str)
print("str_pol_exp_date_limit : "+str_pol_exp_date_limit)
print("start_date : "+start_date)
print("end_date : "+end_date)

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "underwriting_gc_cnfgtr_d_all_transactions",
         "partitionColumn":"row_num","partitions":2,"server.port":"1107"} 

rccn_trans_src = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('TRANS_ID')\
.filter(col("TRANS_DATE").between(to_date(lit(trans_start_date_str), format='yyyy-MM-dd'), to_date(lit(trans_end_date_str), format='yyyy-MM-dd')))\
.filter(col("STATUS").isin(["RCCN", "NC", "RC", "EC"]))\
.filter(col("ACTIVE_FLAG")==lit("A"))\
.filter(col("PROD_NAME").isin(["PrivateCarInsurancePolicy","MotorPrivateCarPolicyInsurance"]))
# .filter(col("PROD_NAME")==lit("PrivateCarInsurancePolicy"))

rccn_trans = rccn_trans_src.distinct()

rccn_trans_1 = rccn_trans.withColumnRenamed('TRANS_ID', 'NUM_REFERENCE_NO')
rccn_trans_2 = rccn_trans_1.withColumn('NUM_REFERENCE_NO', col('NUM_REFERENCE_NO').cast(DecimalType(15,0)).cast(StringType()))

# Below dataframe will be used to remove wip transaction data from premium register.
rccn_trans_2_tmp = rccn_trans_2.withColumn('FLAG', lit('Y'))

# input_indices_name = 'policy_gc_gen_prop_information_tab_alias'
# input_doc_type_name = input_indices_name+'/gen_prop_information_tab'
# document_count =documentcount(input_doc_type_name)

# gen_prop_information_tab_src = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
# option('es.port',es_port).option('es.resource',input_doc_type_name).\
# option('es.read.field.include','NUM_REFERENCE_NUMBER, TXT_POLICY_NO_CHAR').\
# option('es.query', esq_gen_prop).\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "policy_gc_gen_prop_information_tab",
         "partitionColumn":"row_num","partitions":9,"server.port":"1107"} 

gen_prop_information_tab_src = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REFERENCE_NUMBER','TXT_POLICY_NO_CHAR')\
.filter(col("DAT_POLICY_EFF_TODATE")>=to_date(lit(str_pol_exp_date_limit), format='yyyy-MM-dd'))\
.filter(col("NUM_PRODUCT_CODE").isin(["3121","3184"]))\

gen_prop = gen_prop_information_tab_src\
                    .withColumn('NUM_REFERENCE_NO',col('NUM_REFERENCE_NUMBER').cast(DecimalType(15,0)).cast(StringType()))\
                    .withColumnRenamed('TXT_POLICY_NO_CHAR', 'POLICY_NO')
    
gen_prop_1 = gen_prop.groupBy('NUM_REFERENCE_NO')\
                                       .agg(sf.max('POLICY_NO').alias('POLICY_NO'))

rccn_trans_3 = rccn_trans_2.join(gen_prop_1,'NUM_REFERENCE_NO', 'left')

# input_indices_name = 'policy_dh_risk_headers_mot_alias'
# input_doc_type_name = input_indices_name+'/risk_headers_mot'

# document_count =documentcount(input_doc_type_name)

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# print document_count

# #esq2 = """{  "query": {"bool": {"must": {"terms": {"policy_no": [  "018
# riskhdrmot_trans_src = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','REFERENCE_NUM,UW_DISCOUNT,UNDERWRITINGLOADINGDISC,DISCOUNT_PER,REFERENCE_DATE').\
# option('es.query', esq_risk_hdr_mot_trans).\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "policy_dh_risk_headers_mot",
         "partitionColumn":"row_num","partitions":10,"server.port":"1107"} 

riskhdrmot_trans_src = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('REFERENCE_NUM','UW_DISCOUNT','UNDERWRITINGLOADINGDISC','DISCOUNT_PER','REFERENCE_DATE')

riskhdrmot_trans_src_1 = riskhdrmot_trans_src.withColumn("ROW_NUM", sf.row_number()\
                               .over(Window.partitionBy('REFERENCE_NUM')\
                                     .orderBy(riskhdrmot_trans_src.REFERENCE_DATE.desc())))

riskhdrmot_trans = riskhdrmot_trans_src_1.filter(riskhdrmot_trans_src_1.ROW_NUM==1)

riskhdrmot_trans_1 = riskhdrmot_trans.withColumnRenamed("REFERENCE_NUM", "NUM_REFERENCE_NO")\
                             .withColumn('UW_DISC', when(riskhdrmot_trans.UW_DISCOUNT.isNull(), riskhdrmot_trans.DISCOUNT_PER)\
                                                    .otherwise(riskhdrmot_trans.UW_DISCOUNT))
    
riskhdrmot_trans_2 = riskhdrmot_trans_1.withColumn('CURR_YR_TARIFF_DISC', when(riskhdrmot_trans_1.UNDERWRITINGLOADINGDISC.isNull(),\
                                                               riskhdrmot_trans_1.UW_DISC)\
                                                    .otherwise(riskhdrmot_trans_1.UNDERWRITINGLOADINGDISC))
            
riskhdrmot_trans_3 = riskhdrmot_trans_2.groupBy('NUM_REFERENCE_NO')\
                                       .agg(sf.sum('CURR_YR_TARIFF_DISC').alias('CURR_YR_TARIFF_DISC'))


rccn_trans_4 = rccn_trans_3.join(riskhdrmot_trans_3, 'NUM_REFERENCE_NO', 'left')

rccn_trans_5 = rccn_trans_4.groupBy('POLICY_NO').agg(sf.max('CURR_YR_TARIFF_DISC').alias('CURR_YR_TARIFF_DISC'))

# policy data preparation

########################################################

# input_indices_name = 'prem_reg_final_alias'
# input_doc_type_name = input_indices_name+'/prem_reg_final'
# document_count =documentcount(input_doc_type_name)
# print document_count

# prem_reg_policy = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).\
# option('es.port',es_port).option('es.resource',input_doc_type_name).\
# option('es.read.field.include', 'NUM_REFERENCE_NO,POLICY_NO,POL_INCEPT_DATE,POL_EXP_DATE,PRODUCT_CD,PRODUCT_NAME,\
#                                  PREMIUM_WITH_TAX,CUSTOMER_ID').\
# option('es.query', esq_policy).\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "registers","dbtable": "premium_register",
         "partitionColumn":"row_num","partitions":2,"server.port":"1107"} 

prem_reg_policy = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_REFERENCE_NO','POLICY_NO','POL_INCEPT_DATE','POL_EXP_DATE','PRODUCT_CD','PRODUCT_NAME','PREMIUM_WITH_TAX','CUSTOMER_ID')\
.filter(col("POL_EXP_DATE")>=to_date(lit(str_pol_exp_date_limit), format='yyyy-MM-dd'))\
.filter(col("PRODUCT_CD")==lit('3121'))\
.filter(col("SOURCE_SYSTEM")==lit('GC'))

# to remove wip transaction data from premium register
prem_reg_policy_1 = prem_reg_policy.join(rccn_trans_2_tmp, 'NUM_REFERENCE_NO', 'left')
prem_reg_policy_2 = prem_reg_policy_1.filter(prem_reg_policy_1.FLAG.isNull())


prem_reg_policy_3 = prem_reg_policy_2.groupBy('NUM_REFERENCE_NO','POLICY_NO','POL_INCEPT_DATE','POL_EXP_DATE','CUSTOMER_ID','PRODUCT_CD')\
                                   .agg(sf.sum('PREMIUM_WITH_TAX').alias('LAST_YEAR_PREMIUM'))

# to fetch mobile no

# reading few columns from customer 
# input_indices_name = 'customer_gc_genmst_customer_alias'
# input_doc_type_name = input_indices_name+'/genmst_customer'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# customer = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','MOBILE,MOBILE2,CUSTOMER_CODE').\
# option('es.query', esq_cust).\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "customer_gc_genmst_customer",
         "partitionColumn":"row_num","partitions":10,"server.port":"1107"} 

customer = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('MOBILE','MOBILE2','CUSTOMER_CODE')

customer_1 = customer.withColumn('MOBILE_NO',when(customer.MOBILE.isNull(), customer.MOBILE2)\
                                             .otherwise(customer.MOBILE))\
                     .withColumnRenamed('CUSTOMER_CODE', 'CUSTOMER_ID')

customer_2 = customer_1.groupBy('CUSTOMER_ID').agg(sf.max('MOBILE_NO').alias('MOBILE_NO'))

customer_2 = customer_2.repartition('CUSTOMER_ID')

cust_smry_policy = prem_reg_policy_3.join(customer_2, 'CUSTOMER_ID', 'left')

# fetch lob

# input_indices_name = 'underwriting_gc_uw_product_master'
# input_doc_type_name = input_indices_name+'/uw_product_master'

# document_count =documentcount(input_doc_type_name)

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# print document_count

# #esq2 = """{  "query": {"bool": {"must": {"terms": {"policy_no": [  "018
# prod_master = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','PRODUCTCODE,DEPARTMENTCODE').\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "underwriting_gc_uw_product_master",
         "partitionColumn":"row_num","partitions":1,"server.port":"1107"} 

prod_master = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('PRODUCTCODE','DEPARTMENTCODE')

prod_master_1 = prod_master.withColumnRenamed("PRODUCTCODE", "PRODUCT_CD")
prod_master_2 = prod_master_1.groupBy('PRODUCT_CD').agg(sf.max('DEPARTMENTCODE').alias('DEPARTMENTCODE'))

# input_indices_name = 'reference_gc_uw_department_master'
# input_doc_type_name = input_indices_name+'/uw_department_master'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# print document_count

# #esq2 = """{  "query": {"bool": {"must": {"terms": {"policy_no": [  "018
# dept_master = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','DEPARTMENTCODE,DEPARTMENTNAME').\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "reference_gc_uw_department_master",
         "partitionColumn":"row_num","partitions":1,"server.port":"1107"} 

dept_master = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('DEPARTMENTNAME','DEPARTMENTCODE')

dept_master_1 = dept_master.withColumnRenamed("DEPARTMENTNAME", "LOB")
dept_master_2 = dept_master_1.groupBy('DEPARTMENTCODE').agg(sf.max('LOB').alias('LOB'))

lob_df = prod_master_2.join(dept_master_2, 'DEPARTMENTCODE', 'left')

lob_1 = lob_df.drop('DEPARTMENTCODE')
lob_2 = lob_1.groupBy('PRODUCT_CD').agg(sf.max('LOB').alias('LOB'))

cust_smry_policy_1 = cust_smry_policy.join(lob_2, 'PRODUCT_CD', 'left')

# input_indices_name = 'policy_dh_risk_headers_mot_alias'
# input_doc_type_name = input_indices_name+'/risk_headers_mot'

# document_count =documentcount(input_doc_type_name)

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# print document_count

# #esq2 = """{  "query": {"bool": {"must": {"terms": {"policy_no": [  "018
# riskhdrmot_pr_src = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','REFERENCE_NUM,UW_DISCOUNT,UNDERWRITINGLOADINGDISC,DISCOUNT_PER,REFERENCE_DATE').\
# option('es.query', esq_risk_hdr_mot_trans).\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "policy_dh_risk_headers_mot",
         "partitionColumn":"row_num","partitions":10,"server.port":"1107"} 

riskhdrmot_pr_src = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('REFERENCE_NUM','UW_DISCOUNT','UNDERWRITINGLOADINGDISC','DISCOUNT_PER','REFERENCE_DATE')

riskhdrmot_pr_src_1 = riskhdrmot_pr_src.withColumn("ROW_NUM", sf.row_number()\
                               .over(Window.partitionBy('REFERENCE_NUM')\
                                     .orderBy(riskhdrmot_pr_src.REFERENCE_DATE.desc())))

riskhdrmot_pr = riskhdrmot_pr_src_1.filter(riskhdrmot_pr_src_1.ROW_NUM==1)

riskhdrmot_pr_1 = riskhdrmot_pr.withColumnRenamed("REFERENCE_NUM", "NUM_REFERENCE_NO")\
                             .withColumn('UW_DISC', when(riskhdrmot_pr.UW_DISCOUNT.isNull(), riskhdrmot_pr.DISCOUNT_PER)\
                                                    .otherwise(riskhdrmot_pr.UW_DISCOUNT))
    
riskhdrmot_pr_2 = riskhdrmot_pr_1.withColumn('LAST_YR_TARIFF_DISC', when(riskhdrmot_pr_1.UNDERWRITINGLOADINGDISC.isNull(),\
                                                               riskhdrmot_pr_1.UW_DISC)\
                                                    .otherwise(riskhdrmot_pr_1.UNDERWRITINGLOADINGDISC))
            
riskhdrmot_pr_3 = riskhdrmot_pr_2.groupBy('NUM_REFERENCE_NO').agg(sf.max('LAST_YR_TARIFF_DISC').alias('LAST_YR_TARIFF_DISC'))

cust_smry_policy_2 = cust_smry_policy_1.join(riskhdrmot_pr_3, 'NUM_REFERENCE_NO', 'left')

cust_smry_policy_3 = cust_smry_policy_2.groupBy('POLICY_NO','CUSTOMER_ID','POL_INCEPT_DATE','POL_EXP_DATE','MOBILE_NO','LOB','PRODUCT_CD')\
                                       .agg(sf.sum('LAST_YEAR_PREMIUM').alias('LAST_YEAR_PREMIUM'),\
                                            sf.max('LAST_YR_TARIFF_DISC').alias('LAST_YR_TARIFF_DISC'))

rccn_trans_6 = rccn_trans_5.join(cust_smry_policy_3, 'POLICY_NO', 'left')

rccn_trans_6A = rccn_trans_6.withColumn('ROW_NUM', sf.row_number()\
                               .over(Window.partitionBy('POLICY_NO')\
                               .orderBy(rccn_trans_6.POL_EXP_DATE.desc())))

rccn_trans_6B = rccn_trans_6A.filter(rccn_trans_6A.ROW_NUM==1).drop('ROW_NUM')

rccn_trans_6C = rccn_trans_6B\
.withColumn('LOAD_DATE', lit(current_date))\
.withColumn('POL_INCEPT_DATE_str', rccn_trans_6B.POL_INCEPT_DATE.cast(StringType()))\
.withColumn('POL_EXP_DATE_str', rccn_trans_6B.POL_INCEPT_DATE.cast(StringType()))\

rccn_trans_6D = rccn_trans_6C.drop('POL_INCEPT_DATE').drop('POL_EXP_DATE')
rccn_trans_7 = rccn_trans_6D\
.withColumnRenamed('POL_INCEPT_DATE_str', 'POL_INCEPT_DATE')\
.withColumnRenamed('POL_EXP_DATE_str', 'POL_EXP_DATE')\

rccn_trans_7.printSchema()
rccn_trans_8 = rccn_trans_7[['POLICY_NO','POL_INCEPT_DATE','POL_EXP_DATE','CURR_YR_TARIFF_DISC','LAST_YR_TARIFF_DISC','LAST_YEAR_PREMIUM','PRODUCT_CD','LOB','MOBILE_NO','CUSTOMER_ID','LOAD_DATE']]

target_index = 'cust_summary_policy'+"_"+currentdate
# target_index = 'cust_summary_policy_test_1'+"_"+currentdate
target_doc_type = "cust_summary"

target_index_doc_type = target_index +'/'+ target_doc_type
print (target_index_doc_type)

output_index = target_index
doc_type = target_doc_type

# rccn_trans_6.filter(cust_smry_policy_3.POL_INCEPT_DATE.isNull()).count()

###delete index if already exists########################################################
headers = {'Content-Type': 'application/json'}
query = {"query":{"match_all":{}}}
URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index
r = requests.delete(URL, data=json.dumps(query), headers=headers)
time.sleep(180)

###create index ######################################################
headers = {'Content-Type': 'application/json'}
query_create= {"settings": { "index.number_of_shards": "1" , "index.number_of_replicas":"0"}}
URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index
r = requests.put(URL, data=json.dumps(query_create), headers=headers)
time.sleep(180)

### create mapping######################################################################################

mapping_output = {doc_type:{
'properties': {
'POLICY_NO': {'type': 'keyword'},
'POL_INCEPT_DATE': {'type': 'date','ignore_malformed': 'true','format': 'yyyy-MM-dd'},
'POL_EXP_DATE': {'type': 'date','ignore_malformed': 'true','format': 'yyyy-MM-dd'},
'CURR_YR_TARIFF_DISC': {'type': 'double'},
'LAST_YR_TARIFF_DISC': {'type': 'keyword'},
'LAST_YEAR_PREMIUM': {'type': 'keyword'},
'PRODUCT_CD': {'type': 'keyword'},
'LOB': {'type': 'keyword'},
'MOBILE_NO': {'type': 'keyword'},
'CUSTOMER_ID': {'type': 'keyword'},
'LOAD_DATE': {'type': 'date'}
}}}



URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index+'/_mapping/'+doc_type
headers = {'Content-Type': 'application/json'}
r = requests.put(URL, data=json.dumps(mapping_output), headers=headers)
time.sleep(120)

# rccn_trans_8.show(10)

final_spark_data_new = rccn_trans_8

try:
    final_spark_data_new.write.format('org.elasticsearch.spark.sql').mode('append').option('es.index.auto.create', 'true').\
    option('es.nodes' , es_nodes).option('es.port', es_port).option('es.resource',target_index_doc_type ).save() 
except Exception as e:
    x = e
    pol_load_flg = -1
else: 
    x = 200 # success
    pol_load_flg = 0
print (x)

input_indices_name = 'cust_summary_policy'+"_"+currentdate
# input_indices_name = 'cust_summary_policy_test_1'+"_"+currentdate
input_doc_type_name = input_indices_name+'/cust_summary'

document_count =documentcount(input_doc_type_name)

print ("record count - "+input_indices_name+" : "+str(document_count))

cust_policy = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
option('es.resource',input_doc_type_name).\
option('es.read.field.include','POLICY_NO,MOBILE_NO').\
option('es.input.max.docs.per.partition',document_count).load()

cust_policy_1 = cust_policy.groupBy('POLICY_NO').agg(sf.max('MOBILE_NO').alias('MOBILE_NO'))
cust_policy_2 = cust_policy_1.withColumnRenamed('POLICY_NO', 'TXT_POLICY_NO_CHAR')

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "claim_gc_gc_clm_gen_info",
         "partitionColumn":"row_num","partitions":2,"server.port":"1107"} 

clm_gen_info_src = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','TXT_MASTER_CLAIM_NO_NEW','DAT_NOTIFICATION_DATE','NUM_NATURE_OF_LOSS','TXT_POLICY_NO_CHAR')

clm_gen_info = cust_policy_2.join(clm_gen_info_src, 'TXT_POLICY_NO_CHAR')

distinct_ncn = clm_gen_info.select('NUM_CLAIM_NO').distinct()

clm_gen_info_1 = clm_gen_info.withColumn("ROW_NUM", sf.row_number()\
                               .over(Window.partitionBy('NUM_CLAIM_NO')\
                                     .orderBy(clm_gen_info.NUM_UPDATE_NO.desc())))

clm_gen_info_1A = clm_gen_info_1.groupBy('NUM_CLAIM_NO').agg(sf.max('TXT_MASTER_CLAIM_NO_NEW').alias('CLAIM_FEATURE_NO'))

clm_gen_info_1B = clm_gen_info_1.filter(clm_gen_info_1.ROW_NUM==1).drop('TXT_MASTER_CLAIM_NO_NEW')
clm_gen_info_2 = clm_gen_info_1B.join(clm_gen_info_1A, 'NUM_CLAIM_NO', 'left')
clm_gen_info_3 = clm_gen_info_2.withColumnRenamed('DAT_NOTIFICATION_DATE', 'CLAIM_REPORTED_DATE')\
                               .withColumnRenamed('TXT_POLICY_NO_CHAR', 'POLICY_NO')\
                               .withColumn('COVERAGE_CD', \
                                                  clm_gen_info_2.NUM_NATURE_OF_LOSS.cast(DecimalType(12,0)).cast(StringType()))\
                               .drop('ROW_NUM')\
                               .drop('NUM_UPDATE_NO')\
                               .drop('NUM_NATURE_OF_LOSS')

## acc_general_ledger (acc_general_ledger)

# reading few columns from gc_clm_gen_settlement_info

# input_indices_name = 'account_gc_acc_general_ledger_alias'
# input_doc_type_name = input_indices_name+'/acc_general_ledger'

# document_count =documentcount(input_doc_type_name)
# document_count = 2140520725
# print "record count - "+input_indices_name+" : "+str(document_count)

# gen_ledger_src = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','DAT_VOUCHER_DATE,NUM_REFERENCE_NO,NUM_CLAIM_NO,TXT_DR_CR,NUM_AMOUNT').\
# option('es.query', esq_gl).\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "account_gc_acc_general_ledger",
         "partitionColumn":"row_num","partitions":4,"server.port":"1107"} 

gen_ledger_src = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('DAT_VOUCHER_DATE','NUM_REFERENCE_NO','NUM_CLAIM_NO','TXT_DR_CR','NUM_AMOUNT')\
.filter(col("TXT_LEDGER_ACCOUNT_CD").isin(['7010001110','7010002110','7010003110','7010004110','7010005110','7010002130','7010002150','7010003130','7010004130','7010001130']))\
.filter(col("DAT_VOUCHER_DATE").between(to_date(lit(start_date), format='yyyy-MM-dd'), to_date(lit(end_date), format='yyyy-MM-dd')))

gen_ledger = distinct_ncn.join(gen_ledger_src, 'NUM_CLAIM_NO')

gen_ledger_1 = gen_ledger.withColumn('NUM_AMOUNT_SIGN', when(gen_ledger.TXT_DR_CR=='CR', gen_ledger.NUM_AMOUNT*-1)\
                                                                 .otherwise(gen_ledger.NUM_AMOUNT))\
                         .withColumn('NUM_REFERENCE_NO',col('NUM_REFERENCE_NO').cast(DecimalType(15,0)).cast(StringType()))\


gen_ledger_1A = gen_ledger_1.groupBy('NUM_REFERENCE_NO','NUM_CLAIM_NO')\
                           .agg(sf.sum('NUM_AMOUNT_SIGN').alias('CLAIM_AMOUNT'),\
                                sf.max('DAT_VOUCHER_DATE').alias('DAT_VOUCHER_DATE'))

gen_ledger_2 = gen_ledger_1A.repartition('NUM_REFERENCE_NO')

## covercodemaster (covercodemaster)
# input_indices_name = 'reference_gc_covercodemaster'
# input_doc_type_name = input_indices_name+'/covercodemaster'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# covercodemaster = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include', 'NUM_COVER_CODE,TXT_COVER_DESCRIPTION').\
# option('es.input.max.docs.per.partition',document_count).load()
        
gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "reference_gc_covercodemaster",
         "partitionColumn":"row_num","partitions":1,"server.port":"1107"} 

covercodemaster = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_COVER_CODE','TXT_COVER_DESCRIPTION')

covercodemaster_1 = covercodemaster.withColumn('COVERAGE_CD', \
                                               covercodemaster.NUM_COVER_CODE.cast(DecimalType(12,0)).cast(StringType()))\
                                   .withColumnRenamed('TXT_COVER_DESCRIPTION', 'COVERAGE_DESC')

covercodemaster_2 = covercodemaster_1.groupBy('COVERAGE_CD')\
                                     .agg(sf.max('COVERAGE_DESC').alias('COVERAGE_DESC'))

clm_gen_info_4 = clm_gen_info_3.join(covercodemaster_2, 'COVERAGE_CD', 'left')

# clm_gen_info_4.show(20, False)

###################################################
# reading few columns from gc_clm_gen_info_extra

# input_indices_name = 'claim_gc_gc_clm_gen_info_extra'
# input_doc_type_name = input_indices_name+'/gc_clm_gen_info_extra'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# clm_gen_info_extra_src = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','NUM_CLAIM_NO,NUM_UPDATE_NO,TXT_INFO4').\
# option('es.query', esq_clm).\
# option('es.input.max.docs.per.partition',document_count).load()
        
        
gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "claim_gc_gc_clm_gen_info_extra",
         "partitionColumn":"row_num","partitions":2,"server.port":"1107"} 

clm_gen_info_extra_src = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','TXT_INFO4')\

clm_gen_info_extra = distinct_ncn.join(clm_gen_info_extra_src, 'NUM_CLAIM_NO')

clm_gen_info_extra_1 = clm_gen_info_extra.withColumn("ROW_NUM", sf.row_number()\
                               .over(Window.partitionBy('NUM_CLAIM_NO')\
                                     .orderBy(clm_gen_info_extra.NUM_UPDATE_NO.desc())))

clm_gen_info_extra_2 = clm_gen_info_extra_1.filter(clm_gen_info_extra_1.ROW_NUM==1)
clm_gen_info_extra_3 = clm_gen_info_extra_2.drop('ROW_NUM')\
                                           .drop('NUM_UPDATE_NO')\

#################################################################################
# reading few columns from gc_clm_gen_settlement_info

# input_indices_name = 'claim_gc_gc_clm_gen_settlement_info'
# input_doc_type_name = input_indices_name+'/gc_clm_gen_settlement_info'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# clm_stlmnt_info_src = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','NUM_CLAIM_NO,NUM_UPDATE_NO,NUM_SERIAL_NO,NUM_SETTLEMENT_TYPE_CD,TXT_INFO2').\
# option('es.query', esq_clm_stlmnt_info).\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "claim_gc_gc_clm_gen_settlement_info",
         "partitionColumn":"row_num","partitions":2,"server.port":"1107"} 

clm_stlmnt_info_src = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_UPDATE_NO','NUM_SERIAL_NO','NUM_SETTLEMENT_TYPE_CD','TXT_INFO2')\
.filter(col("NUM_SETTLEMENT_TYPE_CD")==lit("1"))
        
clm_stlmnt_info = distinct_ncn.join(clm_stlmnt_info_src, 'NUM_CLAIM_NO')

clm_stlmnt_info_1 = clm_stlmnt_info.withColumn("ROW_NUM", sf.row_number()\
                               .over(Window.partitionBy('NUM_CLAIM_NO')\
                                     .orderBy(clm_stlmnt_info.NUM_UPDATE_NO.desc(),clm_stlmnt_info.NUM_SERIAL_NO.desc())))

clm_stlmnt_info_2 = clm_stlmnt_info_1.filter(clm_stlmnt_info_1.ROW_NUM==1)
clm_stlmnt_info_3 = clm_stlmnt_info_2.drop('ROW_NUM')\
                                     .drop('NUM_UPDATE_NO')\
                                     .drop('NUM_SERIAL_NO')\
                                     .withColumn('TYPE_OF_SETTLEMENT_CD_MOTOR', \
                                                 when(clm_stlmnt_info_2.NUM_SETTLEMENT_TYPE_CD=='1', \
                                                              clm_stlmnt_info_2.TXT_INFO2).otherwise(lit('')))\

clm_stlmnt_info_4 = clm_stlmnt_info_3.drop('NUM_SETTLEMENT_TYPE_CD')\
                                     .drop('TXT_INFO2')

# input_indices_name = 'reference_gc_gc_clmmst_generic_value'
# input_doc_type_name = input_indices_name+'/gc_clmmst_generic_value'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# gc_clmmst_generic_value = sqlContext.read.format('org.elasticsearch.spark.sql').\
# option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include', 'NUM_MASTER_CD,TXT_INFO1,TXT_INFO2,TXT_INFO3,TXT_INFO5').\
# option('es.input.max.docs.per.partition',document_count).load()       
        
gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "reference_gc_gc_clmmst_generic_value",
         "partitionColumn":"row_num","partitions":2,"server.port":"1107"} 

gc_clmmst_generic_value = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_MASTER_CD','TXT_INFO1','TXT_INFO2','TXT_INFO3','TXT_INFO5')
            
## join for TYPE_OF_SETTLEMENT_CD_MOTOR

gc_genvalue1 = gc_clmmst_generic_value\
                        .filter(gc_clmmst_generic_value.NUM_MASTER_CD == 1)

gc_genvalue1 = gc_genvalue1.withColumn('TYPE_OF_SETTLEMENT_CD_MOTOR', trim(gc_genvalue1.TXT_INFO1))

gc_genvalue2 = gc_genvalue1.groupBy('TYPE_OF_SETTLEMENT_CD_MOTOR')\
                           .agg(sf.max('TXT_INFO2').alias('TYPE_OF_SETTLEMENT_MOTOR'))

clm_stlmnt_info_5 = clm_stlmnt_info_4.join(gc_genvalue2, 'TYPE_OF_SETTLEMENT_CD_MOTOR', "left_outer")
clm_stlmnt_info_6 = clm_stlmnt_info_5.withColumnRenamed('TYPE_OF_SETTLEMENT_MOTOR','SETTLEMENT_TYPE')\
                                     .drop('TYPE_OF_SETTLEMENT_CD_MOTOR')\
                                

# input_indices_name = 'claim_gc_gc_clm_transaction_history'
# input_doc_type_name = input_indices_name+'/gc_clm_transaction_history'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# trans_hist_src = sqlContext.read.format('org.elasticsearch.spark.sql').\
# option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include', 'NUM_CLAIM_NO,NUM_ISACTIVE,DAT_TRANSACTIONDATE,NUM_PAYMENTNUMBER,TXT_TRANSACTIONTYPE,TXT_ACCOUNTLINE,NUM_SERIAL_NO,TXT_USERID').\
# option('es.query',esq_trans_hist).\
# option('es.input.max.docs.per.partition',document_count).load()  
        
gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "claim_gc_gc_clm_transaction_history",
         "partitionColumn":"row_num","partitions":2,"server.port":"1107"} 

trans_hist_src = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('NUM_CLAIM_NO','NUM_ISACTIVE','DAT_TRANSACTIONDATE','NUM_PAYMENTNUMBER','TXT_TRANSACTIONTYPE','TXT_ACCOUNTLINE','NUM_SERIAL_NO','TXT_USERID')\

trans_hist = distinct_ncn.join(trans_hist_src, 'NUM_CLAIM_NO')

trans_hist_1 = trans_hist.withColumn("ROW_NUM", sf.row_number()\
                               .over(Window.partitionBy('NUM_CLAIM_NO','TXT_ACCOUNTLINE')\
                                     .orderBy(trans_hist.DAT_TRANSACTIONDATE.desc(),trans_hist.NUM_PAYMENTNUMBER.desc(),trans_hist.NUM_SERIAL_NO.desc())))

# trans_hist_2 = trans_hist_1.filter((trans_hist_1.ROW_NUM==1)&\
#                                    ((trans_hist_1.TXT_TRANSACTIONTYPE=='Final Payment')|
#                                     (trans_hist_1.TXT_TRANSACTIONTYPE=='Payment After Closing'))

    
trans_hist_2 = trans_hist_1.filter(trans_hist_1.ROW_NUM==1)\
                           .drop('DAT_TRANSACTIONDATE')\
                           .drop('NUM_ISACTIVE')\
                           .drop('NUM_PAYMENTNUMBER')\
                           .drop('NUM_SERIAL_NO')\
                           .drop('ROW_NUM')\
                    
# trans_hist_indemnity = trans_hist_1.filter((trans_hist_1.ROW_NUM==1)&(trans_hist_1.TXT_ACCOUNTLINE=='INDEMNITY'))
# trans_hist_expense = trans_hist_1.filter((trans_hist_1.ROW_NUM==1)&(trans_hist_1.TXT_ACCOUNTLINE=='EXPENSE'))

trans_hist_3 = trans_hist_2\
                .withColumn('INDEMNITY_STATUS_AT_FEATURE_LEVEL',\
                            when((trans_hist_2.TXT_ACCOUNTLINE == 'INDEMNITY')&\
                                ((lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'advice')|\
                                 (lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'partial payment')|\
                                 (lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'decrease in reserve')|\
                                 (lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'reopen')|\
                                 (lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'payment voiding')|\
                                 (lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 're-open')|\
                                ((lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'increase in reserve')&\
                                 (lower(trim(trans_hist_2.TXT_USERID)) != 'system generated entry'))|\
                                 (lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'payment listing')), lit('OPEN'))\
                           .otherwise(lit('')))\
                .withColumn('EXPENSES_STATUS_AT_FEATURE_LEVEL',\
                            when((trans_hist_2.TXT_ACCOUNTLINE == 'EXPENSE')&\
                              (((lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'increase in reserve') &\
                                (lower(trim(trans_hist_2.TXT_USERID)) != 'system generated entry'))|\
                                (lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'advice')|\
                                (lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'partial payment')|\
                                (lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'decrease in reserve')|\
                                (lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'reopen')|\
                                (lower(trim(trans_hist_2.TXT_TRANSACTIONTYPE)) == 'payment listing')), lit('OPEN'))\
                           .otherwise(lit('')))

trans_hist_4 = trans_hist_3\
                .withColumn('INDEMNITY_STATUS_AT_FEATURE_LEVEL',\
                            when((trans_hist_3.TXT_ACCOUNTLINE == 'INDEMNITY')&\
                                ((lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'payment after closing')|\
                                ((lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'reserves released - final payment') &\
                                 (lower(trim(trans_hist_3.TXT_USERID)) == 'system generated entry'))|\
                                ((lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'increase in reserve')&\
                                 (lower(trim(trans_hist_3.TXT_USERID)) == 'system generated entry'))|\
                                 (lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'reserves released - markoff/cancel')|\
                                 (lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'final payment-reinstatement deduction')|\
                                 (lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'final payment')|\
                                 (lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'mark off')|\
                                 (lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'mark off reserve')|\
                                 (lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'markoff')), lit('CLOSED'))\
                                .otherwise(trans_hist_3.INDEMNITY_STATUS_AT_FEATURE_LEVEL))\
                .withColumn('EXPENSES_STATUS_AT_FEATURE_LEVEL',\
                            when((trans_hist_3.TXT_ACCOUNTLINE == 'EXPENSE')&\
                                 (((lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'reserves released - final payment') &\
                                   (lower(trim(trans_hist_3.TXT_USERID)) == 'system generated entry'))|\
                                  ((lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'increase in reserve') &\
                                   (lower(trim(trans_hist_3.TXT_USERID)) == 'system generated entry'))|\
                                   (lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'payment after closing')|\
                                   (lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'reserve released')|\
                                   (lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'final payment')|\
                                   (lower(trim(trans_hist_3.TXT_TRANSACTIONTYPE)) == 'mark off reserve')), lit('CLOSED'))\
                                .otherwise(trans_hist_3.EXPENSES_STATUS_AT_FEATURE_LEVEL))\
                .drop('TXT_USERID')

trans_hist_5 = trans_hist_4\
                .withColumn('INDEMNITY_STATUS_AT_FEATURE_LEVEL',\
                            when((trans_hist_4.TXT_ACCOUNTLINE == 'INDEMNITY')&\
                                  ((lower(trim(trans_hist_4.TXT_TRANSACTIONTYPE)) == 'subrogation')|\
                                   (lower(trim(trans_hist_4.TXT_TRANSACTIONTYPE)) == 'salvage')), lit(''))\
                             .otherwise(trans_hist_4.INDEMNITY_STATUS_AT_FEATURE_LEVEL))
    
trans_hist_5_ind = trans_hist_5.filter(trans_hist_5.TXT_ACCOUNTLINE == 'INDEMNITY')\
                               .drop('EXPENSES_STATUS_AT_FEATURE_LEVEL')\
                               .drop('TXT_ACCOUNTLINE')\
                               .drop('TXT_TRANSACTIONTYPE')
trans_hist_5_exp = trans_hist_5.filter(trans_hist_5.TXT_ACCOUNTLINE == 'EXPENSE')\
                               .drop('INDEMNITY_STATUS_AT_FEATURE_LEVEL')\
                               .drop('TXT_ACCOUNTLINE')\
                               .drop('TXT_TRANSACTIONTYPE')

trans_hist_6A = trans_hist_5_ind.join(trans_hist_5_exp, 'NUM_CLAIM_NO', 'left')
trans_hist_6 = trans_hist_6A.withColumn('INDEMNITY_STATUS_AT_FEATURE_LEVEL', \
                                       when(trans_hist_6A.INDEMNITY_STATUS_AT_FEATURE_LEVEL.isNull(), lit(''))\
                                       .otherwise(trans_hist_6A.INDEMNITY_STATUS_AT_FEATURE_LEVEL))\
                           .withColumn('EXPENSES_STATUS_AT_FEATURE_LEVEL', \
                                       when(trans_hist_6A.EXPENSES_STATUS_AT_FEATURE_LEVEL.isNull(), lit(''))\
                                       .otherwise(trans_hist_6A.EXPENSES_STATUS_AT_FEATURE_LEVEL))

trans_hist_7 = trans_hist_6.withColumn('CLAIM_STATUS_CD',\
                                  when((lower(trans_hist_6.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'open')&\
                                  (lower(trans_hist_6.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'open'), 'open')\
                                   .when((lower(trans_hist_6.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'closed')&\
                                  (lower(trans_hist_6.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'open'), 'open')\
                                  .when((lower(trans_hist_6.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'closed')&\
                                  (lower(trans_hist_6.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'closed'), 'closed')\
                                  .when((lower(trans_hist_6.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'open')&\
                                  (lower(trans_hist_6.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'closed'), 'open')
                                  .when((lower(trans_hist_6.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'open')&\
                                  (trim(trans_hist_6.EXPENSES_STATUS_AT_FEATURE_LEVEL)==''), 'open')
                                  .when((trim(trans_hist_6.INDEMNITY_STATUS_AT_FEATURE_LEVEL)=='')&\
                                  (lower(trans_hist_6.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'open'), 'open')
                                  .when((lower(trans_hist_6.INDEMNITY_STATUS_AT_FEATURE_LEVEL) == 'closed')&\
                                  (trim(trans_hist_6.EXPENSES_STATUS_AT_FEATURE_LEVEL)==''), 'closed')
                                  .when((trim(trans_hist_6.INDEMNITY_STATUS_AT_FEATURE_LEVEL)=='')&\
                                  (lower(trans_hist_6.EXPENSES_STATUS_AT_FEATURE_LEVEL) == 'closed'), 'closed'))

trans_hist_8 = trans_hist_7.withColumn('CLAIM_STATUS_CD', upper(trans_hist_7.CLAIM_STATUS_CD))\
                           .drop('INDEMNITY_STATUS_AT_FEATURE_LEVEL')\
                           .drop('EXPENSES_STATUS_AT_FEATURE_LEVEL')

# rccn_clm_policy = rccn_policy_distinct.join(clm_gen_info_4,'POLICY_NO')
claim_df_1 = clm_gen_info_4.join(gen_ledger_2, 'NUM_CLAIM_NO', 'left')
claim_df_2 = claim_df_1.join(clm_gen_info_extra_3, 'NUM_CLAIM_NO', 'left')
claim_df_3 = claim_df_2.join(clm_stlmnt_info_6, 'NUM_CLAIM_NO', 'left')
claim_df_4 = claim_df_3.join(trans_hist_8, 'NUM_CLAIM_NO', 'left')

claim_df_5 = claim_df_4.groupBy('CLAIM_FEATURE_NO')\
                       .agg(sf.max('DAT_VOUCHER_DATE').alias('CLAIM_PAID_DATE'),\
                            sf.max('COVERAGE_CD').alias('COVERAGE_CD'),\
                            sf.max('CLAIM_REPORTED_DATE').alias('CLAIM_REPORTED_DATE'),\
                            sf.max('POLICY_NO').alias('POLICY_NO'),\
                            sf.max('MOBILE_NO').alias('MOBILE_NO'),\
                            sf.max('COVERAGE_DESC').alias('COVERAGE_DESC'),\
                            sf.max('TXT_INFO4').alias('TXT_INFO4'),\
                            sf.max('SETTLEMENT_TYPE').alias('SETTLEMENT_TYPE'),\
                            sf.max('CLAIM_STATUS_CD').alias('CLAIM_STATUS_CD'),\
                            sf.sum('CLAIM_AMOUNT').alias('CLAIM_AMOUNT'))
    
claim_df_5A = claim_df_5\
.withColumn('LOAD_DATE',lit(current_date))\
.withColumn('CLAIM_REPORTED_DATE_str', claim_df_5.CLAIM_REPORTED_DATE.cast(StringType()))\
.withColumn('CLAIM_PAID_DATE_str', claim_df_5.CLAIM_PAID_DATE.cast(StringType()))\

claim_df_5B = claim_df_5A.drop('CLAIM_REPORTED_DATE').drop('CLAIM_PAID_DATE')

claim_df_6 = claim_df_5B\
.withColumnRenamed('CLAIM_REPORTED_DATE_str', 'CLAIM_REPORTED_DATE')\
.withColumnRenamed('CLAIM_PAID_DATE_str', 'CLAIM_PAID_DATE')

claim_df_7 = claim_df_6[['CLAIM_FEATURE_NO','POLICY_NO','MOBILE_NO','COVERAGE_CD','COVERAGE_DESC','TXT_INFO4','SETTLEMENT_TYPE','CLAIM_PAID_DATE','CLAIM_REPORTED_DATE','CLAIM_STATUS_CD','CLAIM_AMOUNT','LOAD_DATE']]

target_index = 'cust_summary_claim'+"_"+currentdate
# target_index = 'cust_summary_claim_test_1'+"_"+currentdate
target_doc_type = "cust_summary"

target_index_doc_type = target_index +'/'+ target_doc_type
print (target_index_doc_type)

output_index = target_index
doc_type = target_doc_type

###delete index if already exists########################################################
headers = {'Content-Type': 'application/json'}
query = {"query":{"match_all":{}}}
URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index
r = requests.delete(URL, data=json.dumps(query), headers=headers)
# time.sleep(180)

###create index ######################################################
headers = {'Content-Type': 'application/json'}
query_create= {"settings": { "index.number_of_shards": "1" , "index.number_of_replicas":"0"}}
URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index
r = requests.put(URL, data=json.dumps(query_create), headers=headers)
# time.sleep(180)

### create mapping######################################################################################

mapping_output = {doc_type:{
'properties': {
'CLAIM_FEATURE_NO': {'type': 'keyword'},
'POLICY_NO': {'type': 'keyword'},
'MOBILE_NO': {'type': 'keyword'},
'COVERAGE_CD': {'type': 'keyword'},
'COVERAGE_DESC': {'type': 'keyword'},
'TXT_INFO4': {'type': 'keyword'},
'SETTLEMENT_TYPE': {'type': 'keyword'},
'CLAIM_PAID_DATE': {'type': 'date','ignore_malformed': 'true','format': 'yyyy-MM-dd'},
'CLAIM_REPORTED_DATE': {'type': 'date','ignore_malformed': 'true','format': 'yyyy-MM-dd'},
'CLAIM_STATUS_CD': {'type': 'keyword'},
'CLAIM_AMOUNT': {'type': 'double'},
'LOAD_DATE': {'type': 'date','ignore_malformed': 'true','format': 'yyyy-MM-dd'}
}}}



URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index+'/_mapping/'+doc_type
headers = {'Content-Type': 'application/json'}
r = requests.put(URL, data=json.dumps(mapping_output), headers=headers)      
# time.sleep(180)

claim_df_6A = claim_df_6.withColumn('CLAIM_AMOUNT',claim_df_6.CLAIM_AMOUNT.cast(DoubleType()))

final_spark_data_new = claim_df_6A
try:
    final_spark_data_new.write.format('org.elasticsearch.spark.sql').mode('append').option('es.index.auto.create', 'true').\
    option('es.nodes' , es_nodes).option('es.port', es_port).option('es.resource',target_index_doc_type ).save() 
except Exception as e:
    x = e
    clm_load_flg = -1
else: 
    x = 200 # success
    clm_load_flg = 0
print (x)

# ticket data

# input_indices_name = 'marketing_icrm_cases_alias'
# input_doc_type_name = input_indices_name+'/icrm_cases'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# icrm_cases = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','id,name,date_entered,case_closure_date,service_status,source,work_note,queue_name,interaction_category').\
# option('es.query', esq_icrm_cases).\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "marketing_icrm_cases",
         "partitionColumn":"row_num","partitions":1,"server.port":"1107"} 

icrm_cases = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('ID','NAME','DATE_ENTERED','CASE_CLOSURE_DATE','SERVICE_STATUS','SOURCE','WORK_NOTE','QUEUE_NAME','INTERACTION_CATEGORY')

# ticket data

# input_indices_name = 'marketing_icrm_mph_policy_alias'
# input_doc_type_name = input_indices_name+'/icrm_mph_policy'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# icrm_policy = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','id, name').\
# option('es.query', esq_icrm_policy).\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "marketing_icrm_mph_policy",
         "partitionColumn":"row_num","partitions":1,"server.port":"1107"} 

icrm_policy = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('ID','NAME')

# input_indices_name = 'marketing_icrm_mph_queues_alias'
# input_doc_type_name = input_indices_name+'/icrm_mph_queues'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# icrm_queues = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','id, name').\
# option('es.query', esq_icrm_queues).\
# option('es.input.max.docs.per.partition',document_count).load()
        
gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "marketing_icrm_mph_queues",
         "partitionColumn":"row_num","partitions":1,"server.port":"1107"} 

icrm_queues = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('ID','NAME')

# input_indices_name = 'marketing_icrm_mph_sr_alias'
# input_doc_type_name = input_indices_name+'/icrm_mph_sr'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# icrm_sr = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','id, name').\
# option('es.query', esq_icrm_sr).\
# option('es.input.max.docs.per.partition',document_count).load()

gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "marketing_icrm_mph_sr",
         "partitionColumn":"row_num","partitions":1,"server.port":"1107"} 

icrm_sr = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('ID','NAME')

        
# input_indices_name = 'marketing_icrm_mph_policy_cases_1_c_alias'
# input_doc_type_name = input_indices_name+'/icrm_mph_policy_cases_1_c'

# document_count =documentcount(input_doc_type_name)
# print "record count - "+input_indices_name+" : "+str(document_count)

# icrm_policy_cases = sqlContext.read.format('org.elasticsearch.spark.sql').option('es.nodes' , es_nodes).option('es.port',es_port).\
# option('es.resource',input_doc_type_name).\
# option('es.read.field.include','mph_policy_cases_1cases_idb, mph_policy_cases_1mph_policy_ida').\
# option('es.query', esq_icrm_pol_case).\
# option('es.input.max.docs.per.partition',document_count).load()
        
gscPythonOptions = {"url": "jdbc:postgresql://10.35.12.194:5432/gpadmin","user": "gpspark","password": "spark@456",
         "dbschema": "public","dbtable": "marketing_icrm_mph_policy_cases_1_c",
         "partitionColumn":"row_num","partitions":1,"server.port":"1107"} 

icrm_policy_cases = sqlContext.read.format("greenplum").options(**gscPythonOptions).load()\
.select('MPH_POLICY_CASES_1CASES_IDB', 'MPH_POLICY_CASES_1MPH_POLICY_IDA')

icrm_cases_1 = icrm_cases.withColumnRenamed('id', 'CASE_ID')\
                         .withColumnRenamed('name', 'INBOUND_TICKET')\
                         .withColumnRenamed('date_entered', 'TICKET_RAISED_DATE')\
                         .withColumnRenamed('case_closure_date', 'TICKET_CLOSED_DATE')\
                         .withColumnRenamed('service_status', 'TICKET_STATUS')\
                         .withColumnRenamed('source', 'SOURCE')\
                         .withColumnRenamed('queue_name', 'QUEUE_ID')\
                         .withColumnRenamed('interaction_category', 'SR_ID')\
                         .withColumn('TICKET_REMARK', substring(col("work_note"), 1, 255))\
                    
icrm_cases_2 = icrm_cases_1.groupBy('CASE_ID')\
                           .agg(sf.max('INBOUND_TICKET').alias('INBOUND_TICKET'),\
                                sf.max('TICKET_RAISED_DATE').alias('TICKET_RAISED_DATE'),\
                                sf.max('TICKET_CLOSED_DATE').alias('TICKET_CLOSED_DATE'),\
                                sf.max('TICKET_STATUS').alias('TICKET_STATUS'),\
                                sf.max('SOURCE').alias('SOURCE'),\
                                sf.max('TICKET_REMARK').alias('TICKET_REMARK'),\
                                sf.max('QUEUE_ID').alias('QUEUE_ID'),\
                                sf.max('SR_ID').alias('SR_ID'))

icrm_policy_1 = icrm_policy.withColumnRenamed('name', 'POLICY_NO')\
                           .withColumnRenamed('id', 'POLICY_ID')
    
icrm_policy_2 = icrm_policy_1.groupBy('POLICY_ID').agg(sf.max('POLICY_NO').alias('POLICY_NO'))

icrm_queues_1 = icrm_queues.withColumnRenamed('name', 'TICKET_TYPE')\
                           .withColumnRenamed('id', 'QUEUE_ID')
    
icrm_queues_2 = icrm_queues_1.groupBy('QUEUE_ID').agg(sf.max('TICKET_TYPE').alias('TICKET_TYPE'))

icrm_sr_1 = icrm_sr.withColumnRenamed('name', 'SUB_TYPE')\
                   .withColumnRenamed('id', 'SR_ID')
    
icrm_sr_2 = icrm_sr_1.groupBy('SR_ID').agg(sf.max('SUB_TYPE').alias('SUB_TYPE'))

icrm_policy_cases_1 = icrm_policy_cases.withColumnRenamed('mph_policy_cases_1cases_idb', 'CASE_ID')\
                                       .withColumnRenamed('mph_policy_cases_1mph_policy_ida', 'POLICY_ID')

rccn_icrm_policy_1 = cust_policy_1.join(icrm_policy_2, 'POLICY_NO','inner')
icrm_df = rccn_icrm_policy_1.join(icrm_policy_cases_1, 'POLICY_ID', 'left')
icrm_df_1 = icrm_df.join(icrm_cases_2, 'CASE_ID', 'left')
icrm_df_2 = icrm_df_1.join(icrm_sr_2, 'SR_ID', 'left')
icrm_df_3 = icrm_df_2.join(icrm_queues_2, 'QUEUE_ID', 'left')

icrm_df_3A = icrm_df_3\
.withColumn('LOAD_DATE',lit(current_date))\
.withColumn('TICKET_CLOSED_DATE_str', icrm_df_3.TICKET_CLOSED_DATE.cast(StringType()))\
.withColumn('TICKET_RAISED_DATE_str', icrm_df_3.TICKET_RAISED_DATE.cast(StringType()))\
.drop('SR_ID')\
.drop('CASE_ID')\
.drop('POLICY_ID')\
.drop('QUEUE_ID')\

icrm_df_3B = icrm_df_3A.drop('TICKET_CLOSED_DATE').drop('TICKET_RAISED_DATE')
icrm_df_4 = icrm_df_3B\
.withColumnRenamed('TICKET_CLOSED_DATE_str', 'TICKET_CLOSED_DATE')\
.withColumnRenamed('TICKET_RAISED_DATE_str', 'TICKET_RAISED_DATE')\


icrm_df_5 = icrm_df_4[['INBOUND_TICKET','LOAD_DATE','MOBILE_NO','POLICY_NO','SOURCE','SUB_TYPE','TICKET_CLOSED_DATE','TICKET_RAISED_DATE','TICKET_REMARK','TICKET_STATUS','TICKET_TYPE']]

target_index = 'cust_summary_ticket'+"_"+currentdate
# target_index = 'cust_summary_ticket_test_1'+"_"+currentdate
target_doc_type = "cust_summary"

target_index_doc_type = target_index +'/'+ target_doc_type
print (target_index_doc_type)

output_index = target_index
doc_type = target_doc_type

###delete index if already exists########################################################
headers = {'Content-Type': 'application/json'}
query = {"query":{"match_all":{}}}
URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index
r = requests.delete(URL, data=json.dumps(query), headers=headers)

# time.sleep(180)

###create index ######################################################
headers = {'Content-Type': 'application/json'}
query_create= {"settings": { "index.number_of_shards": "1" , "index.number_of_replicas":"0"}}
URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index
r = requests.put(URL, data=json.dumps(query_create), headers=headers)

# time.sleep(180)

### create mapping######################################################################################

mapping_output = {doc_type:{
'properties': {
'POLICY_NO': {'type': 'keyword'},
'TICKET_CLOSED_DATE': {'type': 'date','ignore_malformed': 'true','format': 'yyyy-MM-dd'},
'TICKET_RAISED_DATE': {'type': 'date','ignore_malformed': 'true','format': 'yyyy-MM-dd'},
'INBOUND_TICKET': {'type': 'keyword'},
'TICKET_STATUS': {'type': 'keyword'},
'TICKET_TYPE': {'type': 'keyword'},
'SOURCE': {'type': 'keyword'},
'TICKET_REMARK': {'type': 'text'},
'SUB_TYPE': {'type': 'keyword'},
'MOBILE_NO': {'type': 'keyword'},
'LOAD_DATE': {'type': 'date'}
}}}



URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+output_index+'/_mapping/'+doc_type
headers = {'Content-Type': 'application/json'}
r = requests.put(URL, data=json.dumps(mapping_output), headers=headers)

# time.sleep(300)

final_spark_data_new = icrm_df_5
try:
    final_spark_data_new.write.format('org.elasticsearch.spark.sql').mode('append').option('es.index.auto.create', 'true').\
    option('es.nodes' , es_nodes).option('es.port', es_port).option('es.resource',target_index_doc_type ).save() 
except Exception as e:
    x = e
    crm_load_flg = -1
else: 
    x = 200 # success
    crm_load_flg = 0
print (x)

# alias management for high availibility

headers = {'Content-Type': 'application/json'}

# policy
today_index = 'cust_summary_policy'+"_"+currentdate
yesterday_index = 'cust_summary_policy'+"_"+yesterday
alias_name = "cust_summary_policy_alias"

if pol_load_flg==0:
    query_alias= {"actions" : [{ "remove" : { "index" : "cust_summary_policy_*", "alias" : alias_name }}]}
    URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/_aliases'
    r = requests.post(URL, data=json.dumps(query_alias), headers=headers)
    time.sleep(60)
    query_alias= {"actions" : [{ "add" : { "index" : today_index, "alias" : alias_name }}]}
    URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/_aliases'
    r = requests.post(URL, data=json.dumps(query_alias), headers=headers)
    time.sleep(60)

# claim
today_index = 'cust_summary_claim'+"_"+currentdate
yesterday_index = 'cust_summary_claim'+"_"+yesterday
alias_name = "cust_summary_claim_alias"

if clm_load_flg==0:
    query_alias= {"actions" : [{ "remove" : { "index" : "cust_summary_claim_*", "alias" : alias_name }}]}
    URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/_aliases'
    r = requests.post(URL, data=json.dumps(query_alias), headers=headers)
    time.sleep(60)
    query_alias= {"actions" : [{ "add" : { "index" : today_index, "alias" : alias_name }}]}
    URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/_aliases'
    r = requests.post(URL, data=json.dumps(query_alias), headers=headers)
    time.sleep(60)

# crm
today_index = 'cust_summary_ticket'+"_"+currentdate
yesterday_index = 'cust_summary_ticket'+"_"+yesterday
alias_name = "cust_summary_ticket_alias"

if crm_load_flg==0:
    query_alias= {"actions" : [{ "remove" : { "index" : "cust_summary_ticket_*", "alias" : alias_name }}]}
    URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/_aliases'
    r = requests.post(URL, data=json.dumps(query_alias), headers=headers)
    time.sleep(60)
    query_alias= {"actions" : [{ "add" : { "index" : today_index, "alias" : alias_name }}]}
    URL = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/_aliases'
    r = requests.post(URL, data=json.dumps(query_alias), headers=headers)
    time.sleep(60)

# delete indices created before yesterday

query = {"query":{"match_all":{}}}

pol_index = 'cust_summary_policy'+"_"+dayBeforeYesterday
clm_index = 'cust_summary_claim'+"_"+dayBeforeYesterday
crm_index = 'cust_summary_ticket'+"_"+dayBeforeYesterday

URL_pol = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+pol_index
URL_clm = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+clm_index
URL_crm = 'http://'+es_user+':'+es_pwd+'@'+es_nodes+':'+es_port+'/'+crm_index

r = requests.delete(URL_pol, data=json.dumps(query), headers=headers)
time.sleep(60)

r = requests.delete(URL_clm, data=json.dumps(query), headers=headers)
time.sleep(60)

r = requests.delete(URL_crm, data=json.dumps(query), headers=headers)
time.sleep(60)