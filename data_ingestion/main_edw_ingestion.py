# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Ingestion for EDW Data
# MAGIC Exports table to data catalog.

# COMMAND ----------

from src import query_class_edw
from src import helper_class
import pandas as pd
import re

# COMMAND ----------

#static input variables
EDW_USER = dbutils.secrets.get(scope="clinical-analysis", key="edw-service")
EDW_PASSWORD = dbutils.secrets.get(scope="clinical-analysis", key="edw-password")

# COMMAND ----------

#get list of customers from cg table
#note: if get deletionVectors error, update cluster to one with Databricks Runtime 12.2 LTS - 15.3
cust = (
    spark
    .sql("SELECT distinct customer_nm FROM dev.`clinical-analysis`.cohort_matching_cg_mem")
)

#convert to edw customers
hc = helper_class.CG_Helper()
cust = cust.toPandas()
cust = hc.map_customers(cust, 'customer_nm')
cust_list = cust[~cust['edw_cust'].isin(['BK', 'NON-ACCOLADE', 'ACCOLADE', 'BLANK'])]['edw_cust'].unique()
cust_list = [re.sub("\'", "\'\'", s) for s in cust_list]
cust_list_string = "','".join(cust_list)

#print statements
#print(cust_list_string)
print(str(len(cust_list))+" Customer(s) selected")

# COMMAND ----------

# MAGIC %run /Workspace/common/spokes/edw-spoke

# COMMAND ----------

spark_reader = EDWSpoke(EDW_USER, EDW_PASSWORD).connect()
start_year = '2023'

# COMMAND ----------

#query edw events
qc = query_class_edw.QueryClass()
q = qc.query_spi_events(start_year, cust_list_string)
s_df = spark_reader.option('query', q).load()

#check
edw_df= s_df.toPandas()
print(edw_df.head())
#print(edw_df['org_nm'].unique().tolist())

# COMMAND ----------

#write data to table
(
    s_df
    .write
    .format("delta")
    .option("overwriteSchema", "true")
    .mode("overwrite")
    .saveAsTable("dev.`clinical-analysis`.cohort_matching_edw_events")
)
