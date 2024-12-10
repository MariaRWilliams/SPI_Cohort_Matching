# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Ingestion for EDW Data
# MAGIC Steps Contained in this Notebook:
# MAGIC - import libraries and establish connections
# MAGIC - get customer list from CG table 
# MAGIC   - this is to limit the query to members for whom we have claims data
# MAGIC - query edw cluster
# MAGIC - write results to data catalog

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

# MAGIC %run /Workspace/common/spokes/edw-spoke

# COMMAND ----------

spark_reader = EDWSpoke(EDW_USER, EDW_PASSWORD).connect()
start_year = '2022'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Query data

# COMMAND ----------

#get list of customers from cg table
#note: if get deletionVectors error, update cluster to one with Databricks Runtime 12.2 LTS - 15.3
cust = (
    spark
    .sql("SELECT distinct edw_cust FROM dev.`clinical-analysis`.cohort_matching_cg_mem")
)

#convert to edw customers
hc = helper_class.CG_Helper()
cust = cust.toPandas()
#cust = hc.map_customers(cust, 'customer_nm')
cust_list = cust[~cust['edw_cust'].isin(['BK', 'NON-ACCOLADE', 'ACCOLADE', 'BLANK'])]['edw_cust'].unique()
cust_list = [re.sub("\'", "\'\'", s) for s in cust_list]
cust_list_string = "','".join(cust_list)

#print statements
#print(cust_list_string)
print(str(len(cust_list))+" Customer(s) selected")

# COMMAND ----------

#query EDW
qc = query_class_edw.QueryClass()
#q = qc.query_spi_events(start_year, cust_list_string)
#q = qc.query_hcc_clinical_events(start_year, cust_list_string)
#q = qc.query_funnel(start_year, cust_list_string)
#q = qc.query_eng_events(start_year, cust_list_string)
#q = qc.query_TPE_enrollment(start_year, cust_list_string)
# q = qc.query_care(start_year, cust_list_string)
s_df = spark_reader.option('query', q).load()

#check
s_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to data catalog

# COMMAND ----------

#append data to table (will add columns if not already present)
# (
#     s_df
#     .write
#     .format("delta")
#     .option("mergeSchema", "true")
#     .mode("append")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_edw_events")
# )

# COMMAND ----------

#overwrite table with data
# (
#     s_df
#     .write
#     .format("delta")
#     .option("overwriteSchema", "true")
#     .mode("overwrite")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_edw_events")
# )

# COMMAND ----------

# these lines bring in the code again if updated after original run
# import importlib
# from src import query_class_edw
# from src import helper_class

# importlib.reload(query_class_edw)
# importlib.reload(helper_class)

# qc = query_class_edw.QueryClass()
