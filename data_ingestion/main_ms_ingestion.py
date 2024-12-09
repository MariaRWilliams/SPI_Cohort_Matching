# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Ingestion for IBM MarketScan Data
# MAGIC Steps Contained in this Notebook:
# MAGIC
# MAGIC - import libraries and establish connections
# MAGIC - query emo cluster
# MAGIC - write results to data catalog

# COMMAND ----------

from src import query_class_ms
from src import query_class_conditions
import pandas as pd
import re

# COMMAND ----------

#static input variables
EDW_USER = dbutils.secrets.get(scope="clinical-analysis", key="emo-edw-user")
EDW_PASSWORD = dbutils.secrets.get(scope="clinical-analysis", key="emo-edw-pass")

# COMMAND ----------

# MAGIC %run /Workspace/common/spokes/emo-spoke

# COMMAND ----------

spark_reader = EDWSpoke(EDW_USER, EDW_PASSWORD).connect()
start_year = '2023'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Query data

# COMMAND ----------

# these lines bring in the code again if updated after original run
# import importlib
# from src import query_class_ms
# from src import query_class_conditions

# importlib.reload(query_class_ms)
# importlib.reload(query_class_conditions)

# qc = query_class_ms.QueryClass()
# qc = query_class_conditions.ConditionsClass()

# COMMAND ----------

qc_ms = query_class_ms.QueryClass()
qc_cond = query_class_conditions.ConditionsClass()

# COMMAND ----------

#demographics, claims, utilization
q = qc_ms.query_demographics(start_year)
s_df = spark_reader.option('query', q).load()

# q = qc_ms.query_med_claims(start_year)
# s_df = spark_reader.option('query', q).load()

# q = qc_ms.query_pharma_claims(start_year)
# s_df = spark_reader.option('query', q).load()

# q = qc_ms.query_utilization(start_year)
# s_df = spark_reader.option('query', q).load()

#check
s_df.show(5)

# COMMAND ----------

#conditions
q = qc_cond.temp_conditions()
s_df = spark_reader.option('query', q).load()

q = qc_cond.temp_conditions_ms()
s_df = spark_reader.option('query', q).load()

q = qc_cond.query_conditions()
s_df = spark_reader.option('query', q).load()


# COMMAND ----------

example_query = """
select * 
from emo_edw.ibm_marketscan.ccaed
limit 20
"""

# example_query = """
# select * 
# from emo_edw.ibm_marketscan.all_claims
# limit 20
# """

query_df = spark_reader.option('query', example_query).load()

# COMMAND ----------

query_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write Data to Tables

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
