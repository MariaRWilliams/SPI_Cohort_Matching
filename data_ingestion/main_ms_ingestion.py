# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Ingestion for IBM MarketScan Data
# MAGIC Steps Contained in this Notebook:
# MAGIC
# MAGIC - Import libraries and establish connections
# MAGIC - Base tables:
# MAGIC   - chronic conditions from edw
# MAGIC   - utilization categories from emo
# MAGIC - Participant Demographics
# MAGIC - Participant Claims and Utilization
# MAGIC - Participant Chronic Conditions
# MAGIC - write results to data catalog

# COMMAND ----------

from src import query_class_ms
from src import query_class_source_codes
from src import helper_class_ms
import pyspark.sql.functions as F
import pandas as pd
import re

# COMMAND ----------

qc_ms = query_class_ms.QueryClass()
qc_base = query_class_source_codes.QueryClass()
hc = helper_class_ms.MS_Helper()

# COMMAND ----------

#queries one year at a time for memory reasons
svc_year = '2023'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Connect to EDW spoke
# MAGIC Queries base tables: 
# MAGIC - disease_joiner_tmp
# MAGIC - drug_table_tmp
# MAGIC - avoidable_er_tmp

# COMMAND ----------

#edw spoke
EDW_USER = dbutils.secrets.get(scope="clinical-analysis", key="edw-service")
EDW_PASSWORD = dbutils.secrets.get(scope="clinical-analysis", key="edw-password")

# COMMAND ----------

# MAGIC %run /Workspace/common/spokes/edw-spoke

# COMMAND ----------

edw_spoke = EDWSpoke(EDW_USER, EDW_PASSWORD).connect()

# COMMAND ----------

#query base conditions from edw: chronic conditions, avoidable er
q = qc_base.query_cond()
disease_joiner_tmp = edw_spoke.option('query', q).load()

q = qc_base.query_d_med()
drug_table_tmp = edw_spoke.option('query', q).load()

q = qc_base.query_avoidable_er()
avoidable_er_tmp = edw_spoke.option('query', q).load()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Connect to EMO spoke

# COMMAND ----------

#emo spoke
EDW_USER = dbutils.secrets.get(scope="clinical-analysis", key="emo-edw-user")
EDW_PASSWORD = dbutils.secrets.get(scope="clinical-analysis", key="emo-edw-pass")

# COMMAND ----------

# MAGIC %run /Workspace/common/spokes/emo-spoke

# COMMAND ----------

emo_spoke = EDWSpoke(EDW_USER, EDW_PASSWORD).connect()

# COMMAND ----------

#base utilization table
q = qc_base.query_utilization_codes()
util_tmp = emo_spoke.option('query', q).load()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Participant Demographic Data

# COMMAND ----------

q = qc_ms.query_demographics(svc_year)
mem_df = emo_spoke.option('query', q).load()

mem_df = hc.map_state(mem_df)

# COMMAND ----------

#overwrite table with data
# (
#     mem_df
#     .write
#     .format("delta")
#     .option("overwriteSchema", "true")
#     .mode("overwrite")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_ms_mem")
# )

# COMMAND ----------

#append data to table
# (
#     mem_df
#     .write
#     .format("delta")
#     .option("mergeSchema", "true")
#     .mode("append")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_ms_mem")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Particpant Spend and Utilization Data

# COMMAND ----------

#claims: spend
q = qc_ms.query_med_claims(svc_year)
med_df = emo_spoke.option('query', q).load()

q = qc_ms.query_rx_claims(svc_year)
pharma_df = emo_spoke.option('query', q).load()

claims_df = hc.join_claims_spend(med_df, pharma_df)

# COMMAND ----------

#claims: codes (~20 minutes)
q = qc_ms.query_rx_codes(svc_year)
pharma_codes_df = emo_spoke.option('query', q).load()

q = qc_ms.query_med_codes(svc_year)
med_codes_df = emo_spoke.option('query', q).load()

med_codes_df = hc.join_med_codes(med_codes_df, util_tmp, avoidable_er_tmp)
med_codes_df = med_codes_df.cache()

# COMMAND ----------

#limit to relevant utilization and compute visits
visit_df = hc.get_visit_util(med_codes_df)

#add avoidable er and readmissions
full_util_df = hc.add_util(visit_df)

#count by member month
util_df = hc.count_util(full_util_df)

#add to claims doc
claims_df = claims_df.join(util_df, on=['member_id', 'service_month'], how='left').fillna(0)

# COMMAND ----------

#overwrite table with data (~20 minutes including code above)
# (
#     claims_df
#     .write
#     .format("delta")
#     .option("overwriteSchema", "true")
#     .mode("overwrite")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_ms_claims")
# )

# COMMAND ----------

#append data to table
# (
#     claims_df
#     .write
#     .format("delta")
#     .option("mergeSchema", "true")
#     .mode("append")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_ms_claims")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Participant Medical Conditions

# COMMAND ----------

#limit codes data to relevant conditions
cond_df = hc.get_conditions(med_codes_df, disease_joiner_tmp)
cond_df = cond_df.cache()

# COMMAND ----------

#get member_id and condition when condition was first dx for Inpatient or ER Utilization
cond_util = hc.util_conditions(cond_df)

#get member_id and condition when diagnosed twice (note: full logic requires it being twice within two years)
cond_dx = hc.dx_conditions(cond_df)

#get member_id and condition based on ndc codes (only for diabetes at this point)
cond_rx = hc.rx_conditions(pharma_codes_df, drug_table_tmp)

# COMMAND ----------

#union conditions tables and pivot to flag columns (~15 minutes)
cond_full = cond_util.union(cond_dx).union(cond_rx).dropDuplicates()
cond_full = cond_full.withColumn('condition', F.lower(F.col('condition'))).groupBy('member_id').pivot('condition').agg(F.when(F.first('condition').isNotNull(), 1).otherwise(0)).fillna(0)

#add year since we are doing one year at a time
cond_full = cond_full.withColumn('cal_yr', F.lit(svc_year))

# COMMAND ----------

#overwrite table with data
# (
#     cond_full
#     .write
#     .format("delta")
#     .option("overwriteSchema", "true")
#     .mode("overwrite")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_ms_chron")
# )

# COMMAND ----------

#append data to table
# (
#     cond_full
#     .write
#     .format("delta")
#     .option("mergeSchema", "true")
#     .mode("append")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_ms_chron")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Extra

# COMMAND ----------

#these lines bring in the code again if updated after original run
# import importlib
# from src import query_class_ms
# from src import query_class_source_codes
# from src import helper_class_ms

# importlib.reload(query_class_ms)
# importlib.reload(query_class_source_codes)
# importlib.reload(helper_class_ms)

# qc_ms = query_class_ms.QueryClass()
# qc_base = query_class_source_codes.QueryClass()
# hc = helper_class_ms.MS_Helper()

# COMMAND ----------

# example_query = """
#                 select * 
#                 from emo_edw.ibm_marketscan.all_medical_claims ac
#                 limit 20
#                 """

# query_df = emo_spoke.option('query', example_query).load()
# query_df.limit(10).display()
