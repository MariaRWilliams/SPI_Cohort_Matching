# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Ingestion for IBM MarketScan Data
# MAGIC Exports table to data catalog.

# COMMAND ----------

from src import query_class_ms
from src import helper_class
import pandas as pd
import re

# COMMAND ----------

#static input variables
EDW_USER = dbutils.secrets.get(scope="clinical-analysis", key="edw-service")
EDW_PASSWORD = dbutils.secrets.get(scope="clinical-analysis", key="edw-password")

# COMMAND ----------

# MAGIC %run /Workspace/common/spokes/emo-spoke

# COMMAND ----------

spark_reader = EDWSpoke(EDW_USER, EDW_PASSWORD).connect()
start_year = '2023'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Query data

# COMMAND ----------

example_query = """
select * 
from emo_edw.edw.specialist_qualification 
limit 20
"""

query_df = spark_reader.option('query', example_query).load()

# COMMAND ----------

query_df.display()

# COMMAND ----------

#test
q = f"""select * from emo_edw.edw.member limit 10"""
s_df = spark_reader.option('query', q).load()

#check
s_df.show(5)
