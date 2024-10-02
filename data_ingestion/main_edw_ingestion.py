# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Ingestion for EDW Data
# MAGIC Exports table to data catalog.

# COMMAND ----------

from src import query_class_edw
import pandas as pd
import re

# COMMAND ----------

#static input variables
EDW_USER = dbutils.secrets.get(scope="clinical-analysis", key="edw-service")
EDW_PASSWORD = dbutils.secrets.get(scope="clinical-analysis", key="edw-password")

start_year = '2024'
#cust_list_string = 'STATE FARM'

# COMMAND ----------

#get list of customers from cg table
cust = (
    spark
    .sql("SELECT distinct table_schema FROM dev.`clinical-analysis`.test")
)

cust = cust.toPandas()
cust_list = cust['table_schema'].unique()
cust_list = [re.sub("\'", "\'\'", s) for s in cust_list]
cust_list_string = "','".join(cust_list)
#print(cust_list_string)
print(str(len(cust_list))+" Customer(s) selected")

# COMMAND ----------

# MAGIC %run /Workspace/common/spokes/edw-spoke

# COMMAND ----------

spark_reader = EDWSpoke(EDW_USER, EDW_PASSWORD).connect()

# COMMAND ----------

#query edw events
qc = query_class_edw.QueryClass()
q = qc.query_spi_events(start_year, cust_list_string)
sdf = spark_reader.option('query', q).load()
display(sdf)

# COMMAND ----------

#write data to table
(
    sdf
    .write
    .format("delta")
    .option("overwriteSchema", "true")
    .mode("overwrite")
    .saveAsTable("dev.`clinical-analysis`.test")
)
