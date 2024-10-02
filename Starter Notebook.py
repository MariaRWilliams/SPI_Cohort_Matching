# Databricks notebook source
#imports
import time
import datetime
import pandas as pd
import numpy as np
import pyspark
import pyspark.pandas as ps
import os
from sys import path

# builtin
from string import Template
import time

from pyspark.sql import (
    functions as psf,
    types as pst,
)

# custom
from src_queries import query_class_edw

# COMMAND ----------

#these two lines bring in the code again if updated after original run
import importlib
importlib.reload(query_class_edw)

# COMMAND ----------

#passwords, etc
EDW_USER = dbutils.secrets.get(scope="clinical-analysis", key="edw-service")
EDW_PASSWORD = dbutils.secrets.get(scope="clinical-analysis", key="edw-password")


# COMMAND ----------

# MAGIC %run /Workspace/common/spokes/edw-spoke

# COMMAND ----------

#add connection
spark_reader = EDWSpoke(EDW_USER, EDW_PASSWORD).connect()


# COMMAND ----------

qc = query_class_edw.QueryClass()

# COMMAND ----------

q = qc.query_spi_events('2024', 'STATE FARM')
sdf = spark_reader.option('query', q).load()
display(sdf)

#query
sdf_ingest = spark_reader.option('query', q).load()

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

# COMMAND ----------

sdf_tester = (
    spark
    .sql("SELECT * FROM dev.`clinical-analysis`.test")
)
