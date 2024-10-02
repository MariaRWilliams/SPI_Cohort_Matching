# Databricks notebook source
# MAGIC %md
# MAGIC - DataPrep
# MAGIC   - load event table and list available event categories
# MAGIC   - choose 'exposed' cohort + month
# MAGIC   - choose 'control' cohort
# MAGIC   - collect matching variables for both
# MAGIC - Cohort Matching
# MAGIC   - scale, etc. to prep for matching
# MAGIC   - run matching algorithm
# MAGIC   - export matching data

# COMMAND ----------

from src import matching_class
from src import prep_class
import pandas as pd

# COMMAND ----------

#get list of event categories from cg table
#note: if get deletionVectors error, update cluster to one with Databricks Runtime 12.2 LTS - 15.3
categories = (
    spark
    .sql("SELECT distinct category FROM dev.`clinical-analysis`.cohort_matching_edw_events")
)

categories = categories.toPandas()
categories = categories['category'].unique()

print(categories)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select Exposed Subset

# COMMAND ----------


