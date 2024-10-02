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

pc = prep_class.Data_Prep()

# COMMAND ----------

#these two lines bring in the code again if updated after original run
import importlib
import src.prep_class as prep_class
importlib.reload(prep_class)
pc = prep_class.Data_Prep()

# COMMAND ----------

#get event data
#note: if get deletionVectors error, update cluster to one with Databricks Runtime 12.2 LTS - 15.3
event_df = (
    spark
    .sql("SELECT * FROM dev.`clinical-analysis`.cohort_matching_edw_events")
)

event_df = event_df.toPandas()
pc.set_categories(event_df)
print(pc.category_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select Exposed Subset

# COMMAND ----------

#limit exposed cohort to only those without other events in period window

preperiod = 3
postperiod = 0

exposed_subset = pc.clean_exposed(event_df, preperiod, postperiod)

# COMMAND ----------

print(exposed_subset.head())
