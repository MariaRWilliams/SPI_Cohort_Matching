# Databricks notebook source
# MAGIC %md
# MAGIC - DataPrep
# MAGIC   - load event table and list available event categories
# MAGIC   - choose 'exposed' cohort + month (at this point its the same old process)
# MAGIC   - choose 'control' cohort (should be able to choose from Accolade or Marketscan)
# MAGIC   - collect matching variables for both
# MAGIC - Cohort Matching
# MAGIC   - scale, etc. to prep for matching
# MAGIC   - run matching algorithm
# MAGIC   - export matching data

# COMMAND ----------

#note: if get deletionVectors error while querying tables, update cluster to one with Databricks Runtime 12.2 LTS - 15.3
from pyspark.sql.functions import to_date
import pyspark.sql.functions as F
from src import matching_class
from src import prep_class
import pandas as pd

pc = prep_class.Data_Prep()

# COMMAND ----------

#clean window for exposed cohort: what months pre and post should not have other events?
clean_preperiod = 3
clean_postperiod = 0

#evaluation window: in what window do we want to make sure members are fully eligible?
eval_preperiod = 3
eval_postperiod = 6

#cohort matching window: what months should we consider for matching?
match_preperiod = 3
match_postperiod = 0

#claims max: what month is the max to expect correct claim data?
claims_cap = '2024-08-01'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Data

# COMMAND ----------

#pull in event data
event_df = pc.query_data(spark, dbutils, 'cohort_matching_edw_events')
event_df = event_df.withColumn('utc_period', F.to_date(event_df.utc_period, 'yyyyMM'))

pc.set_event_categories(event_df)
print(pc.event_list)

# COMMAND ----------

#pull in claims and utilization data (fix ingestion so total_allowed adds null as 0)
claims_df = pc.query_data(spark, dbutils, 'cohort_matching_cg_claims')
claims_df = claims_df.withColumn('service_month', F.to_date(claims_df.service_month, 'yyyyMM'))

pc.set_util_categories(claims_df)
print(pc.util_list)

# COMMAND ----------

#pull in member demographic data
mem_df = pc.query_data(spark, dbutils, 'cohort_matching_cg_mem')
mem_df = mem_df.withColumn('start_date', F.to_date(mem_df.start_date, 'yyyyMM'))
mem_df = mem_df.withColumn('end_date', F.to_date(mem_df.end_date, 'yyyyMM'))

print(mem_df.columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Exposed Subset

# COMMAND ----------

#available events to choose from 
print(pc.event_list)

# COMMAND ----------

#select event categories to use in exposed subset
exposed_categories = ['Case Management','High Cost Claimants (HCC)']

# COMMAND ----------

#limit exposed cohort to only those without other events in clean window (evaluates on all events, not just those selected)
exposed_subset = event_df.filter(event_df.category!='exclude')
exposed_subset = pc.clean_exposed(spark, exposed_subset, exposed_categories, clean_preperiod, clean_postperiod)

# COMMAND ----------

#add demographics and limit to those eligible for full evaluation window
exposed_subset = pc.limit_exposed(exposed_subset, mem_df, eval_preperiod, eval_postperiod)

# COMMAND ----------

print('Customers: '+ str(exposed_subset.select('customer_nm').distinct().count()))
print('Event sample size:')
exposed_subset.select('category', 'person_id').groupby('category').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Control Subset - from Accolade Members

# COMMAND ----------

#remove members with events (all events from original data pull, not just from exposed subset)
control_subset = mem_df.join(event_df, on='person_id', how='left_anti')

# COMMAND ----------

#add months: within claims data, within eligibility window for each member, with buffer for evaluation window
#get preperiod/postperiod and service_month tightened up
control_subset = pc.limit_control(spark, claims_df, control_subset, eval_preperiod, eval_postperiod, claims_cap)
control_subset.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Control Subset - from Marketscan Members

# COMMAND ----------

#do this once Marketscan is available

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add claims features

# COMMAND ----------

#these lines bring in the code again if updated after original run
import pyspark.sql.functions as F
import importlib
import src.prep_class as prep_class

importlib.reload(prep_class)
pc = prep_class.Data_Prep()

# COMMAND ----------

#choose what claims data to include as variables
print(pc.util_list)

claims_list = ['total_allowed', 'Emergency Room']

# COMMAND ----------

#exposed dataset
exposed_claims = pc.merge_claims(spark, exposed_subset, claims_df, match_preperiod, match_postperiod)
exposed_claims = pc.pivot_claims(exposed_claims, claims_list)
exposed_subset = exposed_subset.join(exposed_claims, ['dw_member_id', 'utc_period'], how='inner')

exposed_subset.display()

# COMMAND ----------

#control dataset
control_claims = pc.merge_claims(spark, control_subset, claims_df, match_preperiod, match_postperiod)
control_claims = pc.pivot_claims(control_claims, claims_list)
control_subset = control_subset.join(control_claims, ['dw_member_id', 'utc_period'], how='inner')

control_subset.display()
