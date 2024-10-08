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

#pull in claims and utilization data (would be better if full claims were already calculated by upload)
claims_df = pc.query_data(spark, dbutils, 'cohort_matching_cg_claims')
claims_df = claims_df.withColumn('service_month', F.to_date(claims_df.service_month, 'yyyyMM'))

pc.set_util_categories(claims_df)
print(pc.util_list)

# COMMAND ----------

#pull in member demographic data (would be better if 'end date' was date of data pull)
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

exposed_subset.columns

# COMMAND ----------

print('Customers: '+ str(exposed_subset.select('customer_nm').distinct().count()))
print('Event sample size:')
exposed_subset.select('category', 'person_id').groupby('category').count().show()

# COMMAND ----------

print('Customers: '+ str(len(exposed_subset['org_nm'].unique().tolist())))
print('Event sample size:')
print(exposed_subset[['category', 'person_id']].groupby('category').count().reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Control Subset - from Accolade Members

# COMMAND ----------

#these lines bring in the code again if updated after original run
import pyspark.sql.functions as F
import importlib
import src.prep_class as prep_class

importlib.reload(prep_class)
pc = prep_class.Data_Prep()

# COMMAND ----------

#remove members with events (all events from original data pull, not just from exposed subset)
control_subset = mem_df.join(event_df, on='person_id', how='left_anti')

# COMMAND ----------

min_claim = claims_df.agg(F.min('service_month')).collect()[0][0]
max_claim = claims_df.agg(F.max('service_month')).collect()[0][0]

print(min_claim)
print(max_claim)

# COMMAND ----------

claims_df.filter(claims_df['service_month'] >= max_claim).show()

# COMMAND ----------

hm = claims_df.select(F.collect_set('service_month').alias('service_month')).first()['service_month']
print(hm)

# COMMAND ----------

#add months within evaluation window and within claims data
#min_claim = min(claims_df['service_month'])
control_subset = pc.limit_control(control_subset, eval_preperiod, eval_postperiod, min_claim, max_claim)
#control_subset.show(5)
print(control_subset)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Control Subset - from Marketscan Members

# COMMAND ----------

#do this once Marketscan is available

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add features and prepare for matching

# COMMAND ----------

#exposed dataset
exposed_claims = pc.merge_claims(exposed_subset, claims_df, match_preperiod, match_postperiod)
exposed_claims = pc.pivot_claims(exposed_claims, ['full_claims', 'Emergency Room'])

exposed_subset = exposed_subset.merge(exposed_claims, on=['dw_member_id', 'utc_period'])
exposed_subset.head()

# COMMAND ----------

#control dataset
control_subset_2 = pc.merge_claims(control_subset, claims_df, match_preperiod, match_postperiod)


# COMMAND ----------

control_subset_2 = pc.pivot_claims(control_subset_2, ['full_claims', 'Emergency Room'])
control_subset_2.head()
