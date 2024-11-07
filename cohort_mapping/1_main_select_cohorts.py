# Databricks notebook source
# MAGIC %md
# MAGIC ##Create Datasets for Event Subset and Control Subset
# MAGIC Steps Contained in this Notebook:
# MAGIC   - load and clean data
# MAGIC   - choose 'exposed' / 'control' subsets from Accolade data or Marketscan
# MAGIC   - manipulate matching variables
# MAGIC   - export to Data Catalog
# MAGIC
# MAGIC Remaining Tasks:
# MAGIC   - more flexible outlier handling
# MAGIC   - incorporate Marketscan
# MAGIC   - more dataset analytics
# MAGIC   - change utilization column names up front (categories as well?)
# MAGIC
# MAGIC

# COMMAND ----------

#note: if get deletionVectors error while querying tables, update cluster to one with Databricks Runtime 12.2 LTS - 15.3
from pyspark.sql.functions import to_date
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from src import prep_class
from src import data_class
import pandas as pd

pc = prep_class.Data_Prep()
dec = data_class.Data_Stats()

# COMMAND ----------

#clean window for exposed cohort: what months pre and post should not have other events?
clean_preperiod = 3
clean_postperiod = 0

#evaluation window: in what window do we want to make sure members are fully eligible?
eval_preperiod = 3
eval_postperiod = 5

#cohort matching window: what months should we consider for matching?
match_preperiod = 3
match_postperiod = 5

#claims max: what month is the max to expect correct claim data?
claims_cap = '2024-09-01'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Data

# COMMAND ----------

#pull in event data
event_df = pc.query_data(spark, dbutils, 'cohort_matching_edw_events')
event_df = event_df.withColumn('utc_period', F.to_date(event_df.utc_period, 'yyyyMM'))

# COMMAND ----------

#pull in claims and utilization data
claims_df = pc.query_data(spark, dbutils, 'cohort_matching_cg_claims')
claims_df = claims_df.withColumn('service_month', F.to_date(claims_df.service_month, 'yyyyMM'))

# COMMAND ----------

#pull in member demographic data
mem_df = pc.query_data(spark, dbutils, 'cohort_matching_cg_mem')
mem_df = mem_df.withColumn('start_date', F.trunc(F.to_date(mem_df.start_date, 'yyyyMM'), 'month'))
mem_df = mem_df.withColumn('end_date', F.trunc(F.to_date(mem_df.end_date, 'yyyyMM'), 'month'))
mem_df = mem_df.withColumn('birth_year', F.trunc(F.to_date(mem_df.birth_year, 'yyyyMM'), 'month'))
mem_df = mem_df.dropDuplicates()

# COMMAND ----------

#set some frequently used variables
#this should really be replaced with some useful stats about each
pc.set_event_categories(event_df)
pc.set_util_categories(claims_df)
pc.set_claims_window(claims_df, claims_cap)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Exposed Subset

# COMMAND ----------

#available event categories
print(pc.event_list)

# COMMAND ----------

#select event categories to use in exposed subset
#select categories that should disqualify members from the exposed cohort (within clean window)
exposed_categories = ['High Cost Claimants (HCC)', 'HCC Clinical Eng']
clean_categories = ['Disease Management', 'Treatment Decision Support', 'Maternity Program', 'Case Management']

# COMMAND ----------

#limit exposed cohort to the chosen categories, without other events in clean window
exposed_subset = pc.clean_exposed(spark, event_df, exposed_categories, clean_categories, clean_preperiod, clean_postperiod)

# COMMAND ----------

#add demographics and limit to those eligible for full evaluation window, and within claims data
exposed_subset = pc.limit_exposed(exposed_subset, mem_df, eval_preperiod, eval_postperiod, claims_cap)

# COMMAND ----------

print('Customers: '+ str(exposed_subset.select('edw_cust').distinct().count()))
print('Event sample size:')
exposed_subset.select('category', 'person_id').groupby('category').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Control Subset - from Accolade Members

# COMMAND ----------

#available event categories
print(pc.event_list)

# COMMAND ----------

#select event categories that are ok to have in control, otherwise control will be composed of members without any of the events
control_ok_categories = ['Engaged']

#remove members with events
control_subset = mem_df.join(event_df.filter(~F.col('category').isin(control_ok_categories)), on='person_id', how='left_anti')

# COMMAND ----------

#add months: within claims data, within eligibility window for each member, with buffer for evaluation window
control_subset = pc.generate_control(spark, control_subset, eval_preperiod, eval_postperiod, claims_cap)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Control Subset - from Marketscan Members

# COMMAND ----------

#do this once Marketscan is available

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add claims features
# MAGIC Also adds datatsets together, distinguished by category

# COMMAND ----------

#available claims categories to choose from
print(pc.util_list)

# COMMAND ----------

#select claims categories to add as matching/evaluation variables: these will be added for chosen leading and trialing periods
#leading_list = ['med_allowed', 'pharma_allowed', 'total_allowed', 'Emergency Room']
leading_list = ['med_allowed', 'pharma_allowed', 'Emergency Room', 'Inpatient Medical', 'Inpatient Surgical', 'Office Procedures', 'Outpatient Services', 'Outpatient Urgent Care', 'Physician-PCP Visit', 'Physician-Preventive', 'Physician-Specialist Visit', 'Physician-Telehealth', 'total_allowed']

trailing_list = ['total_allowed']

# COMMAND ----------

#add claims for matching periods to both subsets and combine subsets into one dataset
#also removes members with negative claims or utilization (data error)
for subset in [exposed_subset, control_subset]:
    subset_claims = pc.merge_claims(spark, subset, claims_df, match_preperiod, match_postperiod)
    subset_claims = pc.pivot_claims(subset_claims, leading_list, trailing_list)
    subset_claims = pc.remove_negatives(subset_claims, leading_list, trailing_list)
    subset_joined = subset.join(subset_claims, ['dw_member_id', 'utc_period'], how='inner')

    if subset == exposed_subset:
        combined_cohorts = subset_joined
    else:
        combined_cohorts = combined_cohorts.unionByName(subset_joined)


# COMMAND ----------

print('Customers: '+ str(combined_cohorts.select('edw_cust').distinct().count()))
print('Event sample size:')
combined_cohorts.select('category', 'person_id').groupby('category').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add calculated features
# MAGIC Age, sum of utilization, etc

# COMMAND ----------

#add age
combined_cohorts = pc.calc_age(combined_cohorts)

# COMMAND ----------

#add sums for certain periods: runup, postperiod, etc
combined_cohorts = pc.sum_periods(combined_cohorts, leading_list, -match_preperiod, 0)
combined_cohorts = pc.sum_periods(combined_cohorts, trailing_list, 0, eval_postperiod)

# COMMAND ----------

#combine Utilization
combined_cohorts = combined_cohorts.withColumn('inpatient_-3to0sum', F.col('inpatient_medical_-3to0sum') + F.col('inpatient_surgical_-3to0sum'))
combined_cohorts = combined_cohorts.withColumn('physician_-3to0sum', F.col('physician-pcp_visit_-3to0sum') + F.col('physician-specialist_visit_-3to0sum') + F.col('physician-preventive_-3to0sum'))


# COMMAND ----------

# MAGIC %md
# MAGIC ###Remove Outliers

# COMMAND ----------

#stats for numeric, non-binary columns

#cc_sample = combined_cohorts.limit(100)
#continuous_variables = dec.num_col_stats(cc_sample, threshold_multiplier=3)

continuous_variables = dec.num_col_stats(combined_cohorts, threshold_multiplier=4)
continuous_variables.display()

# COMMAND ----------

#remove outliers
filtered_cohorts = combined_cohorts.filter(combined_cohorts['age']>17)
filtered_cohorts = filtered_cohorts.filter(filtered_cohorts['age']<71)

for x in range(-3, 6):
    filtered_cohorts = filtered_cohorts.filter(filtered_cohorts['total_allowed'+str(x)]<250000)

# COMMAND ----------

filtered_cohorts.select('category', 'person_id').groupby('category').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write data to table

# COMMAND ----------

#write data to table
(
    filtered_cohorts
    .write
    .format("delta")
    .option("overwriteSchema", "true")
    .mode("overwrite")
    .saveAsTable("dev.`clinical-analysis`.cohort_matching_cohorts")
)

# COMMAND ----------

# these lines bring in the code again if updated after original run
# import pyspark.sql.functions as F
# import importlib
# import src.prep_class as prep_class

# importlib.reload(prep_class)
# pc = prep_class.Data_Prep()
