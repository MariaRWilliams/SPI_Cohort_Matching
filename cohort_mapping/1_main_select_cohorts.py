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
dec = data_class.Data_Processing()

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
event_df = dec.query_data(spark, dbutils, 'cohort_matching_edw_events')
event_df = event_df.withColumn('utc_period', F.to_date(event_df.utc_period, 'yyyyMM'))

# COMMAND ----------

#pull in claims and utilization data
claims_df = dec.query_data(spark, dbutils, 'cohort_matching_cg_claims')
claims_df = claims_df.withColumn('service_month', F.to_date(claims_df.service_month, 'yyyyMM'))

# COMMAND ----------

#pull in member demographic data
mem_df = dec.query_data(spark, dbutils, 'cohort_matching_cg_mem')
mem_df = mem_df.withColumn('start_date', F.trunc(F.to_date(mem_df.start_date, 'yyyyMM'), 'month'))
mem_df = mem_df.withColumn('end_date', F.trunc(F.to_date(mem_df.end_date, 'yyyyMM'), 'month'))
mem_df = mem_df.withColumn('birth_year', F.trunc(F.to_date(mem_df.birth_year, 'yyyyMM'), 'month'))
mem_df = mem_df.dropDuplicates()

# COMMAND ----------

#pull in member chronic conditions data
chron_df = dec.query_data(spark, dbutils, 'cohort_matching_cg_chron')

#de-duplicate
exprs = {x: "max" for x in chron_df.columns if x != 'dw_member_id' and x != 'cal_year' and x != 'table_schema'}
chron_df = chron_df.groupBy('dw_member_id').agg(exprs)
chron_df = chron_df.select(*[F.col(c).alias(c.replace('max(','')) for c in chron_df.columns])
chron_df = chron_df.select(*[F.col(c).alias(c.replace(')','')) for c in chron_df.columns])

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

#limit customers - this should be updated so that we can use product data, once product data is available
cust_list = ['ADVANTAGE SOLUTIONS','AMERICAN AIRLINES', 'BANK OF NEW YORK MELLON',
                                            'BENEFITS4ME', 'CALIFORNIA ASSOCIATION OF HIGHWAY PATROLMEN','CBIZ',
                                            'CHOCTAW NATION','CITY OF SEATTLE','COMMSCOPE','COUNTY OF SANTA BARBARA',
                                            'CSL BEHRING','DUPONT','FIDELITY','FIRST AMERICAN FINANCIAL','GENERAL MILLS',
                                            'GENTIVA','GREIF INC','HYATT HOTEL CORPORATION','INTERNATIONAL PAPER',
                                            'INTUITIVE SURGICAL','L3HARRIS','LANDRYS INC',"LOWE'S",'MCKESSON','META',
                                            'MOFFITT CANCER CENTER','NEVADA GOLD MINES','PAYPAL','PERATON','SAFRAN USA',
                                            'SAN FRANCISCO HEALTH SERVICE SYSTEM','SEDGWICK','SEVEN ELEVEN','STATE FARM',
                                            'UNIVERSITY OF CALIFORNIA']

# COMMAND ----------

#available event categories
print(pc.event_list)

# COMMAND ----------

#select event categories to use in exposed subset
#select categories that should disqualify members from the exposed cohort (within clean window)
exposed_categories = ['High Cost Claimants (HCC)']
clean_categories = ['Disease Management', 'Treatment Decision Support', 'Maternity Program', 'Case Management', 'Transition Care - Adult']

# COMMAND ----------

#limit exposed cohort to the chosen categories, without other events in clean window
exposed_subset = pc.clean_exposed(spark, event_df, exposed_categories, clean_categories, clean_preperiod, clean_postperiod)

# COMMAND ----------

#add demographics and limit to those eligible for full evaluation window, and within claims data
exposed_subset = pc.limit_exposed(exposed_subset, mem_df, eval_preperiod, eval_postperiod, claims_cap)

# COMMAND ----------

#limit to those from specific customers
exposed_subset = exposed_subset.filter(F.col('edw_cust').isin(cust_list))

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
control_ok_categories = ['Care Navigation', 'Transition Care - Adult', 'Preventive Care', 'HCC Clinical Eng', 'Engaged', 'Rising Risk', 'Case Management - High Risk Maternity', 'Wellness Care', 'Case Management - Oncology', 'Case Management - Adult']

#remove members with events
control_subset = mem_df.join(event_df.filter(~F.col('category').isin(control_ok_categories)), on='person_id', how='left_anti')

#limit to same customers as exposed
control_subset = control_subset.filter(F.col('edw_cust').isin(cust_list))

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
#crashes cluster sometimes... don't know why

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
# MAGIC ###Add chronic conditions and calculated features

# COMMAND ----------

#add chronic conditions
combined_cohorts = combined_cohorts.join(chron_df, ['dw_member_id'], how='left').fillna(0)

# COMMAND ----------

#add age (exact age and age band)
combined_cohorts = pc.calc_age(combined_cohorts)

# COMMAND ----------

#add sums for certain periods: runup, postperiod, etc
combined_cohorts = pc.sum_periods(combined_cohorts, leading_list, -match_preperiod, 0)
combined_cohorts = pc.sum_periods(combined_cohorts, trailing_list, 0, eval_postperiod)

# COMMAND ----------

#combine utilization
combined_cohorts = combined_cohorts.withColumn('inpatient_-3to0sum', F.col('inpatient_medical_-3to0sum') + F.col('inpatient_surgical_-3to0sum'))
combined_cohorts = combined_cohorts.withColumn('physician_-3to0sum', F.col('physician-pcp_visit_-3to0sum') + F.col('physician-specialist_visit_-3to0sum') + F.col('physician-preventive_-3to0sum'))


# COMMAND ----------

#add date as a scalable integer
combined_cohorts = combined_cohorts.withColumn('date_int', F.round(F.unix_timestamp('utc_period'), 0))

# COMMAND ----------

#update zip code
combined_cohorts = combined_cohorts.withColumn('zip_code', F.col('zip_code').cast(T.IntegerType())).fillna(0)

# COMMAND ----------

#add spend pattern indicators
#percent medical, percent increase and decrease
for x in range(-2, 2):
  #dollar value change from previous month
  combined_cohorts = combined_cohorts.withColumn('spend_increase'+str(x), F.round(F.when(F.col('total_allowed'+str(x))>0, F.col('total_allowed'+str(x))).otherwise(0) - 
                                            F.when(F.col('total_allowed'+str(x-1))>0, F.col('total_allowed'+str(x-1))).otherwise(0), 2))

  #change from previous month as a percent of total pre-intervention
  combined_cohorts = combined_cohorts.withColumn('spend_increase_perc'+str(x), F.round( (F.when(F.col('total_allowed'+str(x))>0, F.col('total_allowed'+str(x))).otherwise(0) - 
                                            F.when(F.col('total_allowed'+str(x-1))>0, F.col('total_allowed'+str(x-1))).otherwise(0)) / F.col('total_allowed_-3to0sum'), 2))

for x in range(-3, 1):
  #%spend
  combined_cohorts = combined_cohorts.withColumn('spend_perc'+str(x), F.round( (F.when(F.col('total_allowed'+str(x))>0, F.col('total_allowed'+str(x)) / F.col('total_allowed_-3to0sum')).otherwise(0) ), 2))

#percent of pre-intervention spend that is medical
combined_cohorts = combined_cohorts.withColumn('med_percent', 
                             F.round(F.when(F.col('med_allowed_-3to0sum') > 0, F.col('med_allowed_-3to0sum') / F.col('total_allowed_-3to0sum')).otherwise(0), 2))
#categories
combined_cohorts = combined_cohorts.withColumn('med_percent_cat', F.when(F.col('med_percent') < 0.5, '0-50%_med')
                                                .otherwise(F.when(F.col('med_percent') < 0.9, '50-90%_med').otherwise('mostly_med')))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Some Data Cleaning

# COMMAND ----------

#if the person_id starts with DW, then there is a risk the id is not unique, so replace with dw_member_id
combined_cohorts = combined_cohorts.withColumn('person_id', F.when(F.col('person_id').startswith('DW'), F.col('dw_member_id')).otherwise(F.col('person_id')))

# COMMAND ----------

combined_cohorts = combined_cohorts.distinct().fillna(0)
combined_cohorts = combined_cohorts.distinct()

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

#remove outliers: currently based on original specifications
filtered_cohorts = combined_cohorts.filter(combined_cohorts['age']>17)
filtered_cohorts = filtered_cohorts.filter(filtered_cohorts['age']<71)

# for x in range(-3, 6):
#     filtered_cohorts = filtered_cohorts.filter(filtered_cohorts['total_allowed'+str(x)]<250000)

# filtered_cohorts = filtered_cohorts.filter(filtered_cohorts['total_allowed_0to5sum']<250000)
# filtered_cohorts = filtered_cohorts.distinct()

# COMMAND ----------

filtered_cohorts.select('category', 'person_id').groupby('category').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write data to table

# COMMAND ----------

#write data to table
# (
#     filtered_cohorts
#     .write
#     .format("delta")
#     .option("overwriteSchema", "true")
#     .mode("overwrite")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_cohorts")
# )

# COMMAND ----------

# these lines bring in the code again if updated after original run
# import pyspark.sql.functions as F
# import importlib
# import src.prep_class as prep_class

# importlib.reload(prep_class)
# pc = prep_class.Data_Prep()
