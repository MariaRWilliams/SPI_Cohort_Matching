# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##Create Datasets for Event Subset and Control Subset
# MAGIC Steps Contained in this Notebook:
# MAGIC    - load and clean data
# MAGIC    - choose 'exposed' / 'control' subsets from Accolade data or Marketscan
# MAGIC    - manipulate matching variables
# MAGIC    - export to Data Catalog
# MAGIC
# MAGIC Remaining Tasks:
# MAGIC    - more flexible outlier handling
# MAGIC    - incorporate Marketscan
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

#claims max: what month is the max to expect correct claim data? (last claims pull: 12-03-24)
claims_cap = '2024-11-01'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Data

# COMMAND ----------

#pull in event data
event_df = dec.query_data(spark, dbutils, 'cohort_matching_edw_events')
event_df = event_df.withColumn('utc_period', F.to_date(event_df.utc_period, 'yyyyMM'))

# COMMAND ----------

#pull in Cedargate claims and utilization data
claims_df = dec.query_data(spark, dbutils, 'cohort_matching_cg_claims')
claims_df = claims_df.withColumn('service_month', F.to_date(claims_df.service_month, 'yyyyMM'))

# COMMAND ----------

#pull in Marketscan claims and utilization data
ms_claims_df = dec.query_data(spark, dbutils, 'cohort_matching_ms_claims')
ms_claims_df = ms_claims_df.withColumn('service_month', F.to_date(ms_claims_df.service_month, 'yyyyMM'))

# COMMAND ----------

#pull in Cedargate member demographic data
mem_df = dec.query_data(spark, dbutils, 'cohort_matching_cg_mem')
mem_df = mem_df.withColumn('start_date', F.trunc(F.to_date(mem_df.start_date, 'yyyyMM'), 'month'))
mem_df = mem_df.withColumn('end_date', F.trunc(F.to_date(mem_df.end_date, 'yyyyMM'), 'month'))
mem_df = mem_df.withColumn('birth_year', F.trunc(F.to_date(mem_df.birth_year, 'yyyy'), 'year'))
mem_df = mem_df.dropDuplicates()

# COMMAND ----------

#pull in Marketscan member demographic data
ms_mem_df = dec.query_data(spark, dbutils, 'cohort_matching_ms_mem')
ms_mem_df = ms_mem_df.withColumn('start_date', F.trunc(F.to_date(ms_mem_df.start_date, 'yyyyMM'), 'month'))
ms_mem_df = ms_mem_df.withColumn('end_date', F.trunc(F.to_date(ms_mem_df.end_date, 'yyyyMM'), 'month'))
ms_mem_df = ms_mem_df.withColumn('birth_year', F.trunc(F.to_date(ms_mem_df.birth_year, 'yyyy'), 'year'))
ms_mem_df = ms_mem_df.dropDuplicates()

# COMMAND ----------

#pull in Cedargate member chronic conditions data
chron_df = dec.query_data(spark, dbutils, 'cohort_matching_cg_chron')

#de-duplicate
exprs = {x: "max" for x in chron_df.columns if x != 'member_id' and x != 'cal_year' }
chron_df = chron_df.groupBy('member_id').agg(exprs)
chron_df = chron_df.select(*[F.col(c).alias(c.replace('max(','')) for c in chron_df.columns])
chron_df = chron_df.select(*[F.col(c).alias(c.replace(')','')) for c in chron_df.columns])

# COMMAND ----------

#pull in Marketscan member chronic conditions data
ms_chron_df = dec.query_data(spark, dbutils, 'cohort_matching_cg_chron')

#de-duplicate
exprs = {x: "max" for x in chron_df.columns if x != 'member_id' and x != 'cal_year' }
chron_df = chron_df.groupBy('member_id').agg(exprs)
chron_df = chron_df.select(*[F.col(c).alias(c.replace('max(','')) for c in chron_df.columns])
chron_df = chron_df.select(*[F.col(c).alias(c.replace(')','')) for c in chron_df.columns])


# COMMAND ----------

#set some frequently used variables
#this should really be replaced with some useful stats about each
pc.set_event_categories(event_df)
pc.set_util_categories(claims_df)
pc.set_cg_claims_window(claims_df, claims_cap)
pc.set_ms_claims_window(ms_claims_df)

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
exposed_categories = ['Care Navigation']
clean_categories = ['Case Management', 'Maternity']

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

# MAGIC %md
# MAGIC ###Select Control Subset - from Cedargate/Accolade Members
# MAGIC Note: Current, members may be represented in the control group AND the cohort group, as long as they are represented for claims periods that do not overlap. If cohort members are excluded from the the control group completely, there will not be enough control members for meaningful matching.

# COMMAND ----------

# #limit to same customers as exposed
# cust_list_2 = exposed_subset.select('edw_cust').distinct().toPandas().edw_cust.tolist()
# control_subset = mem_df.filter(F.col('edw_cust').isin(cust_list_2))

# #if the person_id starts with DW, discard (not able to link to event data)
# control_subset = control_subset.filter(~F.col('person_id').startswith('DW'))

# COMMAND ----------

# #add months: within claims data, within eligibility window for each member, with buffer for evaluation window
# control_subset = pc.generate_control(spark, control_subset, eval_preperiod, eval_postperiod, 'CG')

# COMMAND ----------

# filter_cat = exposed_categories + clean_categories

# #filter out member months that overlap cohort
# #for x in range(-(eval_preperiod+eval_postperiod), eval_preperiod+eval_postperiod+1):

# #filter out member months that overlap leading periods of cohort
# for x in range(eval_preperiod+eval_postperiod+1):
#     control_subset = control_subset.join(event_df.filter(event_df.category.isin(filter_cat)).withColumn('utc_period', F.add_months(event_df.utc_period, x)).select('person_id', 'utc_period'), on=(['person_id', 'utc_period']), how='left_anti')


# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Control Subset - from Marketscan Members

# COMMAND ----------

#add months: within claims data, within eligibility window for each member, with buffer for evaluation window
control_subset = pc.generate_control(spark, ms_mem_df, eval_preperiod, eval_postperiod, 'MS')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add claims features
# MAGIC Also adds datatsets together, distinguished by category.
# MAGIC No longer compatible with accolade control

# COMMAND ----------

#available claims categories to choose from
print(pc.util_list)

# COMMAND ----------

#select claims categories to add as matching/evaluation variables: these will be added for chosen leading and trialing periods
leading_list = ['med_total', 'med_total_net', 'pharma_total', 'pharma_total_net', 'total_claims', 'total_claims_net', 'avoidable_er', 'readmission', 'office_visits', 'er', 'inpatient']

trailing_list = ['total_claims']

# COMMAND ----------

#exposed subset: add matching periods and chronic conditions
exp_claims = pc.merge_claims(spark, exposed_subset, claims_df, match_preperiod, match_postperiod)
exp_claims = pc.pivot_claims(exp_claims, leading_list, trailing_list)
exp_claims = pc.remove_negatives(exp_claims, leading_list, trailing_list)

exp_joined = exposed_subset.join(exp_claims, ['member_id', 'utc_period'], how='inner')
exp_joined = exp_joined.join(chron_df, ['member_id'], how='left').fillna(0)

# COMMAND ----------

#control subset (ms): add matching periods and chronic conditions
ctr_claims = pc.merge_claims(spark, control_subset, ms_claims_df, match_preperiod, match_postperiod)
ctr_claims = pc.pivot_claims(ctr_claims, leading_list, trailing_list)
ctr_claims = pc.remove_negatives(ctr_claims, leading_list, trailing_list)

ctr_joined = control_subset.join(ctr_claims, ['member_id', 'utc_period'], how='inner')
ctr_joined = ctr_joined.join(ms_chron_df, ['member_id'], how='left').fillna(0)

# COMMAND ----------

#union together
ctr_joined = ctr_joined.drop('state') #this one fixed in ms ingestion
ctr_joined = ctr_joined.withColumn('state_abr', F.col('state_abv')).drop('state_abv') #this one fixed in ms ingestion
ctr_joined = ctr_joined.drop('msa_5')

exp_joined = exp_joined.select(ctr_joined.columns)
combined_cohorts = exp_joined.unionByName(ctr_joined).cache()

# COMMAND ----------

#print('Customers: '+ str(combined_cohorts.select('edw_cust').distinct().count()))
# print(combined_cohorts.select(F.collect_set('edw_cust').alias('edw_cust')).first()['edw_cust'])
# print('Event sample size:')
# combined_cohorts.select('category', 'member_id').groupby('category').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add calculated features

# COMMAND ----------

#add age (exact age and age band)
combined_cohorts = pc.calc_age(combined_cohorts)

# COMMAND ----------

#add sums for certain periods: runup, postperiod, etc
combined_cohorts = pc.sum_periods(combined_cohorts, leading_list, -match_preperiod, 0)
combined_cohorts = pc.sum_periods(combined_cohorts, trailing_list, 0, eval_postperiod)

# COMMAND ----------

#add date as a scalable integer
combined_cohorts = combined_cohorts.withColumn('date_int', F.round(F.unix_timestamp('utc_period'), 0))

# COMMAND ----------

#update zip code
#combined_cohorts = combined_cohorts.withColumn('zip_code', F.col('zip_code').cast(T.IntegerType())).fillna(0)

# COMMAND ----------

#percent of pre-intervention spend that is medical
combined_cohorts = combined_cohorts.withColumn('med_percent', 
                             F.round(F.when(F.col('med_total_-3to0sum') > 0, F.col('med_total_-3to0sum') / F.col('total_claims_-3to0sum')).otherwise(0), 2))
#categories
combined_cohorts = combined_cohorts.withColumn('med_percent_cat', F.when(F.col('med_percent') < 0.5, '0-50%_med')
                                                .otherwise(F.when(F.col('med_percent') < 0.9, '50-90%_med').otherwise('mostly_med')))

#percent of spend that is out of pocket
combined_cohorts = combined_cohorts.withColumn('oop_percent', 
                             F.round(F.when(F.col('total_claims_net_-3to0sum') > 0, F.col('total_claims_net_-3to0sum') / F.col('total_claims_-3to0sum')).otherwise(0), 2))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Some Data Cleaning

# COMMAND ----------

combined_cohorts = combined_cohorts.distinct().fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Remove Outliers

# COMMAND ----------

# #stats for numeric, non-binary columns
# continuous_variables = dec.num_col_stats(combined_cohorts, threshold_multiplier=4)
# continuous_variables.display()

# COMMAND ----------

#remove outliers: currently based on original specifications
filtered_cohorts = combined_cohorts.filter(combined_cohorts['age']>17)
filtered_cohorts = filtered_cohorts.filter(filtered_cohorts['age']<71)

for x in range(-eval_preperiod, eval_postperiod+1):
    filtered_cohorts = filtered_cohorts.filter(filtered_cohorts['total_claims'+str(x)]<250000)

filtered_cohorts = filtered_cohorts.filter(filtered_cohorts['total_claims_0to5sum']<250000)
#filtered_cohorts = filtered_cohorts.filter(filtered_cohorts['total_claims_0to2sum']<125000)
#filtered_cohorts = filtered_cohorts.filter(filtered_cohorts['total_claims_0to11sum']<500000)
filtered_cohorts = filtered_cohorts.distinct()

# COMMAND ----------

filtered_cohorts.select('category', 'member_id').groupby('category').count().show()

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
