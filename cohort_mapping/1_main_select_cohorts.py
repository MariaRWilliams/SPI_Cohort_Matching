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

#pull in claims and utilization data
claims_df = dec.query_data(spark, dbutils, 'cohort_matching_cg_claims')
claims_df = claims_df.withColumn('service_month', F.to_date(claims_df.service_month, 'yyyyMM'))

# COMMAND ----------

#pull in member demographic data
mem_df = dec.query_data(spark, dbutils, 'cohort_matching_cg_mem')
mem_df = mem_df.withColumn('start_date', F.trunc(F.to_date(mem_df.start_date, 'yyyyMM'), 'month'))
mem_df = mem_df.withColumn('end_date', F.trunc(F.to_date(mem_df.end_date, 'yyyyMM'), 'month'))
mem_df = mem_df.withColumn('birth_year', F.trunc(F.to_date(mem_df.birth_year, 'yyyy'), 'year'))
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
exposed_categories = ['Carrot', 'Kindbody', 'WellRight', 'FOLX Health', 'Carrum Health', 'Rx Savings Solutions', 'Cylinder', 'Lantern', 'Headspace Care', 'Virta Health', 'Hinge Health', 'Sword', 'Equip Health', 'Lyra']
clean_categories = ['Case Management', 'Transition Care - Adult', 'Rising Risk', 'Case Management - High Risk Maternity', 'Case Management - Oncology', 'Case Management - Adult', 'Maternity']

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
# MAGIC Note: Current, members may be represented in the control group AND the cohort group, as long as they are represented for claims periods that do not overlap. If cohort members are excluded from the the control group completely, there will not be enough control members for meaningful matching.

# COMMAND ----------

#limit to same customers as exposed
cust_list_2 = exposed_subset.select('edw_cust').distinct().toPandas().edw_cust.tolist()
control_subset = mem_df.filter(F.col('edw_cust').isin(cust_list_2))

#if the person_id starts with DW, discard (not able to link to event data)
control_subset = control_subset.filter(~F.col('person_id').startswith('DW'))

# COMMAND ----------

#add months: within claims data, within eligibility window for each member, with buffer for evaluation window
control_subset = pc.generate_control(spark, control_subset, eval_preperiod, eval_postperiod, claims_cap)

# COMMAND ----------

filter_cat = exposed_categories + clean_categories

#filter out member months that overlap cohort
#for x in range(-(eval_preperiod+eval_postperiod), eval_preperiod+eval_postperiod+1):

#filter out member months that overlap leading periods of cohort
for x in range(eval_preperiod+eval_postperiod+1):
    # print(x)
    # print(control_subset.count())
    control_subset = control_subset.join(event_df.filter(event_df.category.isin(filter_cat)).withColumn('utc_period', F.add_months(event_df.utc_period, x)).select('person_id', 'utc_period'), on=(['person_id', 'utc_period']), how='left_anti')


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
leading_list = ['med_total', 'med_total_net', 'pharma_total', 'pharma_total_net', 'er_visits', 'avoidable_er_visits', 'ip_admits', 'ip_readmits', 'ip_er_admits', 'op_surgery', 'office_visits', 'uc_visits', 'total_claims', 'total_claims_net']

trailing_list = ['total_claims', 'avoidable_er_visits', 'er_visits', 'ip_readmits', 'ip_admits', 'op_surgery']

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
# print(combined_cohorts.select(F.collect_set('edw_cust').alias('edw_cust')).first()['edw_cust'])
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

#add date as a scalable integer
combined_cohorts = combined_cohorts.withColumn('date_int', F.round(F.unix_timestamp('utc_period'), 0))

# COMMAND ----------

#update zip code
combined_cohorts = combined_cohorts.withColumn('zip_code', F.col('zip_code').cast(T.IntegerType())).fillna(0)

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

#stats for numeric, non-binary columns

#cc_sample = combined_cohorts.limit(100)
#continuous_variables = dec.num_col_stats(cc_sample, threshold_multiplier=3)

continuous_variables = dec.num_col_stats(combined_cohorts, threshold_multiplier=4)
continuous_variables.display()

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
