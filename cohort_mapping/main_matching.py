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

#get event data
#note: if get deletionVectors error, update cluster to one with Databricks Runtime 12.2 LTS - 15.3
event_df = (
    spark
    .sql("SELECT * FROM dev.`clinical-analysis`.cohort_matching_edw_events")
)

event_df = event_df.toPandas()
event_df['utc_period'] = pd.to_datetime(event_df['utc_period'].astype(str), format='%Y%m')
pc.set_categories(event_df)

print(pc.category_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Exposed Subset

# COMMAND ----------

#limit exposed cohort to only those without other events in period window

preperiod = 3
postperiod = 0

exposed_subset = event_df[event_df['category']!='exclude'].copy()
exposed_subset = pc.clean_exposed(exposed_subset, preperiod, postperiod)

# COMMAND ----------

#print(exposed_subset.head()
print('Customers: '+ str(len(exposed_subset['org_nm'].unique().tolist())))
print('Event sample size:')
print(exposed_subset[['category', 'person_id']].groupby('category').count().reset_index())

# COMMAND ----------

exposed_subset.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Control Subset - from Accolade Members

# COMMAND ----------

#get member data
#note: if get deletionVectors error, update cluster to one with Databricks Runtime 12.2 LTS - 15.3
mem_df = (
    spark
    .sql("SELECT * FROM dev.`clinical-analysis`.cohort_matching_cg_mem")
)

mem_df = mem_df.toPandas()
mem_df['start_date'] = pd.to_datetime(mem_df['start_date'])
mem_df['end_date'] = pd.to_datetime(mem_df['end_date'])
mem_df.head()


# COMMAND ----------

#remove members with events
control_subset = mem_df[~mem_df['person_id'].isin(event_df['person_id'])]

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Control Subset - from Marketscan Members

# COMMAND ----------

#do this once Marketscan is available

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add features and prepare for matching

# COMMAND ----------

#pull in claims and utilization data
claims_df = (
    spark
    .sql("SELECT * FROM dev.`clinical-analysis`.cohort_matching_cg_claims")
)

claims_df = claims_df.toPandas()
claims_df['service_month'] = pd.to_datetime(claims_df['service_month'].astype(str), format='%Y%m')
claims_df.head()

# COMMAND ----------

#Exposed dataset: collect member-level variables
exposed_subset = mem_df.merge(exposed_subset, on='person_id', how = 'inner')
exposed_subset = exposed_subset[(exposed_subset['utc_period']>=exposed_subset['start_date']) &
                                (exposed_subset['utc_period']<=exposed_subset['end_date'])]

exposed_subset.head()

# COMMAND ----------

#these lines bring in the code again if updated after original run
import importlib
import src.prep_class as prep_class
importlib.reload(prep_class)
pc = prep_class.Data_Prep()

# COMMAND ----------

#add matching claims
preperiod = 3
postperiod = 0
ex_sub_c = pc.merge_claims_exp(exposed_subset, claims_df, preperiod, postperiod)

ex_sub_c.head()

# COMMAND ----------

#pivot
ex_sub_c_2 = pd.pivot_table(ex_sub_c, values=['med_allowed'], index=['dw_member_id', 'utc_period'], columns=['mo_seq']).reset_index()



# COMMAND ----------

ex_sub_c_2[ex_sub_c_2['dw_member_id']=='1c7ff8e7b62a91c516fa547ab83c5b43']

# COMMAND ----------

#Control dataset: add claims, then create instances of possible windows
