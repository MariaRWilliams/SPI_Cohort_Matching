# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Matching Model
# MAGIC Steps contained in this Notebook:
# MAGIC - load cohort data and display stats
# MAGIC - prepare matching variables (scale, encode, etc)
# MAGIC - run matching algorithm
# MAGIC - export matched dataset with matching variables to data catalog

# COMMAND ----------

# MAGIC %pip install faiss-cpu

# COMMAND ----------

from src import prep_class
from src import matching_class
import pyspark.sql.functions as F
import pyspark.sql.types as T

mc = matching_class.Cohort_Matching()
pc = prep_class.Data_Prep()

# COMMAND ----------

full_df = pc.query_data(spark, dbutils, 'cohort_matching_cohorts')

# COMMAND ----------

#stats: what else would be useful? do both before and after
full_df.groupby('category').agg(F.count('person_id').alias('member_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at period 0'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min_period'),
                                F.max('utc_period').alias('max_period'),
                                ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Choose / Prepare Variables

# COMMAND ----------

#available variables
full_df.columns

# COMMAND ----------

#curently, the binary columns are used for the index and the scale columns are used for similarity matching
#in the future, may need to separate lists of columns to scale/encode and list of columns to index/match
mc.id_columns = ['person_id', 'category', 'utc_period']

#select variables used for indexing (perfect match)
mc.binary_columns = []
mc.to_binary_columns = ['edw_cust']

#select variables used for closest match
mc.scale_columns = ['total_allowed0',
                    'total_allowed-1',
                    'total_allowed-2',
                    'total_allowed-3',
                    'age',
                    'inpatient_-3to0sum',
                    'emergency_room_-3to0sum',
                    'physician_-3to0sum',
                    'date_int',
                    'cancer',
                    'hyperlipidemia',
                    'diabetes',
                    'osteoarthritis',
                    'depression'
                ]

#dictionary of weights (weighted after scaling)
mc.weights = {'total_allowed0': 5,
              'total_allowed-1': 3,
              'total_allowed-2': 2,
              'total_allowed-3': 1}

mc.final_columns = mc.id_columns + mc.binary_columns + mc.scale_columns + mc.to_binary_columns

# COMMAND ----------

ready_df = mc.full_transformation(full_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Matching Algorithm: Setup

# COMMAND ----------

#analysis variables
mc.num_possible_matches = 10
mc.num_final_matches = 1

#model variables
#nlist = the number of cells to cluster the control into (4 * sqrt(n) is standard?)
#nprobe = the number of cells to check for the nearest neighbors
#max_distance = (look into this one- what distance does FAISS return? euclidian?)
mc.n_list = 50
mc.n_probe = 10
mc.max_distance = 50

# COMMAND ----------

#possible exposed cohorts
print(ready_df.select('category').distinct().toPandas()['category'].to_list())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Matching Algorithm: Loop
# MAGIC 1) An index is created of the control records that match exactly on the relevant variables
# MAGIC 2) That index is searched and returns a set of closest matches for each exposed record
# MAGIC 3) Each set is filtered until each exposed record has a set of close matches unique to itself
# MAGIC 4) Matched records are recorded and loop continues for next set of exact matches

# COMMAND ----------

#reload
# import importlib
# from src import matching_class

# importlib.reload(matching_class)
# mc = matching_class.Cohort_Matching()

# COMMAND ----------

#for cohort in ready_df.select('category').distinct().toPandas()['category'].to_list():
cohort = 'High Cost Claimants (HCC)'

# COMMAND ----------

#get columns
mc.fixed_cols = list(set(ready_df.columns) - set(mc.id_columns) - set(mc.scale_columns))

#loop through fixed sets
final_matched = mc.main_match(spark, cohort, full_df, ready_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Final Steps: Statistics Overview and Export

# COMMAND ----------

#check sample
sample_exposed_df, sample_control_df = mc.sample_matches(final_matched, 3)
sample_exposed_df.display()
sample_control_df.display()

# COMMAND ----------

final_matched.groupby('category').agg(F.count('person_id').alias('count'), 
                                F.round(F.sum('cancer')).alias('members with cancer'), 
                                F.round(F.sum('diabetes')).alias('members with diabetes'), 
                                F.round(F.mean('total_allowed-1'), 2).alias('avg spend at period -1'), 
                                F.round(F.mean('age'), 2).alias('avg age')
                                ).display()

# COMMAND ----------

#add to final table
# (
#     final_matched
#     .write
#     .format("delta")
#     .option("overwriteSchema", "true")
#     .mode("append")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_cohorts_matched")
# )


# COMMAND ----------

# check_df = pc.query_data(spark, dbutils, 'cohort_matching_cohorts_matched')
# check_df.count()

# COMMAND ----------

#write data to table
# (
#     final_matched
#     .write
#     .format("delta")
#     .option("overwriteSchema", "true")
#     .mode("overwrite")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_cohorts_matched")
# )
