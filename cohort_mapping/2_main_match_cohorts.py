# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Matching Model
# MAGIC Steps contained in this Notebook:
# MAGIC - load cohort data and display stats
# MAGIC - prepare matching variables (scale, encode, etc)
# MAGIC - run matching algorithm
# MAGIC - export matched dataset with matching variables to data catalog
# MAGIC
# MAGIC Pending Updates:
# MAGIC - be able to restrict to perfect matches on chosen variables
# MAGIC - analyse more than one cohort
# MAGIC - resolve kernel crashing on many columns

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
full_df.groupby('category').agg(F.count('person_id').alias('count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at period 0'), 
                                F.round(F.mean('age'), 2).alias('avg age')
                                ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Choose / Prepare Variables

# COMMAND ----------

#available variables
full_df.columns

# COMMAND ----------

#select all variables to be used for matching
id_columns = ['person_id', 'category', 'utc_period']
binary_columns = ['depression', 'hyperlipidemia', 'osteoarthritis', 'chf', 'cancer', 'diabetes', 'cad', 'copd']
scale_columns = ['age',
                'total_allowed-1',
                'total_allowed-2',
                'total_allowed-3',
                'total_allowed0',
                'med_allowed_-3to0sum',
                'pharma_allowed_-3to0sum',
                'total_allowed_-3to0sum',
                'emergency_room_-3to0sum'
                ]
to_binary_columns = ['sex', 'region']

final_columns = id_columns + binary_columns + scale_columns + to_binary_columns


# COMMAND ----------

ready_df = mc.full_transformation(id_columns, binary_columns, scale_columns, to_binary_columns, full_df)

# COMMAND ----------

cols = list(set(ready_df.columns) - set(id_columns))
# print(cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Matching Algorithm: Create Control Index

# COMMAND ----------

#analysis variables
num_possible_matches = 10
num_final_matches = 2

#model variables
#nlist = the number of cells to cluster the control into (4 * sqrt(n) is standard?)
#nprobe = the number of cells to check for the nearest neighbors
#max_distance = (look into this one- what distance does FAISS return? euclidian?)
n_list = 5000
n_probe = 2500
max_distance = 5

# COMMAND ----------

#pull out control - may need to optimize since many rows seem to crash kernel
control_df = ready_df.filter(ready_df['category']=='control').toPandas()
control_ids = control_df[id_columns]
control_vars = control_df[cols]

# COMMAND ----------

#create index of control members
index = mc.create_index(control_vars, n_list)
#mc.search_index_test(index, control_vars, num_possible_matches)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Matching Algorithm: Cohort Similarity Search

# COMMAND ----------

#possible exposed cohorts
print(ready_df.select('category').distinct().toPandas()['category'].to_list())

# COMMAND ----------

#seemed better to process one cohort at a time
cohort = 'HCC Clinical Engagement'
exp_df = ready_df.filter(ready_df['category']==cohort).toPandas()
exp_ids = exp_df[id_columns]
exp_vars = exp_df[cols]

# COMMAND ----------

#collect matches
distances, neighbor_indexes = mc.search_index(index, exp_vars, num_possible_matches, n_probe)
matched_record = mc.pick_matches(distances, neighbor_indexes, exp_vars, max_distance, num_final_matches)
exposed_matched, control_matched = mc.tag_matches(matched_record, control_ids, exp_ids, num_final_matches)

# COMMAND ----------

#switch back to spark
matched_record = spark.createDataFrame(matched_record)
exposed_matched = spark.createDataFrame(exposed_matched)
control_matched = spark.createDataFrame(control_matched)

# COMMAND ----------

#detail matches
final_matched = mc.detail_matches(spark, matched_record, exposed_matched, control_matched, full_df, final_columns, id_columns, num_final_matches)

# COMMAND ----------

#check sample
sample_exposed_df, sample_control_df = mc.sample_matches(final_matched, 3)
sample_exposed_df.display()
sample_control_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Final Steps: Statistics Overview and Export

# COMMAND ----------

final_matched.groupby('category').agg(F.count('person_id').alias('count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at period 0'), 
                                F.round(F.mean('age'), 2).alias('avg age')
                                ).show()

# COMMAND ----------

#add to final table


# COMMAND ----------

#write data to table
(
    final_matched
    .write
    .format("delta")
    .option("overwriteSchema", "true")
    .mode("overwrite")
    .saveAsTable("dev.`clinical-analysis`.cohort_matching_cohorts_matched")
)

# COMMAND ----------

#reload
# import importlib
# from src import matching_class

# importlib.reload(matching_class)
# mc = matching_class.Cohort_Matching()
