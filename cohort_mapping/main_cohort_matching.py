# Databricks notebook source
# MAGIC %md
# MAGIC ###Run Matching Model
# MAGIC - load cohorts and display stats
# MAGIC - choose matching variables
# MAGIC - scale, etc
# MAGIC - run matching algorithm
# MAGIC - export matching index

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

#in each category: count of members, mean value, stddev, min, max of total_allowed?
#full_df.columns
print('Category sample size:')
full_df.select('category', 'person_id').groupby('category').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Choose / Prepare Variables

# COMMAND ----------

full_df.columns

# COMMAND ----------

#select all columns used for matching here
id_columns = ['person_id', 'category', 'utc_period']
binary_columns = ['depression', 'hyperlipidemia', 'osteoarthritis', 'chf', 'cancer', 'diabetes', 'cad', 'copd']
scale_columns = ['total_allowed-1', 'total_allowed-2', 'total_allowed-3', 'total_allowed0', 'total_allowed_0to5sum', 'age']
to_binary_columns = ['sex']


# COMMAND ----------

#change scale columns to vector and scale
df_with_vector = mc.create_vector(full_df, scale_columns)
full_df_scaled = mc.scale_vector(df_with_vector)
full_df_scaled = mc.unpack_vector(full_df_scaled, id_columns, scale_columns, 'scaledFeatures')

#change cat_columns to individual columns
full_df_cat = mc.hot_encode(full_df, to_binary_columns, id_columns)

# COMMAND ----------

#add scaled columns, binary columns in one dataset
ready_df = full_df.select(*id_columns, *binary_columns)
ready_df = ready_df.join(full_df_cat, [*id_columns], how='outer')
ready_df = ready_df.join(full_df_scaled, [*id_columns], how='outer')
ready_df = ready_df.na.fill(0)
#ready_df.display()

# COMMAND ----------

cols = list(set(ready_df.columns) - set(id_columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Matching Algorithm

# COMMAND ----------

#number of control matches to return for each exposed member
num_matches = 5

#nlist = the number of cells to cluster the control into
n_list = 50

#nprobe = the number of cells to check for the nearest neighbors
n_probe = 10

#max_distance = (look into this one- what distance does FAISS return? euclidian?)
max_distance = 50

# COMMAND ----------

exp_df = ready_df.filter(ready_df['category']=='Case Management')
exp_ids = exp_df.select(*id_columns).toPandas()
exp_vars = exp_df.select(*cols).toPandas()

# COMMAND ----------

control_df = ready_df.filter(ready_df['category']=='control')
control_ids = control_df.select(*id_columns).toPandas()
control_vars = control_df.select(*cols).toPandas()

# COMMAND ----------

control_df = control_df.toPandas()
exp_df = exp_df.toPandas()

# COMMAND ----------

#original code broken out
index = mc.create_index(control_vars, n_list)
distances, neighbor_indexes = mc.search_index(index, exp_vars, num_matches, n_probe)

# COMMAND ----------

mc.search_index_test(index, control_vars, num_matches)

# COMMAND ----------

#this needs to be expanded to allow for more than one match
matched_record = mc.pick_matches(distances, neighbor_indexes, exp_vars, max_distance)

# COMMAND ----------

print(matched_record[(matched_record['control_index']<8000) & (matched_record['control_index']>0)])
#print(matched_record)

# COMMAND ----------

hm, hm2 = mc.tag_matches(matched_record, control_df, exp_df)
print(hm.head())
print(hm2.head())

# COMMAND ----------

hm = mc.tag_matches(matched_record, control_ids, exp_ids)
print(hm.head())

# COMMAND ----------

#reload
import importlib
from src import matching_class

importlib.reload(matching_class)
mc = matching_class.Cohort_Matching()
