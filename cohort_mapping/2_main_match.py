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

#dictionary of weights
mc.weights = {'total_allowed0': 5,
              'total_allowed-1': 3,
              'total_allowed-2': 2,
              'total_allowed-3': 1, }


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
# MAGIC ###(Non Loop Version)

# COMMAND ----------

#choose cohort (loop later)
cohort = 'HCC Clinical Eng'

#loop through demographic combos
mc.fixed_cols = list(set(ready_df.columns) - set(mc.id_columns) - set(mc.scale_columns))

demo_combos = ready_df.select(*mc.fixed_cols).distinct()
dc_num = len(demo_combos.collect())
#demo_combos.display()

counter = 1
#for row in demo_combos.rdd.toLocalIterator():
row = demo_combos.first()
#print(row)
print('Processing '+cohort+' '+str(counter)+' of '+str(dc_num))


# COMMAND ----------


#get control/ exposed (there has got to be a more pythonic way...)
this_control = ready_df.filter(ready_df['category']=='control')
this_exposed = ready_df.filter(ready_df['category']==cohort)

for x in mc.fixed_cols:
    this_control = this_control.filter(this_control[x] == row[x])
    this_exposed = this_exposed.filter(this_exposed[x] == row[x])

# print('exposed count '+str(this_exposed.count()))
# print('countrol count '+str(this_control.count()))
# add line here to check if enough control? or should that be covered by match distance?

# COMMAND ----------

#make index
control_ids = this_control.select(*mc.id_columns).toPandas()
control_vars = this_control.select(*mc.scale_columns).toPandas()
index = mc.create_index(control_vars)

# COMMAND ----------

#search index
exp_ids = this_exposed.select(*mc.id_columns).toPandas()
exp_vars = this_exposed.select(*mc.scale_columns).toPandas()
distances, neighbor_indexes = mc.search_index(index, exp_vars)


# COMMAND ----------

#collect matches
matched_record = mc.pick_matches(distances, neighbor_indexes, exp_vars)
exposed_matched, control_matched = mc.tag_matches(matched_record, control_ids, exp_ids)

# COMMAND ----------

#switch back to spark
matched_record = spark.createDataFrame(matched_record)
exposed_matched = spark.createDataFrame(exposed_matched)
control_matched = spark.createDataFrame(control_matched)

# COMMAND ----------

#detail matches
final_matched = mc.detail_matches(spark, matched_record, exposed_matched, control_matched, full_df, 1)

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
