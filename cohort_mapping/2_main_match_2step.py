# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Matching Model
# MAGIC Version that creates an index for exact matching before searching it for most similar

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

#curently, the binary columns are used for the index and the scale columns are used for similarity matching
#in the future, may need to separate lists of columns to scale/encode and list of columns to index/match
id_columns = ['person_id', 'category', 'utc_period']

#select variables used for indexing (perfect match)
binary_columns = ['depression', 'hyperlipidemia', 'osteoarthritis', 'chf', 'cancer', 'diabetes', 'cad', 'copd']
to_binary_columns = ['sex', 'age_band']

#select variables used for closest match
scale_columns = ['age',
                'date_int',
                'med_allowed_-3to0sum',
                'pharma_allowed_-3to0sum',
                'total_allowed_-3to0sum',
                'inpatient_-3to0sum',
                'physician_-3to0sum',
                'emergency_room_-3to0sum',
                'office_procedures_-3to0sum',
                'outpatient_services_-3to0sum',
                'outpatient_urgent_care_-3to0sum'
                ]


final_columns = id_columns + binary_columns + scale_columns + to_binary_columns


# COMMAND ----------

ready_df = mc.full_transformation(id_columns, binary_columns, scale_columns, to_binary_columns, full_df)

# COMMAND ----------

cols = list(set(ready_df.columns) - set(id_columns))
#print(cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Matching Algorithm: Setup

# COMMAND ----------

#analysis variables
num_possible_matches = 10
num_final_matches = 2

#model variables
#nlist = the number of cells to cluster the control into (4 * sqrt(n) is standard?)
#nprobe = the number of cells to check for the nearest neighbors
#max_distance = (look into this one- what distance does FAISS return? euclidian?)
n_list = 5
n_probe = 5
max_distance = 5

# COMMAND ----------

#possible exposed cohorts
print(ready_df.select('category').distinct().toPandas()['category'].to_list())

# COMMAND ----------

#choose cohort (loop later)
cohort = 'HCC Clinical Eng'

# COMMAND ----------

#loop through demographic combos
demo_cols = list(set(ready_df.columns) - set(id_columns) - set(scale_columns))
demo_combos = ready_df.select(*demo_cols).distinct()
dc_num = len(demo_combos.collect())
#demo_combos.display()

counter = 1
#for row in demo_combos.rdd.toLocalIterator():
row = demo_combos.first()
print('Processing '+cohort+' '+str(counter)+' of '+str(dc_num))


# COMMAND ----------

# MAGIC %md
# MAGIC ###Matching Algorithm: Loop
# MAGIC 1) An index is created of the control records that match exactly on the relevant variables
# MAGIC 2) That index is searched and returns a set of closest matches for each exposed record
# MAGIC 3) Each set is filtered until each exposed record has a set of close matches unique to itself
# MAGIC 4) Matched records are recorded and loop continues for next set of exact matches

# COMMAND ----------

#get control/ exposed (there has got to be a more pythonic way...)
this_control = ready_df.filter(ready_df['category']=='control')
this_exposed = ready_df.filter(ready_df['category']==cohort)

for x in demo_cols:
    this_control = this_control.filter(this_control[x] == row[x])
    this_exposed = this_exposed.filter(this_exposed[x] == row[x])

print('exposed count '+str(this_exposed.count()))
print('countrol count '+str(this_control.count()))

# COMMAND ----------

#make index
control_ids = this_control.select(*id_columns).toPandas()
control_vars = this_control.select(*scale_columns).toPandas()
index = mc.create_index(control_vars, n_list)

# COMMAND ----------

#search index
exp_ids = this_exposed.select(*id_columns).toPandas()
exp_vars = this_exposed.select(*scale_columns).toPandas()
distances, neighbor_indexes = mc.search_index(index, exp_vars, num_possible_matches, n_probe)


# COMMAND ----------

#collect matches
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

#reload
# import importlib
# from src import matching_class

# importlib.reload(matching_class)
# mc = matching_class.Cohort_Matching()

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
# (
#     final_matched
#     .write
#     .format("delta")
#     .option("overwriteSchema", "true")
#     .mode("overwrite")
#     .saveAsTable("dev.`clinical-analysis`.cohort_matching_cohorts_matched")
# )
