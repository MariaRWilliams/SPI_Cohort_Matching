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

from src import data_class
from src import matching_class
import pyspark.sql.functions as F
import pyspark.sql.types as T

mc = matching_class.Cohort_Matching()
dc = data_class.Data_Processing()

# COMMAND ----------

full_df = dc.query_data(spark, dbutils, 'cohort_matching_cohorts')

# COMMAND ----------

#stats: what else would be useful? do both before and after
full_df.groupby('category').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at mo0'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min_period'),
                                F.max('utc_period').alias('max_period'),
                                ).orderBy('category').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Choose / Prepare Variables

# COMMAND ----------

#available variables
full_df.columns

# COMMAND ----------

#currently, the binary columns are used for the index and the scale columns are used for similarity matching
#in the future, may need to separate lists of columns to scale/encode and list of columns to index/match
mc.id_columns = ['person_id', 'category', 'utc_period']

#select variables used for indexing (perfect match)
mc.binary_columns = []
mc.to_binary_columns = ['age_band', 'sex']

#select variables used for closest match
mc.scale_columns = ['total_allowed-1',
                    'total_allowed-2',
                    'total_allowed-3',
                    'total_allowed0',
                    'inpatient_-3to0sum',
                    'emergency_room_-3to0sum',
                    'physician_-3to0sum',
                    'age',
                    'cancer',
                    'chf',
                    'cad',
                    'hyperlipidemia',
                    'copd',
                    'diabetes',
                    'osteoarthritis',
                    'date_int',
                    # 'med_percent',
                    'outpatient_services_0to5sum',
                    'inpatient_0to5sum'
                ]

#dictionary of weights (weighted after scaling)
mc.weights = {
            'total_allowed-1':3,
            'total_allowed-2':3,
            'total_allowed-3':3,
            'total_allowed0':3,
              }

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
mc.n_list = 10
mc.n_probe = 8
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

match_cat = ['Carrum Health', 'Lantern']
for cohort in match_cat:

    matched, demo_combos_full = mc.main_match(spark, cohort, full_df, ready_df)

    if cohort == match_cat[0]:
        final_matched = matched
    else:
        final_matched = final_matched.union(matched)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Final Steps: Statistics Overview and Export

# COMMAND ----------

#statistics of original cohort
full_df.groupby('category').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at mo0'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min_period'),
                                F.max('utc_period').alias('max_period'),
                                ).orderBy('category').display()

# COMMAND ----------

#statistics for matched cohort
final_matched.groupby('category').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at mo0'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min_period'),
                                F.max('utc_period').alias('max_period'),
                                ).orderBy('category').display()

# COMMAND ----------

#join comparison details to matched cohort
matched_df = final_matched.withColumn('category_long', final_matched['category'])
matched_df = matched_df.withColumn('category', F.when(F.col('category').contains('control'), 'control').otherwise(F.col('category')))

matched_df = matched_df.alias('df1').join(full_df.alias('df2'), mc.id_columns, 'left').select('df2.*', 'category_long', 'match_key')
matched_df = matched_df.withColumn('category', matched_df['category_long']).distinct()

# COMMAND ----------

#statistics for matched cohort with details (created duplicates?)
matched_df.groupby('category').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at mo0'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min_period'),
                                F.max('utc_period').alias('max_period'),
                                ).orderBy('category').display()

# COMMAND ----------

#chart
chart_category = 'Lantern'
join_id_col = ['person_id', 'category', 'utc_period']
display_id_col = ['category']
compare_col = ['total_allowed-3', 'total_allowed-2', 'total_allowed-1', 'total_allowed0', 'total_allowed1', 'total_allowed2', 'total_allowed3', 'total_allowed4', 'total_allowed5']

col = display_id_col + compare_col
chart_df = matched_df.filter(F.col('category').startswith(chart_category)).groupby(*display_id_col).agg(*[F.round(F.avg(F.col(x)),2).alias(x) for x in col if x not in join_id_col])
#chart_df.display()

chart_df = chart_df.toPandas()
chart_df = chart_df[col].set_index('category').T
chart_df.plot.line(figsize = (9,3), rot=30).vlines(x=3, ymin=0, ymax=chart_df.to_numpy().max(), ls='--')

# COMMAND ----------

#check sample
sample_exposed_df, sample_control_df = dc.sample_matches(final_matched, 3)
sample_exposed_df.orderBy('match_key').display()
sample_control_df.orderBy('match_key').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write Data to Table

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

#reload
# import importlib
# from src import matching_class

# importlib.reload(matching_class)
# mc = matching_class.Cohort_Matching()
