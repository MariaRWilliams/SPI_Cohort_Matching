# Databricks notebook source
# MAGIC %md
# MAGIC ##Process Matched Cohorts
# MAGIC - load matched cohorts and additional details
# MAGIC - select sample matches
# MAGIC - create graphs and tables of statistics

# COMMAND ----------

from src import data_class
import pyspark.sql.functions as F

dc = data_class.Data_Processing()

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended dev.`clinical-analysis`.cohort_matching_cohorts

# COMMAND ----------

from delta.tables import DeltaTable

table_path = "s3://nexus-ops-uc-metastore-806618191677/metastore/af4696e8-4682-4504-ba50-57cc83320343/tables/b53976ef-10bf-4df4-8cca-0d3e7258bfa9"
deltaTable = DeltaTable.forPath(spark, table_path)

history_df = deltaTable.history() \
    .select("version") \
    .orderBy("version", ascending=False)

version = history_df.collect()[0][0]

print(version)

# df = spark.read \
#     .format("delta") \
#     .option("versionAsOf",version) \
#     .load(delta_table_path)

# COMMAND ----------

#matched_df has matching variables, details_df has additional information
matched_df = dc.query_data(spark, dbutils, 'cohort_matching_cohorts_matched')
details_df = dc.query_data(spark, dbutils, 'cohort_matching_cohorts')

# COMMAND ----------

#available variables for comparison
details_df.columns

# COMMAND ----------

#choose id columns, and variables for analysis
join_id_col = ['person_id', 'category', 'utc_period']
display_id_col = ['category']

#select compare columns for the final graph (selected this way so they are ordered)
compare_col = ['total_allowed-3', 'total_allowed-2', 'total_allowed-1', 'total_allowed0', 'total_allowed1', 'total_allowed2', 'total_allowed3', 'total_allowed4', 'total_allowed5', 'total_allowed6', 'total_allowed7', 'total_allowed8', 'total_allowed9', 'total_allowed10', 'total_allowed11']
preperiod = 3
postperiod = 11

col = display_id_col + compare_col

# COMMAND ----------

cats = matched_df.select('category').distinct().toPandas()['category'].tolist()
cats = [x for x in cats if 'control' not in x]
print(cats)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Samples

# COMMAND ----------

#check sample (can leave off sample category)
sample_category = 'Lantern'
sample_num = 3

sample_exposed_df, sample_control_df = dc.sample_matches(matched_df, sample_num, sample_category)
#sample_exposed_df, sample_control_df = dc.sample_matches(matched_df, sample_num)
sample_exposed_df.orderBy('match_key').display()
sample_control_df.orderBy('match_key').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Statistics

# COMMAND ----------

#full stats for matched cohorts
#dev note: will need to solve for duplicates made when details are joined, add more details such as matching criteria/closeness

matched_agg_df = matched_df.groupby('category').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at time of int'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min interaction period'),
                                F.max('utc_period').alias('max interaction period')
                                )

#matched_agg_df.display()

# COMMAND ----------

#add difference from original cohort
orginal_agg_df = details_df.groupby('category').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at time of int'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min interaction period'),
                                F.max('utc_period').alias('max interaction period')
                                )

cohort_high_level = matched_agg_df.join(orginal_agg_df.withColumn('cohort_records', F.col('record_count')).select('category', 'cohort_records'), on='category', how='inner').withColumn('cohort_percent', F.round((F.col('record_count')/F.col('cohort_records'))*100, 2))

# COMMAND ----------

#high level display
cohort_high_level.display()

# COMMAND ----------

#join comparison details to matched cohort
full_df = matched_df.withColumn('category_long', matched_df['category'])
full_df = full_df.withColumn('category', F.when(F.col('category').contains('control'), 'control').otherwise(F.col('category')))

full_df = full_df.alias('df1').join(details_df.alias('df2'), join_id_col, 'left').select('df2.*', 'category_long', 'match_key')
full_df = full_df.withColumn('category', full_df['category_long']).distinct()

# COMMAND ----------

#details statistics
full_agg_df = full_df.groupby('category').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at time of int'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min interaction period'),
                                F.max('utc_period').alias('max interaction period')
                                )

#full_agg_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Graph: Matched Cohort versus Matched Control

# COMMAND ----------

#chart
for this in cats:

  chart_df = full_df.filter(full_df['category'].contains(this)).select(*col).orderBy('category')
  chart_df = chart_df.groupby(*display_id_col).agg(*[F.round(F.avg(F.col(x)),2).alias(x) for x in col if x not in join_id_col])

  if this == cats[0]:
    full_chart = chart_df
  else:
    full_chart = full_chart.union(chart_df)

  chart_df = chart_df.toPandas()
  chart_df = chart_df[col].set_index('category').T
  chart_df.plot.line(figsize = (9,3), title = this, rot=30).vlines(x=3, ymin=0, ymax=chart_df.to_numpy().max(), ls='--')

# COMMAND ----------

# full_chart.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Graph: Matched Cohort, Matched Control, Original Cohort

# COMMAND ----------

#discarded members
disc_df = details_df.join(matched_df, on=join_id_col, how='leftanti')

#full dataset with labels
hm_df = details_df.withColumn('category', F.concat(F.lit('full '), details_df.category)).select(*details_df.columns).distinct()
hm_df = hm_df.union(full_df.withColumn('category', F.concat(F.lit('matched '), full_df.category)).select(*hm_df.columns).distinct())

for this in cats:

  chart_df = hm_df.filter(hm_df['category'].contains(this)).select(*col).orderBy('category')
  chart_df = chart_df.groupby(*display_id_col).agg(*[F.round(F.avg(F.col(x)),2).alias(x) for x in col if x not in join_id_col])
  
  if this == cats[0]:
    full_chart = chart_df
  else:
    full_chart = full_chart.union(chart_df)

  chart_df = chart_df.toPandas()
  chart_df = chart_df[col].set_index('category').T
  chart_df.plot.line(figsize = (9,3), title = this, rot=30).vlines(x=3, ymin=0, ymax=chart_df.to_numpy().max(), ls='--')

# COMMAND ----------

#full_chart.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Final Analysis

# COMMAND ----------

final_table = full_chart.withColumn('PMPM_preperiod', F.round((F.col('total_allowed-3')+F.col('total_allowed-2')+F.col('total_allowed-1'))/3, 2))
final_table = final_table.withColumn('PMPM_postperiod',  F.round((F.col('total_allowed0')+F.col('total_allowed1')+F.col('total_allowed2')+F.col('total_allowed3')+F.col('total_allowed4')+F.col('total_allowed5'))/6, 2))

final_table.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Extra

# COMMAND ----------

#reload
# import importlib
# from src import data_class
# importlib.reload(data_class)
# dc = data_class.Data_Processing()
