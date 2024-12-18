# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Exploration
# MAGIC Exploring matched pairs after they have already been selected

# COMMAND ----------

from src import data_class
import pyspark.sql.functions as F

dc = data_class.Data_Processing()

# COMMAND ----------

#matched_df has matching variables, details_df has additional information
matched_df = dc.query_data(spark, dbutils, 'cohort_matching_cohorts_matched')
details_df = dc.query_data(spark, dbutils, 'cohort_matching_cohorts')

# COMMAND ----------

join_id_col = ['person_id', 'category', 'utc_period']
display_id_col = ['category']
compare_col = ['total_allowed-3', 'total_allowed-2', 'total_allowed-1', 'total_allowed0', 'total_allowed1', 'total_allowed2', 'total_allowed3', 'total_allowed4', 'total_allowed5']

col = display_id_col + compare_col

# COMMAND ----------

#join comparison details to matched cohort
full_df = matched_df.withColumn('category_long', matched_df['category'])
full_df = full_df.withColumn('category', F.when(F.col('category').contains('control'), 'control').otherwise(F.col('category')))

full_df = full_df.alias('df1').join(details_df.alias('df2'), join_id_col, 'left').select('df2.*', 'category_long', 'match_key')
full_df = full_df.withColumn('category', full_df['category_long']).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Samples

# COMMAND ----------

#reload
# import importlib
# from src import data_class
# importlib.reload(data_class)
# dc = data_class.Data_Processing()

# COMMAND ----------

#check sample (can leave off sample category)
sample_category = 'Virta'
sample_num = 3

#sample_exposed_df, sample_control_df = dc.sample_matches(matched_df, sample_num, sample_category)
sample_exposed_df, sample_control_df = dc.sample_matches(full_df, sample_num)
sample_exposed_df.orderBy('match_key').display()
sample_control_df.orderBy('match_key').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Stats

# COMMAND ----------

#original cohort stats
details_df = details_df.distinct()
details_df.groupby('category').agg(F.count(F.lit(1)).alias('record_count'),  
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at period 0'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min_period'),
                                F.max('utc_period').alias('max_period'),
                                ).orderBy('category').display()

# COMMAND ----------

#matched cohort stats
matched_df.groupby('category').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at period 0'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min_period'),
                                F.max('utc_period').alias('max_period'),
                                ).orderBy('category').display()

# COMMAND ----------

#discarded cohort stats
disc_df = details_df.join(matched_df, on=join_id_col, how='leftanti')

disc_df.groupby('category').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at period 0'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min_period'),
                                F.max('utc_period').alias('max_period'),
                                ).orderBy('category').display()

# COMMAND ----------

#together to check for issues: duplicates caused by zip code?
full_df.groupby('category').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at period 0'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min_period'),
                                F.max('utc_period').alias('max_period'),
                                ).orderBy('category').display()

# COMMAND ----------

#graph differences in spend by cohort
# hm_df = disc_df.withColumn('category', F.concat(F.lit('discarded '), F.col('category')))
# hm_df = hm_df.union(details_df.withColumn('category', F.concat(F.lit('full '), details_df.category)).select(*hm_df.columns).distinct())
hm_df = details_df.withColumn('category', F.concat(F.lit('full '), details_df.category)).select(*disc_df.columns).distinct()
hm_df = hm_df.union(full_df.withColumn('category', F.concat(F.lit('matched '), full_df.category)).select(*hm_df.columns).distinct())

# COMMAND ----------

# cats = hm_df.select('category').distinct().toPandas()['category'].tolist()
# cats = [x for x in cats if 'control' not in x]

cats = disc_df.filter(F.col('category')!='control').select('category').distinct().toPandas()['category'].tolist()

for this in cats:

  chart_df = hm_df.filter(hm_df['category'].contains(this)).select(*col).orderBy('category')
  chart_df = chart_df.groupby(*display_id_col).agg(*[F.round(F.avg(F.col(x)),2).alias(x) for x in col if x not in join_id_col])
  #chart_df.display()

  chart_df = chart_df.toPandas()
  chart_df = chart_df[col].set_index('category').T
  #print(chart_df)
  chart_df.plot.line(figsize = (9,3), title = this, rot=30).vlines(x=3, ymin=0, ymax=chart_df.to_numpy().max(), ls='--')

# COMMAND ----------

# MAGIC %md
# MAGIC ####(Work area: optimizing some other logic)

# COMMAND ----------

tester = matched_df.limit(10000)
tester2 = matched_df.limit(10000)

