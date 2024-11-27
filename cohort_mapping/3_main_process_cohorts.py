# Databricks notebook source
# MAGIC %md
# MAGIC ##Process Matched Cohorts
# MAGIC - load matched data with matching variables
# MAGIC - collect additional data for comparisons
# MAGIC - stats on differences between groups

# COMMAND ----------

from src import data_class
import pyspark.sql.functions as F

dc = data_class.Data_Processing()

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

#compare_col_prefix = ['total_allowed']
#col = display_id_col + [column for column in details_df.columns if column.startswith(tuple(compare_col_prefix)) and not column.endswith('sum')]

#doing this way to order the columns in the final graph
compare_col = ['total_allowed-3', 'total_allowed-2', 'total_allowed-1', 'total_allowed0', 'total_allowed1', 'total_allowed2', 'total_allowed3', 'total_allowed4', 'total_allowed5']

col = display_id_col + compare_col

# COMMAND ----------

# MAGIC %md
# MAGIC ###Statistics

# COMMAND ----------

#stats - will change when joined to other details 
#would be great to get what matched on exactly, and not exactly, but that would need to be exported at earlier step
#how many matches made, how close they are on average
agg_df = matched_df.groupby('category').agg(F.count('person_id').alias('count'), 
                                F.round(F.sum('cancer')).alias('members with cancer'), 
                                F.round(F.sum('diabetes')).alias('members with diabetes'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at time of int'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min interaction period'),
                                F.max('utc_period').alias('max interaction period')
                                )

agg_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Samples

# COMMAND ----------

sample_category = 'HCC Clinical Eng'
sample_num = 5

sample_exposed_df, sample_control_df = dc.sample_matches(matched_df, sample_num, sample_category)
sample_exposed_df.orderBy('match_key').display()
sample_control_df.orderBy('match_key').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Chart

# COMMAND ----------

#join comparison details to matched cohort
full_df = matched_df.withColumn('category_long', matched_df['category'])
full_df = full_df.withColumn('category', F.when(F.col('category').contains('control'), 'control').otherwise(F.col('category')))

full_df = full_df.alias('df1').join(details_df.alias('df2'), join_id_col, 'left').select('df2.*', 'category_long', 'match_key')
full_df = full_df.withColumn('category', full_df['category_long']).distinct()

# COMMAND ----------

#download details
#d_df = full_df.filter(full_df['category'].contains('Case Management - Adult')).orderBy('match_key').display()
#d_df = full_df.filter(full_df['category'].contains('HCC Clinical Eng')).orderBy('match_key').limit(100).display()

# COMMAND ----------

#chart
cats = full_df.select('category').distinct().toPandas()['category'].tolist()
cats = [x for x in cats if 'control' not in x]

for this in cats:

  chart_df = full_df.filter(full_df['category'].contains(this)).select(*col).orderBy('category')
  chart_df = chart_df.groupby(*display_id_col).agg(*[F.round(F.avg(F.col(x)),2).alias(x) for x in col if x not in join_id_col])
  #chart_df.display()

  chart_df = chart_df.toPandas()
  chart_df = chart_df[col].set_index('category').T
  #print(chart_df)
  chart_df.plot.line(figsize = (9,3), title = this, rot=30).vlines(x=3, ymin=0, ymax=chart_df.to_numpy().max(), ls='--')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Optional

# COMMAND ----------

#reload
# import importlib
# from src import data_class
# importlib.reload(data_class)
# dc = data_class.Data_Processing()
