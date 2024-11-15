# Databricks notebook source
# MAGIC %md
# MAGIC ##Process Matched Cohorts
# MAGIC - load matched data with matching variables
# MAGIC - collect additional data for comparisons
# MAGIC - stats on differences between groups

# COMMAND ----------

from src import prep_class
import pyspark.sql.functions as F

pc = prep_class.Data_Prep()

# COMMAND ----------

matched_df = pc.query_data(spark, dbutils, 'cohort_matching_cohorts_matched')
details_df = pc.query_data(spark, dbutils, 'cohort_matching_cohorts')

# COMMAND ----------

#available variables for comparison
details_df.columns

# COMMAND ----------

print(details_df.select('category').distinct().toPandas()['category'].to_list())

# COMMAND ----------

#choose id columns, and variables for analysis
join_id_col = ['person_id', 'category', 'utc_period']
display_id_col = ['category']
compare_col_prefix = ['total_allowed']

#only works with one prefix- fix that
col = display_id_col + [column for column in details_df.columns if column.startswith(*compare_col_prefix) and not column.endswith('sum')]

# COMMAND ----------

print(col)

# COMMAND ----------

#join comparison details to matched cohort and aggregate
full_df = matched_df.alias('df1').join(details_df.alias('df2'), join_id_col, 'left').select('df2.*')
full_df = full_df.select(*col)

agg_df = full_df.groupby(*display_id_col).agg(*[F.round(F.avg(F.col(x)),2).alias(x) for x in col if x not in join_id_col])
agg_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Chart

# COMMAND ----------

chart_df = agg_df.toPandas()
chart_df = chart_df[col].set_index('category').T
#print(chart_df)
chart_df.plot.line(figsize = (15,5), title = 'PMPM spend')
