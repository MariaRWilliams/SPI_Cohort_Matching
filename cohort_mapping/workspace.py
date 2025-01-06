# Databricks notebook source
# MAGIC %md
# MAGIC ##Workspace
# MAGIC This is just a notebook for noodling so that we aren't constantly pushing updates to the main scripts

# COMMAND ----------

from src import data_class
import pyspark.sql.functions as F

dc = data_class.Data_Processing()

# COMMAND ----------

#matched_df has matching variables, details_df has additional information
matched_df = dc.query_data(spark, dbutils, 'cohort_matching_cohorts_matched')
details_df = dc.query_data(spark, dbutils, 'cohort_matching_cohorts')

# COMMAND ----------

# MAGIC %md
# MAGIC ##checking ms versus cg

# COMMAND ----------

ms_df = dc.query_data(spark, dbutils, 'cohort_matching_ms_claims')
cg_df = dc.query_data(spark, dbutils, 'cohort_matching_cg_claims')

# COMMAND ----------

ms_df.columns

# COMMAND ----------

#claims stats
ms_df.withColumn('year', F.substring(F.col('service_month'), 1, 4).cast('int')).groupby('year').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('med_total'), 2).alias('med_total'), 
                                F.round(F.mean('pharma_total'), 2).alias('pharma_total'), 
                                F.round(F.mean('total_claims'), 2).alias('total_claims'),
                                F.mean('readmission').alias('readmission'),
                                #F.mean('mammogram').alias('mammogram'),
                                F.mean('avoidable_er').alias('avoidable_er'),
                                F.mean('er').alias('er'),
                                #F.mean('surgery').alias('surgery'),
                                F.mean('inpatient').alias('inpatient'),
                                F.mean('office_visits').alias('office_visits')
                                ).display()

cg_df.withColumn('year', F.substring(F.col('service_month'), 1, 4).cast('int')).groupby('year').agg(F.count(F.lit(1)).alias('record_count'), 
                                F.round(F.mean('med_total'), 2).alias('med_total'), 
                                F.round(F.mean('pharma_total'), 2).alias('pharma_total'), 
                                F.round(F.mean('total_claims'), 2).alias('total_claims'),
                                F.mean('readmission').alias('readmission'),
                                #F.mean('mammogram').alias('mammogram'),
                                F.mean('avoidable_er').alias('avoidable_er'),
                                F.mean('er').alias('er'),
                                #F.mean('surgery').alias('surgery'),
                                F.mean('inpatient').alias('inpatient'),
                                F.mean('office_visits').alias('office_visits')
                                ).display()

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ###limiting duplicates when details added

# COMMAND ----------

#join comparison details to matched cohort
full_df = matched_df.withColumn('category_long', matched_df['category'])
full_df = full_df.withColumn('category', F.when(F.col('category').contains('control'), 'control').otherwise(F.col('category')))

full_df = full_df.alias('df1').join(details_df.alias('df2'), join_id_col, 'left').select('df2.*', 'category_long', 'match_key')
full_df = full_df.withColumn('category', full_df['category_long']).distinct()

# COMMAND ----------

full_df.filter(F.col('category').isin(['Carrum Health', 'Lantern'])).select('edw_cust').distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###getting earlier version?

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history dev.`clinical-analysis`.cohort_matching_cohorts

# COMMAND ----------

df = spark.sql(f"""SELECT * FROM dev.`clinical-analysis`.cohort_matching_cohorts Version as of 31""")

df.limit(10).display()

# COMMAND ----------

path = 's3://nexus-ops-uc-metastore-806618191677/metastore/af4696e8-4682-4504-ba50-57cc83320343/tables/3425ebcd-fad1-45a1-991e-5b632bc68b87'
path2 = "dev.`clinical-analysis`.cohort_matching_cohorts"
# df = spark.read.format("delta").option("versionAsOf", 24).load(path)

df = spark.read.option("versionAsOf", 24).table(path2)

df.limit(10).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM system.access.table_lineage
# MAGIC limit 4

# COMMAND ----------

#reload
# import importlib
# from src import data_class
# importlib.reload(data_class)
# dc = data_class.Data_Processing()

# COMMAND ----------

tester = matched_df.limit(10000)
tester2 = matched_df.limit(10000)

